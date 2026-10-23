#!/usr/bin/env python3

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "data" / "dev" / "cluster"
STATE_FILE = STATE_DIR / "processes.json"
NODES = ("node-1", "node-2", "node-3")


def read_process_args(pid: int) -> list[str] | None:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except FileNotFoundError:
        return None
    return [part.decode(errors="replace") for part in raw.split(b"\0") if part]


def matches_process(entry: dict[str, str]) -> bool:
    args = read_process_args(int(entry["pid"]))
    if not args:
        return False
    expected_binary = str(Path(entry["binary"]).resolve())
    return (
        args[0] == expected_binary
        and "--dev-placeholder" in args
        and entry["node_id"] in args
        and entry["run_id"] in args
    )


def load_entries() -> list[dict[str, str]]:
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    if not isinstance(data, list):
        raise ValueError(f"invalid cluster state file: {STATE_FILE}")
    return data


def write_entries(entries: list[dict[str, str]]) -> None:
    temporary = STATE_FILE.with_suffix(".tmp")
    temporary.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, STATE_FILE)


def start_cluster() -> None:
    if not Path("/proc").is_dir():
        raise RuntimeError("the local process runner is supported on Linux only")

    binary = ROOT / "data" / "dev" / "bitacorad"
    if not binary.is_file():
        raise RuntimeError("missing data/dev/bitacorad; run `make cluster` from the repository root")

    existing = load_entries()
    if existing:
        unknown = [
            entry
            for entry in existing
            if read_process_args(int(entry["pid"])) and not matches_process(entry)
        ]
        if unknown:
            raise RuntimeError(
                "saved process IDs no longer identify the Bitacora test processes; inspect processes.json before continuing"
            )
        if any(matches_process(entry) for entry in existing):
            raise RuntimeError("the local test processes are already running; use `make stop` first")
        STATE_FILE.unlink(missing_ok=True)

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    processes: list[subprocess.Popen[bytes]] = []
    entries: list[dict[str, str]] = []
    log_files = []

    try:
        for node_id in NODES:
            log_file = (STATE_DIR / f"{node_id}.log").open("ab")
            log_files.append(log_file)
            process = subprocess.Popen(
                [
                    str(binary),
                    "--dev-placeholder",
                    "--node-id",
                    node_id,
                    "--run-id",
                    run_id,
                ],
                cwd=ROOT,
                stdin=subprocess.DEVNULL,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            processes.append(process)
            entries.append(
                {
                    "pid": str(process.pid),
                    "binary": str(binary),
                    "node_id": node_id,
                    "run_id": run_id,
                }
            )
            write_entries(entries)

        time.sleep(0.2)
        failed = [
            entry["node_id"]
            for entry, process in zip(entries, processes)
            if process.poll() is not None
        ]
        if failed:
            raise RuntimeError(f"test process exited during startup: {', '.join(failed)}")
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError):
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            process.wait(timeout=5)
        STATE_FILE.unlink(missing_ok=True)
        raise
    finally:
        for log_file in log_files:
            log_file.close()

    for entry in entries:
        print(f"started {entry['node_id']} (pid {entry['pid']})")


def stop_cluster() -> None:
    if not Path("/proc").is_dir():
        raise RuntimeError("the local process runner is supported on Linux only")

    entries = load_entries()
    if not entries:
        print("no local test processes are recorded")
        return

    mismatched = [
        entry
        for entry in entries
        if read_process_args(int(entry["pid"])) and not matches_process(entry)
    ]
    if mismatched:
        raise RuntimeError("a saved process ID belongs to another process; no processes were stopped")

    live = [entry for entry in entries if matches_process(entry)]
    for entry in live:
        os.kill(int(entry["pid"]), signal.SIGTERM)

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and any(matches_process(entry) for entry in live):
        time.sleep(0.1)

    for entry in live:
        if matches_process(entry):
            os.kill(int(entry["pid"]), signal.SIGKILL)

    if any(matches_process(entry) for entry in live):
        raise RuntimeError("one or more test processes could not be stopped")

    STATE_FILE.unlink(missing_ok=True)
    for entry in live:
        print(f"stopped {entry['node_id']} (pid {entry['pid']})")


def main() -> int:
    parser = argparse.ArgumentParser(description="Start or stop the three local BIT-01 test processes.")
    parser.add_argument("action", choices=("start", "stop"))
    args = parser.parse_args()

    try:
        if args.action == "start":
            start_cluster()
        else:
            stop_cluster()
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError, subprocess.SubprocessError) as error:
        print(f"cluster: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
