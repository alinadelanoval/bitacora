#!/usr/bin/env python3
"""Una ejecución de EXP-001, EXP-002 o EXP-003 sobre el prototipo Ray.

Arranca el gateway con una carpeta de datos nueva, lanza la carga con
tools/loadgen y, si se pide, termina con SIGKILL el proceso worker de la
réplica líder (`F1` / `F15`). Al acabar la carga ejecuta tools/checker y deja
en la carpeta de salida:

    config.yaml      parámetros de la ejecución, versiones y commit
    history.ndjson   historial del LoadGen
    summary.json     resumen del LoadGen (rendimiento, latencia, errores)
    checker.json     informe del Checker; checker.txt, su versión en texto
    fault.json       caída inyectada, cambio de líder y hueco sin escrituras ok
    nodes/           events.ndjson y controller.json del Controller, log del gateway

    python scripts/experiments/leader_loss.py --out evidence/local/EXP-002/run-01 \\
        --acks all --kill-after 10 [--task-retries 3]

Sin --kill-after es la línea base (EXP-001). El código de salida es el del
Checker (1 si alguna propiedad está violada) o 2 si la ejecución no se pudo
completar.
"""

import argparse
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import ray
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts" / "test"))

from contract_ray import wait_for_listening  # noqa: E402

from prototype.actors.controller import CONTROLLER_NAME, EVENTS_FILE  # noqa: E402
from prototype.actors.partition_replica import replica_actor_name  # noqa: E402
from prototype.config import NAMESPACE  # noqa: E402

WORKLOAD = ROOT / "tools" / "loadgen" / "workloads" / "w5.json"
PARTITION = 0
CONDITION_TIMEOUT = 60  # s: plazo de cada espera por condición


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="carpeta de salida (no debe existir)")
    parser.add_argument("--acks", choices=["all", "leader"], default="all")
    parser.add_argument("--duration", type=int, default=30, help="segundos de carga")
    parser.add_argument("--kill-after", type=float, default=0,
                        help="segundos de carga antes de matar a la líder; 0 = sin fallo")
    parser.add_argument("--task-retries", type=int, default=0, help="max_task_retries de las réplicas")
    parser.add_argument("--workload", type=Path, default=WORKLOAD)
    args = parser.parse_args()
    if args.kill_after and not 0 < args.kill_after < args.duration:
        parser.error("--kill-after debe caer dentro de la carga")

    out: Path = args.out
    out.mkdir(parents=True, exist_ok=False)     # nunca se mezcla con una ejecución anterior
    (out / "nodes").mkdir()
    workload = json.loads(args.workload.read_text())
    topic = workload["topic"]

    with tempfile.TemporaryDirectory(prefix="bitacora-exp-") as tmp:
        tmp = Path(tmp)
        data_dir = tmp / "data"
        loadgen, checker = build_tools(tmp)
        write_config(out, args, workload)

        env = {**os.environ, "BITACORA_DATA_DIR": str(data_dir), "PYTHONPATH": str(ROOT),
               "BITACORA_REPLICA_TASK_RETRIES": str(args.task_retries)}
        env.pop("RAY_ADDRESS", None)            # siempre un nodo Ray propio
        gateway_log = open(out / "nodes" / "gateway.log", "w")
        gateway = subprocess.Popen(
            [sys.executable, "-u", "-m", "prototype.gateway", "--listen", "127.0.0.1:0"],
            cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=gateway_log, text=True)
        try:
            endpoint = wait_for_listening(gateway)
            log(f"gateway en {endpoint}")
            started = time.monotonic()
            load = subprocess.Popen(
                [loadgen, "-endpoint", endpoint, "-workload", str(args.workload), "-acks", args.acks,
                 "-duration", str(args.duration), "-history", str(out / "history.ndjson"),
                 "-summary", str(out / "summary.json")],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

            fault = {"injected": False}
            if args.kill_after:
                fault = inject_leader_loss(out / "history.ndjson", data_dir, topic, started, args.kill_after)

            load_out, _ = load.communicate(timeout=args.duration + 2 * CONDITION_TIMEOUT)
            (out / "loadgen.txt").write_text(load_out)
            print(load_out, end="")
            if load.returncode != 0:
                log(f"el LoadGen terminó con {load.returncode}")
                return 2
        except Exception as e:                  # la ejecución no vale: no se juzga
            log(f"ejecución incompleta: {e!r}")
            if load is not None and load.poll() is None:
                load.kill()
            return 2
        finally:
            gateway.send_signal(signal.SIGTERM)
            try:
                gateway.wait(timeout=60)
            except subprocess.TimeoutExpired:
                gateway.kill()
            gateway_log.close()
            for name in (EVENTS_FILE, "controller.json"):
                if (data_dir / name).exists():
                    shutil.copy(data_dir / name, out / "nodes" / name)

        fault.update(ok_gaps(out / "history.ndjson", fault.get("killed_at")))
        (out / "fault.json").write_text(json.dumps(fault, indent=2, ensure_ascii=False) + "\n")
        result = subprocess.run([checker, "-history", str(out / "history.ndjson"),
                                 "-json", str(out / "checker.json")], capture_output=True, text=True)
        (out / "checker.txt").write_text(result.stdout + result.stderr)
        print(result.stdout, end="")
        return result.returncode


def log(message: str) -> None:
    print(f"[{now()}] {message}", flush=True)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_tools(tmp: Path) -> tuple[str, str]:
    """Compila el LoadGen y el Checker del commit actual."""
    go = os.environ.get("GO", "go")
    paths = []
    for tool in ("loadgen", "checker"):
        path = tmp / tool
        subprocess.run([go, "build", "-o", str(path), f"./tools/{tool}"], cwd=ROOT, check=True)
        paths.append(str(path))
    return paths[0], paths[1]


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def write_config(out: Path, args, workload: dict) -> None:
    """Todo lo necesario para repetir la ejecución. El LoadGen no usa azar."""
    config = {
        "started": now(),
        "commit": git("rev-parse", "--short", "HEAD"),
        "dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
        "command": " ".join([Path(sys.argv[0]).name, *sys.argv[1:]]),
        "workload": {**workload, "acks": args.acks, "duration_s": args.duration},
        "fault": ({"scenario": "F1/F15", "signal": "SIGKILL", "target": "worker de la réplica líder",
                   "after_s": args.kill_after} if args.kill_after else None),
        "prototype": {"replica_task_retries": args.task_retries,
                      "replication_timeout_ms": int(os.environ.get("BITACORA_REPLICATION_TIMEOUT_MS", 2000)),
                      "fsync_per_batch": os.environ.get("BITACORA_FSYNC", "0") == "1"},
        "environment": {"platform": platform.platform(), "python": platform.python_version(),
                        "ray": ray.__version__,
                        "go": subprocess.run([os.environ.get("GO", "go"), "version"], capture_output=True,
                                             text=True).stdout.strip()},
        "seed": None,
    }
    (out / "config.yaml").write_text(yaml.safe_dump(config, allow_unicode=True, sort_keys=False))


def wait_for(condition, what: str):
    """Repite condition() hasta que devuelve algo verdadero o vence el plazo."""
    deadline = time.monotonic() + CONDITION_TIMEOUT
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.05)
    raise TimeoutError(f"{what}: no ocurrió en {CONDITION_TIMEOUT} s")


def ok_writes(history: Path) -> int:
    if not history.exists():
        return 0
    with open(history) as f:
        return sum(1 for line in f if '"result":"ok"' in line)


def inject_leader_loss(history: Path, data_dir: Path, topic: str, started: float, after: float) -> dict:
    """Mata el worker de la líder cuando la carga lleva `after` segundos y ya
    hay escrituras confirmadas; espera después a que el Controller registre el
    cambio de líder."""
    wait_for(lambda: time.monotonic() - started >= after and ok_writes(history) > 0,
             "carga en marcha con escrituras ok")
    ray.init(address="auto", namespace=NAMESPACE, log_to_driver=False)
    try:
        controller = ray.get_actor(CONTROLLER_NAME, namespace=NAMESPACE)
        leader = ray.get(controller.describe_topic.remote(topic)).leaders[PARTITION]
        replica = ray.get_actor(replica_actor_name(topic, PARTITION, leader), namespace=NAMESPACE)
        pid = ray.get(replica.runtime_info.remote())["pid"]
        ok_before = ok_writes(history)
        killed_at = now()
        os.kill(pid, signal.SIGKILL)
        log(f"SIGKILL al worker de la líder {leader} (pid {pid}) con {ok_before} escrituras ok")
    finally:
        ray.shutdown()

    def leader_change():
        events = data_dir / EVENTS_FILE
        if not events.exists():
            return None
        for line in events.read_text().splitlines():
            e = json.loads(line)
            if e.get("event") == "leader_change" and e.get("old_leader") == leader:
                return e
        return None

    change = wait_for(leader_change, "cambio de líder en events.ndjson")
    log(f"nueva líder {change.get('leader')} (ISR {change.get('isr')})")
    return {"injected": True, "scenario": "F1/F15", "killed_at": killed_at, "old_leader": leader, "pid": pid,
            "ok_writes_before_kill": ok_before, "leader_change": change}


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def ok_gaps(history: Path, killed_at: str | None) -> dict:
    """Mayor intervalo sin escrituras ok en toda la carga y, si hubo caída, el
    que empieza en la última escritura ok anterior a ella: la indisponibilidad
    que ve el cliente."""
    times = []
    with open(history) as f:
        for line in f:
            e = json.loads(line)
            if e["op"] == "produce" and e["result"] == "ok":
                times.append(parse_ts(e["ts"]))
    times.sort()
    gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
    result = {"max_ok_gap_s": round(max(gaps), 3) if gaps else None}
    if killed_at:
        kill = parse_ts(killed_at)
        before = [t for t in times if t <= kill]
        after = [t for t in times if t > kill]
        if before and after:
            result["unavailable_s"] = round((after[0] - before[-1]).total_seconds(), 3)
        else:
            result["unavailable_s"] = None     # no hubo escrituras ok a uno de los lados
    return result


if __name__ == "__main__":
    sys.exit(main())
