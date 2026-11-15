#!/usr/bin/env python3
"""Ejecuta contracttest/ contra el gateway del prototipo Ray (`make contract`).

Arranca `python -m prototype.gateway` en un puerto libre y con una carpeta de
datos temporal, espera a que escriba «listening», ejecuta la suite de Go con
BITACORA_ENDPOINTS apuntando a él (perfil básico) y detiene el gateway, que a
su vez apaga el nodo Ray que arrancó. El código de salida es el de `go test`.
"""

import os
import queue
import signal
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
START_TIMEOUT = 120  # s: arrancar Ray puede tardar en una máquina lenta


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="bitacora-contract-") as data_dir:
        env = {**os.environ, "BITACORA_DATA_DIR": data_dir, "PYTHONPATH": str(ROOT)}
        env.pop("RAY_ADDRESS", None)        # siempre un nodo Ray propio
        gateway = subprocess.Popen(
            [sys.executable, "-m", "prototype.gateway", "--listen", "127.0.0.1:0"],
            cwd=ROOT, env=env, stdout=subprocess.PIPE, text=True)
        try:
            endpoint = wait_for_listening(gateway)
            print(f"gateway en {endpoint}", flush=True)
            go = os.environ.get("GO", "go")
            test_env = {**os.environ, "BITACORA_ENDPOINTS": endpoint, "BITACORA_PROFILE": "basic"}
            return subprocess.call([go, "test", "-count=1", "-v", "./contracttest/..."], cwd=ROOT, env=test_env)
        finally:
            gateway.send_signal(signal.SIGTERM)
            try:
                gateway.wait(timeout=60)
            except subprocess.TimeoutExpired:
                gateway.kill()


def wait_for_listening(gateway: subprocess.Popen) -> str:
    """Lee la salida del gateway, en un hilo aparte, hasta «listening host:puerto»."""
    lines: queue.Queue = queue.Queue()

    def reader():
        for line in gateway.stdout:
            lines.put(line)
        lines.put(None)                     # el proceso cerró su salida

    threading.Thread(target=reader, daemon=True).start()
    while True:
        try:
            line = lines.get(timeout=START_TIMEOUT)
        except queue.Empty:
            raise TimeoutError(f"el gateway no arrancó en {START_TIMEOUT} s") from None
        if line is None:
            raise RuntimeError(f"el gateway terminó sin escuchar (código {gateway.wait()})")
        if line.startswith("listening "):
            return line.split()[1]


if __name__ == "__main__":
    sys.exit(main())
