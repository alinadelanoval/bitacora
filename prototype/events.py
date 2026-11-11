"""Registro de transiciones: una línea JSON por evento, solo añadiendo al final.

Cada archivo tiene un único escritor (el actor que lo creó), así que no hace
falta coordinar escrituras. El registro es para diagnóstico y evidencia: el
estado que importa se guarda aparte, y un evento perdido en una caída no
cambia el comportamiento del sistema.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path


def append_event(path: Path, event: str, fsync: bool = False, **fields) -> None:
    """Añade `{"ts": ..., "event": ..., **fields}` como una línea al final de `path`."""
    timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    line = json.dumps({"ts": timestamp, "event": event, **fields}, ensure_ascii=False)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()
        if fsync:
            os.fsync(f.fileno())
