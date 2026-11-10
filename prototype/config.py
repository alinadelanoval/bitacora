"""Configuración del prototipo: carpeta de datos, política de fsync y timeout de replicación.

Las réplicas y el Controller leen la misma configuración, de modo que todos
guardan sus archivos en la misma carpeta.

Variables de entorno:
- BITACORA_DATA_DIR: carpeta de los archivos. Por defecto, `data/ray/` en la
  raíz del repositorio (ignorada por Git).
- BITACORA_FSYNC: "1" para hacer fsync tras cada lote. Por defecto solo flush,
  que sobrevive a la caída del proceso pero no a la del sistema.
- BITACORA_REPLICATION_TIMEOUT_MS: cuánto espera el líder a una seguidora, y el
  Controller a una réplica, antes de tratarla como no disponible. Por defecto,
  2000. Un valor que no sea un entero positivo impide arrancar.
"""

import os
from dataclasses import dataclass
from pathlib import Path

# Namespace de Ray donde viven los actores con nombre del prototipo.
NAMESPACE = "bitacora"

# La ruta por defecto se calcula desde este archivo y no desde el directorio de
# trabajo, que puede cambiar según quién arranque el proceso.
DEFAULT_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "ray"

DEFAULT_REPLICATION_TIMEOUT_MS = 2000


@dataclass(frozen=True)
class Config:
    data_dir: Path
    fsync_per_batch: bool = False
    # En segundos, la unidad de ray.wait y ray.get.
    replication_timeout: float = DEFAULT_REPLICATION_TIMEOUT_MS / 1000


def load() -> Config:
    """Lee la configuración de las variables de entorno."""
    data_dir = Path(os.environ.get("BITACORA_DATA_DIR", DEFAULT_DATA_DIR))
    fsync_per_batch = os.environ.get("BITACORA_FSYNC", "0") == "1"
    return Config(
        data_dir=data_dir,
        fsync_per_batch=fsync_per_batch,
        replication_timeout=_replication_timeout_ms() / 1000,
    )


def _replication_timeout_ms() -> int:
    """Lee BITACORA_REPLICATION_TIMEOUT_MS y rechaza los valores sin sentido.

    Con 0, el líder sacaría del ISR a toda seguidora en cada lote; con un texto
    no numérico no hay valor razonable que suponer. En ambos casos es mejor
    no arrancar que funcionar con un timeout distinto del pedido.
    """
    raw = os.environ.get("BITACORA_REPLICATION_TIMEOUT_MS", str(DEFAULT_REPLICATION_TIMEOUT_MS))
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"BITACORA_REPLICATION_TIMEOUT_MS debe ser un entero: {raw!r}") from None
    if value <= 0:
        raise ValueError(f"BITACORA_REPLICATION_TIMEOUT_MS debe ser positivo: {value}")
    return value
