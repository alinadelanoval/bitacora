"""Configuración del prototipo: carpeta de datos, fsync y plazos de replicación y de grupos.

Las réplicas y el Controller leen la misma configuración, de modo que todos
guardan sus archivos en la misma carpeta.

Variables de entorno:
- BITACORA_DATA_DIR: carpeta de los archivos. Por defecto, `data/ray/` en la
  raíz del repositorio (ignorada por Git).
- BITACORA_FSYNC: "1" para hacer fsync tras cada lote. Por defecto solo flush,
  que sobrevive a la caída del proceso pero no a la del sistema.
- BITACORA_REPLICATION_TIMEOUT_MS: cuánto espera el líder a una seguidora, y el
  Controller a una réplica, antes de tratarla como no disponible. Por defecto,
  2000.
- BITACORA_GROUP_SESSION_TIMEOUT_MS: tiempo sin latidos tras el que un miembro
  sale de su grupo. Por defecto, 10000.
- BITACORA_GROUP_HEARTBEAT_INTERVAL_MS: cada cuánto debe latir un miembro; el
  coordinador lo comunica en cada respuesta. Por defecto, 3000.
- BITACORA_GROUP_REBALANCE_TIMEOUT_MS: tiempo que tiene un miembro para dejar
  las particiones que ya no le corresponden. Por defecto, 15000.

Un plazo que no sea un entero positivo impide arrancar.
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
DEFAULT_GROUP_SESSION_TIMEOUT_MS = 10_000
DEFAULT_GROUP_HEARTBEAT_INTERVAL_MS = 3_000
DEFAULT_GROUP_REBALANCE_TIMEOUT_MS = 15_000


@dataclass(frozen=True)
class Config:
    data_dir: Path
    fsync_per_batch: bool = False
    # En segundos, la unidad de ray.wait y ray.get.
    replication_timeout: float = DEFAULT_REPLICATION_TIMEOUT_MS / 1000
    group_session_timeout: float = DEFAULT_GROUP_SESSION_TIMEOUT_MS / 1000
    group_heartbeat_interval: float = DEFAULT_GROUP_HEARTBEAT_INTERVAL_MS / 1000
    group_rebalance_timeout: float = DEFAULT_GROUP_REBALANCE_TIMEOUT_MS / 1000


def load() -> Config:
    """Lee la configuración de las variables de entorno."""
    data_dir = Path(os.environ.get("BITACORA_DATA_DIR", DEFAULT_DATA_DIR))
    fsync_per_batch = os.environ.get("BITACORA_FSYNC", "0") == "1"
    return Config(
        data_dir=data_dir,
        fsync_per_batch=fsync_per_batch,
        replication_timeout=_positive_ms("BITACORA_REPLICATION_TIMEOUT_MS", DEFAULT_REPLICATION_TIMEOUT_MS) / 1000,
        group_session_timeout=_positive_ms(
            "BITACORA_GROUP_SESSION_TIMEOUT_MS", DEFAULT_GROUP_SESSION_TIMEOUT_MS) / 1000,
        group_heartbeat_interval=_positive_ms(
            "BITACORA_GROUP_HEARTBEAT_INTERVAL_MS", DEFAULT_GROUP_HEARTBEAT_INTERVAL_MS) / 1000,
        group_rebalance_timeout=_positive_ms(
            "BITACORA_GROUP_REBALANCE_TIMEOUT_MS", DEFAULT_GROUP_REBALANCE_TIMEOUT_MS) / 1000,
    )


def _positive_ms(variable: str, default: int) -> int:
    """Lee un plazo en milisegundos y rechaza los valores sin sentido.

    Con 0, por ejemplo, el líder sacaría del ISR a toda seguidora en cada lote,
    o el coordinador expulsaría a todos los miembros. Con un texto no numérico
    no hay valor razonable que suponer. En ambos casos es mejor no arrancar que
    funcionar con un plazo distinto del pedido.
    """
    raw = os.environ.get(variable, str(default))
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{variable} debe ser un entero: {raw!r}") from None
    if value <= 0:
        raise ValueError(f"{variable} debe ser positivo: {value}")
    return value
