"""Réplica de una partición: un log de lotes en memoria respaldado por un archivo.

Cada réplica es el único escritor de su archivo `{tema}-{partición}-{nodo}.log`,
que contiene los lotes uno tras otro, con su baseOffset ya asignado. Al
arrancar, la réplica relee el archivo para reconstruir el log.

Sin replicación entre réplicas, todo lo escrito es visible: HW = LEO.

La lógica está en la clase `PartitionReplica`, que no depende de Ray;
`PartitionReplicaActor` es esa misma clase como actor.
"""

import bisect
import os
from dataclasses import dataclass

import ray

from prototype import batch
from prototype.config import Config
from prototype.errors import OffsetOutOfRange, StorageFailure


@dataclass(frozen=True)
class StoredBatch:
    """Un lote del log: su primer y último offset y sus bytes tal como se guardaron."""

    base_offset: int
    last_offset: int
    data: bytes


def replica_file_name(topic: str, partition: int, node: str) -> str:
    return f"{topic}-{partition}-{node}.log"


class PartitionReplica:
    def __init__(self, topic: str, partition: int, node: str, config: Config):
        config.data_dir.mkdir(parents=True, exist_ok=True)
        self._path = config.data_dir / replica_file_name(topic, partition, node)
        self._fsync_per_batch = config.fsync_per_batch
        self._batches: list[StoredBatch] = []
        self._leo = 0          # log end offset: el primer offset libre
        self._hw = 0           # high watermark: hasta dónde es visible
        self._broken = False   # una escritura dejó el archivo en un estado que no se pudo deshacer
        self._path.touch(exist_ok=True)
        self._recover()
        # Modo «añadir»: cada write va al final del archivo.
        self._file = open(self._path, "ab")

    # --- Escritura ---------------------------------------------------------

    def append(self, record_batch: bytes) -> int:
        """Añade un lote al log y devuelve el offset de su primer registro."""
        header = batch.parse_header(record_batch)
        batch.validate(header, record_batch)
        if self._broken:
            raise StorageFailure("la réplica no puede escribir en su archivo")

        base_offset = self._leo
        stored = batch.with_base_offset(record_batch, base_offset)

        # Primero el archivo y después la memoria: lo que se lee nunca va por
        # delante de lo escrito.
        size_before = self._file.tell()
        try:
            self._file.write(stored)
            self._file.flush()
            if self._fsync_per_batch:
                os.fsync(self._file.fileno())
        except OSError:
            # La escritura falló sin matar el proceso y puede haber dejado parte
            # del lote. Si quedara en medio del archivo, la recuperación se
            # detendría ahí y descartaría los lotes posteriores, ya confirmados.
            try:
                self._file.truncate(size_before)
                self._file.seek(size_before)
            except OSError:
                self._broken = True
            raise

        last_offset = base_offset + header.record_count - 1
        self._batches.append(StoredBatch(base_offset, last_offset, stored))
        self._leo = last_offset + 1
        self._hw = self._leo
        return base_offset

    # --- Lectura ------------------------------------------------------------

    def read(self, offset: int, max_bytes: int) -> list[bytes]:
        """Devuelve lotes enteros desde el que contiene `offset`.

        El primer lote puede empezar antes de `offset`: quien lee descarta los
        registros sobrantes. Se devuelve al menos un lote aunque supere
        `max_bytes`, para que quien lee siempre avance.
        """
        if offset < 0 or offset > self._leo:
            raise OffsetOutOfRange(offset, self._leo)
        if offset >= self._hw:
            return []

        i = bisect.bisect_left(self._batches, offset, key=lambda b: b.last_offset)
        result: list[bytes] = []
        size = 0
        while i < len(self._batches) and self._batches[i].base_offset < self._hw:
            stored = self._batches[i].data
            if result and size + len(stored) > max_bytes:
                break
            result.append(stored)
            size += len(stored)
            i += 1
        return result

    # --- Estado, para diagnóstico y pruebas -------------------------------

    def log_end_offset(self) -> int:
        return self._leo

    def high_watermark(self) -> int:
        return self._hw

    def runtime_info(self) -> dict:
        """Proceso y nodo Ray en los que se ejecuta la réplica."""
        info = {"pid": os.getpid(), "node_id": None}
        if ray.is_initialized():
            info["node_id"] = ray.get_runtime_context().get_node_id()
        return info

    # --- Recuperación -------------------------------------------------------

    def _recover(self) -> None:
        """Reconstruye el log a partir del archivo y recorta lo incompleto del final.

        Acepta lotes mientras estén enteros y su baseOffset sea el siguiente
        esperado. Lo que queda detrás del último lote aceptado es un lote
        cortado por una caída a mitad de escritura: como la réplica solo
        confirma un lote después de escribirlo entero, nunca se confirmó.
        """
        data = self._path.read_bytes()
        pos = 0
        while pos < len(data):
            if len(data) - pos < batch.LENGTH_PREFIX_SIZE:
                break
            end = pos + batch.LENGTH_PREFIX_SIZE + batch.read_batch_length(data, pos)
            if end > len(data) or end - pos < batch.HEADER_SIZE:
                break
            header = batch.parse_header(data[pos:end])
            if header.magic != batch.MAGIC or header.base_offset != self._leo:
                break
            last_offset = header.base_offset + header.record_count - 1
            self._batches.append(StoredBatch(header.base_offset, last_offset, data[pos:end]))
            self._leo = last_offset + 1
            pos = end

        if pos < len(data):
            with open(self._path, "r+b") as f:
                f.truncate(pos)
        self._hw = self._leo


# El actor: una llamada cada vez, sin CPU reservada (solo espera E/S), y
# reiniciado por Ray cada vez que su proceso muere. max_task_retries=0: una
# llamada interrumpida no se repite, para no duplicar lotes.
PartitionReplicaActor = ray.remote(num_cpus=0, max_restarts=-1, max_task_retries=0)(PartitionReplica)
