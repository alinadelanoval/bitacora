"""Pruebas de la réplica de partición.

Las primeras usan la clase directamente, sin Ray. Las del final crean el actor
para comprobar que sobrevive a la muerte de su proceso.
"""

import errno
import os
import signal
import time

import pytest
import ray

from prototype import batch
from prototype.actors.partition_replica import PartitionReplica, PartitionReplicaActor, replica_file_name
from prototype.errors import CorruptBatch, InvalidBatch, OffsetOutOfRange, StorageFailure
from prototype.tests.test_batch import make_batch

# Tres lotes de 2, 3 y 4 registros: offsets 0–1, 2–4 y 5–8.
LOTES = [make_batch([b"a", b"b"]), make_batch([b"c", b"d", b"e"]), make_batch([b"f", b"g", b"h", b"i"])]


def new_replica(config) -> PartitionReplica:
    """Réplica líder sin seguidoras: el papel que le daría el Controller con un factor de replicación 1."""
    replica = PartitionReplica("pedidos", 0, "n1", config)
    replica.become_leader([], ["n1"], None)
    return replica


def fill(replica: PartitionReplica) -> list[int]:
    return [replica.append(lote) for lote in LOTES]


# --- Escritura -------------------------------------------------------------------

def test_offsets_are_contiguous_from_zero(config):
    replica = new_replica(config)
    assert fill(replica) == [0, 2, 5]
    assert replica.log_end_offset() == 9
    assert replica.high_watermark() == 9


def test_file_holds_exactly_the_bytes_returned_by_read(config):
    replica = new_replica(config)
    fill(replica)
    stored = replica.read(0, 10_000)
    assert [batch.parse_header(b).base_offset for b in stored] == [0, 2, 5]
    path = config.data_dir / replica_file_name("pedidos", 0, "n1")
    assert path.read_bytes() == b"".join(stored)
    # Cada lote se guarda tal como llegó, salvo el baseOffset.
    assert [b[8:] for b in stored] == [lote[8:] for lote in LOTES]


@pytest.mark.parametrize("data, error", [(LOTES[0][:40], CorruptBatch), (make_batch([b"x"], attributes=1), InvalidBatch)])
def test_invalid_batch_changes_nothing(config, data, error):
    replica = new_replica(config)
    replica.append(LOTES[0])
    with pytest.raises(error):
        replica.append(data)
    assert replica.log_end_offset() == 2
    assert replica.append(LOTES[1]) == 2


# --- Lectura ---------------------------------------------------------------------

def test_read_returns_whole_batches_from_the_one_containing_offset(config):
    replica = new_replica(config)
    fill(replica)
    sizes = [len(lote) for lote in LOTES]
    # El 3 está dentro del segundo lote (offsets 2–4): empieza en el 2.
    assert [batch.parse_header(b).base_offset for b in replica.read(3, 10_000)] == [2, 5]
    # Solo cabe uno: se para antes de superar max_bytes.
    assert len(replica.read(3, sizes[1] + sizes[2] - 1)) == 1
    # El primero se devuelve aunque no quepa.
    assert len(replica.read(3, 1)) == 1


def test_read_at_and_beyond_the_log_end(config):
    replica = new_replica(config)
    fill(replica)
    assert replica.read(9, 10_000) == []
    for offset in (10, -1):
        with pytest.raises(OffsetOutOfRange):
            replica.read(offset, 10_000)


# --- Recuperación ----------------------------------------------------------------

def test_restart_rebuilds_the_log_from_the_file(config):
    replica = new_replica(config)
    fill(replica)
    before = replica.read(0, 10_000)
    restarted = new_replica(config)
    assert restarted.log_end_offset() == 9
    assert restarted.read(0, 10_000) == before


def test_restart_truncates_a_batch_cut_in_the_middle(config):
    replica = new_replica(config)
    fill(replica)
    path = config.data_dir / replica_file_name("pedidos", 0, "n1")
    good_size = path.stat().st_size
    with open(path, "ab") as f:
        f.write(batch.with_base_offset(make_batch([b"j", b"k"]), 9)[:50])   # caída a mitad de escritura
    restarted = new_replica(config)
    assert restarted.log_end_offset() == 9
    assert path.stat().st_size == good_size
    assert restarted.append(LOTES[0]) == 9


# --- Escritura que falla sin matar el proceso -------------------------------------

class FullDisk:
    """Envuelve el archivo de la réplica: write escribe solo una parte y falla."""

    def __init__(self, real_file, written: int, truncate_fails: bool = False):
        self._real = real_file
        self._written = written
        self._truncate_fails = truncate_fails

    def write(self, data):
        self._real.write(data[: self._written])
        self._real.flush()
        raise OSError(errno.ENOSPC, "No space left on device")

    def truncate(self, size):
        if self._truncate_fails:
            raise OSError(errno.EIO, "Input/output error")
        return self._real.truncate(size)

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_failed_write_is_undone_and_later_batches_survive_a_restart(config):
    replica = new_replica(config)
    replica.append(LOTES[0])
    real_file = replica._file
    replica._file = FullDisk(real_file, written=40)
    with pytest.raises(OSError):
        replica.append(LOTES[1])
    replica._file = real_file                                   # se libera espacio
    assert replica.log_end_offset() == 2
    assert replica.append(LOTES[1]) == 2
    assert replica.append(LOTES[2]) == 5
    assert new_replica(config).log_end_offset() == 9           # nada confirmado se pierde


def test_replica_refuses_writes_when_the_failed_write_cannot_be_undone(config):
    replica = new_replica(config)
    replica.append(LOTES[0])
    replica._file = FullDisk(replica._file, written=40, truncate_fails=True)
    with pytest.raises(OSError):
        replica.append(LOTES[1])
    with pytest.raises(StorageFailure):
        replica.append(LOTES[1])
    assert replica.read(0, 10_000) == [batch.with_base_offset(LOTES[0], 0)]


# --- Como actor de Ray -------------------------------------------------------------

def wait_for_restart(actor, old_pid: int, timeout: float = 60.0) -> dict:
    """Espera hasta que el actor responda desde un proceso nuevo."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            info = ray.get(actor.runtime_info.remote(), timeout=5)
            if info["pid"] != old_pid:
                return info
        except (ray.exceptions.RayActorError, ray.exceptions.GetTimeoutError):
            pass
        time.sleep(0.2)
    raise AssertionError("el actor no se reinició a tiempo")


def test_actor_recovers_its_log_after_kill_9(config, topic, detached_actors):
    name = f"{topic}-0-n1"
    detached_actors.append(name)
    actor = PartitionReplicaActor.options(
        name=name, lifetime="detached", resources={"node:n1": 0.01}
    ).remote(topic, 0, "n1", config)
    ray.get(actor.become_leader.remote([], ["n1"], None))
    assert [ray.get(actor.append.remote(lote)) for lote in LOTES] == [0, 2, 5]
    before = ray.get(actor.read.remote(0, 10_000))

    pid = ray.get(actor.runtime_info.remote())["pid"]
    os.kill(pid, signal.SIGKILL)
    wait_for_restart(actor, pid)

    # Ray la reinicia como seguidora; el Controller volvería a hacerla líder.
    assert ray.get(actor.status.remote())["role"] == "follower"
    ray.get(actor.become_leader.remote([], ["n1"], None))
    assert ray.get(actor.log_end_offset.remote()) == 9
    assert ray.get(actor.read.remote(0, 10_000)) == before
    assert ray.get(actor.append.remote(LOTES[0])) == 9


def test_errors_reach_the_caller_with_their_class(config, topic, detached_actors):
    name = f"{topic}-0-n1"
    detached_actors.append(name)
    actor = PartitionReplicaActor.options(name=name, lifetime="detached").remote(topic, 0, "n1", config)
    ray.get(actor.become_leader.remote([], ["n1"], None))
    with pytest.raises(OffsetOutOfRange) as caught:
        ray.get(actor.read.remote(5, 100))
    assert (caught.value.code, caught.value.offset, caught.value.log_end_offset) == (4, 5, 0)
