"""Pruebas de la replicación líder → seguidoras: acks, ISR, HW y alineación.

Las primeras usan la clase `PartitionReplica` directamente, sin Ray, para el
lado de la seguidora. Las demás crean un tema de una partición con tres réplicas
y congelan procesos (SIGSTOP) para simular seguidoras que no responden.
"""

import json
import os
import signal
import time
from contextlib import contextmanager
from dataclasses import replace

import pytest
import ray

from prototype import batch
from prototype.actors.controller import EVENTS_FILE, get_or_create_controller, replica_actor_name
from prototype.actors.partition_replica import PartitionReplica, replica_file_name
from prototype.config import NAMESPACE
from prototype.errors import NotLeader
from prototype.tests.test_batch import make_batch

# Tres lotes de 2, 3 y 4 registros: offsets 0–1, 2–4 y 5–8.
LOTES = [make_batch([b"a", b"b"]), make_batch([b"c", b"d", b"e"]), make_batch([b"f", b"g", b"h", b"i"])]


# --- Lado de la seguidora, sin Ray -------------------------------------------------

def standalone(config, node: str) -> PartitionReplica:
    """Réplica líder sin seguidoras, para preparar un log a mano."""
    replica = PartitionReplica("pedidos", 0, node, config)
    replica.become_leader([], [node], None)
    return replica


def test_follower_stores_numbered_batches_and_skips_those_it_has(config):
    leader = standalone(config, "n1")
    for lote in LOTES:
        leader.append(lote)
    follower = PartitionReplica("pedidos", 0, "n2", config)
    stored = leader.read(0, 10_000)

    assert follower.replicate(stored[:2], 5, 5) == 5
    # Llega todo otra vez: los dos primeros ya están y son idénticos.
    assert follower.replicate(stored, 9, 9) == 9
    files = [config.data_dir / replica_file_name("pedidos", 0, n) for n in ("n1", "n2")]
    assert files[0].read_bytes() == files[1].read_bytes()


def test_follower_stops_at_a_gap_and_reports_its_leo(config):
    leader = standalone(config, "n1")
    for lote in LOTES:
        leader.append(lote)
    follower = PartitionReplica("pedidos", 0, "n2", config)
    # Le falta el lote 0–1: no guarda nada y devuelve 0 para que se le reenvíe.
    assert follower.replicate(leader.read(0, 10_000)[1:], 9, 9) == 0


def test_follower_discards_what_the_leader_does_not_have(config):
    leader = standalone(config, "n1")
    leader.append(LOTES[0])
    leader.append(LOTES[1])                        # la líder: offsets 0–4

    follower = standalone(config, "n2")
    follower.append(LOTES[0])
    follower.append(make_batch([b"x", b"y", b"z"]))   # distinto en 2–4
    follower.append(LOTES[2])                      # y uno más que la líder no tiene
    follower.become_follower()

    assert follower.replicate(leader.read(0, 10_000), 5, 5) == 5
    files = [config.data_dir / replica_file_name("pedidos", 0, n) for n in ("n1", "n2")]
    assert files[0].read_bytes() == files[1].read_bytes()
    # Tras reiniciar, el archivo recortado se relee igual.
    assert PartitionReplica("pedidos", 0, "n2", config).log_end_offset() == 5


def test_a_follower_rejects_writes_and_reads(config):
    follower = PartitionReplica("pedidos", 0, "n2", config)
    with pytest.raises(NotLeader) as caught:
        follower.append(LOTES[0])
    assert (caught.value.code, caught.value.replica) == (1, "n2")
    with pytest.raises(NotLeader):
        follower.read(0, 10_000)


# --- Con Ray: un tema de una partición con tres réplicas ------------------------

@pytest.fixture
def fast_config(config):
    return replace(config, replication_timeout=0.5)


@pytest.fixture
def cluster(fast_config, topic, detached_actors):
    """Crea el tema con una partición en n1 (líder), n2 y n3."""
    controller_name = f"controller-{topic}"
    detached_actors.append(controller_name)
    detached_actors.extend(replica_actor_name(topic, 0, n) for n in ("n1", "n2", "n3"))
    controller = get_or_create_controller(fast_config, name=controller_name)
    ray.get(controller.create_topic.remote(topic, 1, 3))
    replicas = {n: ray.get_actor(replica_actor_name(topic, 0, n), namespace=NAMESPACE) for n in ("n1", "n2", "n3")}
    return controller, replicas


def file_of(config, topic, node) -> bytes:
    return (config.data_dir / replica_file_name(topic, 0, node)).read_bytes()


def isr_of(controller, topic) -> list[str]:
    return ray.get(controller.describe_topic.remote(topic)).isr[0]


@contextmanager
def frozen(*replicas):
    """Congela los procesos de esas réplicas (SIGSTOP) y los descongela al salir."""
    pids = [ray.get(r.runtime_info.remote())["pid"] for r in replicas]
    for pid in pids:
        os.kill(pid, signal.SIGSTOP)
    try:
        yield
    finally:
        for pid in pids:
            os.kill(pid, signal.SIGCONT)


def wait_until(condition, timeout: float = 30.0) -> None:
    """Repite `condition` hasta que devuelva True; falla si no ocurre a tiempo."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("la condición no se cumplió a tiempo")
        time.sleep(0.05)


def test_acks_all_returns_when_every_replica_has_the_batch(fast_config, topic, cluster):
    controller, replicas = cluster
    leader = replicas["n1"]
    assert [ray.get(leader.append.remote(lote)) for lote in LOTES] == [0, 2, 5]

    # Al responder, las tres réplicas tienen los mismos bytes y el HW llega al final.
    assert file_of(fast_config, topic, "n1") == file_of(fast_config, topic, "n2") == file_of(fast_config, topic, "n3")
    assert ray.get(leader.status.remote()) == {"role": "leader", "leo": 9, "hw": 9}
    assert isr_of(controller, topic) == ["n1", "n2", "n3"]
    assert len(ray.get(leader.read.remote(0, 10_000))) == 3


def test_a_follower_that_does_not_answer_leaves_the_isr_and_rejoins(fast_config, topic, cluster):
    controller, replicas = cluster
    leader = replicas["n1"]
    ray.get(leader.append.remote(LOTES[0]))

    with frozen(replicas["n3"]):
        start = time.monotonic()
        assert ray.get(leader.append.remote(LOTES[1])) == 2
        elapsed = time.monotonic() - start
        # Esperó el timeout y, con permiso del Controller, sacó a n3 del ISR.
        assert elapsed >= fast_config.replication_timeout
        assert isr_of(controller, topic) == ["n1", "n2"]
        assert ray.get(leader.isr.remote()) == ["n1", "n2"]
        assert ray.get(leader.status.remote())["hw"] == 5
        assert file_of(fast_config, topic, "n2") == file_of(fast_config, topic, "n1")
        # Sin n3 en el ISR, el siguiente lote ya no espera.
        start = time.monotonic()
        assert ray.get(leader.append.remote(LOTES[2])) == 5
        assert time.monotonic() - start < fast_config.replication_timeout

    events = [json.loads(line) for line in (fast_config.data_dir / EVENTS_FILE).read_text().splitlines()]
    assert events[0]["event"] == "isr_change"
    assert (events[0]["old_isr"], events[0]["isr"]) == (["n1", "n2", "n3"], ["n1", "n2"])

    # n3 vuelve: recibe lo que le falta y, al alcanzar a la líder, vuelve al ISR.
    # Cada llamada a la líder revisa las copias pendientes.
    wait_until(lambda: ray.get(leader.read.remote(0, 10_000)) and isr_of(controller, topic) == ["n1", "n2", "n3"])
    assert file_of(fast_config, topic, "n3") == file_of(fast_config, topic, "n1")


def test_acks_1_answers_without_waiting_and_the_hw_advances_later(config, topic, detached_actors):
    # Timeout largo: las seguidoras congeladas no deben salir del ISR durante la prueba.
    slow_config = replace(config, replication_timeout=10.0)
    controller_name = f"controller-{topic}"
    detached_actors.append(controller_name)
    detached_actors.extend(replica_actor_name(topic, 0, n) for n in ("n1", "n2", "n3"))
    controller = get_or_create_controller(slow_config, name=controller_name)
    ray.get(controller.create_topic.remote(topic, 1, 3))
    leader, n2, n3 = (ray.get_actor(replica_actor_name(topic, 0, n), namespace=NAMESPACE) for n in ("n1", "n2", "n3"))

    with frozen(n2, n3):
        start = time.monotonic()
        assert ray.get(leader.append.remote(LOTES[0], "1")) == 0
        assert time.monotonic() - start < 1.0
        # Escrito en la líder, pero ninguna seguidora lo tiene aún: no es visible.
        assert ray.get(leader.status.remote()) == {"role": "leader", "leo": 2, "hw": 0}
        assert ray.get(leader.read.remote(0, 10_000)) == []

    # Las seguidoras lo copian; la líder lo sabe en una llamada posterior.
    wait_until(lambda: len(ray.get(leader.read.remote(0, 10_000))) == 1)
    assert ray.get(leader.status.remote())["hw"] == 2
    assert isr_of(controller, topic) == ["n1", "n2", "n3"]


def test_unknown_acks_value_is_rejected(topic, cluster):
    _, replicas = cluster
    with pytest.raises(ValueError):
        ray.get(replicas["n1"].append.remote(LOTES[0], "0"))
    assert ray.get(replicas["n1"].log_end_offset.remote()) == 0


def test_the_controller_rejects_isr_changes_from_a_non_leader(topic, cluster):
    controller, _ = cluster
    assert ray.get(controller.alter_isr.remote(topic, 0, "n2", ["n2", "n3"])) is False
    assert ray.get(controller.alter_isr.remote(topic, 0, "n1", ["n2", "n3"])) is False   # sin la líder
    assert isr_of(controller, topic) == ["n1", "n2", "n3"]


def test_replicated_batches_keep_their_offsets(fast_config, topic, cluster):
    _, replicas = cluster
    for lote in LOTES:
        ray.get(replicas["n1"].append.remote(lote))
    data = file_of(fast_config, topic, "n2")
    offsets, pos = [], 0
    while pos < len(data):
        end = pos + batch.LENGTH_PREFIX_SIZE + batch.read_batch_length(data, pos)
        offsets.append(batch.parse_header(data[pos:end]).base_offset)
        pos = end
    assert offsets == [0, 2, 5]
