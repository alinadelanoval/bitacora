"""Pruebas de la promoción: la líder muere y el Controller elige otra del ISR."""

import json
import os
import signal
import time
from dataclasses import replace

import pytest
import ray

from prototype import batch
from prototype.actors.controller import EVENTS_FILE, get_or_create_controller, replica_actor_name
from prototype.config import NAMESPACE
from prototype.errors import NotLeader
from prototype.tests.test_batch import make_batch

LOTES = [make_batch([b"a", b"b"]), make_batch([b"c", b"d", b"e"]), make_batch([b"f", b"g", b"h", b"i"])]


@pytest.fixture
def cluster(config, topic, detached_actors):
    """Tema de una partición en n1 (líder), n2 y n3, con un timeout corto."""
    fast_config = replace(config, replication_timeout=0.5)
    controller_name = f"controller-{topic}"
    detached_actors.append(controller_name)
    detached_actors.extend(replica_actor_name(topic, 0, n) for n in ("n1", "n2", "n3"))
    controller = get_or_create_controller(fast_config, name=controller_name)
    ray.get(controller.create_topic.remote(topic, 1, 3))
    return fast_config, controller


def replica(topic, node):
    return ray.get_actor(replica_actor_name(topic, 0, node), namespace=NAMESPACE)


def append_with_failover(controller, topic, record_batch, timeout: float = 60.0):
    """Lo que hará quien escribe: si la líder falla, avisa al Controller y reintenta en la vigente."""
    deadline = time.monotonic() + timeout
    while True:
        leader_node = ray.get(controller.describe_topic.remote(topic)).leader(0)
        try:
            return leader_node, ray.get(replica(topic, leader_node).append.remote(record_batch), timeout=5)
        except (ray.exceptions.RayActorError, ray.exceptions.GetTimeoutError, NotLeader):
            if time.monotonic() > deadline:
                raise
            ray.get(controller.report_leader_unavailable.remote(topic, 0))


def test_the_controller_promotes_an_isr_replica_when_the_leader_dies(topic, cluster):
    config, controller = cluster
    for lote in LOTES[:2]:
        ray.get(replica(topic, "n1").append.remote(lote))      # acks=all: confirmados, offsets 0–4

    pid = ray.get(replica(topic, "n1").runtime_info.remote())["pid"]
    os.kill(pid, signal.SIGKILL)

    leader_node, base_offset = append_with_failover(controller, topic, LOTES[2])
    assert leader_node != "n1"
    assert base_offset == 5                                     # nada confirmado se ha perdido

    stored = ray.get(replica(topic, leader_node).read.remote(0, 10_000))
    assert [batch.parse_header(b).base_offset for b in stored] == [0, 2, 5]

    events = [json.loads(line) for line in (config.data_dir / EVENTS_FILE).read_text().splitlines()]
    changes = [e for e in events if e["event"] == "leader_change"]
    assert len(changes) == 1
    assert (changes[0]["old_leader"], changes[0]["leader"]) == ("n1", leader_node)
    assert leader_node in changes[0]["isr"]

    # La líder anterior ya no puede cambiar el ISR.
    assert ray.get(controller.alter_isr.remote(topic, 0, "n1", ["n1"])) is False


def test_a_report_about_a_live_leader_changes_nothing(topic, cluster):
    config, controller = cluster
    assert ray.get(controller.report_leader_unavailable.remote(topic, 0)) == "n1"
    assert not (config.data_dir / EVENTS_FILE).exists()


def test_the_new_leader_is_remembered_after_the_controller_restarts(topic, cluster):
    _, controller = cluster
    ray.kill(replica(topic, "n1"), no_restart=True)            # muere y no vuelve
    assert ray.get(controller.report_leader_unavailable.remote(topic, 0)) == "n2"

    ray.kill(controller, no_restart=False)                      # el Controller muere; Ray lo reinicia
    deadline = time.monotonic() + 60
    while True:
        try:
            info = ray.get(controller.describe_topic.remote(topic), timeout=5)
            break
        except (ray.exceptions.RayActorError, ray.exceptions.GetTimeoutError):
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)
    assert info.leader(0) == "n2"
    assert info.isr[0] == ["n2", "n3"]
