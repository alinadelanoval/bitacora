"""Pruebas del Controller: reparto en anillo, actores detached y metadatos."""

import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest
import ray

from prototype.actors.controller import (
    METADATA_FILE,
    get_or_create_controller,
    replica_actor_name,
    ring_assignment,
)
from prototype.config import NAMESPACE
from prototype.errors import (
    InvalidReplicationFactor,
    InvalidRequest,
    TopicAlreadyExists,
    UnknownTopicOrPartition,
)
REPO_ROOT = Path(__file__).resolve().parents[2]


def replica_names(info) -> list[str]:
    return [replica_actor_name(info.name, p, n) for p, nodes in info.assignment.items() for n in nodes]


def replica_names_for(topic: str) -> list[str]:
    return [replica_actor_name(topic, p, n) for p, nodes in ring_assignment(3, 3).items() for n in nodes]


@pytest.fixture
def controller(config, topic, detached_actors):
    name = f"controller-{topic}"
    detached_actors.append(name)
    return get_or_create_controller(config, name=name)


def create(controller, topic, detached_actors, partitions=3, replication_factor=3):
    info = ray.get(controller.create_topic.remote(topic, partitions, replication_factor))
    detached_actors.extend(replica_names(info))
    return info


# --- Reparto en anillo (sin Ray) ---------------------------------------------------

def test_ring_puts_each_copy_on_a_different_node_and_spreads_leaders():
    assert ring_assignment(3, 3) == {0: ["n1", "n2", "n3"], 1: ["n2", "n3", "n1"], 2: ["n3", "n1", "n2"]}
    for partitions in (1, 2, 3, 6):
        for rf in (1, 2, 3):
            assignment = ring_assignment(partitions, rf)
            assert all(len(set(nodes)) == rf for nodes in assignment.values())
    leaders = [nodes[0] for nodes in ring_assignment(6, 2).values()]
    assert {n: leaders.count(n) for n in set(leaders)} == {"n1": 2, "n2": 2, "n3": 2}


# --- create_topic ------------------------------------------------------------------

def test_create_topic_starts_one_named_replica_per_copy(controller, topic, detached_actors):
    info = create(controller, topic, detached_actors)
    assert info.assignment == ring_assignment(3, 3)
    assert info.leader(1) == "n2"
    for name in replica_names(info):
        replica = ray.get_actor(name, namespace=NAMESPACE)
        assert ray.get(replica.log_end_offset.remote()) == 0


def test_create_topic_is_idempotent_and_rejects_conflicts(controller, topic, detached_actors):
    info = create(controller, topic, detached_actors)
    assert ray.get(controller.create_topic.remote(topic, 3, 3)) == info
    with pytest.raises(TopicAlreadyExists):
        ray.get(controller.create_topic.remote(topic, 6, 3))
    with pytest.raises(InvalidReplicationFactor):
        ray.get(controller.create_topic.remote(f"{topic}x", 3, 4))
    with pytest.raises(UnknownTopicOrPartition):
        ray.get(controller.describe_topic.remote(f"{topic}y"))


def test_create_topic_rejects_names_that_would_escape_the_data_dir(controller, config):
    # «a/../../../x» haría que la réplica escribiera fuera de la carpeta de datos.
    for name in ("a/../../../x", "..", "con espacio", ""):
        with pytest.raises(InvalidRequest) as caught:
            ray.get(controller.create_topic.remote(name, 1, 1))
        assert caught.value.field == "topic"
    with pytest.raises(InvalidRequest) as caught:
        ray.get(controller.create_topic.remote("valido", 0, 1))
    assert caught.value.field == "partitions"
    assert not any(config.data_dir.parent.rglob("*.log"))        # no se ha creado ninguna réplica


def test_metadata_is_written_atomically(controller, config, topic, detached_actors):
    create(controller, topic, detached_actors)
    assert (config.data_dir / METADATA_FILE).exists()
    assert not (config.data_dir / (METADATA_FILE + ".tmp")).exists()


def test_controller_remembers_topics_after_its_process_dies(controller, topic, detached_actors):
    info = create(controller, topic, detached_actors)
    ray.kill(controller, no_restart=False)                  # muere; Ray lo reinicia
    deadline = time.monotonic() + 60
    while True:
        try:
            assert ray.get(controller.describe_topic.remote(topic), timeout=5) == info
            break
        except (ray.exceptions.RayActorError, ray.exceptions.GetTimeoutError):
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


# --- Las réplicas sobreviven al programa que las creó ------------------------------

CREATOR = textwrap.dedent(
    """
    import sys
    from pathlib import Path

    import ray

    from prototype.actors.controller import get_or_create_controller, replica_actor_name
    from prototype.config import NAMESPACE, Config
    from prototype.tests.test_batch import make_batch

    address, data_dir, topic = sys.argv[1:4]
    ray.init(address=address, namespace=NAMESPACE, log_to_driver=False)
    controller = get_or_create_controller(Config(Path(data_dir)), name=f"controller-{topic}")
    info = ray.get(controller.create_topic.remote(topic, 3, 3))
    leader = ray.get_actor(replica_actor_name(topic, 0, info.leader(0)))
    print(ray.get(leader.append.remote(make_batch([b"creado", b"pagado"]))))
    """
)


def test_replicas_survive_the_program_that_created_them(ray_address, config, topic, detached_actors):
    detached_actors.append(f"controller-{topic}")
    detached_actors.extend(replica_names_for(topic))
    result = subprocess.run(
        [sys.executable, "-c", CREATOR, ray_address, str(config.data_dir), topic],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert result.stdout.strip().splitlines()[-1] == "0"
    # El programa ha terminado: sus actores detached siguen vivos y con su log.
    leader = ray.get_actor(replica_actor_name(topic, 0, "n1"), namespace=NAMESPACE)
    assert ray.get(leader.log_end_offset.remote()) == 2
    assert len(ray.get(leader.read.remote(0, 10_000))) == 1
