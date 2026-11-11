"""Pruebas del coordinador de grupos.

Casi todas usan la clase sin Ray, con un reloj simulado, para que los plazos
sean deterministas. Las del final usan los actores de verdad: el despertador y
la recuperación tras kill -9.
"""

import json
import os
import signal
import time
from dataclasses import replace

import pytest
import ray

from prototype.actors.controller import get_or_create_controller, replica_actor_name
from prototype.actors.group_coordinator import (
    SHARDS,
    GroupCoordinator,
    coordinator_name,
    get_group_coordinator,
    group_file_name,
    shard_for,
)
from prototype.config import NAMESPACE
from prototype.errors import IllegalGeneration, InvalidRequest, UnknownMember
from prototype.tests.group_member import GroupMember

PARTITIONS = {"pedidos": 3, "pagos": 2}
P0, P1, P2 = ("pedidos", 0), ("pedidos", 1), ("pedidos", 2)


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def group_in_shard(shard: int, base: str = "facturacion") -> str:
    """Un nombre de grupo que cae en el fragmento pedido."""
    i = 0
    while shard_for(f"{base}{i}") != shard:
        i += 1
    return f"{base}{i}"


GROUP = group_in_shard(0)


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def coordinator(config, clock):
    return GroupCoordinator(0, config, partition_counts=PARTITIONS, clock=clock)


def assert_one_owner(coordinator, group_id=GROUP):
    """Invariante de la reconciliación: ninguna partición en la asignación de dos miembros."""
    owners = [tuple(tp) for m in coordinator.describe(group_id)["members"].values() for tp in m["current"]]
    assert len(owners) == len(set(owners)), f"partición con dos dueños: {owners}"


def converge(coordinator, members, rounds=6):
    """Hace latir a todos varias veces, comprobando el invariante tras cada latido."""
    for _ in range(rounds):
        for m in members:
            m.beat()
            assert_one_owner(coordinator)


# --- Entrar y converger: reparto disjunto y completo ---------------------------------------------

def test_a_single_member_receives_every_partition(coordinator):
    c1 = GroupMember(coordinator, GROUP, "c1", ["pedidos"])
    response = c1.enter()
    assert response["assignment"] == [P0, P1, P2]
    assert (response["member_epoch"], response["group_epoch"]) == (1, 1)
    assert response["heartbeat_interval_ms"] == 3000


def test_a_partition_changes_owner_only_after_it_is_released(coordinator):
    c1 = GroupMember(coordinator, GROUP, "c1", ["pedidos"])
    c2 = GroupMember(coordinator, GROUP, "c2", ["pedidos"])
    c1.enter()
    r = c2.enter()                                    # p2 le toca, pero aún es de c1
    assert (r["assignment"], r["pending"], r["group_epoch"]) == ([], [P2], 2)
    assert_one_owner(coordinator)

    r = c1.beat()                                     # c1: deja p2 (sigue en su época)
    assert (r["assignment"], r["member_epoch"]) == ([P0, P1], 1)
    assert coordinator.describe(GROUP)["members"]["c1"]["revoking"] == [P2]
    assert c2.beat()["assignment"] == []              # p2 sigue sin estar libre
    r = c1.beat()                                     # c1 dice que ya no la tiene: llega a la época 2
    assert (r["assignment"], r["member_epoch"]) == ([P0, P1], 2)
    assert c2.beat()["assignment"] == [P2]
    assert_one_owner(coordinator)


@pytest.mark.parametrize("count", [2, 3, 5])
def test_members_converge_to_a_disjoint_and_complete_assignment(coordinator, count):
    members = [GroupMember(coordinator, GROUP, f"c{i}", ["pedidos", "pagos"]) for i in range(count)]
    epochs = []
    for m in members:
        epochs.append(m.enter()["group_epoch"])
        converge(coordinator, members[: len(epochs)])
    assert epochs == list(range(1, count + 1))        # la época del grupo sube en cada entrada
    current = coordinator.describe(GROUP)["members"]
    owned = [tuple(tp) for m in current.values() for tp in m["current"]]
    assert sorted(owned) == sorted({(t, p) for t, n in PARTITIONS.items() for p in range(n)})
    assert {m.epoch for m in members} == {count}      # todos han llegado a la época del grupo


# --- Commits --------------------------------------------------------

def test_commit_with_an_old_epoch_is_illegal_generation(coordinator):
    c1 = GroupMember(coordinator, GROUP, "c1", ["pedidos"])
    c1.enter()
    old_epoch = c1.epoch
    GroupMember(coordinator, GROUP, "c2", ["pedidos"]).enter()
    c1.beat()
    c1.beat()                                         # c1 ya está en la época 2
    with pytest.raises(IllegalGeneration) as caught:
        coordinator.commit(GROUP, "c1", old_epoch, [("pedidos", 0, 5)])
    assert caught.value.code == 7


def test_commit_of_a_partition_being_released_is_accepted_and_continues_elsewhere(coordinator):
    c1 = GroupMember(coordinator, GROUP, "c1", ["pedidos"])
    c2 = GroupMember(coordinator, GROUP, "c2", ["pedidos"])
    c1.enter()
    c1.process(P2, 54)                                # c1 procesa p2 hasta el 53
    c2.enter()
    c1.beat()                                         # se le pide dejar p2: la confirma antes
    assert coordinator.fetch(GROUP, [P2]) == {P2: 54}
    c1.beat()
    c2.beat()
    assert P2 in c2.owned and c2.start_of(P2) == 54   # el nuevo dueño sigue donde lo dejó c1
    with pytest.raises(IllegalGeneration):            # p2 ya no es de c1
        coordinator.commit(GROUP, "c1", c1.epoch, [("pedidos", 2, 60)])


def test_committed_offsets_never_go_back(coordinator):
    c1 = GroupMember(coordinator, GROUP, "c1", ["pedidos"])
    c1.enter()
    coordinator.commit(GROUP, "c1", c1.epoch, [("pedidos", 0, 50)])
    coordinator.commit(GROUP, "c1", c1.epoch, [("pedidos", 0, 30)])    # llega tarde
    assert coordinator.fetch(GROUP, [P0, P1]) == {P0: 50}             # p1 sin commit: no aparece


# --- Reanudar desde lo confirmado --------------------------------------------------------

def test_a_restarted_consumer_resumes_from_its_last_committed_offset(coordinator):
    c1 = GroupMember(coordinator, GROUP, "c1", ["pedidos"])
    c1.enter()
    c1.process(P0, 42)
    c1.commit_all()
    restarted = GroupMember(coordinator, GROUP, "c1", ["pedidos"])    # mismo member_id
    restarted.enter()
    assert restarted.start_of(P0) == 42


# --- Plazos -------------------------------------------------------------------------

def test_a_silent_member_is_expelled_and_its_partitions_reassigned(coordinator, clock):
    c1 = GroupMember(coordinator, GROUP, "c1", ["pedidos"])
    c2 = GroupMember(coordinator, GROUP, "c2", ["pedidos"])
    c1.enter(); c2.enter(); converge(coordinator, [c1, c2])
    epoch = coordinator.describe(GROUP)["group_epoch"]
    for _ in range(4):                                # c2 se calla; c1 late cada 3 s
        clock.advance(3)
        c1.beat()
    assert set(coordinator.describe(GROUP)["members"]) == {"c1"}
    assert coordinator.describe(GROUP)["group_epoch"] == epoch + 1
    converge(coordinator, [c1])
    assert c1.owned == {P0, P1, P2}
    with pytest.raises(UnknownMember):                # el zombi vuelve
        c2.beat()
    with pytest.raises(UnknownMember):
        coordinator.commit(GROUP, "c2", c2.epoch, [("pedidos", 2, 9)])


def test_expire_works_without_any_other_call(coordinator, clock):
    GroupMember(coordinator, GROUP, "c1", ["pedidos"]).enter()
    clock.advance(11)
    coordinator.expire()                              # lo que hará el despertador
    assert coordinator.describe(GROUP)["members"] == {}


def test_a_member_that_never_releases_is_expelled_after_the_rebalance_timeout(coordinator, clock):
    c1 = GroupMember(coordinator, GROUP, "c1", ["pedidos"])
    c2 = GroupMember(coordinator, GROUP, "c2", ["pedidos"])
    c1.enter(); c2.enter()
    # Se le pide dejar p2 en su primer latido (t = 3 s); el plazo es de 15 s.
    for _ in range(6):                                # hasta t = 18 s: c1 late pero nunca deja p2
        clock.advance(3)
        coordinator.heartbeat(GROUP, "c1", c1.epoch, [], [P0, P1, P2])
        c2.beat()
    clock.advance(3)                                  # t = 21 s: el plazo ha vencido
    with pytest.raises(UnknownMember):                # se le expulsa al empezar su propio latido
        coordinator.heartbeat(GROUP, "c1", c1.epoch, [], [P0, P1, P2])
    assert set(coordinator.describe(GROUP)["members"]) == {"c2"}
    converge(coordinator, [c2])
    assert c2.owned == {P0, P1, P2}


# --- Épocas obsoletas y salida --------------------------------------------------------

def test_a_stale_epoch_must_rejoin_with_zero(coordinator):
    c1 = GroupMember(coordinator, GROUP, "c1", ["pedidos"])
    c1.enter()
    with pytest.raises(IllegalGeneration):
        coordinator.heartbeat(GROUP, "c1", c1.epoch + 5, [], [])
    c1.enter()                                        # vuelve con época 0 y el mismo id
    assert len(coordinator.describe(GROUP)["members"]) == 1
    assert c1.owned == {P0, P1, P2}


def test_leaving_is_idempotent_and_frees_the_partitions(coordinator):
    c1 = GroupMember(coordinator, GROUP, "c1", ["pedidos"])
    c2 = GroupMember(coordinator, GROUP, "c2", ["pedidos"])
    c1.enter(); c2.enter(); converge(coordinator, [c1, c2])
    assert c2.leave()["member_epoch"] == -1
    assert coordinator.heartbeat(GROUP, "c2", -1, [], [])["member_epoch"] == -1
    converge(coordinator, [c1])
    assert c1.owned == {P0, P1, P2}


# --- Validación ------------------------------------------------------------------------

@pytest.mark.parametrize("group_id, member_id, epoch, topics, field", [
    ("a/../../secreto", "c1", 0, ["pedidos"], "group_id"),
    (GROUP, "", 0, ["pedidos"], "member_id"),
    (GROUP, "c1", -2, ["pedidos"], "member_epoch"),
    (GROUP, "c1", 0, [], "topics"),
    (GROUP, "c1", 0, ["a/b"], "topic"),
    (group_in_shard(1), "c1", 0, ["pedidos"], "group_id"),   # otro fragmento
])
def test_invalid_requests(coordinator, group_id, member_id, epoch, topics, field):
    with pytest.raises(InvalidRequest) as caught:
        coordinator.heartbeat(group_id, member_id, epoch, topics, [])
    assert caught.value.field == field


# --- Disco y reinicio ----------------------------------------------------------------------

def test_state_survives_a_restart_and_sessions_start_again(config, clock, coordinator):
    c1 = GroupMember(coordinator, GROUP, "c1", ["pedidos"])
    c2 = GroupMember(coordinator, GROUP, "c2", ["pedidos"])
    c1.enter(); c2.enter(); c1.beat()                 # c1 está dejando p2
    c1.process(P0, 7); c1.commit([P0])
    before = coordinator.describe(GROUP)

    clock.advance(9)                                  # casi una sesión entera
    restarted = GroupCoordinator(0, config, partition_counts=PARTITIONS, clock=clock)
    assert restarted.describe(GROUP) == before        # época, miembros, asignaciones y offsets
    clock.advance(3)
    restarted.expire()                                # la sesión empezó de nuevo al reiniciar
    assert set(restarted.describe(GROUP)["members"]) == {"c1", "c2"}

    stored = json.loads((config.data_dir / group_file_name(GROUP)).read_text())
    assert stored["group_id"] == GROUP and stored["offsets"] == [["pedidos", 0, 7]]


def test_each_shard_loads_only_its_groups(config, clock):
    other = group_in_shard(1)
    GroupCoordinator(0, config, partition_counts=PARTITIONS, clock=clock).heartbeat(GROUP, "c1", 0, ["pedidos"], [])
    GroupCoordinator(1, config, partition_counts=PARTITIONS, clock=clock).heartbeat(other, "c1", 0, ["pedidos"], [])
    assert set(GroupCoordinator(0, config, partition_counts=PARTITIONS, clock=clock).groups) == {GROUP}
    assert set(GroupCoordinator(1, config, partition_counts=PARTITIONS, clock=clock).groups) == {other}


# --- Con Ray: actores, Controller, despertador y kill -9 ------------------------------------

@pytest.fixture
def cluster(config, topic, detached_actors):
    """Un Controller con un tema de 3 particiones y el prefijo de coordinadores de esta prueba."""
    fast = replace(config, group_session_timeout=1.0, group_rebalance_timeout=2.0)
    controller_name = f"controller-{topic}"
    prefix = f"gc-{topic}"
    detached_actors.append(controller_name)
    detached_actors.extend(replica_actor_name(topic, 0, n) for n in ("n1", "n2", "n3"))
    detached_actors.extend(replica_actor_name(topic, p, f"n{((p) % 3) + 1}") for p in range(3))
    detached_actors.extend(coordinator_name(s, prefix) for s in range(SHARDS))
    controller = get_or_create_controller(fast, name=controller_name)
    info = ray.get(controller.create_topic.remote(topic, 3, 1))
    detached_actors.extend(replica_actor_name(topic, p, nodes[0]) for p, nodes in info.assignment.items())
    return fast, controller_name, prefix


def wait_until(condition, timeout=30.0):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("la condición no se cumplió a tiempo")
        time.sleep(0.05)


def test_actor_converges_with_partitions_from_the_controller(cluster, topic):
    config, controller_name, prefix = cluster
    k = get_group_coordinator(config, GROUP, prefix=prefix, controller_name=controller_name, tick=0.2)
    c1 = GroupMember(k, GROUP, "c1", [topic])
    c2 = GroupMember(k, GROUP, "c2", [topic])
    assert len(c1.enter()["assignment"]) == 3
    c2.enter()
    for _ in range(4):
        c1.beat(); c2.beat()
    assert sorted(c1.owned | c2.owned) == [(topic, p) for p in range(3)]
    assert not (c1.owned & c2.owned)


def test_the_alarm_expels_a_silent_member_without_any_call(cluster, topic):
    config, controller_name, prefix = cluster
    k = get_group_coordinator(config, GROUP, prefix=prefix, controller_name=controller_name, tick=0.2)
    GroupMember(k, GROUP, "c1", [topic]).enter()
    # Nadie más llama: solo el despertador puede expulsarlo (sesión de 1 s).
    wait_until(lambda: ray.get(k.describe.remote(GROUP))["members"] == {})


def test_the_coordinator_recovers_its_groups_after_kill_9(cluster, topic):
    config, controller_name, prefix = cluster
    k = get_group_coordinator(config, GROUP, prefix=prefix, controller_name=controller_name, tick=0.2)
    c1 = GroupMember(k, GROUP, "c1", [topic])
    c1.enter()
    c1.process((topic, 0), 42)
    c1.commit_all()
    epoch = ray.get(k.describe.remote(GROUP))["group_epoch"]

    pid = ray.get(k.__ray_call__.remote(lambda self: os.getpid()))
    os.kill(pid, signal.SIGKILL)
    def recovered():
        try:
            return ray.get(k.describe.remote(GROUP), timeout=5)
        except (ray.exceptions.RayActorError, ray.exceptions.GetTimeoutError):
            return None
    wait_until(lambda: recovered() is not None)
    state = recovered()
    # Con una sesión de 1 s, c1 puede haber sido expulsado mientras Ray reiniciaba
    # el actor, y entonces la época sube. Lo que nunca puede pasar es que baje.
    assert state["group_epoch"] >= epoch
    assert ray.get(k.fetch.remote(GROUP, [(topic, 0)])) == {(topic, 0): 42}
