"""Replicación y grupos sobre un clúster de tres nodos Ray.

El resto de las pruebas usa un solo nodo Ray con las tres etiquetas. Aquí, en
un proceso aparte, se arranca un clúster de tres nodos Ray en la misma máquina
(ray.cluster_utils.Cluster) y se repiten sobre él los comportamientos de la
replicación y de los grupos:

- acks=all responde cuando todas las réplicas vivas tienen el lote;
- una réplica que no responde sale del ISR tras el timeout y vuelve después;
- si la líder muere, el Controller promueve otra réplica del ISR;
- N consumidores reciben un reparto disjunto y completo, y la época del grupo
  aumenta con cada cambio;
- un commit con una época antigua devuelve ILLEGAL_GENERATION;
- un consumidor reiniciado reanuda desde su último offset confirmado, también
  después de que el coordinador muera con kill -9.

Un proceso solo puede conectarse a un clúster Ray, y los nodos añadidos exigen
RAY_AUTH_MODE=token en todos los procesos. El límite es el de un solo host:
los nodos son procesos distintos, pero comparten disco, reloj y red local.
"""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

SCENARIO = textwrap.dedent(
    """
    import json, os, signal, sys, time
    from dataclasses import replace
    from pathlib import Path

    import ray
    from ray.cluster_utils import Cluster

    from prototype.actors.controller import EVENTS_FILE, get_or_create_controller, replica_actor_name
    from prototype.actors.group_coordinator import SHARDS, get_group_coordinator
    from prototype.config import NAMESPACE, Config
    from prototype.errors import IllegalGeneration
    from prototype.tests.group_member import GroupMember
    from prototype.tests.test_group_coordinator import group_in_shard
    from prototype.tests.test_batch import make_batch
    from prototype.tests.test_promotion import append_with_failover

    def wait_until(condition, timeout=60.0):
        deadline = time.monotonic() + timeout
        while not condition():
            assert time.monotonic() < deadline, "la condición no se cumplió a tiempo"
            time.sleep(0.05)

    def call(handle, fn):
        return ray.get(handle.__ray_call__.remote(fn))

    cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 1, "resources": {"node:n1": 1}})
    for node in ("n2", "n3"):
        cluster.add_node(num_cpus=1, resources={f"node:{node}": 1})
    ray.init(address=cluster.address, namespace=NAMESPACE, log_to_driver=False)
    out = {}
    try:
        config = replace(Config(Path(sys.argv[1])), replication_timeout=0.5)
        controller = get_or_create_controller(config)
        node_of = lambda self: ray.get_runtime_context().get_node_id()

        # --- Replicación -----------------------------------------------------
        ray.get(controller.create_topic.remote("pedidos", 1, 3))
        replica = {n: ray.get_actor(replica_actor_name("pedidos", 0, n)) for n in ("n1", "n2", "n3")}
        out["replica_nodes"] = [call(replica[n], node_of) for n in ("n1", "n2", "n3")]

        for values in ([b"a", b"b"], [b"c", b"d", b"e"]):
            ray.get(replica["n1"].append.remote(make_batch(values)))
        out["leo_after_acks_all"] = [ray.get(replica[n].log_end_offset.remote()) for n in ("n1", "n2", "n3")]

        pid = ray.get(replica["n3"].runtime_info.remote())["pid"]
        os.kill(pid, signal.SIGSTOP)                       # n3 deja de responder
        try:
            out["offset_with_n3_frozen"] = ray.get(replica["n1"].append.remote(make_batch([b"f", b"g"])))
            out["isr_with_n3_frozen"] = ray.get(controller.describe_topic.remote("pedidos")).isr[0]
        finally:
            os.kill(pid, signal.SIGCONT)
        wait_until(lambda: ray.get(replica["n1"].read.remote(0, 10_000)) is not None
                   and ray.get(controller.describe_topic.remote("pedidos")).isr[0] == ["n1", "n2", "n3"])
        out["isr_after_n3_returns"] = ray.get(controller.describe_topic.remote("pedidos")).isr[0]

        os.kill(ray.get(replica["n1"].runtime_info.remote())["pid"], signal.SIGKILL)   # muere la líder
        out["new_leader"], out["offset_after_failover"] = append_with_failover(controller, "pedidos", make_batch([b"h"]))
        events = [json.loads(l) for l in (config.data_dir / EVENTS_FILE).read_text().splitlines()]
        out["leader_changes"] = [(e["old_leader"], e["leader"]) for e in events if e["event"] == "leader_change"]

        # --- Grupos ----------------------------------------------------------
        ray.get(controller.create_topic.remote("pagos", 3, 1))
        group = "facturacion"
        k = get_group_coordinator(config, group)
        # Un coordinador por fragmento, cada uno en su nodo.
        out["coordinator_nodes"] = [call(get_group_coordinator(config, group_in_shard(s)), node_of)
                                    for s in range(SHARDS)]
        members = [GroupMember(k, group, f"c{i}", ["pagos"]) for i in (1, 2, 3)]
        out["group_epoch_after_each_join"] = [m.enter()["group_epoch"] for m in members]
        for _ in range(6):
            for m in members:
                m.beat()
        out["assignments"] = {m.member_id: sorted(m.owned) for m in members}

        c1 = members[0]
        tp = sorted(c1.owned)[0]
        c1.process(tp, 42)
        c1.commit([tp])
        try:
            ray.get(k.commit.remote(group, "c1", c1.epoch - 1, [(tp[0], tp[1], 50)]))
            out["old_epoch_commit"] = "accepted"
        except IllegalGeneration as error:
            out["old_epoch_commit"] = error.code

        os.kill(call(k, lambda self: os.getpid()), signal.SIGKILL)   # muere el coordinador
        def alive():
            try:
                return ray.get(k.describe.remote(group), timeout=5) is not None
            except (ray.exceptions.RayActorError, ray.exceptions.GetTimeoutError):
                return False
        wait_until(alive)
        restarted = GroupMember(k, group, "c1", ["pagos"])            # el consumidor se reinicia
        restarted.enter()
        out["resume_offset"] = restarted.start_of(tp)
        out["events"] = sorted({json.loads(l)["event"]
                                for f in config.data_dir.glob("group-events-*.ndjson")
                                for l in f.read_text().splitlines()})
        print("RESULT " + json.dumps(out), flush=True)
    finally:
        ray.shutdown()
        cluster.shutdown()
    """
)


def test_replication_and_groups_on_three_ray_nodes(tmp_path):
    env = {k: v for k, v in os.environ.items() if k != "RAY_ADDRESS"}
    env["RAY_AUTH_MODE"] = "token"
    result = subprocess.run(
        [sys.executable, "-c", SCENARIO, str(tmp_path / "data")],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=600,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    # Ray también escribe avisos en la salida estándar: el resultado va en su propia línea.
    line = next(l for l in result.stdout.splitlines() if l.startswith("RESULT "))
    out = json.loads(line.removeprefix("RESULT "))

    # Replicación
    assert len(set(out["replica_nodes"])) == 3                   # tres copias, tres nodos Ray
    assert out["leo_after_acks_all"] == [5, 5, 5]                # acks=all: todas lo tienen
    assert out["offset_with_n3_frozen"] == 5                     # responde tras el timeout…
    assert out["isr_with_n3_frozen"] == ["n1", "n2"]             # …sin la réplica que no contesta
    assert out["isr_after_n3_returns"] == ["n1", "n2", "n3"]
    assert out["new_leader"] in ("n2", "n3")
    assert out["offset_after_failover"] == 7                     # nada confirmado se perdió
    assert out["leader_changes"] == [["n1", out["new_leader"]]]

    # Grupos
    assert out["coordinator_nodes"] == [out["replica_nodes"][s] for s in range(3)]   # fragmento s en n(s+1)
    assert out["group_epoch_after_each_join"] == [1, 2, 3]       # la época sube en cada cambio
    owned = [tuple(tp) for tps in out["assignments"].values() for tp in tps]
    assert sorted(owned) == [("pagos", p) for p in range(3)]     # disjunto y completo
    assert out["old_epoch_commit"] == 7                          # ILLEGAL_GENERATION
    assert out["resume_offset"] == 42                            # reanuda desde lo confirmado
    assert {"coordinator_started", "member_joined", "member_reconciled"} <= set(out["events"])
