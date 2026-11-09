"""Reparto real entre tres nodos Ray.

El resto de las pruebas usa un solo nodo Ray con las tres etiquetas, donde
todas las réplicas comparten nodo. Esta prueba arranca, en un proceso aparte,
un clúster de tres nodos Ray en la misma máquina (ray.cluster_utils.Cluster) y
comprueba que las copias de cada partición quedan en tres nodos distintos.

Va en un proceso aparte porque un proceso solo puede conectarse a un clúster
Ray. Los nodos añadidos exigen RAY_AUTH_MODE=token en todos los procesos.
"""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

THREE_NODES = textwrap.dedent(
    """
    import json
    import sys
    from pathlib import Path

    import ray
    from ray.cluster_utils import Cluster

    from prototype.actors.controller import get_or_create_controller, replica_actor_name
    from prototype.config import NAMESPACE, Config

    cluster = Cluster(initialize_head=True, head_node_args={"num_cpus": 1, "resources": {"node:n1": 1}})
    for node in ("n2", "n3"):
        cluster.add_node(num_cpus=1, resources={f"node:{node}": 1})
    ray.init(address=cluster.address, namespace=NAMESPACE, log_to_driver=False)
    try:
        controller = get_or_create_controller(Config(Path(sys.argv[1])))
        info = ray.get(controller.create_topic.remote("pedidos", 3, 3))
        ray_nodes = {
            p: [ray.get(ray.get_actor(replica_actor_name("pedidos", p, n)).runtime_info.remote())["node_id"]
                for n in nodes]
            for p, nodes in info.assignment.items()
        }
        print(json.dumps(ray_nodes))
    finally:
        ray.shutdown()
        cluster.shutdown()
    """
)


def test_copies_of_each_partition_run_on_distinct_ray_nodes(tmp_path):
    env = {k: v for k, v in os.environ.items() if k != "RAY_ADDRESS"}
    env["RAY_AUTH_MODE"] = "token"
    result = subprocess.run(
        [sys.executable, "-c", THREE_NODES, str(tmp_path / "data")],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=300,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    ray_nodes = json.loads(result.stdout.strip().splitlines()[-1])

    for nodes in ray_nodes.values():
        assert len(set(nodes)) == 3                 # tres copias, tres nodos Ray
    leaders = {nodes[0] for nodes in ray_nodes.values()}
    assert len(leaders) == 3                        # cada nodo lidera una partición
