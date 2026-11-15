"""Arranca el gateway gRPC del prototipo: `python -m prototype.gateway`.

Sin `--ray-address` arranca un nodo Ray local con los recursos `node:n1..n3`,
como las pruebas. Con `--ray-address` (o `RAY_ADDRESS`) se une a un clúster Ray
ya arrancado, por ejemplo el de tres nodos de `test_three_nodes.py`.

El proceso escribe «listening <host:puerto>» en la salida estándar cuando ya
acepta llamadas, y se detiene con SIGINT o SIGTERM.
"""

import argparse
import asyncio
import os
import signal

import ray

from prototype import config as config_module
from prototype.config import NAMESPACE
from prototype.gateway.server import Cluster, start

NODE_RESOURCES = {"node:n1": 1, "node:n2": 1, "node:n3": 1}


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--listen", default="127.0.0.1:9091", help="dirección gRPC (puerto 0 = libre)")
    parser.add_argument("--ray-address", default=os.environ.get("RAY_ADDRESS"), help="clúster Ray existente")
    args = parser.parse_args()

    if args.ray_address:
        ray.init(address=args.ray_address, namespace=NAMESPACE, log_to_driver=False)
    else:
        ray.init(num_cpus=2, resources=NODE_RESOURCES, namespace=NAMESPACE,
                 include_dashboard=False, log_to_driver=False)

    server, port = await start(Cluster(config_module.load()), args.listen)
    host = args.listen.rsplit(":", 1)[0]
    print(f"listening {host}:{port}", flush=True)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    await server.stop(grace=1)
    ray.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
