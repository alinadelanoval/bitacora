"""Controller: crea las réplicas de cada tema y recuerda dónde está cada una.

Reparto en anillo entre tres nodos: la copia r de la partición p va al nodo
((p + r) mod 3) + 1. Cada partición empieza en un nodo distinto, de modo que
las copias de una partición quedan en nodos distintos y los líderes (la copia 0)
se reparten entre los nodos.

Los metadatos se guardan en `controller.json` y se escriben de forma atómica,
para que el Controller los recupere si su proceso se reinicia.
"""

import json
import os
from dataclasses import dataclass

import ray

from prototype.actors.partition_replica import PartitionReplicaActor
from prototype.config import NAMESPACE, Config
from prototype.errors import InvalidReplicationFactor, TopicAlreadyExists, UnknownTopicOrPartition

CONTROLLER_NAME = "controller"
NODES = 3
METADATA_FILE = "controller.json"


@dataclass(frozen=True)
class TopicInfo:
    name: str
    partitions: int
    replication_factor: int
    # partición → nodos de sus réplicas, con el líder primero
    assignment: dict[int, list[str]]

    def leader(self, partition: int) -> str:
        return self.assignment[partition][0]


def replica_actor_name(topic: str, partition: int, node: str) -> str:
    return f"{topic}-{partition}-{node}"


def ring_assignment(partitions: int, replication_factor: int) -> dict[int, list[str]]:
    """Nodos de cada partición según el anillo, con el líder primero."""
    return {
        p: [f"n{((p + r) % NODES) + 1}" for r in range(replication_factor)]
        for p in range(partitions)
    }


class Controller:
    def __init__(self, config: Config):
        config.data_dir.mkdir(parents=True, exist_ok=True)
        self._config = config
        self._path = config.data_dir / METADATA_FILE
        self._topics: dict[str, TopicInfo] = self._load_metadata()

    def create_topic(self, name: str, partitions: int, replication_factor: int) -> TopicInfo:
        """Crea el tema y sus réplicas; si ya existe con los mismos parámetros, lo devuelve."""
        existing = self._topics.get(name)
        if existing is not None:
            if (existing.partitions, existing.replication_factor) == (partitions, replication_factor):
                return existing
            raise TopicAlreadyExists(name)
        if not 1 <= replication_factor <= NODES:
            raise InvalidReplicationFactor(replication_factor)

        assignment = ring_assignment(partitions, replication_factor)
        for p, nodes in assignment.items():
            for node in nodes:
                self._start_replica(name, p, node)

        info = TopicInfo(name, partitions, replication_factor, assignment)
        self._topics[name] = info
        self._save_metadata()
        return info

    def describe_topic(self, name: str) -> TopicInfo:
        info = self._topics.get(name)
        if info is None:
            raise UnknownTopicOrPartition(name)
        return info

    def list_topics(self) -> list[str]:
        return sorted(self._topics)

    # --- Réplicas ---------------------------------------------------------------

    def _start_replica(self, topic: str, partition: int, node: str) -> None:
        # detached: la réplica sigue viva cuando termina el programa que pidió
        # crear el tema. get_if_exists: si ya existe (por ejemplo, tras un
        # reinicio del Controller), se reutiliza en vez de fallar.
        PartitionReplicaActor.options(
            name=replica_actor_name(topic, partition, node),
            namespace=NAMESPACE,
            lifetime="detached",
            get_if_exists=True,
            resources={f"node:{node}": 0.01},
        ).remote(topic, partition, node, self._config)

    # --- Metadatos ------------------------------------------------------------

    def _load_metadata(self) -> dict[str, TopicInfo]:
        if not self._path.exists():
            return {}
        stored = json.loads(self._path.read_text(encoding="utf-8"))
        return {
            name: TopicInfo(
                name=name,
                partitions=t["partitions"],
                replication_factor=t["replication_factor"],
                # Las claves de un objeto JSON son textos; aquí vuelven a ser números.
                assignment={int(p): nodes for p, nodes in t["assignment"].items()},
            )
            for name, t in stored["topics"].items()
        }

    def _save_metadata(self) -> None:
        """Escribe controller.json de forma atómica.

        Se escribe un archivo temporal entero y después se renombra sobre el
        bueno: quien lea ve el contenido anterior o el nuevo, nunca uno a medias.
        """
        content = {
            "topics": {
                t.name: {
                    "partitions": t.partitions,
                    "replication_factor": t.replication_factor,
                    "assignment": {str(p): nodes for p, nodes in t.assignment.items()},
                }
                for t in self._topics.values()
            }
        }
        tmp = self._path.with_name(self._path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._path)
        if self._config.fsync_per_batch:
            # Tras un corte de luz, el cambio de nombre solo es durable si también
            # se hace fsync de la carpeta.
            dir_fd = os.open(self._path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)


ControllerActor = ray.remote(num_cpus=0, max_restarts=-1, max_task_retries=0)(Controller)


def get_or_create_controller(config: Config, name: str = CONTROLLER_NAME):
    """Devuelve el Controller del clúster, creándolo en el nodo n1 si no existe."""
    return ControllerActor.options(
        name=name,
        namespace=NAMESPACE,
        lifetime="detached",
        get_if_exists=True,
        resources={"node:n1": 0.01},
    ).remote(config)
