"""Controller: crea las réplicas de cada tema, asigna los papeles y guarda líder e ISR.

Reparto en anillo entre tres nodos: la copia r de la partición p va al nodo
((p + r) mod 3) + 1. Cada partición empieza en un nodo distinto, de modo que
las copias de una partición quedan en nodos distintos y los líderes iniciales
(la copia 0) se reparten entre los nodos.

El Controller es la única autoridad sobre quién es líder y qué réplicas forman
el ISR de cada partición:

- La líder le propone cada cambio del ISR (`alter_isr`). Solo se acepta si lo
  pide la líder actual, y se guarda antes de responder.

Los metadatos se guardan en `controller.json`, de forma atómica, para que el
Controller los recupere si su proceso se reinicia. Cada cambio de ISR se
añade, como una línea JSON, a `events.ndjson`.
"""

import json
import os
from dataclasses import dataclass, replace
from datetime import datetime, timezone

import ray

from prototype.actors.partition_replica import PartitionReplicaActor, replica_actor_name
from prototype.config import NAMESPACE, Config
from prototype.errors import (
    InvalidReplicationFactor,
    TopicAlreadyExists,
    UnknownTopicOrPartition,
)

CONTROLLER_NAME = "controller"
NODES = 3
METADATA_FILE = "controller.json"
EVENTS_FILE = "events.ndjson"

__all__ = [
    "CONTROLLER_NAME",
    "EVENTS_FILE",
    "METADATA_FILE",
    "Controller",
    "ControllerActor",
    "TopicInfo",
    "get_or_create_controller",
    "replica_actor_name",
    "ring_assignment",
]


@dataclass(frozen=True)
class TopicInfo:
    name: str
    partitions: int
    replication_factor: int
    # partición → nodos de sus réplicas, en el orden del anillo
    assignment: dict[int, list[str]]
    # partición → nodo de la líder actual
    leaders: dict[int, str]
    # partición → nodos del ISR, la líder incluida
    isr: dict[int, list[str]]

    def leader(self, partition: int) -> str:
        return self.leaders[partition]


def ring_assignment(partitions: int, replication_factor: int) -> dict[int, list[str]]:
    """Nodos de cada partición según el anillo; el primero es el líder inicial."""
    return {
        p: [f"n{((p + r) % NODES) + 1}" for r in range(replication_factor)]
        for p in range(partitions)
    }


class Controller:
    def __init__(self, config: Config, name: str = CONTROLLER_NAME):
        config.data_dir.mkdir(parents=True, exist_ok=True)
        self._config = config
        # Las líderes buscan al Controller por este nombre para proponerle cambios del ISR.
        self._name = name
        self._path = config.data_dir / METADATA_FILE
        self._events_path = config.data_dir / EVENTS_FILE
        self._topics: dict[str, TopicInfo] = self._load_metadata()

    # --- Temas ------------------------------------------------------------------

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
        handles = {
            (p, node): self._start_replica(name, p, node)
            for p, nodes in assignment.items()
            for node in nodes
        }
        info = TopicInfo(
            name=name,
            partitions=partitions,
            replication_factor=replication_factor,
            assignment=assignment,
            leaders={p: nodes[0] for p, nodes in assignment.items()},
            isr={p: list(nodes) for p, nodes in assignment.items()},
        )
        # Primero se guarda y después se asignan los papeles: si el Controller
        # cayera en medio, al volver sabría quién debe ser líder.
        self._topics[name] = info
        self._save_metadata()
        # El tema está listo cuando todas sus réplicas han arrancado y cada
        # líder conoce su papel. Si no se esperase a las seguidoras, su arranque
        # podría superar el plazo de replicación y el primer lote las sacaría
        # del ISR.
        ray.get([h.status.remote() for h in handles.values()])
        ray.get([
            handles[(p, nodes[0])].become_leader.remote(nodes[1:], nodes, self._name)
            for p, nodes in assignment.items()
        ])
        return info

    def describe_topic(self, name: str) -> TopicInfo:
        info = self._topics.get(name)
        if info is None:
            raise UnknownTopicOrPartition(name)
        return info

    def list_topics(self) -> list[str]:
        return sorted(self._topics)

    # --- ISR ----------------------------------------------------------------------

    def alter_isr(self, topic: str, partition: int, requester: str, isr: list[str]) -> bool:
        """Cambia el ISR de una partición si lo pide su líder actual.

        Devuelve False si quien lo pide no es la líder (por ejemplo, una líder
        antigua tras una promoción). El cambio se guarda antes de responder,
        para que la líder solo lo use cuando ya es durable.
        """
        info = self._topics.get(topic)
        if info is None or partition not in info.leaders:
            return False
        if info.leaders[partition] != requester or requester not in isr:
            return False
        new_isr = sorted(isr)
        old_isr = info.isr[partition]
        if new_isr != sorted(old_isr):
            self._topics[topic] = replace(info, isr={**info.isr, partition: new_isr})
            self._save_metadata()
            self._record_event(
                "isr_change", topic=topic, partition=partition, leader=requester,
                old_isr=sorted(old_isr), isr=new_isr,
            )
        return True

    # --- Réplicas ---------------------------------------------------------------

    def _start_replica(self, topic: str, partition: int, node: str):
        # detached: la réplica sigue viva cuando termina el programa que pidió
        # crear el tema. get_if_exists: si ya existe (por ejemplo, tras un
        # reinicio del Controller), se reutiliza en vez de fallar.
        return PartitionReplicaActor.options(
            name=replica_actor_name(topic, partition, node),
            namespace=NAMESPACE,
            lifetime="detached",
            get_if_exists=True,
            resources={f"node:{node}": 0.01},
        ).remote(topic, partition, node, self._config)

    # --- Eventos ------------------------------------------------------------------

    def _record_event(self, event: str, **fields) -> None:
        """Añade una línea JSON a events.ndjson con la hora UTC y los datos del evento."""
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        line = json.dumps({"ts": timestamp, "event": event, **fields}, ensure_ascii=False)
        with open(self._events_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            if self._config.fsync_per_batch:
                os.fsync(f.fileno())

    # --- Metadatos ------------------------------------------------------------

    def _load_metadata(self) -> dict[str, TopicInfo]:
        if not self._path.exists():
            return {}
        stored = json.loads(self._path.read_text(encoding="utf-8"))
        topics = {}
        for name, t in stored["topics"].items():
            # Las claves de un objeto JSON son textos; aquí vuelven a ser números.
            assignment = {int(p): nodes for p, nodes in t["assignment"].items()}
            # Un archivo sin líderes ni ISR se lee con los valores iniciales.
            leaders = {int(p): n for p, n in t.get("leaders", {}).items()} or {
                p: nodes[0] for p, nodes in assignment.items()
            }
            isr = {int(p): nodes for p, nodes in t.get("isr", {}).items()} or {
                p: list(nodes) for p, nodes in assignment.items()
            }
            topics[name] = TopicInfo(
                name=name,
                partitions=t["partitions"],
                replication_factor=t["replication_factor"],
                assignment=assignment,
                leaders=leaders,
                isr=isr,
            )
        return topics

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
                    "leaders": {str(p): node for p, node in t.leaders.items()},
                    "isr": {str(p): nodes for p, nodes in t.isr.items()},
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
    ).remote(config, name)
