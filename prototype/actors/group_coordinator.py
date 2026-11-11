"""Coordinador de grupos de consumidores, repartido en fragmentos fijos.

Fragmentos. Hay `SHARDS` coordinadores, uno por nodo. Cada grupo lo atiende
siempre el mismo, el `crc32(group_id) % SHARDS`: cualquier proceso calcula a
cuál dirigirse sin consultar ninguna tabla. `crc32` y no `hash()`, porque el
`hash()` de Python cambia entre procesos.

Protocolo (reconciliación incremental). Entrar, latir y salir usan `heartbeat`,
según la época del miembro: 0 entra, -1 sale y un valor positivo late.

- Cada cambio de membresía incrementa la época del grupo y produce un reparto
  objetivo con `range`.
- En cada latido, el miembro envía todo lo que tiene. Si le sobra algo, la
  respuesta no lo incluye: debe dejarlo. Cuando ya no le sobra nada, su época
  avanza a la del grupo y recibe lo que le falta que nadie más tenga.
- Nunca se entrega una partición que otro miembro tiene en su asignación
  actual.
- Un commit se acepta si la época del miembro es la vigente y la partición
  está en su asignación actual, aunque la esté dejando.

Plazos. Se revisan al principio de cada llamada y, además, cuando un hilo
despertador deja `expire()` en el buzón del propio actor. Ese hilo nunca toca
el estado: todo lo que lo modifica se ejecuta en el hilo del actor, de una en
una, sin carreras.

Persistencia. Cada grupo se guarda en `group-<id>.json`, de forma atómica,
antes de responder a cualquier llamada que lo cambie. No se guardan los
instantes del reloj monotónico, que no tienen sentido en otro proceso: tras un
reinicio, las sesiones y los plazos de revocación vuelven a contar desde cero.

Observabilidad. Cada transición (entrar, salir, expulsar, pedir que se deje
algo, completar la reconciliación) se añade a `group-events-<fragmento>.ndjson`
después de guardar el grupo, de modo que el registro nunca muestra un cambio
que no llegó al disco. `metrics()` devuelve contadores en memoria, que vuelven
a cero si el proceso se reinicia.
"""

import json
import os
import threading
import time
import zlib
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import ray
from ray.exceptions import GetTimeoutError, RayActorError

from prototype.actors.controller import CONTROLLER_NAME
from prototype.config import NAMESPACE, Config
from prototype.errors import (
    BitacoraError,
    IllegalGeneration,
    InvalidRequest,
    RequestTimedOut,
    UnknownMember,
    UnknownTopicOrPartition,
)
from prototype.events import append_event
from prototype.names import validate_group_id, validate_topic_name
from prototype.range_strategy import assign

SHARDS = 3                                  # no se cambia tras desplegar
COORDINATOR_PREFIX = "group-coordinator"
TICK = 1.0                                  # cada cuánto suena el despertador (s)
LEAVE = -1
JOIN = 0

TopicPartition = tuple[str, int]


def shard_for(group_id: str) -> int:
    """Fragmento de un grupo: siempre el mismo, en cualquier proceso."""
    return zlib.crc32(group_id.encode("utf-8")) % SHARDS


def coordinator_name(shard: int, prefix: str = COORDINATOR_PREFIX) -> str:
    return f"{prefix}-{shard}"


def group_file_name(group_id: str) -> str:
    return f"group-{group_id}.json"


def events_file_name(shard: int) -> str:
    return f"group-events-{shard}.ndjson"


# --- Estado -------------------------------------------------------------------

@dataclass
class Member:
    member_id: str
    topics: list[str]
    epoch: int = 0                                   # época del miembro
    current: set = field(default_factory=set)        # lo que tiene (incluye lo que está dejando)
    revoking: set = field(default_factory=set)       # lo que debe dejar
    last_seen: float = 0.0                           # último latido (solo en memoria)
    revoke_since: float | None = None                # desde cuándo debe dejarlo (solo en memoria)


@dataclass
class Group:
    group_id: str
    epoch: int = 0                                   # época del grupo
    members: dict = field(default_factory=dict)      # member_id → Member
    offsets: dict = field(default_factory=dict)      # (tema, partición) → siguiente offset a leer
    target: dict | None = None                       # reparto objetivo; None = hay que recalcularlo

    def owned_by_other(self, tp: TopicPartition, member_id: str) -> bool:
        return any(tp in m.current for m in self.members.values() if m.member_id != member_id)


# --- El coordinador -------------------------------------------------------------

class GroupCoordinator:
    def __init__(
        self,
        shard: int,
        config: Config,
        controller_name: str = CONTROLLER_NAME,
        partition_counts: dict[str, int] | None = None,
        clock=time.monotonic,
        start_alarm: bool = False,
        tick: float = TICK,
    ):
        """
        partition_counts: particiones de cada tema, para usar la clase sin Ray en
        las pruebas. Si es None, se preguntan al Controller.
        clock: reloj monotónico; las pruebas pasan uno simulado.
        start_alarm: arrancar el hilo despertador; solo dentro de un actor.
        """
        config.data_dir.mkdir(parents=True, exist_ok=True)
        self._shard = shard
        self._config = config
        self._dir: Path = config.data_dir
        self._controller_name = controller_name
        self._fixed_partitions = partition_counts
        self._partition_cache: dict[str, int] = {}
        self._clock = clock
        self._events_path = self._dir / events_file_name(shard)
        self._pending_events: list[tuple[str, dict]] = []   # se escriben tras guardar
        self._counts: Counter = Counter()                   # latidos, commits, expulsiones
        self._rejected: Counter = Counter()                 # errores devueltos, por tipo
        self.groups: dict[str, Group] = self._load()
        append_event(self._events_path, "coordinator_started", fsync=config.fsync_per_batch,
                     shard=shard, groups=sorted(self.groups))

        if start_alarm:
            # El handle de este mismo actor: el despertador lo usa para dejar
            # «expire()» en el buzón, como haría cualquier cliente.
            me = ray.get_runtime_context().current_actor
            threading.Thread(target=self._alarm, args=(me, tick), daemon=True).start()

    def _alarm(self, me, tick: float) -> None:
        """Hilo despertador: nunca toca el estado, solo deja notas en el buzón."""
        while True:
            time.sleep(tick)
            me.expire.remote()

    # --- Operaciones del contrato -------------------------------------------------

    def heartbeat(self, group_id: str, member_id: str, member_epoch: int,
                  topics: list[str] | None = None, owned: list[TopicPartition] | None = None) -> dict:
        """Entrar (época 0), latir (época vigente) o salir (época -1)."""
        self._counts["heartbeats"] += 1
        return self._counted(self._heartbeat, group_id, member_id, member_epoch, topics, owned)

    def _heartbeat(self, group_id, member_id, member_epoch, topics, owned) -> dict:
        self.expire()
        self._check_group_id(group_id)
        if not isinstance(member_id, str) or member_id == "":
            raise InvalidRequest("member_id", "no puede estar vacío")
        if not isinstance(member_epoch, int) or member_epoch < LEAVE:
            raise InvalidRequest("member_epoch", f"debe ser -1, 0 o una época; es {member_epoch!r}")
        topics = list(topics or [])
        for topic in topics:
            validate_topic_name(topic)
        owned_set = {(t, p) for t, p in (owned or [])}

        if member_epoch == LEAVE:
            return self._leave(group_id, member_id)
        if member_epoch == JOIN:
            if not topics:
                raise InvalidRequest("topics", "al entrar hay que indicar al menos un tema")
            return self._enter(group_id, member_id, topics)
        return self._beat(group_id, member_id, member_epoch, topics, owned_set)

    def commit(self, group_id: str, member_id: str, member_epoch: int,
               offsets: list[tuple[str, int, int]]) -> None:
        """Apunta el siguiente offset a leer de cada partición; nunca retrocede."""
        self._counts["commits"] += 1
        return self._counted(self._commit, group_id, member_id, member_epoch, offsets)

    def _commit(self, group_id, member_id, member_epoch, offsets) -> None:
        self.expire()
        self._check_group_id(group_id)
        group = self.groups.get(group_id)
        member = group.members.get(member_id) if group else None
        if member is None:
            raise UnknownMember(group_id, member_id)
        if member_epoch != member.epoch:
            raise IllegalGeneration(group_id, member_id, f"época {member_epoch}, vigente {member.epoch}")
        for topic, partition, next_offset in offsets:
            if (topic, partition) not in member.current:
                raise IllegalGeneration(group_id, member_id, f"{topic}-{partition} no está asignada a este miembro")
            if next_offset < 0:
                raise InvalidRequest("next_offset", "no puede ser negativo")
        for topic, partition, next_offset in offsets:
            tp = (topic, partition)
            group.offsets[tp] = max(group.offsets.get(tp, 0), next_offset)
        self._save(group)

    def fetch(self, group_id: str, partitions: list[TopicPartition]) -> dict:
        """Offsets confirmados; las particiones sin commit no aparecen."""
        self._check_group_id(group_id)
        group = self.groups.get(group_id)
        if group is None:
            return {}
        return {(t, p): group.offsets[(t, p)] for t, p in partitions if (t, p) in group.offsets}

    def expire(self) -> None:
        """Expulsa a quien lleva una sesión sin latir o no deja a tiempo lo que debe."""
        now = self._clock()
        for group in self.groups.values():
            gone = [m for m in group.members.values()
                    if now - m.last_seen > self._config.group_session_timeout
                    or (m.revoking and now - m.revoke_since > self._config.group_rebalance_timeout)]
            for m in gone:
                del group.members[m.member_id]       # sus particiones quedan libres
            if gone:
                self._new_epoch(group)
                for m in gone:
                    reason = ("session_timeout" if now - m.last_seen > self._config.group_session_timeout
                              else "rebalance_timeout")
                    self._counts["expelled"] += 1
                    self._event("member_expelled", group, m.member_id, reason=reason,
                                partitions=sorted(m.current))
                self._save(group)

    def describe(self, group_id: str) -> dict:
        """Estado de un grupo, para el diagnóstico y las pruebas."""
        group = self.groups.get(group_id)
        if group is None:
            return {"group_epoch": 0, "members": {}, "offsets": {}}
        return {
            "group_epoch": group.epoch,
            "members": {m.member_id: {"epoch": m.epoch, "current": sorted(m.current),
                                      "revoking": sorted(m.revoking), "topics": list(m.topics)}
                        for m in group.members.values()},
            "offsets": dict(group.offsets),
        }

    def metrics(self) -> dict:
        """Contadores desde que arrancó este proceso y tamaño actual de los grupos."""
        return {
            "shard": self._shard,
            "heartbeats": self._counts["heartbeats"],
            "commits": self._counts["commits"],
            "expelled": self._counts["expelled"],
            "rejected": dict(self._rejected),
            "groups": {g.group_id: {"group_epoch": g.epoch, "members": len(g.members)}
                       for g in self.groups.values()},
        }

    def _counted(self, operation, *args):
        """Ejecuta una operación y cuenta, por tipo, los errores que devuelve."""
        try:
            return operation(*args)
        except BitacoraError as error:
            self._rejected[type(error).__name__] += 1
            raise

    # --- Entrar, latir y salir ----------------------------------------------------

    def _enter(self, group_id: str, member_id: str, topics: list[str]) -> dict:
        for topic in topics:                         # antes de cambiar nada: puede fallar
            self._partitions(topic)
        group = self.groups.setdefault(group_id, Group(group_id))
        member = group.members.get(member_id)
        if member is None:                           # miembro nuevo: cambia quién está
            member = Member(member_id, topics)
            group.members[member_id] = member
            self._new_epoch(group)
            self._event("member_joined", group, member_id, topics=topics)
        else:                                        # vuelve a entrar: ha dejado todo
            member.current.clear()
            member.revoking.clear()
            member.revoke_since = None
            if member.topics != topics:
                member.topics = topics
                self._new_epoch(group)
            self._event("member_rejoined", group, member_id, topics=topics)
        member.last_seen = self._clock()
        response = self._reconcile(group, member)
        self._save(group)
        return response

    def _beat(self, group_id: str, member_id: str, member_epoch: int,
              topics: list[str], owned: set) -> dict:
        group = self.groups.get(group_id)
        member = group.members.get(member_id) if group else None
        if member is None:
            raise UnknownMember(group_id, member_id)
        if member_epoch != member.epoch:
            raise IllegalGeneration(group_id, member_id, f"época {member_epoch}, vigente {member.epoch}")
        changed = False
        if topics and topics != member.topics:       # cambia la suscripción
            for topic in topics:
                self._partitions(topic)
            member.topics = topics
            self._new_epoch(group)
            self._event("subscription_changed", group, member_id, topics=topics)
            changed = True
        self._target(group)                          # puede preguntar al Controller: antes de tocar nada
        member.last_seen = self._clock()

        released = member.revoking - owned           # lo que debía dejar y ya no dice tener
        if released:
            member.current -= released
            member.revoking -= released
            self._event("partitions_released", group, member_id, partitions=sorted(released))
            changed = True
        before = (member.epoch, frozenset(member.current), frozenset(member.revoking))
        response = self._reconcile(group, member)
        if changed or before != (member.epoch, frozenset(member.current), frozenset(member.revoking)):
            self._save(group)
        return response

    def _leave(self, group_id: str, member_id: str) -> dict:
        group = self.groups.get(group_id)
        if group is not None and member_id in group.members:   # salir dos veces: también OK
            del group.members[member_id]
            self._new_epoch(group)
            self._event("member_left", group, member_id)
            self._save(group)
        return self._response(group, LEAVE, [], [])

    def _reconcile(self, group: Group, member: Member) -> dict:
        """Acerca la asignación del miembro a su objetivo, sin quitarle nada a otro."""
        target = set(self._target(group).get(member.member_id, []))
        extra = member.current - target
        if extra:
            # Primero debe dejar lo que le sobra: la respuesta no lo incluye.
            if member.revoking != extra:
                member.revoking = set(extra)
                member.revoke_since = self._clock()
                self._event("revocation_requested", group, member.member_id, partitions=sorted(extra))
            usable = member.current - extra
        else:
            # Ya no le sobra nada: llega a la época del grupo y recibe lo libre.
            member.revoking.clear()
            member.revoke_since = None
            reached = member.epoch != group.epoch
            member.epoch = group.epoch
            free = {tp for tp in target - member.current if not group.owned_by_other(tp, member.member_id)}
            member.current |= free
            if reached or free:
                self._event("member_reconciled", group, member.member_id, member_epoch=member.epoch,
                            added=sorted(free), assignment=sorted(member.current),
                            pending=sorted(target - member.current))
            usable = member.current
        return self._response(group, member.epoch, sorted(usable), sorted(target - usable))

    def _response(self, group: Group | None, member_epoch: int, assignment: list, pending: list) -> dict:
        return {
            "member_epoch": member_epoch,
            "heartbeat_interval_ms": int(self._config.group_heartbeat_interval * 1000),
            "assignment": assignment,
            "pending": pending,
            "group_epoch": group.epoch if group else 0,
        }

    # --- Reparto ---------------------------------------------------------------------

    def _new_epoch(self, group: Group) -> None:
        group.epoch += 1
        group.target = None                          # se recalculará cuando haga falta

    def _target(self, group: Group) -> dict:
        if group.target is None:
            subscriptions = {m.member_id: m.topics for m in group.members.values()}
            topics = {t for ts in subscriptions.values() for t in ts}
            group.target = assign(subscriptions, {t: self._partitions(t) for t in topics})
        return group.target

    def _partitions(self, topic: str) -> int:
        """Particiones de un tema, preguntadas al Controller una vez (con plazo)."""
        if self._fixed_partitions is not None:
            return self._fixed_partitions.get(topic, 0)
        if topic in self._partition_cache:
            return self._partition_cache[topic]
        try:
            controller = ray.get_actor(self._controller_name, namespace=NAMESPACE)
            info = ray.get(controller.describe_topic.remote(topic), timeout=self._config.replication_timeout)
        except UnknownTopicOrPartition:
            return 0                                 # un tema que no existe no aporta particiones
        except (GetTimeoutError, RayActorError, ValueError):
            raise RequestTimedOut("el Controller no respondió a tiempo") from None
        self._partition_cache[topic] = info.partitions
        return info.partitions

    # --- Eventos ---------------------------------------------------------------------

    def _event(self, event: str, group: Group, member_id: str, **fields) -> None:
        """Anota una transición; se escribe en `_save`, después de guardar el grupo."""
        self._pending_events.append(
            (event, {"group_id": group.group_id, "group_epoch": group.epoch, "member_id": member_id, **fields}))

    # --- Validación -----------------------------------------------------------------

    def _check_group_id(self, group_id: str) -> None:
        validate_group_id(group_id)                  # sin «/»: el archivo no puede salir de la carpeta
        if shard_for(group_id) != self._shard:
            raise InvalidRequest("group_id", f"pertenece al fragmento {shard_for(group_id)}, no al {self._shard}")

    # --- Disco ----------------------------------------------------------------------

    def _save(self, group: Group) -> None:
        """Escribe el archivo del grupo de forma atómica: temporal y os.replace."""
        content = {
            "group_id": group.group_id,
            "group_epoch": group.epoch,
            "members": {
                m.member_id: {"topics": m.topics, "member_epoch": m.epoch,
                              "current": sorted(m.current), "revoking": sorted(m.revoking)}
                for m in group.members.values()
            },
            "offsets": [[t, p, off] for (t, p), off in sorted(group.offsets.items())],
        }
        path = self._dir / group_file_name(group.group_id)
        tmp = path.with_name(path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=1, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        for event, fields in self._pending_events:
            append_event(self._events_path, event, fsync=self._config.fsync_per_batch, **fields)
        self._pending_events.clear()

    def _load(self) -> dict[str, Group]:
        """Lee los archivos de los grupos de este fragmento.

        Las sesiones y los plazos de revocación empiezan de nuevo ahora: los
        instantes anteriores eran de otro proceso.
        """
        now = self._clock()
        groups = {}
        for path in sorted(self._dir.glob("group-*.json")):
            data = json.loads(path.read_text(encoding="utf-8"))
            group_id = data["group_id"]
            if shard_for(group_id) != self._shard:
                continue                             # de otro coordinador
            group = Group(group_id, epoch=data["group_epoch"])
            for member_id, m in data["members"].items():
                revoking = {tuple(tp) for tp in m["revoking"]}
                group.members[member_id] = Member(
                    member_id, m["topics"], epoch=m["member_epoch"],
                    current={tuple(tp) for tp in m["current"]}, revoking=revoking,
                    last_seen=now, revoke_since=now if revoking else None,
                )
            group.offsets = {(t, p): off for t, p, off in data["offsets"]}
            groups[group_id] = group
        return groups


# El actor: una llamada cada vez, sin CPU reservada, reiniciado siempre por Ray.
# max_task_retries=0: una llamada interrumpida no se repite (un commit no se
# aplica dos veces por un reintento automático).
GroupCoordinatorActor = ray.remote(num_cpus=0, max_restarts=-1, max_task_retries=0)(GroupCoordinator)


def get_group_coordinator(config: Config, group_id: str, prefix: str = COORDINATOR_PREFIX,
                          controller_name: str = CONTROLLER_NAME, tick: float = TICK):
    """Handle del coordinador que atiende a `group_id`; lo crea si no existe.

    El fragmento i vive en el nodo n(i+1), como las réplicas del Controller.
    """
    shard = shard_for(group_id)
    return GroupCoordinatorActor.options(
        name=coordinator_name(shard, prefix),
        namespace=NAMESPACE,
        lifetime="detached",
        get_if_exists=True,
        resources={f"node:n{shard + 1}": 0.01},
    ).remote(shard, config, controller_name=controller_name, start_alarm=True, tick=tick)
