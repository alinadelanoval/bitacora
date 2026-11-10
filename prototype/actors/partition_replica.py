"""Réplica de una partición: un log de lotes en memoria respaldado por un archivo.

Cada réplica es el único escritor de su archivo `{tema}-{partición}-{nodo}.log`,
que contiene los lotes uno tras otro, con su baseOffset ya asignado. Al
arrancar, la réplica relee el archivo para reconstruir el log.

Papeles. Toda réplica arranca como seguidora; solo el Controller la convierte
en líder (`become_leader`) o la devuelve a seguidora (`become_follower`). Una
réplica que Ray reinicia vuelve, por tanto, como seguidora.

- La líder atiende `append` y `read`. Numera cada lote, lo escribe y lo envía a
  sus seguidoras (`replicate`). Con acks="all" responde cuando todas las réplicas
  del ISR tienen el lote; con acks="1", tras escribirlo ella, y revisa las copias
  pendientes al principio de cada llamada posterior.
- La seguidora guarda, en orden, los lotes que le envía la líder, y descarta lo
  que tenga y la líder no.

El ISR (réplicas sincronizadas) lo guarda el Controller. La líder le propone
cada cambio (`alter_isr`) y no lo aplica hasta que el Controller lo acepta;
mientras tanto calcula el high watermark (HW) con el ISR anterior. El HW es el
menor LEO entre las réplicas del ISR: hasta ahí, los lotes están en todas.

Sin epochs: una líder antigua que siga viva tras una promoción puede seguir
enviando lotes a las seguidoras. Es un riesgo conocido del prototipo.

La lógica está en la clase `PartitionReplica`, que no depende de Ray mientras
no tenga seguidoras; `PartitionReplicaActor` es esa misma clase como actor.
"""

import bisect
import os
import time
from dataclasses import dataclass

import ray
from ray.exceptions import GetTimeoutError, RayActorError

from prototype import batch
from prototype.config import NAMESPACE, Config
from prototype.errors import NotLeader, OffsetOutOfRange, RequestTimedOut, StorageFailure

LEADER = "leader"
FOLLOWER = "follower"
ACKS = ("1", "all")


@dataclass(frozen=True)
class StoredBatch:
    """Un lote del log: su primer y último offset y sus bytes tal como se guardaron."""

    base_offset: int
    last_offset: int
    data: bytes


@dataclass(frozen=True)
class PendingCopy:
    """Una llamada a `replicate` enviada a una seguidora cuya respuesta aún no se ha leído."""

    ref: ray.ObjectRef     # el ticket de la llamada
    node: str              # a qué seguidora se envió
    seq: int               # orden de envío, para quedarse con la respuesta más reciente
    sent_at: float         # time.monotonic() del envío
    sent_end: int          # LEO de la líder al enviarla: hasta dónde llegaban los lotes


def replica_file_name(topic: str, partition: int, node: str) -> str:
    return f"{topic}-{partition}-{node}.log"


def replica_actor_name(topic: str, partition: int, node: str) -> str:
    return f"{topic}-{partition}-{node}"


class PartitionReplica:
    def __init__(self, topic: str, partition: int, node: str, config: Config):
        config.data_dir.mkdir(parents=True, exist_ok=True)
        self._topic = topic
        self._partition = partition
        self._node = node
        self._path = config.data_dir / replica_file_name(topic, partition, node)
        self._fsync_per_batch = config.fsync_per_batch
        self._timeout = config.replication_timeout
        self._batches: list[StoredBatch] = []
        self._leo = 0          # log end offset: el primer offset libre
        self._hw = 0           # high watermark: hasta dónde es visible
        self._broken = False   # una escritura dejó el archivo en un estado que no se pudo deshacer
        self._path.touch(exist_ok=True)
        self._recover()
        # Modo «añadir»: cada write va al final del archivo.
        self._file = open(self._path, "ab")

        self._role = FOLLOWER
        self._reset_leader_state()

    def _reset_leader_state(self) -> None:
        """Estado que solo tiene sentido mientras la réplica es líder."""
        self._followers: list[str] = []              # nodos de las seguidoras
        self._isr: set[str] = set()                  # nodos del ISR, la líder incluida
        self._controller_name: str | None = None     # nombre del actor Controller
        self._follower_leo: dict[str, int] = {}      # último LEO conocido de cada seguidora
        self._last_seq: dict[str, int] = {}          # seq de la respuesta que dio ese LEO
        self._next_send: dict[str, int] = {}         # desde qué offset enviar a cada seguidora
        self._pending: list[PendingCopy] = []        # copias enviadas sin respuesta leída
        self._handles: dict[str, object] = {}        # caché nombre de nodo → handle
        self._seq = 0

    # --- Papeles (los asigna el Controller) -----------------------------------

    def become_leader(self, followers: list[str], isr: list[str], controller_name: str | None) -> None:
        """Pasa a ser líder con estas seguidoras y este ISR.

        No se sabe aún cuánto tiene cada seguidora: se supone 0, lo que no deja
        avanzar el HW hasta que respondan. A cada una se le enviará desde el HW
        actual; lo anterior al HW ya está en todas las réplicas del ISR.
        """
        self._reset_leader_state()
        self._role = LEADER
        self._followers = list(followers)
        self._isr = set(isr) | {self._node}
        self._controller_name = controller_name
        for node in self._followers:
            self._follower_leo[node] = 0
            self._last_seq[node] = 0
            self._next_send[node] = self._hw

    def become_follower(self) -> None:
        self._role = FOLLOWER
        self._reset_leader_state()

    def _require_leader(self) -> None:
        if self._role != LEADER:
            raise NotLeader(self._topic, self._partition, self._node)

    # --- Escritura en la líder -------------------------------------------------

    def append(self, record_batch: bytes, acks: str = "all") -> int:
        """Añade un lote al log y devuelve el offset de su primer registro.

        Con acks="all", si alguna réplica del ISR no tiene el lote a tiempo, se
        propone al Controller sacarla del ISR. Si el Controller no contesta, el
        lote queda escrito pero sin confirmar (RequestTimedOut); si lo rechaza,
        esta réplica ya no es líder (NotLeader).
        """
        self._require_leader()
        if acks not in ACKS:
            raise ValueError(f"acks no admitido: {acks!r}")
        self._review_pending()

        header = batch.parse_header(record_batch)
        batch.validate(header, record_batch)
        base_offset = self._leo
        self._write_stored(batch.with_base_offset(record_batch, base_offset), header.record_count)
        batch_end = self._leo

        sent = self._send_to_followers()
        if acks == "1":
            # Las respuestas se leerán al principio de una llamada posterior.
            self._pending.extend(sent)
            self._update_hw()
            return base_offset

        # acks="all": se espera a las copias enviadas a réplicas del ISR, como
        # mucho el timeout. Las que no lleguen siguen pendientes.
        waited = [p for p in sent if p.node in self._isr]
        if waited:
            ready, _ = ray.wait([p.ref for p in waited], num_returns=len(waited), timeout=self._timeout)
            ready_set = set(ready)
            self._learn([p for p in waited if p.ref in ready_set])
            self._pending.extend(p for p in waited if p.ref not in ready_set)
        self._pending.extend(p for p in sent if p.node not in self._isr)

        # Toda réplica del ISR que no tenga el lote sale del ISR, con permiso
        # del Controller, antes de confirmar.
        lagging = {n for n in self._isr if n != self._node and self._follower_leo.get(n, 0) < batch_end}
        if lagging:
            self._propose_isr(self._isr - lagging)
        self._update_hw()
        return base_offset

    # --- Lectura en la líder -----------------------------------------------------

    def read(self, offset: int, max_bytes: int) -> list[bytes]:
        """Devuelve lotes enteros desde el que contiene `offset`, por debajo del HW.

        El primer lote puede empezar antes de `offset`: quien lee descarta los
        registros sobrantes. Se devuelve al menos un lote aunque supere
        `max_bytes`, para que quien lee siempre avance.
        """
        self._require_leader()
        self._review_pending()
        if offset < 0 or offset > self._leo:
            raise OffsetOutOfRange(offset, self._leo)
        if offset >= self._hw:
            return []

        i = bisect.bisect_left(self._batches, offset, key=lambda b: b.last_offset)
        result: list[bytes] = []
        size = 0
        while i < len(self._batches) and self._batches[i].base_offset < self._hw:
            stored = self._batches[i].data
            if result and size + len(stored) > max_bytes:
                break
            result.append(stored)
            size += len(stored)
            i += 1
        return result

    # --- Replicación: lado de la líder ------------------------------------------

    def _handle(self, node: str):
        """Handle de la réplica de `node`, buscado por nombre la primera vez."""
        if node not in self._handles:
            name = replica_actor_name(self._topic, self._partition, node)
            self._handles[node] = ray.get_actor(name, namespace=NAMESPACE)
        return self._handles[node]

    def _send_to_followers(self) -> list[PendingCopy]:
        """Envía a cada seguidora, sin esperar, los lotes que aún no se le han enviado.

        Se envía a todas, estén o no en el ISR: así una seguidora rezagada se
        pone al día y puede volver al ISR.
        """
        sent = []
        for node in self._followers:
            start = self._next_send[node]
            if start >= self._leo:
                continue
            try:
                handle = self._handle(node)
            except ValueError:
                continue                     # el actor no existe ahora mismo
            batches = self._batches_from(start)
            self._seq += 1
            ref = handle.replicate.remote(batches, self._leo, self._hw)
            sent.append(PendingCopy(ref, node, self._seq, time.monotonic(), self._leo))
            self._next_send[node] = self._leo
        return sent

    def _learn(self, done: list[PendingCopy]) -> None:
        """Lee las respuestas de copias terminadas y actualiza el LEO de cada seguidora."""
        for copy in done:
            try:
                leo = ray.get(copy.ref)
            except Exception:
                # La seguidora murió o falló durante la llamada: no se sabe qué
                # guardó. Se le reenviará desde su último LEO conocido.
                self._next_send[copy.node] = min(self._next_send[copy.node], self._follower_leo[copy.node])
                continue
            if copy.seq > self._last_seq[copy.node]:
                self._follower_leo[copy.node] = leo
                self._last_seq[copy.node] = copy.seq
            if leo < copy.sent_end:
                # No guardó todo lo enviado (le faltaba algo antes): reenviar.
                self._next_send[copy.node] = min(self._next_send[copy.node], leo)

    def _review_pending(self) -> None:
        """Revisa, sin esperar, las copias pendientes y ajusta el ISR y el HW."""
        if self._pending:
            refs = [p.ref for p in self._pending]
            ready, _ = ray.wait(refs, num_returns=len(refs), timeout=0)
            ready_set = set(ready)
            done = [p for p in self._pending if p.ref in ready_set]
            self._pending = [p for p in self._pending if p.ref not in ready_set]
            self._learn(done)

        # Reenvía lo que falte a quien no respondió del todo.
        self._pending.extend(self._send_to_followers())

        # Una réplica del ISR con una copia sin respuesta desde hace más del
        # timeout sale del ISR. Si el Controller no contesta, se intentará
        # en la próxima llamada.
        now = time.monotonic()
        stalled = {p.node for p in self._pending if p.node in self._isr and now - p.sent_at > self._timeout}
        if stalled:
            try:
                self._propose_isr(self._isr - stalled)
            except RequestTimedOut:
                pass

        # Una seguidora fuera del ISR que ha alcanzado a la líder vuelve a él.
        caught_up = {n for n in self._followers if n not in self._isr and self._follower_leo[n] == self._leo}
        if caught_up:
            try:
                self._propose_isr(self._isr | caught_up)
            except RequestTimedOut:
                pass
        self._update_hw()

    def _propose_isr(self, new_isr: set[str]) -> None:
        """Pide al Controller cambiar el ISR y lo aplica solo si lo acepta."""
        try:
            controller = ray.get_actor(self._controller_name, namespace=NAMESPACE)
            accepted = ray.get(
                controller.alter_isr.remote(self._topic, self._partition, self._node, sorted(new_isr)),
                timeout=self._timeout,
            )
        except (GetTimeoutError, RayActorError, ValueError):
            # ValueError: no hay ningún actor con ese nombre.
            raise RequestTimedOut("el Controller no confirmó el cambio del ISR") from None
        if not accepted:
            self.become_follower()
            raise NotLeader(self._topic, self._partition, self._node)
        self._isr = set(new_isr)

    def _update_hw(self) -> None:
        """HW = menor LEO entre la líder y las seguidoras del ISR. Nunca retrocede."""
        leos = [self._leo] + [self._follower_leo.get(n, 0) for n in self._isr if n != self._node]
        self._hw = max(self._hw, min(leos))

    def _batches_from(self, offset: int) -> list[bytes]:
        i = bisect.bisect_left(self._batches, offset, key=lambda b: b.last_offset)
        return [b.data for b in self._batches[i:]]

    # --- Replicación: lado de la seguidora ---------------------------------------

    def replicate(self, batches: list[bytes], leader_leo: int, leader_hw: int) -> int:
        """Guarda los lotes que envía la líder y devuelve el LEO de esta réplica.

        Los lotes llegan ya numerados. Si uno empieza antes del LEO, se compara
        con el que hay en esa posición: si es idéntico se salta (ya se tenía);
        si no, se descarta desde ahí. Si empieza después del LEO, falta algo
        antes y se para: la líder lo reenviará al ver el LEO devuelto. Al final,
        lo que pase del LEO de la líder se descarta.
        """
        complete = True
        for stored in batches:
            header = batch.parse_header(stored)
            base = header.base_offset
            if base < self._leo:
                if self._batch_at(base) == stored:
                    continue
                self._truncate_to(base)
            if base > self._leo:
                complete = False
                break
            self._write_stored(stored, header.record_count)
        if complete and self._leo > leader_leo:
            self._truncate_to(leader_leo)
        self._hw = min(leader_hw, self._leo)
        return self._leo

    def _batch_at(self, base_offset: int) -> bytes | None:
        i = bisect.bisect_left(self._batches, base_offset, key=lambda b: b.base_offset)
        if i < len(self._batches) and self._batches[i].base_offset == base_offset:
            return self._batches[i].data
        return None

    def _truncate_to(self, offset: int) -> None:
        """Descarta del log y del archivo el lote que contiene `offset` y los siguientes."""
        i = bisect.bisect_left(self._batches, offset, key=lambda b: b.last_offset)
        if i == len(self._batches):
            return
        position = sum(len(b.data) for b in self._batches[:i])
        try:
            self._file.truncate(position)
            self._file.seek(position)
        except OSError:
            self._broken = True
            raise
        self._leo = self._batches[i].base_offset
        del self._batches[i:]
        self._hw = min(self._hw, self._leo)

    # --- Escritura común -------------------------------------------------------

    def _write_stored(self, stored: bytes, record_count: int) -> None:
        """Escribe al final del log un lote cuyo baseOffset es el LEO actual."""
        if self._broken:
            raise StorageFailure("la réplica no puede escribir en su archivo")

        # Primero el archivo y después la memoria: lo que se lee nunca va por
        # delante de lo escrito.
        size_before = self._file.tell()
        try:
            self._file.write(stored)
            self._file.flush()
            if self._fsync_per_batch:
                os.fsync(self._file.fileno())
        except OSError:
            # La escritura falló sin matar el proceso y puede haber dejado parte
            # del lote. Si quedara en medio del archivo, la recuperación se
            # detendría ahí y descartaría los lotes posteriores, ya confirmados.
            try:
                self._file.truncate(size_before)
                self._file.seek(size_before)
            except OSError:
                self._broken = True
            raise

        base_offset = self._leo
        last_offset = base_offset + record_count - 1
        self._batches.append(StoredBatch(base_offset, last_offset, stored))
        self._leo = last_offset + 1

    # --- Estado, para el Controller, el diagnóstico y las pruebas ---------------

    def status(self) -> dict:
        """Papel, LEO y HW. El Controller lo usa para comprobar si sigue viva."""
        return {"role": self._role, "leo": self._leo, "hw": self._hw}

    def isr(self) -> list[str]:
        return sorted(self._isr)

    def log_end_offset(self) -> int:
        return self._leo

    def high_watermark(self) -> int:
        return self._hw

    def runtime_info(self) -> dict:
        """Proceso y nodo Ray en los que se ejecuta la réplica."""
        info = {"pid": os.getpid(), "node_id": None}
        if ray.is_initialized():
            info["node_id"] = ray.get_runtime_context().get_node_id()
        return info

    # --- Recuperación -------------------------------------------------------

    def _recover(self) -> None:
        """Reconstruye el log a partir del archivo y recorta lo incompleto del final.

        Acepta lotes mientras estén enteros y su baseOffset sea el siguiente
        esperado. Lo que queda detrás del último lote aceptado es un lote
        cortado por una caída a mitad de escritura: como la réplica solo
        confirma un lote después de escribirlo entero, nunca se confirmó.

        El HW no se guarda: tras reiniciar se toma 0, y como la réplica vuelve
        como seguidora, la líder se lo comunicará en la siguiente copia.
        """
        data = self._path.read_bytes()
        pos = 0
        while pos < len(data):
            if len(data) - pos < batch.LENGTH_PREFIX_SIZE:
                break
            end = pos + batch.LENGTH_PREFIX_SIZE + batch.read_batch_length(data, pos)
            if end > len(data) or end - pos < batch.HEADER_SIZE:
                break
            header = batch.parse_header(data[pos:end])
            if header.magic != batch.MAGIC or header.base_offset != self._leo:
                break
            last_offset = header.base_offset + header.record_count - 1
            self._batches.append(StoredBatch(header.base_offset, last_offset, data[pos:end]))
            self._leo = last_offset + 1
            pos = end

        if pos < len(data):
            with open(self._path, "r+b") as f:
                f.truncate(pos)


# El actor: una llamada cada vez, sin CPU reservada (solo espera E/S), y
# reiniciado por Ray cada vez que su proceso muere. max_task_retries=0: una
# llamada interrumpida no se repite, para no duplicar lotes.
PartitionReplicaActor = ray.remote(num_cpus=0, max_restarts=-1, max_task_retries=0)(PartitionReplica)
