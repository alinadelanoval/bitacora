"""Gateway gRPC del prototipo: el contrato `bitacora.v1` delante de los actores.

Los clientes solo ven el contrato (`Log`, `Groups`, `Admin`); el gateway
traduce cada operación en llamadas a los actores y cada error a su
`ErrorCode`. Nada de Ray llega al cliente.

- Errores del dominio: los actores lanzan `BitacoraError`, que viaja con su
  `code` hasta aquí y se devuelve en el campo `error` de la respuesta.
- Fallos del runtime: si la réplica líder no responde (su proceso murió o Ray
  la está reiniciando) o responde que ya no es líder (Ray la reinició y volvió
  como seguidora), el gateway avisa al Controller para que la compruebe y
  responde `NOT_LEADER`: el cliente reintenta y, para entonces, el Controller
  ha podido promover a otra. En `Produce` la respuesta es `REQUEST_TIMED_OUT`,
  porque la líder pudo escribir el lote antes de morir: el resultado es
  indeterminado (docs/contract.md, «Resultado indeterminado»).
- Plazos: toda llamada a un actor tiene plazo. Vencido, la respuesta es
  `REQUEST_TIMED_OUT`; la llamada sigue en curso en el actor.
- Espera en `Fetch`: el actor atiende una llamada cada vez y no puede quedarse
  esperando datos sin bloquear la partición, así que el gateway repite la
  lectura cada `POLL_INTERVAL` hasta `max_wait_ms`.

El gateway guarda en memoria los metadatos de cada tema para no preguntar al
Controller en cada escritura. Los olvida en cuanto una réplica responde
`NOT_LEADER` o deja de responder.

`InitProducerId` responde `UNIMPLEMENTED`: el prototipo cumple el perfil básico.
"""

import asyncio
import sys
import threading
import uuid
from pathlib import Path

import grpc
import ray
from ray.exceptions import RayActorError

from prototype import batch
from prototype.actors.controller import CONTROLLER_NAME, TopicInfo, get_or_create_controller, replica_actor_name
from prototype.actors.group_coordinator import COORDINATOR_PREFIX, get_group_coordinator, shard_for
from prototype.config import NAMESPACE, Config
from prototype.errors import BitacoraError, InvalidRequest, NotLeader, RequestTimedOut, UnknownTopicOrPartition

# Los stubs se generan con `make proto` en gen/python (no se versionan).
GEN_DIR = Path(__file__).resolve().parents[2] / "gen" / "python"
if str(GEN_DIR) not in sys.path:
    sys.path.insert(0, str(GEN_DIR))
from bitacora.v1 import bitacora_pb2 as pb  # noqa: E402
from bitacora.v1 import bitacora_pb2_grpc as pb_grpc  # noqa: E402

try:                                    # Ray ≥ 2.30: el actor existe pero no responde ahora
    from ray.exceptions import ActorUnavailableError
except ImportError:                     # pragma: no cover
    ActorUnavailableError = RayActorError

UNAVAILABLE = (RayActorError, ActorUnavailableError)
POLL_INTERVAL = 0.05                    # s entre lecturas de un Fetch que espera
ADMIN_TIMEOUT = 60.0                    # s para CreateTopic: arranca un actor por réplica

ACKS = {pb.ACKS_LEADER: "1", pb.ACKS_ALL: "all"}
POSITIONS = {
    pb.OFFSET_POSITION_EARLIEST: "earliest",
    pb.OFFSET_POSITION_LATEST: "latest",
    pb.OFFSET_POSITION_TIMESTAMP: "timestamp",
}


async def wait(ref, timeout: float):
    """Espera el resultado de una llamada a un actor como mucho `timeout` segundos.

    Un error lanzado por el actor llega como la misma clase (Ray la envuelve en
    una subclase), así que `except NotLeader` funciona aquí.
    """
    try:
        return await asyncio.wait_for(asyncio.wrap_future(ref.future()), timeout)
    except TimeoutError:
        raise RequestTimedOut(f"el actor no respondió en {timeout:g} s") from None


def tp_message(tp) -> "pb.TopicPartition":
    return pb.TopicPartition(topic=tp[0], partition=tp[1])


def topic_metadata(info: TopicInfo) -> "pb.TopicMetadata":
    # El prototipo no tiene épocas de líder: leader_epoch vale siempre 0.
    return pb.TopicMetadata(topic=info.name, partitions=[
        pb.PartitionMetadata(partition=p, leader_id=info.leaders[p], leader_epoch=0,
                             replica_ids=info.assignment[p], isr_ids=info.isr[p])
        for p in sorted(info.assignment)
    ])


class Cluster:
    """Acceso a los actores, con los metadatos y los handles en memoria."""

    def __init__(self, config: Config, controller_name: str = CONTROLLER_NAME,
                 coordinator_prefix: str = COORDINATOR_PREFIX):
        self.config = config
        self.controller_name = controller_name
        self.coordinator_prefix = coordinator_prefix
        self.controller = get_or_create_controller(config, controller_name)
        self.call_timeout = 2 * config.replication_timeout   # la líder puede esperar a sus seguidoras
        self._topics: dict[str, TopicInfo] = {}
        self._replicas: dict[str, object] = {}
        self._coordinators: dict[int, object] = {}

    async def topic(self, name: str) -> TopicInfo:
        if name not in self._topics:
            self._topics[name] = await wait(self.controller.describe_topic.remote(name), self.call_timeout)
        return self._topics[name]

    def forget(self, name: str) -> None:
        self._topics.pop(name, None)

    async def leader(self, topic: str, partition: int):
        """Handle de la réplica líder de una partición, según el Controller."""
        info = await self.topic(topic)
        if partition not in info.leaders:
            raise UnknownTopicOrPartition(topic, partition)
        name = replica_actor_name(topic, partition, info.leaders[partition])
        if name not in self._replicas:
            try:
                self._replicas[name] = ray.get_actor(name, namespace=NAMESPACE)
            except ValueError:                 # no hay ningún actor con ese nombre
                self.leader_unavailable(topic, partition)
                raise NotLeader(topic, partition, None) from None
        return self._replicas[name]

    def leader_unavailable(self, topic: str, partition: int) -> None:
        """Pide al Controller que compruebe la líder, sin esperar, y olvida el tema."""
        self.forget(topic)
        self.controller.report_leader_unavailable.remote(topic, partition)

    def coordinator(self, group_id: str):
        shard = shard_for(group_id)
        if shard not in self._coordinators:
            self._coordinators[shard] = get_group_coordinator(
                self.config, group_id, prefix=self.coordinator_prefix, controller_name=self.controller_name)
        return self._coordinators[shard]


class Gateway:
    """Una operación del contrato por método; cada uno devuelve el mensaje de respuesta."""

    def __init__(self, cluster: Cluster):
        self.cluster = cluster

    async def _on_replica(self, topic: str, partition: int, call, indeterminate: bool):
        """Llama a la líder de una partición y traduce los fallos del runtime."""
        replica = await self.cluster.leader(topic, partition)
        try:
            return await wait(call(replica), self.cluster.call_timeout)
        except NotLeader:
            # O la caché estaba vieja, o Ray reinició a la líder y volvió como
            # seguidora. En el segundo caso solo un aviso hace que el Controller
            # promueva a otra; si la líder es otra y responde, no cambia nada.
            self.cluster.leader_unavailable(topic, partition)
            raise
        except UNAVAILABLE:
            self.cluster.leader_unavailable(topic, partition)
            if indeterminate:
                raise RequestTimedOut("la líder dejó de responder; la escritura pudo tener efecto") from None
            raise NotLeader(topic, partition, None) from None

    # --- Log --------------------------------------------------------------------------

    async def produce(self, request: "pb.ProduceRequest") -> "pb.ProduceResponse":
        if not request.HasField("partition"):
            raise InvalidRequest("partition", "es obligatoria")
        if request.acks not in ACKS and request.acks != pb.ACKS_NONE:
            raise InvalidRequest("acks", "hay que especificarlo")
        if request.timeout_ms <= 0:
            raise InvalidRequest("timeout_ms", "debe ser positivo")
        data = request.record_batch
        header = batch.parse_header(data)
        batch.validate(header, data)
        batch.check_crc(header, data)            # frontera del contrato: el lote llega íntegro

        topic, partition = request.topic, request.partition
        if request.acks == pb.ACKS_NONE:
            replica = await self.cluster.leader(topic, partition)
            replica.append.remote(data, "1")     # sin esperar: el productor no lo sabrá
            return pb.ProduceResponse(base_offset=-1)

        acks = ACKS[request.acks]
        replica = await self.cluster.leader(topic, partition)
        try:
            base = await wait(replica.append.remote(data, acks), request.timeout_ms / 1000)
        except NotLeader:
            self.cluster.leader_unavailable(topic, partition)   # ver _on_replica
            raise
        except UNAVAILABLE:
            self.cluster.leader_unavailable(topic, partition)
            raise RequestTimedOut("la líder dejó de responder; la escritura pudo tener efecto") from None
        return pb.ProduceResponse(base_offset=base, log_start_offset=0)

    async def fetch(self, request: "pb.FetchRequest") -> "pb.FetchResponse":
        if not request.HasField("partition"):
            raise InvalidRequest("partition", "es obligatoria")
        if request.max_bytes <= 0:
            raise InvalidRequest("max_bytes", "debe ser positivo")
        if request.max_wait_ms < 0:
            raise InvalidRequest("max_wait_ms", "no puede ser negativo")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + request.max_wait_ms / 1000
        while True:
            result = await self._on_replica(
                request.topic, request.partition,
                lambda r: r.fetch.remote(request.offset, request.max_bytes), indeterminate=False)
            if result["batches"] or loop.time() >= deadline:
                break
            await asyncio.sleep(min(POLL_INTERVAL, max(0.0, deadline - loop.time())))
        return pb.FetchResponse(record_batches=b"".join(result["batches"]),
                                high_watermark=result["high_watermark"], log_start_offset=0)

    async def list_offsets(self, request: "pb.ListOffsetsRequest") -> "pb.ListOffsetsResponse":
        if not request.HasField("topic_partition"):
            raise InvalidRequest("topic_partition", "es obligatoria")
        if request.position not in POSITIONS:
            raise InvalidRequest("position", "hay que especificarla")
        tp = request.topic_partition
        offset = await self._on_replica(
            tp.topic, tp.partition,
            lambda r: r.list_offset.remote(POSITIONS[request.position], request.timestamp_unix_ms),
            indeterminate=False)
        return pb.ListOffsetsResponse(offset=offset)

    # --- Groups -----------------------------------------------------------------------

    async def _on_group(self, group_id: str, call):
        try:
            return await wait(call(self.cluster.coordinator(group_id)), self.cluster.call_timeout)
        except UNAVAILABLE:
            raise RequestTimedOut("el coordinador del grupo no respondió") from None

    async def heartbeat(self, request: "pb.HeartbeatRequest") -> "pb.HeartbeatResponse":
        owned = [(tp.topic, tp.partition) for tp in request.owned_partitions]
        result = await self._on_group(request.group_id, lambda c: c.heartbeat.remote(
            request.group_id, request.member_id, request.member_epoch, list(request.topics), owned))
        return pb.HeartbeatResponse(
            member_epoch=result["member_epoch"],
            heartbeat_interval_ms=result["heartbeat_interval_ms"],
            assignment=pb.Assignment(partitions=[tp_message(tp) for tp in result["assignment"]]),
            pending=pb.Assignment(partitions=[tp_message(tp) for tp in result["pending"]]),
            group_epoch=result["group_epoch"],
        )

    async def commit_offsets(self, request: "pb.CommitOffsetsRequest") -> "pb.CommitOffsetsResponse":
        offsets = [(o.topic_partition.topic, o.topic_partition.partition, o.next_offset) for o in request.offsets]
        await self._on_group(request.group_id, lambda c: c.commit.remote(
            request.group_id, request.member_id, request.member_epoch, offsets))
        return pb.CommitOffsetsResponse()

    async def fetch_committed_offsets(self, request: "pb.FetchCommittedOffsetsRequest"
                                      ) -> "pb.FetchCommittedOffsetsResponse":
        wanted = [(tp.topic, tp.partition) for tp in request.topic_partitions]
        found = await self._on_group(request.group_id, lambda c: c.fetch.remote(request.group_id, wanted))
        return pb.FetchCommittedOffsetsResponse(offsets=[
            pb.CommittedOffset(topic_partition=tp_message(tp), next_offset=found[tp])
            for tp in wanted if tp in found
        ])

    # --- Admin ------------------------------------------------------------------------

    async def get_metadata(self, request: "pb.GetMetadataRequest") -> "pb.GetMetadataResponse":
        controller, timeout = self.cluster.controller, self.cluster.call_timeout
        names = list(request.topics) or await wait(controller.list_topics.remote(), timeout)
        infos = []
        for name in names:
            info = await wait(controller.describe_topic.remote(name), timeout)
            infos.append(info)
            self.cluster._topics[name] = info    # de paso, refresca la caché
        return pb.GetMetadataResponse(topics=[topic_metadata(i) for i in infos])

    async def create_topic(self, request: "pb.CreateTopicRequest") -> "pb.CreateTopicResponse":
        info = await wait(self.cluster.controller.create_topic.remote(
            request.topic, request.partitions, request.replication_factor, request.request_id or None),
            ADMIN_TIMEOUT)
        return pb.CreateTopicResponse(metadata=topic_metadata(info))

    async def reset_group_offsets(self, request: "pb.ResetGroupOffsetsRequest") -> "pb.ResetGroupOffsetsResponse":
        offsets = [(o.topic_partition.topic, o.topic_partition.partition, o.next_offset) for o in request.offsets]
        await self._on_group(request.group_id, lambda c: c.reset_offsets.remote(request.group_id, offsets))
        return pb.ResetGroupOffsetsResponse()


# --- Servicios gRPC -------------------------------------------------------------------

def _rpc(operation, response_type):
    """Método gRPC: ejecuta la operación y pone cada BitacoraError en el campo `error`."""
    async def method(self, request, context):
        try:
            return await operation(self.gateway, request)
        except BitacoraError as error:
            if error.code is None:               # p. ej. StorageFailure: sin código en el contrato
                await context.abort(grpc.StatusCode.INTERNAL, str(error))
            return response_type(error=error.code)
    return method


class LogService(pb_grpc.LogServicer):
    def __init__(self, gateway: Gateway):
        self.gateway = gateway

    Produce = _rpc(Gateway.produce, pb.ProduceResponse)
    Fetch = _rpc(Gateway.fetch, pb.FetchResponse)
    ListOffsets = _rpc(Gateway.list_offsets, pb.ListOffsetsResponse)


class GroupsService(pb_grpc.GroupsServicer):
    def __init__(self, gateway: Gateway):
        self.gateway = gateway

    Heartbeat = _rpc(Gateway.heartbeat, pb.HeartbeatResponse)
    CommitOffsets = _rpc(Gateway.commit_offsets, pb.CommitOffsetsResponse)
    FetchCommittedOffsets = _rpc(Gateway.fetch_committed_offsets, pb.FetchCommittedOffsetsResponse)


class AdminService(pb_grpc.AdminServicer):
    def __init__(self, gateway: Gateway):
        self.gateway = gateway

    GetMetadata = _rpc(Gateway.get_metadata, pb.GetMetadataResponse)
    CreateTopic = _rpc(Gateway.create_topic, pb.CreateTopicResponse)
    ResetGroupOffsets = _rpc(Gateway.reset_group_offsets, pb.ResetGroupOffsetsResponse)

    async def InitProducerId(self, request, context):
        await context.abort(grpc.StatusCode.UNIMPLEMENTED,
                            "el prototipo cumple el perfil básico: sin identidad de productor")


async def start(cluster: Cluster, address: str) -> tuple[grpc.aio.Server, int]:
    """Arranca el servidor en `address` («host:puerto»; puerto 0 = libre) y devuelve el puerto."""
    gateway = Gateway(cluster)
    server = grpc.aio.server()
    pb_grpc.add_LogServicer_to_server(LogService(gateway), server)
    pb_grpc.add_GroupsServicer_to_server(GroupsService(gateway), server)
    pb_grpc.add_AdminServicer_to_server(AdminService(gateway), server)
    port = server.add_insecure_port(address)
    await server.start()
    return server, port


class BackgroundGateway:
    """El gateway en un hilo con su propio bucle de eventos, para las pruebas."""

    def __init__(self, cluster: Cluster, address: str = "127.0.0.1:0"):
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name=f"gateway-{uuid.uuid4().hex[:6]}",
                                        daemon=True)
        self._thread.start()
        self._server, self.port = asyncio.run_coroutine_threadsafe(start(cluster, address), self._loop).result()

    @property
    def target(self) -> str:
        return f"127.0.0.1:{self.port}"

    def stop(self) -> None:
        asyncio.run_coroutine_threadsafe(self._server.stop(grace=None), self._loop).result()
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
