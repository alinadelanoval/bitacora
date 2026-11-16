"""Pruebas del gateway gRPC: el contrato de extremo a extremo sobre los actores.

Cada prueba arranca un gateway propio, en un hilo, con su Controller y sus
coordinadores de grupo, y le habla con un cliente gRPC normal. Las pruebas de
conformidad compartidas con la versión Go están en contracttest/; aquí se
comprueba la traducción entre el contrato y los actores, incluidos los fallos
del runtime.
"""

import os
import signal
import struct
import time
from contextlib import contextmanager
from dataclasses import dataclass, replace

import grpc
import pytest
import ray

from prototype.actors.controller import replica_actor_name
from prototype.actors.group_coordinator import SHARDS, coordinator_name
from prototype.config import NAMESPACE
from prototype.gateway.server import BackgroundGateway, Cluster, pb, pb_grpc
from prototype.tests.test_batch import BASE_TS, make_batch

ERR = pb.ErrorCode


@dataclass
class Stubs:
    log: pb_grpc.LogStub
    groups: pb_grpc.GroupsStub
    admin: pb_grpc.AdminStub
    topic: str
    detached_actors: list


@contextmanager
def running_gateway(config, topic, detached_actors):
    """Gateway propio en un hilo, con su Controller y sus coordinadores."""
    controller = f"controller-{topic}"
    prefix = f"gc-{topic}"
    detached_actors.append(controller)
    detached_actors.extend(coordinator_name(s, prefix) for s in range(SHARDS))
    gateway = BackgroundGateway(Cluster(config, controller, prefix))
    channel = grpc.insecure_channel(gateway.target)
    try:
        yield Stubs(pb_grpc.LogStub(channel), pb_grpc.GroupsStub(channel), pb_grpc.AdminStub(channel),
                    topic, detached_actors)
    finally:
        channel.close()
        gateway.stop()


@pytest.fixture
def api(config, topic, detached_actors):
    with running_gateway(config, topic, detached_actors) as stubs:
        yield stubs


def create(api: Stubs, partitions: int = 1, replication_factor: int = 3, request_id: str = "r1"):
    response = api.admin.CreateTopic(pb.CreateTopicRequest(
        request_id=request_id, topic=api.topic, partitions=partitions, replication_factor=replication_factor))
    for p in response.metadata.partitions:
        api.detached_actors.extend(replica_actor_name(api.topic, p.partition, n) for n in p.replica_ids)
    return response


def produce(api: Stubs, data: bytes, partition: int = 0, acks=pb.ACKS_ALL, timeout_ms: int = 5000):
    return api.log.Produce(pb.ProduceRequest(
        topic=api.topic, partition=partition, acks=acks, timeout_ms=timeout_ms, record_batch=data))


def fetch(api: Stubs, offset: int, partition: int = 0, max_bytes: int = 1 << 20, max_wait_ms: int = 0):
    return api.log.Fetch(pb.FetchRequest(
        topic=api.topic, partition=partition, offset=offset, max_bytes=max_bytes, max_wait_ms=max_wait_ms))


def base_offsets(data: bytes) -> list[int]:
    """baseOffset de cada lote de una respuesta Fetch."""
    result, pos = [], 0
    while pos < len(data):
        base, length = struct.unpack_from(">qi", data, pos)
        result.append(base)
        pos += 12 + length
    return result


# --- Log ---------------------------------------------------------------------------

def test_produce_and_fetch_round_trip(api):
    create(api)
    assert produce(api, make_batch([b"a", b"b"])).base_offset == 0
    assert produce(api, make_batch([b"c"]), acks=pb.ACKS_LEADER).base_offset == 2
    response = fetch(api, 0, max_wait_ms=2000)
    assert response.error == ERR.ERROR_CODE_NONE
    # Con acks=1 el último lote puede no ser visible todavía: Fetch espera.
    deadline = time.monotonic() + 10
    while response.high_watermark < 3 and time.monotonic() < deadline:
        response = fetch(api, 0, max_wait_ms=200)
    assert response.high_watermark == 3
    assert base_offsets(response.record_batches) == [0, 2]
    # Los bytes vuelven tal como se enviaron; solo cambia el baseOffset (0–7).
    first = make_batch([b"a", b"b"])
    assert response.record_batches[8:len(first)] == first[8:]


def test_fetch_at_the_end_waits_up_to_max_wait_and_returns_empty(api):
    create(api)
    produce(api, make_batch([b"a"]))
    start = time.monotonic()
    response = fetch(api, 1, max_wait_ms=300)
    assert response.error == ERR.ERROR_CODE_NONE and response.record_batches == b""
    assert time.monotonic() - start >= 0.25


def test_list_offsets(api):
    create(api)
    produce(api, make_batch([b"a", b"b"]))            # marcas BASE_TS y BASE_TS + 1
    tp = pb.TopicPartition(topic=api.topic, partition=0)

    def list_offset(position, ts=0):
        return api.log.ListOffsets(pb.ListOffsetsRequest(topic_partition=tp, position=position,
                                                         timestamp_unix_ms=ts))

    assert list_offset(pb.OFFSET_POSITION_EARLIEST).offset == 0
    assert list_offset(pb.OFFSET_POSITION_LATEST).offset == 2
    assert list_offset(pb.OFFSET_POSITION_TIMESTAMP, BASE_TS + 1).offset == 1
    assert list_offset(pb.OFFSET_POSITION_TIMESTAMP, BASE_TS + 9).offset == -1
    assert list_offset(pb.OFFSET_POSITION_UNSPECIFIED).error == ERR.ERROR_CODE_INVALID_REQUEST


def test_log_errors_are_returned_as_error_codes(api):
    create(api)
    good = make_batch([b"a"])
    damaged = good[:-1] + bytes([good[-1] ^ 0xFF])
    no_partition = api.log.Produce(pb.ProduceRequest(topic=api.topic, acks=pb.ACKS_ALL, timeout_ms=1000,
                                                     record_batch=good))
    assert no_partition.error == ERR.ERROR_CODE_INVALID_REQUEST
    assert produce(api, good, acks=pb.ACKS_UNSPECIFIED).error == ERR.ERROR_CODE_INVALID_REQUEST
    assert produce(api, good, timeout_ms=0).error == ERR.ERROR_CODE_INVALID_REQUEST
    assert produce(api, damaged).error == ERR.ERROR_CODE_CORRUPT_MESSAGE
    assert produce(api, good, partition=5).error == ERR.ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION
    assert fetch(api, 1).error == ERR.ERROR_CODE_OFFSET_OUT_OF_RANGE
    assert fetch(api, 0, max_bytes=0).error == ERR.ERROR_CODE_INVALID_REQUEST
    unknown = api.log.Fetch(pb.FetchRequest(topic=f"{api.topic}x", partition=0, offset=0, max_bytes=100))
    assert unknown.error == ERR.ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION
    # Nada de lo rechazado se escribió.
    assert produce(api, good).base_offset == 0


def test_acks_none_answers_without_an_offset(api):
    create(api)
    assert produce(api, make_batch([b"a"]), acks=pb.ACKS_NONE).base_offset == -1


# --- Admin -------------------------------------------------------------------------

def test_create_topic_retries_and_metadata(api):
    first = create(api, partitions=3)
    assert first.error == ERR.ERROR_CODE_NONE
    assert create(api, partitions=3).metadata == first.metadata          # mismo request_id: reintento
    assert create(api, partitions=3, request_id="r2").error == ERR.ERROR_CODE_TOPIC_ALREADY_EXISTS
    other = api.admin.CreateTopic(pb.CreateTopicRequest(request_id="r3", topic=f"{api.topic}x",
                                                        partitions=1, replication_factor=4))
    assert other.error == ERR.ERROR_CODE_INVALID_REPLICATION_FACTOR

    metadata = api.admin.GetMetadata(pb.GetMetadataRequest(topics=[api.topic]))
    (topic,) = metadata.topics
    assert [p.leader_id for p in topic.partitions] == ["n1", "n2", "n3"]
    assert all(sorted(p.isr_ids) == ["n1", "n2", "n3"] for p in topic.partitions)
    missing = api.admin.GetMetadata(pb.GetMetadataRequest(topics=[f"{api.topic}y"]))
    assert missing.error == ERR.ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION


def test_init_producer_id_is_unimplemented(api):
    with pytest.raises(grpc.RpcError) as caught:
        api.admin.InitProducerId(pb.InitProducerIdRequest(request_id="p1"))
    assert caught.value.code() == grpc.StatusCode.UNIMPLEMENTED


# --- Groups ------------------------------------------------------------------------

def test_join_commit_resume_and_reset(api):
    create(api, partitions=2)
    group = f"g-{api.topic}"
    tps = [pb.TopicPartition(topic=api.topic, partition=p) for p in range(2)]
    joined = api.groups.Heartbeat(pb.HeartbeatRequest(group_id=group, member_id="c1", member_epoch=0,
                                                      topics=[api.topic]))
    assert joined.error == ERR.ERROR_CODE_NONE
    assert sorted(tp.partition for tp in joined.assignment.partitions) == [0, 1]

    commit = api.groups.CommitOffsets(pb.CommitOffsetsRequest(
        group_id=group, member_id="c1", member_epoch=joined.member_epoch,
        offsets=[pb.OffsetCommit(topic_partition=tps[0], next_offset=7)]))
    assert commit.error == ERR.ERROR_CODE_NONE
    stale = api.groups.CommitOffsets(pb.CommitOffsetsRequest(
        group_id=group, member_id="c1", member_epoch=joined.member_epoch + 5,
        offsets=[pb.OffsetCommit(topic_partition=tps[0], next_offset=9)]))
    assert stale.error == ERR.ERROR_CODE_ILLEGAL_GENERATION

    def committed():
        response = api.groups.FetchCommittedOffsets(pb.FetchCommittedOffsetsRequest(group_id=group,
                                                                                    topic_partitions=tps))
        return {o.topic_partition.partition: o.next_offset for o in response.offsets}

    assert committed() == {0: 7}                                          # P1 sin commit: no aparece
    reset = pb.ResetGroupOffsetsRequest(request_id="x1", group_id=group,
                                        offsets=[pb.OffsetCommit(topic_partition=tps[0], next_offset=2)])
    assert api.admin.ResetGroupOffsets(reset).error == ERR.ERROR_CODE_GROUP_NOT_EMPTY
    left = api.groups.Heartbeat(pb.HeartbeatRequest(group_id=group, member_id="c1", member_epoch=-1))
    assert left.member_epoch == -1
    assert api.admin.ResetGroupOffsets(reset).error == ERR.ERROR_CODE_NONE
    assert committed() == {0: 2}                                          # el replay explícito retrocede


def test_unknown_member_and_invalid_group(api):
    create(api)
    beat = api.groups.Heartbeat(pb.HeartbeatRequest(group_id=f"g-{api.topic}", member_id="nadie",
                                                    member_epoch=3))
    assert beat.error == ERR.ERROR_CODE_UNKNOWN_MEMBER
    bad = api.groups.Heartbeat(pb.HeartbeatRequest(group_id="a/b", member_id="c1", member_epoch=0,
                                                   topics=[api.topic]))
    assert bad.error == ERR.ERROR_CODE_INVALID_REQUEST


# --- Fallo de la líder ---------------------------------------------------------------

def test_a_dead_leader_is_reported_and_the_client_continues_after_retrying(api):
    create(api)
    assert produce(api, make_batch([b"a"])).base_offset == 0
    leader = ray.get_actor(replica_actor_name(api.topic, 0, "n1"), namespace=NAMESPACE)
    os.kill(ray.get(leader.runtime_info.remote())["pid"], signal.SIGKILL)

    # La primera respuesta no puede ser un éxito con la líder muerta; después
    # de que el Controller promueva a otra, el mismo lote se acepta.
    codes = []
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        response = produce(api, make_batch([b"b"]), timeout_ms=3000)
        codes.append(response.error)
        if response.error == ERR.ERROR_CODE_NONE:
            break
        time.sleep(0.2)
    assert codes[-1] == ERR.ERROR_CODE_NONE
    assert set(codes[:-1]) <= {ERR.ERROR_CODE_NOT_LEADER, ERR.ERROR_CODE_REQUEST_TIMED_OUT}
    metadata = api.admin.GetMetadata(pb.GetMetadataRequest(topics=[api.topic]))
    assert metadata.topics[0].partitions[0].leader_id != "n1"
    # Lo confirmado antes de la caída sigue ahí, con su offset.
    assert base_offsets(fetch(api, 0).record_batches)[0] == 0


@pytest.mark.parametrize("retries, expected", [
    (0, ERR.ERROR_CODE_NOT_LEADER),          # la llamada no escribió: rechazo
    (3, ERR.ERROR_CODE_REQUEST_TIMED_OUT),   # pudo ser un reintento de Ray que ya escribió (L-14)
])
def test_produce_on_a_replica_that_is_no_longer_leader(config, topic, detached_actors, retries, expected):
    with running_gateway(replace(config, replica_task_retries=retries), topic, detached_actors) as api:
        create(api)
        assert produce(api, make_batch([b"a"])).base_offset == 0
        # La líder pasa a seguidora sin que el gateway lo sepa: su próxima
        # escritura recibe NotLeader de la réplica.
        leader = ray.get_actor(replica_actor_name(api.topic, 0, "n1"), namespace=NAMESPACE)
        ray.get(leader.become_follower.remote())
        assert produce(api, make_batch([b"b"])).error == expected
