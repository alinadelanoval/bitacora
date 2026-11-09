"""Pruebas de prototype.batch. No necesitan Ray."""

import struct

import pytest

from prototype import batch
from prototype.errors import CorruptBatch, InvalidBatch

BASE_TS = 1794153600000


def _varint(n: int) -> bytes:
    """Entero con signo en zigzag + 7 bits por byte, como en los registros."""
    n = (n << 1) ^ (n >> 63)
    out = bytearray()
    while n > 0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def make_batch(
    values: list[bytes],
    *,
    key: bytes = b"pedido-812",
    producer_id: int = -1,
    producer_epoch: int = -1,
    base_sequence: int = -1,
    magic: int = 2,
    attributes: int = 0,
    last_offset_delta: int | None = None,
    record_count: int | None = None,
) -> bytes:
    """Construye un lote como lo enviaría un productor (baseOffset = 0, CRC correcto).

    Los parámetros con valor por defecto permiten fabricar lotes que incumplen
    una regla concreta.
    """
    records = b""
    for i, value in enumerate(values):
        body = (
            b"\x00"                                   # attributes del registro
            + _varint(i) + _varint(i)                 # timestampDelta, offsetDelta
            + _varint(len(key)) + key
            + _varint(len(value)) + value
            + _varint(0)                              # headersCount
        )
        records += _varint(len(body)) + body
    count = len(values) if record_count is None else record_count
    delta = count - 1 if last_offset_delta is None else last_offset_delta
    covered = struct.pack(
        ">hiqqqhii", attributes, delta, BASE_TS, BASE_TS + len(values) - 1,
        producer_id, producer_epoch, base_sequence, count,
    ) + records
    rest = struct.pack(">ibI", -1, magic, batch.crc32c(covered)) + covered
    return struct.pack(">qi", 0, len(rest)) + rest


# --- CRC-32C ---------------------------------------------------------------

def test_crc32c_matches_rfc_3720_vectors():
    # RFC 3720, apéndice B.4, y el valor de comprobación estándar de CRC-32C.
    assert batch.crc32c(b"123456789") == 0xE3069283
    assert batch.crc32c(bytes(32)) == 0x8A9136AA
    assert batch.crc32c(b"\xff" * 32) == 0x62A8AB43


# --- Cabecera ---------------------------------------------------------------

def test_parse_header_reads_every_field():
    data = make_batch([b"creado", b"pagado"])
    header = batch.parse_header(data)
    assert len(data) == batch.LENGTH_PREFIX_SIZE + header.batch_length
    assert header.base_offset == 0
    assert header.partition_leader_epoch == -1
    assert header.magic == 2
    assert header.attributes == 0
    assert header.last_offset_delta == 1
    assert header.base_timestamp == BASE_TS
    assert header.max_timestamp == BASE_TS + 1
    assert (header.producer_id, header.producer_epoch, header.base_sequence) == (-1, -1, -1)
    assert header.record_count == 2


def test_parse_header_rejects_data_shorter_than_header():
    with pytest.raises(CorruptBatch):
        batch.parse_header(make_batch([b"x"])[:60])


def test_read_batch_length_at_any_position():
    first, second = make_batch([b"a"]), make_batch([b"b", b"c"])
    data = first + second
    assert batch.read_batch_length(data, 0) == len(first) - batch.LENGTH_PREFIX_SIZE
    assert batch.read_batch_length(data, len(first)) == len(second) - batch.LENGTH_PREFIX_SIZE


# --- Validación ---------------------------------------------------------------

def test_valid_batch_passes_validation_and_crc():
    data = make_batch([b"creado", b"pagado"])
    header = batch.parse_header(data)
    batch.validate(header, data)
    batch.check_crc(header, data)


def test_valid_batch_with_complete_producer_identity():
    data = make_batch([b"x"], producer_id=7, producer_epoch=0, base_sequence=0)
    batch.validate(batch.parse_header(data), data)


@pytest.mark.parametrize(
    "data, error",
    [
        (make_batch([b"x"], magic=1), CorruptBatch),
        (make_batch([b"x"]) + b"\x00", CorruptBatch),               # sobra un byte
        (make_batch([b"x", b"y"])[:-1], CorruptBatch),              # falta un byte
        (make_batch([b"x"], record_count=0, last_offset_delta=-1), InvalidBatch),
        (make_batch([b"x", b"y"], last_offset_delta=0), InvalidBatch),
        (make_batch([b"x"], attributes=1), InvalidBatch),
        (make_batch([b"x"], producer_id=7), InvalidBatch),          # identidad incompleta
        (make_batch([b"x"], producer_id=7, producer_epoch=0), InvalidBatch),
    ],
    ids=["magic", "byte-de-mas", "byte-de-menos", "record-count-0",
         "last-offset-delta", "attributes", "identidad-1-de-3", "identidad-2-de-3"],
)
def test_validate_rejects_each_rule(data, error):
    with pytest.raises(error):
        batch.validate(batch.parse_header(data), data)


# --- baseOffset y CRC ----------------------------------------------------------

def test_with_base_offset_changes_only_the_first_8_bytes():
    data = make_batch([b"creado", b"pagado"])
    stored = batch.with_base_offset(data, 5)
    assert stored[:8] == bytes.fromhex("00 00 00 00 00 00 00 05")
    assert stored[8:] == data[8:]
    header = batch.parse_header(stored)
    assert header.base_offset == 5
    batch.check_crc(header, stored)          # el CRC no cubre los bytes 0–7


def test_check_crc_detects_a_damaged_record_byte():
    data = bytearray(make_batch([b"pagado"]))
    data[-3] ^= 0x12                         # un byte del valor cambia
    with pytest.raises(CorruptBatch):
        batch.check_crc(batch.parse_header(bytes(data)), bytes(data))


def test_check_crc_does_not_cover_base_offset():
    # Límite del formato: un daño en los bytes 0–7 no lo detecta el CRC.
    data = bytearray(make_batch([b"x"]))
    data[6] = 0x7F
    batch.check_crc(batch.parse_header(bytes(data)), bytes(data))
