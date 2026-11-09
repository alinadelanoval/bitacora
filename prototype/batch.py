"""Funciones sobre los bytes de un lote: cabecera, validación, baseOffset y CRC.

El formato es el de `docs/contract.md` («Formato del lote»): una cabecera fija
de 61 bytes en big-endian seguida de los registros. Este módulo solo entiende
la cabecera; los registros se guardan y se devuelven sin decodificar.

No depende de Ray ni de ningún actor: lo usan la réplica y el gateway.
"""

import struct
from dataclasses import dataclass

from prototype.errors import CorruptBatch, InvalidBatch

HEADER_SIZE = 61
# baseOffset (8 bytes) + batchLength (4): lo mínimo para saber cuánto mide un lote.
LENGTH_PREFIX_SIZE = 12
MAGIC = 2
# El CRC cubre desde `attributes` hasta el final del lote.
CRC_START = 21

# Los trece campos de la cabecera, en orden. «>» = big-endian; q = int64,
# i = int32, b = int8, I = uint32, h = int16. Suman 61 bytes.
_HEADER_FORMAT = ">qiibIhiqqqhii"
assert struct.calcsize(_HEADER_FORMAT) == HEADER_SIZE


@dataclass(frozen=True)
class Header:
    """Los campos de la cabecera de un lote."""

    base_offset: int
    batch_length: int
    partition_leader_epoch: int
    magic: int
    crc: int
    attributes: int
    last_offset_delta: int
    base_timestamp: int
    max_timestamp: int
    producer_id: int
    producer_epoch: int
    base_sequence: int
    record_count: int


def parse_header(data: bytes) -> Header:
    """Lee los 61 bytes de cabecera de un lote."""
    if len(data) < HEADER_SIZE:
        raise CorruptBatch(f"el lote mide {len(data)} bytes, menos que su cabecera ({HEADER_SIZE})")
    return Header(*struct.unpack_from(_HEADER_FORMAT, data))


def validate(header: Header, data: bytes) -> None:
    """Comprueba las reglas de estructura del contrato; no comprueba el CRC.

    Lanza CorruptBatch o InvalidBatch en el primer incumplimiento.
    """
    if header.magic != MAGIC:
        raise CorruptBatch(f"magic = {header.magic}, se esperaba {MAGIC}")
    if LENGTH_PREFIX_SIZE + header.batch_length != len(data):
        raise CorruptBatch(
            f"batchLength indica {LENGTH_PREFIX_SIZE + header.batch_length} bytes y llegaron {len(data)}"
        )
    if header.record_count < 1:
        raise InvalidBatch(f"recordCount = {header.record_count}, debe ser al menos 1")
    if header.last_offset_delta != header.record_count - 1:
        raise InvalidBatch(
            f"lastOffsetDelta = {header.last_offset_delta}, debe ser recordCount - 1 = {header.record_count - 1}"
        )
    if header.attributes != 0:
        raise InvalidBatch(f"attributes = {header.attributes}, debe ser 0")
    # La identidad del productor va completa o no va: o los tres campos valen -1
    # o ninguno.
    identity = (header.producer_id, header.producer_epoch, header.base_sequence)
    missing = sum(1 for value in identity if value == -1)
    if missing not in (0, len(identity)):
        raise InvalidBatch("identidad de productor incompleta")


def with_base_offset(data: bytes, base_offset: int) -> bytes:
    """Devuelve una copia del lote con `base_offset` escrito en los bytes 0–7.

    El CRC no cubre esos bytes, así que sigue siendo válido.
    """
    return struct.pack(">q", base_offset) + data[8:]


def read_batch_length(data: bytes, pos: int) -> int:
    """Lee el campo batchLength del lote que empieza en `pos`."""
    return struct.unpack_from(">i", data, pos + 8)[0]


# CRC-32C (Castagnoli), con una tabla de 256 entradas. zlib.crc32 calcula otro
# polinomio y daría otro resultado.
def _make_crc32c_table() -> list[int]:
    table = []
    for n in range(256):
        c = n
        for _ in range(8):
            c = (c >> 1) ^ 0x82F63B78 if c & 1 else c >> 1
        table.append(c)
    return table


_CRC32C_TABLE = _make_crc32c_table()


def crc32c(data: bytes) -> int:
    """CRC-32C de `data`."""
    c = 0xFFFFFFFF
    for byte in data:
        c = _CRC32C_TABLE[(c ^ byte) & 0xFF] ^ (c >> 8)
    return c ^ 0xFFFFFFFF


def check_crc(header: Header, data: bytes) -> None:
    """Lanza CorruptBatch si el CRC guardado no coincide con el de los bytes."""
    computed = crc32c(data[CRC_START:])
    if computed != header.crc:
        raise CorruptBatch(f"CRC guardado {header.crc:08x} y calculado {computed:08x}")
