"""Vectores del formato de lote comunes a Python y Go. No necesitan Ray.

Los casos están en internal/record/testdata/batch_vectors.json, el mismo
archivo que usan las pruebas de internal/record. Cada caso tiene los registros
y los bytes exactos del lote que debe producir un productor (baseOffset = 0,
partitionLeaderEpoch = -1). Si una implementación codifica o lee distinto, la
otra lo detecta aquí.
"""

import json
from pathlib import Path

import pytest

from prototype import batch

VECTORS_FILE = Path(__file__).resolve().parents[2] / "internal" / "record" / "testdata" / "batch_vectors.json"
VECTORS = json.loads(VECTORS_FILE.read_text(encoding="utf-8"))


def _bytes(text: str | None) -> bytes | None:
    return None if text is None else text.encode("utf-8")


@pytest.mark.parametrize("case", VECTORS, ids=[c["name"] for c in VECTORS])
def test_header_is_valid_and_matches_the_records(case):
    data = bytes.fromhex(case["batch"])
    header = batch.parse_header(data)
    batch.validate(header, data)
    batch.check_crc(header, data)

    timestamps = [r["timestamp_ms"] for r in case["records"]]
    producer = case["producer"] or {"id": -1, "epoch": -1, "base_sequence": -1}
    assert header.base_offset == 0
    assert header.partition_leader_epoch == -1
    assert header.record_count == len(case["records"])
    assert header.base_timestamp == timestamps[0]
    assert header.max_timestamp == max(timestamps)
    assert (header.producer_id, header.producer_epoch, header.base_sequence) == (
        producer["id"], producer["epoch"], producer["base_sequence"])


@pytest.mark.parametrize("case", VECTORS, ids=[c["name"] for c in VECTORS])
def test_records_decode_with_offset_and_timestamp(case):
    data = batch.with_base_offset(bytes.fromhex(case["batch"]), 100)
    decoded = batch.records(data)
    assert decoded == [
        batch.Record(100 + i, r["timestamp_ms"], _bytes(r["key"]), _bytes(r["value"]))
        for i, r in enumerate(case["records"])
    ]
