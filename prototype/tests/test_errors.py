"""Pruebas de prototype.errors. No necesitan Ray."""

import pickle

import pytest

from prototype.errors import (
    BitacoraError,
    CorruptBatch,
    IllegalGeneration,
    InvalidBatch,
    InvalidRequest,
    InvalidReplicationFactor,
    NotLeader,
    OffsetOutOfRange,
    RequestTimedOut,
    StorageFailure,
    TopicAlreadyExists,
    UnknownMember,
    UnknownTopicOrPartition,
)

ALL_ERRORS = [
    (CorruptBatch("crc"), 10),
    (InvalidBatch("attributes"), 18),
    (OffsetOutOfRange(11, 10), 4),
    (UnknownTopicOrPartition("pedidos", 1), 14),
    (TopicAlreadyExists("pedidos"), 17),
    (InvalidReplicationFactor(4), 13),
    (StorageFailure("disco lleno"), None),
    (NotLeader("pedidos", 0, "n2"), 1),
    (NotLeader("pedidos", 0, None), 1),
    (RequestTimedOut("el Controller no respondió"), 12),
    (InvalidRequest("group_id", "contiene «/»"), 18),
    (IllegalGeneration("facturacion", "c1", "época 1, vigente 2"), 7),
    (UnknownMember("facturacion", "c1"), 8),
]


@pytest.mark.parametrize("error, code", ALL_ERRORS, ids=lambda e: type(e).__name__)
def test_errors_carry_their_contract_code(error, code):
    assert isinstance(error, BitacoraError)
    assert error.code == code


@pytest.mark.parametrize("error, code", ALL_ERRORS, ids=lambda e: type(e).__name__)
def test_errors_survive_pickle(error, code):
    # Ray envía las excepciones entre procesos con pickle.
    copy = pickle.loads(pickle.dumps(error))
    assert type(copy) is type(error)
    assert copy.args == error.args
    assert vars(copy) == vars(error)
    assert str(copy) == str(error)
