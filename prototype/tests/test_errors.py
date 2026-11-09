"""Pruebas de prototype.errors. No necesitan Ray."""

import pickle

import pytest

from prototype.errors import (
    BitacoraError,
    CorruptBatch,
    InvalidBatch,
    InvalidReplicationFactor,
    OffsetOutOfRange,
    StorageFailure,
    TopicAlreadyExists,
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
