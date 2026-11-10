"""Pruebas de prototype.config. No necesitan Ray."""

import pytest

from prototype import config


def test_replication_timeout_defaults_to_two_seconds(monkeypatch):
    monkeypatch.delenv("BITACORA_REPLICATION_TIMEOUT_MS", raising=False)
    assert config.load().replication_timeout == 2.0


def test_replication_timeout_is_read_in_milliseconds(monkeypatch):
    monkeypatch.setenv("BITACORA_REPLICATION_TIMEOUT_MS", "500")
    assert config.load().replication_timeout == 0.5


@pytest.mark.parametrize("value", ["0", "-1", "abc", "1.5"])
def test_invalid_replication_timeout_prevents_starting(monkeypatch, value):
    monkeypatch.setenv("BITACORA_REPLICATION_TIMEOUT_MS", value)
    with pytest.raises(ValueError):
        config.load()
