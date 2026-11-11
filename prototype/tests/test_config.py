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


GROUP_TIMEOUTS = [
    ("BITACORA_GROUP_SESSION_TIMEOUT_MS", "group_session_timeout", 10.0),
    ("BITACORA_GROUP_HEARTBEAT_INTERVAL_MS", "group_heartbeat_interval", 3.0),
    ("BITACORA_GROUP_REBALANCE_TIMEOUT_MS", "group_rebalance_timeout", 15.0),
]


@pytest.mark.parametrize("variable, field, default", GROUP_TIMEOUTS)
def test_group_timeouts_have_defaults_and_are_read_in_milliseconds(monkeypatch, variable, field, default):
    monkeypatch.delenv(variable, raising=False)
    assert getattr(config.load(), field) == default
    monkeypatch.setenv(variable, "1500")
    assert getattr(config.load(), field) == 1.5


@pytest.mark.parametrize("variable, field, default", GROUP_TIMEOUTS)
def test_invalid_group_timeout_prevents_starting(monkeypatch, variable, field, default):
    monkeypatch.setenv(variable, "0")
    with pytest.raises(ValueError):
        config.load()
