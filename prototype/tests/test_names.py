"""Pruebas de las reglas de nombres. No necesitan Ray."""

import pytest

from prototype.errors import InvalidRequest
from prototype.names import GROUP_ID_MAX, TOPIC_NAME_MAX, validate_group_id, validate_topic_name


@pytest.mark.parametrize("name", ["pedidos", "facturacion", "a.b_c-1", "X" * TOPIC_NAME_MAX, "..a", "a.."])
def test_valid_topic_names(name):
    validate_topic_name(name)


@pytest.mark.parametrize("name", [
    "",                         # vacío
    ".", "..",                  # nombres especiales
    "a/b",                      # separador de rutas
    "a/../../../x",             # saldría de la carpeta de datos
    "con espacio",
    "facturación",              # no ASCII
    "X" * (TOPIC_NAME_MAX + 1),
])
def test_invalid_topic_names(name):
    with pytest.raises(InvalidRequest) as caught:
        validate_topic_name(name)
    assert (caught.value.code, caught.value.field) == (18, "topic")


def test_group_id_has_its_own_limit():
    validate_group_id("g" * GROUP_ID_MAX)
    with pytest.raises(InvalidRequest) as caught:
        validate_group_id("g" * (GROUP_ID_MAX + 1))
    assert caught.value.field == "group_id"
    with pytest.raises(InvalidRequest):
        validate_group_id("a/../../secreto")
