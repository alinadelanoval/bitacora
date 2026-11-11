"""Reglas para los nombres de tema y los identificadores de grupo.

Los dos se usan para nombrar archivos: `{tema}-{partición}-{nodo}.log` y
`group-{id}.json`. Por eso solo se admiten caracteres sin significado especial
en una ruta: letras ASCII, dígitos, `.`, `_` y `-`. Sin `/` no hay forma de
salir del directorio de datos; `.` y `..` se rechazan como nombre completo.

La regla es una lista de caracteres permitidos y no de secuencias prohibidas:
una lista de prohibidos siempre puede dejarse algún caso.
"""

import re

from prototype.errors import InvalidRequest

# Un nombre entero formado solo por caracteres permitidos.
_ALLOWED = re.compile(r"[A-Za-z0-9._-]+")

TOPIC_NAME_MAX = 249     # deja sitio al sufijo de la partición en 255 bytes
GROUP_ID_MAX = 64


def validate_name(field: str, name: str, max_length: int) -> None:
    """Lanza InvalidRequest si `name` no es un nombre válido para `field`."""
    if not isinstance(name, str) or name == "":
        raise InvalidRequest(field, "no puede estar vacío")
    if name in (".", ".."):
        raise InvalidRequest(field, f"«{name}» no está permitido")
    if len(name) > max_length:
        raise InvalidRequest(field, f"tiene {len(name)} caracteres; el máximo es {max_length}")
    if not _ALLOWED.fullmatch(name):
        raise InvalidRequest(field, "solo admite letras ASCII, dígitos, «.», «_» y «-»")


def validate_topic_name(name: str) -> None:
    validate_name("topic", name, TOPIC_NAME_MAX)


def validate_group_id(group_id: str) -> None:
    validate_name("group_id", group_id, GROUP_ID_MAX)
