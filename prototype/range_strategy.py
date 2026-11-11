"""Asignación `range`: reparte las particiones de cada tema entre sus suscriptores.

Para cada tema, por separado:
  1. se toman los miembros suscritos a ese tema, ordenados por nombre;
  2. se divide el número de particiones entre el de miembros;
  3. cada miembro recibe un bloque contiguo de particiones; si la división no
     es exacta, los primeros miembros reciben una más.

Es determinista: con los mismos miembros y particiones el resultado es siempre
el mismo, se calcule donde se calcule. Los casos de prueba están en
`internal/group/testdata/range_cases.json`, compartidos con la versión Go.
"""

TopicPartition = tuple[str, int]


def assign(subscriptions: dict[str, list[str]], partitions: dict[str, int]) -> dict[str, list[TopicPartition]]:
    """Reparte las particiones.

    subscriptions: miembro → temas a los que está suscrito.
    partitions: tema → número de particiones. Un tema que no aparece no tiene
    particiones y no se reparte.
    Devuelve miembro → lista de (tema, partición), ordenada.
    """
    result: dict[str, list[TopicPartition]] = {member: [] for member in subscriptions}
    topics = sorted({topic for topics in subscriptions.values() for topic in topics})
    for topic in topics:
        count = partitions.get(topic, 0)
        members = sorted(m for m, topics_of_m in subscriptions.items() if topic in topics_of_m)
        per_member, extra = divmod(count, len(members))
        start = 0
        for index, member in enumerate(members):
            size = per_member + (1 if index < extra else 0)
            result[member].extend((topic, p) for p in range(start, start + size))
            start += size
    return {member: sorted(parts) for member, parts in result.items()}
