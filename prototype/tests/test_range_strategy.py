"""Pruebas de la asignación range. No necesitan Ray.

Los casos están en internal/group/testdata/range_cases.json, el mismo archivo
que usarán las pruebas de la versión Go.
"""

import json
import random
from pathlib import Path

import pytest

from prototype.range_strategy import assign

CASES_FILE = Path(__file__).resolve().parents[2] / "internal" / "group" / "testdata" / "range_cases.json"
CASES = json.loads(CASES_FILE.read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_shared_cases(case):
    expected = {m: [tuple(tp) for tp in parts] for m, parts in case["expected"].items()}
    assert assign(case["subscriptions"], case["partitions"]) == expected


def test_disjoint_complete_and_deterministic():
    rng = random.Random(13)
    for _ in range(200):
        topics = {f"t{i}": rng.randint(0, 7) for i in range(rng.randint(1, 3))}
        members = [f"c{i}" for i in range(rng.randint(1, 5))]
        subscriptions = {m: rng.sample(sorted(topics), rng.randint(1, len(topics))) for m in members}
        result = assign(subscriptions, topics)

        owners = [tp for parts in result.values() for tp in parts]
        assert len(owners) == len(set(owners))                    # disjunta: nadie repite partición
        wanted = {(t, p) for t in {t for ts in subscriptions.values() for t in ts} for p in range(topics[t])}
        assert set(owners) == wanted                              # completa: todas las suscritas
        for member, parts in result.items():                      # solo temas a los que se suscribió
            assert all(t in subscriptions[member] for t, _ in parts)
        shuffled = dict(rng.sample(list(subscriptions.items()), len(subscriptions)))
        assert assign(shuffled, topics) == result                 # el orden de llegada no importa
