"""Un consumidor de prueba que reconcilia como lo hará un cliente real.

Funciona con el coordinador como clase normal (sin Ray) o como actor.
"""

import ray


class GroupMember:
    def __init__(self, coordinator, group_id: str, member_id: str, topics: list[str]):
        self.coordinator = coordinator
        self.group_id = group_id
        self.member_id = member_id
        self.topics = topics
        self.epoch = None                 # None: fuera del grupo
        self.owned: set = set()
        self.progress: dict = {}          # (tema, partición) → siguiente offset que procesaría
        self.last_response = None

    def _call(self, method: str, *args):
        target = getattr(self.coordinator, method)
        if isinstance(self.coordinator, ray.actor.ActorHandle):
            return ray.get(target.remote(*args))
        return target(*args)

    def enter(self) -> dict:
        self.owned = set()
        return self._apply(self._call("heartbeat", self.group_id, self.member_id, 0, self.topics, []))

    def beat(self) -> dict:
        return self._apply(self._call("heartbeat", self.group_id, self.member_id, self.epoch, [],
                                      sorted(self.owned)))

    def leave(self) -> dict:
        self.commit_all()                 # confirmar antes de salir
        response = self._call("heartbeat", self.group_id, self.member_id, -1, [], [])
        self.epoch, self.owned = None, set()
        return response

    def process(self, tp, count: int = 1) -> None:
        """Simula procesar `count` registros de una partición propia."""
        assert tuple(tp) in self.owned
        self.progress[tuple(tp)] = self.progress.get(tuple(tp), self.start_of(tp)) + count

    def start_of(self, tp) -> int:
        return self._call("fetch", self.group_id, [tuple(tp)]).get(tuple(tp), 0)

    def commit(self, tps) -> None:
        offsets = [(t, p, self.progress[(t, p)]) for t, p in tps if (t, p) in self.progress]
        if offsets:
            self._call("commit", self.group_id, self.member_id, self.epoch, offsets)

    def commit_all(self) -> None:
        if self.epoch is not None:
            self.commit(sorted(self.owned))

    def _apply(self, response: dict) -> dict:
        allowed = {tuple(tp) for tp in response["assignment"]}
        self.commit(sorted(self.owned - allowed))   # confirmar lo que deja, antes de dejarlo
        for tp in self.owned - allowed:
            self.progress.pop(tp, None)
        self.owned = allowed
        self.epoch = response["member_epoch"]
        self.last_response = response
        return response
