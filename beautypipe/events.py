"""Event generation. Event ids are deterministic so replays hit the same keys."""

import hashlib
import itertools
import json
import random
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    product_id: int
    shade_id: int
    user_id: int
    rating: int | None
    produced_at_ms: int

    def to_json(self) -> bytes:
        return json.dumps(self.__dict__, separators=(",", ":")).encode()

    @staticmethod
    def from_json(raw: bytes) -> "Event":
        return Event(**json.loads(raw))


def event_id_for(seed: int, index: int) -> str:
    return hashlib.md5(f"{seed}:{index}".encode()).hexdigest()


class EventGenerator:
    """Zipf-skewed product popularity: a few products get most of the traffic."""

    def __init__(self, num_products: int, shades_per_product: int, seed: int = 1, zipf_s: float = 0.8):
        self._rng = random.Random(seed)
        self._seed = seed
        self._shades = shades_per_product
        weights = [1.0 / (rank**zipf_s) for rank in range(1, num_products + 1)]
        self._cum_weights = list(itertools.accumulate(weights))
        self._product_ids = list(range(1, num_products + 1))
        self._index = 0

    def next(self, now_ms: int | None = None) -> Event:
        rng = self._rng
        product_id = rng.choices(self._product_ids, cum_weights=self._cum_weights)[0]
        roll = rng.random()
        event_type = "view" if roll < 0.60 else "search" if roll < 0.85 else "review"
        event = Event(
            event_id=event_id_for(self._seed, self._index),
            event_type=event_type,
            product_id=product_id,
            shade_id=product_id * 10 + rng.randrange(self._shades),
            user_id=rng.randrange(1, 200_000),
            rating=rng.choice([1, 2, 3, 4, 4, 5, 5, 5]) if event_type == "review" else None,
            produced_at_ms=now_ms if now_ms is not None else int(time.time() * 1000),
        )
        self._index += 1
        return event
