"""Paced event producer.

    python -m beautypipe.producer --topic beauty.events --rate 1000 --duration 30
"""

import argparse
import random
import time

from confluent_kafka import Producer

from . import config
from .events import BAD_KINDS, EventGenerator, corrupt


def produce(
    topic: str,
    rate: int,
    duration: float,
    seed: int = 1,
    on_progress=None,
    bad_rate: float = 0.0,
    v2_after: float | None = None,
    stats: dict | None = None,
) -> int:
    """Produce events at `rate` for `duration` seconds.

    bad_rate: fraction of events damaged in one of BAD_KINDS (malformed records).
    v2_after: from this many seconds on, publish schema v2 instead of v1 (a producer deploy).
    stats: updated live with sent / valid / v1_valid / bad / v2 counts. `valid` is what a consumer that
           understands every schema version should end up storing; `v1_valid` what a v1-only consumer can.
    """
    stats = stats if stats is not None else {}
    stats.update(sent=0, valid=0, v1_valid=0, bad=0, v2=0)
    chaos = random.Random(seed + 1000)
    producer = Producer({"bootstrap.servers": config.bootstrap(), "linger.ms": 5, "compression.type": "lz4"})
    gen = EventGenerator(config.num_products(), config.shades_per_product(), seed=seed)
    start = time.perf_counter()
    sent = 0
    while True:
        elapsed = time.perf_counter() - start
        if elapsed >= duration:
            break
        due = int(elapsed * rate) - sent
        for _ in range(max(due, 0)):
            use_v2 = v2_after is not None and elapsed >= v2_after
            event = gen.next(channel=chaos.choice(["app", "web", "store"]) if use_v2 else None)
            payload = event.to_json_v2() if use_v2 else event.to_json()
            if bad_rate and chaos.random() < bad_rate:
                payload = corrupt(payload, chaos.choice(BAD_KINDS))
                stats["bad"] += 1
            else:
                stats["valid"] += 1
                stats["v2" if use_v2 else "v1_valid"] += 1
            producer.produce(topic, key=str(event.product_id), value=payload)
            sent += 1
            stats["sent"] = sent
        if on_progress:
            on_progress(sent)
        producer.poll(0)
        time.sleep(0.005)
    producer.flush(30)
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default="beauty.events")
    parser.add_argument("--rate", type=int, default=1000, help="events per second")
    parser.add_argument("--duration", type=float, default=30, help="seconds")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--bad-rate", type=float, default=0.0, help="fraction of malformed events, e.g. 0.01")
    parser.add_argument("--v2-after", type=float, default=None, help="switch to schema v2 after this many seconds")
    args = parser.parse_args()
    stats: dict = {}
    sent = produce(args.topic, args.rate, args.duration, args.seed, bad_rate=args.bad_rate, v2_after=args.v2_after, stats=stats)
    print(f"produced {sent} events to {args.topic}: {stats}")


if __name__ == "__main__":
    main()
