"""Paced event producer.

    python -m beautypipe.producer --topic beauty.events --rate 1000 --duration 30
"""

import argparse
import time

from confluent_kafka import Producer

from . import config
from .events import EventGenerator


def produce(topic: str, rate: int, duration: float, seed: int = 1, on_progress=None) -> int:
    producer = Producer({"bootstrap.servers": config.bootstrap(), "linger.ms": 5, "compression.type": "lz4"})
    gen = EventGenerator(config.NUM_PRODUCTS, config.SHADES_PER_PRODUCT, seed=seed)
    start = time.perf_counter()
    sent = 0
    while True:
        elapsed = time.perf_counter() - start
        if elapsed >= duration:
            break
        due = int(elapsed * rate) - sent
        for _ in range(max(due, 0)):
            event = gen.next()
            producer.produce(topic, key=str(event.product_id), value=event.to_json())
            sent += 1
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
    args = parser.parse_args()
    sent = produce(args.topic, args.rate, args.duration, args.seed)
    print(f"produced {sent} events to {args.topic}")


if __name__ == "__main__":
    main()
