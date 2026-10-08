"""Dead-letter topic tools: inspect what was diverted, and redrive it once the cause is fixed.

    python -m beautypipe.dlq inspect --topic beauty.events.dlq
    python -m beautypipe.dlq redrive --topic beauty.events.dlq --target beauty.events

Redriving republishes the original bytes with the original key. It is safe to run more than once
because the sinks are idempotent on event_id: events that were already stored are ignored.
"""

import argparse
import time
from collections import Counter

from confluent_kafka import Consumer, KafkaError, Producer

from . import config


def read_all(topic: str, idle_timeout: float = 5.0):
    """Yield every message currently in `topic` (a one-off read from the beginning, no offsets committed)."""
    consumer = Consumer(
        {
            "bootstrap.servers": config.bootstrap(),
            "group.id": f"dlq-reader-{time.time_ns()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    last_message = time.time()
    try:
        while time.time() - last_message < idle_timeout:
            for m in consumer.consume(num_messages=1000, timeout=0.5):
                if m.error():
                    if m.error().code() not in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                        print(f"dlq reader error: {m.error()}")
                    continue
                last_message = time.time()
                yield m
    finally:
        consumer.close()


def _origin(m) -> tuple[str, int, int]:
    headers = {k: v.decode() for k, v in (m.headers() or [])}
    return headers.get("source_topic", ""), int(headers.get("source_partition", -1)), int(headers.get("source_offset", -1))


def inspect(topic: str, idle_timeout: float = 5.0) -> dict:
    """Count dead letters by reason. A message that was diverted twice (consumer restarted before its
    commit) is counted once, keyed by where it came from."""
    seen: dict[tuple, str] = {}
    for m in read_all(topic, idle_timeout):
        headers = {k: v.decode() for k, v in (m.headers() or [])}
        seen[_origin(m)] = headers.get("reason", "unknown")
    return {"total": len(seen), "by_reason": dict(Counter(seen.values()))}


def redrive(topic: str, target: str, idle_timeout: float = 5.0) -> int:
    producer = Producer({"bootstrap.servers": config.bootstrap(), "linger.ms": 5})
    seen: set[tuple] = set()
    for m in read_all(topic, idle_timeout):
        origin = _origin(m)
        if origin in seen:
            continue
        seen.add(origin)
        producer.produce(target, key=m.key(), value=m.value())
        producer.poll(0)
    producer.flush(30)
    return len(seen)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_inspect = sub.add_parser("inspect")
    p_inspect.add_argument("--topic", required=True)
    p_redrive = sub.add_parser("redrive")
    p_redrive.add_argument("--topic", required=True)
    p_redrive.add_argument("--target", required=True)
    args = parser.parse_args()
    if args.cmd == "inspect":
        print(inspect(args.topic))
    else:
        print(f"redriven {redrive(args.topic, args.target)} messages from {args.topic} to {args.target}")


if __name__ == "__main__":
    main()
