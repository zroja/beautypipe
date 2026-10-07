"""Instrumented consumer. Writes one JSON line of metrics per second.

    python -m beautypipe.consumer --topic beauty.events --sink pg-batched --metrics out/c0.jsonl
"""

import argparse
import json
import signal
import time
from array import array

from confluent_kafka import Consumer, KafkaError

from . import config
from .events import Event
from .sinks import SINKS, make_sink


def percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    return sorted_values[min(len(sorted_values) - 1, int(q * len(sorted_values)))]


class Window:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.processed = 0
        self.sink_calls_ms: list[float] = []
        self.e2e_ms: list[float] = []


def current_lag(consumer: Consumer) -> int:
    lag = 0
    for tp in consumer.assignment():
        low, high = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
        position = consumer.position([tp])[0].offset
        lag += high - (position if position >= 0 else low)
    return lag


def run(args: argparse.Namespace) -> None:
    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    sink = make_sink(args.sink)
    consumer = Consumer(
        {
            "bootstrap.servers": config.bootstrap(),
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 10000,
        }
    )
    ready = False

    def on_assign(c, partitions):
        nonlocal ready
        ready = True
        print(f"READY partitions={[p.partition for p in partitions]}", flush=True)

    consumer.subscribe([args.topic], on_assign=on_assign)

    window = Window()
    all_e2e = array("f")
    total = 0
    zero_lag_windows = 0
    started = time.time()
    next_flush = time.time() + 1.0
    metrics = open(args.metrics, "w")

    try:
        while not stop:
            messages = consumer.consume(num_messages=args.poll_size, timeout=0.2)
            events = []
            for m in messages:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        print(f"consumer error: {m.error()}", flush=True)
                    continue
                events.append(Event.from_json(m.value()))

            if events:
                t0 = time.perf_counter()
                sink.write(events)
                window.sink_calls_ms.append((time.perf_counter() - t0) * 1000)
                done_ms = time.time() * 1000
                for e in events:
                    latency = done_ms - e.produced_at_ms
                    window.e2e_ms.append(latency)
                    all_e2e.append(latency)
                window.processed += len(events)
                total += len(events)
                consumer.commit(asynchronous=True)

            now = time.time()
            if now >= next_flush:
                lag = current_lag(consumer) if ready else 0
                e2e = sorted(window.e2e_ms)
                calls = sorted(window.sink_calls_ms)
                metrics.write(
                    json.dumps(
                        {
                            "ts": now,
                            "processed": window.processed,
                            "lag": lag,
                            "e2e_p50_ms": percentile(e2e, 0.50),
                            "e2e_p99_ms": percentile(e2e, 0.99),
                            "sink_calls": len(calls),
                            "sink_ms_total": sum(calls),
                            "sink_call_p50_ms": percentile(calls, 0.50),
                            "sink_call_p99_ms": percentile(calls, 0.99),
                        }
                    )
                    + "\n"
                )
                metrics.flush()
                window.reset()
                next_flush = now + 1.0
                if args.until_drained:
                    zero_lag_windows = zero_lag_windows + 1 if (lag == 0 and total > 0) else 0
                    if zero_lag_windows >= 2 or now - started > args.timeout:
                        break
    finally:
        metrics.close()
        with open(args.metrics.replace(".jsonl", ".latencies.f32"), "wb") as f:
            all_e2e.tofile(f)
        sink.close()
        consumer.close()
        print(f"DONE total={total}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--group", default="beautypipe")
    parser.add_argument("--sink", choices=sorted(SINKS), default="pg-batched")
    parser.add_argument("--metrics", required=True, help="path of the .jsonl metrics file")
    parser.add_argument("--poll-size", type=int, default=500, help="max messages per poll")
    parser.add_argument("--until-drained", action="store_true", help="exit once lag has been 0 for 2 seconds")
    parser.add_argument("--timeout", type=float, default=300)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
