"""Instrumented consumer. Writes one JSON line of metrics per second.

    python -m beautypipe.consumer --topic beauty.events --sink pg-batched --metrics out/c0.jsonl

Lag is computed as (high watermark - next offset to be consumed) per assigned partition.
High watermarks come from a separate background consumer so that measuring lag never
blocks the consuming thread (a blocking lag query on the main consumer was found to stall
it for ~2 s per call while a backlog existed, which distorted the very thing being measured).
"""

import argparse
import json
import signal
import threading
import time
from array import array

from confluent_kafka import Consumer, KafkaError, TopicPartition

from . import config
from .events import Event
from .sinks import SINKS, make_sink


def percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    return sorted_values[min(len(sorted_values) - 1, int(q * len(sorted_values)))]


class Window:
    def __init__(self) -> None:
        self.started = time.time()
        self.processed = 0
        self.sink_calls_ms: list[float] = []
        self.e2e_ms: list[float] = []


class HighWatermarkSampler(threading.Thread):
    """Polls partition high watermarks on its own connection, never touching the main consumer."""

    def __init__(self, topic: str, interval: float = 0.25) -> None:
        super().__init__(daemon=True)
        self._topic = topic
        self._interval = interval
        self._stop_event = threading.Event()
        self.highs: dict[int, int] = {}

    def run(self) -> None:
        sampler = Consumer({"bootstrap.servers": config.bootstrap(), "group.id": f"lag-sampler-{id(self)}"})
        try:
            partitions = list(sampler.list_topics(self._topic, timeout=10).topics[self._topic].partitions)
            while not self._stop_event.is_set():
                for p in partitions:
                    _, high = sampler.get_watermark_offsets(TopicPartition(self._topic, p), timeout=5, cached=False)
                    self.highs[p] = high
                self._stop_event.wait(self._interval)
        finally:
            sampler.close()

    def stop(self) -> None:
        self._stop_event.set()


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
    assigned: set[int] = set()
    next_offset: dict[int, int] = {}

    def on_assign(c, partitions):
        assigned.update(p.partition for p in partitions)
        print(f"READY partitions={sorted(assigned)}", flush=True)

    def on_revoke(c, partitions):
        for p in partitions:
            assigned.discard(p.partition)

    consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)
    sampler = HighWatermarkSampler(args.topic)
    sampler.start()

    def lag() -> int:
        return sum(max(0, sampler.highs.get(p, 0) - next_offset.get(p, 0)) for p in list(assigned))

    window = Window()
    all_e2e = array("f")
    total = 0
    zero_lag_windows = 0
    started = time.time()
    metrics = None if args.metrics == "-" else open(args.metrics, "w")

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
                next_offset[m.partition()] = m.offset() + 1

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
            if now - window.started >= 1.0:
                current_lag = lag()
                e2e = sorted(window.e2e_ms)
                calls = sorted(window.sink_calls_ms)
                line = (
                    json.dumps(
                        {
                            "ts": now,
                            "dt": now - window.started,
                            "processed": window.processed,
                            "lag": current_lag,
                            "e2e_p50_ms": percentile(e2e, 0.50),
                            "e2e_p99_ms": percentile(e2e, 0.99),
                            "sink_calls": len(calls),
                            "sink_ms_total": sum(calls),
                            "sink_call_p50_ms": percentile(calls, 0.50),
                            "sink_call_p99_ms": percentile(calls, 0.99),
                        }
                    )
                )
                if metrics:
                    metrics.write(line + "\n")
                    metrics.flush()
                else:
                    print(line, flush=True)
                window = Window()
                if args.until_drained:
                    zero_lag_windows = zero_lag_windows + 1 if (current_lag == 0 and total > 0) else 0
                    if zero_lag_windows >= 2 or now - started > args.timeout:
                        break
    finally:
        if metrics:
            metrics.close()
            with open(args.metrics.replace(".jsonl", ".latencies.f32"), "wb") as f:
                all_e2e.tofile(f)
        sampler.stop()
        sink.close()
        consumer.close()
        print(f"DONE total={total}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--group", default="beautypipe")
    parser.add_argument("--sink", choices=sorted(SINKS), default="pg-batched")
    parser.add_argument("--metrics", default="-", help="path of the .jsonl metrics file, or - for stdout")
    parser.add_argument("--poll-size", type=int, default=500, help="max messages per poll")
    parser.add_argument("--until-drained", action="store_true", help="exit once lag has been 0 for 2 seconds")
    parser.add_argument("--timeout", type=float, default=300)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
