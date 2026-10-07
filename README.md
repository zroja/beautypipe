# beautypipe

A small, runnable reference pipeline for makeup / beauty product data, built to measure one thing:
**when a Kafka consumer falls behind, is it the stream or the sink?**

Everything is Python (`confluent-kafka`, `psycopg`, `matplotlib`). There is no Java code to write or
read. The Kafka-compatible broker is just infrastructure.

It backs the talk *"Blush, Batch, and Backpressure: Building Data Pipelines for Beauty Product
Intelligence"* with numbers anyone can reproduce.

## What it does

```
producer ──► Kafka topic (4 partitions, keyed by product) ──► consumer(s) ──► Postgres
(paced events:                                                 (instrumented)    events + product_stats rollup
 view / search / review,                                                          ▲
 Zipf-skewed products)                          batch job (every 6 s) ────────────┘
                                                normalizes messy shade names, rewrites dimension tables
```

- **Events**: product views, searches and reviews with deterministic ids, so replays hit the same keys.
- **Messy catalog**: 5,000 products and 30,000 shades written the way retailers really write them
  (`Ruby Woo`, `RUBY-WOO 01`, `Rubý Woo #3`, `No. 12 Ruby Woo (Matte)`). The batch job collapses these into
  canonical keys (`beautypipe/normalize.py`).
- **Three sinks**, same consumer, same data:
  - `blackhole`: does nothing. The ceiling for Kafka plus Python deserialization alone.
  - `pg-naive`: one transaction per event (insert, bump rollup row, commit). How first versions are usually written.
  - `pg-batched`: one set-based statement and one commit per poll.
- **Idempotent by design**: events are inserted with `ON CONFLICT DO NOTHING`, and the per-product rollup is only
  incremented for rows that were actually inserted. Redelivery and full replays cannot double count.
- **Instrumented consumer**: per-second lag, throughput, sink call latency and end-to-end latency.
- **Correctness checks**: after every run the rollup is compared with a recount of the raw events, and the
  batched scenarios replay the whole topic through a new consumer group to prove the database does not change.

## Results

Producer: 1,500 events/s for 30 s (about 45,000 events), 4 partitions, one Python consumer unless stated.
Everything ran on one 4 vCPU / 15 GB VM with Kafka, Postgres 16 (default settings, `fsync` on) and all
Python processes on the same machine. Raw per-second metrics are in [`results/`](results/).

![overview](results/overview.png)

| Scenario | Sustained throughput (events/s) | Peak lag | Drain time after producer stops | Sink ms/event | E2E p50 | E2E p99 | Rollup mismatches |
|---|---:|---:|---:|---:|---:|---:|---:|
| No-op sink (ceiling) | 1500 | 0 | 0 s | 0.000 | 0.11 s | 0.21 s | n/a |
| Postgres, per-event commit, 1 consumer | 1122 | 11,942 | 10 s | 0.882 | 5.90 s | 10.46 s | 0 |
| Postgres, per-event commit, 4 consumers | 1496 | 102 | 1 s | 0.953 | 0.27 s | 0.50 s | 0 |
| Postgres, batched upsert, 1 consumer | 1500 | 0 | 0 s | 0.028 | 0.12 s | 0.22 s | 0 |
| Per-event commit + batch refresh job | 1122 | 10,406 | 10 s | 0.874 | 5.83 s | 10.04 s | 0 |
| Batched upsert + batch refresh job | 1500 | 8 | 0 s | 0.029 | 0.12 s | 0.23 s | 0 |

Full table: [`results/summary.md`](results/summary.md). Replay check: re-consuming all ~45,000 events through a
new consumer group left the database identical (0 rollup mismatches).

Headroom test ([`results-high-rate/`](results-high-rate/summary.md)): at 8,000 events/s a single batched
consumer kept up with the no-op sink (peak lag 59 events, p99 end-to-end latency 0.10 s).

### What the numbers say

1. **Kafka was never the problem.** The no-op consumer handled 1,500 events/s with 0 lag and 0.21 s p99 latency,
   and kept up at 8,000 events/s as well. The broker and the Python consumer were not the limit.
2. **The sink was.** Per-event commits cost about 0.88 ms per event, so one consumer tops out near
   1,100 events/s. At 1,500 events/s lag grew to about 12,000 events and p99 end-to-end latency reached 10 s.
3. **Batching the same writes removed the problem.** Same database, same schema, same single consumer:
   0.028 ms per event (about 30x less), no lag, and headroom beyond 8,000 events/s.
4. **Adding consumers also worked here, but it is the expensive fix.** Four consumers with the naive sink kept
   up, but with 4x the database connections and a higher p99 (0.50 s vs 0.22 s). It is also the fix people reach
   for first, before looking at the sink.
5. **Correctness is a design property, not a speed trade-off.** Every scenario had 0 rollup mismatches, and a
   full replay is a no-op, which is what makes recovery and storage migrations safe.

### Honest limitations

- Postgres is on the same machine as the consumer, so there is no network round trip. Against a remote database
  every per-event commit gets slower, so the naive sink would look worse, not better.
- **The batch refresh job did not create a visible lag spike.** At 5,000 products it rewrites everything in about
  0.26 s, which is too small to contend with the stream. Raise `NUM_PRODUCTS` (and re-seed) to make it heavier. Do
  not claim batch interference from these results.
- One run per scenario, on a shared VM. Treat differences of a few percent as noise; the order-of-magnitude gaps
  are what matter. Re-run with `--duration` and `--rate` of your choice.
- Synthetic data. Shapes (skewed popularity, messy names) are realistic; absolute values are not.
- I measured on Apache Kafka 4.x in KRaft mode, because Docker was not available in the environment where the
  numbers were produced. `docker-compose.yml` uses Redpanda, which speaks the same protocol, so the code is
  unchanged but absolute numbers will differ slightly.

### A measurement lesson worth telling in the talk

The first benchmark run showed the per-event sink draining at roughly 220 events/s, far slower than its own
0.9 ms/event sink time implied. The cause was my instrumentation: querying high watermarks on the consuming
consumer blocked it for about 2 s per call whenever a backlog existed. Moving the watermark queries to a separate
background consumer (see the docstring in `beautypipe/consumer.py`) fixed it, and the drain time dropped from
66 s to 10 s. **Check that your metrics are not part of your latency.**

## Run it yourself

Requirements: Python 3.10+, and a Kafka-compatible broker and Postgres. With Docker:

```bash
docker compose up -d                       # Redpanda on :9092, Postgres on :5432
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

pytest                                     # unit + idempotency tests (needs Postgres)

python -m beautypipe.bench                 # all scenarios, about 10 minutes -> results/
python -m beautypipe.report                # results/overview.png + results/summary.md
```

Useful variations:

```bash
# pick scenarios and load
python -m beautypipe.bench --scenarios blackhole-1,pg-naive-1,pg-batched-1 --rate 3000 --duration 20

# run the pieces by hand
python -m beautypipe.batchjob seed
python -m beautypipe.producer --topic beauty.events --rate 1000 --duration 60
python -m beautypipe.consumer --topic beauty.events --sink pg-batched --metrics /tmp/c0.jsonl
python -m beautypipe.batchjob loop --every 6
```

Settings (environment variables): `KAFKA_BOOTSTRAP` (default `localhost:9092`), `DATABASE_URL` (default
`postgresql://beauty:beauty@localhost:5432/beauty`), `NUM_PRODUCTS`, `SHADES_PER_PRODUCT`, `PARTITIONS`.

## Layout

| Path | Purpose |
|---|---|
| `beautypipe/events.py` | Event model and deterministic, skewed event generator |
| `beautypipe/catalog.py`, `normalize.py` | Messy catalog generator and shade-name normalization |
| `beautypipe/producer.py` | Paced Kafka producer |
| `beautypipe/sinks.py` | `blackhole`, `pg-naive`, `pg-batched` sinks |
| `beautypipe/consumer.py` | Instrumented consumer (lag, throughput, latency) |
| `beautypipe/batchjob.py` | Catalog seeding and the periodic batch refresh |
| `beautypipe/bench.py` | Runs scenarios, validates, runs the replay check |
| `beautypipe/report.py` | Charts and markdown summary |
| `tests/` | Normalization, event determinism and sink idempotency tests |

## Mapping to the talk outline

| Talk section | Where to look |
|---|---|
| Messy beauty data | `catalog.py`, `normalize.py`, `tests/test_normalize.py` |
| Batch plus streaming architecture | `batchjob.py`, `consumer.py`, diagram above |
| Diagnosis: lag vs throughput vs sink latency | `results/overview.png`, `consumer.py` metrics |
| Idempotency and replay | `sinks.py`, `bench.py` replay check, `tests/test_events_and_sinks.py` |
| Storage and bottlenecks | `sinks.py` naive vs batched, results table |

## License

MIT
