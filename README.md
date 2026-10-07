# beautypipe

A small, runnable reference pipeline for makeup / beauty product data, built to measure one thing:
**when a Kafka consumer falls behind, is it the stream or the sink?**

Everything is Python (`confluent-kafka`, `psycopg`, `matplotlib`). There is no Java code to write or
read. The Kafka-compatible broker (Redpanda in the Kubernetes setup) is just infrastructure.

Two layers of experiments:

1. **Single machine** ([Results](#results)): same consumer, different sinks. Is it the stream or the sink?
2. **Kubernetes** ([Kubernetes experiments](#kubernetes-experiments)): pod failures, KEDA autoscaling on Kafka lag,
   and a resource-limited Postgres pod.

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

- In the single-machine results, Postgres is on the same machine as the consumer, so there is no network round trip. Against a remote database
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

## Kubernetes experiments

The same consumer runs as a Deployment on a kind cluster, with Redpanda and Postgres as pods, the catalog seed as a
Kubernetes Job (and a suspended refresh CronJob), and [KEDA](https://keda.sh) (a CNCF project) scaling consumers
on Kafka lag. The producer and all measurements run outside the consumers: the backlog is
`events produced - events stored` (read from the database), data staleness is the age of the newest stored event,
and Postgres CPU use and throttling come from its container cgroup. Raw samples are in [`results-k8s/`](results-k8s/)
and the full tables in [`results-k8s/summary.md`](results-k8s/summary.md).

### 1. What happens when a consumer pod dies?

Batched sink, 2,000 events/s, the consumer pod is killed at t=15 s.

![pod failure](results-k8s/pod-failure.png)

| Failure | Peak backlog | Max data staleness | Back to normal after | Events sent / stored | Rollup mismatches |
|---|---:|---:|---:|---:|---:|
| Graceful deletion (deploy, node drain) | 541 | 1.0 s | 2 s | 89,994 / 89,994 | 0 |
| Hard crash (SIGKILL of the container) | 20,083 | 10.0 s | 11 s | 89,996 / 89,996 | 0 |

- No events were lost or double counted in either case. Redelivered events are absorbed by the idempotent sink.
- A crash is far more expensive than a graceful stop: the consumer never leaves the group, so Kafka waits for
  `session.timeout.ms` (10 s here) before reassigning partitions. The recovery time matches that setting.
  Static group membership or a shorter session timeout trade this off differently.

### 2. Scale out the consumers, or fix the sink?

3,000 events/s for 60 s, 8 partitions. KEDA scales the Deployment from 1 to 8 pods when lag exceeds 2,000 per pod.

![scaling](results-k8s/scaling-vs-sink.png)

| Scenario | Peak backlog | Drain after producer stops | Max pods | Pod-seconds | Peak Postgres connections |
|---|---:|---:|---:|---:|---:|
| Per-event sink, 1 pod | 134,322 | 164 s | 1 | 226 | 1 |
| Per-event sink, KEDA autoscaling | 33,813 | 7 s | 8 | 398 | 8 |
| Batched sink, 1 pod | 548 | 2 s | 1 | 62 | 1 |

- Autoscaling on lag **works**. It fixed the symptom, but it needed about 28 s to reach 8 pods (polling interval,
  scale-up steps, pod start), so the backlog still peaked at about 34,000 events.
- Fixing the sink is far cheaper: the batched single pod used about 6x fewer pod-seconds and 8x fewer database
  connections than the autoscaled per-event consumers, and never built a backlog.

### 3. What if the database is the constrained resource?

4,000 events/s for 45 s, batched sink. Postgres runs as a pod with a CPU limit.

![postgres limits](results-k8s/postgres-limits.png)

| Scenario | Peak backlog | Drain after producer stops | Max pods | Pod-seconds | Postgres CPU-throttled |
|---|---:|---:|---:|---:|---:|
| Postgres 2 CPU, 1 pod | 551 | 2 s | 1 | 48 | 0 s |
| Postgres 0.05 CPU, 1 pod | 96,498 | 56 s | 1 | 102 | 60 s |
| Postgres 0.05 CPU, KEDA autoscaling | 94,998 | 55 s | 5 | 370 | 111 s |

- With a starved database the lag is real, but it is not a Kafka or consumer problem. KEDA saw the lag and added
  consumers (5 pods, 3.6x the pod-seconds) with **no improvement** (55 s vs 56 s drain), and Postgres was throttled for
  almost twice as long. Scaling the consumers only moved more load onto the bottleneck.
- The batched sink is very cheap for Postgres: at 2 CPUs it used roughly 0.1 cores or less for 4,000 events/s.

### Honest limitations and things that went wrong

- Everything runs on one 4 vCPU VM in a single-node kind cluster, so the producer, Redpanda, Postgres and consumers
  share CPU. One run per scenario. The order-of-magnitude differences matter; small ones are noise.
- "Constrained storage" here means a **CPU limit on the Postgres pod**. The data directory is an `emptyDir`, so disk IOPS
  are not limited. The 0.05 CPU limit is deliberately extreme. A first attempt with 0.25 CPU did not bind at all
  (the batched sink only needs about 0.1 cores at this rate), so I lowered it.
- Pod-seconds are estimated from 2-second samples of ready pods.
- Redpanda (Kafka protocol) is used as the broker. [Strimzi](https://strimzi.io) (CNCF) would be the usual operator
  for Apache Kafka on Kubernetes and is a natural swap; the consumer code would not change.
- Pitfalls hit while building this, which are good talk material:
  - `kubectl delete pod --force` still sends SIGTERM, and a consumer that handles it leaves the group cleanly, so the
    first "crash" test showed no failure at all. A real crash needed SIGKILL via the container runtime.
  - KEDA runs in its own namespace. With the broker advertising the short name `redpanda`, KEDA could not reach it,
    **so it never scaled and silently looked like "autoscaling does not help"**. The broker must advertise a name that
    resolves from every namespace (`redpanda.default.svc.cluster.local`).
  - Some minimal kernels lack the netfilter `statistic` module that kube-proxy needs for Services with more than one
    backend, which breaks every Service. `scripts/k8s-up.sh` runs CoreDNS with one replica to avoid it.

### Run the Kubernetes experiments

Requirements: Docker, [kind](https://kind.sigs.k8s.io), kubectl, helm.

```bash
scripts/k8s-up.sh                          # kind cluster, Redpanda, Postgres, KEDA, catalog seed Job
python -m beautypipe.k8s_bench --list      # the experiments
python -m beautypipe.k8s_bench             # all of them, about 25 minutes -> results-k8s/
python -m beautypipe.k8s_report            # charts + results-k8s/summary.md
kind delete cluster --name beautypipe      # tear down
```

The hard-crash experiment kills the container with `docker exec <kind node> crictl stop`, so the user running the
benchmark needs Docker access (the harness calls `sudo -n docker`).

## Run it yourself

Requirements: Python 3.10+, and a Kafka-compatible broker and Postgres. With Docker:

```bash
docker compose up -d                       # Redpanda on :9092, Postgres on :5432
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"                  # ".[report]" if you only need the charts

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
| `beautypipe/k8s_bench.py`, `k8s_report.py` | Kubernetes experiments and their charts |
| `k8s/` | Manifests: Redpanda, Postgres, consumer Deployment, batch Job/CronJob, KEDA ScaledObject, kind config |
| `scripts/k8s-up.sh` | Builds and loads images, creates the cluster, installs KEDA |
| `Dockerfile` | Image used by the consumer, producer and batch pods |
| `tests/` | Normalization, event determinism and sink idempotency tests |

## Mapping to the talk outline

| Talk section | Where to look |
|---|---|
| Messy beauty data | `catalog.py`, `normalize.py`, `tests/test_normalize.py` |
| Batch plus streaming architecture | `batchjob.py`, `consumer.py`, diagram above |
| Diagnosis: lag vs throughput vs sink latency | `results/overview.png`, `consumer.py` metrics |
| Idempotency and replay | `sinks.py`, `bench.py` replay check, `tests/test_events_and_sinks.py` |
| Storage and bottlenecks | `sinks.py` naive vs batched, results table |
| Kubernetes: scaling, failure, backpressure | `k8s/`, `k8s_bench.py`, [Kubernetes experiments](#kubernetes-experiments) |

## License

MIT
