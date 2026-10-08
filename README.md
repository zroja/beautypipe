# beautypipe

A small, runnable reference pipeline for makeup / beauty product data, built to measure one thing:
**when a Kafka consumer falls behind, is it the stream or the sink?**

Everything is Python (`confluent-kafka`, `psycopg`, `matplotlib`). There is no Java code to write or
read. The Kafka-compatible broker (Redpanda in the Kubernetes setup) is just infrastructure.

Experiments, in the order they are described below:

1. **Single machine** ([Results](#results)): same consumer, different sinks. Is it the stream or the sink?
2. **Kubernetes** ([Kubernetes experiments](#kubernetes-experiments)): pod failures, KEDA autoscaling on Kafka lag,
   and a resource-limited Postgres pod.
3. **Schema changes and malformed records** ([section 4 and 5](#4-what-happens-when-the-producer-changes-the-schema)):
   a producer deploys a new schema version, and bad events arrive. Crash loop, silent loss, or dead-letter topic?
4. **Storage format** ([Postgres vs Parquet + DuckDB](#storage-format-postgres-vs-parquet--duckdb)): the same
   2 million events in a row store and in a columnar format, ingest cost, disk, and which queries each one wins.
5. **Real catalog data** ([Open Beauty Facts](#real-catalog-data-open-beauty-facts)): the product catalog can be
   real, not generated. The events are always synthetic.

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
- **Real catalog option**: `CATALOG=obf` swaps the generated catalog for 1,126 real makeup products from Open Beauty
  Facts, with their ingredient lists ([details and limits](#real-catalog-data-open-beauty-facts)). All results in the
  Results and Kubernetes sections below were measured with the default synthetic catalog.
- **Four sinks**, same consumer, same data:
  - `blackhole`: does nothing. The ceiling for Kafka plus Python deserialization alone.
  - `pg-naive`: one transaction per event (insert, bump rollup row, commit). How first versions are usually written.
  - `pg-batched`: one set-based statement and one commit per poll.
  - `parquet`: one Parquet file per poll, read with DuckDB ([trade-offs](#storage-format-postgres-vs-parquet--duckdb)).
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

### 4. What happens when the producer changes the schema?

1,000 events/s for 45 s. At t=15 s the producer starts publishing **schema v2**, which renames two fields
(`user_id` to `customer_id`, `produced_at_ms` to `occurred_at_ms`) and adds one (`channel`). Two thirds of the events
are v2. Three consumers: one that only knows v1 and treats anything else as fatal, one that only knows v1 but
sends what it cannot parse to a dead-letter topic (DLQ), and one that understands both versions.

![schema change](results-k8s/schema-change.png)

| Scenario | Sent | Stored | In the DLQ | Missing | Consumer restarts |
|---|---:|---:|---:|---:|---:|
| v1-only consumer, fail on a bad event | 44,995 | 14,930 | 0 | 30,065 stuck in Kafka | 3 |
| v1-only consumer, dead-letter topic, then fix and redrive | 44,998 | 44,998 | 30,001 (all redriven) | 0 | 0 |
| Consumer understands v1 and v2 | 44,998 | 44,998 | 0 | 0 | 0 |

- **Fail-fast stalls the whole pipeline**, including the v1 events that were fine. The consumer sits in a crash loop
  (Kubernetes restarts it with growing back-off) and the offset never moves past the first unreadable event. Nothing is
  lost, because Kafka still has it, but nothing is stored either, and the lag grows until someone intervenes.
- **A dead-letter topic turns an outage into a backlog.** The v1 events kept flowing; the 30,001 v2 events were parked
  with headers saying why (`reason=schema_mismatch`, plus source topic, partition and offset). After rolling out a
  consumer that understands v2, `python -m beautypipe.dlq redrive` republished them and all 44,998 events were stored
  within 14 s (including the consumer rollout), with 0 rollup mismatches. Redriving is safe because the sink is
  idempotent on `event_id`.
- Understanding both versions (`beautypipe/schema.py`) is the real fix, and it needs no DLQ at all. This repo does not
  use a schema registry; a registry with compatibility rules would stop an incompatible v2 before it is published,
  which is the better first line of defense. This experiment shows what happens when that line fails.

### 5. What happens when individual events are malformed?

Same setup, no schema change. 0.2% to 0.5% of the events are damaged in one of six ways (truncated JSON, missing field,
wrong type, rating out of range, unknown event type, unsupported schema version).

![malformed events](results-k8s/malformed-events.png)

| Scenario | Sent | Valid | Stored | In the DLQ | Missing | Consumer restarts |
|---|---:|---:|---:|---:|---:|---:|
| 0.2% bad, fail on a bad event | 44,995 | 44,883 | 1,800 | 0 | 43,195 stuck in Kafka | 4 |
| 0.2% bad, fail on a bad event, KEDA autoscaling | 44,998 | 44,886 | 2,330 | 0 | 42,668 stuck in Kafka | 26 |
| 0.5% bad, log and skip | 44,999 | 44,762 | 44,762 | 0 | 237 silently dropped | 0 |
| 0.5% bad, dead-letter topic | 44,998 | 44,761 | 44,761 | 237 | 0 | 0 |

- **A single poison pill stops a partition for good**: with fail-fast the consumer stored only 1,800 of 44,883 valid
  events before the first bad one blocked it, and was still crash-looping when the run ended.
- **Autoscaling does not unblock a stuck partition.** The same fail-fast consumer with KEDA scaling on lag requested 8
  pods and restarted 26 times, and stored 2,330 of 44,886 valid events (5%) against 1,800 without it. Kafka lets only one
  consumer read a partition, and every partition contained a bad event, so more pods had nothing to take over. (Pods
  were crash-looping, so they were rarely ready at the same time.)
- **Skipping looks healthy and loses data.** Lag was 0, the dashboard was green, and 237 events were gone. The only
  way to notice is to reconcile what was sent against what was stored, which is how this repo checks every run.
- **The DLQ kept everything**: stored + dead-lettered equals sent, and the headers say why each one was rejected
  (`invalid_json` 44, `unknown_event_type` 44, `out_of_range:rating` 41, `unsupported_version` 39,
  `missing_field:product_id` 38, `wrong_type:product_id` 31).

Limits of these two experiments: one consumer pod, one run each, a fail-fast consumer that restarts from its last
commit (so it also re-reads the good events of the batch that contained the bad one), and a DLQ that is a single-partition
topic with no retention policy or alerting. A production DLQ needs an alert on its size and an owner who reads it.
The DLQ publish is flushed before offsets are committed, so a crash in between can produce duplicate dead letters;
`beautypipe.dlq` deduplicates them by source partition and offset.

### 6. Do the results hold on Apache Kafka managed by Strimzi?

All of the above was run on Redpanda, a Kafka-compatible broker from a vendor. I repeated every Kubernetes experiment on
**Apache Kafka 4.3.1 (KRaft) deployed by the [Strimzi](https://strimzi.io) operator 1.2.0** (a CNCF project), with the
same consumer code, the same producer and the same harness; only the bootstrap address differs. Results are in
[`results-k8s-strimzi/`](results-k8s-strimzi/summary.md), side by side with the Redpanda runs in
[`comparison.md`](results-k8s-strimzi/comparison.md). Set it up with `scripts/k8s-up.sh && scripts/k8s-strimzi-up.sh`, then
`python -m beautypipe.k8s_bench --broker strimzi`.

| Experiment | Redpanda | Apache Kafka on Strimzi |
|---|---|---|
| Per-event sink, 1 pod, 3,000/s: peak backlog, drain | 134,322, 164 s | 138,840, 180 s |
| Per-event sink, KEDA: peak backlog, drain, pods, pod-seconds | 33,813, 7 s, 8, 398 | 42,261, 10 s, 8, 428 |
| Batched sink, 1 pod: peak backlog, pod-seconds | 548, 62 | 556, 64 |
| Postgres 0.05 CPU, 1 pod: drain | 56 s | 54 s |
| Postgres 0.05 CPU, KEDA: drain, pods, pod-seconds | 55 s, 5, 370 | 52 s, 6, 364 |
| Hard crash: peak backlog, back under 1,000 after | 20,083, 11 s | 19,978, 11 s |
| Graceful pod deletion: peak backlog, back under 1,000 after | 541, 2 s | 6,845, 5 s |
| Schema v2, v1-only consumer, fail-fast: stored | 14,930 of 44,995 | 14,799 of 44,999 |
| Schema v2, v1-only consumer, DLQ then redrive: stored | 44,998 of 44,998 | 44,997 of 44,997 |
| 0.2% malformed, fail-fast: stored (valid events 44,883) | 1,800 | 2,316 |
| 0.2% malformed, fail-fast + KEDA: stored | 2,330 | 3,435 |
| 0.5% malformed: skip loses / DLQ loses | 237 / 0 | 237 / 0 |

- **Every conclusion held.** Per-event sink: autoscaling fixed the symptom at about 6.7x the pod-seconds of the
  batched sink's single pod (428 vs 64). Starved Postgres: autoscaling did not help (52 s vs 54 s drain). A crash cost one
  session timeout (11 s on both). Fail-fast stalled on both schema change and bad events, and KEDA
  scaling to 8 pods stored only 3,435 of 44,886 valid events. Skipping dropped 237 events silently on both; the DLQ lost none.
  Rollup mismatches were 0 in every run on both brokers.
- **One difference I did not expect and did not investigate**: graceful pod deletion produced a peak backlog of 6,845
  events on Kafka against 541 on Redpanda (5 s to recover against 2 s). It is one run on each, and the cause could be
  the group coordinator, the broker, or run-to-run noise; I would not claim it is a property of either broker.
- Small differences (for example KEDA's peak backlog of 42,261 against 33,813) are within what I would expect from one run
  on a shared VM.
- **A finding about the harness, not the broker**: in the starved-Postgres runs with KEDA, consumers restarted 15 to 16
  times. At 0.05 CPU Postgres fails its readiness probe, its Service then has no endpoints, and consumers started during that
  window crash on "connection refused" because they do not retry the initial connection. Re-running that
  experiment reproduced it (16 restarts, 54 s drain). The Redpanda runs of the same experiment did not record restarts, so I
  cannot say whether it happened there too. A production consumer should retry its connection.
- Limits: a single Kafka node (no replication, ephemeral storage, 768 MB heap) against a single Redpanda node, so this
  compares behaviour of the pipeline, not the brokers' performance or Strimzi's high-availability features. Redpanda was
  removed from the cluster while Kafka ran (both use host port 31092 and the VM is small).

### Honest limitations and things that went wrong

- Everything runs on one 4 vCPU VM in a single-node kind cluster, so the producer, Redpanda, Postgres and consumers
  share CPU. One run per scenario. The order-of-magnitude differences matter; small ones are noise.
- "Constrained storage" here means a **CPU limit on the Postgres pod**. The data directory is an `emptyDir`, so disk IOPS
  are not limited. The 0.05 CPU limit is deliberately extreme. A first attempt with 0.25 CPU did not bind at all
  (the batched sink only needs about 0.1 cores at this rate), so I lowered it.
- Pod-seconds are estimated from 2-second samples of ready pods.
- Sections 1 to 5 were measured on Redpanda (Kafka protocol). Section 6 repeats them on Apache Kafka with the
  [Strimzi](https://strimzi.io) operator; the consumer code did not change.
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
python -m beautypipe.k8s_bench             # all of them, about 40 minutes -> results-k8s/
python -m beautypipe.k8s_report            # charts + results-k8s/summary.md
scripts/k8s-strimzi-up.sh                  # optional: swap Redpanda for Apache Kafka on Strimzi
python -m beautypipe.k8s_bench --broker strimzi   # -> results-k8s-strimzi/
python -m beautypipe.k8s_report --results results-k8s-strimzi
kind delete cluster --name beautypipe      # tear down
```

The hard-crash experiment kills the container with `docker exec <kind node> crictl stop`, so the user running the
benchmark needs Docker access (the harness calls `sudo -n docker`).

## Storage format: Postgres vs Parquet + DuckDB

The same 2,000,000 events (spread over 7 days, 5,000 products) written to both stores in consumer-sized batches
of 500, then queried. Full tables and raw numbers: [`results-storage/`](results-storage/summary.md). Reproduce with
`pip install -e ".[columnar]"` and `python -m beautypipe.storage_bench` (about 4 minutes, needs the Postgres from
`docker compose` or the kind cluster).

| | Postgres (batched, idempotent) | Parquet (one file per batch) |
|---|---:|---:|
| Mean ingest per 500-event batch | 17.3 ms | 2.1 ms |
| p99 ingest per batch | 58.8 ms | 3.2 ms |
| Rows after 40 batches were delivered twice | 2,000,000 | 2,040,000 raw, 2,000,000 after dedupe on read |
| Disk | 485 MB (186 table + 299 indexes) | 78 MB as 4,087 small files, 55 MB compacted into 8 |

Queries, median of 5 warm runs in milliseconds (all engines returned the same answers):

| Query | Postgres | DuckDB, 4,087 small files | DuckDB, small files + dedupe | DuckDB, compacted |
|---|---:|---:|---:|---:|
| Top 20 products by review count and rating (scan + group by) | 90 | 285 | 613 | 11 |
| Events per hour and type over 7 days | 964 | 465 | 798 | 234 |
| Top 20 shades by distinct users | 2,132 | 364 | n/a | 88 |
| Last 20 events of one product | 0.3 | 352 | 635 | 14 |
| Review count and rating of one product | 0.1 | 198 | 620 | 2.9 |

What this shows, and what it does not:

- **Columnar wins scans, row store wins lookups.** Compacted Parquet answered the three analytical queries 4x to 24x
  faster than Postgres, while a Postgres index (or the maintained `product_stats` rollup row) answers point questions in a
  fraction of a millisecond, about 30x to 50x faster than DuckDB's best case here. Neither is "better"; they serve different
  questions, which is the argument for writing the stream to both.
- **Small files are the price of cheap ingest.** The Parquet sink is about 8x cheaper per batch because it
  maintains no indexes and no rollup, but the files it leaves behind are slow to read: reading 4,087 files was 2x to 68x
  slower than reading the same data compacted into 8 sorted files (26x for the top-products scan), and dedupe-on-read
  added another 1.7x to 3x. Compaction (1.8 s here)
  fixes it, and sorting by product made the point lookups possible at all.
- **Delivery semantics differ.** The Postgres sink is exactly-once in effect (idempotent insert). The Parquet sink is
  at-least-once: a redelivered batch becomes a second file, so readers must deduplicate on `event_id` or wait for
  compaction. Neither sink is "wrong"; know which one you are running.
- **The comparison is not apples to apples**: Postgres paid for two secondary indexes and the rollup update on every
  batch and fsyncs on commit; the Parquet sink does a rename but no fsync, so its ingest number is optimistic about
  durability on power loss. Postgres was reached over a Kubernetes NodePort (adds a round trip to every query),
  limited to 2 CPUs with default settings (128 MB `shared_buffers`, no tuning); DuckDB ran in-process with 2 threads.
  Compaction is a separate step that must be scheduled, and it does not delete the small files here.
- Single machine, warm caches, one dataset size. Absolute numbers will differ on object storage, where small files
  hurt even more. Use the ratios as an illustration of the trade-off, not as a benchmark of either product.

## Real catalog data: Open Beauty Facts

`CATALOG=obf python -m beautypipe.batchjob seed` loads a real catalog instead of the generated one:
1,126 makeup products (lipsticks, foundations, mascaras, nail polish and so on) extracted from
[Open Beauty Facts](https://world.openbeautyfacts.org), stored in the repo as `beautypipe/data/obf-makeup.jsonl.gz`
(100 KB). Rebuild it from the current export with `python -m beautypipe.obf build`.

The batch refresh normalizes it the way a real pipeline has to:

- **Products**: brand names and product names are collapsed into a canonical key, so the same product entered with
  different spelling or casing becomes one (1,126 records became 1,073 distinct products).
- **Ingredients**: 575 products have an ingredient list. Splitting and normalizing it (`aqua`, `eau` and `water` become
  `water`, `parfum` becomes `fragrance`, duplicates removed) gives 14,590 ingredient rows and 2,519 distinct
  ingredients, stored in `product_ingredients` for ingredient-level questions.
- **Shades**: only 99 of the 1,126 products (about 9%) have a shade that can be extracted from the name with a
  heuristic (`Rouge Pur 03 Rose`), the rest get one `unspecified` shade. Treat shade results on this catalog as a demo
  of the technique, not as clean data.

Limits: the **events are still synthetic** (generated over this catalog), so this makes the catalog realistic, not the
traffic. The Open Beauty Facts data is user-contributed and incomplete. All throughput and Kubernetes results in this
README were measured with the synthetic 5,000-product catalog and were not re-run on this one.

**Licence and attribution**: the extract is licensed under the Open Database License (ODbL) v1.0 and its contents under the
Database Contents License; see [`beautypipe/data/ATTRIBUTION.md`](beautypipe/data/ATTRIBUTION.md). Keep the attribution
if you redistribute it.

## Run it yourself

Requirements: Python 3.10+, and a Kafka-compatible broker and Postgres. With Docker:

```bash
docker compose up -d                       # Redpanda on :9092, Postgres on :5432
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"                  # ".[report]" if you only need the charts, ".[columnar]" for Parquet/DuckDB

pytest                                     # unit + idempotency tests (needs Postgres)

python -m beautypipe.bench                 # all scenarios, about 10 minutes -> results/
python -m beautypipe.report                # results/overview.png + results/summary.md
python -m beautypipe.storage_bench         # Postgres vs Parquet + DuckDB -> results-storage/
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
`postgresql://beauty:beauty@localhost:5432/beauty`), `NUM_PRODUCTS`, `SHADES_PER_PRODUCT`, `PARTITIONS`, `CATALOG` (`synthetic` or `obf`), `PARQUET_DIR`.

By hand, the new pieces look like this:

```bash
python -m beautypipe.producer --topic beauty.events --rate 1000 --duration 45 --v2-after 15 --bad-rate 0.005
python -m beautypipe.consumer --topic beauty.events --decoder strict-v1 --on-bad-event dlq   # or fail / skip
python -m beautypipe.dlq inspect --topic beauty.events.dlq
python -m beautypipe.dlq redrive --topic beauty.events.dlq --target beauty.events
python -m beautypipe.consumer --topic beauty.events --sink parquet                              # needs .[columnar]
```

## Layout

| Path | Purpose |
|---|---|
| `beautypipe/events.py` | Event model and deterministic, skewed event generator |
| `beautypipe/catalog.py`, `normalize.py` | Messy catalog generator (or Open Beauty Facts), shade, brand and ingredient normalization |
| `beautypipe/obf.py`, `beautypipe/data/` | Builds and stores the Open Beauty Facts makeup extract, with attribution |
| `beautypipe/schema.py` | Event decoders: strict v1, and versioned (v1 + v2) with validation |
| `beautypipe/dlq.py` | Inspect and redrive the dead-letter topic |
| `beautypipe/producer.py` | Paced Kafka producer |
| `beautypipe/sinks.py` | `blackhole`, `pg-naive`, `pg-batched` sinks |
| `beautypipe/parquet_sink.py`, `storage_bench.py` | Parquet sink, compaction, Postgres vs DuckDB benchmark |
| `beautypipe/consumer.py` | Instrumented consumer (lag, throughput, latency, decoder and bad-event policy) |
| `beautypipe/batchjob.py` | Catalog seeding and the periodic batch refresh |
| `beautypipe/bench.py` | Runs scenarios, validates, runs the replay check |
| `beautypipe/report.py` | Charts and markdown summary |
| `beautypipe/k8s_bench.py`, `k8s_report.py` | Kubernetes experiments and their charts |
| `k8s/` | Manifests: Redpanda, Postgres, consumer Deployment, batch Job/CronJob, KEDA ScaledObject, kind config |
| `scripts/k8s-up.sh` | Builds and loads images, creates the cluster, installs KEDA |
| `scripts/k8s-strimzi-up.sh`, `k8s/strimzi-kafka.yaml` | Swaps Redpanda for Apache Kafka managed by Strimzi |
| `beautypipe/compare_brokers.py` | Side-by-side table of the two brokers' results |
| `Dockerfile` | Image used by the consumer, producer and batch pods |
| `tests/` | Normalization, schema decoding, event determinism, sink idempotency and Parquet sink tests |

## Mapping to the talk outline

| Talk section | Where to look |
|---|---|
| Messy beauty data | `catalog.py`, `normalize.py`, `tests/test_normalize.py` |
| Batch plus streaming architecture | `batchjob.py`, `consumer.py`, diagram above |
| Diagnosis: lag vs throughput vs sink latency | `results/overview.png`, `consumer.py` metrics |
| Idempotency and replay | `sinks.py`, `bench.py` replay check, `tests/test_events_and_sinks.py` |
| Storage and bottlenecks | `sinks.py` naive vs batched, results table |
| Kubernetes: scaling, failure, backpressure | `k8s/`, `k8s_bench.py`, [Kubernetes experiments](#kubernetes-experiments) |
| Schema evolution, bad records, dead letters | `schema.py`, `dlq.py`, [experiments 4 and 5](#4-what-happens-when-the-producer-changes-the-schema) |
| Storage formats | `parquet_sink.py`, `storage_bench.py`, [Postgres vs Parquet + DuckDB](#storage-format-postgres-vs-parquet--duckdb) |
| Real data | `obf.py`, [Open Beauty Facts](#real-catalog-data-open-beauty-facts) |

## License

MIT
