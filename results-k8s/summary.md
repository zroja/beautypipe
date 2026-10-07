# Kubernetes results

All runs: kind (single node), Redpanda, Postgres 16 and consumers as pods, producer on the host at the stated rate, 8 partitions. Kubernetes-side metrics come from outside the consumers.

- **Graceful pod deletion** at t=15 s: peak backlog 541 events, max staleness 1.0 s, back under 1,000 events after 2.0 s; sent 89,994, stored 89,994, rollup mismatches 0.
- **Hard crash (SIGKILL)** at t=15 s: peak backlog 20,083 events, max staleness 10.0 s, back under 1,000 events after 11.0 s; sent 89,996, stored 89,996, rollup mismatches 0.

![pod failure](pod-failure.png)

## Scale out consumers, or fix the sink? (3,000 events/s for 60 s)

| Scenario | Peak backlog | Max data staleness | Drain after producer stops | Max pods | Pod-seconds | Peak PG connections | Rollup mismatches |
|---|---:|---:|---:|---:|---:|---:|---:|
| Per-event sink, 1 pod | 134,322 | 163.0 s | 164 s | 1 | 226 | 1 | 0 |
| Per-event sink, KEDA autoscaling | 33,813 | 9.7 s | 7 s | 8 | 398 | 8 | 0 |
| Batched sink, 1 pod | 548 | 1.0 s | 2 s | 1 | 62 | 1 | 0 |

![scaling](scaling-vs-sink.png)

## Resource-limited Postgres (4,000 events/s for 45 s)

| Scenario | Peak backlog | Max data staleness | Drain after producer stops | Max pods | Pod-seconds | Peak PG connections | Rollup mismatches |
|---|---:|---:|---:|---:|---:|---:|---:|
| Postgres 2 CPU, 1 pod | 551 | 1.0 s | 2 s | 1 | 48 | 1 | 0 |
| Postgres 0.05 CPU, 1 pod | 96,498 | 55.2 s | 56 s | 1 | 102 | 1 | 0 |
| Postgres 0.05 CPU, KEDA autoscaling | 94,998 | 54.1 s | 55 s | 5 | 370 | 4 | 0 |
- Postgres 2 CPU, 1 pod: Postgres was CPU-throttled for 0 s in total.
- Postgres 0.05 CPU, 1 pod: Postgres was CPU-throttled for 60 s in total.
- Postgres 0.05 CPU, KEDA autoscaling: Postgres was CPU-throttled for 111 s in total.

![postgres limits](postgres-limits.png)
