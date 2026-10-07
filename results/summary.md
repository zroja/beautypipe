# Results

Producer rate: **1500 events/s** for **30.0 s**, 4 partitions, 5000 products with Zipf-skewed traffic.

![overview](overview.png)

| Scenario | Sustained throughput (events/s) | Peak lag | Drain time after producer stops | Sink ms/event | E2E p50 | E2E p99 | E2E max | Rollup mismatches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| No-op sink (ceiling) | 1500 | 0 | 0 s | 0.000 | 0.11 s | 0.21 s | 0.21 s | n/a |
| Postgres, per-event commit, 1 consumer | 1122 | 11,942 | 10 s | 0.882 | 5.90 s | 10.46 s | 10.60 s | 0 |
| Postgres, per-event commit, 4 consumers | 1496 | 102 | 1 s | 0.953 | 0.27 s | 0.50 s | 0.63 s | 0 |
| Postgres, batched upsert, 1 consumer | 1500 | 0 | 0 s | 0.028 | 0.12 s | 0.22 s | 0.24 s | 0 |
| Per-event commit + batch refresh job | 1122 | 10,406 | 10 s | 0.874 | 5.83 s | 10.04 s | 10.27 s | 0 |
| Batched upsert + batch refresh job | 1500 | 8 | 0 s | 0.029 | 0.12 s | 0.23 s | 0.24 s | 0 |

Batch refresh job (rewrites all products and shades every 6 s):

- Per-event commit + batch refresh job: 7 refreshes, mean 0.26 s each
- Batched upsert + batch refresh job: 6 refreshes, mean 0.26 s each

Replay check (Postgres, batched upsert, 1 consumer): re-consumed 44,997 events through a new consumer group; database identical before/after: **True** (events=44,997, rollup mismatches=0).

Replay check (Batched upsert + batch refresh job): re-consumed 44,996 events through a new consumer group; database identical before/after: **True** (events=44,996, rollup mismatches=0).
