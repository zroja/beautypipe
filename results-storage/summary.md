# Storage comparison: Postgres vs Parquet + DuckDB (2,000,000 events)

Batches of 500 events (what one consumer poll hands to a sink). DuckDB threads: 2.
80 batches were delivered twice on purpose (consumer crash before commit).

## Ingest

| Sink | Mean per batch | p99 per batch | Throughput of the sink alone | Rows after redelivery |
|---|---:|---:|---:|---:|
| Postgres (batched, idempotent) | 17.3 ms | 58.8 ms | 28,316/s | 2,000,000 (exactly once) |
| Parquet (one file per batch) | 2.1 ms | 3.2 ms | 235,959/s | 2,040,000 raw, 2,000,000 after dedupe on read |

## Disk

| | Size | Files |
|---|---:|---:|
| Postgres table | 186 MB | |
| Postgres indexes | 299 MB | |
| Postgres total | 485 MB | |
| Parquet, small files | 78 MB | 4,087 |
| Parquet, compacted | 55 MB | 8 |

Compaction (merge, dedupe, sort by product and time) took 1.8 s and removed 40,000 duplicate rows.

## Queries (median of repeated warm runs, milliseconds)

| Query | Postgres | DuckDB, small files | DuckDB, small files + dedupe | DuckDB, compacted | Same answer |
|---|---:|---:|---:|---:|:--:|
| Top 20 products by review count and average rating (full scan + group by) | 90 | 285 | 613 | 11 | yes |
| Events per hour and type over the whole period (scan + time bucket) | 964 | 465 | 798 | 234 | yes |
| Top 20 shades by distinct users (count distinct) | 2,132 | 364 | n/a | 88 | yes |
| Last 20 events of one product (id 777) | 0.3 | 352 | 635 | 14 | yes |
| Review count and average rating of one product (id 777) | 0.1 | 198 | 620 | 2.9 | yes |
