"""Postgres versus Parquet + DuckDB for the same events.

    python -m beautypipe.storage_bench --events 2000000 --out results-storage

Answers two questions with the same data in both stores:

1. What does it cost to ingest (per poll-sized batch, the way the consumer calls a sink)?
2. What does each store do well? Analytical scans (columnar wins), and point lookups / rollup reads
   (indexed row store wins), plus how much on-disk space each uses and what the small-files problem costs.

Timestamps are spread over `--days` days so that time-bucket queries are meaningful. Everything runs
on one machine; Postgres is reached over a NodePort (like the other experiments) and DuckDB runs in
process, limited to `--threads` threads to match the Postgres CPU limit. Read the numbers as relative.
"""

import argparse
import json
import shutil
import statistics
import time
from pathlib import Path

from . import config, db
from .events import EventGenerator
from .parquet_sink import ParquetSink, compact, connect, read_events
from .sinks import PostgresBatchedSink

BATCH = 500
POINT_PRODUCT = 777

QUERIES = {
    "top_products": dict(
        label="Top 20 products by review count and average rating (full scan + group by)",
        pg="SELECT product_id, count(*) AS reviews, round(avg(rating)::numeric, 3) AS avg_rating FROM events "
           "WHERE event_type = 'review' GROUP BY product_id ORDER BY reviews DESC, product_id LIMIT 20",
        duck="SELECT product_id, count(*) AS reviews, round(avg(rating), 3) AS avg_rating FROM {src} "
             "WHERE event_type = 'review' GROUP BY product_id ORDER BY reviews DESC, product_id LIMIT 20",
    ),
    "hourly": dict(
        label="Events per hour and type over the whole period (scan + time bucket)",
        pg="SELECT extract(epoch FROM date_trunc('hour', event_ts)) AS h, event_type, count(*) FROM events GROUP BY 1, 2 ORDER BY 1, 2",
        duck="SELECT epoch(date_trunc('hour', event_ts)) AS h, event_type, count(*) FROM {src} GROUP BY 1, 2 ORDER BY 1, 2",
    ),
    "distinct_users": dict(
        label="Top 20 shades by distinct users (count distinct)",
        pg="SELECT shade_id, count(DISTINCT user_id) AS users FROM events GROUP BY shade_id ORDER BY users DESC, shade_id LIMIT 20",
        duck="SELECT shade_id, count(DISTINCT user_id) AS users FROM {src} GROUP BY shade_id ORDER BY users DESC, shade_id LIMIT 20",
    ),
    "point_recent": dict(
        label=f"Last 20 events of one product (id {POINT_PRODUCT})",
        pg=f"SELECT event_id, event_type, extract(epoch FROM event_ts) FROM events WHERE product_id = {POINT_PRODUCT} ORDER BY event_ts DESC LIMIT 20",
        duck=f"SELECT event_id, event_type, epoch(event_ts) FROM {{src}} WHERE product_id = {POINT_PRODUCT} ORDER BY event_ts DESC LIMIT 20",
    ),
    "rollup_one": dict(
        label=f"Review count and average rating of one product (id {POINT_PRODUCT})",
        pg=f"SELECT review_count, round(rating_sum::numeric / nullif(review_count, 0), 3) FROM product_stats WHERE product_id = {POINT_PRODUCT}",
        duck=f"SELECT count(*), round(avg(rating), 3) FROM {{src}} WHERE product_id = {POINT_PRODUCT} AND event_type = 'review'",
    ),
}


def dir_bytes(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*.parquet"))


def timed(fn, repeat: int) -> dict:
    samples = []
    result = None
    for _ in range(repeat):
        started = time.perf_counter()
        result = fn()
        samples.append((time.perf_counter() - started) * 1000)
    return {"median_ms": statistics.median(samples), "min_ms": min(samples), "first_ms": samples[0], "result": result}


def ingest(n: int, days: int, root: Path, redeliver_every: int) -> dict:
    db.init_schema()
    with db.connect(autocommit=True) as conn:
        conn.execute("TRUNCATE events, product_stats")
    gen = EventGenerator(config.num_products(), config.shades_per_product(), seed=7)
    pg, pq_sink = PostgresBatchedSink(), ParquetSink(root)
    span_ms = days * 86_400_000
    start_ms = int(time.time() * 1000) - span_ms
    pg_ms, pq_ms = [], []
    written = 0
    batch_no = 0
    while written < n:
        size = min(BATCH, n - written)
        batch = [gen.next(now_ms=start_ms + (written + i) * span_ms // n) for i in range(size)]
        writes = [batch]
        if redeliver_every and batch_no % redeliver_every == redeliver_every - 1:
            writes.append(batch)  # the consumer crashed before committing, the same poll arrives again
        for b in writes:
            t0 = time.perf_counter()
            pg.write(b)
            pg_ms.append((time.perf_counter() - t0) * 1000)
            t0 = time.perf_counter()
            pq_sink.write(b)
            pq_ms.append((time.perf_counter() - t0) * 1000)
        written += size
        batch_no += 1
        if batch_no % 500 == 0:
            print(f"  ingested {written:,}/{n:,}", flush=True)
    pg.close()

    def summarize(ms: list[float]) -> dict:
        ms = sorted(ms)
        return {
            "calls": len(ms), "total_s": sum(ms) / 1000, "mean_ms": sum(ms) / len(ms),
            "p50_ms": ms[len(ms) // 2], "p99_ms": ms[int(len(ms) * 0.99)],
            "events_per_s_of_sink_time": written / (sum(ms) / 1000),
        }

    return {"events": written, "batch": BATCH, "postgres": summarize(pg_ms), "parquet": summarize(pq_ms),
            "redelivered_batches": batch_no // redeliver_every if redeliver_every else 0}


def run(args: argparse.Namespace) -> dict:
    root = Path(args.dir)
    shutil.rmtree(root, ignore_errors=True)
    print(f"ingesting {args.events:,} events into Postgres and Parquet ...", flush=True)
    out: dict = {"threads": args.threads, "catalog": config.catalog_source(), "num_products": config.num_products()}
    out["ingest"] = ingest(args.events, args.days, root, args.redeliver_every)

    with db.connect(autocommit=True) as conn:
        conn.execute("VACUUM ANALYZE events")
        conn.execute("VACUUM ANALYZE product_stats")
        out["postgres_bytes"] = {
            "table": conn.execute("SELECT pg_relation_size('events')").fetchone()[0],
            "indexes": conn.execute("SELECT pg_indexes_size('events')").fetchone()[0],
            "total": conn.execute("SELECT pg_total_relation_size('events')").fetchone()[0],
        }
        out["postgres_rows"] = conn.execute("SELECT count(*) FROM events").fetchone()[0]

    duck = connect(args.threads)
    small_files = len(list((root / "events").glob("dt=*/*.parquet")))
    out["parquet_small"] = {
        "files": small_files, "bytes": dir_bytes(root / "events"),
        "rows_raw": duck.execute(f"SELECT count(*) FROM {read_events(root, dedupe=False)}").fetchone()[0],
        "rows_deduped": duck.execute(f"SELECT count(*) FROM {read_events(root)}").fetchone()[0],
    }
    print("compacting ...", flush=True)
    out["compaction"] = compact(root, args.threads)
    out["parquet_compacted"] = {"files": out["compaction"]["files_after"], "bytes": dir_bytes(root / "compacted")}

    sources = {
        "duckdb_small_files": read_events(root, dedupe=False),
        "duckdb_small_files_deduped": read_events(root),
        "duckdb_compacted": read_events(root, compacted=True, dedupe=False),
    }
    out["queries"] = {}
    with db.connect(autocommit=True) as conn:
        for name, q in QUERIES.items():
            print(f"query {name} ...", flush=True)
            row = {"label": q["label"]}
            row["postgres"] = timed(lambda: conn.execute(q["pg"]).fetchall(), args.repeat)
            for label, src in sources.items():
                if name == "distinct_users" and label == "duckdb_small_files_deduped":
                    continue
                row[label] = timed(lambda: duck.execute(q["duck"].format(src=src)).fetchall(), args.repeat)
            row["agree"] = _agree(name, row)
            for engine in [v for v in row.values() if isinstance(v, dict)]:
                engine.pop("result", None)
            out["queries"][name] = row
    return out


def _agree(name: str, row: dict) -> bool:
    """Do the engines return the same answer? (Ignoring float formatting.)"""
    def norm(rows):
        return [tuple(round(float(v), 2) if hasattr(v, "as_tuple") or isinstance(v, float) else str(v) for v in r) for r in rows]

    base = norm(row["postgres"]["result"])
    return all(norm(v["result"]) == base for k, v in row.items() if k.startswith("duckdb_") and k != "duckdb_small_files")


def markdown(out: dict) -> str:
    mb = lambda b: f"{b / 1e6:,.0f} MB"  # noqa: E731
    ing = out["ingest"]
    lines = [
        f"# Storage comparison: Postgres vs Parquet + DuckDB ({ing['events']:,} events)",
        "",
        f"Batches of {ing['batch']} events (what one consumer poll hands to a sink). DuckDB threads: {out['threads']}.",
        f"{ing['redelivered_batches']} batches were delivered twice on purpose (consumer crash before commit).",
        "",
        "## Ingest",
        "",
        "| Sink | Mean per batch | p99 per batch | Throughput of the sink alone | Rows after redelivery |",
        "|---|---:|---:|---:|---:|",
        f"| Postgres (batched, idempotent) | {ing['postgres']['mean_ms']:.1f} ms | {ing['postgres']['p99_ms']:.1f} ms | "
        f"{ing['postgres']['events_per_s_of_sink_time']:,.0f}/s | {out['postgres_rows']:,} (exactly once) |",
        f"| Parquet (one file per batch) | {ing['parquet']['mean_ms']:.1f} ms | {ing['parquet']['p99_ms']:.1f} ms | "
        f"{ing['parquet']['events_per_s_of_sink_time']:,.0f}/s | {out['parquet_small']['rows_raw']:,} raw, "
        f"{out['parquet_small']['rows_deduped']:,} after dedupe on read |",
        "",
        "## Disk",
        "",
        "| | Size | Files |",
        "|---|---:|---:|",
        f"| Postgres table | {mb(out['postgres_bytes']['table'])} | |",
        f"| Postgres indexes | {mb(out['postgres_bytes']['indexes'])} | |",
        f"| Postgres total | {mb(out['postgres_bytes']['total'])} | |",
        f"| Parquet, small files | {mb(out['parquet_small']['bytes'])} | {out['parquet_small']['files']:,} |",
        f"| Parquet, compacted | {mb(out['parquet_compacted']['bytes'])} | {out['parquet_compacted']['files']} |",
        "",
        f"Compaction (merge, dedupe, sort by product and time) took {out['compaction']['seconds']:.1f} s and removed "
        f"{out['compaction']['rows_before'] - out['compaction']['rows_after']:,} duplicate rows.",
        "",
        "## Queries (median of repeated warm runs, milliseconds)",
        "",
        "| Query | Postgres | DuckDB, small files | DuckDB, small files + dedupe | DuckDB, compacted | Same answer |",
        "|---|---:|---:|---:|---:|:--:|",
    ]
    for q in out["queries"].values():
        cell = lambda k: (f"{q[k]['median_ms']:,.0f}" if q[k]["median_ms"] >= 10 else f"{q[k]['median_ms']:.1f}") if k in q else "n/a"  # noqa: E731
        lines.append(
            f"| {q['label']} | {cell('postgres')} | {cell('duckdb_small_files')} | "
            f"{cell('duckdb_small_files_deduped')} | {cell('duckdb_compacted')} | {'yes' if q['agree'] else 'NO'} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=2_000_000)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--redeliver-every", type=int, default=50, help="every Nth batch is delivered twice (0 = never)")
    parser.add_argument("--dir", default="data/storage-bench", help="where the Parquet files are written")
    parser.add_argument("--out", default="results-storage")
    args = parser.parse_args()
    out = run(args)
    results = Path(args.out)
    results.mkdir(exist_ok=True)
    (results / "storage.json").write_text(json.dumps(out, indent=2, default=str))
    (results / "summary.md").write_text(markdown(out))
    print(markdown(out))


if __name__ == "__main__":
    main()
