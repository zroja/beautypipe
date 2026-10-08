"""Columnar sink: events as Parquet files, queried with DuckDB.

    pip install 'beautypipe[columnar]'
    python -m beautypipe.consumer --topic beauty.events --sink parquet     # writes to $PARQUET_DIR

Layout (hive style, one directory per event date):

    $PARQUET_DIR/events/dt=2026-10-08/part-<epoch_ms>-<random>.parquet      one file per consumer poll
    $PARQUET_DIR/compacted/dt=2026-10-08/part-0.parquet                   after `compact`, sorted by product

Delivery semantics differ from the Postgres sinks, on purpose, and this is the trade-off worth talking about:

* `write()` is durable when it returns (file written to a temp name, then renamed), so the consumer can
  commit its offsets afterwards and nothing is lost.
* There is no unique constraint. A redelivered batch becomes a second file containing the same event_ids,
  so the sink is at-least-once. Readers must deduplicate on event_id (`read_events(dedupe=True)`), and
  `compact` removes duplicates for good.
* One file per poll is many small files. They are cheap to write and expensive to read; `compact` merges
  them into a few large files sorted by (product_id, event_ts) so that row-group statistics can skip data.
"""

import os
import secrets
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .events import Event

SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("product_id", pa.int32()),
        ("shade_id", pa.int32()),
        ("user_id", pa.int32()),
        ("rating", pa.int16()),
        ("event_ts", pa.timestamp("us", tz="UTC")),
        ("channel", pa.string()),
    ]
)


def root_dir() -> Path:
    return Path(os.environ.get("PARQUET_DIR", "parquet-out"))


def _table(events: list[Event]) -> pa.Table:
    return pa.table(
        {
            "event_id": [e.event_id for e in events],
            "event_type": [e.event_type for e in events],
            "product_id": [e.product_id for e in events],
            "shade_id": [e.shade_id for e in events],
            "user_id": [e.user_id for e in events],
            "rating": [e.rating for e in events],
            "event_ts": [e.produced_at_ms * 1000 for e in events],
            "channel": [e.channel for e in events],
        },
        schema=pa.schema([(f.name, pa.int64() if f.name == "event_ts" else f.type) for f in SCHEMA]),
    ).cast(SCHEMA)


class ParquetSink:
    def __init__(self, root: Path | None = None) -> None:
        self.root = (root or root_dir()) / "events"

    def write(self, events: list[Event]) -> None:
        if not events:
            return
        by_day: dict[str, list[Event]] = defaultdict(list)
        for e in events:
            by_day[datetime.fromtimestamp(e.produced_at_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")].append(e)
        for day, batch in by_day.items():
            folder = self.root / f"dt={day}"
            folder.mkdir(parents=True, exist_ok=True)
            name = f"part-{int(time.time() * 1000)}-{secrets.token_hex(4)}.parquet"
            tmp = folder / f".{name}.tmp"
            pq.write_table(_table(batch), tmp, compression="zstd")
            os.replace(tmp, folder / name)

    def close(self) -> None:
        return None


def events_glob(root: Path | None = None, compacted: bool = False) -> str:
    return str((root or root_dir()) / ("compacted" if compacted else "events") / "dt=*" / "*.parquet")


def connect(threads: int | None = None):
    import duckdb

    conn = duckdb.connect()
    if threads:
        conn.execute(f"SET threads = {int(threads)}")
    return conn


def read_events(root: Path | None = None, compacted: bool = False, dedupe: bool = True) -> str:
    """SQL source for the events, to be used in a FROM clause."""
    source = f"read_parquet('{events_glob(root, compacted)}', hive_partitioning = true)"
    if not dedupe:
        return source
    return f"(SELECT * EXCLUDE (rn) FROM (SELECT *, row_number() OVER (PARTITION BY event_id) AS rn FROM {source}) WHERE rn = 1)"


def compact(root: Path | None = None, threads: int | None = None) -> dict:
    """Merge the small files into one sorted, deduplicated file per day. Returns before/after counts."""
    base = root or root_dir()
    conn = connect(threads)
    before = conn.execute(f"SELECT count(*), count(DISTINCT event_id) FROM {read_events(base, dedupe=False)}").fetchone()
    files_before = len(list((base / "events").glob("dt=*/*.parquet")))
    started = time.perf_counter()
    for day_dir in sorted((base / "events").glob("dt=*")):
        out = base / "compacted" / day_dir.name
        out.mkdir(parents=True, exist_ok=True)
        tmp = out / ".part-0.parquet.tmp"
        conn.execute(
            f"""COPY (
                    SELECT event_id, event_type, product_id, shade_id, user_id, rating, event_ts, channel
                    FROM (SELECT *, row_number() OVER (PARTITION BY event_id) AS rn
                          FROM read_parquet('{day_dir}/*.parquet'))
                    WHERE rn = 1
                    ORDER BY product_id, event_ts
                ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)"""
        )
        os.replace(tmp, out / "part-0.parquet")
    seconds = time.perf_counter() - started
    after = conn.execute(f"SELECT count(*) FROM {read_events(base, compacted=True, dedupe=False)}").fetchone()[0]
    return {
        "rows_before": before[0], "distinct_before": before[1], "rows_after": after,
        "files_before": files_before, "files_after": len(list((base / "compacted").glob("dt=*/*.parquet"))),
        "seconds": seconds,
    }
