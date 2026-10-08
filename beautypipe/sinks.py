"""Sinks the consumer can write to.

All Postgres sinks are idempotent: events are inserted with ON CONFLICT DO NOTHING and the
rollup is only incremented for rows that were actually inserted. Redelivery or a full replay
therefore never double counts.
"""

from datetime import datetime, timezone
from typing import Protocol

from . import db
from .events import Event


class Sink(Protocol):
    def write(self, events: list[Event]) -> None: ...
    def close(self) -> None: ...


class BlackholeSink:
    """Does nothing. Measures the ceiling of Kafka + Python deserialization alone."""

    def write(self, events: list[Event]) -> None:
        return None

    def close(self) -> None:
        return None


def _ts(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


_INSERT_ONE = """
INSERT INTO events (event_id, event_type, product_id, shade_id, user_id, rating, event_ts, channel)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
RETURNING 1
"""

_UPSERT_STATS_ONE = """
INSERT INTO product_stats (product_id, review_count, rating_sum, search_count, view_count, last_event_ts)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (product_id) DO UPDATE SET
    review_count  = product_stats.review_count  + EXCLUDED.review_count,
    rating_sum    = product_stats.rating_sum    + EXCLUDED.rating_sum,
    search_count  = product_stats.search_count  + EXCLUDED.search_count,
    view_count    = product_stats.view_count    + EXCLUDED.view_count,
    last_event_ts = greatest(product_stats.last_event_ts, EXCLUDED.last_event_ts)
"""


class PostgresNaiveSink:
    """One transaction per event: insert the event, bump the hot rollup row, commit.

    This is how many first versions of a consumer are written. Every event pays for
    three network round trips, a WAL flush and a lock on a (possibly hot) rollup row.
    """

    def __init__(self) -> None:
        self._conn = db.connect()

    def write(self, events: list[Event]) -> None:
        conn = self._conn
        for e in events:
            ts = _ts(e.produced_at_ms)
            inserted = conn.execute(
                _INSERT_ONE, (e.event_id, e.event_type, e.product_id, e.shade_id, e.user_id, e.rating, ts, e.channel)
            ).fetchone()
            if inserted:
                is_review = e.event_type == "review"
                conn.execute(
                    _UPSERT_STATS_ONE,
                    (
                        e.product_id,
                        1 if is_review else 0,
                        e.rating or 0,
                        1 if e.event_type == "search" else 0,
                        1 if e.event_type == "view" else 0,
                        ts,
                    ),
                )
            conn.commit()

    def close(self) -> None:
        self._conn.close()


_BATCH_SQL = """
WITH inserted AS (
    INSERT INTO events (event_id, event_type, product_id, shade_id, user_id, rating, event_ts, channel)
    SELECT * FROM unnest(
        %(ids)s::text[], %(types)s::text[], %(products)s::int[], %(shades)s::int[],
        %(users)s::int[], %(ratings)s::smallint[], %(ts)s::timestamptz[], %(channels)s::text[]
    )
    ON CONFLICT (event_id) DO NOTHING
    RETURNING product_id, event_type, rating, event_ts
)
INSERT INTO product_stats (product_id, review_count, rating_sum, search_count, view_count, last_event_ts)
SELECT product_id,
       count(*) FILTER (WHERE event_type = 'review'),
       coalesce(sum(rating) FILTER (WHERE event_type = 'review'), 0),
       count(*) FILTER (WHERE event_type = 'search'),
       count(*) FILTER (WHERE event_type = 'view'),
       max(event_ts)
FROM inserted
GROUP BY product_id
ORDER BY product_id
ON CONFLICT (product_id) DO UPDATE SET
    review_count  = product_stats.review_count  + EXCLUDED.review_count,
    rating_sum    = product_stats.rating_sum    + EXCLUDED.rating_sum,
    search_count  = product_stats.search_count  + EXCLUDED.search_count,
    view_count    = product_stats.view_count    + EXCLUDED.view_count,
    last_event_ts = greatest(product_stats.last_event_ts, EXCLUDED.last_event_ts)
"""


class PostgresBatchedSink:
    """One transaction per poll: a single set-based statement inserts the events and
    applies the rollup deltas, grouped per product, for rows that were newly inserted."""

    def __init__(self) -> None:
        self._conn = db.connect()

    def write(self, events: list[Event]) -> None:
        if not events:
            return
        params = {
            "ids": [e.event_id for e in events],
            "types": [e.event_type for e in events],
            "products": [e.product_id for e in events],
            "shades": [e.shade_id for e in events],
            "users": [e.user_id for e in events],
            "ratings": [e.rating for e in events],
            "ts": [_ts(e.produced_at_ms) for e in events],
            "channels": [e.channel for e in events],
        }
        self._conn.execute(_BATCH_SQL, params)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


def _parquet_sink() -> Sink:
    from .parquet_sink import ParquetSink  # optional dependency: pip install 'beautypipe[columnar]'

    return ParquetSink()


SINKS = {
    "blackhole": BlackholeSink,
    "pg-naive": PostgresNaiveSink,
    "pg-batched": PostgresBatchedSink,
    "parquet": _parquet_sink,
}


def make_sink(name: str) -> Sink:
    return SINKS[name]()
