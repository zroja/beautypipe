"""Postgres schema and validation queries."""

import psycopg

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id    text PRIMARY KEY,
    event_type  text NOT NULL,
    product_id  integer NOT NULL,
    shade_id    integer NOT NULL,
    user_id     integer NOT NULL,
    rating      smallint,
    event_ts    timestamptz NOT NULL,
    channel     text
);
ALTER TABLE events ADD COLUMN IF NOT EXISTS channel text;
CREATE INDEX IF NOT EXISTS events_product_ts ON events (product_id, event_ts);
CREATE INDEX IF NOT EXISTS events_shade ON events (shade_id);
CREATE INDEX IF NOT EXISTS events_ts ON events (event_ts);

CREATE TABLE IF NOT EXISTS product_stats (
    product_id    integer PRIMARY KEY,
    review_count  bigint NOT NULL DEFAULT 0,
    rating_sum    bigint NOT NULL DEFAULT 0,
    search_count  bigint NOT NULL DEFAULT 0,
    view_count    bigint NOT NULL DEFAULT 0,
    last_event_ts timestamptz
);

CREATE TABLE IF NOT EXISTS products (
    product_id     integer PRIMARY KEY,
    brand          text NOT NULL,
    raw_name       text NOT NULL,
    category       text NOT NULL,
    ingredients_text text,
    canonical_key  text,
    refreshed_at   timestamptz
);
ALTER TABLE products ADD COLUMN IF NOT EXISTS ingredients_text text;
ALTER TABLE products ADD COLUMN IF NOT EXISTS canonical_key text;

CREATE TABLE IF NOT EXISTS product_ingredients (
    product_id  integer NOT NULL,
    position    integer NOT NULL,
    normalized  text NOT NULL,
    PRIMARY KEY (product_id, position)
);

CREATE TABLE IF NOT EXISTS shades (
    shade_id        integer PRIMARY KEY,
    product_id      integer NOT NULL,
    raw_name        text NOT NULL,
    canonical_name  text,
    hex             text NOT NULL,
    refreshed_at    timestamptz
);
"""


def connect(**kwargs) -> psycopg.Connection:
    return psycopg.connect(config.dsn(), **kwargs)


def init_schema() -> None:
    with connect(autocommit=True) as conn:
        conn.execute(SCHEMA)


def reset_streaming_tables() -> None:
    with connect(autocommit=True) as conn:
        conn.execute("TRUNCATE events, product_stats")
        conn.execute("VACUUM ANALYZE events")


_VALIDATE_SQL = """
WITH recount AS (
    SELECT product_id,
           count(*) FILTER (WHERE event_type = 'review') AS review_count,
           coalesce(sum(rating) FILTER (WHERE event_type = 'review'), 0) AS rating_sum,
           count(*) FILTER (WHERE event_type = 'search') AS search_count,
           count(*) FILTER (WHERE event_type = 'view') AS view_count
    FROM events GROUP BY product_id
)
SELECT count(*) FROM recount r FULL JOIN product_stats p ON p.product_id = r.product_id
WHERE r.review_count IS DISTINCT FROM p.review_count
   OR r.rating_sum   IS DISTINCT FROM p.rating_sum
   OR r.search_count IS DISTINCT FROM p.search_count
   OR r.view_count   IS DISTINCT FROM p.view_count
"""


def validate() -> dict:
    """Check that the incrementally maintained rollup matches a recount of the raw events."""
    with connect() as conn:
        events = conn.execute("SELECT count(*) FROM events").fetchone()[0]
        stats_total = conn.execute(
            "SELECT coalesce(sum(review_count + search_count + view_count), 0) FROM product_stats"
        ).fetchone()[0]
        mismatched = conn.execute(_VALIDATE_SQL).fetchone()[0]
    return {"events": events, "stats_total": int(stats_total), "mismatched_products": mismatched}
