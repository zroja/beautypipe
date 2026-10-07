"""Scheduled batch job: seeds the catalog and periodically refreshes the dimension tables.

The refresh rewrites every product and shade row in one transaction (normalizing shade
names), which is the kind of heavy, time-driven write that shares a database with a live
stream.

    python -m beautypipe.batchjob seed
    python -m beautypipe.batchjob refresh
    python -m beautypipe.batchjob loop --every 8 --log out/batch.jsonl
"""

import argparse
import json
import signal
import time

from . import config, db
from .catalog import build_catalog
from .normalize import normalize_shade


def seed() -> None:
    db.init_schema()
    products, shades = build_catalog(config.NUM_PRODUCTS, config.SHADES_PER_PRODUCT)
    with db.connect() as conn:
        conn.execute("TRUNCATE products, shades")
        with conn.cursor() as cur:
            with cur.copy("COPY products (product_id, brand, raw_name, category) FROM STDIN") as copy:
                for p in products:
                    copy.write_row((p.product_id, p.brand, p.raw_name, p.category))
            with cur.copy("COPY shades (shade_id, product_id, raw_name, hex) FROM STDIN") as copy:
                for s in shades:
                    copy.write_row((s.shade_id, s.product_id, s.raw_name, s.hex))
        conn.commit()
    print(f"seeded {len(products)} products, {len(shades)} shades")


def refresh() -> dict:
    """Normalize shade names and rewrite all dimension rows in a single transaction."""
    started = time.perf_counter()
    with db.connect() as conn:
        rows = conn.execute("SELECT shade_id, raw_name FROM shades").fetchall()
        ids = [r[0] for r in rows]
        canon = [normalize_shade(r[1]) for r in rows]
        conn.execute(
            """
            UPDATE shades s SET canonical_name = v.canonical_name, refreshed_at = now()
            FROM unnest(%s::int[], %s::text[]) AS v(shade_id, canonical_name)
            WHERE s.shade_id = v.shade_id
            """,
            (ids, canon),
        )
        conn.execute("UPDATE products SET refreshed_at = now()")
        conn.commit()
    return {
        "ts": time.time(),
        "shades": len(ids),
        "distinct_canonical": len(set(canon)),
        "duration_s": time.perf_counter() - started,
    }


def loop(every: float, log_path: str | None) -> None:
    stop = False

    def handle_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    log = open(log_path, "w") if log_path else None
    try:
        while not stop:
            result = refresh()
            print(f"refresh took {result['duration_s']:.2f}s", flush=True)
            if log:
                log.write(json.dumps(result) + "\n")
                log.flush()
            deadline = time.time() + every
            while not stop and time.time() < deadline:
                time.sleep(0.2)
    finally:
        if log:
            log.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("seed")
    sub.add_parser("refresh")
    p_loop = sub.add_parser("loop")
    p_loop.add_argument("--every", type=float, default=8.0)
    p_loop.add_argument("--log")
    args = parser.parse_args()
    if args.cmd == "seed":
        seed()
    elif args.cmd == "refresh":
        print(refresh())
    else:
        loop(args.every, args.log)


if __name__ == "__main__":
    main()
