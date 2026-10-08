"""Scheduled batch job: seeds the catalog and periodically refreshes the dimension tables.

The refresh normalizes shade names, collapses duplicate products (same product entered with
different spelling or casing) and rebuilds the normalized ingredient lists, all in one
transaction. It is the kind of heavy, time-driven write that shares a database with a live stream.

    python -m beautypipe.batchjob seed                  # synthetic catalog (default)
    CATALOG=obf python -m beautypipe.batchjob seed      # Open Beauty Facts makeup products
    python -m beautypipe.batchjob refresh
    python -m beautypipe.batchjob loop --every 8 --log out/batch.jsonl
"""

import argparse
import json
import signal
import time

from . import config, db
from .catalog import get_catalog
from .normalize import normalize_shade, product_key, split_ingredients


def seed() -> None:
    db.init_schema()
    catalog = get_catalog()
    with db.connect() as conn:
        conn.execute("TRUNCATE products, shades, product_ingredients")
        with conn.cursor() as cur:
            with cur.copy(
                "COPY products (product_id, brand, raw_name, category, ingredients_text) FROM STDIN"
            ) as copy:
                for p in catalog.products:
                    copy.write_row((p.product_id, p.brand, p.raw_name, p.category, p.ingredients_text or None))
            with cur.copy("COPY shades (shade_id, product_id, raw_name, hex) FROM STDIN") as copy:
                for s in catalog.shades:
                    copy.write_row((s.shade_id, s.product_id, s.raw_name, s.hex))
        conn.commit()
    print(f"seeded {len(catalog.products)} products, {len(catalog.shades)} shades ({config.catalog_source()} catalog)")


def refresh() -> dict:
    """Normalize shades, products and ingredients, and rewrite the dimension rows in one transaction."""
    started = time.perf_counter()
    with db.connect() as conn:
        shades = conn.execute("SELECT shade_id, raw_name FROM shades").fetchall()
        shade_ids = [r[0] for r in shades]
        shade_names = [normalize_shade(r[1]) for r in shades]
        conn.execute(
            """
            UPDATE shades s SET canonical_name = v.canonical_name, refreshed_at = now()
            FROM unnest(%s::int[], %s::text[]) AS v(shade_id, canonical_name)
            WHERE s.shade_id = v.shade_id
            """,
            (shade_ids, shade_names),
        )

        products = conn.execute("SELECT product_id, brand, raw_name, ingredients_text FROM products").fetchall()
        product_ids = [r[0] for r in products]
        keys = [product_key(r[1], r[2]) for r in products]
        conn.execute(
            """
            UPDATE products p SET canonical_key = v.canonical_key, refreshed_at = now()
            FROM unnest(%s::int[], %s::text[]) AS v(product_id, canonical_key)
            WHERE p.product_id = v.product_id
            """,
            (product_ids, keys),
        )

        ingredient_rows = [(r[0], pos, name) for r in products if r[3] for pos, name in enumerate(split_ingredients(r[3]))]
        conn.execute("DELETE FROM product_ingredients")
        if ingredient_rows:
            conn.execute(
                "INSERT INTO product_ingredients (product_id, position, normalized) "
                "SELECT * FROM unnest(%s::int[], %s::int[], %s::text[])",
                ([r[0] for r in ingredient_rows], [r[1] for r in ingredient_rows], [r[2] for r in ingredient_rows]),
            )
        conn.commit()
    return {
        "ts": time.time(),
        "shades": len(shade_ids),
        "distinct_canonical_shades": len(set(shade_names)),
        "products": len(product_ids),
        "distinct_products": len(set(keys)),
        "ingredient_rows": len(ingredient_rows),
        "distinct_ingredients": len({r[2] for r in ingredient_rows}),
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
