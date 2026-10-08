"""Open Beauty Facts makeup catalog.

Source: https://world.openbeautyfacts.org (data licensed under the Open Database License, ODbL;
see data/ATTRIBUTION.md). A small filtered extract is shipped inside the package so that nothing
has to be downloaded to run the pipeline. To rebuild it from the full export:

    python -m beautypipe.obf build
"""

import argparse
import csv
import gzip
import json
import re
import urllib.request
from pathlib import Path

SOURCE_URL = "https://static.openbeautyfacts.org/data/en.openbeautyfacts.org.products.csv"
PACKAGED = Path(__file__).resolve().parent / "data" / "obf-makeup.jsonl.gz"
CACHE = Path(__file__).resolve().parent.parent / "data" / "obf.csv"

MAKEUP = re.compile(
    r"makeup|make-up|lipstick|lip-gloss|lip-balm|foundation|mascara|eye-?shadow|eyeliner|nail-polish|blush|"
    r"concealer|face-powder|bronzer|highlighter|eyebrow|lip-liner|rouges?-a-levres|vernis",
    re.I,
)
EXCLUDE = re.compile(r"remover|hair|shampoo|conditioner|soap|shower", re.I)


def download(dest: Path = CACHE) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(SOURCE_URL, dest)
    return dest


def _best_category(tags: str) -> str:
    makeup_tags = [t for t in tags.split(",") if MAKEUP.search(t)]
    return (makeup_tags[-1] if makeup_tags else "makeup").split(":")[-1]


def build(csv_path: Path = CACHE) -> int:
    csv.field_size_limit(10**8)
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            tags, name, brand = r["categories_tags"] or "", (r["product_name"] or "").strip(), (r["brands"] or "").strip()
            if not name or not brand:
                continue
            if not (MAKEUP.search(tags) or MAKEUP.search(name)) or EXCLUDE.search(tags) or EXCLUDE.search(name):
                continue
            rows.append(
                {
                    "code": r["code"],
                    "name": name,
                    "brand": brand,
                    "category": _best_category(tags),
                    "ingredients_text": (r["ingredients_text"] or "").strip()[:2000],
                }
            )
    rows.sort(key=lambda x: x["code"])
    PACKAGED.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(PACKAGED, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return len(rows)


def load() -> list[dict]:
    with gzip.open(PACKAGED, "rt", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("download")
    sub.add_parser("build")
    args = parser.parse_args()
    if args.cmd == "download":
        print(download())
    else:
        if not CACHE.exists():
            download()
        print(f"wrote {build()} products to {PACKAGED}")


if __name__ == "__main__":
    main()
