# Data attribution

`obf-makeup.jsonl.gz` is a filtered extract of [Open Beauty Facts](https://world.openbeautyfacts.org),
a collaborative, open database of cosmetic products.

- Source file: https://static.openbeautyfacts.org/data/en.openbeautyfacts.org.products.csv
- Filter: products with a name and a brand whose categories or name mention makeup
  (lipstick, foundation, mascara, nail polish, and similar), excluding removers, hair products and soaps.
  Fields kept: barcode, name, brand, most specific makeup category, ingredients text (first 2,000 characters).
- Licence: the database is available under the [Open Database License (ODbL) v1.0](https://opendatacommons.org/licenses/odbl/1-0/),
  individual contents under the [Database Contents License](https://opendatacommons.org/licenses/dbcl/1-0/).
  Any public use of this extract must keep this attribution, and derived databases must stay under the ODbL.
- Rebuild it from the current export with `python -m beautypipe.obf build`.

The event data in this project is synthetic. Only the product catalog is real.
