"""Shade-name normalization: the "messy human data" part of the pipeline.

Different retailers write the same shade as "Ruby Woo (Matte)", "RUBY-WOO 01",
"Rubý  Woo #3", and so on. The batch job maps all of those to one canonical key.
"""

import re
import unicodedata

_PAREN = re.compile(r"\([^)]*\)")
_NON_ALNUM = re.compile(r"[^a-z0-9#]+")
_LEADING_NO = re.compile(r"^no\s+\d{1,3}\s+")
_HASH_CODE = re.compile(r"#\s*\d{1,3}")
_TRAILING_CODE = re.compile(r"\s+\d{1,3}$")


def normalize_shade(raw: str) -> str:
    text = unicodedata.normalize("NFKD", raw)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = _PAREN.sub(" ", text.lower())
    text = _NON_ALNUM.sub(" ", text).strip()
    text = _HASH_CODE.sub("", text).strip()
    text = _LEADING_NO.sub("", text)
    text = _TRAILING_CODE.sub("", text)
    return re.sub(r"\s+", " ", text).strip()
