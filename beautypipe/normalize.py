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
_LEADING_CODE = re.compile(r"^\d{1,3}\s+(?=[a-z])")


def normalize_shade(raw: str) -> str:
    text = unicodedata.normalize("NFKD", raw)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = _PAREN.sub(" ", text.lower())
    text = _NON_ALNUM.sub(" ", text).strip()
    text = _HASH_CODE.sub("", text).strip()
    text = _LEADING_NO.sub("", text)
    text = _TRAILING_CODE.sub("", text)
    text = _LEADING_CODE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or re.sub(r"[^a-z0-9]+", " ", raw.lower()).strip()


_INGREDIENT_ALIASES = {
    "aqua": "water",
    "eau": "water",
    "water": "water",
    "parfum": "fragrance",
    "perfume": "fragrance",
    "fragrance": "fragrance",
    "cera alba": "beeswax",
    "mica": "mica",
}
_INGREDIENT_SPLIT = re.compile(r"[,;•\n\r]+")
_PERCENT = re.compile(r"\b\d+([.,]\d+)?\s*%")
_TRAILING_NOISE = re.compile(r"[\s.*†‡]+$")
_LEADING_LABEL = re.compile(r"^\s*(?:ingredients?|ingr[eé]dients?|inci)\s*:\s*", re.I)


def normalize_ingredient(raw: str) -> str:
    text = unicodedata.normalize("NFKD", raw)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = _PERCENT.sub(" ", text.lower())
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[^a-z0-9/ \-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -")
    text = re.sub(r"^ci\s*(\d{5})$", r"ci \1", text)
    return _INGREDIENT_ALIASES.get(text, text)


def split_ingredients(text: str) -> list[str]:
    """Split an ingredient list as printed on a label into normalized, de-duplicated ingredients.

    "Parfum/Fragrance" and "CI 77491/CI 77492" are slash-joined variants; each part is normalized
    on its own and repeats within one product are dropped.
    """
    seen: dict[str, None] = {}
    for chunk in _INGREDIENT_SPLIT.split(_LEADING_LABEL.sub("", text)):
        chunk = _TRAILING_NOISE.sub("", chunk)
        for part in chunk.split("/"):
            name = normalize_ingredient(part)
            if name and len(name) > 1:
                seen.setdefault(name, None)
    return list(seen)


def normalize_brand(raw: str) -> str:
    """First brand of a possibly multi-brand string ("Couleurs Nature, Yves Rocher"), canonicalized."""
    first = raw.split(",")[0]
    text = unicodedata.normalize("NFKD", first)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text.lower())).strip()


def product_key(brand: str, name: str) -> str:
    """Key that collapses the same product entered several times with different spelling or casing."""
    text = unicodedata.normalize("NFKD", name)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    name_key = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text.lower())).strip()
    return f"{normalize_brand(brand)}|{name_key}"
