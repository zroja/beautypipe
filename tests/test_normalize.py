import random

import pytest

from beautypipe.catalog import _mess_up
from beautypipe.normalize import normalize_shade


@pytest.mark.parametrize(
    "raw",
    ["Ruby Woo", "RUBY WOO", "ruby-woo 01", "Ruby Woo (Matte)", "  Rubý Woo  #3", "No. 12 Ruby Woo"],
)
def test_variants_collapse_to_one_key(raw):
    assert normalize_shade(raw) == "ruby woo"


def test_distinct_shades_stay_distinct():
    assert normalize_shade("Coral Fig") != normalize_shade("Coral Mocha")


def test_generated_mess_always_normalizes_back():
    rng = random.Random(0)
    for _ in range(500):
        assert normalize_shade(_mess_up("Honey Amber", rng)) == "honey amber"


def test_leading_codes_and_code_only_names():
    assert normalize_shade("230 Unleash The Drama") == "unleash the drama"
    assert normalize_shade("#1") == "1"


from beautypipe.catalog import extract_shade
from beautypipe.normalize import normalize_brand, product_key, split_ingredients


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Fit Me Dewy + Smooth Foundation SPF 18 310 Sun Beige", "310 Sun Beige"),
        ("Color Craze Purple Passion Nail Polish (No. 612)", "No. 612"),
        ("Hydro gel eye patches", None),
    ],
)
def test_extract_shade_from_product_name(name, expected):
    assert extract_shade(name) == expected


def test_ingredient_lists_are_split_normalized_and_deduplicated():
    parsed = split_ingredients("Ingredients: Aqua, Parfum/ Fragrance, CI 77491/CI 77492 (iron oxides), mica 2%, Water.")
    assert parsed == ["water", "fragrance", "ci 77491", "ci 77492", "mica"]


def test_duplicate_products_collapse_to_one_key():
    a = product_key("Couleurs Nature, Yves Rocher", "Poudre libre veloutée")
    b = product_key("couleurs nature", "POUDRE  LIBRE VELOUTEE")
    assert a == b
    assert normalize_brand("Dr. Bronner's, Others") == "dr bronner s"
