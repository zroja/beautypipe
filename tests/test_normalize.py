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
