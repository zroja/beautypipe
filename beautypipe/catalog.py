"""Synthetic but deliberately messy beauty catalog (brands, products, shades)."""

import random
from dataclasses import dataclass

BRANDS = ["Lumière", "Rouge & Co", "Petal Lab", "Velvet Hour", "Dune Cosmetics", "Maison Éclat", "Orchid Row"]
CATEGORIES = ["lipstick", "foundation", "blush", "eyeshadow", "concealer", "highlighter"]
SHADE_WORDS = [
    "Ruby", "Woo", "Sienna", "Dusk", "Peony", "Mocha", "Ivory", "Honey", "Rosewood", "Coral",
    "Berry", "Nude", "Amber", "Fig", "Blush", "Terracotta", "Mauve", "Cinnamon", "Pearl", "Espresso",
]
FINISHES = ["Matte", "Satin", "Dewy", "Shimmer", "Velvet"]


@dataclass(frozen=True)
class Shade:
    shade_id: int
    product_id: int
    raw_name: str
    hex: str


@dataclass(frozen=True)
class Product:
    product_id: int
    brand: str
    raw_name: str
    category: str


def _mess_up(base: str, rng: random.Random) -> str:
    """Return one of the many ways retailers write the same shade."""
    variant = rng.randrange(6)
    if variant == 0:
        return base
    if variant == 1:
        return base.upper()
    if variant == 2:
        return base.replace(" ", "-").lower() + f" {rng.randrange(1, 40):02d}"
    if variant == 3:
        return f"{base} ({rng.choice(FINISHES)})"
    if variant == 4:
        return f"  {base.replace('e', 'é', 1)}  #{rng.randrange(1, 40)}"
    return f"No. {rng.randrange(1, 40)} {base}"


def build_catalog(num_products: int, shades_per_product: int, seed: int = 7):
    rng = random.Random(seed)
    products: list[Product] = []
    shades: list[Shade] = []
    for pid in range(1, num_products + 1):
        brand = rng.choice(BRANDS)
        category = rng.choice(CATEGORIES)
        products.append(Product(pid, brand, f"{brand} {category.title()} {pid:05d}", category))
        base_names = rng.sample(SHADE_WORDS, k=min(shades_per_product, len(SHADE_WORDS)))
        for k, word in enumerate(base_names):
            base = f"{word} {rng.choice(SHADE_WORDS)}"
            shades.append(Shade(pid * 10 + k, pid, _mess_up(base, rng), f"#{rng.randrange(0x1000000):06x}"))
    return products, shades
