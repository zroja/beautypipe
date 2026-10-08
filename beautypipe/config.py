import os


def bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def dsn() -> str:
    return os.getenv("DATABASE_URL", "postgresql://beauty:beauty@localhost:5432/beauty")


NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", "5000"))
SHADES_PER_PRODUCT = int(os.getenv("SHADES_PER_PRODUCT", "6"))
PARTITIONS = int(os.getenv("PARTITIONS", "4"))


def catalog_source() -> str:
    """"synthetic" (generated, default) or "obf" (Open Beauty Facts makeup products)."""
    return os.getenv("CATALOG", "synthetic")


def num_products() -> int:
    if catalog_source() == "obf":
        from . import obf

        return len(obf.load())
    return NUM_PRODUCTS


def shades_per_product() -> int:
    return 1 if catalog_source() == "obf" else SHADES_PER_PRODUCT
