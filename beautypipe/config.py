import os


def bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def dsn() -> str:
    return os.getenv("DATABASE_URL", "postgresql://beauty:beauty@localhost:5432/beauty")


NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", "5000"))
SHADES_PER_PRODUCT = int(os.getenv("SHADES_PER_PRODUCT", "6"))
PARTITIONS = int(os.getenv("PARTITIONS", "4"))
