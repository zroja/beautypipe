import time

from confluent_kafka.admin import AdminClient, NewTopic

from . import config


def admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": config.bootstrap()})


def create_topic(name: str, partitions: int | None = None) -> None:
    client = admin()
    futures = client.create_topics(
        [NewTopic(name, num_partitions=partitions or config.PARTITIONS, replication_factor=1)]
    )
    for fut in futures.values():
        fut.result(timeout=30)
    time.sleep(0.5)


def delete_topic(name: str) -> None:
    client = admin()
    for fut in client.delete_topics([name], operation_timeout=30).values():
        try:
            fut.result(timeout=30)
        except Exception:
            pass
