"""Kubernetes experiments. Needs the kind cluster from scripts/k8s-up.sh.

    python -m beautypipe.k8s_bench --list
    python -m beautypipe.k8s_bench --experiments pod-kill
    python -m beautypipe.k8s_bench                    # everything

The producer and the sampler run on the host and reach Redpanda and Postgres through NodePorts.
The consumers run as a Deployment inside the cluster. Metrics are taken from outside the
consumers (Postgres statistics, kubectl, Postgres container cgroup) so that measuring never
slows the pipeline down. Per-window consumer metrics are read back from the pod logs.
"""

import argparse
import json
import os
import subprocess
import threading
import time
from pathlib import Path
from string import Template

from . import config, db, dlq, kafkautil
from .producer import produce

K8S_DIR = Path(__file__).resolve().parent.parent / "k8s"
PARTITIONS = 8

# name -> settings. Each experiment answers one question; see README.
EXPERIMENTS = {
    "pod-delete": dict(
        question="Graceful pod deletion (deploy or node drain): any loss or double counting?",
        sink="pg-batched", replicas=1, keda=False, pg_cpu="2", rate=2000, duration=45, kill_at=15, kill_mode="delete",
    ),
    "pod-crash": dict(
        question="Hard crash (SIGKILL) of the consumer container: any loss or double counting?",
        sink="pg-batched", replicas=1, keda=False, pg_cpu="2", rate=2000, duration=45, kill_at=15, kill_mode="crash",
    ),
    "naive-fixed": dict(
        question="Per-event sink, 1 consumer pod, 3,000 events/s",
        sink="pg-naive", replicas=1, keda=False, pg_cpu="2", rate=3000, duration=60, kill_at=None,
    ),
    "naive-keda": dict(
        question="Per-event sink, KEDA scales consumers on Kafka lag (1 to 8 pods)",
        sink="pg-naive", replicas=1, keda=True, pg_cpu="2", rate=3000, duration=60, kill_at=None,
    ),
    "batched-fixed": dict(
        question="Batched sink, 1 consumer pod, 3,000 events/s",
        sink="pg-batched", replicas=1, keda=False, pg_cpu="2", rate=3000, duration=60, kill_at=None,
    ),
    "pgcpu-full": dict(
        question="Batched sink, Postgres with 2 CPUs, 1 consumer, 4,000 events/s",
        sink="pg-batched", replicas=1, keda=False, pg_cpu="2", rate=4000, duration=45, kill_at=None,
    ),
    "pgcpu-limited": dict(
        question="Batched sink, Postgres limited to 0.05 CPU, 1 consumer, 4,000 events/s",
        sink="pg-batched", replicas=1, keda=False, pg_cpu="50m", rate=4000, duration=45, kill_at=None,
    ),
    "pgcpu-limited-keda": dict(
        question="Batched sink, Postgres limited to 0.05 CPU, KEDA scales consumers on lag",
        sink="pg-batched", replicas=1, keda=True, pg_cpu="50m", rate=4000, duration=45, kill_at=None,
    ),
    # Schema change and malformed records. Gentler rate: the point is correctness, not throughput.
    "schema-strict-fail": dict(
        question="Producer deploys schema v2 at t=15s; consumer only knows v1 and crashes on what it cannot parse",
        sink="pg-batched", replicas=1, keda=False, pg_cpu="2", rate=1000, duration=45, kill_at=None,
        decoder="strict-v1", on_bad="fail", v2_after=15, expect="v1_valid", drain_timeout=45,
    ),
    "schema-strict-dlq": dict(
        question="Same v2 deploy, but unparseable events go to a dead-letter topic; then the consumer is fixed and the DLQ redriven",
        sink="pg-batched", replicas=1, keda=False, pg_cpu="2", rate=1000, duration=45, kill_at=None,
        decoder="strict-v1", on_bad="dlq", v2_after=15, expect="v1_valid", drain_timeout=60,
        then=dict(decoder="versioned", redrive=True),
    ),
    "schema-versioned": dict(
        question="Same v2 deploy against a consumer that understands both schema versions",
        sink="pg-batched", replicas=1, keda=False, pg_cpu="2", rate=1000, duration=45, kill_at=None,
        decoder="versioned", on_bad="dlq", v2_after=15, expect="valid", drain_timeout=60,
    ),
    "poison-fail": dict(
        question="0.2% malformed events (truncated JSON, missing fields, bad values), consumer treats them as fatal",
        sink="pg-batched", replicas=1, keda=False, pg_cpu="2", rate=1000, duration=45, kill_at=None,
        decoder="versioned", on_bad="fail", bad_rate=0.002, expect="valid", drain_timeout=45,
    ),
    "poison-skip": dict(
        question="0.5% malformed events, consumer logs and skips them",
        sink="pg-batched", replicas=1, keda=False, pg_cpu="2", rate=1000, duration=45, kill_at=None,
        decoder="versioned", on_bad="skip", bad_rate=0.005, expect="valid", drain_timeout=60,
    ),
    "poison-dlq": dict(
        question="0.5% malformed events, consumer sends them to a dead-letter topic",
        sink="pg-batched", replicas=1, keda=False, pg_cpu="2", rate=1000, duration=45, kill_at=None,
        decoder="versioned", on_bad="dlq", bad_rate=0.005, expect="valid", drain_timeout=60,
    ),
}


def kubectl(*args: str, input: str | None = None, check: bool = True) -> str:
    result = subprocess.run(["kubectl", *args], input=input, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout


def sigkill_container(pod: str) -> None:
    """Kill the container's processes immediately through the container runtime (like an OOM kill)."""
    container_id = kubectl("get", "pod", pod, "-o", "jsonpath={.status.containerStatuses[0].containerID}").split("//")[-1]
    node = subprocess.run(
        ["sudo", "-n", "docker", "exec", "beautypipe-control-plane", "crictl", "stop", "--timeout", "0", container_id],
        capture_output=True, text=True,
    )
    if node.returncode != 0:
        raise RuntimeError(f"crictl stop failed: {node.stderr.strip()}")


def wait_for_postgres(timeout: float = 120) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with db.connect(connect_timeout=3) as conn:
                conn.execute("SELECT 1")
            return
        except Exception:
            time.sleep(1)
    raise TimeoutError("postgres not reachable")


def ensure_database() -> None:
    wait_for_postgres()
    db.init_schema()
    with db.connect() as conn:
        seeded = conn.execute("SELECT count(*) FROM products").fetchone()[0]
    if not seeded:
        from .batchjob import seed

        seed()


def set_postgres_cpu(cpu: str) -> None:
    current = kubectl("get", "deploy/postgres", "-o", "jsonpath={.spec.template.spec.containers[0].resources.limits.cpu}")
    if current == cpu:
        return
    request = cpu if cpu.endswith("m") else "1"
    kubectl("set", "resources", "deploy/postgres", f"--limits=cpu={cpu}", f"--requests=cpu={request}")
    kubectl("rollout", "status", "deploy/postgres", "--timeout=180s")
    ensure_database()


def configure_consumers(
    sink: str, topic: str, group: str, replicas: int, decoder: str = "versioned", on_bad: str = "dlq"
) -> None:
    kubectl("delete", "scaledobject/consumer", "--ignore-not-found")
    kubectl(
        "set", "env", "deploy/consumer",
        f"SINK={sink}", f"TOPIC={topic}", f"GROUP={group}", f"DECODER={decoder}", f"ON_BAD={on_bad}",
    )
    kubectl("scale", "deploy/consumer", f"--replicas={replicas}")
    kubectl("rollout", "status", "deploy/consumer", "--timeout=180s")
    time.sleep(8)  # let the group join and partitions get assigned


def enable_keda(topic: str, group: str) -> None:
    manifest = Template((K8S_DIR / "keda-scaledobject.yaml").read_text()).substitute(TOPIC=topic, GROUP=group)
    kubectl("apply", "-f", "-", input=manifest)


class Sampler(threading.Thread):
    """Samples from outside the consumers: database (fast) and Kubernetes (slower).

    Events stored = sum of the rollup counters, which are updated in the same transaction as the
    events, so it is exact and cheap (pg_stat counters are flushed too lazily to use here)."""

    def __init__(self, out: Path) -> None:
        super().__init__(daemon=True)
        self.out = out
        self.produced = 0
        self.stop_event = threading.Event()
        self.rows: list[dict] = []
        self._lock = threading.Lock()
        self._k8s = threading.Thread(target=self._sample_k8s, daemon=True)

    def _add(self, row: dict) -> None:
        with self._lock:
            self.rows.append(row)

    def run(self) -> None:
        self._k8s.start()
        conn = db.connect(autocommit=True)
        try:
            while not self.stop_event.is_set():
                started = time.time()
                stored = conn.execute(
                    "SELECT coalesce(sum(review_count + search_count + view_count), 0)::bigint FROM product_stats"
                ).fetchone()[0]
                connections = conn.execute(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = 'beauty' AND backend_type = 'client backend' AND pid <> pg_backend_pid()"
                ).fetchone()[0]
                freshness = conn.execute("SELECT extract(epoch FROM now() - max(event_ts)) FROM events").fetchone()[0]
                self._add(
                    {
                        "kind": "db", "ts": started, "produced": self.produced,
                        "stored": int(stored), "pg_connections": connections,
                        "freshness_s": float(freshness) if freshness is not None else None,
                    }
                )
                self.stop_event.wait(max(0.0, 1.0 - (time.time() - started)))
        finally:
            conn.close()

    def _sample_k8s(self) -> None:
        while not self.stop_event.is_set():
            started = time.time()
            try:
                status = json.loads(kubectl("get", "deploy/consumer", "-o", "json"))["status"]
                cpu_stat = kubectl("exec", "deploy/postgres", "--", "cat", "/sys/fs/cgroup/cpu.stat", check=False)
                stat = {k: int(v) for k, v in (line.split() for line in cpu_stat.splitlines() if line)} if cpu_stat else {}
                self._add(
                    {
                        "kind": "k8s", "ts": started,
                        "replicas": status.get("replicas", 0), "ready": status.get("readyReplicas", 0),
                        "pg_usage_usec": stat.get("usage_usec"), "pg_throttled_usec": stat.get("throttled_usec"),
                        "pg_nr_throttled": stat.get("nr_throttled"),
                    }
                )
            except Exception as exc:  # sampling must never break the run
                self._add({"kind": "k8s_error", "ts": started, "error": str(exc)[:200]})
            self.stop_event.wait(max(0.0, 2.0 - (time.time() - started)))

    def finish(self) -> None:
        self.stop_event.set()
        self.join(timeout=10)
        self._k8s.join(timeout=10)
        with open(self.out / "samples.jsonl", "w") as f:
            for row in sorted(self.rows, key=lambda r: r["ts"]):
                f.write(json.dumps(row) + "\n")


def collect_consumer_logs(out: Path) -> None:
    pods = kubectl("get", "pods", "-l", "app=consumer", "-o", "jsonpath={.items[*].metadata.name}").split()
    with open(out / "consumer-logs.jsonl", "w") as f:
        for pod in pods:
            for container, flags in (("previous", ["--previous"]), ("current", [])):
                for line in kubectl("logs", pod, *flags, check=False).splitlines():
                    if line.startswith("{"):
                        f.write(json.dumps({"pod": pod, "container": container, **json.loads(line)}) + "\n")


def consumer_restarts() -> int:
    out = kubectl("get", "pods", "-l", "app=consumer", "-o", "jsonpath={.items[*].status.containerStatuses[0].restartCount}")
    return sum(int(x) for x in out.split())


def wait_for_stored(sampler: Sampler, target: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = [r for r in sampler.rows if r["kind"] == "db"]
        if rows and rows[-1]["stored"] >= target:
            return True
        time.sleep(1)
    return False


def run_experiment(name: str, results: Path) -> dict:
    cfg = {
        "kill_at": None, "kill_mode": None, "decoder": "versioned", "on_bad": "dlq", "bad_rate": 0.0,
        "v2_after": None, "expect": "sent", "drain_timeout": 420, "then": None, **EXPERIMENTS[name],
    }
    out = results / name
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*"):
        old.unlink()

    stamp = int(time.time())
    topic, group = f"beauty.events.{name}.{stamp}", f"beauty-{name}-{stamp}"
    print(f"[{name}] {cfg['question']}", flush=True)

    kubectl("delete", "scaledobject/consumer", "--ignore-not-found")
    kubectl("scale", "deploy/consumer", "--replicas=0")
    set_postgres_cpu(cfg["pg_cpu"])
    ensure_database()
    kafkautil.create_topic(topic, PARTITIONS)
    dlq_topic = f"{topic}.dlq"
    kafkautil.ensure_topic(dlq_topic)
    configure_consumers(cfg["sink"], topic, group, cfg["replicas"], cfg["decoder"], cfg["on_bad"])
    with db.connect(autocommit=True) as conn:
        conn.execute("TRUNCATE events, product_stats")
        conn.execute("VACUUM ANALYZE events")
    if cfg["keda"]:
        enable_keda(topic, group)
        time.sleep(3)

    sampler = Sampler(out)
    sampler.start()
    time.sleep(2)

    def on_progress(n: int) -> None:
        sampler.produced = n

    result: dict = {}
    stats: dict = {}
    t0 = time.time()
    producer = threading.Thread(
        target=lambda: result.update(
            sent=produce(
                topic, cfg["rate"], cfg["duration"], on_progress=on_progress,
                bad_rate=cfg["bad_rate"], v2_after=cfg["v2_after"], stats=stats,
            )
        )
    )
    producer.start()
    killed = None
    if cfg["kill_at"]:
        time.sleep(max(0, t0 + cfg["kill_at"] - time.time()))
        pod = kubectl("get", "pods", "-l", "app=consumer", "-o", "jsonpath={.items[0].metadata.name}")
        killed = {"pod": pod, "at": time.time()}
        if cfg["kill_mode"] == "crash":
            sigkill_container(pod)
        else:
            kubectl("delete", "pod", pod, "--wait=false")
        print(f"[{name}] {cfg['kill_mode']} {pod} at t={killed['at'] - t0:.1f}s", flush=True)
    producer.join()
    t_end = time.time()
    sent = result["sent"]
    sampler.produced = sent

    target = sent if cfg["expect"] == "sent" else stats[cfg["expect"]]
    drained = wait_for_stored(sampler, target, cfg["drain_timeout"])
    drain_end = time.time()
    last_db = [r for r in sampler.rows if r["kind"] == "db"][-1]
    phase_one = {"stored": last_db["stored"], "restarts": consumer_restarts(), "dlq": dlq.inspect(dlq_topic, idle_timeout=4)}

    recovery = None
    if cfg["then"]:
        t_fix = time.time()
        print(f"[{name}] phase 1: stored={phase_one['stored']} dlq={phase_one['dlq']['total']}; fixing consumer and redriving", flush=True)
        configure_consumers(cfg["sink"], topic, group, cfg["replicas"], cfg["then"]["decoder"], cfg["on_bad"])
        redriven = dlq.redrive(dlq_topic, topic, idle_timeout=4) if cfg["then"].get("redrive") else 0
        recovered = wait_for_stored(sampler, stats["valid"], 120)
        recovery = {"redriven": redriven, "recovered": recovered, "seconds": time.time() - t_fix}
        drain_end = time.time()
    time.sleep(3)
    sampler.finish()
    collect_consumer_logs(out)

    validation = db.validate()
    summary = {
        "experiment": name, **cfg, "topic": topic, "sent": sent, "producer_stats": stats,
        "t0": t0, "produce_end": t_end, "drain_end": drain_end, "drained": drained,
        "killed": killed, "validation": validation, "phase_one": phase_one, "recovery": recovery,
        "final_dlq": dlq.inspect(dlq_topic, idle_timeout=4), "restarts": consumer_restarts(),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    kafkautil.delete_topic(topic)
    kafkautil.delete_topic(dlq_topic)
    kubectl("delete", "scaledobject/consumer", "--ignore-not-found")
    print(f"[{name}] sent={sent} stored={validation['events']} drained={drained} "
          f"drain_time={drain_end - t_end:.0f}s mismatches={validation['mismatched_products']}", flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiments", default=",".join(EXPERIMENTS))
    parser.add_argument("--out", default="results-k8s")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    if args.list:
        for name, cfg in EXPERIMENTS.items():
            print(f"{name:20s} {cfg['question']}")
        return

    os.environ.setdefault("KAFKA_BOOTSTRAP", "localhost:31092")
    os.environ.setdefault("DATABASE_URL", "postgresql://beauty:beauty@localhost:30432/beauty")
    results = Path(args.out)
    for name in args.experiments.split(","):
        run_experiment(name.strip(), results)
    (results / "run_config.json").write_text(json.dumps({"partitions": PARTITIONS, "num_products": config.num_products(), "catalog": config.catalog_source()}, indent=2))


if __name__ == "__main__":
    main()
