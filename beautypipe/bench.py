"""Runs scenarios end to end and records raw metrics under results/<scenario>/.

    python -m beautypipe.bench --rate 1500 --duration 30
    python -m beautypipe.bench --scenarios blackhole,pg-batched-1 --rate 3000
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from . import config, db, kafkautil
from .producer import produce

SCENARIOS = {
    "blackhole-1": {"sink": "blackhole", "consumers": 1, "batchjob": False},
    "pg-naive-1": {"sink": "pg-naive", "consumers": 1, "batchjob": False},
    "pg-naive-4": {"sink": "pg-naive", "consumers": 4, "batchjob": False},
    "pg-batched-1": {"sink": "pg-batched", "consumers": 1, "batchjob": False},
    "pg-naive-1+batch": {"sink": "pg-naive", "consumers": 1, "batchjob": True},
    "pg-batched-1+batch": {"sink": "pg-batched", "consumers": 1, "batchjob": True},
}


def _spawn(module: str, *args: str, log: Path) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-u", "-m", f"beautypipe.{module}", *args],
        stdout=open(log, "w"),
        stderr=subprocess.STDOUT,
        env=os.environ.copy(),
    )


def _wait_for(path: Path, text: str, timeout: float) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists() and text in path.read_text():
            return
        time.sleep(0.2)
    raise TimeoutError(f"{text!r} not seen in {path}")


def _processed(out: Path) -> int:
    total = 0
    for f in out.glob("consumer-*.jsonl"):
        for line in f.read_text().splitlines():
            try:
                total += json.loads(line)["processed"]
            except (json.JSONDecodeError, KeyError):
                pass
    return total


def _stop(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        p.send_signal(signal.SIGTERM)
    for p in procs:
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()


def run_scenario(name: str, rate: int, duration: float, results: Path, drain_timeout: float, replay_check: bool) -> dict:
    cfg = SCENARIOS[name]
    out = results / name
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*"):
        old.unlink()

    topic = f"beauty.bench.{name.replace('+', '-')}.{int(time.time())}"
    group = f"bench-{name.replace('+', '-')}"
    kafkautil.create_topic(topic)
    db.reset_streaming_tables()

    consumers = []
    for i in range(cfg["consumers"]):
        log = out / f"consumer-{i}.log"
        consumers.append(
            _spawn("consumer", "--topic", topic, "--group", group, "--sink", cfg["sink"],
                   "--metrics", str(out / f"consumer-{i}.jsonl"), log=log)
        )
    for i in range(cfg["consumers"]):
        _wait_for(out / f"consumer-{i}.log", "READY", 60)
    time.sleep(3)

    batch = None
    if cfg["batchjob"]:
        batch = _spawn("batchjob", "loop", "--every", "6", "--log", str(out / "batchjob.jsonl"), log=out / "batchjob.log")
        time.sleep(1)

    print(f"[{name}] producing {rate}/s for {duration:.0f}s ...", flush=True)
    produce_start = time.time()
    sent = produce(topic, rate, duration)
    produce_end = time.time()

    print(f"[{name}] produced {sent}, draining ...", flush=True)
    drained = False
    while time.time() - produce_end < drain_timeout:
        if _processed(out) >= sent:
            drained = True
            break
        time.sleep(1)
    drain_end = time.time()

    if batch:
        _stop([batch])
    time.sleep(1.5)
    _stop(consumers)

    summary = {
        "scenario": name,
        **cfg,
        "rate": rate,
        "duration": duration,
        "sent": sent,
        "produce_start": produce_start,
        "produce_end": produce_end,
        "drain_end": drain_end,
        "drained": drained,
    }
    if cfg["sink"] != "blackhole":
        summary["validation"] = db.validate()
        if replay_check and cfg["sink"] == "pg-batched":
            summary["replay"] = _replay(topic, cfg["sink"], out)
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    kafkautil.delete_topic(topic)
    print(f"[{name}] done: drained={drained} drain_time={drain_end - produce_end:.1f}s", flush=True)
    return summary


def _replay(topic: str, sink: str, out: Path) -> dict:
    """Re-consume the whole topic with a brand new group and check nothing changes."""
    before = db.validate()
    proc = _spawn("consumer", "--topic", topic, "--group", f"replay-{int(time.time())}", "--sink", sink,
                  "--metrics", str(out / "replay.jsonl"), "--until-drained", "--timeout", "180", log=out / "replay.log")
    proc.wait(timeout=240)
    after = db.validate()
    redelivered = sum(json.loads(l)["processed"] for l in (out / "replay.jsonl").read_text().splitlines())
    return {"before": before, "after": after, "redelivered": redelivered, "identical": before == after}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenarios", default=",".join(SCENARIOS), help=f"comma separated: {', '.join(SCENARIOS)}")
    parser.add_argument("--rate", type=int, default=1500, help="events per second")
    parser.add_argument("--duration", type=float, default=30, help="seconds of production")
    parser.add_argument("--drain-timeout", type=float, default=240)
    parser.add_argument("--out", default="results")
    parser.add_argument("--no-replay-check", action="store_true")
    args = parser.parse_args()

    db.init_schema()
    with db.connect() as conn:
        seeded = conn.execute("SELECT count(*) FROM products").fetchone()[0]
    if not seeded:
        from .batchjob import seed
        seed()

    results = Path(args.out)
    for name in args.scenarios.split(","):
        run_scenario(name.strip(), args.rate, args.duration, results, args.drain_timeout, not args.no_replay_check)
    (results / "run_config.json").write_text(
        json.dumps({"rate": args.rate, "duration": args.duration, "partitions": config.PARTITIONS,
                    "num_products": config.NUM_PRODUCTS, "shades_per_product": config.SHADES_PER_PRODUCT}, indent=2)
    )


if __name__ == "__main__":
    main()
