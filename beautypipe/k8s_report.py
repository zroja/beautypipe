"""Charts and a markdown summary for results-k8s/.

    python -m beautypipe.k8s_report --results results-k8s
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

LABELS = {
    "pod-delete": "Graceful pod deletion",
    "pod-crash": "Hard crash (SIGKILL)",
    "naive-fixed": "Per-event sink, 1 pod",
    "naive-keda": "Per-event sink, KEDA autoscaling",
    "batched-fixed": "Batched sink, 1 pod",
    "pgcpu-full": "Postgres 2 CPU, 1 pod",
    "pgcpu-limited": "Postgres 0.05 CPU, 1 pod",
    "pgcpu-limited-keda": "Postgres 0.05 CPU, KEDA autoscaling",
    "schema-strict-fail": "v2 schema, v1-only consumer, fail on bad event",
    "schema-strict-dlq": "v2 schema, v1-only consumer, dead-letter topic, then fix and redrive",
    "schema-versioned": "v2 schema, consumer understands v1 and v2",
    "poison-fail": "0.2% malformed events, fail on bad event",
    "poison-fail-keda": "0.2% malformed events, fail on bad event, KEDA autoscaling",
    "poison-skip": "0.5% malformed events, log and skip",
    "poison-dlq": "0.5% malformed events, dead-letter topic",
}
SCHEMA_RUNS = ["schema-strict-fail", "schema-strict-dlq", "schema-versioned"]
POISON_RUNS = ["poison-fail", "poison-fail-keda", "poison-skip", "poison-dlq"]
COLORS = {
    "pod-delete": "#2a9d8f", "pod-crash": "#d93f6b",
    "naive-fixed": "#d93f6b", "naive-keda": "#e9a23b", "batched-fixed": "#2a9d8f",
    "pgcpu-full": "#2a9d8f", "pgcpu-limited": "#d93f6b", "pgcpu-limited-keda": "#e9a23b",
    "schema-strict-fail": "#d93f6b", "schema-strict-dlq": "#e9a23b", "schema-versioned": "#2a9d8f",
    "poison-fail": "#d93f6b", "poison-fail-keda": "#e9a23b", "poison-skip": "#8d6bb8", "poison-dlq": "#2a9d8f",
}


def _jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.exists() else []


def load(folder: Path) -> dict:
    summary = json.loads((folder / "summary.json").read_text())
    t0 = summary["t0"]
    samples = _jsonl(folder / "samples.jsonl")
    db_rows = [r for r in samples if r["kind"] == "db" and r["ts"] >= t0 - 1]
    k8s_rows = [r for r in samples if r["kind"] == "k8s" and r["ts"] >= t0 - 1]

    t = [r["ts"] - t0 for r in db_rows]
    backlog = [r["produced"] - r["stored"] for r in db_rows]
    fresh = [r["freshness_s"] if r["freshness_s"] is not None else 0 for r in db_rows]
    conns = [r["pg_connections"] for r in db_rows]
    kt = [r["ts"] - t0 for r in k8s_rows]
    ready = [r["ready"] or 0 for r in k8s_rows]

    cpu_t, cpu_used, cpu_throttled = [], [], []
    for a, b in zip(k8s_rows, k8s_rows[1:]):
        if None in (a["pg_usage_usec"], b["pg_usage_usec"]):
            continue
        dt = b["ts"] - a["ts"]
        cpu_t.append(b["ts"] - t0)
        cpu_used.append((b["pg_usage_usec"] - a["pg_usage_usec"]) / 1e6 / dt)
        cpu_throttled.append((b["pg_throttled_usec"] - a["pg_throttled_usec"]) / 1e6 / dt)

    e2e = defaultdict(float)
    for row in _jsonl(folder / "consumer-logs.jsonl"):
        e2e[int(row["ts"] - t0)] = max(e2e[int(row["ts"] - t0)], row["e2e_p99_ms"])

    stored_t = [r["ts"] - t0 for r in db_rows]
    stored = [r["stored"] for r in db_rows]
    produced = [r["produced"] for r in db_rows]

    produce_s = summary["produce_end"] - t0
    drain_s = max(0.0, summary["drain_end"] - summary["produce_end"])
    active = [i for i, tt in enumerate(t) if tt <= produce_s + drain_s + 1]
    t, backlog, fresh, conns = ([series[i] for i in active] for series in (t, backlog, fresh, conns))
    pod_seconds = sum(((b["ts"] - a["ts"]) * (a["ready"] or 0)) for a, b in zip(k8s_rows, k8s_rows[1:]) if a["ts"] <= summary["drain_end"])

    recovery = None
    if summary.get("killed"):
        kill_t = summary["killed"]["at"] - t0
        peak_after = max((bl for tt, bl in zip(t, backlog) if tt >= kill_t), default=0)
        back = [tt for tt, bl in zip(t, backlog) if tt > kill_t and bl < 1000 and tt > kill_t + 1]
        past_peak = [tt for tt, bl in zip(t, backlog) if tt > kill_t and bl == peak_after]
        after_peak = [tt for tt in back if past_peak and tt > past_peak[0]]
        recovery = (after_peak[0] - kill_t) if after_peak else None

    return {
        "stored_t": stored_t, "stored": stored, "produced": produced,
        "summary": summary, "t": t, "backlog": backlog, "fresh": fresh, "conns": conns,
        "kt": kt, "ready": ready, "cpu_t": cpu_t, "cpu_used": cpu_used, "cpu_throttled": cpu_throttled,
        "e2e_t": sorted(e2e), "e2e_p99": [e2e[s] / 1000 for s in sorted(e2e)],
        "produce_s": produce_s, "drain_s": drain_s, "pod_seconds": pod_seconds,
        "peak_backlog": max(backlog, default=0),
        "peak_fresh": max((f for tt, f in zip(t, fresh) if tt <= produce_s + drain_s), default=0),
        "peak_ready": max(ready, default=0), "peak_conns": max(conns, default=0),
        "peak_e2e_p99": max((v for v in e2e.values()), default=0) / 1000,
        "throttled_s": sum(cpu_throttled), "recovery_s": recovery,
    }


def _style(ax, title, ylabel, produce_s=None):
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel("seconds since producer start (dotted = producer stops)")
    ax.set_xlim(left=0)
    ax.grid(alpha=0.25)
    if produce_s:
        ax.axvline(produce_s, color="black", linestyle=":", linewidth=1)


def plot_pods(runs: dict, results: Path) -> None:
    names = [n for n in ("pod-delete", "pod-crash") if n in runs]
    if not names:
        return
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.8), constrained_layout=True)
    for name in names:
        r = runs[name]
        kill = r["summary"]["killed"]["at"] - r["summary"]["t0"]
        axes[0].plot(r["t"], r["backlog"], label=LABELS[name], color=COLORS[name], linewidth=2)
        axes[1].plot(r["t"], r["fresh"], label=LABELS[name], color=COLORS[name], linewidth=2)
    axes[0].axvline(kill, color="gray", linestyle="--", linewidth=1)
    axes[1].axvline(kill, color="gray", linestyle="--", linewidth=1)
    _style(axes[0], "Backlog (produced minus stored events); dashed = pod killed", "events", runs[names[0]]["produce_s"])
    _style(axes[1], "Data freshness: age of newest stored event", "seconds", runs[names[0]]["produce_s"])
    axes[0].legend()
    fig.suptitle("Consumer pod failure: nothing lost, nothing double counted, but a crash costs about one session timeout")
    fig.savefig(results / "pod-failure.png", dpi=130)
    plt.close(fig)


def plot_group(runs: dict, names: list[str], results: Path, filename: str, title: str, with_cpu: bool) -> None:
    names = [n for n in names if n in runs]
    if not names:
        return
    rows = 3 if with_cpu else 2
    fig, axes = plt.subplots(rows, 1, figsize=(11, 3.6 * rows), constrained_layout=True, sharex=True)
    for name in names:
        r = runs[name]
        axes[0].plot(r["t"], r["backlog"], label=LABELS[name], color=COLORS[name], linewidth=2)
        axes[1].step(r["kt"], r["ready"], where="post", label=LABELS[name], color=COLORS[name], linewidth=2)
        if with_cpu:
            axes[2].plot(r["cpu_t"], r["cpu_used"], label=LABELS[name], color=COLORS[name], linewidth=2)
    p = runs[names[0]]["produce_s"]
    _style(axes[0], "Backlog: produced minus stored events", "events", p)
    _style(axes[1], "Consumer pods ready", "pods", p)
    axes[0].legend(fontsize=9)
    if with_cpu:
        _style(axes[2], "Postgres CPU actually used (cores)", "cores", p)
    fig.suptitle(title)
    fig.savefig(results / filename, dpi=130)
    plt.close(fig)


def plot_correctness(runs: dict, names: list[str], results: Path, filename: str, title: str) -> None:
    names = [n for n in names if n in runs]
    if not names:
        return
    fig, axes = plt.subplots(1, len(names), figsize=(5.2 * len(names), 4.6), constrained_layout=True, sharey=True)
    axes = [axes] if len(names) == 1 else list(axes)
    for ax, name in zip(axes, names):
        r = runs[name]
        ax.plot(r["stored_t"], r["produced"], color="#999999", linewidth=1.5, label="produced")
        ax.plot(r["stored_t"], r["stored"], color=COLORS[name], linewidth=2.2, label="stored in Postgres")
        _style(ax, LABELS[name], "events", r["produce_s"])
        ax.title.set_fontsize(9)
        ax.set_xlabel("seconds since producer start")
    axes[0].legend(fontsize=8, loc="upper left")
    fig.suptitle(title)
    fig.savefig(results / filename, dpi=130)
    plt.close(fig)


def correctness_table(runs: dict, names: list[str]) -> list[str]:
    lines = [
        "| Scenario | Sent | Should be stored | Stored | In dead-letter topic | Missing | Consumer restarts |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for n in names:
        if n not in runs:
            continue
        s = runs[n]["summary"]
        stored = s["validation"]["events"]
        dead = s["final_dlq"]["total"]
        redriven = s["recovery"]["redriven"] if s.get("recovery") else 0
        should = s["producer_stats"]["valid"]
        # after a redrive the dead letters were stored too, so they are not "in" the topic as a loss
        accounted = stored + (dead - redriven)
        missing = s["sent"] - accounted
        lines.append(
            f"| {LABELS[n]} | {s['sent']:,} | {should:,} | {stored:,} | {dead:,}{' (all redriven)' if redriven else ''} | "
            f"{missing:,} | {s['restarts']} |"
        )
    return lines


def table(runs: dict, names: list[str], extra: bool = False) -> list[str]:
    head = (
        "| Scenario | Peak backlog | Max data staleness | Drain after producer stops | Max pods | Pod-seconds | "
        "Peak PG connections | Rollup mismatches |"
    )
    lines = [head, "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for n in names:
        if n not in runs:
            continue
        r = runs[n]
        s = r["summary"]
        lines.append(
            f"| {LABELS[n]} | {r['peak_backlog']:,} | {r['peak_fresh']:.1f} s | {r['drain_s']:.0f} s | {r['peak_ready']} | "
            f"{r['pod_seconds']:.0f} | {r['peak_conns']} | {s['validation']['mismatched_products']} |"
        )
    return lines


def render(results: Path) -> None:
    runs = {n: load(results / n) for n in LABELS if (results / n / "summary.json").exists()}
    if not runs:
        raise SystemExit(f"no results in {results}")
    plot_pods(runs, results)
    plot_group(runs, ["naive-fixed", "naive-keda", "batched-fixed"], results, "scaling-vs-sink.png",
               "3,000 events/s: scale out the per-event consumers, or fix the sink?", with_cpu=False)
    plot_group(runs, ["pgcpu-full", "pgcpu-limited", "pgcpu-limited-keda"], results, "postgres-limits.png",
               "4,000 events/s into a resource-limited Postgres pod", with_cpu=True)

    config_file = results / "run_config.json"
    broker = json.loads(config_file.read_text()).get("broker", "Redpanda v25.2.3") if config_file.exists() else "Redpanda"
    out = ["# Kubernetes results", "", f"All runs: kind (single node), {broker}, Postgres 16 and consumers as pods, "
           "producer on the host at the stated rate, 8 partitions. Kubernetes-side metrics come from outside the consumers.", ""]
    for key in ("pod-delete", "pod-crash"):
        if key in runs:
            r, s = runs[key], runs[key]["summary"]
            rec = f"{r['recovery_s']:.1f} s" if r["recovery_s"] is not None else "n/a"
            out += [f"- **{LABELS[key]}** at t=15 s: peak backlog {r['peak_backlog']:,} events, max staleness "
                    f"{r['peak_fresh']:.1f} s, back under 1,000 events after {rec}; sent {s['sent']:,}, stored "
                    f"{s['validation']['events']:,}, rollup mismatches {s['validation']['mismatched_products']}."]
    out += ["", "![pod failure](pod-failure.png)", "", "## Scale out consumers, or fix the sink? (3,000 events/s for 60 s)", ""]
    out += table(runs, ["naive-fixed", "naive-keda", "batched-fixed"]) + ["", "![scaling](scaling-vs-sink.png)", "",
            "## Resource-limited Postgres (4,000 events/s for 45 s)", ""]
    out += table(runs, ["pgcpu-full", "pgcpu-limited", "pgcpu-limited-keda"])
    for n in ("pgcpu-full", "pgcpu-limited", "pgcpu-limited-keda"):
        if n in runs:
            out.append(f"- {LABELS[n]}: Postgres was CPU-throttled for {runs[n]['throttled_s']:.0f} s in total.")
    out += ["", "![postgres limits](postgres-limits.png)"]
    plot_correctness(runs, SCHEMA_RUNS, results, "schema-change.png",
                     "Producer deploys schema v2 at t=15 s (customer_id, occurred_at_ms, channel)")
    plot_correctness(runs, POISON_RUNS, results, "malformed-events.png", "Malformed events: three ways to react")
    if any(n in runs for n in SCHEMA_RUNS):
        out += ["", "## Schema change (1,000 events/s for 45 s, producer switches to v2 at t=15 s)", ""]
        out += correctness_table(runs, SCHEMA_RUNS) + ["", "![schema change](schema-change.png)"]
        for n in SCHEMA_RUNS:
            rec = runs.get(n, {}).get("summary", {}).get("recovery")
            if rec:
                out.append(f"- {LABELS[n]}: after the consumer was fixed, redriving {rec['redriven']:,} dead letters and "
                           f"storing all events took {rec['seconds']:.0f} s (includes the consumer rollout).")
    if any(n in runs for n in POISON_RUNS):
        out += ["", "## Malformed events (1,000 events/s for 45 s)", ""]
        out += correctness_table(runs, POISON_RUNS) + ["", "![malformed events](malformed-events.png)"]
        for n in POISON_RUNS:
            if n in runs and runs[n]["summary"]["final_dlq"]["total"]:
                out.append(f"- {LABELS[n]}: dead letters by reason: {runs[n]['summary']['final_dlq']['by_reason']}")
    (results / "summary.md").write_text("\n".join(out) + "\n")
    print("\n".join(out))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", default="results-k8s")
    render(Path(parser.parse_args().results))


if __name__ == "__main__":
    main()
