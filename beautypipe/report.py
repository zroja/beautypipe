"""Turns raw results/<scenario>/ metrics into charts and a markdown summary.

    python -m beautypipe.report --results results
"""

import argparse
import json
from array import array
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

LABELS = {
    "blackhole-1": "No-op sink (ceiling)",
    "pg-naive-1": "Postgres, per-event commit, 1 consumer",
    "pg-naive-4": "Postgres, per-event commit, 4 consumers",
    "pg-batched-1": "Postgres, batched upsert, 1 consumer",
    "pg-naive-1+batch": "Per-event commit + batch refresh job",
    "pg-batched-1+batch": "Batched upsert + batch refresh job",
}
COLORS = {
    "blackhole-1": "#9aa0a6",
    "pg-naive-1": "#d93f6b",
    "pg-naive-4": "#f08aa8",
    "pg-batched-1": "#2a9d8f",
    "pg-naive-1+batch": "#8a1c3d",
    "pg-batched-1+batch": "#14665c",
}


def _lines(path: Path):
    for line in path.read_text().splitlines():
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


def _percentile(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))] if values else float("nan")


def load(scenario_dir: Path) -> dict:
    summary = json.loads((scenario_dir / "summary.json").read_text())
    t0 = summary["produce_start"]
    processed, lag, p99, sink_ms, calls = (defaultdict(float) for _ in range(5))
    for f in sorted(scenario_dir.glob("consumer-*.jsonl")):
        for row in _lines(f):
            sec = int(row["ts"] - t0)
            processed[sec] += row["processed"]
            lag[sec] += row["lag"]
            p99[sec] = max(p99[sec], row["e2e_p99_ms"])
            sink_ms[sec] += row.get("sink_ms_total", 0.0)
            calls[sec] += row["sink_calls"]
    seconds = sorted(processed)

    latencies = array("f")
    for f in scenario_dir.glob("consumer-*.latencies.f32"):
        with open(f, "rb") as fh:
            latencies.frombytes(fh.read())

    busy = [s for s in seconds if processed[s] > 0]
    produce_s = summary["produce_end"] - t0
    during = sum(processed[s] for s in seconds if 0 <= s <= produce_s)
    total_processed = sum(processed.values())
    batch = list(_lines(scenario_dir / "batchjob.jsonl")) if (scenario_dir / "batchjob.jsonl").exists() else []

    return {
        "summary": summary,
        "seconds": seconds,
        "processed": [processed[s] for s in seconds],
        "lag": [lag[s] for s in seconds],
        "p99": [p99[s] for s in seconds],
        "throughput_during_produce": during / max(produce_s, 1),
        "peak_lag": max(lag.values(), default=0),
        "drain_s": max(0, (max(busy) if busy else 0) - produce_s),
        "e2e_p50": _percentile(latencies, 0.50),
        "e2e_p99": _percentile(latencies, 0.99),
        "e2e_max": max(latencies) if latencies else float("nan"),
        "sink_ms_per_event": (sum(sink_ms.values()) / total_processed) if total_processed else float("nan"),
        "batch_refreshes": len(batch),
        "batch_mean_s": (sum(b["duration_s"] for b in batch) / len(batch)) if batch else None,
    }


def render(results: Path) -> None:
    runs = {}
    for name in LABELS:
        if (results / name / "summary.json").exists():
            runs[name] = load(results / name)
    if not runs:
        raise SystemExit(f"no results found in {results}")
    cfg = json.loads((results / "run_config.json").read_text()) if (results / "run_config.json").exists() else {}
    produce_s = cfg.get("duration")

    fig, axes = plt.subplots(2, 2, figsize=(14, 9), constrained_layout=True)
    panels = [
        (axes[0][0], "lag", "Consumer lag (events behind)", False),
        (axes[0][1], "processed", "Throughput (events processed / s)", False),
        (axes[1][0], "p99", "End-to-end latency p99 per second (ms, log scale)", True),
    ]
    for ax, key, title, log in panels:
        for name, r in runs.items():
            ax.plot(r["seconds"], r[key], label=LABELS[name], color=COLORS[name], linewidth=1.8)
        if produce_s:
            ax.axvline(produce_s, color="black", linestyle=":", linewidth=1)
        ax.set_title(title)
        ax.set_xlabel("seconds since producer start (dotted line = producer stops)")
        ax.set_xlim(left=0)
        if log:
            ax.set_yscale("log")
        ax.grid(alpha=0.25)
    axes[0][0].legend(fontsize=8, loc="upper right")

    names = list(runs)
    ax = axes[1][1]
    ax.barh([LABELS[n] for n in names], [runs[n]["sink_ms_per_event"] for n in names], color=[COLORS[n] for n in names])
    ax.set_title("Sink time per event (ms)")
    ax.invert_yaxis()
    ax.grid(alpha=0.25, axis="x")
    ax.tick_params(axis="y", labelsize=8)
    fig.suptitle(
        f"Beauty events pipeline: producer {cfg.get('rate', '?')} events/s for {cfg.get('duration', '?')}s, "
        f"{cfg.get('partitions', '?')} partitions",
        fontsize=13,
    )
    fig.savefig(results / "overview.png", dpi=130)
    plt.close(fig)

    lines = [
        "# Results",
        "",
        f"Producer rate: **{cfg.get('rate', '?')} events/s** for **{cfg.get('duration', '?')} s**, "
        f"{cfg.get('partitions', '?')} partitions, {cfg.get('num_products', '?')} products with Zipf-skewed traffic.",
        "",
        "![overview](overview.png)",
        "",
        "| Scenario | Sustained throughput (events/s) | Peak lag | Drain time after producer stops | "
        "Sink ms/event | E2E p50 | E2E p99 | E2E max | Rollup mismatches |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, r in runs.items():
        v = r["summary"].get("validation")
        mismatches = "n/a" if v is None else str(v["mismatched_products"])
        lines.append(
            f"| {LABELS[name]} | {r['throughput_during_produce']:.0f} | {r['peak_lag']:,.0f} | {r['drain_s']:.0f} s | "
            f"{r['sink_ms_per_event']:.3f} | {r['e2e_p50'] / 1000:.2f} s | {r['e2e_p99'] / 1000:.2f} s | "
            f"{r['e2e_max'] / 1000:.2f} s | {mismatches} |"
        )
    batch_rows = [(n, r) for n, r in runs.items() if r["batch_refreshes"]]
    if batch_rows:
        lines += ["", "Batch refresh job (rewrites all products and shades every 6 s):", ""]
        for n, r in batch_rows:
            lines.append(f"- {LABELS[n]}: {r['batch_refreshes']} refreshes, mean {r['batch_mean_s']:.2f} s each")
    for n, r in runs.items():
        rep = r["summary"].get("replay")
        if rep:
            lines += [
                "",
                f"Replay check ({LABELS[n]}): re-consumed {rep['redelivered']:,} events through a new consumer group; "
                f"database identical before/after: **{rep['identical']}** "
                f"(events={rep['after']['events']:,}, rollup mismatches={rep['after']['mismatched_products']}).",
            ]
    (results / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", default="results")
    render(Path(parser.parse_args().results))


if __name__ == "__main__":
    main()
