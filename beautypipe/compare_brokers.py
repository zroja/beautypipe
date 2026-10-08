"""Side-by-side table of the Kubernetes experiments on two brokers.

    python -m beautypipe.compare_brokers --a results-k8s --b results-k8s-strimzi
"""

import argparse
import json
from pathlib import Path

from .k8s_report import LABELS, load


def _row(runs: dict, name: str) -> dict | None:
    if name not in runs:
        return None
    r, s = runs[name], runs[name]["summary"]
    return {
        "peak": r["peak_backlog"], "drain": r["drain_s"], "pods": r["peak_ready"], "pod_s": r["pod_seconds"],
        "stored": s["validation"]["events"], "sent": s["sent"], "restarts": s.get("restarts"), "recovery": r["recovery_s"],
        "dlq": s.get("final_dlq", {}).get("total", 0), "drained": s["drained"],
        "mismatch": s["validation"]["mismatched_products"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--a", default="results-k8s")
    parser.add_argument("--b", default="results-k8s-strimzi")
    args = parser.parse_args()
    folders = [Path(args.a), Path(args.b)]
    names = [json.loads((f / "run_config.json").read_text()).get("broker", "Redpanda v25.2.3") for f in folders]
    runs = [{n: load(f / n) for n in LABELS if (f / n / "summary.json").exists()} for f in folders]

    def cell(row: dict | None) -> str:
        if row is None:
            return "not run"
        back = f" / back under 1,000 after {row['recovery']:.0f} s" if row["recovery"] is not None else ""
        return f"{row['peak']:,} / {row['drain']:.0f} s / {row['pods']} / {row['pod_s']:.0f}{back}"

    def outcome(row: dict | None) -> str:
        if row is None:
            return ""
        extra = f", {row['dlq']:,} dead-lettered" if row["dlq"] else ""
        stuck = "" if row["drained"] else " (did not drain)"
        return f"{row['stored']:,} of {row['sent']:,} stored{extra}{stuck}; {'n/a' if row['restarts'] is None else row['restarts']} restarts; {row['mismatch']} mismatches"

    lines = [
        f"# {names[0]} vs {names[1]}",
        "",
        "Each cell: peak backlog (events) / drain time after the producer stops / max ready pods / pod-seconds "
        "(pod-failure rows add the time until the backlog was back under 1,000). "
        "Restarts are n/a for the Redpanda runs made before that counter existed. One run per scenario per broker, same VM, same code.",
        "",
        f"| Experiment | {names[0]} | {names[1]} |",
        "|---|---|---|",
    ]
    for name in LABELS:
        a, b = (_row(r, name) for r in runs)
        if a is None and b is None:
            continue
        lines.append(f"| {LABELS[name]} | {cell(a)} | {cell(b)} |")
    lines += ["", "## Outcomes", "", f"| Experiment | {names[0]} | {names[1]} |", "|---|---|---|"]
    for name in LABELS:
        a, b = (_row(r, name) for r in runs)
        if a is None and b is None:
            continue
        lines.append(f"| {LABELS[name]} | {outcome(a)} | {outcome(b)} |")
    out = folders[1] / "comparison.md"
    out.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
