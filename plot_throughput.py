#!/usr/bin/env python3
"""Plot per-second throughput from local_run_logs/node_*.log files."""

import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt

LOG_DIR = Path(__file__).resolve().parent / "local_run_logs"


def parse_node_log(path: Path) -> dict[int, int]:
    data = {}
    for line in path.read_text().splitlines():
        m = re.match(r"\[T=(\d+)s\]\s+(\d+)\s+ops/s", line)
        if m:
            data[int(m.group(1))] = int(m.group(2))
    return data


def main():
    logs = sorted(LOG_DIR.glob("node_*.log"), key=lambda p: int(re.search(r"(\d+)", p.stem).group()))
    if not logs:
        print("No log files found in", LOG_DIR, file=sys.stderr)
        return 1

    fig, ax = plt.subplots(figsize=(12, 6))

    # Detect killed node (the one with fewest data points)
    all_series = {}
    for lf in logs:
        idx = int(re.search(r"(\d+)", lf.stem).group())
        all_series[idx] = parse_node_log(lf)

    max_t = max(max(d.keys(), default=0) for d in all_series.values())
    min_len = min(len(d) for d in all_series.values())
    max_len = max(len(d) for d in all_series.values())

    killed = {}  # idx -> kill_time
    if min_len < max_len:
        for idx, d in all_series.items():
            if len(d) < max_len:
                killed[idx] = max(d.keys()) + 1

    # Aggregate throughput across all nodes
    agg = {}
    for t in range(1, max_t + 1):
        total = sum(d.get(t, 0) for d in all_series.values())
        agg[t] = total

    # Compute y-axis cap: use 95th percentile of aggregate * 1.5 to clip outlier spikes
    agg_vals = sorted(agg.values())
    if agg_vals:
        p95 = agg_vals[int(len(agg_vals) * 0.95)]
        y_cap = max(p95 * 1.5, 100)
    else:
        y_cap = None

    # Plot individual nodes
    for idx in sorted(all_series):
        d = all_series[idx]
        ts = sorted(d.keys())
        label = f"Node {idx}"
        if idx in killed:
            label += " (killed)"
        ax.plot(ts, [d[t] for t in ts], marker=".", markersize=4, linewidth=1, alpha=0.7, label=label)

    # Plot aggregate
    ts_agg = sorted(agg.keys())
    ax.plot(ts_agg, [agg[t] for t in ts_agg], color="black", linewidth=2.5, linestyle="--", label="Aggregate")

    # Mark kill events
    kill_colors = ["red", "darkred", "orangered", "crimson"]
    for i, (idx, kt) in enumerate(sorted(killed.items())):
        c = kill_colors[i % len(kill_colors)]
        ax.axvline(x=kt, color=c, linewidth=2, linestyle=":", label=f"Node {idx} killed (T={kt}s)")

    n_nodes = len(all_series)
    title = f"Per-Second Throughput — {n_nodes}-Node Cluster"
    if killed:
        title += f" ({len(killed)} node{'s' if len(killed) > 1 else ''} killed mid-run)"
    ax.set_xlabel("Time (s)", fontsize=12)
    ax.set_ylabel("Throughput (ops/s)", fontsize=12)
    ax.set_title(title, fontsize=14)
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(1, max_t)
    if y_cap is not None:
        ax.set_ylim(0, y_cap)

    out = LOG_DIR.parent / "throughput_plot.png"
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print(f"Saved: {out}")
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
