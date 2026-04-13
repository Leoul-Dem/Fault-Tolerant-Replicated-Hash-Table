#!/usr/bin/env python3
"""Plot per-second throughput and latency from local_run_logs/node_*.log files."""

import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt

LOG_DIR = Path(__file__).resolve().parent / "local_run_logs"


def parse_node_log(path: Path) -> tuple[dict[int, int], dict[int, float]]:
    """Returns (throughput_dict, latency_dict) keyed by time in seconds."""
    throughput = {}
    latency = {}
    for line in path.read_text().splitlines():
        m = re.match(r"\[T=(\d+)s\]\s+(\d+)\s+ops/s\s+([\d.]+)\s+us", line)
        if m:
            t = int(m.group(1))
            throughput[t] = int(m.group(2))
            latency[t] = float(m.group(3))
            continue
        # Fallback: old format without latency
        m = re.match(r"\[T=(\d+)s\]\s+(\d+)\s+ops/s", line)
        if m:
            t = int(m.group(1))
            throughput[t] = int(m.group(2))
    return throughput, latency


def main():
    logs = sorted(LOG_DIR.glob("node_*.log"), key=lambda p: int(re.search(r"(\d+)", p.stem).group()))
    if not logs:
        print("No log files found in", LOG_DIR, file=sys.stderr)
        return 1

    all_tp = {}
    all_lat = {}
    for lf in logs:
        idx = int(re.search(r"(\d+)", lf.stem).group())
        tp, lat = parse_node_log(lf)
        all_tp[idx] = tp
        all_lat[idx] = lat

    max_t = max(max(d.keys(), default=0) for d in all_tp.values())
    max_len = max(len(d) for d in all_tp.values())

    killed = {}
    threshold = int(max_len * 0.8)
    for idx, d in all_tp.items():
        if len(d) < threshold:
            killed[idx] = max(d.keys()) + 1

    # Aggregate throughput
    agg_tp = {}
    for t in range(1, max_t + 1):
        agg_tp[t] = sum(d.get(t, 0) for d in all_tp.values())

    # Weighted average latency (weight by ops/s so high-throughput nodes count more)
    agg_lat = {}
    has_latency = any(len(d) > 0 for d in all_lat.values())
    if has_latency:
        for t in range(1, max_t + 1):
            total_ops = 0
            weighted_sum = 0.0
            for idx in all_tp:
                ops = all_tp[idx].get(t, 0)
                lat = all_lat[idx].get(t, 0.0)
                if ops > 0 and lat > 0:
                    weighted_sum += ops * lat
                    total_ops += ops
            agg_lat[t] = weighted_sum / total_ops if total_ops > 0 else 0.0

    n_nodes = len(all_tp)
    suffix = ""
    if killed:
        suffix = f" ({len(killed)} node{'s' if len(killed) > 1 else ''} killed mid-run)"

    # --- Throughput plot ---
    fig, ax = plt.subplots(figsize=(12, 6))
    ts = sorted(agg_tp.keys())
    ax.plot(ts, [agg_tp[t] for t in ts], color="black", linewidth=2.5, label="Aggregate Throughput")

    agg_vals = sorted(agg_tp.values())
    if agg_vals:
        p95 = agg_vals[int(len(agg_vals) * 0.95)]
        ax.set_ylim(0, max(p95 * 1.5, 100))

    ax.set_xlabel("Time (s)", fontsize=12)
    ax.set_ylabel("Throughput (ops/s)", fontsize=12)
    ax.set_title(f"Aggregate Throughput — {n_nodes}-Node Cluster{suffix}", fontsize=14)
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(1, max_t)
    fig.tight_layout()
    out_tp = LOG_DIR.parent / "throughput_plot.png"
    fig.savefig(out_tp, dpi=150)
    print(f"Saved: {out_tp}")
    plt.close(fig)

    # --- Latency plot ---
    if has_latency and agg_lat:
        fig, ax = plt.subplots(figsize=(12, 6))
        ts_lat = sorted(t for t in agg_lat if agg_lat[t] > 0)
        vals = [agg_lat[t] for t in ts_lat]
        ax.plot(ts_lat, vals, color="black", linewidth=2.5, label="Avg Latency")

        lat_sorted = sorted(vals)
        if lat_sorted:
            p95 = lat_sorted[int(len(lat_sorted) * 0.95)]
            ax.set_ylim(0, max(p95 * 1.5, 10))

        ax.set_xlabel("Time (s)", fontsize=12)
        ax.set_ylabel("Latency (us)", fontsize=12)
        ax.set_title(f"Average Latency — {n_nodes}-Node Cluster{suffix}", fontsize=14)
        ax.legend(loc="upper right", fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_xlim(1, max_t)
        fig.tight_layout()
        out_lat = LOG_DIR.parent / "latency_plot.png"
        fig.savefig(out_lat, dpi=150)
        print(f"Saved: {out_lat}")
        plt.close(fig)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
