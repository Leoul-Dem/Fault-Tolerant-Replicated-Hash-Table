#!/usr/bin/env python3
"""
Remote launcher for Replicated_Hash_Table on a cluster of machines via SSH.

Usage:
  ./startup.py                                              # 6 nodes, 60s
  ./startup.py --nodes 6 --duration 30
  ./startup.py --nodes 6 --duration 30 --kill-node 2,5 --kill-at 10,20
  ./startup.py --sweep 4,6,8,10 --duration 60               # multi-trial
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
import sys
import time

# ─── Configuration ───────────────────────────────────────────────────────────
SSH_USER = "lgd226"
DOMAIN = "cse.lehigh.edu"
PROGRAM_PATH = "/home/lgd226/symmetrical-fishstick/build/Replicated_Hash_Table"
PROJECT_DIR = "/home/lgd226/symmetrical-fishstick"
PORT = 6007
TMUX_SESSION = "rht"
CSV_FILE = "results.csv"

MACHINES = [
    "ariel", "caliban", "callisto", "ceres", "chiron", "cupid",
    "eris", "europa", "hydra", "iapetus", "io", "ixion",
    "mars", "mercury", "neptune", "nereid", "nix", "orcus",
    "phobos", "puck", "saturn", "triton", "varda", "vesta", "xena",
]
# ─────────────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Replicated_Hash_Table on remote cluster.")
    p.add_argument("--nodes", type=int, default=6, help="number of nodes (default: 6)")
    p.add_argument("--duration", type=int, default=60, help="seconds before shutdown (default: 60)")
    p.add_argument("--kill-node", type=str, default=None,
                   help="node index(es) to kill, comma-separated (e.g. 2 or 2,5)")
    p.add_argument("--kill-at", type=str, default=None,
                   help="seconds into run to kill, comma-separated (e.g. 10 or 10,20)")
    p.add_argument("--sweep", type=str, default=None,
                   help="comma-separated node counts for multi-trial sweep (e.g. 4,6,8,10)")
    return p.parse_args()


def ssh_host(machine: str) -> str:
    return f"{SSH_USER}@{machine}.{DOMAIN}"


def run_ssh(machine: str, cmd: str, timeout: int = 10) -> str | None:
    try:
        result = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
             ssh_host(machine), cmd],
            capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except (subprocess.TimeoutExpired, Exception):
        return None


def get_load(machine: str) -> float | None:
    out = run_ssh(machine, "cat /proc/loadavg")
    if out:
        try:
            return float(out.split()[0])
        except (ValueError, IndexError):
            return None
    return None


def get_ip(machine: str) -> str | None:
    out = run_ssh(machine, "hostname -I")
    if out:
        return out.split()[0]
    return None


def pick_best_machines(n: int) -> list[tuple[str, float]]:
    print(f"Surveying {len(MACHINES)} machines for load averages...")
    loads: list[tuple[str, float]] = []
    for m in MACHINES:
        load = get_load(m)
        if load is not None:
            loads.append((m, load))
            print(f"  {m:12s}  load: {load:.2f}")
        else:
            print(f"  {m:12s}  unreachable")
    loads.sort(key=lambda x: x[1])
    return loads


def select_and_resolve(n: int) -> tuple[list[str], list[str]]:
    ranked = pick_best_machines(n * 2)
    print("Resolving IP addresses...")
    machines: list[str] = []
    ips: list[str] = []
    for name, load in ranked:
        if len(machines) >= n:
            break
        ip = get_ip(name)
        if ip is None:
            print(f"  {name:12s}  SKIPPED (could not resolve IP)")
            continue
        machines.append(name)
        ips.append(ip)
        print(f"  {name:12s}  ->  {ip}")

    if len(machines) < n:
        print(f"  ERROR: only found {len(machines)} usable machines (need {n})", file=sys.stderr)
        return [], []

    print(f"\nSelected machines: {machines}")
    return machines, ips


def launch_tmux(machines: list[str], ips: list[str]):
    addr_args = " ".join(f"{ip}:{PORT}" for ip in ips)

    subprocess.run(["tmux", "kill-session", "-t", TMUX_SESSION], capture_output=True)
    time.sleep(0.5)

    def wrap_cmd(machine, idx):
        outfile = f"{PROJECT_DIR}/output{idx}.txt"
        remote_cmd = f"{PROGRAM_PATH} {idx} {PORT} {addr_args} > {outfile} 2>&1"
        return (f"ssh {ssh_host(machine)} '{remote_cmd}'; "
                f"echo \"--- exited with code $? ---\"; read -p 'Press enter to close...'")

    subprocess.run(
        ["tmux", "new-session", "-d", "-s", TMUX_SESSION, "-n", machines[0],
         "bash", "-c", wrap_cmd(machines[0], 0)],
        check=True,
    )
    print(f"  Window 0: {machines[0]} (index=0)")

    for idx in range(1, len(machines)):
        machine = machines[idx]
        subprocess.run(
            ["tmux", "new-window", "-t", TMUX_SESSION, "-n", machine,
             "bash", "-c", wrap_cmd(machine, idx)],
            check=True,
        )
        print(f"  Window {idx}: {machine} (index={idx})")

    print(f"\nAll {len(machines)} nodes launched in tmux session '{TMUX_SESSION}'.")
    print(f"Attach with:  tmux attach -t {TMUX_SESSION}")


def kill_remote_node(machine: str, idx: int):
    print(f"\n*** Killing node {idx} ({machine}) ***")
    run_ssh(machine, f"pkill -KILL -f 'Replicated_Hash_Table.*{PORT}'", timeout=5)


def stop_nodes(machines: list[str], skip: set[int] | None = None):
    print("\nStopping remaining nodes...")
    for idx, machine in enumerate(machines):
        if skip and idx in skip:
            continue
        run_ssh(machine, f"pkill -INT -f 'Replicated_Hash_Table.*{PORT}'", timeout=5)
    time.sleep(10)


def collect_results(machines: list[str]) -> dict:
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    all_throughputs = []
    all_avg_latencies = []
    all_p50 = []
    all_p99 = []
    per_second: dict[int, dict[int, int]] = {}

    for idx, machine in enumerate(machines):
        outfile = f"{PROJECT_DIR}/output{idx}.txt"
        out = run_ssh(machine, f"cat {outfile}", timeout=10)

        print(f"\n--- Node {idx} ({machine}) ---")
        if out is None:
            print("  [Could not retrieve output]")
            continue

        print(out)

        node_ts: dict[int, int] = {}
        for line in out.splitlines():
            m = re.search(r"Throughput:\s+([\d.]+)", line)
            if m:
                all_throughputs.append(float(m.group(1)))
            m = re.search(r"Avg latency:\s+([\d.]+)", line)
            if m:
                all_avg_latencies.append(float(m.group(1)))
            m = re.search(r"P50 latency:\s+([\d.]+)", line)
            if m:
                all_p50.append(float(m.group(1)))
            m = re.search(r"P99 latency:\s+([\d.]+)", line)
            if m:
                all_p99.append(float(m.group(1)))
            m = re.match(r"\[T=(\d+)s\]\s+(\d+)\s+ops/s", line)
            if m:
                node_ts[int(m.group(1))] = int(m.group(2))

        if node_ts:
            per_second[idx] = node_ts

    stats: dict = {}
    if all_throughputs:
        stats["total_throughput"] = sum(all_throughputs)
        stats["avg_throughput"] = sum(all_throughputs) / len(all_throughputs)
        stats["avg_latency_us"] = sum(all_avg_latencies) / len(all_avg_latencies) if all_avg_latencies else 0
        stats["avg_p50_us"] = sum(all_p50) / len(all_p50) if all_p50 else 0
        stats["avg_p99_us"] = sum(all_p99) / len(all_p99) if all_p99 else 0

        print("\n" + "=" * 60)
        print("AGGREGATE SUMMARY")
        print("=" * 60)
        print(f"  Total throughput:  {stats['total_throughput']:.2f} ops/s")
        print(f"  Avg throughput:    {stats['avg_throughput']:.2f} ops/s per node")
        print(f"  Avg latency:       {stats['avg_latency_us']:.2f} us")
        print(f"  Avg P50 latency:   {stats['avg_p50_us']:.2f} us")
        print(f"  Avg P99 latency:   {stats['avg_p99_us']:.2f} us")
        print("=" * 60)
    else:
        print("\n[No performance stats found in output]")

    stats["per_second"] = per_second
    return stats


def download_logs(machines: list[str]):
    """SCP output files from remote machines into local_run_logs/ for plotting."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, "local_run_logs")
    os.makedirs(log_dir, exist_ok=True)

    print(f"\nDownloading logs to {log_dir}/ ...")
    for idx, machine in enumerate(machines):
        remote_path = f"{ssh_host(machine)}:{PROJECT_DIR}/output{idx}.txt"
        local_path = os.path.join(log_dir, f"node_{idx}.log")
        try:
            subprocess.run(
                ["scp", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
                 remote_path, local_path],
                capture_output=True, timeout=15,
            )
            print(f"  node {idx} ({machine}) -> {local_path}")
        except (subprocess.TimeoutExpired, Exception) as e:
            print(f"  node {idx} ({machine}) FAILED: {e}")

    print(f"\nRun 'python3 plot_throughput.py' to generate the graph.")


def write_per_second_csv(per_second: dict[int, dict[int, int]],
                         machines: list[str], path: str):
    if not per_second:
        return
    all_times: set[int] = set()
    for ts in per_second.values():
        all_times.update(ts.keys())
    if not all_times:
        return

    node_ids = sorted(per_second.keys())
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        header = ["time_s"] + [f"node_{i}_{machines[i]}" for i in node_ids] + ["aggregate"]
        writer.writerow(header)
        for t in sorted(all_times):
            row: list = [t]
            total = 0
            for i in node_ids:
                v = per_second[i].get(t, 0)
                row.append(v)
                total += v
            row.append(total)
            writer.writerow(row)

    print(f"Per-second throughput written to {path}")


def run_trial(num_nodes: int, duration: int,
              kill_nodes: list[int] | None = None,
              kill_times: list[int] | None = None) -> tuple[dict, list[str]]:
    print("\n" + "#" * 60)
    print(f"  TRIAL: {num_nodes} nodes, {duration}s")
    if kill_nodes and kill_times:
        desc = ", ".join(f"node {n} at T={t}s" for n, t in zip(kill_nodes, kill_times))
        print(f"  KILLS: {desc}")
    print("#" * 60)

    chosen, ips = select_and_resolve(num_nodes)
    if not chosen:
        print(f"  SKIPPED: not enough machines for {num_nodes} nodes")
        return {}, []

    launch_tmux(chosen, ips)

    killed_set: set[int] = set()

    if kill_nodes and kill_times:
        schedule = sorted(zip(kill_times, kill_nodes))
        desc = ", ".join(f"node {nd} at T={t}s" for t, nd in schedule)
        print(f"\nRunning {duration}s — will kill {desc} ...")

        elapsed = 0
        for kt, kn in schedule:
            wait = kt - elapsed
            if wait > 0:
                time.sleep(wait)
            elapsed = kt
            kill_remote_node(chosen[kn], kn)
            killed_set.add(kn)

        remaining = duration - elapsed
        if remaining > 0:
            time.sleep(remaining)
    else:
        print(f"\nRunning for {duration} seconds...")
        time.sleep(duration)

    stop_nodes(chosen, skip=killed_set)
    stats = collect_results(chosen)

    ts_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          f"throughput_{num_nodes}nodes.csv")
    write_per_second_csv(stats.get("per_second", {}), chosen, ts_csv)

    subprocess.run(["tmux", "kill-session", "-t", TMUX_SESSION], capture_output=True)
    print(f"\nTmux session '{TMUX_SESSION}' cleaned up.")

    download_logs(chosen)

    return stats, chosen


def main():
    args = parse_args()

    kill_nodes = [int(x) for x in args.kill_node.split(",")] if args.kill_node else None
    kill_times = None
    if kill_nodes:
        if args.kill_at:
            kill_times = [int(x) for x in args.kill_at.split(",")]
        else:
            kill_times = [args.duration // 2] * len(kill_nodes)
        while len(kill_times) < len(kill_nodes):
            kill_times.append(kill_times[-1])

    if args.sweep:
        node_counts = [int(x) for x in args.sweep.split(",")]
        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), CSV_FILE)

        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "num_nodes", "total_throughput_ops_s", "avg_throughput_ops_s",
                "avg_latency_us", "avg_p50_latency_us", "avg_p99_latency_us",
            ])
            for n in node_counts:
                stats, _ = run_trial(n, args.duration)
                if stats and "total_throughput" in stats:
                    writer.writerow([
                        n,
                        f"{stats['total_throughput']:.2f}",
                        f"{stats['avg_throughput']:.2f}",
                        f"{stats['avg_latency_us']:.2f}",
                        f"{stats['avg_p50_us']:.2f}",
                        f"{stats['avg_p99_us']:.2f}",
                    ])
                else:
                    writer.writerow([n, "", "", "", "", ""])
                f.flush()

        print(f"\n\nSweep results written to {csv_path}")
    else:
        run_trial(args.nodes, args.duration, kill_nodes, kill_times)


if __name__ == "__main__":
    main()
