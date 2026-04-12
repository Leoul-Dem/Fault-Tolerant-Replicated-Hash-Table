#!/usr/bin/env python3
"""
Local launcher for Replicated_Hash_Table (duplicate of startup.py, no SSH).

Runs N processes on 127.0.0.1 with consecutive ports, logs to local_run_logs/,
then sends SIGINT for graceful shutdown (same as the binary's main()).

Usage:
  ./startup_local.py
  ./startup_local.py --nodes 3 --duration 20 --base-port 7000
  ./startup_local.py --binary ./build/Replicated_Hash_Table
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

# ─── Defaults (mirror startup.py structure, local-only) ───────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_BINARY = SCRIPT_DIR / "build" / "Replicated_Hash_Table"
DEFAULT_NODES = 3
DEFAULT_BASE_PORT = 7000
DEFAULT_HOST = "127.0.0.1"
DEFAULT_DURATION = 30
DEFAULT_LOG_DIR = SCRIPT_DIR / "local_run_logs"
# ─────────────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Replicated_Hash_Table cluster locally.")
    p.add_argument("--nodes", type=int, default=DEFAULT_NODES, help="number of processes (default: 3)")
    p.add_argument("--base-port", type=int, default=DEFAULT_BASE_PORT, help="first listen port")
    p.add_argument("--host", default=DEFAULT_HOST, help="bind/connect address (default: 127.0.0.1)")
    p.add_argument("--duration", type=int, default=DEFAULT_DURATION, help="seconds before SIGINT")
    p.add_argument("--binary", type=Path, default=DEFAULT_BINARY, help="path to Replicated_Hash_Table")
    p.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR, help="where to write node logs")
    p.add_argument(
        "--stagger",
        type=float,
        default=0.25,
        help="seconds to sleep between spawning processes (default: 0.25)",
    )
    p.add_argument(
        "--kill-after",
        type=float,
        default=20.0,
        help="seconds to wait per process after SIGINT before SIGKILL (default: 20)",
    )
    p.add_argument("--no-wait", action="store_true", help="start nodes and exit (you stop them manually)")
    p.add_argument("--no-stdbuf", action="store_true", help="do not wrap binary with stdbuf (line-buffered stdio)")
    p.add_argument("--kill-node", type=str, default=None, help="node index(es) to kill, comma-separated (e.g. 2 or 2,5)")
    p.add_argument("--kill-at", type=str, default=None, help="seconds into run to kill, comma-separated (e.g. 10 or 10,20)")
    return p.parse_args()


def collect_stats_from_log(text: str) -> dict[str, float]:
    stats: dict[str, float] = {}
    for line in text.splitlines():
        m = re.search(r"Throughput:\s+([\d.]+)", line)
        if m:
            stats["throughput"] = float(m.group(1))
        m = re.search(r"Avg latency:\s+([\d.]+)", line)
        if m:
            stats["avg_latency_us"] = float(m.group(1))
        m = re.search(r"P50 latency:\s+([\d.]+)", line)
        if m:
            stats["p50_us"] = float(m.group(1))
        m = re.search(r"P99 latency:\s+([\d.]+)", line)
        if m:
            stats["p99_us"] = float(m.group(1))
    return stats


def main() -> int:
    args = parse_args()
    binary: Path = args.binary.expanduser().resolve()
    if not binary.is_file():
        print(f"Binary not found: {binary}", file=sys.stderr)
        print("Build first:  cd build && cmake .. && cmake --build .", file=sys.stderr)
        return 1

    n = args.nodes
    if n < 1:
        print("--nodes must be >= 1", file=sys.stderr)
        return 1

    host = args.host
    ports = [args.base_port + i for i in range(n)]
    addr_args = [f"{host}:{p}" for p in ports]
    addr_flat = " ".join(addr_args)

    log_dir: Path = args.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    def log(msg: str = "") -> None:
        print(msg, flush=True)

    log(f"Binary:   {binary}")
    log(f"Cluster:  {n} nodes on {host}, ports {ports[0]}..{ports[-1]}")
    log(f"Args:     <index> <my_port> {addr_flat}")
    log(f"Logs:     {log_dir}/")
    log()

    procs: list[subprocess.Popen] = []
    log_files: list[object] = []

    def command_for_node(idx: int) -> list[str]:
        argv = [str(idx), str(ports[idx]), *addr_args]
        exe = str(binary)
        return [exe, *argv]

    try:
        for idx in range(n):
            cmd = command_for_node(idx)
            log_path = log_dir / f"node_{idx}.log"
            lf = open(log_path, "w", encoding="utf-8", buffering=1)
            log_files.append(lf)
            proc = subprocess.Popen(
                cmd,
                stdout=lf,
                stderr=subprocess.STDOUT,
                cwd=str(SCRIPT_DIR),
                preexec_fn=os.setpgrp,
            )
            procs.append(proc)
            log(f"  started node {idx}  pid={proc.pid}  port={ports[idx]}  -> {log_path.name}")
            if idx + 1 < n and args.stagger > 0:
                time.sleep(args.stagger)

        if args.no_wait:
            log("\nNodes running (--no-wait). Stop with: kill -INT <pids>")
            log("PIDs: " + ", ".join(str(p.pid) for p in procs))
            return 0

        kill_nodes = [int(x) for x in args.kill_node.split(",")] if args.kill_node is not None else []
        if args.kill_at is not None:
            kill_times = [int(x) for x in args.kill_at.split(",")]
        else:
            kill_times = [args.duration // 2] * len(kill_nodes)
        # Pad kill_times if fewer than kill_nodes
        while len(kill_times) < len(kill_nodes):
            kill_times.append(kill_times[-1] if kill_times else args.duration // 2)

        if kill_nodes:
            # Build schedule: list of (time, node_idx) sorted by time
            schedule = sorted(zip(kill_times, kill_nodes))
            desc = ", ".join(f"node {nd} at T={t}s" for t, nd in schedule)
            log(f"\nRunning {args.duration}s — will kill {desc} ...")

            elapsed = 0
            for kill_t, kill_nd in schedule:
                wait = kill_t - elapsed
                if wait > 0:
                    time.sleep(wait)
                elapsed = kill_t
                p = procs[kill_nd]
                if p.poll() is None:
                    log(f"\n*** T={kill_t}s: killing node {kill_nd} (pid {p.pid}) ***")
                    try:
                        os.killpg(p.pid, signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        p.kill()
            remaining = args.duration - elapsed
            if remaining > 0:
                time.sleep(remaining)
        else:
            log(f"\nRunning {args.duration}s (SIGINT = graceful shutdown)...")
            time.sleep(args.duration)

        log("\nSending SIGINT to all process groups...")
        for p in procs:
            if p.poll() is None:
                try:
                    os.killpg(p.pid, signal.SIGINT)
                except (ProcessLookupError, PermissionError):
                    pass

        kill_after = max(1.0, args.kill_after)
        for p in procs:
            try:
                p.wait(timeout=kill_after)
            except subprocess.TimeoutExpired:
                print(f"  pid {p.pid} did not exit after SIGINT; sending SIGKILL", file=sys.stderr, flush=True)
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    p.kill()
                try:
                    p.wait(timeout=5)
                except Exception:
                    pass

        time.sleep(0.3)

    finally:
        for lf in log_files:
            try:
                lf.close()
            except Exception:
                pass

    for i, p in enumerate(procs):
        rc = p.returncode
        sig = ""
        if rc is not None and rc < 0:
            sig = f" (signal {-rc})"
        log(f"node {i} exit code: {rc}{sig}")

    # Summarize from logs (same spirit as startup.py collect_results)
    log("\n" + "=" * 60)
    log("LOG SUMMARY (per node)")
    log("=" * 60)
    throughputs: list[float] = []
    for idx in range(n):
        log_path = log_dir / f"node_{idx}.log"
        text = log_path.read_text(encoding="utf-8", errors="replace")
        log(f"\n--- node {idx} ({log_path.name}) ---")
        tail = "\n".join(text.splitlines()[-25:])
        log(tail if tail.strip() else "(empty)")

        st = collect_stats_from_log(text)
        if "throughput" in st:
            throughputs.append(st["throughput"])

    if throughputs:
        log("\n" + "=" * 60)
        log(f"Sum throughput (nodes reporting): {sum(throughputs):.2f} ops/s")
        log("=" * 60)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
