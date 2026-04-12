#!/usr/bin/env python3

import subprocess
import sys
import time
import re
import csv
import os

# ─── Configuration ───────────────────────────────────────────────────────────
SSH_USER = "lgd226"
DOMAIN = "cse.lehigh.edu"
PROGRAM_PATH = "~/temp/Replicated-Hash-Table/build/Replicated_Hash_Table"
NODE_COUNTS = [4, 6, 8, 10]
PORT = 6007
TMUX_SESSION = "rht"
RUN_DURATION = 60  # seconds
CSV_FILE = "results.csv"

MACHINES = [
    "ariel", "caliban", "callisto", "ceres", "chiron", "cupid",
    "eris", "europa", "hydra", "iapetus", "io", "ixion",
    "mars", "mercury", "neptune", "nereid", "nix", "orcus",
    "phobos", "puck", "saturn", "triton", "varda", "vesta", "xena",
]
# ─────────────────────────────────────────────────────────────────────────────


def ssh_host(machine: str) -> str:
    return f"{SSH_USER}@{machine}.{DOMAIN}"


def run_ssh(machine: str, cmd: str, timeout: int = 10) -> str | None:
    """Run a command on a remote machine via SSH. Returns stdout or None on failure."""
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
    """Get 1-minute load average from a machine."""
    out = run_ssh(machine, "cat /proc/loadavg")
    if out:
        try:
            return float(out.split()[0])
        except (ValueError, IndexError):
            return None
    return None


def get_ip(machine: str) -> str | None:
    """Resolve the IP address of a machine."""
    out = run_ssh(machine, "hostname -I")
    if out:
        return out.split()[0]
    return None


def pick_best_machines(n: int) -> list[str]:
    """Survey all machines and return the n with the lowest load."""
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
    """Pick n machines with lowest load that also resolve an IP."""
    ranked = pick_best_machines(n * 2)  # get extra candidates in case some fail

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
    """Create a tmux session with one window per node, each running the program."""
    # Build ip:port list for all nodes
    # Usage: <program> <index> <my_port> <ip:port1> <ip:port2> ... <ip:portN>
    addr_args = " ".join(f"{ip}:{PORT}" for ip in ips)

    # Kill any existing session with the same name
    subprocess.run(["tmux", "kill-session", "-t", TMUX_SESSION],
                   capture_output=True)
    time.sleep(0.5)

    # Wrap commands so the window stays open on exit (shows errors)
    def wrap_cmd(machine, idx):
        outfile = f"~/temp/Replicated-Hash-Table/output{idx}.txt"
        remote_cmd = f"{PROGRAM_PATH} {idx} {PORT} {addr_args} > {outfile} 2>&1"
        return f"ssh {ssh_host(machine)} '{remote_cmd}'; echo \"--- exited with code $? ---\"; read -p 'Press enter to close...'"

    # Create a detached tmux session with the first node
    subprocess.run(
        ["tmux", "new-session", "-d", "-s", TMUX_SESSION, "-n", machines[0],
         "bash", "-c", wrap_cmd(machines[0], 0)],
        check=True,
    )
    print(f"  Window 0: {machines[0]} (index=0)")

    # Create additional windows for remaining nodes
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


def stop_nodes(machines: list[str]):
    """Kill the remote processes via SSH."""
    print("\nStopping all nodes...")
    for machine in machines:
        run_ssh(machine, f"pkill -INT -f 'Replicated_Hash_Table.*{PORT}'", timeout=5)
    time.sleep(10)  # let nodes flush output files


def collect_results(machines: list[str]) -> dict:
    """Fetch output files from each node and return parsed stats."""
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    all_throughputs = []
    all_avg_latencies = []
    all_p50 = []
    all_p99 = []

    for idx, machine in enumerate(machines):
        outfile = f"~/temp/Replicated-Hash-Table/output{idx}.txt"
        out = run_ssh(machine, f"cat {outfile}", timeout=10)

        print(f"\n--- Node {idx} ({machine}) ---")
        if out is None:
            print("  [Could not retrieve output]")
            continue

        print(out)

        # Parse stats
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

    stats = {}
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

    return stats


def run_trial(num_nodes: int) -> dict:
    """Run a single trial with the given number of nodes."""
    print("\n" + "#" * 60)
    print(f"  TRIAL: {num_nodes} nodes")
    print("#" * 60)

    chosen, ips = select_and_resolve(num_nodes)
    if not chosen:
        print(f"  SKIPPED: not enough machines for {num_nodes} nodes")
        return {}
    launch_tmux(chosen, ips)

    print(f"\nRunning for {RUN_DURATION} seconds...")
    time.sleep(RUN_DURATION)

    stop_nodes(chosen)
    stats = collect_results(chosen)

    # Kill tmux session
    subprocess.run(["tmux", "kill-session", "-t", TMUX_SESSION], capture_output=True)
    print(f"\nTmux session '{TMUX_SESSION}' cleaned up.")

    return stats


def main():
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), CSV_FILE)

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "num_nodes",
            "total_throughput_ops_s",
            "avg_throughput_ops_s",
            "avg_latency_us",
            "avg_p50_latency_us",
            "avg_p99_latency_us",
        ])

        for n in NODE_COUNTS:
            stats = run_trial(n)
            if stats:
                writer.writerow([
                    n,
                    f"{stats['total_throughput']:.2f}",
                    f"{stats['avg_throughput']:.2f}",
                    f"{stats['avg_latency_us']:.2f}",
                    f"{stats['avg_p50_us']:.2f}",
                    f"{stats['avg_p99_us']:.2f}",
                ])
                f.flush()
            else:
                writer.writerow([n, "", "", "", "", ""])
                f.flush()

    print(f"\n\nResults written to {csv_path}")


if __name__ == "__main__":
    main()
