#!/usr/bin/env python3

import csv
import matplotlib.pyplot as plt

# Read data
nodes, total_tp, avg_tp, avg_lat, p50, p99 = [], [], [], [], [], []
with open("results.csv") as f:
    for row in csv.DictReader(f):
        nodes.append(int(row["num_nodes"]))
        total_tp.append(float(row["total_throughput_ops_s"]))
        avg_tp.append(float(row["avg_throughput_ops_s"]))
        avg_lat.append(float(row["avg_latency_us"]) / 1000)
        p50.append(float(row["avg_p50_latency_us"]) / 1000)
        p99.append(float(row["avg_p99_latency_us"]) / 1000)

# 1) Total Throughput
fig, ax = plt.subplots(figsize=(7, 5))
ax.plot(nodes, total_tp, "o-", color="tab:blue", linewidth=2, markersize=8)
ax.set_xlabel("Number of Nodes")
ax.set_ylabel("Total Throughput (ops/s)")
ax.set_title("Total Throughput vs Nodes\n(60% GET, 20% PUT, 20% PUT3)")
ax.set_xticks(nodes)
ax.grid(True, alpha=0.3)
for x, y in zip(nodes, total_tp):
    ax.annotate(f"{y:,.0f}", (x, y), textcoords="offset points", xytext=(0, 10), ha="center", fontsize=9)
plt.tight_layout()
plt.savefig("plot_total_throughput.png", dpi=150)
print("Saved plot_total_throughput.png")

# 2) Per-Node Throughput
fig, ax = plt.subplots(figsize=(7, 5))
ax.plot(nodes, avg_tp, "o-", color="tab:orange", linewidth=2, markersize=8)
ax.set_xlabel("Number of Nodes")
ax.set_ylabel("Avg Throughput per Node (ops/s)")
ax.set_title("Per-Node Throughput vs Nodes\n(60% GET, 20% PUT, 20% PUT3)")
ax.set_xticks(nodes)
ax.grid(True, alpha=0.3)
for x, y in zip(nodes, avg_tp):
    ax.annotate(f"{y:,.0f}", (x, y), textcoords="offset points", xytext=(0, 10), ha="center", fontsize=9)
plt.tight_layout()
plt.savefig("plot_per_node_throughput.png", dpi=150)
print("Saved plot_per_node_throughput.png")

# 3) All Latencies (Avg, P50, P99)
fig, ax = plt.subplots(figsize=(7, 5))
ax.plot(nodes, avg_lat, "o-", color="tab:red", linewidth=2, markersize=8, label="Avg")
ax.plot(nodes, p50, "o-", color="tab:green", linewidth=2, markersize=8, label="P50")
ax.plot(nodes, p99, "s-", color="tab:purple", linewidth=2, markersize=8, label="P99")
ax.set_xlabel("Number of Nodes")
ax.set_ylabel("Latency (ms)")
ax.set_title("Latency vs Nodes\n(60% GET, 20% PUT, 20% PUT3)")
ax.set_xticks(nodes)
ax.legend()
ax.grid(True, alpha=0.3)
for x, ya, y50, y99 in zip(nodes, avg_lat, p50, p99):
    ax.annotate(f"{ya:.2f}", (x, ya), textcoords="offset points", xytext=(10, -5), ha="left", fontsize=9)
    ax.annotate(f"{y50:.2f}", (x, y50), textcoords="offset points", xytext=(10, -5), ha="left", fontsize=9)
    ax.annotate(f"{y99:.2f}", (x, y99), textcoords="offset points", xytext=(0, 10), ha="center", fontsize=9)
plt.tight_layout()
plt.savefig("plot_latency.png", dpi=150)
print("Saved plot_latency.png")
