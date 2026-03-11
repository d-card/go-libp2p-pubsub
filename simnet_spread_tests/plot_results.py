#!/usr/bin/env python3
"""
Plot results from simnet spread experiments.

Usage (from repo root):
    python simnet_spread_tests/plot_results.py [--results PATH] [--out DIR]

Requirements:
    pip install matplotlib seaborn numpy
"""

import argparse
import json
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# ── Style ─────────────────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", palette="Set2")
PROTOCOL_COLORS = {"gossipsub": "#e07b54", "spread": "#5b8db8"}
PROTOCOL_LABELS = {"gossipsub": "GossipSub", "spread": "Spread"}
MARKERS = {"gossipsub": "o", "spread": "s"}

# Categorical palette + distinct markers for per-N scatter plots
_N_PALETTE = sns.color_palette("tab10")
_N_MARKERS  = ["o", "X", "s", "P", "^", "D", "v", "*"]


def n_styles(ns: list) -> dict:
    """Return {n: (color, marker)} for each unique N value, sorted ascending."""
    unique = sorted(set(ns))
    return {
        n: (_N_PALETTE[i % len(_N_PALETTE)], _N_MARKERS[i % len(_N_MARKERS)])
        for i, n in enumerate(unique)
    }


# ── Data loading ──────────────────────────────────────────────────────────────

def load_results(path: str) -> list:
    """Load successful runs from results.jsonl, keeping last attempt per run_id."""
    by_run: dict = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("status") != "success":
                continue
            rid = rec["run_id"]
            if rid not in by_run or rec["attempt"] > by_run[rid]["attempt"]:
                by_run[rid] = rec
    return list(by_run.values())


def extract_rows(records: list) -> list:
    """Flatten each record into a flat dict of all stats."""
    rows = []
    for r in records:
        cfg = r["config"]
        m = r["metrics"]
        row = {"run_id": r["run_id"], "nodes": cfg["nodes"]}
        for proto in ("gossipsub", "spread"):
            for stat in ("latency", "stretch"):
                for q in ("min", "mean", "max", "p50", "p95", "p99", "stddev"):
                    row[f"{proto}_{stat}_{q}"] = m[proto][stat][q]
        rows.append(row)
    return rows


def grouped_by_n(rows: list) -> dict:
    g: dict = defaultdict(list)
    for r in rows:
        g[r["nodes"]].append(r)
    return dict(sorted(g.items()))


# ── I/O helper ────────────────────────────────────────────────────────────────

def savefig(fig, out_dir: str, name: str):
    path = os.path.join(out_dir, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {path}")


# ── Reusable plot primitives ──────────────────────────────────────────────────

def plot_metric_vs_n(ax, groups: dict, key: str, ylabel: str, title: str):
    """Line chart with per-run dots and mean±std across replicated runs."""
    ns = sorted(groups.keys())
    for proto in ("gossipsub", "spread"):
        col = f"{proto}_{key}"
        means, stds, ns_plot = [], [], []
        for n in ns:
            vals = [r[col] for r in groups[n]]
            means.append(np.mean(vals))
            stds.append(np.std(vals))
            ns_plot.append(n)
            for v in vals:
                ax.scatter(n, v, color=PROTOCOL_COLORS[proto],
                           alpha=0.35, s=25, zorder=3)
        ax.errorbar(ns_plot, means, yerr=stds,
                    label=PROTOCOL_LABELS[proto],
                    color=PROTOCOL_COLORS[proto],
                    marker=MARKERS[proto], linewidth=2, markersize=7,
                    capsize=4, zorder=4)
    ax.set_xlabel("Nodes (N)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    ax.set_xticks(ns)


def _scatter_one(ax, rows: list, key: str, title: str, xlabel: str, ylabel: str):
    """
    Scatter on a given axes: X = GossipSub value, Y = Spread value.
    One point per run. Each N gets a unique color + marker.
    Dashed x=y diagonal drawn for reference.
    """
    styles = n_styles([r["nodes"] for r in rows])
    gvals = [r[f"gossipsub_{key}"] for r in rows]
    svals = [r[f"spread_{key}"] for r in rows]

    lo = min(min(gvals), min(svals)) * 0.95
    hi = max(max(gvals), max(svals)) * 1.05
    ax.plot([lo, hi], [lo, hi], "k--", linewidth=1.5, alpha=0.65,
            label="X = Y", zorder=1)

    seen_n: set = set()
    for r, g, s in zip(rows, gvals, svals):
        n = r["nodes"]
        color, marker = styles[n]
        lbl = str(n) if n not in seen_n else None
        seen_n.add(n)
        ax.scatter(g, s, color=color, marker=marker, s=80, zorder=3,
                   label=lbl, edgecolors="white", linewidths=0.5)

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(title="N", fontsize=9, title_fontsize=9)


# ── Individual chart functions ────────────────────────────────────────────────

def chart_latency_vs_n(rows, groups, out_dir):
    """2×2: mean, p50, p95, p99 latency vs N."""
    fig, axes = plt.subplots(2, 2, figsize=(12, 9))
    specs = [
        ("latency_mean", "Latency (ms)", "Mean Latency vs N"),
        ("latency_p50",  "Latency (ms)", "Median (p50) Latency vs N"),
        ("latency_p95",  "Latency (ms)", "p95 Latency vs N"),
        ("latency_p99",  "Latency (ms)", "p99 Latency vs N"),
    ]
    for ax, (key, ylabel, title) in zip(axes.flat, specs):
        plot_metric_vs_n(ax, groups, key, ylabel, title)
    fig.suptitle("Latency vs Network Size", fontsize=13, y=1.01)
    fig.tight_layout()
    savefig(fig, out_dir, "01_latency_vs_n.png")


def chart_stretch_vs_n(rows, groups, out_dir):
    """2×2: mean, p50, p95, p99 stretch vs N."""
    fig, axes = plt.subplots(2, 2, figsize=(12, 9))
    specs = [
        ("stretch_mean", "Stretch (×)", "Mean Stretch vs N"),
        ("stretch_p50",  "Stretch (×)", "Median (p50) Stretch vs N"),
        ("stretch_p95",  "Stretch (×)", "p95 Stretch vs N"),
        ("stretch_p99",  "Stretch (×)", "p99 Stretch vs N"),
    ]
    for ax, (key, ylabel, title) in zip(axes.flat, specs):
        plot_metric_vs_n(ax, groups, key, ylabel, title)
    fig.suptitle("Stretch vs Network Size", fontsize=13, y=1.01)
    fig.tight_layout()
    savefig(fig, out_dir, "02_stretch_vs_n.png")


def chart_variability_vs_n(rows, groups, out_dir):
    """1×2: latency stddev and stretch stddev vs N."""
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    plot_metric_vs_n(axes[0], groups, "latency_stddev",
                     "Latency StdDev (ms)", "Latency StdDev vs N")
    plot_metric_vs_n(axes[1], groups, "stretch_stddev",
                     "Stretch StdDev (×)", "Stretch StdDev vs N")
    fig.suptitle("Variability vs Network Size", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "03_variability_vs_n.png")


def chart_tail_ratio_vs_n(rows, groups, out_dir):
    """1×2: p99/p50 tail ratio for latency and stretch vs N."""
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for ax, stat in [(axes[0], "latency"), (axes[1], "stretch")]:
        ns = sorted(groups.keys())
        for proto in ("gossipsub", "spread"):
            ratio_means, ns_plot = [], []
            for n in ns:
                ratios = [
                    r[f"{proto}_{stat}_p99"] / r[f"{proto}_{stat}_p50"]
                    for r in groups[n]
                    if r[f"{proto}_{stat}_p50"] > 0
                ]
                if not ratios:
                    continue
                ratio_means.append(np.mean(ratios))
                ns_plot.append(n)
                for v in ratios:
                    ax.scatter(n, v, color=PROTOCOL_COLORS[proto],
                               alpha=0.35, s=25, zorder=3)
            ax.plot(ns_plot, ratio_means, label=PROTOCOL_LABELS[proto],
                    color=PROTOCOL_COLORS[proto], marker=MARKERS[proto],
                    linewidth=2, markersize=7, zorder=4)
        ax.axhline(1.0, color="gray", linestyle=":", linewidth=1)
        ax.set_xlabel("Nodes (N)")
        ax.set_ylabel("p99 / p50 ratio")
        ax.set_title(f"{stat.capitalize()} Tail Ratio (p99/p50) vs N")
        ax.set_xticks(ns)
        ax.legend()
    fig.suptitle("Tail Heaviness vs Network Size", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "04_tail_ratio_vs_n.png")


def chart_scatter_latency_mean(rows, out_dir):
    """Scatter: mean latency — X=GossipSub, Y=Spread, coloured+marked by N."""
    fig, ax = plt.subplots(figsize=(6, 6))
    _scatter_one(
        ax, rows, "latency_mean",
        "Latency Scatter by N (mean)",
        "Gossipsub Latency mean (ms) [X]",
        "Spread Latency mean (ms) [Y]",
    )
    fig.tight_layout()
    savefig(fig, out_dir, "05_scatter_latency_mean.png")


def chart_scatter_latency_p50(rows, out_dir):
    """Scatter: p50 latency — X=GossipSub, Y=Spread, coloured+marked by N."""
    fig, ax = plt.subplots(figsize=(6, 6))
    _scatter_one(
        ax, rows, "latency_p50",
        "Latency Scatter by N (p50)",
        "Gossipsub Latency p50 (ms) [X]",
        "Spread Latency p50 (ms) [Y]",
    )
    fig.tight_layout()
    savefig(fig, out_dir, "06_scatter_latency_p50.png")


def chart_scatter_stretch_mean(rows, out_dir):
    """Scatter: mean stretch — X=GossipSub, Y=Spread, coloured+marked by N."""
    fig, ax = plt.subplots(figsize=(6, 6))
    _scatter_one(
        ax, rows, "stretch_mean",
        "Stretch Scatter by N (mean)",
        "Gossipsub Stretch mean (×) [X]",
        "Spread Stretch mean (×) [Y]",
    )
    fig.tight_layout()
    savefig(fig, out_dir, "07_scatter_stretch_mean.png")


def chart_scatter_stretch_p50(rows, out_dir):
    """Scatter: p50 stretch — X=GossipSub, Y=Spread, coloured+marked by N."""
    fig, ax = plt.subplots(figsize=(6, 6))
    _scatter_one(
        ax, rows, "stretch_p50",
        "Stretch Scatter by N (p50)",
        "Gossipsub Stretch p50 (×) [X]",
        "Spread Stretch p50 (×) [Y]",
    )
    fig.tight_layout()
    savefig(fig, out_dir, "08_scatter_stretch_p50.png")


def chart_improvement_heatmap(rows, groups, out_dir):
    """
    Heatmap of % improvement of Spread over GossipSub per (metric × N).
    Positive values mean Spread is better (lower).
    """
    ns = sorted(groups.keys())
    metrics = [
        ("latency_mean",   "Latency mean"),
        ("latency_p50",    "Latency p50"),
        ("latency_p95",    "Latency p95"),
        ("latency_p99",    "Latency p99"),
        ("latency_stddev", "Latency stddev"),
        ("stretch_mean",   "Stretch mean"),
        ("stretch_p50",    "Stretch p50"),
        ("stretch_p95",    "Stretch p95"),
        ("stretch_p99",    "Stretch p99"),
        ("stretch_stddev", "Stretch stddev"),
    ]
    data = np.zeros((len(metrics), len(ns)))
    for j, n in enumerate(ns):
        grp = groups[n]
        for i, (key, _) in enumerate(metrics):
            g_mean = np.mean([r[f"gossipsub_{key}"] for r in grp])
            s_mean = np.mean([r[f"spread_{key}"] for r in grp])
            if g_mean != 0:
                data[i, j] = (g_mean - s_mean) / abs(g_mean) * 100

    fig_w = max(6, len(ns) * 1.8)
    fig, ax = plt.subplots(figsize=(fig_w, 6))
    sns.heatmap(
        data,
        ax=ax,
        annot=True, fmt=".1f",
        xticklabels=[f"N={n}" for n in ns],
        yticklabels=[m[1] for m in metrics],
        center=0,
        cmap="RdYlGn",
        linewidths=0.5,
        cbar_kws={"label": "% improvement (positive = Spread wins)"},
    )
    ax.set_title("Spread vs GossipSub: % Improvement per Metric per N")
    fig.tight_layout()
    savefig(fig, out_dir, "09_improvement_heatmap.png")


def chart_distributions(rows, out_dir):
    """
    2×2 histogram / density comparison:
      - Latency p50 distribution (both protocols)
      - Latency p95 distribution (both protocols)
      - Stretch p50 distribution (both protocols)
      - Delta latency p50 (Spread − GossipSub) with x=0 reference line
    All runs are pooled across N values.
    """
    g_lat_p50 = [r["gossipsub_latency_p50"] for r in rows]
    s_lat_p50 = [r["spread_latency_p50"]    for r in rows]
    g_lat_p95 = [r["gossipsub_latency_p95"] for r in rows]
    s_lat_p95 = [r["spread_latency_p95"]    for r in rows]
    g_str_p50 = [r["gossipsub_stretch_p50"] for r in rows]
    s_str_p50 = [r["spread_stretch_p50"]    for r in rows]

    fig, axes = plt.subplots(2, 2, figsize=(11, 8))

    def _hist(ax, g_vals, s_vals, title, xlabel):
        bins = min(20, max(5, len(g_vals) // 2))
        ax.hist(g_vals, bins=bins, alpha=0.5, density=True,
                color=PROTOCOL_COLORS["gossipsub"],
                label=PROTOCOL_LABELS["gossipsub"])
        ax.hist(s_vals, bins=bins, alpha=0.5, density=True,
                color=PROTOCOL_COLORS["spread"],
                label=PROTOCOL_LABELS["spread"])
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("Density")
        ax.legend(fontsize=8)

    _hist(axes[0, 0], g_lat_p50, s_lat_p50,
          "Latency p50 Distribution", "ms")
    _hist(axes[0, 1], g_lat_p95, s_lat_p95,
          "Latency p95 Distribution", "ms")
    _hist(axes[1, 0], g_str_p50, s_str_p50,
          "Stretch p50 Distribution", "stretch (×)")

    # Delta: Spread − GossipSub for latency p50
    deltas = [s - g for s, g in zip(s_lat_p50, g_lat_p50)]
    ax = axes[1, 1]
    bins = min(20, max(5, len(deltas) // 2))
    ax.hist(deltas, bins=bins, density=True, color="#4c9e8e", alpha=0.85)
    ax.axvline(0, color="black", linestyle="--", linewidth=1.5)
    ax.set_title("Delta Latency p50 (Spread − GossipSub)")
    ax.set_xlabel("ms")
    ax.set_ylabel("Density")

    fig.suptitle("Distribution Comparison — all runs pooled", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "10_distributions.png")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    default_results = os.path.join(
        "simnet_spread_tests", "outputs", "results.jsonl"
    )
    default_out = os.path.join(
        "simnet_spread_tests", "outputs", "plots"
    )

    parser = argparse.ArgumentParser(
        description="Plot simnet spread experiment results."
    )
    parser.add_argument(
        "--results",
        default=default_results,
        help=f"Path to results.jsonl (default: {default_results})",
    )
    parser.add_argument(
        "--out",
        default=default_out,
        help=f"Output directory for plots (default: {default_out})",
    )
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)

    print(f"Loading results from {args.results} ...")
    records = load_results(args.results)
    if not records:
        print("No successful runs found in results.jsonl.", file=sys.stderr)
        sys.exit(1)
    print(f"  {len(records)} successful run(s) loaded.")

    rows = extract_rows(records)
    groups = grouped_by_n(rows)
    ns = sorted(groups.keys())
    print(f"  N values: {ns}")
    for n in ns:
        print(f"    N={n}: {len(groups[n])} run(s)")

    print("Generating charts ...")
    chart_latency_vs_n(rows, groups, args.out)
    chart_stretch_vs_n(rows, groups, args.out)
    chart_variability_vs_n(rows, groups, args.out)
    chart_tail_ratio_vs_n(rows, groups, args.out)
    chart_scatter_latency_mean(rows, args.out)
    chart_scatter_latency_p50(rows, args.out)
    chart_scatter_stretch_mean(rows, args.out)
    chart_scatter_stretch_p50(rows, args.out)
    chart_improvement_heatmap(rows, groups, args.out)
    chart_distributions(rows, args.out)
    print("Done.")


if __name__ == "__main__":
    main()
