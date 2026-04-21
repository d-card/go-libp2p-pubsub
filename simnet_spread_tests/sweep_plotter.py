#!/usr/bin/env python3
"""
Sweep plotter — generate plots from a sweep output directory.

Main plot: scatter Anonymity vs Stretch — each point is a parameter configuration,
each group is rendered as a connected line.

Reads:
  - <data-dir>/runs/<config_tag>/topo-<seed>.json  (raw exports)
  - a plot-groups YAML (can be different from the run config; lets you re-group
    the same data for different plots)

Also emits a per-config CSV with aggregated metrics, so downstream plots don't
need to re-read the raw data.

Usage (from repo root):
    python3 simnet_spread_tests/sweep_plotter.py \\
        --data-dir simnet_spread_tests/outputs/<sweep_name> \\
        --groups simnet_spread_tests/plot_groups_example.yaml \\
        [--attacker-pct 0.2]
"""

import argparse
import csv
import glob
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import yaml

sns.set_theme(style="whitegrid")


def config_tag(p_i, f_i, p_e, f_e):
    return f"ir{p_i}_ip{p_e}_if{f_i}_ef{f_e}"


def parse_tag(tag):
    m = re.match(r"ir([\d.]+)_ip([\d.]+)_if(\d+)_ef(\d+)", tag)
    if not m:
        return None
    return float(m.group(1)), int(m.group(3)), float(m.group(2)), int(m.group(4))


_TOPO_SEED_RE = re.compile(r"topo-(-?\d+)\.json$")


def load_config_data(config_dir, topology_filter=None):
    """Load topo-*.json files under one config dir.

    topology_filter: optional iterable of int seeds. If provided and non-empty,
    only topo-<seed>.json files whose seed is in the set are loaded. Seeds in
    the filter that have no corresponding file are silently skipped (common
    when a sweep hasn't run every seed for every config).
    """
    allowed = None
    if topology_filter:
        allowed = {int(s) for s in topology_filter}
    exports = []
    for path in sorted(glob.glob(os.path.join(config_dir, "topo-*.json"))):
        if allowed is not None:
            m = _TOPO_SEED_RE.search(path)
            if not m or int(m.group(1)) not in allowed:
                continue
        try:
            with open(path) as f:
                exports.append(json.load(f))
        except Exception as e:
            print(f"  warn: failed to load {path}: {e}", file=sys.stderr)
    return exports


def aggregate(exports, attacker_pct=0.2):
    """Compute per-config aggregated metrics across all topologies."""
    gs_s, sp_s, gs_a, sp_a = [], [], [], []
    gs_curve, sp_curve = defaultdict(list), defaultdict(list)

    for exp in exports:
        for t in exp.get("gossipsub", []):
            for d in t.get("deliveries", []):
                gs_s.append(d["stretch"])
        for t in exp.get("spread", []):
            for d in t.get("deliveries", []):
                sp_s.append(d["stretch"])

        for e in exp.get("gossipsub_attacker_accuracy", []):
            p = e["attacker_pct"]
            gs_curve[p].append(e["accuracy"])
            if abs(p - attacker_pct) < 0.01:
                gs_a.append(e["accuracy"])
        for e in exp.get("spread_attacker_accuracy", []):
            p = e["attacker_pct"]
            sp_curve[p].append(e["accuracy"])
            if abs(p - attacker_pct) < 0.01:
                sp_a.append(e["accuracy"])

    if not gs_s or not sp_s:
        return None

    return {
        "n_topologies":       len(exports),
        "n_deliveries_gs":    len(gs_s),
        "n_deliveries_sp":    len(sp_s),

        "gs_stretch_mean":    float(np.mean(gs_s)),
        "sp_stretch_mean":    float(np.mean(sp_s)),
        "gs_stretch_median":  float(np.median(gs_s)),
        "sp_stretch_median":  float(np.median(sp_s)),
        "gs_stretch_p90":     float(np.percentile(gs_s, 90)),
        "sp_stretch_p90":     float(np.percentile(sp_s, 90)),
        "gs_stretch_p99":     float(np.percentile(gs_s, 99)),
        "sp_stretch_p99":     float(np.percentile(sp_s, 99)),

        "gs_accuracy":        float(np.mean(gs_a)) if gs_a else None,
        "sp_accuracy":        float(np.mean(sp_a)) if sp_a else None,

        "gs_accuracy_curve":  {float(k): float(np.mean(v)) for k, v in gs_curve.items()},
        "sp_accuracy_curve":  {float(k): float(np.mean(v)) for k, v in sp_curve.items()},
    }


def build_config_index(data_dir, attacker_pct, topology_filter=None):
    """Scan <data_dir>/runs/* and return {tag: metrics}."""
    runs_dir = Path(data_dir) / "runs"
    index = {}
    if not runs_dir.is_dir():
        return index
    for cfg_dir in sorted(runs_dir.iterdir()):
        if not cfg_dir.is_dir():
            continue
        tag = cfg_dir.name
        params = parse_tag(tag)
        if params is None:
            continue
        exports = load_config_data(cfg_dir, topology_filter=topology_filter)
        if not exports:
            continue
        m = aggregate(exports, attacker_pct=attacker_pct)
        if m is None:
            continue
        p_i, f_i, p_e, f_e = params
        m["p_i"], m["f_i"], m["p_e"], m["f_e"] = p_i, f_i, p_e, f_e
        m["tag"] = tag
        index[tag] = m
    return index


def write_metrics_csv(index, out_path):
    cols = ["tag", "p_i", "f_i", "p_e", "f_e",
            "n_topologies", "n_deliveries_gs", "n_deliveries_sp",
            "gs_stretch_mean", "sp_stretch_mean",
            "gs_stretch_median", "sp_stretch_median",
            "gs_stretch_p90", "sp_stretch_p90",
            "gs_stretch_p99", "sp_stretch_p99",
            "gs_accuracy", "sp_accuracy"]
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for tag, m in sorted(index.items()):
            w.writerow([m.get(c) for c in cols])
    print(f"  Saved: {out_path}")


def chart_anon_vs_stretch(index, groups, out_path,
                          stretch_stat="mean", anon_stat="accuracy",
                          sort_key="p_i"):
    """
    Scatter: X = Spread mean stretch, Y = Spread deanon accuracy.
    One connected line per group. Points sorted by `sort_key` within a group.

    stretch_stat: "mean" | "median" | "p90" | "p99"
    anon_stat:    "accuracy" (raw deanon accuracy at attacker_pct) — lower = more anonymous
    """
    stretch_key = f"sp_stretch_{stretch_stat}"
    anon_key    = "sp_accuracy"

    fig, ax = plt.subplots(figsize=(11, 8))

    palette = sns.color_palette("tab10", n_colors=max(len(groups), 1))
    markers = ["o", "s", "D", "^", "v", "P", "X", "*", "h", "<"]

    # Plot GossipSub baseline (averaged across all configs we have)
    all_gs_stretch = [m[f"gs_stretch_{stretch_stat}"] for m in index.values()]
    all_gs_anon    = [m["gs_accuracy"] for m in index.values() if m["gs_accuracy"] is not None]
    if all_gs_stretch and all_gs_anon:
        ax.scatter(np.mean(all_gs_stretch), np.mean(all_gs_anon),
                   color="black", marker="*", s=260, zorder=10,
                   label="GossipSub baseline", edgecolors="white", linewidths=1.2)

    any_plotted = False
    for i, (group_name, cfg_list) in enumerate(sorted(groups.items())):
        # Resolve each [p_i, f_i, p_e, f_e] to a tag and look up metrics
        points = []
        missing = []
        for cfg in cfg_list:
            p_i, f_i, p_e, f_e = cfg
            tag = config_tag(p_i, f_i, p_e, f_e)
            if tag not in index:
                missing.append(tag)
                continue
            m = index[tag]
            if m.get(stretch_key) is None or m.get(anon_key) is None:
                continue
            points.append((m["p_i"], m["f_i"], m["p_e"], m["f_e"],
                           m[stretch_key], m[anon_key]))

        if missing:
            print(f"  warn: group '{group_name}' missing data for: {missing}")

        if not points:
            continue

        # Sort points by the chosen key for a meaningful line connection
        sort_idx = {"p_i": 0, "f_i": 1, "p_e": 2, "f_e": 3, "stretch": 4, "anon": 5}[sort_key]
        points.sort(key=lambda p: p[sort_idx])

        xs = [p[4] for p in points]
        ys = [p[5] for p in points]

        color = palette[i % len(palette)]
        marker = markers[i % len(markers)]
        ax.plot(xs, ys, "-", color=color, lw=1.5, alpha=0.7)
        ax.scatter(xs, ys, color=color, marker=marker, s=90,
                   edgecolors="white", linewidths=0.8,
                   label=f"{group_name}  (n={len(points)})",
                   zorder=5)

        # Annotate each point with (p_i, f_i, p_e, f_e)
        for p_i, f_i, p_e, f_e, x, y in points:
            ax.annotate(f"({p_i},{f_i},{p_e},{f_e})",
                        xy=(x, y), xytext=(4, 4), textcoords="offset points",
                        fontsize=5.5, alpha=0.7)

        any_plotted = True

    if not any_plotted:
        print("  warn: no data to plot")
        plt.close(fig)
        return

    ax.set_xlabel(f"Spread stretch ({stretch_stat})  →  worse", fontsize=11)
    ax.set_ylabel(f"Spread deanon accuracy  →  worse", fontsize=11)
    ax.set_title(
        "Anonymity vs Stretch — each point is a parameter config, each line a group.\n"
        "Lower-left = better trade-off (faster AND more anonymous).",
        fontsize=11)
    ax.legend(fontsize=8, loc="best")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", required=True, help="Sweep output directory")
    p.add_argument("--groups",   required=True, help="YAML file with plot groups")
    p.add_argument("--out-dir",  default=None, help="Chart output dir (default: <data-dir>/plots)")
    p.add_argument("--attacker-pct", type=float, default=0.2,
                   help="Attacker fraction for the deanon metric (default: 0.2)")
    p.add_argument("--stretch-stat", default="mean",
                   choices=["mean", "median", "p90", "p99"],
                   help="Which stretch statistic to use on the X axis (default: mean)")
    p.add_argument("--sort-key", default="p_i",
                   choices=["p_i", "f_i", "p_e", "f_e", "stretch", "anon"],
                   help="Within a group, connect points sorted by this (default: p_i)")
    args = p.parse_args()

    data_dir = Path(args.data_dir).resolve()
    out_dir = Path(args.out_dir) if args.out_dir else data_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(args.groups) as f:
        plot_cfg = yaml.safe_load(f)
    groups = plot_cfg.get("groups") or {}
    if not groups:
        print("No groups found in plot config.", file=sys.stderr)
        sys.exit(1)

    # Optional: filter which topology seeds feed into the plot. Empty/missing
    # means use every topo-*.json we find (current default behavior).
    topology_filter = plot_cfg.get("topologies") or None

    print(f"Loading data from: {data_dir}")
    print(f"Attacker pct:      {args.attacker_pct}")
    print(f"Stretch stat:      {args.stretch_stat}")
    if topology_filter:
        print(f"Topology filter:   {topology_filter}")

    index = build_config_index(data_dir, args.attacker_pct,
                               topology_filter=topology_filter)
    print(f"Loaded metrics for {len(index)} configs.\n")

    print(f"Writing outputs to {out_dir}/")
    write_metrics_csv(index, out_dir / f"metrics_attacker{args.attacker_pct}.csv")

    chart_anon_vs_stretch(
        index, groups,
        out_dir / f"scatter_anon_vs_stretch_{args.stretch_stat}.png",
        stretch_stat=args.stretch_stat,
        sort_key=args.sort_key,
    )

    print("\nDone.")


if __name__ == "__main__":
    main()
