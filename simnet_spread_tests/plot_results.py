#!/usr/bin/env python3
"""
Plot results from simnet spread experiments (raw per-pair data).

Usage (from repo root):
    python simnet_spread_tests/plot_results.py [--runs-dir PATH] [--results PATH] [--out DIR] [--group-by {n_seed,n}]

Requirements:
    pip install matplotlib seaborn numpy pandas
"""

import argparse
import json
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# -- Style ------------------------------------------------------------------
sns.set_theme(style="whitegrid", palette="Set2")
PROTOCOL_COLORS = {"gossipsub": "#e07b54", "spread": "#5b8db8"}
PROTOCOL_LABELS = {"gossipsub": "GossipSub", "spread": "Spread"}
MARKERS = {"gossipsub": "o", "spread": "s"}

_N_PALETTE = sns.color_palette("tab10")
_N_MARKERS = ["o", "X", "s", "P", "^", "D", "v", "*"]


def n_styles(ns):
    unique = sorted(set(ns))
    return {
        n: (_N_PALETTE[i % len(_N_PALETTE)], _N_MARKERS[i % len(_N_MARKERS)])
        for i, n in enumerate(unique)
    }


# -- Data loading -----------------------------------------------------------

def load_run_exports(runs_dir: str, results_path: str) -> list:
    """
    Load per-run JSON export files.  Use the results.jsonl to find which runs
    succeeded and their export paths, then read the export JSON for each.
    """
    exports = []

    # First try reading results.jsonl to find successful runs
    if results_path and os.path.isfile(results_path):
        by_run = {}
        with open(results_path) as f:
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
        for rid in sorted(by_run.keys()):
            epath = by_run[rid].get("export_path", "")
            if epath and os.path.isfile(epath):
                with open(epath) as f:
                    exports.append(json.load(f))
        if exports:
            return exports

    # Fallback: scan runs/ directory directly
    runs_path = Path(runs_dir)
    if not runs_path.is_dir():
        return exports
    for p in sorted(runs_path.glob("*.json")):
        with open(p) as f:
            exports.append(json.load(f))
    return exports


def flatten_observations(exports: list) -> pd.DataFrame:
    """
    Flatten nested JSON exports into a single DataFrame with columns:
        run_idx, n, seed, protocol, trial, src, dst, latency_ms, stretch
    """
    rows = []
    for run_idx, exp in enumerate(exports):
        n = exp["nodes"]
        seed = exp["seed"]
        for proto in ("gossipsub", "spread"):
            for tr in exp.get(proto, []):
                trial = tr["trial"]
                src = tr["src"]
                for d in tr["deliveries"]:
                    rows.append({
                        "run_idx": run_idx,
                        "n": n,
                        "seed": seed,
                        "protocol": proto,
                        "trial": trial,
                        "src": src,
                        "dst": d["dst"],
                        "latency_ms": d["latency_ms"],
                        "stretch": d["stretch"],
                    })
    return pd.DataFrame(rows)


def build_per_pair_stats(df: pd.DataFrame, group_by: str) -> pd.DataFrame:
    """
    Compute per-pair statistics.
    group_by = 'n_seed': group by (n, seed, protocol, src, dst)
    group_by = 'n':      group by (n, protocol, src, dst)
    """
    if group_by == "n_seed":
        grp_cols = ["n", "seed", "protocol", "src", "dst"]
    else:
        grp_cols = ["n", "protocol", "src", "dst"]

    # Use groupby().quantile([...]) instead of lambdas + np.percentile: lambdas force
    # Python groupby aggregation and are very slow for large group counts.
    gb = df.groupby(grp_cols, sort=False)
    means = gb.agg(
        latency_mean=("latency_ms", "mean"),
        latency_std=("latency_ms", "std"),
        stretch_mean=("stretch", "mean"),
        stretch_std=("stretch", "std"),
        count=("latency_ms", "count"),
    )
    lat_q = gb["latency_ms"].quantile([0.5, 0.95, 0.99]).unstack()
    lat_q.columns = ["latency_p50", "latency_p95", "latency_p99"]
    st_q = gb["stretch"].quantile([0.5, 0.95, 0.99]).unstack()
    st_q.columns = ["stretch_p50", "stretch_p95", "stretch_p99"]
    return pd.concat([means, lat_q, st_q], axis=1).reset_index()


def build_per_topology_stats(df: pd.DataFrame, group_by: str) -> pd.DataFrame:
    """
    Compute per-topology (per-run) statistics.
    group_by = 'n_seed': group by (n, seed, protocol)
    group_by = 'n':      group by (n, protocol)
    """
    if group_by == "n_seed":
        grp_cols = ["n", "seed", "protocol"]
    else:
        grp_cols = ["n", "protocol"]

    gb = df.groupby(grp_cols, sort=False)
    means = gb.agg(
        latency_mean=("latency_ms", "mean"),
        latency_std=("latency_ms", "std"),
        stretch_mean=("stretch", "mean"),
        stretch_std=("stretch", "std"),
        count=("latency_ms", "count"),
    )
    lat_q = gb["latency_ms"].quantile([0.5, 0.95, 0.99]).unstack()
    lat_q.columns = ["latency_p50", "latency_p95", "latency_p99"]
    st_q = gb["stretch"].quantile([0.5, 0.95, 0.99]).unstack()
    st_q.columns = ["stretch_p50", "stretch_p95", "stretch_p99"]
    return pd.concat([means, lat_q, st_q], axis=1).reset_index()


# -- I/O helper -------------------------------------------------------------

def savefig(fig, out_dir: str, name: str):
    path = os.path.join(out_dir, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved {path}")


# -- Scatter helpers --------------------------------------------------------

def _scatter_gs_vs_spread(ax, gs_vals, sp_vals, colors, markers, labels, title, xlabel, ylabel, point_size=50):
    """Draw GossipSub (X) vs Spread (Y) scatter with x=y line."""
    g_arr = np.asarray(gs_vals, dtype=float)
    s_arr = np.asarray(sp_vals, dtype=float)
    colors_arr = np.asarray(colors, dtype=object)
    markers_arr = np.asarray(markers, dtype=object)
    labels_arr = np.asarray(labels, dtype=object)

    lo = float(min(g_arr.min(), s_arr.min()) * 0.95)
    hi = float(max(g_arr.max(), s_arr.max()) * 1.05)
    ax.plot([lo, hi], [lo, hi], "k--", linewidth=1.5, alpha=0.65, label="X = Y", zorder=1)

    # One scatter per distinct label (not per point). Per-point scatter() calls are
    # catastrophically slow at ~1e5 points (matplotlib autoscale cost explodes).
    order = []
    seen = set()
    for lbl in labels_arr:
        if lbl not in seen:
            seen.add(lbl)
            order.append(lbl)
    for lbl in order:
        mask = labels_arr == lbl
        idx = int(np.flatnonzero(mask)[0])
        c = colors_arr[idx]
        m = markers_arr[idx]
        ax.scatter(
            g_arr[mask], s_arr[mask], color=c, marker=m, s=point_size, zorder=3,
            label=str(lbl), edgecolors="white", linewidths=0.4, alpha=0.7,
        )

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(title="N", fontsize=7, title_fontsize=8, loc="best")


def _prepare_scatter_data(stats_df, metric_col, color_col):
    """
    Pivot stats_df so GossipSub and Spread values are side-by-side per pair.
    color_col determines how points are colored (n or seed).
    """
    gs = stats_df[stats_df["protocol"] == "gossipsub"].copy()
    sp = stats_df[stats_df["protocol"] == "spread"].copy()

    # Build a merge key from all grouping columns except protocol
    key_cols = [c for c in stats_df.columns if c not in
                ("protocol", "count",
                 "latency_mean", "latency_p50", "latency_p95", "latency_p99", "latency_std",
                 "stretch_mean", "stretch_p50", "stretch_p95", "stretch_p99", "stretch_std")]

    merged = gs.merge(sp, on=key_cols, suffixes=("_gs", "_sp"))
    gs_vals = merged[f"{metric_col}_gs"].values
    sp_vals = merged[f"{metric_col}_sp"].values
    color_vals = merged[color_col].values

    styles = n_styles(color_vals)
    colors = [styles[v][0] for v in color_vals]
    markers_list = [styles[v][1] for v in color_vals]
    labels = [str(v) for v in color_vals]

    return gs_vals, sp_vals, colors, markers_list, labels


# -- Per-pair scatter charts ------------------------------------------------

def chart_per_pair_scatter(pair_stats, out_dir, group_by):
    """
    Scatter plots of per-pair stats: GossipSub (X) vs Spread (Y).
    4 stats x 2 metrics = 8 subplots, colored by N.
    If group_by == 'n_seed', also produce a version colored by seed.
    """
    stats_specs = [
        ("latency_mean", "Latency mean (ms)"),
        ("latency_p50", "Latency p50 (ms)"),
        ("latency_p95", "Latency p95 (ms)"),
        ("latency_p99", "Latency p99 (ms)"),
        ("stretch_mean", "Stretch mean"),
        ("stretch_p50", "Stretch p50"),
        ("stretch_p95", "Stretch p95"),
        ("stretch_p99", "Stretch p99"),
    ]

    # Colored by N
    fig, axes = plt.subplots(2, 4, figsize=(22, 10))
    for ax, (col, label) in zip(axes.flat, stats_specs):
        gs_v, sp_v, cs, ms, ls = _prepare_scatter_data(pair_stats, col, "n")
        if len(gs_v) == 0:
            ax.set_visible(False)
            continue
        _scatter_gs_vs_spread(ax, gs_v, sp_v, cs, ms, ls,
                              f"Per-pair {label}", f"GossipSub {label}", f"Spread {label}", point_size=5)
    fig.suptitle("Per-pair: GossipSub vs Spread (color = N)", fontsize=14, y=1.01)
    fig.tight_layout()
    savefig(fig, out_dir, "01_per_pair_scatter_by_n.png")

    # Colored by seed (only if grouping by n_seed)
    if group_by == "n_seed" and "seed" in pair_stats.columns:
        fig, axes = plt.subplots(2, 4, figsize=(22, 10))
        for ax, (col, label) in zip(axes.flat, stats_specs):
            gs_v, sp_v, cs, ms, ls = _prepare_scatter_data(pair_stats, col, "seed")
            if len(gs_v) == 0:
                ax.set_visible(False)
                continue
            _scatter_gs_vs_spread(ax, gs_v, sp_v, cs, ms, ls,
                                  f"Per-pair {label}", f"GossipSub {label}", f"Spread {label}", point_size=24)
        fig.suptitle("Per-pair: GossipSub vs Spread (color = seed)", fontsize=14, y=1.01)
        fig.tight_layout()
        savefig(fig, out_dir, "02_per_pair_scatter_by_seed.png")


# -- Per-topology scatter charts --------------------------------------------

def chart_per_topology_scatter(topo_stats, out_dir):
    """
    Scatter plots of per-topology stats: GossipSub (X) vs Spread (Y).
    4 stats x 2 metrics = 8 subplots, colored by N.
    """
    stats_specs = [
        ("latency_mean", "Latency mean (ms)"),
        ("latency_p50", "Latency p50 (ms)"),
        ("latency_p95", "Latency p95 (ms)"),
        ("latency_p99", "Latency p99 (ms)"),
        ("stretch_mean", "Stretch mean"),
        ("stretch_p50", "Stretch p50"),
        ("stretch_p95", "Stretch p95"),
        ("stretch_p99", "Stretch p99"),
    ]

    fig, axes = plt.subplots(2, 4, figsize=(22, 10))
    for ax, (col, label) in zip(axes.flat, stats_specs):
        gs_v, sp_v, cs, ms, ls = _prepare_scatter_data(topo_stats, col, "n")
        if len(gs_v) == 0:
            ax.set_visible(False)
            continue
        _scatter_gs_vs_spread(ax, gs_v, sp_v, cs, ms, ls,
                              f"Per-topology {label}", f"GossipSub {label}", f"Spread {label}")
    fig.suptitle("Per-topology: GossipSub vs Spread (color = N)", fontsize=14, y=1.01)
    fig.tight_layout()
    savefig(fig, out_dir, "03_per_topology_scatter_by_n.png")


# -- CDF charts ------------------------------------------------------------

def _resolve_cdf_xlim(gs_vals, sp_vals, lower_q, upper_q):
    vals = np.concatenate([np.asarray(gs_vals, dtype=float), np.asarray(sp_vals, dtype=float)])
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return None
    lo = float(np.quantile(vals, lower_q))
    hi = float(np.quantile(vals, upper_q))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return None
    return (lo, hi)


def _cdf_plot(ax, gs_vals, sp_vals, title, xlabel, xlim=None):
    """CDF comparison on a given axes."""
    for vals, proto in [(gs_vals, "gossipsub"), (sp_vals, "spread")]:
        sorted_v = np.sort(vals)
        cdf = np.arange(1, len(sorted_v) + 1) / len(sorted_v)
        ax.plot(sorted_v, cdf, color=PROTOCOL_COLORS[proto],
                label=PROTOCOL_LABELS[proto], linewidth=1.5)
    if xlim is not None:
        ax.set_xlim(*xlim)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("CDF")
    ax.set_title(title)
    ax.legend(fontsize=8)


def chart_cdf_per_pair(pair_stats, out_dir, cdf_zoom=None):
    """CDF of per-pair aggregated stats: latency mean and stretch mean."""
    gs = pair_stats[pair_stats["protocol"] == "gossipsub"]
    sp = pair_stats[pair_stats["protocol"] == "spread"]

    lat_xlim = None
    st_xlim = None
    if cdf_zoom is not None:
        lower_q, upper_q = cdf_zoom
        lat_xlim = _resolve_cdf_xlim(gs["latency_mean"].values, sp["latency_mean"].values, lower_q, upper_q)
        st_xlim = _resolve_cdf_xlim(gs["stretch_mean"].values, sp["stretch_mean"].values, lower_q, upper_q)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    _cdf_plot(axes[0], gs["latency_mean"].values, sp["latency_mean"].values,
              "CDF of Per-pair Mean Latency", "Mean latency (ms)", xlim=lat_xlim)
    _cdf_plot(axes[1], gs["stretch_mean"].values, sp["stretch_mean"].values,
              "CDF of Per-pair Mean Stretch", "Mean stretch", xlim=st_xlim)
    fig.suptitle("Per-pair CDF", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "04_cdf_per_pair.png")


def chart_cdf_per_topology(topo_stats, out_dir, cdf_zoom=None):
    """CDF of per-topology aggregated stats: latency mean and stretch mean."""
    gs = topo_stats[topo_stats["protocol"] == "gossipsub"]
    sp = topo_stats[topo_stats["protocol"] == "spread"]

    lat_xlim = None
    st_xlim = None
    if cdf_zoom is not None:
        lower_q, upper_q = cdf_zoom
        lat_xlim = _resolve_cdf_xlim(gs["latency_mean"].values, sp["latency_mean"].values, lower_q, upper_q)
        st_xlim = _resolve_cdf_xlim(gs["stretch_mean"].values, sp["stretch_mean"].values, lower_q, upper_q)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    _cdf_plot(axes[0], gs["latency_mean"].values, sp["latency_mean"].values,
              "CDF of Per-topology Mean Latency", "Mean latency (ms)", xlim=lat_xlim)
    _cdf_plot(axes[1], gs["stretch_mean"].values, sp["stretch_mean"].values,
              "CDF of Per-topology Mean Stretch", "Mean stretch", xlim=st_xlim)
    fig.suptitle("Per-topology CDF", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "05_cdf_per_topology.png")


def chart_cdf_raw(df, out_dir, cdf_zoom=None):
    """CDF of raw observations: latency and stretch."""
    gs = df[df["protocol"] == "gossipsub"]
    sp = df[df["protocol"] == "spread"]

    lat_xlim = None
    st_xlim = None
    if cdf_zoom is not None:
        lower_q, upper_q = cdf_zoom
        lat_xlim = _resolve_cdf_xlim(gs["latency_ms"].values, sp["latency_ms"].values, lower_q, upper_q)
        st_xlim = _resolve_cdf_xlim(gs["stretch"].values, sp["stretch"].values, lower_q, upper_q)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    _cdf_plot(axes[0], gs["latency_ms"].values, sp["latency_ms"].values,
              "CDF of Raw Latency", "Latency (ms)", xlim=lat_xlim)
    _cdf_plot(axes[1], gs["stretch"].values, sp["stretch"].values,
              "CDF of Raw Stretch", "Stretch", xlim=st_xlim)
    fig.suptitle("Raw observations CDF (all pairs, all trials)", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "06_cdf_raw.png")


def chart_topology_delta_cdf(topo_stats, out_dir, cdf_zoom=None):
    """
    CDF of per-topology paired deltas:
      delta = Spread - GossipSub (negative => Spread better for lower-is-better metrics).
    """
    keys = ["n"]
    if "seed" in topo_stats.columns:
        keys.append("seed")

    gs = topo_stats[topo_stats["protocol"] == "gossipsub"]
    sp = topo_stats[topo_stats["protocol"] == "spread"]
    merged = gs.merge(sp, on=keys, suffixes=("_gs", "_sp"))
    if merged.empty:
        return

    delta_latency = merged["latency_mean_sp"].values - merged["latency_mean_gs"].values
    delta_stretch = merged["stretch_mean_sp"].values - merged["stretch_mean_gs"].values

    lat_xlim = None
    st_xlim = None
    if cdf_zoom is not None:
        lower_q, upper_q = cdf_zoom
        # Zoom on absolute values, then map back to signed axis to preserve interpretation.
        abs_lat = np.abs(delta_latency)
        abs_st = np.abs(delta_stretch)
        lat_abs_xlim = _resolve_cdf_xlim(abs_lat, abs_lat, lower_q, upper_q)
        st_abs_xlim = _resolve_cdf_xlim(abs_st, abs_st, lower_q, upper_q)
        if lat_abs_xlim is not None:
            lat_xlim = (-lat_abs_xlim[1], lat_abs_xlim[1])
        if st_abs_xlim is not None:
            st_xlim = (-st_abs_xlim[1], st_abs_xlim[1])

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    _cdf_plot(
        axes[0], delta_latency, delta_latency,
        "CDF of Topology Delta Mean Latency (Spread - GossipSub)", "Delta latency (ms)", xlim=lat_xlim
    )
    axes[0].lines[1].set_visible(False)
    axes[0].lines[0].set_color("#4c9e8e")
    axes[0].lines[0].set_label("Delta (Spread - GossipSub)")
    axes[0].axvline(0, color="black", linestyle="--", linewidth=1.2, alpha=0.8)
    axes[0].legend(fontsize=8)

    _cdf_plot(
        axes[1], delta_stretch, delta_stretch,
        "CDF of Topology Delta Mean Stretch (Spread - GossipSub)", "Delta stretch", xlim=st_xlim
    )
    axes[1].lines[1].set_visible(False)
    axes[1].lines[0].set_color("#8e6ea0")
    axes[1].lines[0].set_label("Delta (Spread - GossipSub)")
    axes[1].axvline(0, color="black", linestyle="--", linewidth=1.2, alpha=0.8)
    axes[1].legend(fontsize=8)

    fig.suptitle("Per-topology Paired Delta CDF", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "09_topology_delta_cdf.png")


# -- Improvement heatmap ----------------------------------------------------

def chart_improvement_heatmap(topo_stats, out_dir):
    """
    Heatmap of % improvement of Spread over GossipSub per (metric x N).
    Positive = Spread is better (lower).
    """
    metrics = [
        ("latency_mean", "Latency mean"),
        ("latency_p50", "Latency p50"),
        ("latency_p95", "Latency p95"),
        ("latency_p99", "Latency p99"),
        ("latency_std", "Latency std"),
        ("stretch_mean", "Stretch mean"),
        ("stretch_p50", "Stretch p50"),
        ("stretch_p95", "Stretch p95"),
        ("stretch_p99", "Stretch p99"),
        ("stretch_std", "Stretch std"),
    ]

    gs = topo_stats[topo_stats["protocol"] == "gossipsub"]
    sp = topo_stats[topo_stats["protocol"] == "spread"]

    ns = sorted(topo_stats["n"].unique())

    # Average across runs for each N
    gs_by_n = gs.groupby("n").mean(numeric_only=True)
    sp_by_n = sp.groupby("n").mean(numeric_only=True)

    data = np.zeros((len(metrics), len(ns)))
    for j, n in enumerate(ns):
        if n not in gs_by_n.index or n not in sp_by_n.index:
            continue
        for i, (col, _) in enumerate(metrics):
            g_val = gs_by_n.loc[n, col]
            s_val = sp_by_n.loc[n, col]
            if g_val != 0:
                data[i, j] = (g_val - s_val) / abs(g_val) * 100

    fig_w = max(6, len(ns) * 1.8)
    fig, ax = plt.subplots(figsize=(fig_w, 6))
    sns.heatmap(
        data, ax=ax, annot=True, fmt=".1f",
        xticklabels=[f"N={n}" for n in ns],
        yticklabels=[m[1] for m in metrics],
        center=0, cmap="RdYlGn", linewidths=0.5,
        cbar_kws={"label": "% improvement (positive = Spread wins)"},
    )
    ax.set_title("Spread vs GossipSub: % Improvement per Metric per N")
    fig.tight_layout()
    savefig(fig, out_dir, "07_improvement_heatmap.png")


# -- Distribution charts ----------------------------------------------------

def chart_distributions(df, out_dir):
    """
    2x2 histogram comparison of raw observations:
      - Latency distribution (both protocols)
      - Stretch distribution (both protocols)
      - Delta latency (Spread - GossipSub) per trial with x=0 reference
      - Delta stretch (Spread - GossipSub) per trial with x=0 reference
    """
    gs = df[df["protocol"] == "gossipsub"]
    sp = df[df["protocol"] == "spread"]

    fig, axes = plt.subplots(2, 2, figsize=(12, 9))

    def _hist(ax, gs_vals, sp_vals, title, xlabel):
        bins = min(50, max(10, len(gs_vals) // 50))
        ax.hist(gs_vals, bins=bins, alpha=0.5, density=True,
                color=PROTOCOL_COLORS["gossipsub"],
                label=PROTOCOL_LABELS["gossipsub"])
        ax.hist(sp_vals, bins=bins, alpha=0.5, density=True,
                color=PROTOCOL_COLORS["spread"],
                label=PROTOCOL_LABELS["spread"])
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("Density")
        ax.legend(fontsize=8)

    _hist(axes[0, 0], gs["latency_ms"].values, sp["latency_ms"].values,
          "Latency Distribution", "ms")
    _hist(axes[0, 1], gs["stretch"].values, sp["stretch"].values,
          "Stretch Distribution", "stretch (x)")

    # Compute per-trial mean deltas: match trials by (run_idx, trial)
    trial_means = df.groupby(["run_idx", "trial", "protocol"], as_index=False).agg(
        lat=("latency_ms", "mean"),
        st=("stretch", "mean"),
    )
    gs_t = trial_means[trial_means["protocol"] == "gossipsub"]
    sp_t = trial_means[trial_means["protocol"] == "spread"]
    merged = gs_t.merge(sp_t, on=["run_idx", "trial"], suffixes=("_gs", "_sp"))

    delta_lat = merged["lat_sp"].values - merged["lat_gs"].values
    delta_st = merged["st_sp"].values - merged["st_gs"].values

    # Delta latency
    ax = axes[1, 0]
    bins = min(50, max(10, len(delta_lat) // 20))
    ax.hist(delta_lat, bins=bins, density=True, color="#4c9e8e", alpha=0.85)
    ax.axvline(0, color="black", linestyle="--", linewidth=1.5)
    ax.set_title("Delta Mean Latency per Trial (Spread - GossipSub)")
    ax.set_xlabel("ms")
    ax.set_ylabel("Density")

    # Delta stretch
    ax = axes[1, 1]
    bins = min(50, max(10, len(delta_st) // 20))
    ax.hist(delta_st, bins=bins, density=True, color="#8e6ea0", alpha=0.85)
    ax.axvline(0, color="black", linestyle="--", linewidth=1.5)
    ax.set_title("Delta Mean Stretch per Trial (Spread - GossipSub)")
    ax.set_xlabel("stretch (x)")
    ax.set_ylabel("Density")

    fig.suptitle("Distribution Comparison -- all runs pooled", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "08_distributions.png")


# -- Attacker accuracy charts -----------------------------------------------

def _load_attacker_accuracy(exports: list) -> pd.DataFrame:
    """
    Extract attacker accuracy records from exports into a DataFrame with columns:
        run_idx, n, seed, protocol, attacker_pct, trials_observed, correct_estimates, accuracy
    """
    rows = []
    for run_idx, exp in enumerate(exports):
        n = exp["nodes"]
        seed = exp["seed"]
        for proto, key in [("gossipsub", "gossipsub_attacker_accuracy"),
                           ("spread", "spread_attacker_accuracy")]:
            for rec in exp.get(key, []):
                rows.append({
                    "run_idx": run_idx,
                    "n": n,
                    "seed": seed,
                    "protocol": proto,
                    "attacker_pct": rec["attacker_pct"],
                    "trials_observed": rec["trials_observed"],
                    "correct_estimates": rec["correct_estimates"],
                    "accuracy": rec["accuracy"],
                })
    return pd.DataFrame(rows)


def chart_attacker_accuracy(exports: list, out_dir: str):
    """
    Grouped bar chart of attacker source-estimation accuracy per attacker
    percentage, comparing GossipSub vs Spread.  When multiple N values or
    seeds exist, bars show the mean accuracy with error bars (std).
    """
    acc_df = _load_attacker_accuracy(exports)
    if acc_df.empty:
        print("  (no attacker accuracy data found, skipping)")
        return

    agg = (
        acc_df
        .groupby(["protocol", "attacker_pct"])["accuracy"]
        .agg(["mean", "std", "count"])
        .reset_index()
    )
    agg["std"] = agg["std"].fillna(0)

    pcts = sorted(agg["attacker_pct"].unique())
    x = np.arange(len(pcts))
    bar_w = 0.35

    fig, ax = plt.subplots(figsize=(max(6, len(pcts) * 2.2), 5))
    for i, proto in enumerate(["gossipsub", "spread"]):
        sub = agg[agg["protocol"] == proto].set_index("attacker_pct")
        means = [sub.loc[p, "mean"] if p in sub.index else 0 for p in pcts]
        stds = [sub.loc[p, "std"] if p in sub.index else 0 for p in pcts]
        ax.bar(
            x + (i - 0.5) * bar_w, means, bar_w, yerr=stds,
            color=PROTOCOL_COLORS[proto], label=PROTOCOL_LABELS[proto],
            edgecolor="white", linewidth=0.6, capsize=4, alpha=0.85,
        )

    ax.set_xticks(x)
    ax.set_xticklabels([f"{p*100:.0f}%" for p in pcts])
    ax.set_xlabel("Attacker fraction")
    ax.set_ylabel("Source-estimation accuracy")
    ax.set_title("Attacker Source-Estimation Accuracy: GossipSub vs Spread")
    ax.set_ylim(0, min(1.05, ax.get_ylim()[1] * 1.1))
    ax.legend(fontsize=9)
    fig.tight_layout()
    savefig(fig, out_dir, "10_attacker_accuracy.png")

    # Per-N breakdown when multiple N values exist
    ns = sorted(acc_df["n"].unique())
    if len(ns) > 1:
        fig, axes_grid = plt.subplots(1, len(ns), figsize=(5 * len(ns), 5), sharey=True)
        if len(ns) == 1:
            axes_grid = [axes_grid]
        for ax_i, (ax_n, n_val) in enumerate(zip(axes_grid, ns)):
            sub_n = acc_df[acc_df["n"] == n_val]
            agg_n = (
                sub_n
                .groupby(["protocol", "attacker_pct"])["accuracy"]
                .agg(["mean", "std"])
                .reset_index()
            )
            agg_n["std"] = agg_n["std"].fillna(0)
            pcts_n = sorted(agg_n["attacker_pct"].unique())
            x_n = np.arange(len(pcts_n))
            for j, proto in enumerate(["gossipsub", "spread"]):
                s = agg_n[agg_n["protocol"] == proto].set_index("attacker_pct")
                m = [s.loc[p, "mean"] if p in s.index else 0 for p in pcts_n]
                e = [s.loc[p, "std"] if p in s.index else 0 for p in pcts_n]
                ax_n.bar(
                    x_n + (j - 0.5) * bar_w, m, bar_w, yerr=e,
                    color=PROTOCOL_COLORS[proto], label=PROTOCOL_LABELS[proto],
                    edgecolor="white", linewidth=0.6, capsize=4, alpha=0.85,
                )
            ax_n.set_xticks(x_n)
            ax_n.set_xticklabels([f"{p*100:.0f}%" for p in pcts_n])
            ax_n.set_xlabel("Attacker fraction")
            ax_n.set_title(f"N = {n_val}")
            if ax_i == 0:
                ax_n.set_ylabel("Source-estimation accuracy")
                ax_n.legend(fontsize=8)
            ax_n.set_ylim(0, 1.05)
        fig.suptitle("Attacker Source-Estimation Accuracy by Network Size", fontsize=13)
        fig.tight_layout()
        savefig(fig, out_dir, "11_attacker_accuracy_by_n.png")


# -- Main -------------------------------------------------------------------

def main():
    default_out_base = os.path.join("simnet_spread_tests", "outputs")
    default_runs = os.path.join(default_out_base, "runs")
    default_results = os.path.join(default_out_base, "results.jsonl")
    default_plots = os.path.join(default_out_base, "plots")

    parser = argparse.ArgumentParser(
        description="Plot simnet spread experiment results from raw per-pair data."
    )
    parser.add_argument(
        "--runs-dir", default=default_runs,
        help=f"Directory containing per-run JSON exports (default: {default_runs})",
    )
    parser.add_argument(
        "--results", default=default_results,
        help=f"Path to results.jsonl for finding successful runs (default: {default_results})",
    )
    parser.add_argument(
        "--out", default=default_plots,
        help=f"Output directory for plots (default: {default_plots})",
    )
    parser.add_argument(
        "--group-by", choices=["n_seed", "n"], default="n_seed",
        help="Grouping for per-pair/per-topology stats: n_seed (default) or n",
    )
    parser.add_argument(
        "--cdf-x-lower-q", type=float, default=0.0,
        help="Lower quantile for CDF x-axis zoom (default: 0.0, no lower crop)",
    )
    parser.add_argument(
        "--cdf-x-upper-q", type=float, default=1.0,
        help="Upper quantile for CDF x-axis zoom (default: 1.0, no upper crop)",
    )
    args = parser.parse_args()
    if not (0.0 <= args.cdf_x_lower_q < args.cdf_x_upper_q <= 1.0):
        print("Invalid CDF x-axis quantiles: require 0 <= lower < upper <= 1.", file=sys.stderr)
        sys.exit(2)

    os.makedirs(args.out, exist_ok=True)

    print(f"Loading run exports from {args.runs_dir} ...")
    exports = load_run_exports(args.runs_dir, args.results)
    if not exports:
        print("No run exports found.", file=sys.stderr)
        sys.exit(1)
    print(f"  {len(exports)} run(s) loaded.")

    print("Flattening observations ...")
    df = flatten_observations(exports)
    if df.empty:
        print("No observations found in exports.", file=sys.stderr)
        sys.exit(1)
    print(f"  {len(df)} raw observations across {df['run_idx'].nunique()} runs.")
    print(f"  N values: {sorted(df['n'].unique())}")
    print(f"  Seeds: {sorted(df['seed'].unique())}")

    group_by = args.group_by
    cdf_zoom = None
    if args.cdf_x_lower_q > 0.0 or args.cdf_x_upper_q < 1.0:
        cdf_zoom = (args.cdf_x_lower_q, args.cdf_x_upper_q)
        print(f"CDF x-axis zoom quantiles: lower={args.cdf_x_lower_q}, upper={args.cdf_x_upper_q}")
    print(f"Computing stats (group-by={group_by}) ...")
    pair_stats = build_per_pair_stats(df, group_by)
    topo_stats = build_per_topology_stats(df, group_by)
    print(f"  {len(pair_stats)} per-pair stat rows, {len(topo_stats)} per-topology stat rows.")

    print("Generating charts ...")
    chart_per_pair_scatter(pair_stats, args.out, group_by)
    chart_per_topology_scatter(topo_stats, args.out)
    chart_cdf_per_pair(pair_stats, args.out, cdf_zoom=cdf_zoom)
    chart_cdf_per_topology(topo_stats, args.out, cdf_zoom=cdf_zoom)
    chart_cdf_raw(df, args.out, cdf_zoom=cdf_zoom)
    chart_topology_delta_cdf(topo_stats, args.out, cdf_zoom=cdf_zoom)
    chart_improvement_heatmap(topo_stats, args.out)
    chart_distributions(df, args.out)
    chart_attacker_accuracy(exports, args.out)
    print("Done.")


if __name__ == "__main__":
    main()
