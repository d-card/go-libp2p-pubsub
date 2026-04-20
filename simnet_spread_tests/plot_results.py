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
PROTOCOLS = ["gossipsub", "spread", "dandelion"]
PROTOCOL_COLORS = {"gossipsub": "#e07b54", "spread": "#5b8db8", "dandelion": "#6bb36b"}
PROTOCOL_LABELS = {"gossipsub": "GossipSub", "spread": "Spread", "dandelion": "Dandelion"}
MARKERS = {"gossipsub": "o", "spread": "s", "dandelion": "^"}

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
        for proto in PROTOCOLS:
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


def _available_protocols(df_or_stats):
    """Return the list of protocols present in a DataFrame's 'protocol' column."""
    present = set(df_or_stats["protocol"].unique())
    return [p for p in PROTOCOLS if p in present]


# -- Scatter helpers --------------------------------------------------------

def _scatter_pairwise(ax, x_vals, y_vals, colors, markers, labels, title, xlabel, ylabel, point_size=50):
    """Draw protocol-X (X) vs protocol-Y (Y) scatter with x=y line."""
    g_arr = np.asarray(x_vals, dtype=float)
    s_arr = np.asarray(y_vals, dtype=float)
    colors_arr = np.asarray(colors, dtype=object)
    markers_arr = np.asarray(markers, dtype=object)
    labels_arr = np.asarray(labels, dtype=object)

    lo = float(min(g_arr.min(), s_arr.min()) * 0.95)
    hi = float(max(g_arr.max(), s_arr.max()) * 1.05)
    ax.plot([lo, hi], [lo, hi], "k--", linewidth=1.5, alpha=0.65, label="X = Y", zorder=1)

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


def _prepare_scatter_data(stats_df, metric_col, color_col, proto_x, proto_y):
    """
    Pivot stats_df so proto_x and proto_y values are side-by-side per pair.
    color_col determines how points are colored (n or seed).
    """
    df_x = stats_df[stats_df["protocol"] == proto_x].copy()
    df_y = stats_df[stats_df["protocol"] == proto_y].copy()

    key_cols = [c for c in stats_df.columns if c not in
                ("protocol", "count",
                 "latency_mean", "latency_p50", "latency_p95", "latency_p99", "latency_std",
                 "stretch_mean", "stretch_p50", "stretch_p95", "stretch_p99", "stretch_std")]

    merged = df_x.merge(df_y, on=key_cols, suffixes=("_x", "_y"))
    if merged.empty:
        return [], [], [], [], []
    x_vals = merged[f"{metric_col}_x"].values
    y_vals = merged[f"{metric_col}_y"].values
    color_vals = merged[color_col].values

    styles = n_styles(color_vals)
    colors = [styles[v][0] for v in color_vals]
    markers_list = [styles[v][1] for v in color_vals]
    labels = [str(v) for v in color_vals]

    return x_vals, y_vals, colors, markers_list, labels


# -- Per-pair scatter charts ------------------------------------------------

def chart_per_pair_scatter(pair_stats, out_dir, group_by):
    """
    Scatter plots of per-pair stats for each protocol vs GossipSub.
    Produces one figure per comparison protocol (Spread, Dandelion).
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

    protos = _available_protocols(pair_stats)
    comparison_protos = [p for p in protos if p != "gossipsub"]
    if "gossipsub" not in protos or not comparison_protos:
        return

    for comp_proto in comparison_protos:
        comp_label = PROTOCOL_LABELS[comp_proto]

        # Colored by N
        fig, axes = plt.subplots(2, 4, figsize=(22, 10))
        for ax, (col, label) in zip(axes.flat, stats_specs):
            x_v, y_v, cs, ms, ls = _prepare_scatter_data(pair_stats, col, "n", "gossipsub", comp_proto)
            if len(x_v) == 0:
                ax.set_visible(False)
                continue
            _scatter_pairwise(ax, x_v, y_v, cs, ms, ls,
                              f"Per-pair {label}", f"GossipSub {label}", f"{comp_label} {label}", point_size=5)
        fig.suptitle(f"Per-pair: GossipSub vs {comp_label} (color = N)", fontsize=14, y=1.01)
        fig.tight_layout()
        savefig(fig, out_dir, f"01_per_pair_scatter_{comp_proto}_by_n.png")

        # Colored by seed (only if grouping by n_seed)
        if group_by == "n_seed" and "seed" in pair_stats.columns:
            fig, axes = plt.subplots(2, 4, figsize=(22, 10))
            for ax, (col, label) in zip(axes.flat, stats_specs):
                x_v, y_v, cs, ms, ls = _prepare_scatter_data(pair_stats, col, "seed", "gossipsub", comp_proto)
                if len(x_v) == 0:
                    ax.set_visible(False)
                    continue
                _scatter_pairwise(ax, x_v, y_v, cs, ms, ls,
                                  f"Per-pair {label}", f"GossipSub {label}", f"{comp_label} {label}", point_size=24)
            fig.suptitle(f"Per-pair: GossipSub vs {comp_label} (color = seed)", fontsize=14, y=1.01)
            fig.tight_layout()
            savefig(fig, out_dir, f"02_per_pair_scatter_{comp_proto}_by_seed.png")


# -- Per-topology scatter charts --------------------------------------------

def chart_per_topology_scatter(topo_stats, out_dir):
    """
    Scatter plots of per-topology stats for each protocol vs GossipSub.
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

    protos = _available_protocols(topo_stats)
    comparison_protos = [p for p in protos if p != "gossipsub"]
    if "gossipsub" not in protos or not comparison_protos:
        return

    for comp_proto in comparison_protos:
        comp_label = PROTOCOL_LABELS[comp_proto]
        fig, axes = plt.subplots(2, 4, figsize=(22, 10))
        for ax, (col, label) in zip(axes.flat, stats_specs):
            x_v, y_v, cs, ms, ls = _prepare_scatter_data(topo_stats, col, "n", "gossipsub", comp_proto)
            if len(x_v) == 0:
                ax.set_visible(False)
                continue
            _scatter_pairwise(ax, x_v, y_v, cs, ms, ls,
                              f"Per-topology {label}", f"GossipSub {label}", f"{comp_label} {label}")
        fig.suptitle(f"Per-topology: GossipSub vs {comp_label} (color = N)", fontsize=14, y=1.01)
        fig.tight_layout()
        savefig(fig, out_dir, f"03_per_topology_scatter_{comp_proto}_by_n.png")


# -- CDF charts ------------------------------------------------------------

CDF_LATENCY_XMIN = 20
CDF_LATENCY_XMAX = 120   # ms – hard cap for latency axes
CDF_STRETCH_XMIN = 1
CDF_STRETCH_XMAX = 3    # hard cap for stretch axes


def _apply_cdf_caps(xlim, xmin, xmax):
    """Clamp an (lo, hi) xlim tuple to [xmin, xmax], or return (xmin, xmax) if None."""
    if xlim is None:
        return (xmin, xmax)
    return (max(xmin, xlim[0]), min(xlim[1], xmax))


def _resolve_cdf_xlim(all_vals_list, lower_q, upper_q):
    vals = np.concatenate([np.asarray(v, dtype=float) for v in all_vals_list])
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return None
    lo = float(np.quantile(vals, lower_q))
    hi = float(np.quantile(vals, upper_q))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return None
    return (lo, hi)


def _cdf_plot(ax, proto_vals, title, xlabel, xlim=None):
    """CDF comparison of multiple protocols on a given axes."""
    for proto, vals in proto_vals.items():
        if len(vals) == 0:
            continue
        sorted_v = np.sort(vals)
        cdf = np.arange(1, len(sorted_v) + 1) / len(sorted_v)
        ax.plot(sorted_v, cdf, color=PROTOCOL_COLORS.get(proto, "#333333"),
                label=PROTOCOL_LABELS.get(proto, proto), linewidth=1.5)
    if xlim is not None:
        ax.set_xlim(*xlim)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("CDF")
    ax.set_title(title)
    ax.legend(fontsize=8)


def _single_cdf_plot(ax, vals, title, xlabel, color, label, xlim=None):
    """CDF of a single series on a given axes."""
    sorted_v = np.sort(vals)
    cdf = np.arange(1, len(sorted_v) + 1) / len(sorted_v)
    ax.plot(sorted_v, cdf, color=color, label=label, linewidth=1.5)
    if xlim is not None:
        ax.set_xlim(*xlim)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("CDF")
    ax.set_title(title)
    ax.legend(fontsize=8)


def _build_proto_vals(df_or_stats, col):
    """Extract per-protocol value arrays from a DataFrame."""
    out = {}
    for proto in PROTOCOLS:
        sub = df_or_stats[df_or_stats["protocol"] == proto]
        if not sub.empty:
            out[proto] = sub[col].values
    return out


def chart_cdf_per_pair(pair_stats, out_dir, cdf_zoom=None):
    """CDF of per-pair aggregated stats: latency mean and stretch mean."""
    lat_vals = _build_proto_vals(pair_stats, "latency_mean")
    st_vals = _build_proto_vals(pair_stats, "stretch_mean")

    lat_xlim = None
    st_xlim = None
    if cdf_zoom is not None:
        lower_q, upper_q = cdf_zoom
        lat_xlim = _resolve_cdf_xlim(list(lat_vals.values()), lower_q, upper_q)
        st_xlim = _resolve_cdf_xlim(list(st_vals.values()), lower_q, upper_q)
    lat_xlim = _apply_cdf_caps(lat_xlim, CDF_LATENCY_XMIN, CDF_LATENCY_XMAX)
    st_xlim = _apply_cdf_caps(st_xlim, CDF_STRETCH_XMIN, CDF_STRETCH_XMAX)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    _cdf_plot(axes[0], lat_vals, "CDF of Per-pair Mean Latency", "Mean latency (ms)", xlim=lat_xlim)
    _cdf_plot(axes[1], st_vals, "CDF of Per-pair Mean Stretch", "Mean stretch", xlim=st_xlim)
    fig.suptitle("Per-pair CDF", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "04_cdf_per_pair.png")


def chart_cdf_per_topology(topo_stats, out_dir, cdf_zoom=None):
    """CDF of per-topology aggregated stats: latency mean and stretch mean."""
    lat_vals = _build_proto_vals(topo_stats, "latency_mean")
    st_vals = _build_proto_vals(topo_stats, "stretch_mean")

    lat_xlim = None
    st_xlim = None
    if cdf_zoom is not None:
        lower_q, upper_q = cdf_zoom
        lat_xlim = _resolve_cdf_xlim(list(lat_vals.values()), lower_q, upper_q)
        st_xlim = _resolve_cdf_xlim(list(st_vals.values()), lower_q, upper_q)
    lat_xlim = _apply_cdf_caps(lat_xlim, CDF_LATENCY_XMIN, CDF_LATENCY_XMAX)
    st_xlim = _apply_cdf_caps(st_xlim, CDF_STRETCH_XMIN, CDF_STRETCH_XMAX)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    _cdf_plot(axes[0], lat_vals, "CDF of Per-topology Mean Latency", "Mean latency (ms)", xlim=lat_xlim)
    _cdf_plot(axes[1], st_vals, "CDF of Per-topology Mean Stretch", "Mean stretch", xlim=st_xlim)
    fig.suptitle("Per-topology CDF", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "05_cdf_per_topology.png")


def chart_cdf_raw(df, out_dir, cdf_zoom=None):
    """CDF of raw observations: latency and stretch."""
    lat_vals = _build_proto_vals(df, "latency_ms")
    st_vals = _build_proto_vals(df, "stretch")

    lat_xlim = None
    st_xlim = None
    if cdf_zoom is not None:
        lower_q, upper_q = cdf_zoom
        lat_xlim = _resolve_cdf_xlim(list(lat_vals.values()), lower_q, upper_q)
        st_xlim = _resolve_cdf_xlim(list(st_vals.values()), lower_q, upper_q)
    lat_xlim = _apply_cdf_caps(lat_xlim, CDF_LATENCY_XMIN, CDF_LATENCY_XMAX)
    st_xlim = _apply_cdf_caps(st_xlim, CDF_STRETCH_XMIN, CDF_STRETCH_XMAX)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    _cdf_plot(axes[0], lat_vals, "CDF of Raw Latency", "Latency (ms)", xlim=lat_xlim)
    _cdf_plot(axes[1], st_vals, "CDF of Raw Stretch", "Stretch", xlim=st_xlim)
    fig.suptitle("Raw observations CDF (all pairs, all trials)", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "06_cdf_raw.png")


def chart_topology_delta_cdf(topo_stats, out_dir, cdf_zoom=None):
    """
    CDF of per-topology paired deltas vs GossipSub for each comparison protocol:
      delta = Protocol - GossipSub (negative => protocol better for lower-is-better metrics).
    """
    keys = ["n"]
    if "seed" in topo_stats.columns:
        keys.append("seed")

    gs = topo_stats[topo_stats["protocol"] == "gossipsub"]
    protos = _available_protocols(topo_stats)
    comparison_protos = [p for p in protos if p != "gossipsub"]
    if not comparison_protos or gs.empty:
        return

    n_comp = len(comparison_protos)
    fig, axes = plt.subplots(n_comp, 2, figsize=(12, 5 * n_comp), squeeze=False)

    delta_colors = {"spread": "#4c9e8e", "dandelion": "#8e6ea0"}

    for row, comp_proto in enumerate(comparison_protos):
        comp_label = PROTOCOL_LABELS[comp_proto]
        comp = topo_stats[topo_stats["protocol"] == comp_proto]
        merged = gs.merge(comp, on=keys, suffixes=("_gs", "_cp"))
        if merged.empty:
            axes[row, 0].set_visible(False)
            axes[row, 1].set_visible(False)
            continue

        delta_latency = merged["latency_mean_cp"].values - merged["latency_mean_gs"].values
        delta_stretch = merged["stretch_mean_cp"].values - merged["stretch_mean_gs"].values

        lat_xlim = None
        st_xlim = None
        if cdf_zoom is not None:
            lower_q, upper_q = cdf_zoom
            abs_lat = np.abs(delta_latency)
            abs_st = np.abs(delta_stretch)
            lat_abs_xlim = _resolve_cdf_xlim([abs_lat], lower_q, upper_q)
            st_abs_xlim = _resolve_cdf_xlim([abs_st], lower_q, upper_q)
            if lat_abs_xlim is not None:
                lat_xlim = (-lat_abs_xlim[1], lat_abs_xlim[1])
            if st_abs_xlim is not None:
                st_xlim = (-st_abs_xlim[1], st_abs_xlim[1])

        dc = delta_colors.get(comp_proto, "#555555")

        ax_lat = axes[row, 0]
        _single_cdf_plot(ax_lat, delta_latency,
                         f"Delta Mean Latency ({comp_label} - GossipSub)", "Delta latency (ms)",
                         color=dc, label=f"Delta ({comp_label} - GossipSub)", xlim=lat_xlim)
        ax_lat.axvline(0, color="black", linestyle="--", linewidth=1.2, alpha=0.8)

        ax_st = axes[row, 1]
        _single_cdf_plot(ax_st, delta_stretch,
                         f"Delta Mean Stretch ({comp_label} - GossipSub)", "Delta stretch",
                         color=dc, label=f"Delta ({comp_label} - GossipSub)", xlim=st_xlim)
        ax_st.axvline(0, color="black", linestyle="--", linewidth=1.2, alpha=0.8)

    fig.suptitle("Per-topology Paired Delta CDF", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "09_topology_delta_cdf.png")


# -- Improvement heatmap ----------------------------------------------------

def chart_improvement_heatmap(topo_stats, out_dir):
    """
    Heatmap of % improvement over GossipSub per (metric x N) for each
    comparison protocol.  Positive = protocol is better (lower value).
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
    protos = _available_protocols(topo_stats)
    comparison_protos = [p for p in protos if p != "gossipsub"]
    if not comparison_protos or gs.empty:
        return

    ns = sorted(topo_stats["n"].unique())
    gs_by_n = gs.groupby("n").mean(numeric_only=True)

    n_comp = len(comparison_protos)
    fig, axes = plt.subplots(1, n_comp, figsize=(max(6, len(ns) * 1.8) * n_comp, 6), squeeze=False)

    for col_idx, comp_proto in enumerate(comparison_protos):
        comp_label = PROTOCOL_LABELS[comp_proto]
        comp = topo_stats[topo_stats["protocol"] == comp_proto]
        comp_by_n = comp.groupby("n").mean(numeric_only=True)

        data = np.zeros((len(metrics), len(ns)))
        for j, n in enumerate(ns):
            if n not in gs_by_n.index or n not in comp_by_n.index:
                continue
            for i, (mcol, _) in enumerate(metrics):
                g_val = gs_by_n.loc[n, mcol]
                c_val = comp_by_n.loc[n, mcol]
                if g_val != 0:
                    data[i, j] = (g_val - c_val) / abs(g_val) * 100

        ax = axes[0, col_idx]
        sns.heatmap(
            data, ax=ax, annot=True, fmt=".1f",
            xticklabels=[f"N={n}" for n in ns],
            yticklabels=[m[1] for m in metrics],
            center=0, cmap="RdYlGn", linewidths=0.5,
            cbar_kws={"label": f"% improvement (positive = {comp_label} wins)"},
        )
        ax.set_title(f"{comp_label} vs GossipSub")

    fig.suptitle("% Improvement over GossipSub per Metric per N", fontsize=14)
    fig.tight_layout()
    savefig(fig, out_dir, "07_improvement_heatmap.png")


# -- Distribution charts ----------------------------------------------------

DIST_LATENCY_XMAX = 300  # ms – hard cap for latency histograms
DIST_STRETCH_XMAX = 3   # hard cap for stretch histograms


def chart_distributions(df, out_dir):
    """
    Histogram comparison of raw observations across all protocols, plus
    per-trial delta histograms for each comparison protocol vs GossipSub.
    """
    protos = _available_protocols(df)
    comparison_protos = [p for p in protos if p != "gossipsub"]

    n_delta_rows = max(1, len(comparison_protos))
    fig, axes = plt.subplots(1 + n_delta_rows, 2, figsize=(12, 4.5 * (1 + n_delta_rows)))

    # Row 0: overlaid histograms of all protocols
    for metric_col, ax_col, xlabel in [("latency_ms", 0, "ms"), ("stretch", 1, "stretch (x)")]:
        ax = axes[0, ax_col]
        cap = DIST_LATENCY_XMAX if metric_col == "latency_ms" else DIST_STRETCH_XMAX
        # Compute shared bin edges over the capped range so all protocols
        # use identical, evenly-spaced bins within the visible area.
        shared_bins = np.linspace(0, cap, 51)
        for proto in protos:
            vals = df[df["protocol"] == proto][metric_col].values
            if len(vals) == 0:
                continue
            ax.hist(vals, bins=shared_bins, alpha=0.45, density=True,
                    color=PROTOCOL_COLORS[proto], label=PROTOCOL_LABELS[proto])
        title_metric = "Latency" if metric_col == "latency_ms" else "Stretch"
        ax.set_xlim(0, cap)
        ax.set_title(f"{title_metric} Distribution")
        ax.set_xlabel(xlabel)
        ax.set_ylabel("Density")
        ax.legend(fontsize=8)

    # Remaining rows: per-trial delta histograms (Protocol - GossipSub)
    trial_means = df.groupby(["run_idx", "trial", "protocol"], as_index=False).agg(
        lat=("latency_ms", "mean"),
        st=("stretch", "mean"),
    )
    gs_t = trial_means[trial_means["protocol"] == "gossipsub"]

    delta_colors = {"spread": "#4c9e8e", "dandelion": "#8e6ea0"}

    for row_offset, comp_proto in enumerate(comparison_protos):
        comp_label = PROTOCOL_LABELS[comp_proto]
        comp_t = trial_means[trial_means["protocol"] == comp_proto]
        merged = gs_t.merge(comp_t, on=["run_idx", "trial"], suffixes=("_gs", "_cp"))
        dc = delta_colors.get(comp_proto, "#555555")

        if merged.empty:
            axes[1 + row_offset, 0].set_visible(False)
            axes[1 + row_offset, 1].set_visible(False)
            continue

        delta_lat = merged["lat_cp"].values - merged["lat_gs"].values
        delta_st = merged["st_cp"].values - merged["st_gs"].values

        ax = axes[1 + row_offset, 0]
        bins = min(50, max(10, len(delta_lat) // 20))
        ax.hist(delta_lat, bins=bins, density=True, color=dc, alpha=0.85)
        ax.axvline(0, color="black", linestyle="--", linewidth=1.5)
        ax.set_title(f"Delta Mean Latency per Trial ({comp_label} - GossipSub)")
        ax.set_xlabel("ms")
        ax.set_ylabel("Density")

        ax = axes[1 + row_offset, 1]
        bins = min(50, max(10, len(delta_st) // 20))
        ax.hist(delta_st, bins=bins, density=True, color=dc, alpha=0.85)
        ax.axvline(0, color="black", linestyle="--", linewidth=1.5)
        ax.set_title(f"Delta Mean Stretch per Trial ({comp_label} - GossipSub)")
        ax.set_xlabel("stretch (x)")
        ax.set_ylabel("Density")

    fig.suptitle("Distribution Comparison -- all runs pooled", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "08_distributions.png")


def chart_topology_p50_p95_histograms(topo_stats, out_dir):
    """
    2x2 histogram grid of per-topology p50/p95 metrics:
      - latency_p50, latency_p95
      - stretch_p50, stretch_p95
    Overlays GossipSub and Spread only in each subplot.
    """
    protos = [p for p in ("gossipsub", "spread") if p in set(topo_stats["protocol"].unique())]
    if not protos:
        return

    specs = [
        ("latency_p50", "Per-topology Latency p50", "Latency (ms)"),
        ("latency_p95", "Per-topology Latency p95", "Latency (ms)"),
        ("stretch_p50", "Per-topology Stretch p50", "Stretch (x)"),
        ("stretch_p95", "Per-topology Stretch p95", "Stretch (x)"),
    ]

    fig, axes = plt.subplots(2, 2, figsize=(12, 9))
    for ax, (metric_col, title, xlabel) in zip(axes.flat, specs):
        vals_by_proto = {
            p: topo_stats[topo_stats["protocol"] == p][metric_col].dropna().values
            for p in protos
        }
        non_empty = [v for v in vals_by_proto.values() if len(v) > 0]
        if not non_empty:
            ax.set_visible(False)
            continue

        all_vals = np.concatenate(non_empty)
        lo = float(np.nanmin(all_vals))
        hi = float(np.nanmax(all_vals))
        if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
            hi = lo + 1.0
        shared_bins = np.linspace(lo, hi, 30)

        for proto in protos:
            vals = vals_by_proto[proto]
            if len(vals) == 0:
                continue
            ax.hist(
                vals,
                bins=shared_bins,
                alpha=0.45,
                density=True,
                color=PROTOCOL_COLORS[proto],
                label=PROTOCOL_LABELS[proto],
            )

        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("Density")
        ax.legend(fontsize=8)

    fig.suptitle("Per-topology p50/p95 Distributions", fontsize=13)
    fig.tight_layout()
    savefig(fig, out_dir, "12_topology_p50_p95_histograms.png")


# -- Attacker accuracy charts -----------------------------------------------

def _load_attacker_accuracy(exports: list) -> pd.DataFrame:
    """
    Extract attacker accuracy records from exports into a DataFrame with columns:
        run_idx, n, seed, protocol, attacker_pct, trials_observed, correct_estimates, accuracy
    """
    rows = []
    proto_keys = [
        ("gossipsub", "gossipsub_attacker_accuracy"),
        ("spread", "spread_attacker_accuracy"),
        ("dandelion", "dandelion_attacker_accuracy"),
    ]
    for run_idx, exp in enumerate(exports):
        n = exp["nodes"]
        seed = exp["seed"]
        for proto, key in proto_keys:
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
    percentage, comparing all protocols.  When multiple N values or seeds
    exist, bars show the mean accuracy with error bars (std).
    """
    acc_df = _load_attacker_accuracy(exports)
    if acc_df.empty:
        print("  (no attacker accuracy data found, skipping)")
        return

    present_protos = [p for p in PROTOCOLS if p in acc_df["protocol"].unique()]

    agg = (
        acc_df
        .groupby(["protocol", "attacker_pct"])["accuracy"]
        .agg(["mean", "std", "count"])
        .reset_index()
    )
    agg["std"] = agg["std"].fillna(0)

    pcts = sorted(agg["attacker_pct"].unique())
    x = np.arange(len(pcts))
    n_protos = len(present_protos)
    bar_w = 0.8 / n_protos

    fig, ax = plt.subplots(figsize=(max(6, len(pcts) * 2.2), 5))
    for i, proto in enumerate(present_protos):
        sub = agg[agg["protocol"] == proto].set_index("attacker_pct")
        means = [sub.loc[p, "mean"] if p in sub.index else 0 for p in pcts]
        stds = [sub.loc[p, "std"] if p in sub.index else 0 for p in pcts]
        offset = (i - (n_protos - 1) / 2) * bar_w
        ax.bar(
            x + offset, means, bar_w, yerr=stds,
            color=PROTOCOL_COLORS[proto], label=PROTOCOL_LABELS[proto],
            edgecolor="white", linewidth=0.6, capsize=4, alpha=0.85,
        )

    ax.set_xticks(x)
    ax.set_xticklabels([f"{p*100:.0f}%" for p in pcts])
    ax.set_xlabel("Attacker fraction")
    ax.set_ylabel("Source-estimation accuracy")
    ax.set_title("Attacker Source-Estimation Accuracy")
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
            for j, proto in enumerate(present_protos):
                s = agg_n[agg_n["protocol"] == proto].set_index("attacker_pct")
                m = [s.loc[p, "mean"] if p in s.index else 0 for p in pcts_n]
                e = [s.loc[p, "std"] if p in s.index else 0 for p in pcts_n]
                offset = (j - (n_protos - 1) / 2) * bar_w
                ax_n.bar(
                    x_n + offset, m, bar_w, yerr=e,
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

def parse_chart_indices(raw: str) -> set[int]:
    """Parse --charts values like '4,8,12' into a set of ints."""
    out = set()
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            idx = int(p)
        except ValueError as exc:
            raise ValueError(f"invalid chart index {p!r}") from exc
        if idx <= 0:
            raise ValueError(f"chart index must be > 0, got {idx}")
        out.add(idx)
    if not out:
        raise ValueError("no valid chart indices provided")
    return out

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
    parser.add_argument(
        "--charts", default="",
        help="Comma-separated chart indices to generate only specific charts, e.g. 4,8,12",
    )
    args = parser.parse_args()
    if not (0.0 <= args.cdf_x_lower_q < args.cdf_x_upper_q <= 1.0):
        print("Invalid CDF x-axis quantiles: require 0 <= lower < upper <= 1.", file=sys.stderr)
        sys.exit(2)
    selected_charts = None
    if args.charts.strip():
        try:
            selected_charts = parse_chart_indices(args.charts)
        except ValueError as err:
            print(f"Invalid --charts: {err}", file=sys.stderr)
            sys.exit(2)
        print(f"Selected chart indices: {sorted(selected_charts)}")

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
    print(f"  Protocols: {sorted(df['protocol'].unique())}")

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
    def should_generate(idx: int) -> bool:
        return selected_charts is None or idx in selected_charts

    if should_generate(1) or should_generate(2):
        chart_per_pair_scatter(pair_stats, args.out, group_by)
    if should_generate(3):
        chart_per_topology_scatter(topo_stats, args.out)
    if should_generate(4):
        chart_cdf_per_pair(pair_stats, args.out, cdf_zoom=cdf_zoom)
    if should_generate(5):
        chart_cdf_per_topology(topo_stats, args.out, cdf_zoom=cdf_zoom)
    if should_generate(6):
        chart_cdf_raw(df, args.out, cdf_zoom=cdf_zoom)
    if should_generate(7):
        chart_improvement_heatmap(topo_stats, args.out)
    if should_generate(8):
        chart_distributions(df, args.out)
    if should_generate(9):
        chart_topology_delta_cdf(topo_stats, args.out, cdf_zoom=cdf_zoom)
    if should_generate(10) or should_generate(11):
        chart_attacker_accuracy(exports, args.out)
    if should_generate(12):
        chart_topology_p50_p95_histograms(topo_stats, args.out)
    print("Done.")


if __name__ == "__main__":
    main()
