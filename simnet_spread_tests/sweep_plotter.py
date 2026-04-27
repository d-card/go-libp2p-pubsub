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
    # ap = intra probability, af = intra fanout, ep = inter probability, ef = inter fanout.
    return f"ap{p_i}_af{f_i}_ep{p_e}_ef{f_e}"


_TAG_RE        = re.compile(r"^ap([\d.]+)_af(\d+)_ep([\d.]+)_ef(\d+)$")
_LEGACY_TAG_RE = re.compile(r"^ir([\d.]+)_ip([\d.]+)_if(\d+)_ef(\d+)$")


def parse_tag(tag):
    """Parse a config tag into (p_i, f_i, p_e, f_e).

    Accepts both the current scheme (ap/af/ep/ef, grouped order) and the legacy
    scheme (ir/ip/if/ef, interleaved order) so old sweep output dirs remain
    readable if anyone has them.
    """
    m = _TAG_RE.match(tag)
    if m:
        return float(m.group(1)), int(m.group(2)), float(m.group(3)), int(m.group(4))
    m = _LEGACY_TAG_RE.match(tag)
    if m:
        # legacy order: p_i, p_e, f_i, f_e  → return as p_i, f_i, p_e, f_e
        return float(m.group(1)), int(m.group(3)), float(m.group(2)), int(m.group(4))
    return None


def migrate_legacy_run_dirs(runs_dir):
    """Rename any legacy-format config dirs under runs_dir to the new scheme.

    Mirror of the helper in sweep_runner.py. Having it here too means a
    colleague who pulls the new code and runs the plotter first (before
    `sweep_runner --extend`) still gets their dirs migrated transparently.
    No-op if nothing needs renaming.
    """
    runs_dir = Path(runs_dir)
    if not runs_dir.is_dir():
        return
    migrated = 0
    for entry in list(runs_dir.iterdir()):
        if not entry.is_dir():
            continue
        m = _LEGACY_TAG_RE.match(entry.name)
        if not m:
            continue
        p_i, p_e, f_i, f_e = m.group(1), m.group(2), m.group(3), m.group(4)
        new_name = f"ap{p_i}_af{f_i}_ep{p_e}_ef{f_e}"
        target = runs_dir / new_name
        if target.exists():
            print(f"  legacy-migrate: SKIP {entry.name} -> {new_name} (target exists)",
                  file=sys.stderr)
            continue
        entry.rename(target)
        migrated += 1
    if migrated:
        print(f"  legacy-migrate: renamed {migrated} dir(s) "
              f"ir*_ip*_if*_ef* -> ap*_af*_ep*_ef*")


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
    """Compute per-config aggregated metrics across all topologies.

    Collects every delivery's `stretch` and `latency_ms` across the exports
    (flattened over trials and topologies), then summarizes each with
    mean / median / p90 / p95 / p99. Accuracy buckets are averaged over
    topologies at the requested attacker_pct.
    """
    gs_s, sp_s = [], []   # stretch values
    gs_l, sp_l = [], []   # latency values (ms)
    gs_a, sp_a = [], []   # accuracy at the requested attacker_pct
    gs_curve, sp_curve = defaultdict(list), defaultdict(list)

    for exp in exports:
        # `.get(..., [])` only defaults on missing keys; a present-but-null value
        # (which the Go test emits when SPREAD_SIMNET_SKIP_SCENARIO was set)
        # would come back as None. `... or []` normalizes both to an empty list.
        for t in (exp.get("gossipsub") or []):
            for d in t.get("deliveries", []):
                gs_s.append(d["stretch"])
                gs_l.append(d["latency_ms"])
        for t in (exp.get("spread") or []):
            for d in t.get("deliveries", []):
                sp_s.append(d["stretch"])
                sp_l.append(d["latency_ms"])

        for e in (exp.get("gossipsub_attacker_accuracy") or []):
            p = e["attacker_pct"]
            gs_curve[p].append(e["accuracy"])
            if abs(p - attacker_pct) < 0.01:
                gs_a.append(e["accuracy"])
        for e in (exp.get("spread_attacker_accuracy") or []):
            p = e["attacker_pct"]
            sp_curve[p].append(e["accuracy"])
            if abs(p - attacker_pct) < 0.01:
                sp_a.append(e["accuracy"])

    # Keep a config as long as it has any data on either side. A config with
    # only spread (no gossipsub baseline) still plots correctly; the missing
    # side just doesn't contribute to the baseline marker.
    if not gs_s and not sp_s:
        return None

    def _stats(values, percentiles):
        if not values:
            return {k: None for k in ("mean", "median", *(f"p{p}" for p in percentiles))}
        out = {"mean": float(np.mean(values)), "median": float(np.median(values))}
        for p in percentiles:
            out[f"p{p}"] = float(np.percentile(values, p))
        return out

    gs_stretch = _stats(gs_s, [90, 95, 99])
    sp_stretch = _stats(sp_s, [90, 95, 99])
    gs_latency = _stats(gs_l, [95])
    sp_latency = _stats(sp_l, [95])

    return {
        "n_topologies":       len(exports),
        "n_deliveries_gs":    len(gs_s),
        "n_deliveries_sp":    len(sp_s),

        "gs_stretch_mean":    gs_stretch["mean"],
        "sp_stretch_mean":    sp_stretch["mean"],
        "gs_stretch_median":  gs_stretch["median"],
        "sp_stretch_median":  sp_stretch["median"],
        "gs_stretch_p90":     gs_stretch["p90"],
        "sp_stretch_p90":     sp_stretch["p90"],
        "gs_stretch_p95":     gs_stretch["p95"],
        "sp_stretch_p95":     sp_stretch["p95"],
        "gs_stretch_p99":     gs_stretch["p99"],
        "sp_stretch_p99":     sp_stretch["p99"],

        "gs_latency_mean":    gs_latency["mean"],
        "sp_latency_mean":    sp_latency["mean"],
        "gs_latency_median":  gs_latency["median"],
        "sp_latency_median":  sp_latency["median"],
        "gs_latency_p95":     gs_latency["p95"],
        "sp_latency_p95":     sp_latency["p95"],

        "gs_accuracy":        float(np.mean(gs_a)) if gs_a else None,
        "sp_accuracy":        float(np.mean(sp_a)) if sp_a else None,

        "gs_accuracy_curve":  {float(k): float(np.mean(v)) for k, v in gs_curve.items()},
        "sp_accuracy_curve":  {float(k): float(np.mean(v)) for k, v in sp_curve.items()},
    }


def _discover_gossipsub_dir(data_dir, gossipsub_d=None):
    """Return the Path to the gossipsub subdir to read from, or None.

    If `gossipsub_d` is a dict like {"D": 8, "D_low": 6, "D_high": 12}, require
    that exact subdir to exist. Otherwise, auto-pick the single d*_dl*_dh*
    subdir present under <data_dir>/gossipsub/. Error if multiple exist and
    no selector was given — user must disambiguate.
    """
    root = Path(data_dir) / "gossipsub"
    if not root.is_dir():
        return None
    candidates = sorted(p for p in root.iterdir()
                        if p.is_dir() and re.match(r"^d\d+_dl\d+_dh\d+$", p.name))
    if not candidates:
        return None
    if gossipsub_d:
        name = f"d{gossipsub_d['D']}_dl{gossipsub_d['D_low']}_dh{gossipsub_d['D_high']}"
        for c in candidates:
            if c.name == name:
                return c
        raise ValueError(f"gossipsub_d {gossipsub_d} -> subdir {name!r} not found "
                         f"under {root} (have: {[c.name for c in candidates]})")
    if len(candidates) > 1:
        raise ValueError(
            f"multiple gossipsub D-param subdirs found under {root} "
            f"({[c.name for c in candidates]}); specify gossipsub_d in plot_groups.yaml"
        )
    return candidates[0]


def build_global_gs_baseline(data_dir, attacker_pct, topology_filter=None,
                             gossipsub_d=None):
    """Pool every gossipsub delivery into one baseline across the sweep.

    New layout: reads <data_dir>/gossipsub/<d-subdir>/topo-*.json directly —
    one file per seed, already deduped by the runner's output structure.

    Legacy layout fallback: if no new-layout gossipsub dir exists, scans the
    old <data_dir>/runs/<tag>/topo-*.json files and dedups gs data per seed
    (keeping the longest trial list seen).

    Returns a dict with keys matching the per-config aggregate shape
    (`gs_stretch_mean`, `gs_latency_p95`, `gs_accuracy`, …) or None if no
    gossipsub data exists anywhere in the sweep.
    """
    gs_by_seed = {}
    gs_dir = _discover_gossipsub_dir(data_dir, gossipsub_d=gossipsub_d)
    if gs_dir is not None:
        for path in sorted(glob.glob(os.path.join(gs_dir, "topo-*.json"))):
            if ".meta." in path:
                continue
            m = _TOPO_SEED_RE.search(path)
            if not m:
                continue
            seed = int(m.group(1))
            if topology_filter and seed not in {int(s) for s in topology_filter}:
                continue
            try:
                with open(path) as f:
                    doc = json.load(f)
            except Exception:
                continue
            gs = doc.get("gossipsub") or []
            if not gs:
                continue
            gs_by_seed[seed] = {"gs": gs, "acc": doc.get("gossipsub_attacker_accuracy") or []}
    else:
        # Legacy: scrape from old combined-scenario exports under runs/<tag>/
        legacy_runs = Path(data_dir) / "runs"
        if legacy_runs.is_dir():
            for cfg_dir in sorted(legacy_runs.iterdir()):
                if not cfg_dir.is_dir():
                    continue
                for path in sorted(glob.glob(os.path.join(cfg_dir, "topo-*.json"))):
                    if ".meta." in path:
                        continue
                    m = _TOPO_SEED_RE.search(path)
                    if not m:
                        continue
                    seed = int(m.group(1))
                    if topology_filter and seed not in {int(s) for s in topology_filter}:
                        continue
                    try:
                        with open(path) as f:
                            doc = json.load(f)
                    except Exception:
                        continue
                    gs = doc.get("gossipsub") or []
                    if not gs:
                        continue
                    prev = gs_by_seed.get(seed)
                    if prev is None or len(gs) > len(prev["gs"]):
                        gs_by_seed[seed] = {"gs": gs, "acc": doc.get("gossipsub_attacker_accuracy") or []}

    if not gs_by_seed:
        return None

    stretch, latency = [], []
    acc_vals, acc_curve = [], defaultdict(list)
    for _, bundle in gs_by_seed.items():
        for t in bundle["gs"]:
            for d in t.get("deliveries", []):
                stretch.append(d["stretch"])
                latency.append(d["latency_ms"])
        for e in bundle["acc"]:
            p = e["attacker_pct"]
            acc_curve[p].append(e["accuracy"])
            if abs(p - attacker_pct) < 0.01:
                acc_vals.append(e["accuracy"])

    def _pct(values, p):
        return float(np.percentile(values, p)) if values else None

    return {
        "n_seeds":            len(gs_by_seed),
        "n_deliveries":       len(stretch),
        "gs_stretch_mean":    float(np.mean(stretch))   if stretch else None,
        "gs_stretch_median":  float(np.median(stretch)) if stretch else None,
        "gs_stretch_p90":     _pct(stretch, 90),
        "gs_stretch_p95":     _pct(stretch, 95),
        "gs_stretch_p99":     _pct(stretch, 99),
        "gs_latency_mean":    float(np.mean(latency))   if latency else None,
        "gs_latency_median": float(np.median(latency))  if latency else None,
        "gs_latency_p95":     _pct(latency, 95),
        "gs_accuracy":        float(np.mean(acc_vals))  if acc_vals else None,
    }


def _spread_runs_dir(data_dir):
    """Resolve the per-config spread output root.

    New layout: <data_dir>/spread/runs/
    Legacy layout: <data_dir>/runs/  (one JSON per (config, seed), with both
    gossipsub and spread data interleaved)
    """
    new = Path(data_dir) / "spread" / "runs"
    if new.is_dir():
        return new
    return Path(data_dir) / "runs"


def build_config_index(data_dir, attacker_pct, topology_filter=None):
    """Scan the per-config spread dirs and return {tag: metrics}."""
    runs_dir = _spread_runs_dir(data_dir)
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
            "gs_stretch_p95", "sp_stretch_p95",
            "gs_stretch_p99", "sp_stretch_p99",
            "gs_latency_mean", "sp_latency_mean",
            "gs_latency_median", "sp_latency_median",
            "gs_latency_p95", "sp_latency_p95",
            "gs_accuracy", "sp_accuracy"]
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for tag, m in sorted(index.items()):
            w.writerow([m.get(c) for c in cols])
    print(f"  Saved: {out_path}")


def chart_C_colored(index, groups, out_path, color_by="C",
                    metric="stretch", stat="mean",
                    gs_baseline=None):
    """Scatter: X = Spread metric, Y = deanon accuracy, color = continuous C or Ci/C.

    Only configs that appear in `groups` are plotted (same set as chart_anon_vs_metric).
    color_by: "C"       — total expected messages: Ci + Ce = 1 + p_i*(f_i-1) + p_e*f_e
              "Ci_frac" — intra fraction: Ci / C
    """
    x_key    = f"sp_{metric}_{stat}"
    gs_x_key = f"gs_{metric}_{stat}"
    anon_key = "sp_accuracy"

    fig, ax = plt.subplots(figsize=(11, 8))

    xs, ys, cs, labels = [], [], [], []
    seen = set()
    for cfg_list in groups.values():
        for cfg in cfg_list:
            p_i, f_i, p_e, f_e = cfg
            tag = config_tag(p_i, f_i, p_e, f_e)
            if tag in seen:
                continue
            seen.add(tag)
            m = index.get(tag)
            if m is None or m.get(x_key) is None or m.get(anon_key) is None:
                continue
            ci = 1.0 + p_i * (f_i - 1)
            ce = p_e * f_e
            C  = ci + ce
            c_val = C if color_by == "C" else (ci / C if C > 0 else 0.0)
            xs.append(m[x_key])
            ys.append(m[anon_key])
            cs.append(c_val)
            labels.append((p_i, f_i, p_e, f_e))

    if not xs:
        print(f"  warn: no data to plot for {out_path}")
        return

    # GossipSub baseline star
    baseline_x = baseline_y = None
    if gs_baseline is not None:
        baseline_x = gs_baseline.get(gs_x_key)
        baseline_y = gs_baseline.get("gs_accuracy")
    if baseline_x is not None and baseline_y is not None:
        ax.scatter(baseline_x, baseline_y,
                   color="black", marker="*", s=260, zorder=10,
                   label="GossipSub baseline", edgecolors="white", linewidths=1.2)

    cmap = "viridis" if color_by == "C" else "plasma"
    sc = ax.scatter(xs, ys, c=cs, cmap=cmap, s=90,
                    edgecolors="white", linewidths=0.8, zorder=5,
                    vmin=min(cs), vmax=max(cs))
    cbar = fig.colorbar(sc, ax=ax, pad=0.02)
    if color_by == "C":
        cbar.set_label(r"$C = C_i + C_e$  (total expected messages)", fontsize=10)
        title_color = r"total expectation $C$"
    else:
        cbar.set_label(r"$C_i / C$  (intra fraction)", fontsize=10)
        title_color = r"intra fraction $C_i / C$"

    for (p_i, f_i, p_e, f_e), x, y in zip(labels, xs, ys):
        ax.annotate(f"({p_i},{f_i},{p_e},{f_e})",
                    xy=(x, y), xytext=(4, 4), textcoords="offset points",
                    fontsize=5.5, alpha=0.7)

    x_unit = " (ms)" if metric == "latency" else ""
    ax.set_xlabel(f"Spread {metric} ({stat}){x_unit}  →  worse", fontsize=11)
    ax.set_ylabel("Spread deanon accuracy  →  worse", fontsize=11)
    ax.set_title(
        f"Anonymity vs {metric.capitalize()} ({stat}) — colored by {title_color}.\n"
        "Lower-left = better trade-off. Darker = lower value.",
        fontsize=11)
    if baseline_x is not None and baseline_y is not None:
        ax.legend(fontsize=8, loc="best")

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def chart_anon_vs_metric(index, groups, out_path,
                         metric="stretch", stat="mean",
                         sort_key="p_i",
                         gs_baseline=None):
    """
    Scatter: X = Spread `<metric>_<stat>`, Y = Spread deanon accuracy.
    One connected line per group. Points sorted by `sort_key` within a group.

    metric: "stretch" | "latency"
    stat:   "mean" | "median" | "p90" | "p95" | "p99"  (what's actually
            available depends on what aggregate() computed for `metric`)
    """
    x_key    = f"sp_{metric}_{stat}"
    gs_x_key = f"gs_{metric}_{stat}"
    anon_key = "sp_accuracy"

    fig, ax = plt.subplots(figsize=(11, 8))

    palette = sns.color_palette("tab10", n_colors=max(len(groups), 1))
    markers = ["o", "s", "D", "^", "v", "P", "X", "*", "h", "<"]

    # Plot GossipSub baseline. Prefer the globally-pooled baseline computed
    # once across the whole sweep (unbiased when gs coverage is uneven across
    # configs). Fall back to averaging per-config means if no pooled baseline
    # was provided.
    baseline_x = baseline_y = None
    if gs_baseline is not None:
        baseline_x = gs_baseline.get(gs_x_key)
        baseline_y = gs_baseline.get("gs_accuracy")
    if baseline_x is None or baseline_y is None:
        all_gs_x    = [m[gs_x_key] for m in index.values() if m.get(gs_x_key) is not None]
        all_gs_anon = [m["gs_accuracy"] for m in index.values() if m.get("gs_accuracy") is not None]
        if all_gs_x and all_gs_anon:
            baseline_x = float(np.mean(all_gs_x))
            baseline_y = float(np.mean(all_gs_anon))
    if baseline_x is not None and baseline_y is not None:
        ax.scatter(baseline_x, baseline_y,
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
            if m.get(x_key) is None or m.get(anon_key) is None:
                continue
            points.append((m["p_i"], m["f_i"], m["p_e"], m["f_e"],
                           m[x_key], m[anon_key]))

        if missing:
            print(f"  warn: group '{group_name}' missing data for: {missing}")

        if not points:
            continue

        # Sort points by the chosen key for a meaningful line connection.
        # "x" sorts by the current X axis (whatever metric/stat we're drawing).
        sort_idx = {"p_i": 0, "f_i": 1, "p_e": 2, "f_e": 3, "x": 4, "anon": 5}[sort_key]
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

    x_unit = " (ms)" if metric == "latency" else ""
    ax.set_xlabel(f"Spread {metric} ({stat}){x_unit}  →  worse", fontsize=11)
    ax.set_ylabel(f"Spread deanon accuracy  →  worse", fontsize=11)
    ax.set_title(
        f"Anonymity vs {metric.capitalize()} ({stat}) — each point is a parameter config, each line a group.\n"
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
    p.add_argument("--attacker-pct", type=float, default=0.1,
                   help="Attacker fraction for the deanon metric (default: 0.1)")
    p.add_argument("--sort-key", default="p_i",
                   choices=["p_i", "f_i", "p_e", "f_e", "x", "anon"],
                   help="Within a group, connect points sorted by this (default: p_i). "
                        "\"x\" sorts by the current chart's X axis.")
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

    # Optional: pick which gossipsub D-param subdir to use for the baseline.
    # Required only when multiple gossipsub/d*_dl*_dh*/ exist under the sweep.
    gossipsub_d = plot_cfg.get("gossipsub_d") or None
    if gossipsub_d is not None:
        missing = {"D", "D_low", "D_high"} - set(gossipsub_d)
        if missing:
            print(f"plot config error: gossipsub_d missing keys: {sorted(missing)}",
                  file=sys.stderr)
            sys.exit(2)

    print(f"Loading data from: {data_dir}")
    print(f"Attacker pct:      {args.attacker_pct}")
    if topology_filter:
        print(f"Topology filter:   {topology_filter}")

    # Rename any legacy-format config dirs in place before indexing, so old
    # sweep outputs are picked up by the new-format scans downstream. Run on
    # whichever spread-runs dir is in use (new or legacy layout).
    migrate_legacy_run_dirs(_spread_runs_dir(data_dir))

    index = build_config_index(data_dir, args.attacker_pct,
                               topology_filter=topology_filter)
    print(f"Loaded metrics for {len(index)} configs.")

    gs_baseline = build_global_gs_baseline(data_dir, args.attacker_pct,
                                           topology_filter=topology_filter,
                                           gossipsub_d=gossipsub_d)
    if gs_baseline is not None:
        print(f"Pooled gossipsub baseline: {gs_baseline['n_seeds']} seeds, "
              f"{gs_baseline['n_deliveries']} deliveries")
    else:
        print("No gossipsub data found — baseline marker will be omitted.")
    print()

    print(f"Writing outputs to {out_dir}/")
    write_metrics_csv(index, out_dir / f"metrics_attacker{args.attacker_pct}.csv")

    # Emit one scatter per (metric, stat). Keys must match what aggregate()
    # produces — see that function for the full set.
    chart_variants = [
        ("stretch", "mean"),
        ("stretch", "median"),
        ("stretch", "p95"),
        ("latency", "mean"),
        ("latency", "median"),
        ("latency", "p95"),
    ]
    for metric, stat in chart_variants:
        chart_anon_vs_metric(
            index, groups,
            out_dir / f"scatter_anon_vs_{metric}_{stat}.png",
            metric=metric, stat=stat,
            sort_key=args.sort_key,
            gs_baseline=gs_baseline,
        )

    # Color-coded plots: all configs as points, colored by C or Ci/C.
    for metric, stat in chart_variants:
        for color_by, suffix in [("C", "C"), ("Ci_frac", "Ci_frac")]:
            chart_C_colored(
                index, groups,
                out_dir / f"scatter_anon_vs_{metric}_{stat}_color_{suffix}.png",
                color_by=color_by,
                metric=metric, stat=stat,
                gs_baseline=gs_baseline,
            )

    print("\nDone.")


if __name__ == "__main__":
    main()
