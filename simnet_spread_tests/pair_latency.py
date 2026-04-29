#!/usr/bin/env python3
"""
Per-pair latency analysis for one (spread config, topology) cell of a sweep.

Given a sweep output directory, a spread config tag, and a topology seed, this
produces:

  * A CSV with one row per delivery (across every trial of both scenarios):
      scenario | trial | source | destination | latency_ms | ideal_latency_ms
              | delta_ms | stretch
    `scenario` is "gossipsub" or "spread".
    `ideal_latency_ms` is `latency_ms / stretch` — the direct-pair latency the
    Go test used as the denominator when computing stretch. Rows where stretch
    is 0 (unknown pairwise weight) get NaN ideal and are dropped from the chart.

  * A chart with ideal latency (ms) on the X axis and delta = actual − ideal
    on the Y axis, colored by scenario.

Usage (from repo root):
    python3 simnet_spread_tests/pair_latency.py \\
        --data-dir simnet_spread_tests/outputs/<sweep> \\
        --tag ap0.25_af3_ep0.9_ef5 \\
        --seed 1337 \\
        [--gossipsub-d 6,5,12]

Outputs land in <data-dir>/analysis/<tag>_topo-<seed>/ by default.
"""

import argparse
import csv
import json
import math
import re
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


_GS_SUBDIR_RE = re.compile(r"^d\d+_dl\d+_dh\d+$")


def parse_d_params(raw):
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            f"--gossipsub-d expects 'D,D_low,D_high' (got {raw!r})")
    try:
        return tuple(int(p) for p in parts)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--gossipsub-d values must be integers (got {raw!r})")


def resolve_gossipsub_file(data_dir, seed, d_params=None):
    """Find gossipsub/<d-subdir>/topo-<seed>.json. Returns Path or None."""
    root = data_dir / "gossipsub"
    if not root.is_dir():
        return None
    candidates = sorted(p for p in root.iterdir()
                        if p.is_dir() and _GS_SUBDIR_RE.match(p.name))
    if d_params:
        D, Dl, Dh = d_params
        target = f"d{D}_dl{Dl}_dh{Dh}"
        picked = next((c for c in candidates if c.name == target), None)
        if picked is None:
            raise FileNotFoundError(
                f"gossipsub D subdir {target!r} not found under {root} "
                f"(have: {[c.name for c in candidates]})")
    else:
        if not candidates:
            return None
        if len(candidates) > 1:
            raise RuntimeError(
                f"multiple gossipsub D subdirs under {root} "
                f"({[c.name for c in candidates]}); specify --gossipsub-d")
        picked = candidates[0]
    f = picked / f"topo-{seed}.json"
    return f if f.is_file() else None


def resolve_spread_file(data_dir, tag, seed):
    """Find spread/runs/<tag>/topo-<seed>.json. Returns Path or None."""
    f = data_dir / "spread" / "runs" / tag / f"topo-{seed}.json"
    return f if f.is_file() else None


def iter_rows(doc, scenario):
    """Yield dicts with the per-delivery fields we care about."""
    trials = doc.get(scenario) or []
    for t in trials:
        trial_idx = t.get("trial")
        src = t.get("src")
        for d in t.get("deliveries", []):
            latency = float(d["latency_ms"])
            stretch = float(d["stretch"])
            if stretch > 0:
                ideal = latency / stretch
                delta = latency - ideal
            else:
                ideal = float("nan")
                delta = float("nan")
            yield {
                "scenario":         scenario,
                "trial":            trial_idx,
                "source":           src,
                "destination":      d["dst"],
                "latency_ms":       latency,
                "ideal_latency_ms": ideal,
                "delta_ms":         delta,
                "stretch":          stretch,
            }


def write_csv(rows, out_path):
    fields = ["scenario", "trial", "source", "destination",
              "latency_ms", "ideal_latency_ms", "delta_ms", "stretch"]
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"  wrote {out_path}")


def plot_delta_vs_ideal(rows, out_path, title_suffix=""):
    gs = [(r["ideal_latency_ms"], r["delta_ms"])
          for r in rows if r["scenario"] == "gossipsub"
          and not math.isnan(r["ideal_latency_ms"])]
    sp = [(r["ideal_latency_ms"], r["delta_ms"])
          for r in rows if r["scenario"] == "spread"
          and not math.isnan(r["ideal_latency_ms"])]

    fig, ax = plt.subplots(figsize=(10, 7))
    if gs:
        xs, ys = zip(*gs)
        ax.scatter(xs, ys, s=14, alpha=0.5, color="tab:orange",
                   label=f"GossipSub (n={len(gs)})")
    if sp:
        xs, ys = zip(*sp)
        ax.scatter(xs, ys, s=14, alpha=0.5, color="tab:blue",
                   label=f"Spread (n={len(sp)})")
    ax.axhline(0, color="black", lw=0.8, alpha=0.4)
    ax.set_xlabel("Ideal (direct-pair) latency [ms]")
    ax.set_ylabel("Delta = actual − ideal [ms]   (lower = closer to optimal)")
    ax.set_title(f"Per-pair latency delta vs ideal{title_suffix}")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  wrote {out_path}")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--data-dir", required=True,
                    help="Sweep output directory (containing gossipsub/ and spread/).")
    ap.add_argument("--tag", required=True,
                    help="Spread config directory name, e.g. ap0.25_af3_ep0.9_ef5.")
    ap.add_argument("--seed", required=True, type=int,
                    help="Topology seed to analyze.")
    ap.add_argument("--gossipsub-d", type=parse_d_params, default=None,
                    help="Comma-separated D,D_low,D_high. Required only if "
                         "multiple gossipsub/d*_dl*_dh*/ subdirs exist.")
    ap.add_argument("--out-dir", default=None,
                    help="Output directory (default: <data-dir>/analysis/<tag>_topo-<seed>/).")
    args = ap.parse_args()

    data_dir = Path(args.data_dir).resolve()
    if args.out_dir:
        out_dir = Path(args.out_dir).resolve()
    else:
        out_dir = data_dir / "analysis" / f"{args.tag}_topo-{args.seed}"
    out_dir.mkdir(parents=True, exist_ok=True)

    sp_path = resolve_spread_file(data_dir, args.tag, args.seed)
    gs_path = resolve_gossipsub_file(data_dir, args.seed, args.gossipsub_d)

    if sp_path is None and gs_path is None:
        print(f"No data found for tag={args.tag} seed={args.seed} under {data_dir}",
              file=sys.stderr)
        sys.exit(1)

    print(f"data-dir: {data_dir}")
    print(f"tag:      {args.tag}")
    print(f"seed:     {args.seed}")
    print(f"spread:   {sp_path or '(missing)'}")
    print(f"gossipsub:{gs_path or '(missing)'}")
    print()

    rows = []
    if sp_path is not None:
        with open(sp_path) as f:
            sp_doc = json.load(f)
        rows.extend(iter_rows(sp_doc, "spread"))
    if gs_path is not None:
        with open(gs_path) as f:
            gs_doc = json.load(f)
        rows.extend(iter_rows(gs_doc, "gossipsub"))

    n_total = len(rows)
    n_nan   = sum(1 for r in rows if math.isnan(r["ideal_latency_ms"]))
    print(f"rows: {n_total}  (with unknown ideal: {n_nan})")

    write_csv(rows, out_dir / "pair_latencies.csv")
    plot_delta_vs_ideal(
        rows,
        out_dir / "delta_vs_ideal.png",
        title_suffix=f"\n{args.tag}, seed={args.seed}",
    )


if __name__ == "__main__":
    main()
