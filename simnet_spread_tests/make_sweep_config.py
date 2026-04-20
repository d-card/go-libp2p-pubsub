#!/usr/bin/env python3
"""
Build a sweep_config.yaml with 8 groups, sampling from generated experiment sets
so each group has a tractable number of points (~5–7).

Groups produced:
  A_Ce1.5  A_Ce3.0  A_Ce4.5    — 3 lines: fix (p_e, f_e) canonically, vary (p_i, f_i)
  B_Ci1.5  B_Ci2.0  B_Ci2.5    — 3 lines: fix (p_i, f_i) canonically, vary (p_e, f_e)
  C_fixed_fanouts              — 1 line: fix (f_i, f_e), vary (p_i, p_e)
  D_fanouts_vs_bernoullis      — 1 line: diagonal trade-off

Usage (from repo root):
    python3 simnet_spread_tests/make_sweep_config.py \\
        --out simnet_spread_tests/sweep_config.yaml \\
        [--points-per-line 6]
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_experiment_sets import build_groups  # type: ignore


def first_n_by(df, key, n):
    """Return first n rows sorted ascending by `key`."""
    if df.empty:
        return df
    return df.sort_values(key).head(n).reset_index(drop=True)


def evenly_spaced(df, key, n):
    """Return up to n rows evenly spaced along `key` (sorted)."""
    if df.empty:
        return df
    df = df.sort_values(key).reset_index(drop=True)
    if len(df) <= n:
        return df
    idx = np.linspace(0, len(df) - 1, n).round().astype(int)
    return df.iloc[idx].drop_duplicates(subset=[key]).reset_index(drop=True)


def row_to_cfg(row):
    """CSV row -> [p_i, f_i, p_e, f_e] list."""
    p_i = round(float(row["p_i"]), 4)
    f_i = int(row["f_i"])
    p_e = round(float(row["p_e"]), 4)
    f_e = int(row["f_e"])
    return [p_i, f_i, p_e, f_e]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True, help="Path to write the YAML")
    p.add_argument("--points-per-line", type=int, default=6,
                   help="Target points per group/line (default: 6)")
    p.add_argument("--max-fanout-intra", type=int, default=15)
    p.add_argument("--max-fanout-ext",   type=int, default=20)
    p.add_argument("--canonical-pe", type=float, default=0.3,
                   help="Fixed p_e for all Group A configs (default: 0.3)")
    p.add_argument("--ci-pairs", default="1.5,0.25,3;2.0,0.5,3;2.5,0.5,4",
                   help="Semicolon-separated ci_total,p_i,f_i triples for Group B "
                        "(default: 1.5,0.25,3;2.0,0.5,3;2.5,0.5,4)")
    p.add_argument("--fixed-fanouts", default="3,6",
                   help="Fanouts (f_i,f_e) to use for Group C")
    p.add_argument("--topologies", default="1337,1338,1339",
                   help="Comma-separated list of topology seeds")
    p.add_argument("--nodes", type=int, default=30,
                   help="SPREAD_SIMNET_NODES (default: 30)")
    p.add_argument("--trials", type=int, default=30,
                   help="SPREAD_SIMNET_TRIALS — messages published per run (default: 30)")
    args = p.parse_args()

    ci_pairs = tuple(
        (float(a), float(b), int(c))
        for a, b, c in (triple.split(",") for triple in args.ci_pairs.split(";"))
    )

    base, grouped = build_groups(
        max_fanout_ext=args.max_fanout_ext,
        max_fanout_intra=args.max_fanout_intra,
        ce_targets=(1.5, 3.0, 4.5),
        ce_canonical_pe=args.canonical_pe,
        ci_pairs=ci_pairs,
        fixed_fanouts=(tuple(int(x) for x in args.fixed_fanouts.split(",")),),
    )

    out_groups = {}
    n = args.points_per_line

    # Groups A: fixed canonical (p_e, f_e), vary (p_i, f_i) — first n by f_i ascending
    for ce in (1.5, 3.0, 4.5):
        sub = grouped[(grouped["group"] == "A") & (grouped["subgroup"] == f"Ce={ce}")]
        sampled = first_n_by(sub, "f_i", n)
        if not sampled.empty:
            out_groups[f"A_Ce{ce}"] = [row_to_cfg(r) for _, r in sampled.iterrows()]

    # Groups B: fixed canonical (p_i, f_i), vary (p_e, f_e) — first n by f_e ascending
    for ci_total, _pi, _fi in ci_pairs:
        sub = grouped[(grouped["group"] == "B") & (grouped["subgroup"] == f"Ci={ci_total}")]
        sampled = first_n_by(sub, "f_e", n)
        if not sampled.empty:
            out_groups[f"B_Ci{ci_total}"] = [row_to_cfg(r) for _, r in sampled.iterrows()]

    # Group C: fix fanouts, vary p_i — evenly spaced
    fi, fe = (int(x) for x in args.fixed_fanouts.split(","))
    sub = grouped[(grouped["group"] == "C") & (grouped["subgroup"] == f"f_i={fi},f_e={fe}")]
    sampled = evenly_spaced(sub, "p_i", n)
    if not sampled.empty:
        out_groups[f"C_f{fi}_f{fe}"] = [row_to_cfg(r) for _, r in sampled.iterrows()]

    # Group D: diagonal walk from high-fanout/low-prob to low-fanout/high-prob
    base_sorted = base.copy()
    base_sorted["fan_sum"]  = base_sorted["f_i"] + base_sorted["f_e"]
    base_sorted["prob_sum"] = base_sorted["p_i"] + base_sorted["p_e"]
    base_sorted["diag"] = base_sorted["fan_sum"] - 10 * base_sorted["prob_sum"]
    sampled = evenly_spaced(base_sorted, "diag", n)
    if not sampled.empty:
        out_groups["D_fanouts_vs_bernoullis"] = [row_to_cfg(r) for _, r in sampled.iterrows()]

    config = {
        "topologies":   [int(x) for x in args.topologies.split(",")],

        "env": {
            "SPREAD_SIMNET_NODES":                    args.nodes,
            "SPREAD_SIMNET_TRIALS":                   args.trials,
            "SPREAD_SIMNET_LINK_MIBPS":               20,
            "SPREAD_SIMNET_SCENARIO_TIMEOUT_MS":      600000,
            "SPREAD_SIMNET_ENABLE_CRASH":             False,
            "SPREAD_SIMNET_CRASH_PCT":                0.0,
            "SPREAD_SIMNET_WARMUP_EVERY":             0,
            "SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH": 1,

            # Clustering
            "SPREAD_CLUSTER_PCT":                     0.25,
            "SPREAD_NUM_RINGS":                       4,
            "SPREAD_FALLBACK_THRESHOLD":              3,
            "SPREAD_DUPLICATE_REPROPAGATION":         5,
            "SPREAD_USE_ANGULAR_INTER_PEERS":         True,

            # Vivaldi
            "SPREAD_CC":                              0.25,
            "SPREAD_CE":                              0.25,
            "SPREAD_NEWTON":                          False,
            "SPREAD_OUTLIER_THRESHOLD":               0,
            "SPREAD_SAMPLES":                         1,
            "SPREAD_INTERVAL_MS":                     150,
            "SPREAD_NEIGHBOR_SET_SIZE":               24,
            "SPREAD_IN1_THRESHOLD_MS":                20,
            "SPREAD_IN2_THRESHOLD_MS":                35,
            "SPREAD_IN3_MADK_RANDOM":                 5,
            "SPREAD_IN3_MADK_CLOSE":                  8,
            "SPREAD_IN3_MIN_SAMPLES":                 4,

            # Attackers
            "SPREAD_ATTACKER_PCTS":                   "0.1,0.2,0.3,0.4,0.5",
        },

        "groups": out_groups,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    class _Dumper(yaml.SafeDumper):
        pass
    def _repr_list(dumper, data):
        is_leaf = all(isinstance(x, (int, float, str, bool)) for x in data)
        return dumper.represent_sequence("tag:yaml.org,2002:seq", data,
                                         flow_style=is_leaf)
    _Dumper.add_representer(list, _repr_list)

    with open(out_path, "w") as f:
        yaml.dump(config, f, Dumper=_Dumper, sort_keys=False, default_flow_style=False)

    total_unique = len({tuple(c) for cfgs in out_groups.values() for c in cfgs})
    total_runs = total_unique * len(config["topologies"])
    print(f"Wrote {out_path}")
    print(f"  Groups:            {len(out_groups)}")
    for name, cfgs in out_groups.items():
        print(f"    {name:<30} {len(cfgs)} configs")
    print(f"  Unique configs:    {total_unique}")
    print(f"  Topologies:        {len(config['topologies'])}")
    print(f"  Total runs needed: {total_runs}")


if __name__ == "__main__":
    main()
