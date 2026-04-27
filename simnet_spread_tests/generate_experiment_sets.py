import pandas as pd
import numpy as np

def generate_base_sets(max_fanout_ext=15, max_fanout_intra=5, prob_step=0.025):
    """
    Generates all sets that satisfy:
      1 * (1 - p_i) + p_i * f_i + p_e * f_e = 6
    which is equivalent to:
      p_i * (f_i - 1) + p_e * f_e = 5
    """
    results = []

    for f_i in range(2, max_fanout_intra + 1):
        for f_e in range(1, max_fanout_ext + 1):
            for p_i in np.arange(prob_step, 1 + prob_step, prob_step):
                p_i = round(p_i, 4)
                for p_e in np.arange(0, 1 + prob_step, prob_step):
                    p_e_rounded = round(p_e, 4)
                    expr = (1 * (1 - p_i)) + (p_i * f_i) + (p_e_rounded * f_e)
                    expr_rounded = round(expr, 4)

                    if expr_rounded != 6:
                        continue

                    results.append({
                        'p_i': p_i,
                        'f_i': f_i,
                        'p_e': p_e_rounded,
                        'f_e': f_e,
                        'expr': expr_rounded,
                        'Ce': round(p_e_rounded * f_e, 4),
                        'Ci': round(p_i * (f_i - 1), 4),
                    })

    return pd.DataFrame(results)


def _attach_group(df, group, subgroup):
    if df.empty:
        return df
    out = df.copy()
    out["group"] = group
    out["subgroup"] = subgroup
    return out


def _group_a_exact(ce, canonical_pe, max_fanout_intra):
    """
    Fix p_e=canonical_pe, f_e=round(ce/canonical_pe).
    Enumerate f_i from 2..max_fanout_intra; compute p_i = (5-ce)/(f_i-1).
    Keeps rows where 0 < p_i <= 1, sorted by f_i ascending.
    """
    f_e = int(round(ce / canonical_pe))
    p_e = canonical_pe
    ci_need = 5.0 - p_e * f_e  # p_i*(f_i-1) needed
    rows = []
    for f_i in range(2, max_fanout_intra + 1):
        p_i = ci_need / (f_i - 1)
        if 0 < p_i <= 1:
            rows.append({
                'p_i': round(p_i, 4), 'f_i': f_i,
                'p_e': p_e,           'f_e': f_e,
                'Ce': round(p_e * f_e, 4),
                'Ci': round(p_i * (f_i - 1), 4),
            })
    return pd.DataFrame(rows)


def _group_b_exact(ci_total, canonical_pi, canonical_fi, max_fanout_ext):
    """
    Fix p_i=canonical_pi, f_i=canonical_fi.
    Enumerate f_e from 1..max_fanout_ext; compute p_e = (5 - ci_code) / f_e.
    Keeps rows where 0 < p_e <= 1, sorted by f_e ascending.
    ci_total is the group label (= 1 + p_i*(f_i-1)).
    """
    ci_code = canonical_pi * (canonical_fi - 1)
    ce_need = 5.0 - ci_code  # p_e*f_e needed
    rows = []
    for f_e in range(1, max_fanout_ext + 1):
        p_e = ce_need / f_e
        if 0 < p_e <= 1:
            rows.append({
                'p_i': canonical_pi, 'f_i': canonical_fi,
                'p_e': round(p_e, 4), 'f_e': f_e,
                'Ce': round(ce_need, 4),
                'Ci': round(ci_code, 4),
            })
    return pd.DataFrame(rows)


def build_groups(
    max_fanout_ext=20,
    max_fanout_intra=15,
    prob_step=0.025,
    ce_targets=(1.5, 3.0, 4.5),
    ce_canonical_pe=0.3,
    ci_pairs=((1.5, 0.25, 3), (2.0, 0.5, 3), (2.5, 0.5, 4)),
    fixed_fanouts=((3, 6),),
):
    """
    Build 4 groups:
      A) For each Ce in ce_targets: fix (p_e=ce_canonical_pe, f_e=Ce/p_e),
         enumerate f_i and compute exact p_i. Rows sorted by f_i ascending.
      B) For each (ci_total, p_i, f_i) in ci_pairs: fix the intra side,
         enumerate f_e and compute exact p_e. Rows sorted by f_e ascending.
      C) Fix fanouts (f_i, f_e) from fixed_fanouts, vary (p_i, p_e) on grid.
      D) Fanouts vs Bernoullis diagonal from grid (high_fanout_low_prob /
         low_fanout_high_prob).
    """
    base = generate_base_sets(
        max_fanout_ext=max_fanout_ext,
        max_fanout_intra=max_fanout_intra,
        prob_step=prob_step,
    )

    tol = prob_step / 2
    groups = []

    # Group A: exact enumeration with fixed canonical (p_e, f_e) per Ce target.
    for ce in ce_targets:
        sub = _group_a_exact(ce, ce_canonical_pe, max_fanout_intra)
        groups.append(_attach_group(sub, "A", f"Ce={ce}"))

    # Group B: exact enumeration with fixed canonical (p_i, f_i) per Ci target.
    for ci_total, pi, fi in ci_pairs:
        sub = _group_b_exact(ci_total, pi, fi, max_fanout_ext)
        groups.append(_attach_group(sub, "B", f"Ci={ci_total}"))

    # Group C: fix fanouts, vary probabilities (grid-based).
    for f_i, f_e in fixed_fanouts:
        sel = base[(base["f_i"] == f_i) & (base["f_e"] == f_e)]
        groups.append(_attach_group(sel, "C", f"f_i={f_i},f_e={f_e}"))

    # Group D: inverse relation between fanouts and Bernoullis (grid-based).
    fanout_sum = base["f_i"] + base["f_e"]
    prob_sum = base["p_i"] + base["p_e"]
    hi_fan = fanout_sum >= fanout_sum.quantile(0.75)
    lo_fan = fanout_sum <= fanout_sum.quantile(0.25)
    hi_prob = prob_sum >= prob_sum.quantile(0.75)
    lo_prob = prob_sum <= prob_sum.quantile(0.25)

    d1 = base[hi_fan & lo_prob]
    d2 = base[lo_fan & hi_prob]
    groups.append(_attach_group(d1, "D", "high_fanout_low_prob"))
    groups.append(_attach_group(d2, "D", "low_fanout_high_prob"))

    combined = pd.concat([g for g in groups if not g.empty], ignore_index=True)
    return base, combined


if __name__ == "__main__":
    df_base, df_groups = build_groups(
        max_fanout_ext=20,
        max_fanout_intra=15,
        prob_step=0.025,
    )

    df_base.to_csv("simnet_spread_tests/p2p_experimental_sets_all.csv", index=False)
    df_groups.to_csv("simnet_spread_tests/p2p_experimental_sets_groups.csv", index=False)

    for grp in ("A", "B", "C", "D"):
        sub = df_groups[df_groups["group"] == grp]
        sub.to_csv(f"simnet_spread_tests/p2p_experimental_sets_{grp}.csv", index=False)

    print(f"All valid sets: {len(df_base)}")
    print(f"Grouped sets: {len(df_groups)}")
    if not df_groups.empty:
        print(df_groups.sample(min(10, len(df_groups))).to_string(index=False))
