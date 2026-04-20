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
                        'Ce': round(p_e_rounded * f_e, 4),        # External contribution
                        'Ci': round(p_i * (f_i - 1), 4),          # Internal contribution (eq-compatible)
                    })

    return pd.DataFrame(results)


def _attach_group(df, group, subgroup):
    if df.empty:
        return df
    out = df.copy()
    out["group"] = group
    out["subgroup"] = subgroup
    return out


def build_groups(
    max_fanout_ext=15,
    max_fanout_intra=5,
    prob_step=0.025,
    ce_targets=(1.5, 3.0, 4.5),
    ci_targets=(1.5, 3.0, 4.5),
    fixed_fanouts=((2, 3), (3, 6), (5, 10)),
):
    """
    Build 4 groups:
      A) Fix Ce in ce_targets, vary (f_i, p_i)
      B) Fix Ci in ci_targets, vary (f_e, p_e)
      C) Fix fanouts (f_i, f_e), vary (p_i, p_e)
      D) Fanouts vs Bernoullis trade-off slices:
         - D_high_fanout_low_prob
         - D_low_fanout_high_prob
    """
    base = generate_base_sets(
        max_fanout_ext=max_fanout_ext,
        max_fanout_intra=max_fanout_intra,
        prob_step=prob_step,
    )

    # Tolerance aligned with probability grid step.
    tol = prob_step / 2
    groups = []

    # Group A: Fix one canonical (p_e, f_e) pair per Ce target, vary (p_i, f_i).
    # Picking the pair that gives the most rows (most variation on the intra side).
    for ce in ce_targets:
        pool = base[np.abs(base["Ce"] - ce) <= tol]
        if pool.empty:
            continue
        best_pe, best_fe = pool.groupby(["p_e", "f_e"]).size().idxmax()
        sel = base[(base["p_e"] == best_pe) & (base["f_e"] == best_fe)]
        groups.append(_attach_group(sel, "A", f"Ce={ce}"))

    # Group B: Fix one canonical (p_i, f_i) pair per Ci target, vary (p_e, f_e).
    for ci in ci_targets:
        pool = base[np.abs(base["Ci"] - ci) <= tol]
        if pool.empty:
            continue
        best_pi, best_fi = pool.groupby(["p_i", "f_i"]).size().idxmax()
        sel = base[(base["p_i"] == best_pi) & (base["f_i"] == best_fi)]
        groups.append(_attach_group(sel, "B", f"Ci={ci}"))

    # Group C: Fix fanouts, vary probabilities.
    for f_i, f_e in fixed_fanouts:
        sel = base[(base["f_i"] == f_i) & (base["f_e"] == f_e)]
        groups.append(_attach_group(sel, "C", f"f_i={f_i},f_e={f_e}"))

    # Group D: inverse relation between fanouts and Bernoullis.
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
        max_fanout_ext=15,
        max_fanout_intra=5,
        prob_step=0.05,
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