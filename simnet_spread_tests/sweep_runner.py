#!/usr/bin/env python3
"""
Sweep runner — execute a parameter sweep over multiple network topologies.

Input: a YAML config describing:
  topologies      — list of seeds (one per network topology)
  env             — dict of SPREAD_* env vars passed through to the Go test
                    (must include SPREAD_SIMNET_NODES and SPREAD_SIMNET_TRIALS)
  groups          — dict of group_name -> list of [p_i, f_i, p_e, f_e] configs

Output layout (in --out-dir):
  sweep_config.yaml           — copy of input
  runs/
    <config_tag>/
      topo-<seed>.json        — raw per-run export from the Go test
      topo-<seed>.meta.json   — timestamps, duration, group memberships
  results.jsonl               — append-only log of every completed run

Resume: skip any (config, topology) whose topo-<seed>.json already exists.

--extend mode: instead of skipping existing topo files, run SPREAD_SIMNET_TRIALS
more trials on top of each one and merge the results. Missing topo files are run
fresh. Continuation is deterministic: trial N's source and RNG are a pure
function of (seed, N), so extending from 30 → 90 is byte-identical to running
90 from scratch.

Usage (from repo root):
    python3 simnet_spread_tests/sweep_runner.py \
        --config simnet_spread_tests/sweep_config_example.yaml \
        --out-dir simnet_spread_tests/outputs/sweep_$(date +%Y%m%d_%H%M%S)
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import yaml


def config_tag(p_i, f_i, p_e, f_e):
    """Canonical tag. Matches existing convention (ir=rho_i, ip=prob_e, if=fanout_i, ef=fanout_e)."""
    return f"ir{p_i}_ip{p_e}_if{f_i}_ef{f_e}"


def build_env(base_env, extra):
    env = os.environ.copy()
    env.update({k: str(v).lower() if isinstance(v, bool) else str(v)
                for k, v in base_env.items()})
    env.update({k: str(v).lower() if isinstance(v, bool) else str(v)
                for k, v in extra.items()})
    return env


def existing_trial_count(export_path):
    """Return the number of trials already recorded in an existing topo JSON.

    Uses len(gossipsub) as the source of truth (matches len(spread) — both are
    written together). Returns 0 if the file is missing or unreadable.
    """
    try:
        with open(export_path) as f:
            doc = json.load(f)
        gs = doc.get("gossipsub") or []
        sp = doc.get("spread") or []
        # Sanity check — both scenarios should have the same count.
        if len(gs) != len(sp):
            print(f"   ⚠️  mismatch in {export_path}: gossipsub={len(gs)} spread={len(sp)}; "
                  f"using min for start_trial")
            return min(len(gs), len(sp))
        return len(gs)
    except Exception:
        return 0


def recompute_attacker_accuracy(trials):
    """Rebuild the per-attacker-pct accuracy summary from the raw trial list.

    Mirrors summarizeAttackerAccuracy() in simnet_spread_experiment_test.go.
    Needed after merging extended trials into an existing JSON.
    """
    by_pct = {}
    for tr in trials:
        for est in tr.get("attacker_estimates", []) or []:
            pct = est.get("attacker_pct")
            if pct is None:
                continue
            bucket = by_pct.setdefault(pct, {"trials": 0, "correct": 0})
            bucket["trials"] += 1
            if est.get("is_correct"):
                bucket["correct"] += 1
    if not by_pct:
        return None
    out = []
    for pct in sorted(by_pct.keys()):
        b = by_pct[pct]
        acc = (b["correct"] / b["trials"]) if b["trials"] > 0 else 0.0
        out.append({
            "attacker_pct":      pct,
            "trials_observed":   b["trials"],
            "correct_estimates": b["correct"],
            "accuracy":          acc,
        })
    return out


def merge_extension(existing_path, extension_path):
    """Merge an extension export into the existing topo JSON in place.

    Concatenates gossipsub + spread trial arrays, updates `trials`, and
    recomputes the attacker_accuracy summaries. Writes atomically via a tmp
    file in the same directory.
    """
    with open(existing_path) as f:
        base = json.load(f)
    with open(extension_path) as f:
        ext = json.load(f)

    base_gs = base.get("gossipsub") or []
    base_sp = base.get("spread") or []
    ext_gs  = ext.get("gossipsub")  or []
    ext_sp  = ext.get("spread")     or []

    merged_gs = base_gs + ext_gs
    merged_sp = base_sp + ext_sp

    base["gossipsub"] = merged_gs
    base["spread"]    = merged_sp
    base["trials"]    = len(merged_gs)

    gs_acc = recompute_attacker_accuracy(merged_gs)
    sp_acc = recompute_attacker_accuracy(merged_sp)
    if gs_acc is not None:
        base["gossipsub_attacker_accuracy"] = gs_acc
    if sp_acc is not None:
        base["spread_attacker_accuracy"]    = sp_acc

    # Carry forward the extension's generated_at to reflect the latest touch.
    if "generated_at" in ext:
        base["generated_at"] = ext["generated_at"]

    tmp = Path(str(existing_path) + ".merge.tmp")
    with open(tmp, "w") as f:
        json.dump(base, f, indent=2)
        f.write("\n")
    os.replace(tmp, existing_path)


def prebuild_test_binary(repo_dir, out_path, log_path):
    """Compile the simnet test binary once so every sweep run reuses it.

    Returns the absolute path to the binary on success. Raises on failure.
    """
    out_path = Path(out_path).resolve()
    cmd = [
        "go", "test",
        "-tags", "simnet",
        "-run", "^TestSimnetSpreadVsGossipsubLatencyStretch$",
        "-c",
        "-o", str(out_path),
        ".",
    ]
    t0 = time.time()
    with open(log_path, "w") as lf:
        proc = subprocess.run(
            cmd, cwd=repo_dir,
            stdout=lf, stderr=subprocess.STDOUT,
        )
    elapsed = time.time() - t0
    if proc.returncode != 0 or not out_path.is_file():
        raise RuntimeError(
            f"prebuild failed (rc={proc.returncode}, {elapsed:.1f}s). "
            f"See {log_path}"
        )
    print(f"  prebuilt test binary in {elapsed:.1f}s → {out_path}")
    return out_path


def run_one(repo_dir, env, export_path, log_path, timeout_sec, test_binary):
    """Invoke the prebuilt test binary once. Returns (success, duration_s, returncode)."""
    cmd = [
        str(test_binary),
        "-test.run", "^TestSimnetSpreadVsGossipsubLatencyStretch$",
        "-test.v",
        "-test.timeout", f"{timeout_sec}s",
    ]
    env_with_export = dict(env)
    env_with_export["SPREAD_SIMNET_EXPORT_PATH"] = str(export_path)

    t0 = time.time()
    with open(log_path, "w") as lf:
        try:
            proc = subprocess.run(
                cmd, cwd=repo_dir, env=env_with_export,
                stdout=lf, stderr=subprocess.STDOUT,
                timeout=timeout_sec + 60,
            )
            rc = proc.returncode
        except subprocess.TimeoutExpired:
            rc = -1
    elapsed = time.time() - t0
    success = (rc == 0) and Path(export_path).is_file()
    return success, elapsed, rc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to sweep config YAML")
    parser.add_argument("--out-dir", required=True, help="Output directory")
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--timeout-sec", type=int, default=1500)
    parser.add_argument("--extend", action="store_true",
                        help="For each (config, topology) whose topo-<seed>.json already "
                             "exists, run SPREAD_SIMNET_TRIALS more trials starting from "
                             "where it left off and merge the results. Missing topo files "
                             "are run fresh.")
    args = parser.parse_args()

    repo_dir = Path(__file__).resolve().parent.parent  # go-libp2p-pubsub root
    config_path = Path(args.config).resolve()
    out_dir = Path(args.out_dir).resolve()

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    topologies = cfg.get("topologies") or [1337]
    base_env = cfg.get("env") or {}
    groups = cfg.get("groups") or {}

    num_nodes = int(base_env.get("SPREAD_SIMNET_NODES", 30))
    trials = int(base_env.get("SPREAD_SIMNET_TRIALS", num_nodes))

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "runs").mkdir(exist_ok=True)

    # Pre-compile the test binary once. Every run reuses it, avoiding the Go
    # build pipeline's per-invocation overhead.
    test_binary = prebuild_test_binary(
        repo_dir,
        out_dir / ".sweep_test_binary",
        out_dir / "prebuild.log",
    )

    # Copy the config to the output dir for reference
    with open(out_dir / "sweep_config.yaml", "w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)

    results_log = out_dir / "results.jsonl"

    # Build work list: each item = (group_name, (p_i,f_i,p_e,f_e), topo_seed)
    # Iterate by topology first so a partial run produces balanced data.
    work = []
    config_to_groups = {}  # tag -> set of groups it belongs to
    for group_name, cfg_list in groups.items():
        for spread_cfg in cfg_list:
            p_i, f_i, p_e, f_e = spread_cfg
            tag = config_tag(p_i, f_i, p_e, f_e)
            config_to_groups.setdefault(tag, set()).add(group_name)

    unique_configs = {}
    for group_name, cfg_list in groups.items():
        for spread_cfg in cfg_list:
            p_i, f_i, p_e, f_e = spread_cfg
            tag = config_tag(p_i, f_i, p_e, f_e)
            unique_configs[tag] = (float(p_i), int(f_i), float(p_e), int(f_e))

    for topo_seed in topologies:
        for tag, (p_i, f_i, p_e, f_e) in unique_configs.items():
            work.append((tag, p_i, f_i, p_e, f_e, topo_seed))

    total = len(work)
    print(f"╔══════════════════════════════════════════════════════════════╗")
    print(f"║  Sweep runner")
    print(f"║  Unique configs: {len(unique_configs)}")
    print(f"║  Topologies:     {len(topologies)}")
    print(f"║  Total runs:     {total}   (trials/run={trials}, nodes={num_nodes})")
    print(f"║  Out dir:        {out_dir}")
    print(f"╚══════════════════════════════════════════════════════════════╝")

    n_skipped = 0
    n_run = 0
    n_extended = 0
    n_failed = 0

    t_start = time.time()
    for i, (tag, p_i, f_i, p_e, f_e, topo_seed) in enumerate(work, start=1):
        cfg_dir = out_dir / "runs" / tag
        cfg_dir.mkdir(parents=True, exist_ok=True)
        export_path = cfg_dir / f"topo-{topo_seed}.json"
        meta_path = cfg_dir / f"topo-{topo_seed}.meta.json"

        existing_ok = False
        start_trial = 0
        if export_path.is_file():
            try:
                with open(export_path) as f:
                    json.load(f)
                existing_ok = True
                start_trial = existing_trial_count(export_path)
            except Exception:
                print(f"[{i}/{total}] re-run {tag} topo={topo_seed} (corrupted)")

        # Decide action based on existing state + --extend flag.
        is_extend = False
        if existing_ok and not args.extend:
            n_skipped += 1
            print(f"[{i}/{total}] SKIP {tag} topo={topo_seed}")
            continue
        if existing_ok and args.extend:
            is_extend = True
            print(f"[{i}/{total}] extend {tag} topo={topo_seed} "
                  f"(start_trial={start_trial}, +{trials} trials)")
        else:
            print(f"[{i}/{total}] run  {tag} topo={topo_seed}")

        # Build env — per-run overrides win over base_env.
        spread_env = {
            "SPREAD_SIMNET_SEED":       topo_seed,
            "SPREAD_INTRA_RHO":         p_i,
            "SPREAD_INTRA_FANOUT":      f_i,
            "SPREAD_INTER_PROB":        p_e,
            "SPREAD_INTER_FANOUT":      f_e,
            "SPREAD_SIMNET_RUN_ID":     f"{tag}_topo-{topo_seed}",
            "SPREAD_SIMNET_START_TRIAL": start_trial,
        }
        env = build_env(base_env, spread_env)

        # Fresh runs write directly to export_path. Extensions write to a temp
        # path and then merge into the existing export_path.
        if is_extend:
            target_path = cfg_dir / f"topo-{topo_seed}.extend.tmp.json"
            if target_path.exists():
                try: target_path.unlink()
                except: pass
        else:
            target_path = export_path

        log_path = cfg_dir / f"topo-{topo_seed}.log"

        success = False
        last_rc = None
        last_elapsed = 0.0
        for attempt in range(args.max_retries + 1):
            success, elapsed, rc = run_one(
                repo_dir, env, target_path,
                cfg_dir / f"topo-{topo_seed}.attempt-{attempt}.log",
                args.timeout_sec,
                test_binary,
            )
            last_rc = rc
            last_elapsed = elapsed
            if success:
                break
            print(f"   ⚠️  attempt {attempt+1} failed (rc={rc}, {elapsed:.0f}s)")
            # If the output file got partially written, delete it
            if target_path.is_file() and not success:
                try: target_path.unlink()
                except: pass

        # On extend success: merge the extension JSON into the existing export.
        if success and is_extend:
            try:
                merge_extension(export_path, target_path)
                target_path.unlink()
            except Exception as e:
                print(f"   ⚠️  merge failed: {e}")
                success = False

        # Write meta + log result
        meta = {
            "tag":            tag,
            "topology_seed":  topo_seed,
            "groups":         sorted(config_to_groups.get(tag, set())),
            "p_i":            p_i,
            "f_i":            f_i,
            "p_e":            p_e,
            "f_e":            f_e,
            "nodes":          num_nodes,
            "trials":         trials,
            "start_trial":    start_trial,
            "extended":       is_extend,
            "success":        success,
            "duration_s":     last_elapsed,
            "returncode":     last_rc,
            "timestamp":      time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        with open(results_log, "a") as f:
            f.write(json.dumps(meta) + "\n")

        if success:
            if is_extend:
                n_extended += 1
            else:
                n_run += 1
        else:
            n_failed += 1

    elapsed_total = time.time() - t_start
    print()
    print(f"✅ Done in {elapsed_total/60:.1f} min")
    print(f"   skipped={n_skipped}  ran={n_run}  extended={n_extended}  failed={n_failed}")
    print()
    print(f"Charts:")
    print(f"   python3 simnet_spread_tests/sweep_plotter.py --data-dir '{out_dir}' --groups <plot_groups.yaml>")


if __name__ == "__main__":
    main()
