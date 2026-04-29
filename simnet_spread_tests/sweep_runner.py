#!/usr/bin/env python3
"""
Sweep runner — execute a parameter sweep over multiple network topologies.

Config (YAML) shape:
  extend: bool                  # extend existing outputs instead of skipping
  topologies: [int, ...]        # list of simnet seeds
  simnet:                       # global simulation knobs (apply to both scenarios)
    nodes, trials, link_mibps, scenario_timeout_ms,
    enable_crash, crash_pct, attacker_pcts: [float, ...]
  gossipsub:                    # GossipSub baseline — one file per seed
    enabled: bool
    D, D_low, D_high: int
  spread:                       # SPREAD sweep — per (group config × seed)
    enabled: bool
    warmup_every, warmup_rounds_per_publish: int
    groups:
      <group_name>:
        - [p_i, f_i, p_e, f_e]
        ...

Output layout (under --out-dir):
  sweep_config.yaml                              (copy of input)
  prebuild.log  .sweep_test_binary
  results.jsonl                                  (append-only run log)
  gossipsub/
    d<D>_dl<D_low>_dh<D_high>/
      topo-<seed>.json            — only gossipsub data
      topo-<seed>.meta.json
  spread/
    runs/
      ap<p_i>_af<f_i>_ep<p_e>_ef<f_e>/
        topo-<seed>.json          — only spread data
        topo-<seed>.meta.json

Resume semantics:
  extend=false → skip any (scenario, …) whose export file already exists.
  extend=true  → for each existing file, run `trials` more trials starting from
                 where it left off and merge. Missing files are run fresh.
                 Continuation is deterministic: trial N's source + RNG are a
                 pure function of (seed, N).

Usage (from repo root):
    python3 simnet_spread_tests/sweep_runner.py \
        --config simnet_spread_tests/sweep_config.yaml \
        --out-dir simnet_spread_tests/outputs/sweep_$(date +%Y%m%d_%H%M%S)
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import yaml


def config_tag(p_i, f_i, p_e, f_e):
    """Canonical tag.

    Layout: intra block first (ap = intra probability, af = intra fanout),
    then inter block (ep = inter probability, ef = inter fanout).
    """
    return f"ap{p_i}_af{f_i}_ep{p_e}_ef{f_e}"


# Legacy tag format (kept for one-way backward compat): ir<p_i>_ip<p_e>_if<f_i>_ef<f_e>.
# Different prefix *and* different field order (interleaved vs. grouped).
_LEGACY_TAG_RE = re.compile(r"^ir([\d.]+)_ip([\d.]+)_if(\d+)_ef(\d+)$")


def migrate_legacy_run_dirs(runs_dir):
    """Rename any legacy-format config dirs under runs_dir to the new scheme.

    Safe to call on any directory: if no legacy dirs exist, it's a no-op.
    If a new-format dir already exists alongside an old one for the same
    config (shouldn't happen in practice, but be paranoid), the legacy dir is
    left alone and a warning is printed — no destructive merge.
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
            print(f"  legacy-migrate: SKIP {entry.name} -> {new_name} (target exists)")
            continue
        entry.rename(target)
        migrated += 1
    if migrated:
        print(f"  legacy-migrate: renamed {migrated} dir(s) "
              f"ir*_ip*_if*_ef* -> ap*_af*_ep*_ef*")


def build_env(base_env, extra):
    env = os.environ.copy()
    env.update({k: str(v).lower() if isinstance(v, bool) else str(v)
                for k, v in base_env.items()})
    env.update({k: str(v).lower() if isinstance(v, bool) else str(v)
                for k, v in extra.items()})
    return env


# ---- Structured config → env mapping --------------------------------------

_ALLOWED_TOP_KEYS       = {"extend", "topologies", "simnet", "gossipsub", "spread", "dandelion"}
_ALLOWED_SIMNET_KEYS    = {
    "nodes", "trials", "link_mibps", "scenario_timeout_ms",
    "enable_crash", "crash_pct", "attacker_pcts",
}
_ALLOWED_GOSSIPSUB_KEYS  = {"enabled", "D", "D_low", "D_high"}
_ALLOWED_SPREAD_KEYS     = {"enabled", "warmup_every", "warmup_rounds_per_publish",
                             "groups", "env"}
_ALLOWED_DANDELION_KEYS  = {"enabled", "fanout", "prob"}


def validate_config(cfg):
    """Raise ValueError on missing/unknown keys so typos surface early."""
    if not isinstance(cfg, dict):
        raise ValueError("sweep config must be a mapping at the top level")
    unknown = set(cfg) - _ALLOWED_TOP_KEYS
    if unknown:
        raise ValueError(f"unknown top-level keys: {sorted(unknown)}")
    if "topologies" not in cfg or not cfg["topologies"]:
        raise ValueError("`topologies` is required and must be non-empty")
    if "simnet" not in cfg:
        raise ValueError("`simnet:` section is required")
    unknown = set(cfg["simnet"]) - _ALLOWED_SIMNET_KEYS
    if unknown:
        raise ValueError(f"unknown simnet keys: {sorted(unknown)}")
    for required in ("nodes", "trials"):
        if required not in cfg["simnet"]:
            raise ValueError(f"`simnet.{required}` is required")
    gs = cfg.get("gossipsub") or {}
    sp = cfg.get("spread") or {}
    dn = cfg.get("dandelion") or {}
    if not gs.get("enabled") and not sp.get("enabled") and not dn.get("enabled"):
        raise ValueError("at least one of gossipsub.enabled / spread.enabled / dandelion.enabled must be true")
    if gs:
        unknown = set(gs) - _ALLOWED_GOSSIPSUB_KEYS
        if unknown:
            raise ValueError(f"unknown gossipsub keys: {sorted(unknown)}")
    if sp:
        unknown = set(sp) - _ALLOWED_SPREAD_KEYS
        if unknown:
            raise ValueError(f"unknown spread keys: {sorted(unknown)}")
    if dn:
        unknown = set(dn) - _ALLOWED_DANDELION_KEYS
        if unknown:
            raise ValueError(f"unknown dandelion keys: {sorted(unknown)}")


def simnet_env(simnet_cfg):
    """Convert the simnet section into the pass-through env vars the Go test reads."""
    e = {
        "SPREAD_SIMNET_NODES":              simnet_cfg["nodes"],
        "SPREAD_SIMNET_TRIALS":             simnet_cfg["trials"],
    }
    if "link_mibps" in simnet_cfg:
        e["SPREAD_SIMNET_LINK_MIBPS"] = simnet_cfg["link_mibps"]
    if "scenario_timeout_ms" in simnet_cfg:
        e["SPREAD_SIMNET_SCENARIO_TIMEOUT_MS"] = simnet_cfg["scenario_timeout_ms"]
    if "enable_crash" in simnet_cfg:
        e["SPREAD_SIMNET_ENABLE_CRASH"] = simnet_cfg["enable_crash"]
    if "crash_pct" in simnet_cfg:
        e["SPREAD_SIMNET_CRASH_PCT"] = simnet_cfg["crash_pct"]
    pcts = simnet_cfg.get("attacker_pcts")
    if pcts:
        e["SPREAD_ATTACKER_PCTS"] = ",".join(str(x) for x in pcts)
    return e


def gossipsub_subdir(gs_cfg):
    """Return `d<D>_dl<D_low>_dh<D_high>`, using Go-side defaults (6/5/12) when absent."""
    D      = gs_cfg.get("D", 6)
    D_low  = gs_cfg.get("D_low", 5)
    D_high = gs_cfg.get("D_high", 12)
    return f"d{D}_dl{D_low}_dh{D_high}"


def gossipsub_env(gs_cfg):
    """Env vars for the Go-side gossipsub D params. Only emitted when explicitly set."""
    e = {}
    if "D" in gs_cfg:
        e["GOSSIPSUB_D"] = gs_cfg["D"]
    if "D_low" in gs_cfg:
        e["GOSSIPSUB_D_LOW"] = gs_cfg["D_low"]
    if "D_high" in gs_cfg:
        e["GOSSIPSUB_D_HIGH"] = gs_cfg["D_high"]
    return e


def dandelion_subdir(dn_cfg):
    """Return `fanout<F>_prob<P>`, using Go-side defaults (6/0.8) when absent."""
    fanout = dn_cfg.get("fanout", 6)
    prob   = dn_cfg.get("prob", 0.8)
    return f"fanout{fanout}_prob{prob}"


def dandelion_env(dn_cfg):
    """Env vars for Dandelion params. Only emitted when explicitly set."""
    e = {}
    if "fanout" in dn_cfg:
        e["DANDELION_FANOUT"] = dn_cfg["fanout"]
    if "prob" in dn_cfg:
        e["DANDELION_PROB"] = dn_cfg["prob"]
    return e


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
        dn = doc.get("dandelion") or []
        # Collect only the arrays that were actually run (non-empty).
        # Each run directory only stores the protocol(s) it ran, so a spread-only
        # file has gs=[] and dn=[], and that is expected — not a mismatch.
        non_empty = {k: v for k, v in [("gossipsub", gs), ("spread", sp), ("dandelion", dn)] if v}
        if not non_empty:
            return 0
        counts = [len(v) for v in non_empty.values()]
        if len(set(counts)) > 1:
            names = ", ".join(f"{k}={len(v)}" for k, v in non_empty.items())
            print(f"   ⚠️  mismatch in {export_path}: {names}; using min for start_trial")
            return min(counts)
        return counts[0]
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
    base_dn = base.get("dandelion") or []
    ext_gs  = ext.get("gossipsub")  or []
    ext_sp  = ext.get("spread")     or []
    ext_dn  = ext.get("dandelion")  or []

    merged_gs = base_gs + ext_gs
    merged_sp = base_sp + ext_sp
    merged_dn = base_dn + ext_dn

    base["gossipsub"] = merged_gs
    base["spread"]    = merged_sp
    base["dandelion"] = merged_dn
    # trials reflects the longest non-empty scenario run
    base["trials"] = max(len(merged_gs), len(merged_sp), len(merged_dn))

    gs_acc = recompute_attacker_accuracy(merged_gs)
    sp_acc = recompute_attacker_accuracy(merged_sp)
    dn_acc = recompute_attacker_accuracy(merged_dn)
    if gs_acc is not None:
        base["gossipsub_attacker_accuracy"]  = gs_acc
    if sp_acc is not None:
        base["spread_attacker_accuracy"]     = sp_acc
    if dn_acc is not None:
        base["dandelion_attacker_accuracy"]  = dn_acc

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
    timed_out = False
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
            timed_out = True
    elapsed = time.time() - t0

    # A run is successful when the export file exists AND parses as valid JSON.
    # On non-zero return codes (including timeout) we still check the file:
    # the Go test writes the export before teardown, and teardown can hang
    # on network.Close (simnet wait group). In that case the export is complete
    # even though the process gets killed — salvage it rather than discarding.
    export_valid = False
    if Path(export_path).is_file():
        try:
            with open(export_path) as f:
                json.load(f)
            export_valid = True
        except Exception:
            export_valid = False

    success = export_valid and (rc == 0 or timed_out)
    return success, elapsed, rc


def dispatch_one(*, label, repo_dir, target_dir, base_name,
                 extra_env, base_env, test_binary, trials,
                 extend, max_retries, timeout_sec,
                 results_log, meta_extras, counters):
    """Run (or extend) one scenario invocation into `target_dir/base_name.json`.

    Handles: existence check → start_trial computation → run/retry → merge (on
    extend) → meta.json write → results.jsonl append → counter update.
    Returns None; mutates `counters` (dict with keys skipped/ran/extended/failed).
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    export_path = target_dir / f"{base_name}.json"
    meta_path   = target_dir / f"{base_name}.meta.json"

    existing_ok = False
    start_trial = 0
    if export_path.is_file():
        try:
            with open(export_path) as f:
                json.load(f)
            existing_ok = True
            start_trial = existing_trial_count(export_path)
        except Exception:
            print(f"  [{label}] re-run (corrupted existing file)")

    is_extend = False
    if existing_ok and not extend:
        print(f"  [{label}] SKIP (exists)")
        counters["skipped"] += 1
        return
    if existing_ok and extend:
        is_extend = True
        print(f"  [{label}] extend (start_trial={start_trial}, +{trials} trials)")
    else:
        print(f"  [{label}] run")

    # Fresh runs write straight to the final path; extends go to a temp file
    # then merge into the existing export.
    if is_extend:
        target_path = target_dir / f"{base_name}.extend.tmp.json"
        if target_path.exists():
            try: target_path.unlink()
            except: pass
    else:
        target_path = export_path

    env = build_env(base_env, {
        **extra_env,
        "SPREAD_SIMNET_START_TRIAL": start_trial,
    })

    success = False
    last_rc = None
    last_elapsed = 0.0
    for attempt in range(max_retries + 1):
        success, elapsed, rc = run_one(
            repo_dir, env, target_path,
            target_dir / f"{base_name}.attempt-{attempt}.log",
            timeout_sec,
            test_binary,
        )
        last_rc = rc
        last_elapsed = elapsed
        if success:
            if rc != 0:
                print(f"     ℹ️  salvaged valid export despite rc={rc} "
                      f"(likely teardown hang)")
            break
        print(f"     ⚠️  attempt {attempt+1} failed (rc={rc}, {elapsed:.0f}s)")
        if target_path.is_file() and not success:
            try: target_path.unlink()
            except: pass

    if success and is_extend:
        try:
            merge_extension(export_path, target_path)
            target_path.unlink()
        except Exception as e:
            print(f"     ⚠️  merge failed: {e}")
            success = False

    meta = {
        **meta_extras,
        "trials":       trials,
        "start_trial":  start_trial,
        "extended":     is_extend,
        "success":      success,
        "duration_s":   last_elapsed,
        "returncode":   last_rc,
        "timestamp":    time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    with open(results_log, "a") as f:
        f.write(json.dumps(meta) + "\n")

    if success:
        counters["extended" if is_extend else "ran"] += 1
    else:
        counters["failed"] += 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to sweep config YAML")
    parser.add_argument("--out-dir", required=True, help="Output directory")
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--timeout-sec", type=int, default=1500)
    args = parser.parse_args()

    repo_dir    = Path(__file__).resolve().parent.parent  # go-libp2p-pubsub root
    config_path = Path(args.config).resolve()
    out_dir     = Path(args.out_dir).resolve()

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    try:
        validate_config(cfg)
    except ValueError as e:
        print(f"config error: {e}", file=sys.stderr)
        sys.exit(2)

    topologies   = list(cfg["topologies"])
    simnet_cfg   = cfg["simnet"]
    gs_cfg       = cfg.get("gossipsub") or {}
    sp_cfg       = cfg.get("spread") or {}
    dn_cfg       = cfg.get("dandelion") or {}
    extend       = bool(cfg.get("extend", False))
    trials       = int(simnet_cfg["trials"])
    num_nodes    = int(simnet_cfg["nodes"])

    gs_enabled = bool(gs_cfg.get("enabled"))
    sp_enabled = bool(sp_cfg.get("enabled"))
    dn_enabled = bool(dn_cfg.get("enabled"))

    out_dir.mkdir(parents=True, exist_ok=True)

    test_binary = prebuild_test_binary(
        repo_dir,
        out_dir / ".sweep_test_binary",
        out_dir / "prebuild.log",
    )

    # Snapshot the config for later reference / debugging.
    with open(out_dir / "sweep_config.yaml", "w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)

    results_log = out_dir / "results.jsonl"
    base_env = simnet_env(simnet_cfg)

    counters = {"skipped": 0, "ran": 0, "extended": 0, "failed": 0}
    t_start = time.time()

    # ---- Gossipsub: one invocation per seed, fast, independent of spread ----
    if gs_enabled:
        gs_subdir = gossipsub_subdir(gs_cfg)
        gs_dir    = out_dir / "gossipsub" / gs_subdir
        gs_env    = gossipsub_env(gs_cfg)
        print(f"╔══════════════════════════════════════════════════════════════╗")
        print(f"║  Gossipsub baseline  ({gs_subdir})")
        print(f"║  Topologies: {len(topologies)}   trials/run={trials}  nodes={num_nodes}")
        print(f"║  Out: {gs_dir}")
        print(f"╚══════════════════════════════════════════════════════════════╝")
        for i, topo_seed in enumerate(topologies, start=1):
            extra = {
                **gs_env,
                "SPREAD_SIMNET_SEED":          topo_seed,
                "SPREAD_SIMNET_SKIP_SCENARIO": "spread,dandelion",
                "SPREAD_SIMNET_RUN_ID":        f"gs_{gs_subdir}_topo-{topo_seed}",
            }
            dispatch_one(
                label=f"gs {i}/{len(topologies)} seed={topo_seed}",
                repo_dir=repo_dir, target_dir=gs_dir, base_name=f"topo-{topo_seed}",
                extra_env=extra, base_env=base_env, test_binary=test_binary,
                trials=trials, extend=extend,
                max_retries=args.max_retries, timeout_sec=args.timeout_sec,
                results_log=results_log,
                meta_extras={
                    "kind":           "gossipsub",
                    "topology_seed":  topo_seed,
                    "nodes":          num_nodes,
                    "gossipsub_d":    gs_cfg.get("D"),
                    "gossipsub_d_low":  gs_cfg.get("D_low"),
                    "gossipsub_d_high": gs_cfg.get("D_high"),
                },
                counters=counters,
            )

    # ---- Spread: per (group config × seed) ----------------------------------
    if sp_enabled:
        groups = sp_cfg.get("groups") or {}
        if not groups:
            print("spread.enabled=true but spread.groups is empty — skipping spread phase.",
                  file=sys.stderr)
        else:
            # Resolve unique tags across all groups, plus which groups each tag belongs to.
            config_to_groups = {}
            unique_configs   = {}
            for group_name, cfg_list in groups.items():
                for scfg in cfg_list:
                    p_i, f_i, p_e, f_e = scfg
                    tag = config_tag(p_i, f_i, p_e, f_e)
                    config_to_groups.setdefault(tag, set()).add(group_name)
                    unique_configs[tag] = (float(p_i), int(f_i), float(p_e), int(f_e))

            total = len(unique_configs) * len(topologies)
            spread_root = out_dir / "spread" / "runs"
            print(f"╔══════════════════════════════════════════════════════════════╗")
            print(f"║  Spread sweep")
            print(f"║  Unique configs: {len(unique_configs)}   topologies: {len(topologies)}")
            print(f"║  Total runs:     {total}   trials/run={trials}  nodes={num_nodes}")
            print(f"║  Out: {spread_root}")
            print(f"╚══════════════════════════════════════════════════════════════╝")

            warmup_every   = sp_cfg.get("warmup_every", 0)
            warmup_rounds  = sp_cfg.get("warmup_rounds_per_publish", 1)
            sp_extra_env   = sp_cfg.get("env") or {}
            i = 0
            for topo_seed in topologies:
                for tag, (p_i, f_i, p_e, f_e) in unique_configs.items():
                    i += 1
                    target_dir = spread_root / tag
                    extra = {
                        **sp_extra_env,
                        "SPREAD_SIMNET_SEED":          topo_seed,
                        "SPREAD_SIMNET_SKIP_SCENARIO": "gossipsub,dandelion",
                        "SPREAD_INTRA_RHO":            p_i,
                        "SPREAD_INTRA_FANOUT":         f_i,
                        "SPREAD_INTER_PROB":           p_e,
                        "SPREAD_INTER_FANOUT":         f_e,
                        "SPREAD_SIMNET_RUN_ID":        f"sp_{tag}_topo-{topo_seed}",
                        "SPREAD_SIMNET_WARMUP_EVERY":  warmup_every,
                        "SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH": warmup_rounds,
                    }
                    dispatch_one(
                        label=f"sp {i}/{total} {tag} seed={topo_seed}",
                        repo_dir=repo_dir, target_dir=target_dir,
                        base_name=f"topo-{topo_seed}",
                        extra_env=extra, base_env=base_env, test_binary=test_binary,
                        trials=trials, extend=extend,
                        max_retries=args.max_retries, timeout_sec=args.timeout_sec,
                        results_log=results_log,
                        meta_extras={
                            "kind":           "spread",
                            "tag":            tag,
                            "topology_seed":  topo_seed,
                            "groups":         sorted(config_to_groups.get(tag, set())),
                            "p_i":            p_i,
                            "f_i":            f_i,
                            "p_e":            p_e,
                            "f_e":            f_e,
                            "nodes":          num_nodes,
                        },
                        counters=counters,
                    )

    # ---- Dandelion: one invocation per seed, similar to gossipsub ----
    if dn_enabled:
        dn_subdir = dandelion_subdir(dn_cfg)
        dn_dir    = out_dir / "dandelion" / dn_subdir
        dn_env    = dandelion_env(dn_cfg)
        print(f"╔══════════════════════════════════════════════════════════════╗")
        print(f"║  Dandelion baseline  ({dn_subdir})")
        print(f"║  Topologies: {len(topologies)}   trials/run={trials}  nodes={num_nodes}")
        print(f"║  Out: {dn_dir}")
        print(f"╚══════════════════════════════════════════════════════════════╝")
        for i, topo_seed in enumerate(topologies, start=1):
            extra = {
                **dn_env,
                "SPREAD_SIMNET_SEED":          topo_seed,
                "SPREAD_SIMNET_SKIP_SCENARIO": "gossipsub,spread",
                "SPREAD_SIMNET_RUN_ID":        f"dn_{dn_subdir}_topo-{topo_seed}",
            }
            dispatch_one(
                label=f"dn {i}/{len(topologies)} seed={topo_seed}",
                repo_dir=repo_dir, target_dir=dn_dir, base_name=f"topo-{topo_seed}",
                extra_env=extra, base_env=base_env, test_binary=test_binary,
                trials=trials, extend=extend,
                max_retries=args.max_retries, timeout_sec=args.timeout_sec,
                results_log=results_log,
                meta_extras={
                    "kind":             "dandelion",
                    "topology_seed":    topo_seed,
                    "nodes":            num_nodes,
                    "dandelion_fanout": dn_cfg.get("fanout"),
                    "dandelion_prob":   dn_cfg.get("prob"),
                },
                counters=counters,
            )

    elapsed_total = time.time() - t_start
    print()
    print(f"✅ Done in {elapsed_total/60:.1f} min")
    print(f"   skipped={counters['skipped']}  "
          f"ran={counters['ran']}  extended={counters['extended']}  "
          f"failed={counters['failed']}")
    print()
    print(f"Charts:")
    print(f"   python3 simnet_spread_tests/sweep_plotter.py --data-dir '{out_dir}' --groups <plot_groups.yaml>")


if __name__ == "__main__":
    main()
