#!/usr/bin/env python3
"""
One-time layout migration — old sweep dirs -> new structured layout.

Run this ONCE per old sweep output directory after pulling the refactor that
split gossipsub and spread into their own subtrees. Safe to rerun; it skips
anything already migrated.

Old layout:
  <sweep>/runs/<tag>/topo-<seed>.json        # both gossipsub + spread
  <sweep>/runs/<tag>/topo-<seed>.meta.json

New layout:
  <sweep>/gossipsub/<d-subdir>/topo-<seed>.json       # only gossipsub
  <sweep>/gossipsub/<d-subdir>/topo-<seed>.meta.json
  <sweep>/spread/runs/<tag>/topo-<seed>.json          # only spread
  <sweep>/spread/runs/<tag>/topo-<seed>.meta.json

Because the old data doesn't record which gossipsub D params it was run with,
you must declare them at migration time via `--gossipsub-d D,D_low,D_high`.

Usage:
    python3 simnet_spread_tests/one_time_migrate_layout.py \
        --data-dir simnet_spread_tests/outputs/sweep_B_case \
        --gossipsub-d 6,5,12

After migration, the original `runs/` dir is moved to `runs.legacy-backup/`.
Delete it once you're satisfied the new layout works.
"""

import argparse
import glob
import json
import os
import re
import shutil
import sys
from pathlib import Path


_LEGACY_TAG_RE = re.compile(r"^ir([\d.]+)_ip([\d.]+)_if(\d+)_ef(\d+)$")
_NEW_TAG_RE    = re.compile(r"^ap([\d.]+)_af(\d+)_ep([\d.]+)_ef(\d+)$")
_TOPO_RE       = re.compile(r"topo-(-?\d+)\.json$")


def legacy_to_new_tag(name):
    """Return the new-format tag for a legacy-format directory name, or the
    name unchanged if it's already in the new format or unrecognized."""
    m = _LEGACY_TAG_RE.match(name)
    if m:
        p_i, p_e, f_i, f_e = m.group(1), m.group(2), m.group(3), m.group(4)
        return f"ap{p_i}_af{f_i}_ep{p_e}_ef{f_e}"
    if _NEW_TAG_RE.match(name):
        return name
    return None


def parse_d_params(raw):
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            f"--gossipsub-d expects 'D,D_low,D_high' (got {raw!r})")
    try:
        D, Dl, Dh = (int(p) for p in parts)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--gossipsub-d values must be integers (got {raw!r})")
    return D, Dl, Dh


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", required=True,
                    help="Sweep output directory to migrate in place.")
    ap.add_argument("--gossipsub-d", required=True, type=parse_d_params,
                    help="Gossipsub D params the old data was produced with, "
                         "comma-separated: D,D_low,D_high. Determines the target "
                         "subdir gossipsub/d<D>_dl<D_low>_dh<D_high>/.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Log what would happen without touching the filesystem.")
    args = ap.parse_args()

    data_dir = Path(args.data_dir).resolve()
    legacy_runs = data_dir / "runs"
    if not legacy_runs.is_dir():
        print(f"nothing to migrate — {legacy_runs} does not exist.", file=sys.stderr)
        sys.exit(0)

    D, Dl, Dh = args.gossipsub_d
    gs_subdir = f"d{D}_dl{Dl}_dh{Dh}"
    gs_dir    = data_dir / "gossipsub" / gs_subdir
    sp_root   = data_dir / "spread" / "runs"

    print(f"data-dir:       {data_dir}")
    print(f"gossipsub subdir: {gs_subdir}")
    print(f"dry-run:        {args.dry_run}")
    print()

    stats = {"spread_written": 0, "spread_skipped": 0,
             "gossipsub_written": 0, "gossipsub_skipped": 0,
             "tag_renamed": 0, "untouched_dirs": 0}

    for cfg_dir in sorted(legacy_runs.iterdir()):
        if not cfg_dir.is_dir():
            continue
        new_tag = legacy_to_new_tag(cfg_dir.name)
        if new_tag is None:
            print(f"  ?? unrecognized dir name, leaving alone: {cfg_dir.name}")
            stats["untouched_dirs"] += 1
            continue
        if new_tag != cfg_dir.name:
            stats["tag_renamed"] += 1

        sp_tag_dir = sp_root / new_tag

        for path in sorted(glob.glob(str(cfg_dir / "topo-*.json"))):
            if ".meta." in path:
                continue
            m = _TOPO_RE.search(path)
            if not m:
                continue
            seed = int(m.group(1))

            try:
                with open(path) as f:
                    doc = json.load(f)
            except Exception as e:
                print(f"  !! skip (cannot parse): {path}: {e}")
                continue

            gs_data = doc.get("gossipsub")
            sp_data = doc.get("spread")

            # Build gossipsub-only view (if gossipsub data exists)
            if gs_data:
                gs_target = gs_dir / f"topo-{seed}.json"
                if gs_target.exists():
                    stats["gossipsub_skipped"] += 1
                else:
                    gs_doc = dict(doc)
                    gs_doc["spread"] = None
                    gs_doc["spread_attacker_accuracy"] = None
                    if not args.dry_run:
                        gs_target.parent.mkdir(parents=True, exist_ok=True)
                        with open(gs_target, "w") as f:
                            json.dump(gs_doc, f, indent=2)
                            f.write("\n")
                    print(f"  gs  <- {cfg_dir.name}/topo-{seed}.json  ->  gossipsub/{gs_subdir}/topo-{seed}.json")
                    stats["gossipsub_written"] += 1

            # Build spread-only view (always, even if sp_data is null — preserves
            # the record that this config was run, so the plotter still sees it)
            sp_target = sp_tag_dir / f"topo-{seed}.json"
            if sp_target.exists():
                stats["spread_skipped"] += 1
            else:
                sp_doc = dict(doc)
                sp_doc["gossipsub"] = None
                sp_doc["gossipsub_attacker_accuracy"] = None
                if not args.dry_run:
                    sp_target.parent.mkdir(parents=True, exist_ok=True)
                    with open(sp_target, "w") as f:
                        json.dump(sp_doc, f, indent=2)
                        f.write("\n")
                print(f"  sp  <- {cfg_dir.name}/topo-{seed}.json  ->  spread/runs/{new_tag}/topo-{seed}.json")
                stats["spread_written"] += 1

            # Copy the sibling .meta.json alongside the spread export
            meta_src = path.replace(".json", ".meta.json")
            meta_dst = sp_target.parent / f"topo-{seed}.meta.json"
            if os.path.isfile(meta_src) and not meta_dst.exists() and not args.dry_run:
                shutil.copy2(meta_src, meta_dst)

    if not args.dry_run:
        # Move the legacy runs/ aside so the plotter picks up the new layout
        # (its legacy-layout fallback only kicks in when <data_dir>/spread/runs/
        # is absent).
        backup = data_dir / "runs.legacy-backup"
        if backup.exists():
            print(f"\n  !! backup dir already exists, leaving runs/ in place: {backup}")
        else:
            legacy_runs.rename(backup)
            print(f"\n  moved legacy dir: runs/  ->  runs.legacy-backup/")

    print()
    print("Migration summary:")
    for k, v in stats.items():
        print(f"  {k:>20}: {v}")
    if args.dry_run:
        print("\n(dry-run — no files were actually written or renamed.)")


if __name__ == "__main__":
    main()
