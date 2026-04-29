# Simnet Spread Experiments

Two ways to run experiments:

1. **Batch runner** — one parameter set, N repeated runs. Good for convergence / noise tests.
2. **Sweep runner** — many parameter sets organized into groups, each run on multiple network topologies. Designed for the Anonymity vs Stretch scatter plot across the E=6 constraint surface. See [Parameter Sweep](#parameter-sweep-groups--topologies) below.

---

## Batch Runner

From repo root:

```bash
go run ./simnet_spread_tests/simnet_batch_runner.go
```

The runner executes:

```bash
go test -tags simnet -run TestSimnetSpreadVsGossipsubLatencyStretch -v .
```

Use CLI overrides if you need:

```bash
go run ./simnet_spread_tests/simnet_batch_runner.go --runs 20 --max-retries 3
```

## Config File (`config.yaml`)

Path: [simnet_spread_tests/config.yaml](./config.yaml)

Behavior:
- Source of defaults for batch + simnet + spread/vivaldi/clustering settings.
- Auto-generated on first run if missing.
- Missing required keys cause failure.
- Unknown keys cause failure.
- Precedence: CLI flags and `--env` overrides > `config.yaml` values.

Main sections:
- `batch`: runner behavior (`runs`, `out_dir`, `max_retries`, `per_run_timeout`, `resume`)
- `simnet`: `SPREAD_SIMNET_*` settings
- `spread`: `SPREAD_*` settings (SpreadConfig, VivaldiConfig, SpreadClusteringConfig)

## Outputs Directory

Default output root:

```text
simnet_spread_tests/outputs/
```

Structure:

```text
outputs/
  checkpoint.json        # resume/progress state
  results.jsonl          # append-only attempt log
  runs/
    run-000000.json      # per-run raw data export (on success)
  logs/
    run-000000-attempt-00.log
  plots/
    *.png
```

Each `run-NNNNNN.json` contains raw per-pair and first-receipt data per trial:

```json
{
  "nodes": 30,
  "seed": 1337,
  "node_ids": [3, 31, ...],
  "gossipsub": [
    {"trial": 0, "src": 31, "deliveries": [
      {"dst": 3, "latency_ms": 53.9, "stretch": 1.15}, ...
    ],
    "first_receipts": [
      {"node": 31, "first_from": 31, "received_delay_ms": 0.0},
      {"node": 3, "first_from": 31, "received_delay_ms": 53.9}, ...
    ],
    "attacker_estimates": [
      {
        "attacker_pct": 0.2,
        "attacker_count": 6,
        "estimated_source": 31,
        "true_source": 31,
        "is_correct": true,
        "observed_by_attacker": 12,
        "observed_delay_ms": 41.7
      }
    ]}, ...
  ],
  "spread": [ ... ],
  "gossipsub_attacker_accuracy": [
    {"attacker_pct": 0.2, "trials_observed": 50, "correct_estimates": 17, "accuracy": 0.34}
  ],
  "spread_attacker_accuracy": [ ... ]
}
```

Notes:
- Directory is created automatically if it does not exist.
- Resume uses `checkpoint.json` + existing `results.jsonl`.
- `received_delay_ms` is measured from the publish time (oracle latency for experimentation).
- Crash experiment: if enabled, a random `crash_pct` fraction of nodes is crashed after peering and before dissemination; trials run only among survivors.


## Plot Results

From repo root:

```bash
python3 simnet_spread_tests/plot_results.py
```

Optional flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--runs-dir PATH` | `simnet_spread_tests/outputs/runs` | Directory with per-run JSON exports |
| `--results PATH` | `simnet_spread_tests/outputs/results.jsonl` | Path to results.jsonl for finding successful runs |
| `--out DIR` | `simnet_spread_tests/outputs/plots` | Output directory for PNGs |
| `--group-by {n_seed,n}` | `n_seed` | Grouping for per-pair/per-topology stats |

Generated charts:

| File | Description |
|------|-------------|
| `01_per_pair_scatter_by_n.png` | Per-pair GossipSub vs Spread scatter, colored by N |
| `02_per_pair_scatter_by_seed.png` | Per-pair scatter, colored by seed (n_seed grouping only) |
| `03_per_topology_scatter_by_n.png` | Per-topology scatter, colored by N |
| `04_cdf_per_pair.png` | CDF of per-pair mean latency & stretch |
| `05_cdf_per_topology.png` | CDF of per-topology mean latency & stretch |
| `06_cdf_raw.png` | CDF of all raw observations |
| `07_improvement_heatmap.png` | % improvement heatmap (Spread vs GossipSub) |
| `08_distributions.png` | Latency & stretch histograms + delta distributions |

---

## Parameter Sweep (groups × topologies)

Designed for exploring the 4-parameter Spread space under the constant-fanout constraint
`(1 - p_i) + p_i·f_i + p_e·f_e = 6`, producing a scatter of Anonymity vs Stretch with configs grouped into semantic regions (fix Ce, fix Ci, fix fanouts, diagonal).

Pipeline:

```
generate_experiment_sets.py  →  make_sweep_config.py  →  sweep_config.yaml
                                                             │
                                        sweep_runner.py  ────┘   writes outputs/<sweep>/runs/
                                                             │
                                        sweep_plotter.py ────┘   reads outputs + plot_groups.yaml
```

### 1. Enumerate valid (p_i, f_i, p_e, f_e) sets

```bash
python3 simnet_spread_tests/generate_experiment_sets.py
```

Produces `p2p_experimental_sets_*.csv` — enumerates every (p_i, f_i, p_e, f_e) satisfying the E=6 constraint and groups them as **A/B/C/D** (fix Ce; fix Ci; fix fanouts; fanouts-vs-bernoullis diagonal).

The CSVs are convenient for manual inspection but **not required** by the rest of the pipeline — `make_sweep_config.py` calls `build_groups` directly.

### 2. Generate a sweep config

```bash
python3 simnet_spread_tests/make_sweep_config.py \
    --out simnet_spread_tests/sweep_config.yaml \
    --points-per-line 6 \
    --topologies 1337,1338,1339 \
    --nodes 30 \
    --trials 30
```

Subsamples the enumerated sets into 8 groups (3 × A subgroups, 3 × B subgroups, 1 × C, 1 × D) with ~5–6 configs each, and writes a ready-to-run YAML.

Config structure (new, post-refactor):

```yaml
extend: false                  # extend mode — see below
topologies: [1337, ...]

simnet:                        # global simulation knobs (apply to both scenarios)
  nodes: 30
  trials: 30
  link_mibps: 20
  scenario_timeout_ms: 600000
  enable_crash: false
  crash_pct: 0.0
  attacker_pcts: [0.1, 0.2, 0.3, 0.4, 0.5]

gossipsub:                     # GossipSub baseline — one run per seed
  enabled: true
  D: 6
  D_low: 5
  D_high: 12

spread:                        # SPREAD sweep — one run per (group config × seed)
  enabled: true
  warmup_every: 0
  warmup_rounds_per_publish: 1
  env:                         # pass-through SPREAD_* implementation knobs
    SPREAD_CLUSTER_PCT: 0.25
    ...
  groups:
    D_fanouts_vs_bernoullis:
      - [p_i, f_i, p_e, f_e]
      ...
```

Gossipsub output depends only on topology (not the spread knobs), so it's run once per seed under `gossipsub/<D-subdir>/`. Spread is run per (config, seed) under `spread/runs/<tag>/`. Setting either section's `enabled: false` skips that whole scenario family.

### 3. Run the sweep

```bash
python3 simnet_spread_tests/sweep_runner.py \
    --config simnet_spread_tests/sweep_config.yaml \
    --out-dir simnet_spread_tests/outputs/sweep_$(date +%Y%m%d_%H%M%S)
```

Resumable: kill at any time and rerun with the same `--out-dir` — every scenario run whose export already exists is skipped.

#### Extending an existing sweep

Flip `extend: true` at the top of `sweep_config.yaml` and re-run against the same `--out-dir`.

Behavior:
- For each existing export (gossipsub per-seed, spread per-config-seed), the runner reads its trial count, runs `simnet.trials` more trials starting from that offset, and merges the new trials into the same JSON. Trial indices stay continuous (`0..29` + `30..89`).
- For each missing export, the runner runs a fresh batch of `simnet.trials` trials.
- The start offset is computed automatically from the existing JSON. You never set `SPREAD_SIMNET_START_TRIAL` yourself.
- Continuation is **deterministic**: trial N's source (`alive[N % len(alive)]`) and RNG seed (`seed + 7919 + N`) are pure functions of `(seed, N)`, so extending `30 → 90` is byte-identical to running `90` from scratch.
- After each merge, the runner recomputes the per-pct attacker accuracy summaries from the combined trial list.

**Numerical example.** You ran with `simnet.trials: 30` and now want 60 more trials:

1. Bump `simnet.trials` to `60` and set `extend: true` in `sweep_config.yaml`.
2. Rerun with the **same** `--out-dir`.
3. Each existing export goes from 30 → 90 trials. Any config/seed you added gets a fresh 60-trial batch.

#### Skipping a scenario

Set `gossipsub.enabled: false` or `spread.enabled: false` to skip that scenario entirely. (Setting both to false is rejected.) Gossipsub is topology-only, so running it once per seed — not per spread config — is the default and saves significant compute compared to running it alongside every spread config.

#### Per-run speedups

Two optimizations are built in and need no configuration:

- The test binary is compiled **once** at the start of a sweep (`go test -c`) and the resulting binary is reused for every run, skipping Go's build pipeline per invocation.
- The Ethereum-like latency matrix is cached in-process by `(nodes, seed)`, so repeated calls to `makeEthereumLikeTopology` with the same args skip the file decode + matrix build.

Output layout:

```text
outputs/<sweep_name>/
  sweep_config.yaml                         # copy of the input
  .sweep_test_binary                        # prebuilt go test binary
  prebuild.log
  results.jsonl                             # append-only log of every completed run
  gossipsub/
    d<D>_dl<D_low>_dh<D_high>/              # one subdir per gossipsub D-param set
      topo-<seed>.json                      # only gossipsub data
      topo-<seed>.meta.json
      topo-<seed>.attempt-N.log
  spread/
    runs/
      ap<p_i>_af<f_i>_ep<p_e>_ef<f_e>/      # one dir per unique spread config
        topo-<seed>.json                    # only spread data
        topo-<seed>.meta.json
        topo-<seed>.attempt-N.log
```

#### Migrating old sweep outputs

If you have a sweep directory from before the layout refactor, migrate it once with:

```bash
python3 simnet_spread_tests/one_time_migrate_layout.py \
    --data-dir simnet_spread_tests/outputs/<sweep> \
    --gossipsub-d 6,5,12
```

`--gossipsub-d` is required: old exports don't record the D params they were run with, so you declare them at migration time (determines the `d<D>_dl<D_low>_dh<D_high>/` subdir for the gossipsub split). Migration is idempotent — safe to rerun. After completion, the original `runs/` dir is moved to `runs.legacy-backup/` as a safety net.

### 4. Plot

```bash
python3 simnet_spread_tests/sweep_plotter.py \
    --data-dir simnet_spread_tests/outputs/<sweep_name> \
    --groups simnet_spread_tests/plot_groups.yaml
```

The plot groups YAML is independent from the run config — you can re-group the same data for different charts without re-running experiments. It needs a `groups:` section (each tuple must match exactly a `(p_i, f_i, p_e, f_e)` that was run) and optionally:

- `topologies:` — list of seeds to restrict the plot to. Omit/empty → use every `topo-*.json` found.
- `gossipsub_d: {D: 6, D_low: 5, D_high: 12}` — pick which gossipsub D-param subdir to use for the baseline. Required only when multiple `gossipsub/d*_dl*_dh*/` exist; auto-picked if only one is present.

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--attacker-pct` | `0.2` | attacker fraction for the anonymity metric |
| `--sort-key` | `p_i` | how to order points within a group for the connecting line (`p_i \| f_i \| p_e \| f_e \| x \| anon`; `x` sorts by the current chart's X axis) |

Outputs to `<data-dir>/plots/` — one scatter per (metric, stat), six total:

| File | X axis |
|------|--------|
| `scatter_anon_vs_stretch_mean.png`    | spread stretch — mean |
| `scatter_anon_vs_stretch_median.png`  | spread stretch — median |
| `scatter_anon_vs_stretch_p95.png`     | spread stretch — p95 |
| `scatter_anon_vs_latency_mean.png`    | spread latency (ms) — mean |
| `scatter_anon_vs_latency_median.png`  | spread latency (ms) — median |
| `scatter_anon_vs_latency_p95.png`     | spread latency (ms) — p95 |
| `metrics_attacker<pct>.csv`           | per-config aggregates (stretch mean/median/p90/p95/p99 + latency mean/median/p95 + accuracy) |

Plot anytime — even while the runner is still going. It only reads completed runs.

### 5. Per-pair latency drill-down (single config, single topology)

The main plotter aggregates across topologies and configs. When you want to look at **one cell** of the sweep — one spread config on one topology — and compare every delivery against its ideal (direct-pair) latency, use `pair_latency.py`.

```bash
python3 simnet_spread_tests/pair_latency.py \
    --data-dir simnet_spread_tests/outputs/<sweep> \
    --tag ap<p_i>_af<f_i>_ep<p_e>_ef<f_e> \
    --seed <topology_seed> \
    [--gossipsub-d D,D_low,D_high]
```

Example — rho_intra=0.5, fanout_intra=4, rho_inter=0.5833, fanout_inter=6, on topology 1337:

```bash
python3 simnet_spread_tests/pair_latency.py \
    --data-dir simnet_spread_tests/outputs/sweep_B_case \
    --tag ap0.5_af4_ep0.5833_ef6 \
    --seed 1337
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--tag` | — | spread config directory name (same format as `spread/runs/`) |
| `--seed` | — | topology seed |
| `--gossipsub-d` | auto | `D,D_low,D_high` — required only when multiple `gossipsub/d*_dl*_dh*/` subdirs exist |
| `--out-dir` | `<data-dir>/analysis/<tag>_topo-<seed>/` | where to write the CSV and chart |

Outputs:

| File | Description |
|------|-------------|
| `pair_latencies.csv` | one row per delivery: `scenario, trial, source, destination, latency_ms, ideal_latency_ms, delta_ms, stretch`. Both scenarios for this (config, seed) in the same file, distinguished by the `scenario` column. |
| `delta_vs_ideal.png` | scatter with X = ideal (direct-pair) latency [ms], Y = delta (actual − ideal) [ms], one point per delivery, colored by scenario. Zero-delta reference line drawn. |

Ideal latency is reconstructed as `latency_ms / stretch` — the same direct-pair value the Go test used when computing stretch. If a delivery has `stretch == 0` (unknown pairwise weight in the dataset), the row lands in the CSV with `NaN` ideal/delta and is dropped from the chart.

If the requested (tag, seed) exists for only one scenario, the script emits what it can and prints a note about the missing side.
