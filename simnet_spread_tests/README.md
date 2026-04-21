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

Config sections:

| Section | Meaning |
|---------|---------|
| `topologies:` | list of seeds — each unique spread config is run once per seed |
| `env:` | pass-through `SPREAD_*` env vars (including `SPREAD_SIMNET_NODES`, `SPREAD_SIMNET_TRIALS`) |
| `groups:` | `group_name → list of [p_i, f_i, p_e, f_e]` — each group is one connected line on the plot |

### 3. Run the sweep

```bash
python3 simnet_spread_tests/sweep_runner.py \
    --config simnet_spread_tests/sweep_config.yaml \
    --out-dir simnet_spread_tests/outputs/sweep_$(date +%Y%m%d_%H%M%S)
```

Resumable: kill at any time and rerun with the same `--out-dir` — every `(config, topology)` whose `topo-<seed>.json` already exists is skipped.

#### Extending an existing sweep (`--extend`)

Already ran a sweep and want **more trials per config** without re-running what you have? Pass `--extend`:

```bash
python3 simnet_spread_tests/sweep_runner.py \
    --config simnet_spread_tests/sweep_config.yaml \
    --out-dir simnet_spread_tests/outputs/<existing_sweep> \
    --extend
```

Behavior:
- For each `(config, topology)` whose `topo-<seed>.json` **already exists**, the runner reads its trial count, runs `SPREAD_SIMNET_TRIALS` more trials starting from that offset, and merges the new trials into the same JSON. Trial indices stay continuous (`0..29` + `30..89`).
- For each `(config, topology)` whose `topo-<seed>.json` **does not exist**, the runner runs a fresh batch of `SPREAD_SIMNET_TRIALS` trials — same as a normal run.
- The start offset is computed automatically from the existing JSON. You never set `SPREAD_SIMNET_START_TRIAL` yourself.
- Continuation is **deterministic**: trial N's source (`alive[N % len(alive)]`) and RNG seed (`seed + 7919 + N`) are pure functions of `(seed, N)`, so extending `30 → 90` is byte-identical to running `90` from scratch.
- After each merge, the runner recomputes `gossipsub_attacker_accuracy` / `spread_attacker_accuracy` from the combined trial list.

**Numerical example.** You ran with `SPREAD_SIMNET_TRIALS: 30` (30 nodes, 1 topology) and now want 60 more trials per config:

1. Bump `SPREAD_SIMNET_TRIALS` to `60` in `sweep_config.yaml`.
2. Rerun with `--extend` and the **same** `--out-dir`.
3. Each existing topo file goes from 30 → 90 trials. Any config you added to the YAML since the first run gets a fresh 60-trial batch.

#### Skipping a scenario

Set `SPREAD_SIMNET_SKIP_SCENARIO` in the config's `env:` section (or the process env) to `gossipsub` or `spread` to run only one side. Halves the per-run cost when you only need more data on one protocol. Leave unset (or empty) to run both, the default.

Caveat: if you skip one side and later extend the same out-dir with both sides enabled, the new gossipsub (or spread) data starts at the trial offset of the other side — which means trials `0..start-1` are missing for the previously-skipped side. Pick a scheme and stick with it per sweep.

#### Per-run speedups

Two optimizations are built in and need no configuration:

- The test binary is compiled **once** at the start of a sweep (`go test -c`) and the resulting binary is reused for every run, skipping Go's build pipeline per invocation.
- The Ethereum-like latency matrix is cached in-process by `(nodes, seed)`, so within one invocation the gossipsub and spread scenarios share the same parsed `spread_data/` files and computed weights matrix instead of redoing the decode + matrix build twice.

Output layout:

```text
outputs/<sweep_name>/
  sweep_config.yaml                       # copy of the input
  runs/
    ir<p_i>_ip<p_e>_if<f_i>_ef<f_e>/     # one directory per unique config
      topo-<seed>.json                    # raw export (same schema as batch runner)
      topo-<seed>.meta.json               # timestamp, duration, groups
      topo-<seed>.attempt-N.log           # stdout from each attempt
  results.jsonl                           # append-only log of every completed run
```

### 4. Plot

```bash
python3 simnet_spread_tests/sweep_plotter.py \
    --data-dir simnet_spread_tests/outputs/<sweep_name> \
    --groups simnet_spread_tests/plot_groups.yaml
```

The plot groups YAML is independent from the run config — you can re-group the same data for different charts without re-running experiments. It needs a `groups:` section (each tuple must match exactly a `(p_i, f_i, p_e, f_e)` that was run) and optionally a `topologies:` list to restrict which seeds feed the plot. If `topologies:` is omitted or empty, every `topo-*.json` under each config dir is used — the default.

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--stretch-stat` | `mean` | `mean \| median \| p90 \| p99` — which stretch statistic on the X axis |
| `--attacker-pct` | `0.2` | attacker fraction for the anonymity metric |
| `--sort-key` | `p_i` | how to order points within a group for the connecting line |

Outputs to `<data-dir>/plots/`:

| File | Description |
|------|-------------|
| `scatter_anon_vs_stretch_<stat>.png` | the main plot — lower-left = better |
| `metrics_attacker<pct>.csv` | per-config aggregated metrics for any downstream analysis |

Plot anytime — even while the runner is still going. It only reads completed runs.
