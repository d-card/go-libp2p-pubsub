# Simnet Spread Experiments

## Run Experiments

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
