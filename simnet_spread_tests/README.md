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
  summary.json           # aggregate summary rebuilt from results.jsonl
  runs/
    run-000000.json      # per-run exported metrics (on success)
  logs/
    run-000000-attempt-00.log
  plots/
    *.png
    summary.txt
```

Notes:
- Directory is created automatically if it does not exist.
- Resume uses `checkpoint.json` + existing `results.jsonl`.


## Plot Results

From repo root:

```bash
python3 simnet_spread_tests/plot_results.py
```

Optional flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--results PATH` | `simnet_spread_tests/outputs/results.jsonl` | Path to results file |
| `--out DIR` | `simnet_spread_tests/outputs/plots` | Output directory for PNGs |
