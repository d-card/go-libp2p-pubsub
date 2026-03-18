package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	testName                   = "TestSimnetSpreadVsGossipsubLatencyStretch"
	defaultConfigPath          = "simnet_spread_tests/config.yaml"
	checkpointFilename         = "checkpoint.json"
	resultsFilename            = "results.jsonl"
	runsDirname                = "runs"
	logsDirname                = "logs"
	spreadExportPathEnv        = "SPREAD_SIMNET_EXPORT_PATH"
	spreadRunIDEnv             = "SPREAD_SIMNET_RUN_ID"
	spreadGitCommitEnv         = "SPREAD_SIMNET_GIT_COMMIT"
	spreadNodesEnv             = "SPREAD_SIMNET_NODES"
	spreadTrialsEnv            = "SPREAD_SIMNET_TRIALS"
	spreadSeedEnv              = "SPREAD_SIMNET_SEED"
	spreadWarmupEveryEnv       = "SPREAD_SIMNET_WARMUP_EVERY"
	spreadWarmupRoundsEnv      = "SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH"
	spreadLinkMibpsEnv         = "SPREAD_SIMNET_LINK_MIBPS"
	spreadScenarioTimeoutMSEnv = "SPREAD_SIMNET_SCENARIO_TIMEOUT_MS"
	spreadClusterPctEnv        = "SPREAD_CLUSTER_PCT"
	spreadNumRingsEnv          = "SPREAD_NUM_RINGS"
	spreadIntraFanoutEnv       = "SPREAD_INTRA_FANOUT"
	spreadInterFanoutEnv       = "SPREAD_INTER_FANOUT"
	spreadIntraRhoEnv          = "SPREAD_INTRA_RHO"
	spreadInterProbEnv         = "SPREAD_INTER_PROB"
	spreadFallbackThresholdEnv = "SPREAD_FALLBACK_THRESHOLD"
	spreadDuplicateRepropEnv   = "SPREAD_DUPLICATE_REPROPAGATION"
	spreadCcEnv                = "SPREAD_CC"
	spreadCeEnv                = "SPREAD_CE"
	spreadNewtonEnv            = "SPREAD_NEWTON"
	spreadOutlierThresholdEnv  = "SPREAD_OUTLIER_THRESHOLD"
	spreadSamplesEnv           = "SPREAD_SAMPLES"
	spreadIntervalMSEnv        = "SPREAD_INTERVAL_MS"
	spreadNeighborSetSizeEnv   = "SPREAD_NEIGHBOR_SET_SIZE"
	spreadIN1ThresholdMSEnv    = "SPREAD_IN1_THRESHOLD_MS"
	spreadIN2ThresholdMSEnv    = "SPREAD_IN2_THRESHOLD_MS"
	spreadIN3MADKRandomEnv     = "SPREAD_IN3_MADK_RANDOM"
	spreadIN3MADKCloseEnv      = "SPREAD_IN3_MADK_CLOSE"
	spreadIN3MinSamplesEnv     = "SPREAD_IN3_MIN_SAMPLES"
)

type stringListFlag []string

func (s *stringListFlag) String() string { return strings.Join(*s, ",") }
func (s *stringListFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

type runConfig struct {
	Nodes             int     `json:"nodes"`
	Trials            int     `json:"trials"`
	Seed              int     `json:"seed"`
	WarmupEvery       int     `json:"warmup_every"`
	WarmupPerPublish  int     `json:"warmup_rounds_per_publish"`
	LinkMiBps         int     `json:"link_mibps"`
	ScenarioTimeoutMs int     `json:"scenario_timeout_ms"`
	SpreadClusterPct  float64 `json:"spread_cluster_pct"`
	SpreadNumRings    int     `json:"spread_num_rings"`
	SpreadIntraFanout int     `json:"spread_intra_fanout"`
	SpreadInterFanout int     `json:"spread_inter_fanout"`
	SpreadIntraRho    float64 `json:"spread_intra_rho"`
	SpreadInterProb   float64 `json:"spread_inter_prob"`
	SpreadFallback    int     `json:"spread_fallback_threshold"`
	SpreadDupReprop   int     `json:"spread_duplicate_repropagation"`
	SpreadCC          float64 `json:"spread_cc"`
	SpreadCE          float64 `json:"spread_ce"`
	SpreadNewton      bool    `json:"spread_newton"`
	SpreadOutlier     float64 `json:"spread_outlier_threshold"`
	SpreadSamples     int     `json:"spread_samples"`
	SpreadIntervalMS  int     `json:"spread_interval_ms"`
	SpreadNeighborSet int     `json:"spread_neighbor_set_size"`
	SpreadIN1MS       float64 `json:"spread_in1_threshold_ms"`
	SpreadIN2MS       float64 `json:"spread_in2_threshold_ms"`
	SpreadIN3Rand     float64 `json:"spread_in3_madk_random"`
	SpreadIN3Close    float64 `json:"spread_in3_madk_close"`
	SpreadIN3Min      int     `json:"spread_in3_min_samples"`
}

type runExport struct {
	RunID       string `json:"run_id"`
	GitCommit   string `json:"git_commit"`
	GeneratedAt string `json:"generated_at"`
	Nodes       int    `json:"nodes"`
	Trials      int    `json:"trials"`
	Seed        int    `json:"seed"`
}

type attemptRecord struct {
	RunID      int       `json:"run_id"`
	Attempt    int       `json:"attempt"`
	RetryCount int       `json:"retry_count"`
	Status     string    `json:"status"`
	StartTime  string    `json:"start_time"`
	EndTime    string    `json:"end_time"`
	DurationMS int64     `json:"duration_ms"`
	ExitCode   *int      `json:"exit_code,omitempty"`
	Error      string    `json:"error,omitempty"`
	ExportPath string    `json:"export_path"`
	LogPath    string    `json:"log_path"`
	GitCommit  string    `json:"git_commit,omitempty"`
	Config     runConfig `json:"config"`
}

type checkpoint struct {
	RunsTarget   int    `json:"runs_target"`
	NextRun      int    `json:"next_run"`
	Completed    int    `json:"completed_runs"`
	SuccessRuns  int    `json:"success_runs"`
	FailedRuns   int    `json:"failed_runs"`
	TimeoutRuns  int    `json:"timeout_runs"`
	Attempts     int    `json:"attempts_total"`
	UpdatedAtUTC string `json:"updated_at"`
}

type batchOptions struct {
	Runs          int
	OutDir        string
	MaxRetries    int
	PerRunTimeout time.Duration
	Resume        bool
	EnvOverrides  map[string]string
}

type fileConfig struct {
	Batch  batchConfigYAML  `yaml:"batch"`
	Simnet simnetConfigYAML `yaml:"simnet"`
	Spread spreadConfigYAML `yaml:"spread"`
}

type batchConfigYAML struct {
	Runs          *int    `yaml:"runs"`
	OutDir        *string `yaml:"out_dir"`
	MaxRetries    *int    `yaml:"max_retries"`
	PerRunTimeout *string `yaml:"per_run_timeout"`
	Resume        *bool   `yaml:"resume"`
}

type simnetConfigYAML struct {
	Nodes                  *int `yaml:"spread_simnet_nodes"`
	Trials                 *int `yaml:"spread_simnet_trials"`
	Seed                   *int `yaml:"spread_simnet_seed"`
	WarmupEvery            *int `yaml:"spread_simnet_warmup_every"`
	WarmupRoundsPerPublish *int `yaml:"spread_simnet_warmup_rounds_per_publish"`
	LinkMibps              *int `yaml:"spread_simnet_link_mibps"`
	ScenarioTimeoutMS      *int `yaml:"spread_simnet_scenario_timeout_ms"`
}

type spreadConfigYAML struct {
	ClusterPct             *float64 `yaml:"spread_cluster_pct"`
	NumRings               *int     `yaml:"spread_num_rings"`
	IntraFanout            *int     `yaml:"spread_intra_fanout"`
	InterFanout            *int     `yaml:"spread_inter_fanout"`
	IntraRho               *float64 `yaml:"spread_intra_rho"`
	InterProb              *float64 `yaml:"spread_inter_prob"`
	FallbackThreshold      *int     `yaml:"spread_fallback_threshold"`
	DuplicateRepropagation *int     `yaml:"spread_duplicate_repropagation"`
	CC                     *float64 `yaml:"spread_cc"`
	CE                     *float64 `yaml:"spread_ce"`
	Newton                 *bool    `yaml:"spread_newton"`
	OutlierThreshold       *float64 `yaml:"spread_outlier_threshold"`
	Samples                *int     `yaml:"spread_samples"`
	IntervalMS             *int     `yaml:"spread_interval_ms"`
	NeighborSetSize        *int     `yaml:"spread_neighbor_set_size"`
	IN1ThresholdMS         *float64 `yaml:"spread_in1_threshold_ms"`
	IN2ThresholdMS         *float64 `yaml:"spread_in2_threshold_ms"`
	IN3MADKRandom          *float64 `yaml:"spread_in3_madk_random"`
	IN3MADKClose           *float64 `yaml:"spread_in3_madk_close"`
	IN3MinSamples          *int     `yaml:"spread_in3_min_samples"`
}

type executionOutcome struct {
	Status   string
	ExitCode *int
	ErrText  string
}

type runExecutor func(ctx context.Context, runID, attempt int, runIDLabel, exportPath, logPath, gitCommit string, cfg runConfig, env map[string]string) executionOutcome

func main() {
	cfg, created, err := loadOrCreateConfig(defaultConfigPath)
	if err != nil {
		panic(err)
	}
	if created {
		_, _ = io.WriteString(os.Stderr, "created default config at "+defaultConfigPath+"\n")
	}

	opts := batchOptions{
		Runs:         *cfg.Batch.Runs,
		OutDir:       *cfg.Batch.OutDir,
		MaxRetries:   *cfg.Batch.MaxRetries,
		Resume:       *cfg.Batch.Resume,
		EnvOverrides: map[string]string{},
	}
	opts.PerRunTimeout, err = time.ParseDuration(*cfg.Batch.PerRunTimeout)
	if err != nil {
		panic(fmt.Errorf("invalid batch.per_run_timeout %q: %w", *cfg.Batch.PerRunTimeout, err))
	}

	var envFlags stringListFlag

	flag.IntVar(&opts.Runs, "runs", opts.Runs, "Number of runs to execute")
	flag.StringVar(&opts.OutDir, "out-dir", opts.OutDir, "Output directory for artifacts")
	flag.IntVar(&opts.MaxRetries, "max-retries", opts.MaxRetries, "Retries per run after a failed attempt")
	flag.DurationVar(&opts.PerRunTimeout, "per-run-timeout", opts.PerRunTimeout, "Timeout per run")
	flag.BoolVar(&opts.Resume, "resume", opts.Resume, "Resume from checkpoint if available")
	flag.Var(&envFlags, "env", "Environment override in KEY=VALUE form (repeatable)")
	flag.Parse()

	if opts.Runs <= 0 {
		exitf("--runs must be > 0")
	}
	if opts.MaxRetries < 0 {
		exitf("--max-retries cannot be negative")
	}
	if opts.PerRunTimeout <= 0 {
		exitf("--per-run-timeout must be > 0")
	}

	overrides, err := parseEnvOverrides(envFlags)
	if err != nil {
		exitf("parse --env: %v", err)
	}
	for k, v := range overrides {
		opts.EnvOverrides[k] = v
	}

	baseEnv := mergedEnv(configToEnvMap(cfg), opts.EnvOverrides)
	baseCfg := configFromEnv(baseEnv)

	if err := runBatch(opts, baseEnv, baseCfg, executeGoTestRun); err != nil {
		exitf("batch run failed: %v", err)
	}
}

func runBatch(opts batchOptions, baseEnv map[string]string, baseCfg runConfig, execFn runExecutor) error {
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return fmt.Errorf("create out-dir: %w", err)
	}
	runsDir := filepath.Join(opts.OutDir, runsDirname)
	logsDir := filepath.Join(opts.OutDir, logsDirname)
	if err := os.MkdirAll(runsDir, 0o755); err != nil {
		return fmt.Errorf("create runs dir: %w", err)
	}
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return fmt.Errorf("create logs dir: %w", err)
	}

	checkpointPath := filepath.Join(opts.OutDir, checkpointFilename)
	resultsPath := filepath.Join(opts.OutDir, resultsFilename)

	if !opts.Resume {
		if err := resetOutputDir(opts.OutDir, runsDir, logsDir, resultsPath, checkpointPath); err != nil {
			return err
		}
	}

	cp, err := loadCheckpoint(checkpointPath)
	if err != nil {
		return err
	}
	cp.RunsTarget = opts.Runs
	if cp.NextRun < 0 {
		cp.NextRun = 0
	}

	gitCommit := currentGitCommit()

	for runID := cp.NextRun; runID < opts.Runs; runID++ {
		runLabel := fmt.Sprintf("run-%06d", runID)
		exportPath := filepath.Join(runsDir, runLabel+".json")

		for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
			_ = os.Remove(exportPath)
			logPath := filepath.Join(logsDir, fmt.Sprintf("%s-attempt-%02d.log", runLabel, attempt))

			start := time.Now().UTC()
			ctx, cancel := context.WithTimeout(context.Background(), opts.PerRunTimeout)
			outcome := execFn(ctx, runID, attempt, runLabel, exportPath, logPath, gitCommit, baseCfg, baseEnv)
			cancel()
			end := time.Now().UTC()

			rec := attemptRecord{
				RunID:      runID,
				Attempt:    attempt,
				RetryCount: attempt,
				Status:     outcome.Status,
				StartTime:  start.Format(time.RFC3339Nano),
				EndTime:    end.Format(time.RFC3339Nano),
				DurationMS: end.Sub(start).Milliseconds(),
				ExitCode:   outcome.ExitCode,
				Error:      outcome.ErrText,
				ExportPath: exportPath,
				LogPath:    logPath,
				GitCommit:  gitCommit,
				Config:     baseCfg,
			}
			if err := appendJSONLine(resultsPath, rec); err != nil {
				return err
			}

			cp.Attempts++
			if outcome.Status == "success" {
				cp.SuccessRuns++
				cp.Completed++
				cp.NextRun = runID + 1
				break
			}

			if attempt == opts.MaxRetries {
				if outcome.Status == "timeout" {
					cp.TimeoutRuns++
				} else {
					cp.FailedRuns++
				}
				cp.Completed++
				cp.NextRun = runID + 1
				break
			}

			cp.UpdatedAtUTC = time.Now().UTC().Format(time.RFC3339Nano)
			if err := writeJSONAtomic(checkpointPath, cp); err != nil {
				return err
			}
		}

		cp.UpdatedAtUTC = time.Now().UTC().Format(time.RFC3339Nano)
		if err := writeJSONAtomic(checkpointPath, cp); err != nil {
			return err
		}
	}

	return nil
}

func executeGoTestRun(ctx context.Context, runID, attempt int, runIDLabel, exportPath, logPath, gitCommit string, cfg runConfig, env map[string]string) executionOutcome {
	envCopy := cloneEnvMap(env)
	envCopy[spreadExportPathEnv] = exportPath
	envCopy[spreadRunIDEnv] = runIDLabel
	envCopy[spreadGitCommitEnv] = gitCommit
	if cfg.ScenarioTimeoutMs > 0 {
		envCopy[spreadScenarioTimeoutMSEnv] = strconv.Itoa(cfg.ScenarioTimeoutMs)
	}

	cmd := exec.CommandContext(ctx, "go", "test", "-tags", "simnet", "-run", testName, "-v", ".")
	cmd.Env = envMapToList(envCopy)
	output, err := cmd.CombinedOutput()
	_ = os.WriteFile(logPath, output, 0o644)

	if ctx.Err() == context.DeadlineExceeded {
		return executionOutcome{Status: "timeout", ErrText: "attempt timed out"}
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			ec := exitErr.ExitCode()
			return executionOutcome{Status: "fail", ExitCode: &ec, ErrText: shortErr(err, output)}
		}
		return executionOutcome{Status: "fail", ErrText: shortErr(err, output)}
	}

	exported, readErr := readRunExport(exportPath)
	if readErr != nil {
		return executionOutcome{Status: "fail", ErrText: readErr.Error()}
	}
	_ = exported // validation only: file exists and is valid JSON
	return executionOutcome{Status: "success"}
}

func shortErr(cmdErr error, output []byte) string {
	trimmed := strings.TrimSpace(string(output))
	if trimmed == "" {
		return cmdErr.Error()
	}
	lines := strings.Split(trimmed, "\n")
	if len(lines) > 8 {
		lines = lines[len(lines)-8:]
	}
	return fmt.Sprintf("%v; tail=%q", cmdErr, strings.Join(lines, " | "))
}

func parseEnvOverrides(items []string) (map[string]string, error) {
	out := make(map[string]string)
	for _, it := range items {
		k, v, ok := strings.Cut(it, "=")
		if !ok || strings.TrimSpace(k) == "" {
			return nil, fmt.Errorf("invalid env override %q (expected KEY=VALUE)", it)
		}
		out[k] = v
	}
	return out, nil
}

func loadOrCreateConfig(path string) (fileConfig, bool, error) {
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return fileConfig{}, false, fmt.Errorf("stat config: %w", err)
		}
		if err := writeDefaultConfig(path); err != nil {
			return fileConfig{}, false, err
		}
		cfg, err := loadConfig(path)
		return cfg, true, err
	}
	cfg, err := loadConfig(path)
	return cfg, false, err
}

func writeDefaultConfig(path string) error {
	cfg := defaultFileConfig()
	b, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal default config yaml: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write default config: %w", err)
	}
	return nil
}

func loadConfig(path string) (fileConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return fileConfig{}, fmt.Errorf("read config: %w", err)
	}
	var cfg fileConfig
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return fileConfig{}, fmt.Errorf("decode config yaml: %w", err)
	}
	if err := validateConfig(cfg); err != nil {
		return fileConfig{}, err
	}
	return cfg, nil
}

func defaultFileConfig() fileConfig {
	runs := 100
	outDir := "simnet_spread_tests/outputs"
	maxRetries := 2
	perRunTimeout := "25m"
	resume := true

	nodes := 20
	trials := 500
	seed := 1337
	warmupEvery := 0
	warmupRounds := 1
	linkMibps := 20
	scenarioTimeoutMS := int((10 * time.Minute) / time.Millisecond)

	clusterPct := 0.25
	numRings := 4
	intraFanout := 3
	interFanout := 8
	intraRho := 0.6
	interProb := 0.8
	fallback := 3
	dupReprop := 5
	cc := 0.25
	ce := 0.25
	newton := false
	outlier := 0.0
	samples := 1
	intervalMS := 150
	neighborSet := 24
	in1 := 20.0
	in2 := 35.0
	in3Random := 5.0
	in3Close := 8.0
	in3Min := 4

	return fileConfig{
		Batch: batchConfigYAML{
			Runs:          &runs,
			OutDir:        &outDir,
			MaxRetries:    &maxRetries,
			PerRunTimeout: &perRunTimeout,
			Resume:        &resume,
		},
		Simnet: simnetConfigYAML{
			Nodes:                  &nodes,
			Trials:                 &trials,
			Seed:                   &seed,
			WarmupEvery:            &warmupEvery,
			WarmupRoundsPerPublish: &warmupRounds,
			LinkMibps:              &linkMibps,
			ScenarioTimeoutMS:      &scenarioTimeoutMS,
		},
		Spread: spreadConfigYAML{
			ClusterPct:             &clusterPct,
			NumRings:               &numRings,
			IntraFanout:            &intraFanout,
			InterFanout:            &interFanout,
			IntraRho:               &intraRho,
			InterProb:              &interProb,
			FallbackThreshold:      &fallback,
			DuplicateRepropagation: &dupReprop,
			CC:                     &cc,
			CE:                     &ce,
			Newton:                 &newton,
			OutlierThreshold:       &outlier,
			Samples:                &samples,
			IntervalMS:             &intervalMS,
			NeighborSetSize:        &neighborSet,
			IN1ThresholdMS:         &in1,
			IN2ThresholdMS:         &in2,
			IN3MADKRandom:          &in3Random,
			IN3MADKClose:           &in3Close,
			IN3MinSamples:          &in3Min,
		},
	}
}

func validateConfig(cfg fileConfig) error {
	missing := make([]string, 0)

	require := func(ok bool, name string) {
		if !ok {
			missing = append(missing, name)
		}
	}

	require(cfg.Batch.Runs != nil, "batch.runs")
	require(cfg.Batch.OutDir != nil, "batch.out_dir")
	require(cfg.Batch.MaxRetries != nil, "batch.max_retries")
	require(cfg.Batch.PerRunTimeout != nil, "batch.per_run_timeout")
	require(cfg.Batch.Resume != nil, "batch.resume")

	require(cfg.Simnet.Nodes != nil, "simnet.spread_simnet_nodes")
	require(cfg.Simnet.Trials != nil, "simnet.spread_simnet_trials")
	require(cfg.Simnet.Seed != nil, "simnet.spread_simnet_seed")
	require(cfg.Simnet.WarmupEvery != nil, "simnet.spread_simnet_warmup_every")
	require(cfg.Simnet.WarmupRoundsPerPublish != nil, "simnet.spread_simnet_warmup_rounds_per_publish")
	require(cfg.Simnet.LinkMibps != nil, "simnet.spread_simnet_link_mibps")
	require(cfg.Simnet.ScenarioTimeoutMS != nil, "simnet.spread_simnet_scenario_timeout_ms")

	require(cfg.Spread.ClusterPct != nil, "spread.spread_cluster_pct")
	require(cfg.Spread.NumRings != nil, "spread.spread_num_rings")
	require(cfg.Spread.IntraFanout != nil, "spread.spread_intra_fanout")
	require(cfg.Spread.InterFanout != nil, "spread.spread_inter_fanout")
	require(cfg.Spread.IntraRho != nil, "spread.spread_intra_rho")
	require(cfg.Spread.InterProb != nil, "spread.spread_inter_prob")
	require(cfg.Spread.FallbackThreshold != nil, "spread.spread_fallback_threshold")
	require(cfg.Spread.DuplicateRepropagation != nil, "spread.spread_duplicate_repropagation")
	require(cfg.Spread.CC != nil, "spread.spread_cc")
	require(cfg.Spread.CE != nil, "spread.spread_ce")
	require(cfg.Spread.Newton != nil, "spread.spread_newton")
	require(cfg.Spread.OutlierThreshold != nil, "spread.spread_outlier_threshold")
	require(cfg.Spread.Samples != nil, "spread.spread_samples")
	require(cfg.Spread.IntervalMS != nil, "spread.spread_interval_ms")
	require(cfg.Spread.NeighborSetSize != nil, "spread.spread_neighbor_set_size")
	require(cfg.Spread.IN1ThresholdMS != nil, "spread.spread_in1_threshold_ms")
	require(cfg.Spread.IN2ThresholdMS != nil, "spread.spread_in2_threshold_ms")
	require(cfg.Spread.IN3MADKRandom != nil, "spread.spread_in3_madk_random")
	require(cfg.Spread.IN3MADKClose != nil, "spread.spread_in3_madk_close")
	require(cfg.Spread.IN3MinSamples != nil, "spread.spread_in3_min_samples")

	if len(missing) > 0 {
		return fmt.Errorf("config missing required keys: %s", strings.Join(missing, ", "))
	}
	if _, err := time.ParseDuration(*cfg.Batch.PerRunTimeout); err != nil {
		return fmt.Errorf("invalid batch.per_run_timeout: %w", err)
	}
	return nil
}

func configToEnvMap(cfg fileConfig) map[string]string {
	return map[string]string{
		spreadNodesEnv:             strconv.Itoa(*cfg.Simnet.Nodes),
		spreadTrialsEnv:            strconv.Itoa(*cfg.Simnet.Trials),
		spreadSeedEnv:              strconv.Itoa(*cfg.Simnet.Seed),
		spreadWarmupEveryEnv:       strconv.Itoa(*cfg.Simnet.WarmupEvery),
		spreadWarmupRoundsEnv:      strconv.Itoa(*cfg.Simnet.WarmupRoundsPerPublish),
		spreadLinkMibpsEnv:         strconv.Itoa(*cfg.Simnet.LinkMibps),
		spreadScenarioTimeoutMSEnv: strconv.Itoa(*cfg.Simnet.ScenarioTimeoutMS),
		spreadClusterPctEnv:        fmt.Sprintf("%g", *cfg.Spread.ClusterPct),
		spreadNumRingsEnv:          strconv.Itoa(*cfg.Spread.NumRings),
		spreadIntraFanoutEnv:       strconv.Itoa(*cfg.Spread.IntraFanout),
		spreadInterFanoutEnv:       strconv.Itoa(*cfg.Spread.InterFanout),
		spreadIntraRhoEnv:          fmt.Sprintf("%g", *cfg.Spread.IntraRho),
		spreadInterProbEnv:         fmt.Sprintf("%g", *cfg.Spread.InterProb),
		spreadFallbackThresholdEnv: strconv.Itoa(*cfg.Spread.FallbackThreshold),
		spreadDuplicateRepropEnv:   strconv.Itoa(*cfg.Spread.DuplicateRepropagation),
		spreadCcEnv:                fmt.Sprintf("%g", *cfg.Spread.CC),
		spreadCeEnv:                fmt.Sprintf("%g", *cfg.Spread.CE),
		spreadNewtonEnv:            strconv.FormatBool(*cfg.Spread.Newton),
		spreadOutlierThresholdEnv:  fmt.Sprintf("%g", *cfg.Spread.OutlierThreshold),
		spreadSamplesEnv:           strconv.Itoa(*cfg.Spread.Samples),
		spreadIntervalMSEnv:        strconv.Itoa(*cfg.Spread.IntervalMS),
		spreadNeighborSetSizeEnv:   strconv.Itoa(*cfg.Spread.NeighborSetSize),
		spreadIN1ThresholdMSEnv:    fmt.Sprintf("%g", *cfg.Spread.IN1ThresholdMS),
		spreadIN2ThresholdMSEnv:    fmt.Sprintf("%g", *cfg.Spread.IN2ThresholdMS),
		spreadIN3MADKRandomEnv:     fmt.Sprintf("%g", *cfg.Spread.IN3MADKRandom),
		spreadIN3MADKCloseEnv:      fmt.Sprintf("%g", *cfg.Spread.IN3MADKClose),
		spreadIN3MinSamplesEnv:     strconv.Itoa(*cfg.Spread.IN3MinSamples),
	}
}

func configFromEnv(env map[string]string) runConfig {
	cfg := runConfig{}
	cfg.Nodes = envIntFromMap(env, spreadNodesEnv)
	cfg.Trials = envIntFromMap(env, spreadTrialsEnv)
	cfg.Seed = envIntFromMap(env, spreadSeedEnv)
	cfg.WarmupEvery = envIntFromMap(env, spreadWarmupEveryEnv)
	cfg.WarmupPerPublish = envIntFromMap(env, spreadWarmupRoundsEnv)
	cfg.LinkMiBps = envIntFromMap(env, spreadLinkMibpsEnv)
	cfg.ScenarioTimeoutMs = envIntFromMap(env, spreadScenarioTimeoutMSEnv)
	cfg.SpreadClusterPct = envFloatFromMap(env, spreadClusterPctEnv)
	cfg.SpreadNumRings = envIntFromMap(env, spreadNumRingsEnv)
	cfg.SpreadIntraFanout = envIntFromMap(env, spreadIntraFanoutEnv)
	cfg.SpreadInterFanout = envIntFromMap(env, spreadInterFanoutEnv)
	cfg.SpreadIntraRho = envFloatFromMap(env, spreadIntraRhoEnv)
	cfg.SpreadInterProb = envFloatFromMap(env, spreadInterProbEnv)
	cfg.SpreadFallback = envIntFromMap(env, spreadFallbackThresholdEnv)
	cfg.SpreadDupReprop = envIntFromMap(env, spreadDuplicateRepropEnv)
	cfg.SpreadCC = envFloatFromMap(env, spreadCcEnv)
	cfg.SpreadCE = envFloatFromMap(env, spreadCeEnv)
	cfg.SpreadNewton = envBoolFromMap(env, spreadNewtonEnv)
	cfg.SpreadOutlier = envFloatFromMap(env, spreadOutlierThresholdEnv)
	cfg.SpreadSamples = envIntFromMap(env, spreadSamplesEnv)
	cfg.SpreadIntervalMS = envIntFromMap(env, spreadIntervalMSEnv)
	cfg.SpreadNeighborSet = envIntFromMap(env, spreadNeighborSetSizeEnv)
	cfg.SpreadIN1MS = envFloatFromMap(env, spreadIN1ThresholdMSEnv)
	cfg.SpreadIN2MS = envFloatFromMap(env, spreadIN2ThresholdMSEnv)
	cfg.SpreadIN3Rand = envFloatFromMap(env, spreadIN3MADKRandomEnv)
	cfg.SpreadIN3Close = envFloatFromMap(env, spreadIN3MADKCloseEnv)
	cfg.SpreadIN3Min = envIntFromMap(env, spreadIN3MinSamplesEnv)
	return cfg
}

func envIntFromMap(env map[string]string, key string) int {
	v, _ := strconv.Atoi(env[key])
	return v
}

func envFloatFromMap(env map[string]string, key string) float64 {
	v, _ := strconv.ParseFloat(env[key], 64)
	return v
}

func envBoolFromMap(env map[string]string, key string) bool {
	v, _ := strconv.ParseBool(env[key])
	return v
}

func mergedEnv(base map[string]string, overrides map[string]string) map[string]string {
	m := cloneEnvMap(base)
	for _, kv := range os.Environ() {
		k, v, ok := strings.Cut(kv, "=")
		if ok {
			m[k] = v
		}
	}
	for k, v := range overrides {
		m[k] = v
	}
	return m
}

func cloneEnvMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func envMapToList(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+m[k])
	}
	return out
}

func appendJSONLine(path string, v any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open results jsonl: %w", err)
	}
	enc, err := json.Marshal(v)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("marshal jsonl record: %w", err)
	}
	enc = append(enc, '\n')
	if _, err := f.Write(enc); err != nil {
		_ = f.Close()
		return fmt.Errorf("append jsonl record: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync jsonl file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close jsonl file: %w", err)
	}
	return nil
}

func loadCheckpoint(path string) (checkpoint, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return checkpoint{}, nil
		}
		return checkpoint{}, fmt.Errorf("read checkpoint: %w", err)
	}
	var cp checkpoint
	if err := json.Unmarshal(b, &cp); err != nil {
		return checkpoint{}, fmt.Errorf("decode checkpoint: %w", err)
	}
	return cp, nil
}

func writeJSONAtomic(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	b = append(b, '\n')
	return writeAtomicFile(path, b, 0o644)
}

func writeAtomicFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create parent dir: %w", err)
	}
	tmp := filepath.Join(dir, "."+filepath.Base(path)+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	df, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := df.Sync(); err != nil {
		_ = df.Close()
		return err
	}
	if err := df.Close(); err != nil {
		return err
	}
	return nil
}

func readRunExport(path string) (runExport, error) {
	var out runExport
	b, err := os.ReadFile(path)
	if err != nil {
		return out, fmt.Errorf("read run export: %w", err)
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return out, fmt.Errorf("decode run export: %w", err)
	}
	return out, nil
}

func currentGitCommit() string {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func resetOutputDir(outDir, runsDir, logsDir, resultsPath, checkpointPath string) error {
	if err := os.RemoveAll(runsDir); err != nil {
		return fmt.Errorf("reset runs dir: %w", err)
	}
	if err := os.RemoveAll(logsDir); err != nil {
		return fmt.Errorf("reset logs dir: %w", err)
	}
	if err := os.MkdirAll(runsDir, 0o755); err != nil {
		return fmt.Errorf("recreate runs dir: %w", err)
	}
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return fmt.Errorf("recreate logs dir: %w", err)
	}
	if err := os.Remove(resultsPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("reset results file: %w", err)
	}
	if err := os.Remove(checkpointPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("reset checkpoint: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, ".keep"), []byte{}, 0o644); err != nil {
		return fmt.Errorf("ensure out-dir writable: %w", err)
	}
	return nil
}

func exitf(format string, args ...any) {
	_, _ = io.WriteString(os.Stderr, fmt.Sprintf(format+"\n", args...))
	os.Exit(1)
}
