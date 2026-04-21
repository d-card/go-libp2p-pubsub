// Simnet experiment
// Command: go test -tags simnet -run TestSimnetSpreadVsGossipsubLatencyStretch -v .

package pubsub

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/vivaldi"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	simnet "github.com/marcopolo/simnet"
)

const experimentTopic = "spread-vs-gossipsub-simnet"

const (
	SPREAD_SIMNET_EXPORT_PATH_ENV           = "SPREAD_SIMNET_EXPORT_PATH"
	SPREAD_SIMNET_RUN_ID_ENV                = "SPREAD_SIMNET_RUN_ID"
	SPREAD_SIMNET_GIT_COMMIT_ENV            = "SPREAD_SIMNET_GIT_COMMIT"
	SPREAD_SIMNET_ENABLE_CRASH_ENV          = "SPREAD_SIMNET_ENABLE_CRASH"
	SPREAD_SIMNET_CRASH_PCT_ENV             = "SPREAD_SIMNET_CRASH_PCT"
	SPREAD_SIMNET_START_TRIAL_ENV           = "SPREAD_SIMNET_START_TRIAL"
	SPREAD_SIMNET_NODES                     = 20               // Number of nodes
	SPREAD_SIMNET_TRIALS                    = 500              // Number of messages published
	SPREAD_SIMNET_WARMUP_EVERY              = 0                // Number of trials between warm-up rounds of vivaldi
	SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH = 1                // Number of vivaldi rounds to warm up before each publish
	SPREAD_SIMNET_SEED                      = 1337             // Random seed for topology generation (latencies and node regions)
	SPREAD_SIMNET_LINK_MIBPS                = 20               // Link bandwidth in MiB/s for all links in the simnet topology
	SPREAD_SIMNET_SCENARIO_TIMEOUT          = 10 * time.Minute // Overall timeout for each scenario
)

const (
	SPREAD_CLUSTER_PCT             = "SPREAD_CLUSTER_PCT"
	SPREAD_NUM_RINGS               = "SPREAD_NUM_RINGS"
	SPREAD_INTRA_FANOUT            = "SPREAD_INTRA_FANOUT"
	SPREAD_INTER_FANOUT            = "SPREAD_INTER_FANOUT"
	SPREAD_INTRA_RHO               = "SPREAD_INTRA_RHO"
	SPREAD_INTER_PROB              = "SPREAD_INTER_PROB"
	SPREAD_FALLBACK_THRESHOLD      = "SPREAD_FALLBACK_THRESHOLD"
	SPREAD_DUPLICATE_REPROPAGATION = "SPREAD_DUPLICATE_REPROPAGATION"
	SPREAD_USE_ANGULAR_INTER_PEERS = "SPREAD_USE_ANGULAR_INTER_PEERS"
	SPREAD_CC                      = "SPREAD_CC"
	SPREAD_CE                      = "SPREAD_CE"
	SPREAD_NEWTON                  = "SPREAD_NEWTON"
	SPREAD_OUTLIER_THRESHOLD       = "SPREAD_OUTLIER_THRESHOLD"
	SPREAD_SAMPLES                 = "SPREAD_SAMPLES"
	SPREAD_INTERVAL_MS             = "SPREAD_INTERVAL_MS"
	SPREAD_NEIGHBOR_SET_SIZE       = "SPREAD_NEIGHBOR_SET_SIZE"
	SPREAD_IN1_THRESHOLD_MS        = "SPREAD_IN1_THRESHOLD_MS"
	SPREAD_IN2_THRESHOLD_MS        = "SPREAD_IN2_THRESHOLD_MS"
	SPREAD_IN3_MADK_RANDOM         = "SPREAD_IN3_MADK_RANDOM"
	SPREAD_IN3_MADK_CLOSE          = "SPREAD_IN3_MADK_CLOSE"
	SPREAD_IN3_MIN_SAMPLES         = "SPREAD_IN3_MIN_SAMPLES"
	SPREAD_ATTACKER_PCTS           = "SPREAD_ATTACKER_PCTS"
)

type delivery struct {
	Dst       int     `json:"dst"`        // original node ID from nodes.json
	LatencyMS float64 `json:"latency_ms"` // observed one-way latency in ms
	Stretch   float64 `json:"stretch"`    // observed / direct pair latency
}

type trialResult struct {
	Trial             int                `json:"trial"`
	Src               int                `json:"src"` // original node ID from nodes.json
	Deliveries        []delivery         `json:"deliveries"`
	FirstReceipts     []firstReceipt     `json:"first_receipts,omitempty"`
	AttackerEstimates []attackerEstimate `json:"attacker_estimates,omitempty"`
}

type firstReceipt struct {
	Node            int     `json:"node"`              // receiver node ID
	FirstFrom       int     `json:"first_from"`        // node ID that first delivered to receiver
	ReceivedDelayMS float64 `json:"received_delay_ms"` // elapsed since publish
}

type attackerEstimate struct {
	AttackerPct        float64 `json:"attacker_pct"`
	AttackerCount      int     `json:"attacker_count"`
	EstimatedSource    int     `json:"estimated_source"`
	TrueSource         int     `json:"true_source"`
	IsCorrect          bool    `json:"is_correct"`
	ObservedByAttacker int     `json:"observed_by_attacker"`
	ObservedDelayMS    float64 `json:"observed_delay_ms"`
}

type experimentResult struct {
	trials []trialResult
}

type attackerAccuracySummary struct {
	AttackerPct      float64 `json:"attacker_pct"`
	TrialsObserved   int     `json:"trials_observed"`
	CorrectEstimates int     `json:"correct_estimates"`
	Accuracy         float64 `json:"accuracy"`
}

type experimentTopology struct {
	hosts   []host.Host       // all hosts in the test
	weights [][]time.Duration // symmetric matrix of direct pair latencies
	nodeIDs []int             // original node IDs from nodes.json (index → ID)
	closeFn func()
}

type spreadExperimentExport struct {
	RunID     string `json:"run_id,omitempty"`
	GitCommit string `json:"git_commit,omitempty"`

	GeneratedAt string `json:"generated_at"`

	Nodes             int   `json:"nodes"`
	Trials            int   `json:"trials"`
	Seed              int   `json:"seed"`
	WarmupEvery       int   `json:"warmup_every"`
	WarmupPerPublish  int   `json:"warmup_rounds_per_publish"`
	LinkMiBps         int   `json:"link_mibps"`
	ScenarioTimeoutMs int   `json:"scenario_timeout_ms"`
	NodeIDs           []int `json:"node_ids"`

	Gossipsub []trialResult `json:"gossipsub"`
	Spread    []trialResult `json:"spread"`

	GossipsubAttackerAccuracy []attackerAccuracySummary `json:"gossipsub_attacker_accuracy,omitempty"`
	SpreadAttackerAccuracy    []attackerAccuracySummary `json:"spread_attacker_accuracy,omitempty"`
}

func TestSimnetSpreadVsGossipsubLatencyStretch(t *testing.T) {

	// Test parameters
	nodeCount := envInt("SPREAD_SIMNET_NODES", SPREAD_SIMNET_NODES)
	trials := envInt("SPREAD_SIMNET_TRIALS", SPREAD_SIMNET_TRIALS)
	seed := int64(envInt("SPREAD_SIMNET_SEED", SPREAD_SIMNET_SEED))
	startTrial := envInt(SPREAD_SIMNET_START_TRIAL_ENV, 0)

	// Run gossipsub
	gossipsubStartTime := time.Now()
	gsTopo := makeEthereumLikeTopology(t, nodeCount, seed)
	defer gsTopo.closeFn()
	gsRes := runScenario(t, gsTopo, scenarioConfig{
		name:             "gossipsub",
		trials:           trials,
		startTrial:       startTrial,
		useSpread:        false,
		warmupEvery:      0,
		warmupPerPublish: 0,
		enableCrash:      envBool(SPREAD_SIMNET_ENABLE_CRASH_ENV, false),
		crashPct:         envFloat(SPREAD_SIMNET_CRASH_PCT_ENV, 0),
	})
	t.Logf("gossipsub: %d trials completed in %s", trials, time.Since(gossipsubStartTime))

	// Run spread
	spreadStartTime := time.Now()
	spreadTopo := makeEthereumLikeTopology(t, nodeCount, seed)
	defer spreadTopo.closeFn()
	spreadRes := runScenario(t, spreadTopo, scenarioConfig{
		name:             "spread",
		trials:           trials,
		startTrial:       startTrial,
		useSpread:        true,
		warmupEvery:      envInt("SPREAD_SIMNET_WARMUP_EVERY", SPREAD_SIMNET_WARMUP_EVERY),
		warmupPerPublish: envInt("SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH", SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH),
		enableCrash:      envBool(SPREAD_SIMNET_ENABLE_CRASH_ENV, false),
		crashPct:         envFloat(SPREAD_SIMNET_CRASH_PCT_ENV, 0),
	})
	t.Logf("spread: %d trials completed in %s", trials, time.Since(spreadStartTime))

	t.Logf("gossipsub: %d raw observations", countDeliveries(gsRes))
	t.Logf("spread:    %d raw observations", countDeliveries(spreadRes))

	if exportPath, err := maybeWriteSpreadExperimentExport(gsRes, spreadRes, gsTopo.nodeIDs, nodeCount, trials, int(seed)); err != nil {
		t.Fatalf("write %s: %v", SPREAD_SIMNET_EXPORT_PATH_ENV, err)
	} else if exportPath != "" {
		t.Logf("wrote spread experiment export to %s", exportPath)
	}
}

func countDeliveries(res experimentResult) int {
	n := 0
	for _, tr := range res.trials {
		n += len(tr.Deliveries)
	}
	return n
}

type scenarioConfig struct {
	name             string
	trials           int
	startTrial       int
	useSpread        bool
	warmupEvery      int
	warmupPerPublish int
	enableCrash      bool
	crashPct         float64
}

func runScenario(t *testing.T, topo experimentTopology, cfg scenarioConfig) experimentResult {
	t.Helper()
	// Set timer for scenario
	ctx, cancel := context.WithTimeout(context.Background(), SPREAD_SIMNET_SCENARIO_TIMEOUT)
	defer cancel()

	startTimeNetBuild := time.Now()

	// Base gossipsub options
	baseOpts := []Option{
		WithGossipSubParams(func() GossipSubParams {
			p := DefaultGossipSubParams()
			p.HeartbeatInitialDelay = 300 * time.Millisecond
			p.HeartbeatInterval = 1000 * time.Millisecond
			return p
		}()),
	}

	// Build pubsubs
	psubs := make([]*PubSub, 0, len(topo.hosts))
	for _, h := range topo.hosts {
		opts := append([]Option{}, baseOpts...)
		// Add spread options if set
		if cfg.useSpread {
			vsvc := vivaldi.NewService(h, &vivaldi.Config{Timeout: 500 * time.Millisecond})
			opts = append(opts,
				WithProtocolChoice(SPREAD),
				withSpreadExtensionAdvertise(),
				WithSpreadClusteringConfig(spreadClusteringConfigFromEnv()),
				WithSpreadPropagationConfig(spreadPropagationConfigFromEnv()),
				WithVivaldi(vsvc, spreadVivaldiConfigFromEnv()),
			)
		}
		psubs = append(psubs, getGossipsub(ctx, h, opts...))
	}

	// Join topics and subscribe
	topics := make([]*Topic, len(psubs))
	subs := make([]*Subscription, len(psubs))
	for i, ps := range psubs {
		topic, err := ps.Join(experimentTopic)
		if err != nil {
			t.Fatalf("%s: join topic[%d]: %v", cfg.name, i, err)
		}
		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatalf("%s: subscribe topic[%d]: %v", cfg.name, i, err)
		}
		topics[i] = topic
		subs[i] = sub
	}

	// Connect all nodes
	connectAll(t, topo.hosts)

	t.Logf("%s: completed setup in %s, starting trials", cfg.name, time.Since(startTimeNetBuild))

	time.Sleep(2 * time.Second)

	alive := make([]int, 0, len(psubs))
	for i := range psubs {
		alive = append(alive, i)
	}
	if cfg.enableCrash && cfg.crashPct > 0 {
		if cfg.crashPct > 1 {
			t.Fatalf("%s: crash pct must be in [0,1], got %.4f", cfg.name, cfg.crashPct)
		}
		crashCount := int(math.Ceil(cfg.crashPct * float64(len(psubs))))
		if crashCount >= len(psubs) {
			crashCount = len(psubs) - 1
		}
		if crashCount > 0 {
			rngCrash := rand.New(rand.NewSource(int64(envInt("SPREAD_SIMNET_SEED", SPREAD_SIMNET_SEED) + 2027)))
			perm := rngCrash.Perm(len(psubs))
			crashed := make(map[int]struct{}, crashCount)
			for _, idx := range perm[:crashCount] {
				crashed[idx] = struct{}{}
				_ = topics[idx].Close()
				subs[idx].Cancel()
				_ = topo.hosts[idx].Close()
			}
			alive = alive[:0]
			for i := range psubs {
				if _, ok := crashed[i]; !ok {
					alive = append(alive, i)
				}
			}
			t.Logf("%s: crashed %d/%d nodes before dissemination (%.2f%%), alive=%d", cfg.name, crashCount, len(psubs), cfg.crashPct*100, len(alive))
		}
	}
	if len(alive) < 2 {
		t.Fatalf("%s: not enough alive nodes after crashes: %d", cfg.name, len(alive))
	}

	res := experimentResult{
		trials: make([]trialResult, 0, cfg.trials),
	}
	peerIndexByID := make(map[peer.ID]int, len(topo.hosts))
	for i, h := range topo.hosts {
		peerIndexByID[h.ID()] = i
	}
	attackerPcts := attackerPercentsFromEnv()
	seedBase := int64(envInt("SPREAD_SIMNET_SEED", SPREAD_SIMNET_SEED))

	// Run trials. Trial indices are absolute — when extending an existing run,
	// cfg.startTrial > 0 shifts the range so trial numbers stay continuous with
	// the previous export. Source selection (trial % len(alive)) picks up where
	// the previous run left off, and each trial's RNG is derived purely from
	// (seed, trial) so continuation is deterministic and independent of any
	// previously executed trials.
	for trial := cfg.startTrial; trial < cfg.startTrial+cfg.trials; trial++ {
		trialRng := rand.New(rand.NewSource(seedBase + 7919 + int64(trial)))
		// Warm-up vivaldi if configured
		if cfg.useSpread && cfg.warmupEvery > 0 && trial%cfg.warmupEvery == 0 {
			for i := 0; i < cfg.warmupPerPublish; i++ {
				warmUpSpreadVivaldiRound(t, ctx, psubs)
			}
		}

		// Select source node for this trial
		src := alive[trial%len(alive)]
		// Sample payload
		payload := []byte(fmt.Sprintf("%s-msg-%d", cfg.name, trial))
		// Channel to receive results from subscribers
		expectedReceivers := len(alive) - 1
		out := make(chan recvResult, expectedReceivers)

		// Start waiting for messages on all subscribers in parallel, except the source
		for _, i := range alive {
			if i == src {
				continue
			}
			go waitForMessage(ctx, i, subs[i], payload, peerIndexByID, out)
		}

		// Start the timer right before publish
		start := time.Now()
		if cfg.useSpread {
			if err := publishWithSpread(ctx, topics[src], payload); err != nil {
				t.Fatalf("%s: publish spread trial=%d source=%d: %v", cfg.name, trial, src, err)
			}
		} else {
			if err := topics[src].Publish(ctx, payload); err != nil {
				t.Fatalf("%s: publish gossipsub trial=%d source=%d: %v", cfg.name, trial, src, err)
			}
		}

		tr := trialResult{
			Trial:         trial,
			Src:           topo.nodeIDs[src],
			Deliveries:    make([]delivery, 0, expectedReceivers),
			FirstReceipts: make([]firstReceipt, 0, len(alive)),
		}
		tr.FirstReceipts = append(tr.FirstReceipts, firstReceipt{
			Node:            topo.nodeIDs[src],
			FirstFrom:       topo.nodeIDs[src],
			ReceivedDelayMS: 0,
		})

		// Wait for all subscribers to receive the message and collect results
		for i := 0; i < expectedReceivers; i++ {
			rr := <-out
			if rr.err != nil {
				t.Fatalf("%s: receive trial=%d source=%d: %v", cfg.name, trial, src, rr.err)
			}

			obs := rr.at.Sub(start)
			obsMS := float64(obs) / float64(time.Millisecond)

			var st float64
			base := topo.weights[src][rr.idx]
			if base > 0 {
				st = float64(obs) / float64(base)
			}

			tr.Deliveries = append(tr.Deliveries, delivery{
				Dst:       topo.nodeIDs[rr.idx],
				LatencyMS: obsMS,
				Stretch:   st,
			})
			tr.FirstReceipts = append(tr.FirstReceipts, firstReceipt{
				Node:            topo.nodeIDs[rr.idx],
				FirstFrom:       topo.nodeIDs[rr.fromIdx],
				ReceivedDelayMS: obsMS,
			})
		}
		sort.Slice(tr.FirstReceipts, func(i, j int) bool {
			return tr.FirstReceipts[i].Node < tr.FirstReceipts[j].Node
		})
		tr.AttackerEstimates = estimateSourceFromAttackers(tr.FirstReceipts, topo.nodeIDs, src, attackerPcts, trialRng)

		res.trials = append(res.trials, tr)
	}

	return res
}

func attackerPercentsFromEnv() []float64 {
	raw := strings.TrimSpace(os.Getenv(SPREAD_ATTACKER_PCTS))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]float64, 0, len(parts))
	for _, part := range parts {
		p, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil {
			continue
		}
		if p <= 0 || p > 1 {
			continue
		}
		out = append(out, p)
	}
	sort.Float64s(out)
	return out
}

func estimateSourceFromAttackers(firstReceipts []firstReceipt, nodeIDs []int, srcIdx int, attackerPcts []float64, rng *rand.Rand) []attackerEstimate {
	if len(attackerPcts) == 0 {
		return nil
	}
	receiptsByNode := make(map[int]firstReceipt, len(firstReceipts))
	for _, fr := range firstReceipts {
		receiptsByNode[fr.Node] = fr
	}
	candidates := make([]int, 0, len(nodeIDs)-1)
	for i, nid := range nodeIDs {
		if i == srcIdx {
			continue
		}
		candidates = append(candidates, nid)
	}
	if len(candidates) == 0 {
		return nil
	}
	perm := append([]int(nil), candidates...)
	rng.Shuffle(len(perm), func(i, j int) {
		perm[i], perm[j] = perm[j], perm[i]
	})
	estimates := make([]attackerEstimate, 0, len(attackerPcts))
	for _, pct := range attackerPcts {
		attackers := int(math.Ceil(pct * float64(len(candidates))))
		if attackers < 1 {
			attackers = 1
		}
		if attackers > len(candidates) {
			attackers = len(candidates)
		}
		bestFound := false
		var best firstReceipt
		for _, attackerNode := range perm[:attackers] {
			fr, ok := receiptsByNode[attackerNode]
			if !ok {
				continue
			}
			if !bestFound || fr.ReceivedDelayMS < best.ReceivedDelayMS {
				best = fr
				bestFound = true
			}
		}
		if !bestFound {
			continue
		}
		estimates = append(estimates, attackerEstimate{
			AttackerPct:        pct,
			AttackerCount:      attackers,
			EstimatedSource:    best.FirstFrom,
			TrueSource:         nodeIDs[srcIdx],
			IsCorrect:          best.FirstFrom == nodeIDs[srcIdx],
			ObservedByAttacker: best.Node,
			ObservedDelayMS:    best.ReceivedDelayMS,
		})
	}
	return estimates
}

type recvResult struct {
	idx     int // index of the receiving peer
	fromIdx int // index of the peer that first delivered this message
	at      time.Time
	err     error
}

func waitForMessage(ctx context.Context, idx int, sub *Subscription, payload []byte, peerIndexByID map[peer.ID]int, out chan<- recvResult) {
	for {
		msg, err := sub.Next(ctx)
		at := time.Now()
		if err != nil {
			out <- recvResult{err: err}
			return
		}
		// Ignore older in-flight messages and only account for the current trial payload.
		if !bytes.Equal(msg.Data, payload) {
			continue
		}
		fromIdx, ok := peerIndexByID[msg.ReceivedFrom]
		if !ok {
			out <- recvResult{err: fmt.Errorf("receiver %d: unknown ReceivedFrom peer %s", idx, msg.ReceivedFrom)}
			return
		}
		out <- recvResult{idx: idx, fromIdx: fromIdx, at: at}
		return
	}
}

func publishWithSpread(ctx context.Context, topic *Topic, data []byte) error {
	msg, err := topic.validate(ctx, data)
	if err != nil {
		return err
	}
	msg.Spread = true
	return topic.p.val.sendMsgBlocking(msg)
}

func withSpreadExtensionAdvertise() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pubsub router is not gossipsub")
		}
		gs.extensions.myExtensions.Spread = true
		return nil
	}
}

func warmUpSpreadVivaldiRound(t *testing.T, ctx context.Context, psubs []*PubSub) {
	t.Helper()

	type task struct {
		state *SpreadState
		peers []peer.ID
	}

	tasks := make([]task, 0, len(psubs))
	tasksOut := make(chan task, len(psubs))
	for _, ps := range psubs {
		psLocal := ps
		psLocal.eval <- func() {
			gs := psLocal.rt.(*GossipSubRouter)
			ss := gs.extensions.spreadState
			peers := make([]peer.ID, 0, len(ss.peers))
			for p := range ss.peers {
				peers = append(peers, p)
			}
			tasksOut <- task{state: ss, peers: peers}
		}
	}
	for i := 0; i < len(psubs); i++ {
		tasks = append(tasks, <-tasksOut)
	}

	// Parallelize warmup across nodes; keep each node's peer updates ordered.
	var wg sync.WaitGroup
	for _, tk := range tasks {
		wg.Add(1)
		go func(tk task) {
			defer wg.Done()
			for _, pid := range tk.peers {
				exCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				_, _ = tk.state.UpdatePeerVivaldi(exCtx, pid)
				cancel()
			}
		}(tk)
	}
	wg.Wait()
}

func makeEthereumLikeTopology(t *testing.T, n int, seed int64) experimentTopology {
	t.Helper()
	if n < 2 {
		t.Fatalf("node count must be >= 2, got %d", n)
	}

	startTime := time.Now()

	// Get latencies and node IDs
	weights, nodeIDs := makeEthereumLikeLatencyMatrix(t, n, seed)

	// Map IP addresses to node idx
	idxByIP := make(map[string]int, n)
	for i := 0; i < n; i++ {
		idxByIP[simnet.IntToPublicIPv4(i).String()] = i
	}

	// Create network with n nodes and with the configured latencies
	linkMiBps := envInt("SPREAD_SIMNET_LINK_MIBPS", SPREAD_SIMNET_LINK_MIBPS)
	network, meta, err := simlibp2p.SimpleLibp2pNetwork([]simlibp2p.NodeLinkSettingsAndCount{
		{
			LinkSettings: simnet.NodeBiDiLinkSettings{
				Downlink: simnet.LinkSettings{BitsPerSecond: linkMiBps * simlibp2p.OneMbps, MTU: 1400},
				Uplink:   simnet.LinkSettings{BitsPerSecond: linkMiBps * simlibp2p.OneMbps, MTU: 1400},
			},
			Count: n,
		},
	}, func(p *simnet.Packet) time.Duration {
		from, fok := p.From.(*net.UDPAddr)
		to, tok := p.To.(*net.UDPAddr)
		if !fok || !tok {
			return 0
		}
		a, aok := idxByIP[from.IP.String()]
		b, bok := idxByIP[to.IP.String()]
		if !aok || !bok || a == b {
			return 0
		}
		return weights[a][b]
	}, simlibp2p.NetworkSettings{UseBlankHost: true})
	if err != nil {
		t.Fatalf("build simlibp2p network: %v", err)
	}

	// Start network
	network.Start()

	// Extract hosts
	hosts := make([]host.Host, 0, len(meta.Nodes))
	for _, h := range meta.Nodes {
		hosts = append(hosts, h)
	}

	t.Logf("created simnet topology with %d nodes in %s", n, time.Since(startTime))

	return experimentTopology{
		hosts:   hosts,
		weights: weights,
		nodeIDs: nodeIDs,
		closeFn: func() {
			network.Close()
			for _, h := range meta.Nodes {
				_ = h.Close()
			}
		},
	}
}

func makeEthereumLikeLatencyMatrix(t *testing.T, n int, seed int64) ([][]time.Duration, []int) {
	t.Helper()

	type nodeEntry struct {
		ID        int     `json:"id"`
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
	}

	type pingEntry struct {
		Source      int     `json:"source"`
		Destination int     `json:"destination"`
		Min         float64 `json:"min"`
		Avg         float64 `json:"avg"`
		Max         float64 `json:"max"`
		Mdev        float64 `json:"mdev"`
	}

	nodesFile, err := os.Open("spread_data/nodes.json")
	if err != nil {
		t.Fatalf("open spread_data/nodes.json: %v", err)
	}
	defer nodesFile.Close()

	var allNodes []nodeEntry
	if err := json.NewDecoder(nodesFile).Decode(&allNodes); err != nil {
		t.Fatalf("decode spread_data/nodes.json: %v", err)
	}
	if len(allNodes) < n {
		t.Fatalf("not enough nodes in spread_data/nodes.json: need %d, have %d", n, len(allNodes))
	}

	rng := rand.New(rand.NewSource(seed))
	indices := make([]int, len(allNodes))
	for i := range indices {
		indices[i] = i
	}
	rng.Shuffle(len(indices), func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})
	selected := indices[:n]

	nodeIDs := make([]int, n)
	idToIdx := make(map[int]int, n)
	for i, idx := range selected {
		nodeIDs[i] = allNodes[idx].ID
		idToIdx[allNodes[idx].ID] = i
	}

	weights := make([][]time.Duration, n)
	for i := range weights {
		weights[i] = make([]time.Duration, n)
	}

	pingsFile, err := os.Open("spread_data/pings.json.gz")
	if err != nil {
		t.Fatalf("open spread_data/pings.json.gz: %v", err)
	}
	defer pingsFile.Close()

	gzr, err := gzip.NewReader(pingsFile)
	if err != nil {
		t.Fatalf("create gzip reader for spread_data/pings.json.gz: %v", err)
	}
	defer gzr.Close()

	dec := json.NewDecoder(gzr)
	// Consume opening '[' of the JSON array in the gzip stream.
	if _, err := dec.Token(); err != nil {
		t.Fatalf("decode spread_data/pings.json.gz: %v", err)
	}

	for dec.More() {
		var pe pingEntry
		if err := dec.Decode(&pe); err != nil {
			t.Fatalf("decode ping entry from spread_data/pings.json: %v", err)
		}

		a, okA := idToIdx[pe.Source]
		b, okB := idToIdx[pe.Destination]
		if !okA || !okB || a == b {
			continue
		}

		// Use half of the average RTT (ms) as an approximation of one-way latency.
		if pe.Avg <= 0 {
			continue
		}
		oneWayMs := pe.Avg / 2.0
		lat := time.Duration(math.Max(1, oneWayMs) * float64(time.Millisecond))

		// If multiple samples exist for a pair, keep the minimum latency.
		if weights[a][b] == 0 || lat < weights[a][b] {
			weights[a][b] = lat
			weights[b][a] = lat
		}
	}

	return weights, nodeIDs
}

func buildSpreadExperimentExport(gsRes, spreadRes experimentResult, nodeIDs []int, nodeCount, trials, seed int) spreadExperimentExport {
	return spreadExperimentExport{
		GeneratedAt:               time.Now().UTC().Format(time.RFC3339Nano),
		Nodes:                     nodeCount,
		Trials:                    trials,
		Seed:                      seed,
		WarmupEvery:               envInt("SPREAD_SIMNET_WARMUP_EVERY", SPREAD_SIMNET_WARMUP_EVERY),
		WarmupPerPublish:          envInt("SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH", SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH),
		LinkMiBps:                 envInt("SPREAD_SIMNET_LINK_MIBPS", SPREAD_SIMNET_LINK_MIBPS),
		ScenarioTimeoutMs:         int(SPREAD_SIMNET_SCENARIO_TIMEOUT / time.Millisecond),
		NodeIDs:                   nodeIDs,
		Gossipsub:                 gsRes.trials,
		Spread:                    spreadRes.trials,
		GossipsubAttackerAccuracy: summarizeAttackerAccuracy(gsRes),
		SpreadAttackerAccuracy:    summarizeAttackerAccuracy(spreadRes),
	}
}

func summarizeAttackerAccuracy(res experimentResult) []attackerAccuracySummary {
	type tally struct {
		trials  int
		correct int
	}
	byPct := make(map[float64]*tally)
	for _, tr := range res.trials {
		for _, est := range tr.AttackerEstimates {
			t := byPct[est.AttackerPct]
			if t == nil {
				t = &tally{}
				byPct[est.AttackerPct] = t
			}
			t.trials++
			if est.IsCorrect {
				t.correct++
			}
		}
	}
	if len(byPct) == 0 {
		return nil
	}
	pcts := make([]float64, 0, len(byPct))
	for pct := range byPct {
		pcts = append(pcts, pct)
	}
	sort.Float64s(pcts)
	out := make([]attackerAccuracySummary, 0, len(pcts))
	for _, pct := range pcts {
		t := byPct[pct]
		acc := 0.0
		if t.trials > 0 {
			acc = float64(t.correct) / float64(t.trials)
		}
		out = append(out, attackerAccuracySummary{
			AttackerPct:      pct,
			TrialsObserved:   t.trials,
			CorrectEstimates: t.correct,
			Accuracy:         acc,
		})
	}
	return out
}

func writeSpreadExperimentExport(path string, doc spreadExperimentExport) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create export dir %q: %w", dir, err)
	}

	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal export doc: %w", err)
	}
	b = append(b, '\n')

	return writeAtomicFile(path, b, 0o644)
}

func maybeWriteSpreadExperimentExport(gsRes, spreadRes experimentResult, nodeIDs []int, nodeCount, trials, seed int) (string, error) {
	exportPath := os.Getenv(SPREAD_SIMNET_EXPORT_PATH_ENV)
	if exportPath == "" {
		return "", nil
	}
	doc := buildSpreadExperimentExport(gsRes, spreadRes, nodeIDs, nodeCount, trials, seed)
	doc.RunID = os.Getenv(SPREAD_SIMNET_RUN_ID_ENV)
	doc.GitCommit = os.Getenv(SPREAD_SIMNET_GIT_COMMIT_ENV)
	return exportPath, writeSpreadExperimentExport(exportPath, doc)
}

func writeAtomicFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	tmp := filepath.Join(dir, "."+base+".tmp")

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return fmt.Errorf("open temp file %q: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("write temp file %q: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync temp file %q: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp file %q: %w", tmp, err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %q -> %q: %w", tmp, path, err)
	}

	// Sync parent directory to persist the rename operation.
	df, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open parent dir %q: %w", dir, err)
	}
	if err := df.Sync(); err != nil {
		_ = df.Close()
		return fmt.Errorf("sync parent dir %q: %w", dir, err)
	}
	if err := df.Close(); err != nil {
		return fmt.Errorf("close parent dir %q: %w", dir, err)
	}
	return nil
}

func spreadClusteringConfigFromEnv() *SpreadClusteringConfig {
	return &SpreadClusteringConfig{
		ClusterPct: envFloat(SPREAD_CLUSTER_PCT, 0.25),
		NumRings:   envInt(SPREAD_NUM_RINGS, 4),
	}
}

func spreadPropagationConfigFromEnv() *SpreadConfig {
	return &SpreadConfig{
		IntraFanout:            envInt(SPREAD_INTRA_FANOUT, DefaultSpreadIntraFanout),
		InterFanout:            envInt(SPREAD_INTER_FANOUT, DefaultSpreadInterFanout),
		IntraRho:               envFloat(SPREAD_INTRA_RHO, DefaultSpreadIntraRho),
		InterProb:              envFloat(SPREAD_INTER_PROB, DefaultSpreadInterProb),
		FallbackThreshold:      envInt(SPREAD_FALLBACK_THRESHOLD, DefaultSpreadFallbackMin),
		DuplicateRepropagation: envInt(SPREAD_DUPLICATE_REPROPAGATION, 5),
		UseAngularInterPeers:   envBool(SPREAD_USE_ANGULAR_INTER_PEERS, false),
	}
}

func spreadVivaldiConfigFromEnv() *VivaldiConfig {
	return &VivaldiConfig{
		Cc:               envFloat(SPREAD_CC, 0.25),
		Ce:               envFloat(SPREAD_CE, 0.25),
		Newton:           envBool(SPREAD_NEWTON, false),
		OutlierThreshold: envFloat(SPREAD_OUTLIER_THRESHOLD, 0),
		Samples:          envInt(SPREAD_SAMPLES, 1),
		Interval:         time.Duration(envInt(SPREAD_INTERVAL_MS, 150)) * time.Millisecond,
		NeighborSetSize:  envInt(SPREAD_NEIGHBOR_SET_SIZE, 24),
		IN1ThresholdMS:   envFloat(SPREAD_IN1_THRESHOLD_MS, 20),
		IN2ThresholdMS:   envFloat(SPREAD_IN2_THRESHOLD_MS, 35),
		IN3MADKRandom:    envFloat(SPREAD_IN3_MADK_RANDOM, 5),
		IN3MADKClose:     envFloat(SPREAD_IN3_MADK_CLOSE, 8),
		IN3MinSamples:    envInt(SPREAD_IN3_MIN_SAMPLES, 4),
	}
}

func envInt(key string, def int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return v
}

func envFloat(key string, def float64) float64 {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return def
	}
	return v
}

func envBool(key string, def bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return def
	}
	return v
}
