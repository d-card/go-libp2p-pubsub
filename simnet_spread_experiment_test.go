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
	"sort"
	"strconv"
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
	SPREAD_SIMNET_NODES                     = 20               // Number of nodes
	SPREAD_SIMNET_TRIALS                    = 500                // Number of messages published
	SPREAD_SIMNET_WINDOW_SIZE               = 10                // Number of trials to aggregate in the same window (for understanding temporal evolution of metrics due to vivaldi warmup)
	SPREAD_SIMNET_WARMUP_EVERY              = 0                // Number of trials between warm-up rounds of vivaldi
	SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH = 1                // Number of vivaldi rounds to warm up before each publish
	SPREAD_SIMNET_SEED                      = 1337             // Random seed for topology generation (latencies and node regions)
	SPREAD_SIMNET_LINK_MIBPS                = 20               // Link bandwidth in MiB/s for all links in the simnet topology
	SPREAD_SIMNET_SCENARIO_TIMEOUT          = 10 * time.Minute // Overall timeout for each scenario
)

type experimentResult struct {
	latencyMS       []float64         // latency in milliseconds for each received message
	stretch         []float64         // stretch for each received message
	windowLatencyMS map[int][]float64 // latency in milliseconds for each received message, grouped by window
	windowStretch   map[int][]float64 // stretch for each received message, grouped by window
}

type experimentTopology struct {
	hosts   []host.Host       // all hosts in the test
	weights [][]time.Duration // symmetric matrix of direct pair latencies
	closeFn func()
}

func TestSimnetSpreadVsGossipsubLatencyStretch(t *testing.T) {

	// Test parameters
	nodeCount := envInt("SPREAD_SIMNET_NODES", SPREAD_SIMNET_NODES)
	trials := envInt("SPREAD_SIMNET_TRIALS", SPREAD_SIMNET_TRIALS)
	windowSize := envInt("SPREAD_SIMNET_WINDOW_SIZE", SPREAD_SIMNET_WINDOW_SIZE)
	seed := int64(envInt("SPREAD_SIMNET_SEED", SPREAD_SIMNET_SEED))

	// Run gossipsub
	gossipsubStartTime := time.Now()
	gsTopo := makeEthereumLikeTopology(t, nodeCount, seed)
	defer gsTopo.closeFn()
	gsRes := runScenario(t, gsTopo, scenarioConfig{
		name:             "gossipsub",
		trials:           trials,
		windowSize:       windowSize,
		useSpread:        false,
		warmupEvery:      0,
		warmupPerPublish: 0,
	})
	t.Logf("gossipsub: full scenario completed in %s", time.Since(gossipsubStartTime))

	// Run spread
	spreadStartTime := time.Now()
	spreadTopo := makeEthereumLikeTopology(t, nodeCount, seed)
	defer spreadTopo.closeFn()
	spreadRes := runScenario(t, spreadTopo, scenarioConfig{
		name:             "spread",
		trials:           trials,
		windowSize:       windowSize,
		useSpread:        true,
		warmupEvery:      envInt("SPREAD_SIMNET_WARMUP_EVERY", SPREAD_SIMNET_WARMUP_EVERY),
		warmupPerPublish: envInt("SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH", SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH),
	})
	t.Logf("spread: full scenario completed in %s", time.Since(spreadStartTime))

	// Print results
	t.Logf("overall gossipsub: latency p50=%.2fms p95=%.2fms; stretch p50=%.3f p95=%.3f",
		percentile(gsRes.latencyMS, 0.50), percentile(gsRes.latencyMS, 0.95),
		percentile(gsRes.stretch, 0.50), percentile(gsRes.stretch, 0.95))
	t.Logf("overall spread: latency p50=%.2fms p95=%.2fms; stretch p50=%.3f p95=%.3f",
		percentile(spreadRes.latencyMS, 0.50), percentile(spreadRes.latencyMS, 0.95),
		percentile(spreadRes.stretch, 0.50), percentile(spreadRes.stretch, 0.95))

	logWindowStats(t, "gossipsub", gsRes, windowSize)
	logWindowStats(t, "spread", spreadRes, windowSize)
}

type scenarioConfig struct {
	name             string
	trials           int
	windowSize       int
	useSpread        bool
	warmupEvery      int
	warmupPerPublish int
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
				WithSpreadClusteringConfig(&SpreadClusteringConfig{
					ClusterPct: 0.25,
					NumRings:   4, // Not used
				}),
				WithSpreadPropagationConfig(&SpreadConfig{
					IntraFanout:            DefaultSpreadIntraFanout,
					InterFanout:            DefaultSpreadInterFanout,
					IntraRho:               DefaultSpreadIntraRho,
					InterProb:              DefaultSpreadInterProb,
					FallbackThreshold:      DefaultSpreadFallbackMin,
					DuplicateRepropagation: 5,
				}),
				WithVivaldi(vsvc, &VivaldiConfig{
					Cc:               0.25,
					Ce:               0.25,
					Newton:           false,
					OutlierThreshold: 0,
					Samples:          1,
					Interval:         150 * time.Millisecond,
					NeighborSetSize:  24,
					IN1ThresholdMS:   20,
					IN2ThresholdMS:   35,
					IN3MADKRandom:    5,
					IN3MADKClose:     8,
					IN3MinSamples:    4,
				}),
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

	res := experimentResult{
		latencyMS:       make([]float64, 0, cfg.trials*(len(psubs)-1)),
		stretch:         make([]float64, 0, cfg.trials*(len(psubs)-1)),
		windowLatencyMS: make(map[int][]float64),
		windowStretch:   make(map[int][]float64),
	}

	// Run trials
	for trial := 0; trial < cfg.trials; trial++ {
		// Warm-up vivaldi if configured
		if cfg.useSpread && cfg.warmupEvery > 0 && trial%cfg.warmupEvery == 0 {
			for i := 0; i < cfg.warmupPerPublish; i++ {
				warmUpSpreadVivaldiRound(t, ctx, psubs)
			}
		}

		// Select source node for this trial
		src := trial % len(psubs)
		// Sample payload
		payload := []byte(fmt.Sprintf("%s-msg-%d", cfg.name, trial))
		// Channel to receive results from subscribers
		out := make(chan recvResult, len(psubs)-1)

		// Start waiting for messages on all subscribers in parallel, except the source
		for i := range psubs {
			if i == src {
				continue
			}
			go waitForMessage(ctx, i, subs[i], payload, out)
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

		// Compute current window for results aggregation
		window := trial / max(1, cfg.windowSize)

		// Wait for all subscribers to receive the message and collect results
		for i := 0; i < len(psubs)-1; i++ {
			rr := <-out
			if rr.err != nil {
				t.Fatalf("%s: receive trial=%d source=%d: %v", cfg.name, trial, src, rr.err)
			}

			// Calculate latency as: recvResult.at - publish start time
			obs := rr.at.Sub(start)
			obsMS := float64(obs) / float64(time.Millisecond)
			res.latencyMS = append(res.latencyMS, obsMS)
			res.windowLatencyMS[window] = append(res.windowLatencyMS[window], obsMS)

			// Compute stretch as: observed latency / direct pair latency between source and receiver
			base := topo.weights[src][rr.idx]
			if base <= 0 {
				continue
			}
			st := float64(obs) / float64(base)
			res.stretch = append(res.stretch, st)
			res.windowStretch[window] = append(res.windowStretch[window], st)
		}
	}

	return res
}

type recvResult struct {
	idx int // index of the receiving peer
	at  time.Time
	err error
}

func waitForMessage(ctx context.Context, idx int, sub *Subscription, payload []byte, out chan<- recvResult) {
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
		out <- recvResult{idx: idx, at: at}
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

	// Get latencies
	weights := makeEthereumLikeLatencyMatrix(t, n, seed)

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
		closeFn: func() {
			network.Close()
			for _, h := range meta.Nodes {
				_ = h.Close()
			}
		},
	}
}

func makeEthereumLikeLatencyMatrix(t *testing.T, n int, seed int64) [][]time.Duration {
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

	idToIdx := make(map[int]int, n)
	for i, idx := range selected {
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

	return weights
}

func logWindowStats(t *testing.T, name string, res experimentResult, windowSize int) {
	windows := make([]int, 0, len(res.windowLatencyMS))
	for w := range res.windowLatencyMS {
		windows = append(windows, w)
	}
	sort.Ints(windows)
	for _, w := range windows {
		lats := res.windowLatencyMS[w]
		st := res.windowStretch[w]
		t.Logf("%s window=%d trials=%d..%d latency p50=%.2fms p95=%.2fms stretch p50=%.3f p95=%.3f",
			name, w, w*windowSize, (w+1)*windowSize-1,
			percentile(lats, 0.50), percentile(lats, 0.95),
			percentile(st, 0.50), percentile(st, 0.95),
		)
	}
}

func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	cpy := append([]float64(nil), xs...)
	sort.Float64s(cpy)
	if p <= 0 {
		return cpy[0]
	}
	if p >= 1 {
		return cpy[len(cpy)-1]
	}
	idx := int(math.Ceil(p*float64(len(cpy)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cpy) {
		idx = len(cpy) - 1
	}
	return cpy[idx]
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
