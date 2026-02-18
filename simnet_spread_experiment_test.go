// Simnet experiment
// Command: RUN_SPREAD_SIMNET_EXPERIMENT=1 go test -tags simnet -run TestSimnetSpreadVsGossipsubLatencyStretch -v .
// Useful env vars:
// - RUN_SPREAD_SIMNET_EXPERIMENT=1
// - SPREAD_SIMNET_NODES (default 30)
// - SPREAD_SIMNET_TRIALS (default 60)
// - SPREAD_SIMNET_WINDOW_SIZE (default 10)
// - SPREAD_SIMNET_WARMUP_EVERY (default 1)
// - SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH (default 1)
// - SPREAD_SIMNET_SEED (default 1337)

package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/vivaldi"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	simnet "github.com/marcopolo/simnet"
)

const experimentTopic = "spread-vs-gossipsub-simnet"

const (
	SPREAD_SIMNET_NODES                     = 30
	SPREAD_SIMNET_TRIALS                    = 60
	SPREAD_SIMNET_WINDOW_SIZE               = 10
	SPREAD_SIMNET_WARMUP_EVERY              = 1
	SPREAD_SIMNET_WARMUP_ROUNDS_PER_PUBLISH = 1
	SPREAD_SIMNET_SEED                      = 1337
)

type experimentResult struct {
	latencyMS       []float64
	stretch         []float64
	windowLatencyMS map[int][]float64
	windowStretch   map[int][]float64
}

type experimentTopology struct {
	hosts   []host.Host
	edges   map[int][]int     // adjacency list
	weights [][]time.Duration // symmetric matrix of edge weights (latencies)
	closeFn func()
}

func TestSimnetSpreadVsGossipsubLatencyStretch(t *testing.T) {
	if os.Getenv("RUN_SPREAD_SIMNET_EXPERIMENT") != "1" {
		t.Skip("set RUN_SPREAD_SIMNET_EXPERIMENT=1 to run this experiment")
	}

	// Test parameters
	nodeCount := envInt("SPREAD_SIMNET_NODES", SPREAD_SIMNET_NODES)
	trials := envInt("SPREAD_SIMNET_TRIALS", SPREAD_SIMNET_TRIALS)
	windowSize := envInt("SPREAD_SIMNET_WINDOW_SIZE", SPREAD_SIMNET_WINDOW_SIZE)
	seed := int64(envInt("SPREAD_SIMNET_SEED", SPREAD_SIMNET_SEED))

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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	baseOpts := []Option{
		WithGossipSubParams(func() GossipSubParams {
			p := DefaultGossipSubParams()
			p.HeartbeatInitialDelay = 300 * time.Millisecond
			p.HeartbeatInterval = 1000 * time.Millisecond
			return p
		}()),
	}

	psubs := make([]*PubSub, 0, len(topo.hosts))
	for _, h := range topo.hosts {
		opts := append([]Option{}, baseOpts...)
		if cfg.useSpread {
			vsvc := vivaldi.NewService(h, &vivaldi.Config{Timeout: 500 * time.Millisecond})
			opts = append(opts,
				WithProtocolChoice(SPREAD),
				withSpreadExtensionAdvertise(),
				WithVivaldi(vsvc, &VivaldiConfig{
					Cc:               0.25,
					Ce:               0.25,
					Newton:           true,
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

	time.Sleep(2 * time.Second)

	res := experimentResult{
		latencyMS:       make([]float64, 0, cfg.trials*(len(psubs)-1)),
		stretch:         make([]float64, 0, cfg.trials*(len(psubs)-1)),
		windowLatencyMS: make(map[int][]float64),
		windowStretch:   make(map[int][]float64),
	}

	for trial := 0; trial < cfg.trials; trial++ {
		if cfg.useSpread && cfg.warmupEvery > 0 && trial%cfg.warmupEvery == 0 {
			for i := 0; i < cfg.warmupPerPublish; i++ {
				warmUpSpreadVivaldiRound(t, ctx, psubs)
			}
		}

		src := trial % len(psubs)
		payload := []byte(fmt.Sprintf("%s-msg-%d", cfg.name, trial))
		out := make(chan recvResult, len(psubs)-1)

		for i := range psubs {
			if i == src {
				continue
			}
			go waitForMessage(i, subs[i], payload, out)
		}

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

		shortest := shortestPathDurations(src, topo.edges, topo.weights)
		window := trial / max(1, cfg.windowSize)

		for i := 0; i < len(psubs)-1; i++ {
			rr := <-out
			if rr.err != nil {
				t.Fatalf("%s: receive trial=%d source=%d: %v", cfg.name, trial, src, rr.err)
			}

			obs := rr.at.Sub(start)
			obsMS := float64(obs) / float64(time.Millisecond)
			res.latencyMS = append(res.latencyMS, obsMS)
			res.windowLatencyMS[window] = append(res.windowLatencyMS[window], obsMS)

			base := shortest[rr.idx]
			if base <= 0 || base >= infDuration {
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
	idx int
	at  time.Time
	err error
}

func waitForMessage(idx int, sub *Subscription, payload []byte, out chan<- recvResult) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

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
	for _, ps := range psubs {
		done := make(chan task, 1)
		psLocal := ps
		psLocal.eval <- func() {
			gs := psLocal.rt.(*GossipSubRouter)
			ss := gs.extensions.spreadState
			peers := make([]peer.ID, 0, len(ss.peers))
			for p := range ss.peers {
				peers = append(peers, p)
			}
			done <- task{state: ss, peers: peers}
		}
		tasks = append(tasks, <-done)
	}

	for _, tk := range tasks {
		for _, pid := range tk.peers {
			exCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			_, _ = tk.state.UpdatePeerVivaldi(exCtx, pid)
			cancel()
		}
	}
}

func makeEthereumLikeTopology(t *testing.T, n int, seed int64) experimentTopology {
	t.Helper()
	if n < 6 {
		t.Fatalf("node count must be >= 6, got %d", n)
	}

	weights, closeSimnet := makeEthereumLikeLatencyMatrix(t, n, seed)

	hosts := getDefaultHosts(t, n)
	edges := make(map[int][]int, n)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			addUndirectedEdge(edges, i, j)
		}
	}
	connectAll(t, hosts)

	return experimentTopology{
		hosts:   hosts,
		edges:   edges,
		weights: weights,
		closeFn: closeSimnet,
	}
}

func makeEthereumLikeLatencyMatrix(t *testing.T, n int, seed int64) ([][]time.Duration, func()) {
	t.Helper()
	// Regions
	type region int
	const (
		usEast region = iota
		usWest
		europe
		asia
		southAmerica
		oceania
	)
	// Approximate median one-way latencies (ms) between regions.
	// We can use real-world latency data through the sources:
	// - https://www.samnet.dev/tools/global-latency/index.html
	// - https://www.cloudping.info/
	// - https://cloudping.net/
	// - https://wondernetwork.com/pings
	// - https://www.cloudping.co/
	// - https://www.economize.cloud/resources/aws/latency
	// - https://dashboard.pipenetwork.com/network-latency
	// We used cloudping with the filter:
	// - usEast → us-east-1 (N. Virginia)
	// - usWest → us-west-2 (Oregon)
	// - Europe → eu-west-1 (Ireland)
	// - Asia → ap-southeast-1 (Singapore)
	// - SouthAmerica → sa-east-1 (São Paulo)
	// - Oceania → ap-southeast-2 (Sydney)
	// Table:
	// from \ to | usEast | usWest | europe | asia | southAmerica | oceania
	// usEast 	 | 1.36   | 64     | 67    | 215  | 113          | 199
	// usWest 	 | 57	 | 8.5    | 118    | 168 | 174 | 140
	// europe 	 | 67    | 123    | 1.57   | 175 | 176 | 255
	// asia 	 | 216 | 177 | 177 | 1.06 | 325 | 94
	// southAmerica | 114 | 178 | 176 | 325 | 1.58 | 311
	// Oceania  | 200 | 148 | 255 | 94 | 311 | 1.05
	base := map[region]map[region]float64{
		usEast:       {usEast: 1.36, usWest: 64, europe: 67, asia: 215, southAmerica: 113, oceania: 199},
		usWest:       {usEast: 57, usWest: 8.5, europe: 118, asia: 168, southAmerica: 174, oceania: 140},
		europe:       {usEast: 67, usWest: 123, europe: 1.57, asia: 175, southAmerica: 176, oceania: 255},
		asia:         {usEast: 216, usWest: 177, europe: 177, asia: 1.06, southAmerica: 325, oceania: 94},
		southAmerica: {usEast: 114, usWest: 178, europe: 176, asia: 325, southAmerica: 1.58, oceania: 311},
		oceania:      {usEast: 200, usWest: 148, europe: 255, asia: 94, southAmerica: 311, oceania: 1.05},
	}

	rng := rand.New(rand.NewSource(seed))
	// Regions with duplication weighted to increase the likelihood of regions
	regionDist := []region{usEast, usEast, usWest, usWest, europe, europe, asia, asia, southAmerica, oceania}
	nodeRegion := make([]region, n)
	for i := 0; i < n; i++ {
		nodeRegion[i] = regionDist[rng.Intn(len(regionDist))]
	}

	weights := make([][]time.Duration, n)
	for i := range weights {
		weights[i] = make([]time.Duration, n)
	}
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			m := base[nodeRegion[i]][nodeRegion[j]]
			jitter := (rng.Float64()*0.20 - 0.10) * m // +/-10%
			latMs := math.Max(2, m+jitter)
			lat := time.Duration(latMs * float64(time.Millisecond))
			weights[i][j] = lat
			weights[j][i] = lat
		}
	}

	// Build a real simnet object with the same matrix for aligned future extensions.
	sn := &simnet.Simnet{}
	settings := simnet.NodeBiDiLinkSettings{
		Downlink: simnet.LinkSettings{BitsPerSecond: 1000 * simnet.Mibps, MTU: 1400},
		Uplink:   simnet.LinkSettings{BitsPerSecond: 1000 * simnet.Mibps, MTU: 1400},
	}
	addrToIdx := make(map[string]int, n)
	addrs := make([]*net.UDPAddr, n)
	for i := 0; i < n; i++ {
		addrs[i] = &net.UDPAddr{IP: net.ParseIP(fmt.Sprintf("10.0.0.%d", i+1)), Port: 10000 + i}
		addrToIdx[addrs[i].String()] = i
		sn.NewEndpoint(addrs[i], settings)
	}
	sn.LatencyFunc = func(p *simnet.Packet) time.Duration {
		a, aok := addrToIdx[p.From.String()]
		b, bok := addrToIdx[p.To.String()]
		if !aok || !bok || a == b {
			return 0
		}
		return weights[a][b]
	}
	sn.Start()

	return weights, func() { sn.Close() }
}

func addUndirectedEdge(edges map[int][]int, a, b int) {
	if a == b {
		return
	}
	if !containsInt(edges[a], b) {
		edges[a] = append(edges[a], b)
	}
	if !containsInt(edges[b], a) {
		edges[b] = append(edges[b], a)
	}
}

func containsInt(xs []int, x int) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

const infDuration = time.Duration(math.MaxInt64 / 4)

// shortestPathDurations computes the shortest path durations from src to all other nodes using Dijkstra's algorithm.
func shortestPathDurations(src int, edges map[int][]int, weights [][]time.Duration) []time.Duration {
	n := len(weights)
	dist := make([]time.Duration, n)
	used := make([]bool, n)
	for i := range dist {
		dist[i] = infDuration
	}
	dist[src] = 0

	for {
		u := -1
		best := infDuration
		for i := 0; i < n; i++ {
			if !used[i] && dist[i] < best {
				best = dist[i]
				u = i
			}
		}
		if u == -1 {
			break
		}
		used[u] = true
		for _, v := range edges[u] {
			w := weights[u][v]
			if w <= 0 || dist[u] == infDuration {
				continue
			}
			cand := dist[u] + w
			if cand < dist[v] {
				dist[v] = cand
			}
		}
	}
	return dist
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
