package pubsub

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/vivaldi"
	"github.com/libp2p/go-libp2p/core/peer"
)

type SpreadState struct {
	lk sync.RWMutex
	// peers that advertise SPREAD
	peers map[peer.ID]struct{}
	// topic -> peers with spread extension that are subscribed to the topic
	topics map[string]map[peer.ID]struct{}

	// Vivaldi integration
	vivaldiService *vivaldi.Service
	vivaldiConfig  *VivaldiConfig
	runnerStop     func()

	// SPREAD clustering configuration and lightweight cache.
	clusteringConfig *SpreadClusteringConfig
	sortedKnownByRTT []peer.ID
	sortedKnownSet   map[peer.ID]struct{}
	cacheDirty       bool
}

// VivaldiConfig holds configurable parameters for Vivaldi/Newton updates and runner.
type VivaldiConfig struct {
	Cc               float64
	Ce               float64
	Newton           bool
	OutlierThreshold float64
	Samples          int
	Interval         time.Duration
	NeighborSetSize  int
	IN1ThresholdMS   float64
	IN2ThresholdMS   float64
	IN3MADKRandom    float64
	IN3MADKClose     float64
	IN3MinSamples    int
}

// SpreadClusteringConfig controls how SPREAD candidates are partitioned.
type SpreadClusteringConfig struct {
	// ClusterPct is the percentage of topic peers selected as "local cluster".
	// It is applied to find the closest ones over all spread peers in the topic (excluding self).
	ClusterPct float64
	// NumRings controls equal-sized ring partitioning of non-cluster known peers.
	// Rings are unused now but can be explored in the future.
	NumRings int
}

const (
	DefaultSpreadClusterPct = 0.25
	DefaultSpreadNumRings   = 3
)

func DefaultSpreadClusteringConfig() *SpreadClusteringConfig {
	return &SpreadClusteringConfig{
		ClusterPct: DefaultSpreadClusterPct,
		NumRings:   DefaultSpreadNumRings,
	}
}

func sanitizeSpreadClusteringConfig(cfg *SpreadClusteringConfig) *SpreadClusteringConfig {
	out := DefaultSpreadClusteringConfig()
	if cfg == nil {
		return out
	}
	if cfg.ClusterPct > 0 && cfg.ClusterPct <= 1 {
		out.ClusterPct = cfg.ClusterPct
	}
	if cfg.NumRings > 0 {
		out.NumRings = cfg.NumRings
	}
	return out
}

func NewSpreadState() *SpreadState {
	return &SpreadState{
		peers:            make(map[peer.ID]struct{}),
		topics:           make(map[string]map[peer.ID]struct{}),
		vivaldiService:   nil,
		vivaldiConfig:    nil,
		runnerStop:       nil,
		clusteringConfig: DefaultSpreadClusteringConfig(),
		cacheDirty:       true,
	}
}

func (s *SpreadState) AddPeer(p peer.ID) {
	s.lk.Lock()
	defer s.lk.Unlock()
	s.peers[p] = struct{}{}
	s.cacheDirty = true
}

func (s *SpreadState) RemovePeer(p peer.ID) {
	s.lk.Lock()
	defer s.lk.Unlock()
	delete(s.peers, p)
	// Remove from all topics
	for t := range s.topics {
		delete(s.topics[t], p)
		if len(s.topics[t]) == 0 {
			delete(s.topics, t)
		}
	}
	s.cacheDirty = true
}

func (s *SpreadState) AddPeerTopic(topic string, p peer.ID) {
	s.lk.Lock()
	defer s.lk.Unlock()
	if _, ok := s.peers[p]; !ok {
		// not a spread peer; ignore
		return
	}
	ps, ok := s.topics[topic]
	if !ok {
		ps = make(map[peer.ID]struct{})
		s.topics[topic] = ps
	}
	ps[p] = struct{}{}
	s.cacheDirty = true
}

func (s *SpreadState) RemovePeerTopic(topic string, p peer.ID) {
	s.lk.Lock()
	defer s.lk.Unlock()
	if ps, ok := s.topics[topic]; ok {
		delete(ps, p)
		if len(ps) == 0 {
			delete(s.topics, topic)
		}
	}
	s.cacheDirty = true
}

// GetSpreadPeers returns a slice of spread-capable peers for the given topic.
func (s *SpreadState) GetSpreadPeers(topic string) []peer.ID {
	s.lk.RLock()
	defer s.lk.RUnlock()
	ps, ok := s.topics[topic]
	if !ok {
		return nil
	}
	out := make([]peer.ID, 0, len(ps))
	for p := range ps {
		out = append(out, p)
	}
	return out
}

// ConfigureVivaldi wires a Vivaldi service and parameters into the SpreadState.
// Passing a nil vsvc disables Vivaldi.
func (s *SpreadState) ConfigureVivaldi(vsvc *vivaldi.Service, cfg *VivaldiConfig) {
	s.lk.Lock()
	defer s.lk.Unlock()
	if s.runnerStop != nil && vsvc == nil {
		s.runnerStop()
		s.runnerStop = nil
	}
	s.vivaldiService = vsvc
	s.vivaldiConfig = sanitizeVivaldiConfig(cfg)
	s.cacheDirty = true
}

func (s *SpreadState) ConfigureClustering(cfg *SpreadClusteringConfig) {
	s.lk.Lock()
	defer s.lk.Unlock()
	s.clusteringConfig = sanitizeSpreadClusteringConfig(cfg)
	s.cacheDirty = true
}

// UpdatePeerVivaldi performs an ExchangeAndUpdate for a single peer.
func (s *SpreadState) UpdatePeerVivaldi(ctx context.Context, p peer.ID) (*vivaldi.VivaldiState, error) {
	s.lk.RLock()
	vsvc := s.vivaldiService
	vconf := s.vivaldiConfig
	s.lk.RUnlock()
	if vsvc == nil || vconf == nil {
		return nil, nil
	}
	cfg := vivaldi.UpdateConfig{
		Cc:                     vconf.Cc,
		Ce:                     vconf.Ce,
		Newton:                 vconf.Newton,
		OutlierThreshold:       vconf.OutlierThreshold,
		Samples:                vconf.Samples,
		Interval:               vconf.Interval,
		IN1CentroidThresholdMS: vconf.IN1ThresholdMS,
		IN2ProjectionThreshold: vconf.IN2ThresholdMS,
		IN3MADKRandom:          vconf.IN3MADKRandom,
		IN3MADKClose:           vconf.IN3MADKClose,
		IN3MinSamples:          vconf.IN3MinSamples,
	}
	st, err := vsvc.ExchangeAndUpdate(ctx, p, cfg)
	if err == nil {
		s.lk.Lock()
		s.cacheDirty = true
		s.lk.Unlock()
	}
	return st, err
}

// StartVivaldiRunner starts periodic exchanges to all known spread peers.
// If a runner is already running, it will be stopped and replaced.
func (s *SpreadState) StartVivaldiRunner() {
	s.lk.Lock()
	defer s.lk.Unlock()
	if s.vivaldiService == nil || s.vivaldiConfig == nil {
		return
	}
	if s.runnerStop != nil {
		s.runnerStop()
	}
	// Start a dynamic runner here in SpreadState so peer list changes are respected.
	stopCh := make(chan struct{})
	go func() {
		interval := s.vivaldiConfig.Interval
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				// snapshot current peers
				s.lk.RLock()
				curPeers := make([]peer.ID, 0, len(s.peers))
				for p := range s.peers {
					curPeers = append(curPeers, p)
				}
				vsvc := s.vivaldiService
				interval = s.vivaldiConfig.Interval
				ucfg := vivaldi.UpdateConfig{
					Cc:                     s.vivaldiConfig.Cc,
					Ce:                     s.vivaldiConfig.Ce,
					Newton:                 s.vivaldiConfig.Newton,
					OutlierThreshold:       s.vivaldiConfig.OutlierThreshold,
					Samples:                s.vivaldiConfig.Samples,
					Interval:               interval,
					IN1CentroidThresholdMS: s.vivaldiConfig.IN1ThresholdMS,
					IN2ProjectionThreshold: s.vivaldiConfig.IN2ThresholdMS,
					IN3MADKRandom:          s.vivaldiConfig.IN3MADKRandom,
					IN3MADKClose:           s.vivaldiConfig.IN3MADKClose,
					IN3MinSamples:          s.vivaldiConfig.IN3MinSamples,
				}
				s.lk.RUnlock()
				if vsvc == nil {
					continue
				}

				selected, closePeers, randomPeers := selectNewtonNeighbors(curPeers, vsvc, s.vivaldiConfig.NeighborSetSize)
				vsvc.SetNeighborSets(closePeers, randomPeers)
				for _, p := range selected {
					ctx, cancel := context.WithTimeout(context.Background(), interval)
					_, _ = vsvc.ExchangeAndUpdate(ctx, p, ucfg)
					cancel()
				}
				s.lk.Lock()
				s.cacheDirty = true
				s.lk.Unlock()
			}
		}
	}()
	s.runnerStop = func() { close(stopCh) }
}

// StopVivaldiRunner stops the background runner if it is running.
func (s *SpreadState) StopVivaldiRunner() {
	s.lk.Lock()
	defer s.lk.Unlock()
	if s.runnerStop != nil {
		s.runnerStop()
		s.runnerStop = nil
	}
}

// ShutdownVivaldi stops background work and closes the service stream handler.
func (s *SpreadState) ShutdownVivaldi() {
	s.lk.Lock()
	defer s.lk.Unlock()
	if s.runnerStop != nil {
		s.runnerStop()
		s.runnerStop = nil
	}
	if s.vivaldiService != nil {
		s.vivaldiService.Close()
		s.vivaldiService = nil
	}
	s.cacheDirty = true
}

// GetPropagationPeers returns cluster peers and inter-cluster peers for topic.
func (s *SpreadState) GetPropagationPeers(topic string, self peer.ID) ([]peer.ID, []peer.ID) {

	// Refresh sorted distance cache before getting propagation peers.
	s.refreshDistanceCache(self)

	// Get topic peers
	s.lk.RLock()
	topicPeers, ok := s.topics[topic]
	if !ok || len(topicPeers) == 0 {
		s.lk.RUnlock()
		return nil, nil
	}
	cfg := s.clusteringConfig
	sortedKnown := append([]peer.ID(nil), s.sortedKnownByRTT...)
	knownSet := make(map[peer.ID]struct{}, len(s.sortedKnownSet))
	for p := range s.sortedKnownSet {
		knownSet[p] = struct{}{}
	}
	topicSet := make(map[peer.ID]struct{}, len(topicPeers))
	for p := range topicPeers {
		topicSet[p] = struct{}{}
	}
	s.lk.RUnlock()

	// Compute topic size
	totalTopicPeers := 0
	for p := range topicSet {
		if p != self {
			totalTopicPeers++
		}
	}
	if totalTopicPeers == 0 {
		return nil, nil
	}

	// Compute cluster size
	clusterSize := int(math.Ceil(float64(totalTopicPeers) * cfg.ClusterPct))
	if clusterSize < 1 {
		clusterSize = 1
	}

	// Identify known peers in the topic
	knownInTopic := make([]peer.ID, 0, len(topicSet))
	for _, p := range sortedKnown {
		if p == self {
			continue
		}
		if _, ok := topicSet[p]; ok {
			knownInTopic = append(knownInTopic, p)
		}
	}

	// Get cluster peers
	if clusterSize > len(knownInTopic) {
		clusterSize = len(knownInTopic)
	}
	clusterPeers := append([]peer.ID(nil), knownInTopic[:clusterSize]...)

	clusterSet := make(map[peer.ID]struct{}, len(clusterPeers))
	for _, p := range clusterPeers {
		clusterSet[p] = struct{}{}
	}

	// Get inter-cluster peers: first the known ones in equal-sized rings, then the unknown ones.
	knownRemainder := append([]peer.ID(nil), knownInTopic[clusterSize:]...)

	unknownInTopic := make([]peer.ID, 0, len(topicSet))
	for p := range topicSet {
		if p == self {
			continue
		}
		if _, inCluster := clusterSet[p]; inCluster {
			continue
		}
		if _, known := knownSet[p]; known {
			continue
		}
		unknownInTopic = append(unknownInTopic, p)
	}
	interPeers := append(knownRemainder, unknownInTopic...)
	return clusterPeers, interPeers
}

// splitPeersForTesting is a helper to split peers into rings. Unused for now.
func splitIntoEqualRings(peers []peer.ID, numRings int) [][]peer.ID {
	if len(peers) == 0 {
		return nil
	}
	if numRings <= 0 {
		numRings = 1
	}
	if numRings > len(peers) {
		numRings = len(peers)
	}

	rings := make([][]peer.ID, 0, numRings)
	base := len(peers) / numRings
	rem := len(peers) % numRings
	start := 0
	for i := 0; i < numRings; i++ {
		size := base
		if i < rem {
			size++
		}
		end := start + size
		rings = append(rings, append([]peer.ID(nil), peers[start:end]...))
		start = end
	}
	return rings
}

// flattenRings is a helper to flatten rings into a single slice. Unused for now.
func flattenRings(rings [][]peer.ID) []peer.ID {
	if len(rings) == 0 {
		return nil
	}
	total := 0
	for _, ring := range rings {
		total += len(ring)
	}
	out := make([]peer.ID, 0, total)
	for _, ring := range rings {
		out = append(out, ring...)
	}
	return out
}

func (s *SpreadState) refreshDistanceCache(self peer.ID) {
	s.lk.RLock()

	// If cache is clean, no need to refresh.
	needsRefresh := s.cacheDirty
	if !needsRefresh {
		s.lk.RUnlock()
		return
	}
	// Get vivaldi service and peers
	vsvc := s.vivaldiService
	peers := make([]peer.ID, 0, len(s.peers))
	for p := range s.peers {
		peers = append(peers, p)
	}
	s.lk.RUnlock()

	// If no vivaldi service or no peers, reset cache to empty.
	if vsvc == nil {
		s.lk.Lock()
		s.sortedKnownByRTT = nil
		s.sortedKnownSet = nil
		s.cacheDirty = false
		s.lk.Unlock()
		return
	}

	// If no local coordinate, we can't compute distances, so reset cache to unsorted.
	local := vsvc.GetLocalState()
	if local == nil {
		s.lk.Lock()
		s.sortedKnownByRTT = nil
		s.sortedKnownSet = nil
		s.cacheDirty = false
		s.lk.Unlock()
		return
	}

	// Compute distances to known peers and sort by distance.
	type distanceEntry struct {
		id   peer.ID
		dist float64
	}
	known := make([]distanceEntry, 0, len(peers))
	for _, p := range peers {
		if p == self {
			continue
		}
		// If peer has no coordinate, we can't compute distance, so treat as unknown
		st := vsvc.GetPeerState(p)
		if st == nil {
			continue
		}
		known = append(known, distanceEntry{
			id:   p,
			dist: vivaldi.Distance(local.Coord, st.Coord),
		})
	}
	// Sort by distance
	sort.Slice(known, func(i, j int) bool {
		return known[i].dist < known[j].dist
	})

	// Extract sorted peer IDs and sets for quick lookup.
	sorted := make([]peer.ID, 0, len(known))
	knownSet := make(map[peer.ID]struct{}, len(known))
	for _, entry := range known {
		sorted = append(sorted, entry.id)
		knownSet[entry.id] = struct{}{}
	}

	// Update cache
	s.lk.Lock()
	s.sortedKnownByRTT = sorted
	s.sortedKnownSet = knownSet
	s.cacheDirty = false
	s.lk.Unlock()
}

func sanitizeVivaldiConfig(cfg *VivaldiConfig) *VivaldiConfig {
	out := &VivaldiConfig{
		Cc:               0.25,
		Ce:               0.25,
		Newton:           true,
		OutlierThreshold: 0,
		Samples:          3,
		Interval:         30 * time.Second,
		NeighborSetSize:  64,
		IN1ThresholdMS:   20,
		IN2ThresholdMS:   35,
		IN3MADKRandom:    5,
		IN3MADKClose:     8,
		IN3MinSamples:    8,
	}
	if cfg == nil {
		return out
	}
	out.Cc = cfg.Cc
	out.Ce = cfg.Ce
	out.Newton = cfg.Newton
	out.OutlierThreshold = cfg.OutlierThreshold
	out.Samples = cfg.Samples
	out.Interval = cfg.Interval
	out.NeighborSetSize = cfg.NeighborSetSize
	out.IN1ThresholdMS = cfg.IN1ThresholdMS
	out.IN2ThresholdMS = cfg.IN2ThresholdMS
	out.IN3MADKRandom = cfg.IN3MADKRandom
	out.IN3MADKClose = cfg.IN3MADKClose
	out.IN3MinSamples = cfg.IN3MinSamples
	if out.Cc <= 0 || out.Cc > 1 {
		out.Cc = 0.25
	}
	if out.Ce <= 0 || out.Ce > 1 {
		out.Ce = 0.25
	}
	if out.Samples <= 0 {
		out.Samples = 3
	}
	if out.Interval <= 0 {
		out.Interval = 30 * time.Second
	}
	if out.NeighborSetSize <= 0 {
		out.NeighborSetSize = 64
	}
	if out.IN1ThresholdMS <= 0 {
		out.IN1ThresholdMS = 20
	}
	if out.IN2ThresholdMS <= 0 {
		out.IN2ThresholdMS = 35
	}
	if out.IN3MADKRandom <= 0 {
		out.IN3MADKRandom = 5
	}
	if out.IN3MADKClose <= 0 {
		out.IN3MADKClose = 8
	}
	if out.IN3MinSamples <= 0 {
		out.IN3MinSamples = 8
	}
	return out
}

func selectNewtonNeighbors(peers []peer.ID, svc *vivaldi.Service, size int) ([]peer.ID, []peer.ID, []peer.ID) {
	if len(peers) == 0 {
		return nil, nil, nil
	}
	if size <= 0 || size > len(peers) {
		size = len(peers)
	}
	type rttEntry struct {
		id  peer.ID
		rtt float64
	}
	rttKnown := make([]rttEntry, 0, len(peers))
	for _, p := range peers {
		if rtt, ok := svc.GetPeerRTTMS(p); ok {
			rttKnown = append(rttKnown, rttEntry{id: p, rtt: rtt})
		}
	}
	sort.Slice(rttKnown, func(i, j int) bool { return rttKnown[i].rtt < rttKnown[j].rtt })

	closeTarget := size / 2
	closePeers := make([]peer.ID, 0, closeTarget)
	used := make(map[peer.ID]struct{}, size)

	for i := 0; i < len(rttKnown) && len(closePeers) < closeTarget; i++ {
		p := rttKnown[i].id
		closePeers = append(closePeers, p)
		used[p] = struct{}{}
	}

	pool := make([]peer.ID, 0, len(peers)-len(closePeers))
	for _, p := range peers {
		if _, ok := used[p]; ok {
			continue
		}
		pool = append(pool, p)
	}
	rand.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	randomTarget := size - len(closePeers)
	if randomTarget > len(pool) {
		randomTarget = len(pool)
	}
	randomPeers := append([]peer.ID(nil), pool[:randomTarget]...)
	selected := append(append([]peer.ID(nil), closePeers...), randomPeers...)
	return selected, closePeers, randomPeers
}
