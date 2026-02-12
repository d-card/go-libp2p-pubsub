package pubsub

import (
	"context"
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
	vsvc       *vivaldi.Service
	vconf      *VivaldiConfig
	runnerStop func()
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

func NewSpreadState() *SpreadState {
	return &SpreadState{
		peers:      make(map[peer.ID]struct{}),
		topics:     make(map[string]map[peer.ID]struct{}),
		vsvc:       nil,
		vconf:      nil,
		runnerStop: nil,
	}
}

func (s *SpreadState) AddPeer(p peer.ID) {
	s.lk.Lock()
	defer s.lk.Unlock()
	s.peers[p] = struct{}{}
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
	s.vsvc = vsvc
	s.vconf = sanitizeVivaldiConfig(cfg)
}

// UpdatePeerVivaldi performs an ExchangeAndUpdate for a single peer.
func (s *SpreadState) UpdatePeerVivaldi(ctx context.Context, p peer.ID) (*vivaldi.VivaldiState, error) {
	s.lk.RLock()
	vsvc := s.vsvc
	vconf := s.vconf
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
	return vsvc.ExchangeAndUpdate(ctx, p, cfg)
}

// StartVivaldiRunner starts periodic exchanges to all known spread peers.
// If a runner is already running, it will be stopped and replaced.
func (s *SpreadState) StartVivaldiRunner() {
	s.lk.Lock()
	defer s.lk.Unlock()
	if s.vsvc == nil || s.vconf == nil {
		return
	}
	if s.runnerStop != nil {
		s.runnerStop()
	}
	// Start a dynamic runner here in SpreadState so peer list changes are respected.
	stopCh := make(chan struct{})
	go func() {
		interval := s.vconf.Interval
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
				vsvc := s.vsvc
				interval = s.vconf.Interval
				ucfg := vivaldi.UpdateConfig{
					Cc:                     s.vconf.Cc,
					Ce:                     s.vconf.Ce,
					Newton:                 s.vconf.Newton,
					OutlierThreshold:       s.vconf.OutlierThreshold,
					Samples:                s.vconf.Samples,
					Interval:               interval,
					IN1CentroidThresholdMS: s.vconf.IN1ThresholdMS,
					IN2ProjectionThreshold: s.vconf.IN2ThresholdMS,
					IN3MADKRandom:          s.vconf.IN3MADKRandom,
					IN3MADKClose:           s.vconf.IN3MADKClose,
					IN3MinSamples:          s.vconf.IN3MinSamples,
				}
				s.lk.RUnlock()
				if vsvc == nil {
					continue
				}

				selected, closePeers, randomPeers := selectNewtonNeighbors(curPeers, vsvc, s.vconf.NeighborSetSize)
				vsvc.SetNeighborSets(closePeers, randomPeers)
				for _, p := range selected {
					ctx, cancel := context.WithTimeout(context.Background(), interval)
					_, _ = vsvc.ExchangeAndUpdate(ctx, p, ucfg)
					cancel()
				}
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
	if s.vsvc != nil {
		s.vsvc.Close()
		s.vsvc = nil
	}
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
