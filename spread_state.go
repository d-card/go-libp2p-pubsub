package pubsub

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

type SpreadState struct {
	lk sync.RWMutex
	// peers that advertise SPREAD
	peers map[peer.ID]struct{}
	// topic -> peers with spread extension that are subscribed to the topic
	topics map[string]map[peer.ID]struct{}
}

func NewSpreadState() *SpreadState {
	return &SpreadState{
		peers:  make(map[peer.ID]struct{}),
		topics: make(map[string]map[peer.ID]struct{}),
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
