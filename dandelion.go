package pubsub

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	DANDELION_FANOUT_DEFAULT = 6
	DANDELION_PROB_DEFAULT   = 0.8
)

type DandelionGossipConfig struct {
	FANOUT int
	PROB   float64
}

type DandelionGossip struct {
	config *DandelionGossipConfig
}

func NewDandelionGossip(config *DandelionGossipConfig) *DandelionGossip {
	return &DandelionGossip{config: config}
}

// GetForwardingPeers implements Dandelion++ forwarding.
//
// dandelionValue 0 = stem phase: forward to one random peer, flip coin to
// transition to fluff. dandelionValue 1 = fluff phase: gossip to FANOUT peers.
func (dg *DandelionGossip) GetForwardingPeers(from peer.ID, topicPeers map[peer.ID]struct{}, dandelionValue uint8) ([]peer.ID, uint8) {
	peersLst := make([]peer.ID, 0, len(topicPeers))
	for peerID := range topicPeers {
		if peerID != from {
			peersLst = append(peersLst, peerID)
		}
	}

	selectedPeers := make(map[peer.ID]struct{})
	newCoin := dandelionValue

	if dandelionValue == 0 {
		// Stem phase: single random hop.
		if p := SelectRandomPeerID(peersLst); p != "" {
			selectedPeers[p] = struct{}{}
		}
		if rand.Float64() < dg.config.PROB {
			newCoin = 1
		}
	} else {
		// Fluff phase: gossip to FANOUT peers.
		for i := 0; i < dg.config.FANOUT; i++ {
			if p := SelectRandomPeerID(peersLst); p != "" {
				selectedPeers[p] = struct{}{}
			}
		}
	}

	out := make([]peer.ID, 0, len(selectedPeers))
	for p := range selectedPeers {
		out = append(out, p)
	}
	return out, newCoin
}

// ReadDandelionCoin extracts the DandelionCoin value from message payload.
func (dg *DandelionGossip) ReadDandelionCoin(msg []byte) (uint8, error) {
	parts := bytes.Split(msg, []byte(" "))
	for i := 0; i < len(parts)-1; i++ {
		if string(parts[i]) == "DandelionCoin" {
			v, err := strconv.Atoi(string(parts[i+1]))
			if err != nil || (v != 0 && v != 1) {
				return 0, fmt.Errorf("invalid DandelionCoin value: %s", parts[i+1])
			}
			return uint8(v), nil
		}
	}
	return 0, fmt.Errorf("DandelionCoin not found in message")
}

// ReplaceDandelionCoin rewrites the DandelionCoin field in a message payload.
func (dg *DandelionGossip) ReplaceDandelionCoin(msg []byte, newVal uint8) ([]byte, error) {
	if newVal != 0 && newVal != 1 {
		return nil, fmt.Errorf("DandelionCoin must be 0 or 1")
	}
	parts := bytes.Split(msg, []byte(" "))
	for i := 0; i < len(parts)-1; i++ {
		if string(parts[i]) == "DandelionCoin" {
			parts[i+1] = []byte(strconv.Itoa(int(newVal)))
			return bytes.Join(parts, []byte(" ")), nil
		}
	}
	return nil, fmt.Errorf("DandelionCoin not found in message")
}
