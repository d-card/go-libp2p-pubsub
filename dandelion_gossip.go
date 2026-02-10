package pubsub

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	FANOUT = 6
	PROB = 0.8
)

type DandelionGossipConfig struct {
	FANOUT int
	PROB float64
}

type DandelionGossip struct {
	config *DandelionGossipConfig
}

func NewDandelionGossip(config *DandelionGossipConfig) *DandelionGossip {
	return &DandelionGossip{
		config: config,
	}
}

func (dg *DandelionGossip) GetForwardingPeers(from peer.ID, topicPeers map[peer.ID]struct{}, dandelionValue uint8) ([]peer.ID, uint8) {
	// Select peers each time this method is called
	selectedPeers := make(map[peer.ID]struct{}) // Use a map to avoid duplicates

	// Convert topicPeers map to a slice of peer.IDs, excluding the 'from' peer
	peersLst := make([]peer.ID, 0)
	for peerID := range topicPeers {
		if peerID != from {
			peersLst = append(peersLst, peerID)
		}
	}
	// Next DandelionCoin value
	newDandelionCoin := dandelionValue


	// If dandelionValue is 0, we are in the "anom" phase
	// So we select a random peer from the set of topicPeers
	if dandelionValue == 0 {
		// Select a random peer from topicPeers
		selectedPeer := SelectRandomPeerID(peersLst)
		if selectedPeer != "" {
			selectedPeers[selectedPeer] = struct{}{}
		}

		if rand.Float64() < dg.config.PROB {
			newDandelionCoin = uint8(1) // Move to "dissemination" phase
		}

	} else {
		// If dandelionValue is 1, we are in the "dissemination" phase
		// We select dg.config.FANOUT random peers from the topicPeers
		for i := 0; i < dg.config.FANOUT; i++ {
			peerID := SelectRandomPeerID(peersLst)
			if peerID == "" {
				break // No peers in the cluster
			}
			selectedPeers[peerID] = struct{}{}
		}
	}

	// Convert map to slice
	forwardingPeers := make([]peer.ID, 0, len(selectedPeers))
	for peerID := range selectedPeers {
		// don't send to sender
		if peerID == from {
			continue
		}
		forwardingPeers = append(forwardingPeers, peerID)
	}

	return forwardingPeers, newDandelionCoin
}

// ReadDandelionCoin extracts the DandelionCoin value ("0" or "1")
// from a message of format: [MSG_NUMBER] from [SOURCE_NODE_ID] DandelionCoin [DANDELION_COIN]
func (dg *DandelionGossip) ReadDandelionCoin(msg []byte) (uint8, error) {
	parts := bytes.Split(msg, []byte(" "))
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid message format")
	}

	// Find the "DandelionCoin" token
	for i := 0; i < len(parts)-1; i++ {
		if string(parts[i]) == "DandelionCoin" {
			coinString := string(parts[i+1])
			coinInt, err := strconv.Atoi(coinString)
			if err != nil || (coinInt != 0 && coinInt != 1) {
				return 0, fmt.Errorf("invalid DandelionCoin value: %s", coinString)
			}
			return uint8(coinInt), nil
		}
	}

	return 0, fmt.Errorf("DandelionCoin value not found")
}

// ReplaceDandelionCoin replaces the DandelionCoin value with a new value (0 or 1)
func (dg *DandelionGossip) ReplaceDandelionCoin(msg []byte, newVal uint8) ([]byte, error) {
	if newVal != 0 && newVal != 1 {
		return nil, fmt.Errorf("invalid DandelionCoin value: must be '0' or '1'")
	}

	parts := bytes.Split(msg, []byte(" "))
	for i := 0; i < len(parts)-1; i++ {
		if string(parts[i]) == "DandelionCoin" {
			parts[i+1] = []byte(strconv.Itoa(int(newVal)))
			return bytes.Join(parts, []byte(" ")), nil
		}
	}

	return nil, fmt.Errorf("DandelionCoin value not found")
}
