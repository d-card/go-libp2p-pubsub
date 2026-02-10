package pubsub

import (
	"math/rand"

	"github.com/libp2p/go-libp2p/core/peer"
)

type SpreadConfig struct {
	// Intra-cluster fanout: number of intra-cluster peers to select
	IntraFanout int
	// Inter-cluster fanout: number of inter-cluster peers to select
	InterFanout int
	// Intra-cluster Cobra-walk rho: probability of selecting intra-cluster peers
	IntraRho float64
	// Inter-cluster communication probability: probability of selecting inter-cluster peers
	InterProb float64
}

// Default configuration values
const (
	INTRA_FANOUT = 3
	INTER_FANOUT = 8
	INTRA_RHO    = 0.6
	INTER_PROB   = 0.8
)

// TODO: wire it to actually be used in propagation.
type SpreadPropagation struct {
	config *SpreadConfig
}

func NewSpreadPropagation(config *SpreadConfig) *SpreadPropagation {
	return &SpreadPropagation{
		config: config,
	}
}

func (sp *SpreadPropagation) GetPeersForPropagation(from peer.ID, clusterPeers []peer.ID, interClusterPeers []peer.ID) []peer.ID {

	selectedPeers := make(map[peer.ID]struct{})

	// Intra-cluster

	// Add mandatory cluster peer
	selectedPeers[SelectRandomPeerID(clusterPeers)] = struct{}{}
	// Coin flip to decide whether to select more intra-cluster peers
	if rand.Float64() < sp.config.IntraRho {
		// Select [intra fanout - 1] more random ones
		for i := 0; i < sp.config.IntraFanout-1; i++ {
			peerID := SelectRandomPeerID(clusterPeers)
			if peerID == "" {
				break // No peers in the cluster
			}
			selectedPeers[peerID] = struct{}{}
		}
	}

	// Inter-cluster

	// Coin flip to decide whether to select any inter-cluster peers
	if rand.Float64() < sp.config.InterProb {
		// Select [inter fanout] random ones
		if len(interClusterPeers) > 0 {
			for i := 0; i < sp.config.InterFanout; i++ {
				peerID := SelectRandomPeerID(interClusterPeers)
				if peerID == "" {
					break // No peers in the cluster
				}
				selectedPeers[peerID] = struct{}{}
			}
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

	return forwardingPeers
}

func SelectRandomPeerID(peers []peer.ID) peer.ID {
	if len(peers) == 0 {
		return ""
	}
	return peers[rand.Intn(len(peers))]
}
