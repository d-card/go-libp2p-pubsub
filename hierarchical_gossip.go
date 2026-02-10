package pubsub

import (
	"math/rand"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	INTRA_FANOUT = 3 // Number of intra-cluster peers to select
	INTER_FANOUT = 8 // Number of inter-cluster peers to select
	INTRA_RHO    = 0.6 // Probability of selecting intra-cluster peers
	INTER_PROB   = 0.8 // Probability of selecting inter-cluster peers
)

type HierarchicalGossipConfig struct {
	// Intra-cluster fanout
	IntraFanout int
	// Inter-cluster fanout
	InterFanout int
	// Intra-cluster Cobra-walk rho
	IntraRho float64
	// Inter-cluster communication probability
	InterProb float64
}

type HierarchicalGossip struct {
	config *HierarchicalGossipConfig
	dataProvider *HierarchicalDataProvider
	meshEstablished bool
}

func NewHierarchicalGossip(config *HierarchicalGossipConfig, dataProvider *HierarchicalDataProvider) *HierarchicalGossip {
	return &HierarchicalGossip{
		config: config,
		dataProvider: dataProvider,
		meshEstablished: false,
	}
}

func (hg *HierarchicalGossip) GetForwardingPeers(from peer.ID) []peer.ID {
	// Select peers each time this method is called
	selectedPeers := make(map[peer.ID]struct{})

	// Intra-cluster
	selfCluster := hg.dataProvider.GetPeersFromClusterID(hg.dataProvider.selfClusterID)
	selectedPeers[SelectRandomPeerID(selfCluster)] = struct{}{}
	if rand.Float64() < hg.config.IntraRho {
		// Select intra-cluster peers
		for i := 0; i < hg.config.IntraFanout-1; i++ {
			peerID := SelectRandomPeerID(selfCluster)
			if peerID == "" {
				break // No peers in the cluster
			}
			selectedPeers[peerID] = struct{}{}
		}
	}

	// Inter-cluster
	if rand.Float64() < hg.config.InterProb {
		// Select inter-cluster peers
		interClusterPeers := hg.dataProvider.GetPeersNotInClusterID(hg.dataProvider.selfClusterID)
		if len(interClusterPeers) > 0 {
			for i := 0; i < hg.config.InterFanout; i++ {
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

	// fmt.Println("Forwarding peers:", forwardingPeers)
	
	return forwardingPeers
}

func SelectRandomPeerID(peers []peer.ID) peer.ID {
	if len(peers) == 0 {
		return ""
	}
	return peers[rand.Intn(len(peers))]
}
