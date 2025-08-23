package pubsub

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
)


type ClusterID int
type NodeID int
type HierarchicalDataProvider struct {
	clusterPeers map[ClusterID][]peer.ID

	selfClusterID ClusterID
}

func ReadParticipants() map[NodeID]bool {
	participants := make(map[NodeID]bool)
	
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatalf("Failed to get current file path")
	}
	libDir := filepath.Dir(filename)
		
	fname := filepath.Join(libDir, "hierarchical_data", "participants.txt")
	file, err := os.Open(fname)
	if err != nil {
		log.Fatalf("Failed to open participants file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		nodeIDInt, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			log.Fatalf("Failed to parse participant NodeID: %s", line)
			continue
		}
		participants[NodeID(nodeIDInt)] = true
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Failed to read participants file: %v", err)
	}

	return participants
}

func CreateNodePeerIDMap() map[NodeID]peer.ID {
	nodePeerID := make(map[NodeID]peer.ID)
	participants := ReadParticipants()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatalf("Failed to get current file path")
	}
	libDir := filepath.Dir(filename)
	
	fname := filepath.Join(libDir, "hierarchical_data", "peerids.txt")
	file, err := os.Open(fname)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			continue
		}
		//convert parts[0] from string to NodeID
		nodeIDInt, err := strconv.Atoi(parts[0])
		if err != nil {
			panic(err)
		}
		nodeID := NodeID(nodeIDInt)
		
		// Only include NodeIDs that are present in participants.txt
		if !participants[nodeID] {
			continue
		}
		
		peerID, err := peer.Decode(parts[1])
		if err != nil {
			fmt.Printf("HierarchicalDataProvider: Failed to decode peer ID %s: %v\n", parts[1], err)
			continue
		}
		nodePeerID[nodeID] = peerID
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	return nodePeerID
}


func CreateClusterPeersMap(nodePeerID map[NodeID]peer.ID) map[ClusterID][]peer.ID {
	clusterPeers := make(map[ClusterID][]peer.ID)

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatalf("Failed to get current file path")
	}
	libDir := filepath.Dir(filename)

	fname := filepath.Join(libDir, "hierarchical_data", "clusters.txt")
	file, err := os.Open(fname)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Line format: <ClusterID>:<NodeID>[, <NodeID>]*
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if len(parts) < 2 {
			continue
		}
		// Convert parts[0] from string to ClusterID
		clusterIDInt, err := strconv.Atoi(parts[0])
		if err != nil {
			panic(err)
		}
		clusterID := ClusterID(clusterIDInt)
		clusterPeers[clusterID] = make([]peer.ID, 0)

		nodeIDs := strings.Split(parts[1], ",") // {"1", "2", "3", ...}
		for _, nodeID := range nodeIDs {
			// Transform nodeID from string to NodeID
			nodeIDInt, err := strconv.Atoi(strings.TrimSpace(nodeID))
			if err != nil {
				panic(err)
			}
			nodeIDTyped := NodeID(nodeIDInt)
			
			// Only include peers that exist in the filtered nodePeerID map
			if peerID, exists := nodePeerID[nodeIDTyped]; exists {
				clusterPeers[clusterID] = append(clusterPeers[clusterID], peerID)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	return clusterPeers
}

func CreateClusterIndexMap(clusterPeers map[ClusterID][]peer.ID) map[peer.ID]ClusterID {
	clusterIndex := make(map[peer.ID]ClusterID)

	// Iterate through clusterPeers to create a mapping from peer.ID to ClusterID
	for clusterID, peers := range clusterPeers {
		for _, peerID := range peers {
			clusterIndex[peerID] = clusterID
		}
	}

	return clusterIndex
}

func NewHierarchicalDataProvider(selfPeerID peer.ID) *HierarchicalDataProvider {

	nodePeerID := CreateNodePeerIDMap()
	clusterPeers := CreateClusterPeersMap(nodePeerID)
	clusterIndex := CreateClusterIndexMap(clusterPeers)

	fmt.Println("nodePeerID:", nodePeerID)
	fmt.Println("clusterPeers:", clusterPeers)
	fmt.Println("clusterIndex:", clusterIndex)

	fmt.Println("selfPeerID:", selfPeerID)
	fmt.Println("selfClusterID:", clusterIndex[selfPeerID])
	// Get self cluster ID
	selfClusterID := clusterIndex[selfPeerID]

	return &HierarchicalDataProvider{
		clusterPeers:  clusterPeers,
		selfClusterID: selfClusterID,
	}
}

// Get own cluster ID
func (hdp *HierarchicalDataProvider) GetSelfClusterID() ClusterID {
	return hdp.selfClusterID
}

// Get peers from cluster ID
func (hdp *HierarchicalDataProvider) GetPeersFromClusterID(clusterID ClusterID) []peer.ID {
	peers, ok := hdp.clusterPeers[clusterID]
	if !ok {
		fmt.Printf("HierarchicalDataProvider: No peers found for cluster %d\n", clusterID)
		return nil
	}
	// fmt.Printf("HierarchicalDataProvider: Returning %d peers from cluster %d: %v\n", len(peers), clusterID, peers)
	return peers
}

// Get all peers not in the provided cluster ID
func (hdp *HierarchicalDataProvider) GetPeersNotInClusterID(clusterID ClusterID) []peer.ID {
	peersNotInCluster := make([]peer.ID, 0)
	for id, peers := range hdp.clusterPeers {
		if id == clusterID {
			continue
		}
		peersNotInCluster = append(peersNotInCluster, peers...)
	}
	return peersNotInCluster
}