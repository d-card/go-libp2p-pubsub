package pubsub

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"net"

	ip2location "github.com/ip2location/ip2location-go/v9"
	"github.com/libp2p/go-libp2p/core/host"
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




const (
	ClusterEurope ClusterID = iota
	ClusterEastNA
	ClusterWestNA
	ClusterSA
	ClusterAfrica
	ClusterEastAsia
	ClusterWestAsia
	ClusterOceania
)

func regionToCluster(country, region string, longitude float64) ClusterID {
	switch country {
	case "US", "CA":
		// Split NA by longitude (roughly at -100)
		if longitude > -100 {
			return ClusterEastNA
		} else {
			return ClusterWestNA
		}
	case "MX":
		return ClusterWestNA
	case "BR", "AR", "CL", "CO", "PE", "VE", "EC", "BO", "PY", "UY", "SR", "GF", "GY":
		return ClusterSA
	case "ZA", "NG", "EG", "DZ", "MA", "KE", "ET", "GH", "TZ", "UG", "SD", "AO", "CM", "CI", "SN", "TN", "ZW", "MZ", "MW", "NE", "BF", "ML", "ZM", "RW", "SO", "GN", "BJ", "BI", "TG", "SL", "TD", "CG", "LY", "LR", "GA", "MR", "SZ", "GQ", "GW", "LS", "DJ", "KM", "SC", "ST", "CV":
		return ClusterAfrica
	case "AU", "NZ", "FJ", "PG", "SB", "VU", "NC", "WS", "TO", "KI", "FM", "MH", "PW":
		return ClusterOceania
	case "CN", "JP", "KR", "TW", "MN":
		return ClusterEastAsia
	case "IN", "PK", "BD", "LK", "NP", "AF", "UZ", "TM", "TJ", "KG":
		return ClusterWestAsia
    case "AL", "AD", "AT", "BY", "BE", "BA", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IS", "IE", "IT", "LV", "LI", "LT", "LU", "MT", "MD", "MC", "ME", "NL", "MK", "NO", "PL", "PT", "RO", "SM", "RS", "SK", "SI", "ES", "SE", "CH", "UA", "GB", "VA":
        return ClusterEurope
	}
	if country == "RU" {
		if longitude > 90 {
			return ClusterEastAsia
		} else {
			return ClusterWestAsia
		}
	}
	if region == "Europe" {
		return ClusterEurope
	}
	if region == "Asia" {
		if longitude > 90 {
			return ClusterEastAsia
		} else {
			return ClusterWestAsia
		}
	}
	if region == "Africa" {
		return ClusterAfrica
	}
	if region == "Oceania" {
		return ClusterOceania
	}
	if region == "South America" {
		return ClusterSA
	}
	if region == "North America" {
		if longitude > -100 {
			return ClusterEastNA
		} else {
			return ClusterWestNA
		}
	}
	// Fallback
	return ClusterEurope
}

func NewHierarchicalDataProvider(selfPeerID peer.ID, h host.Host) *HierarchicalDataProvider {
	db, err := ip2location.OpenDB("./IP2LOCATION-LITE-DB1.BIN")
	if err != nil {
		panic(fmt.Sprintf("Failed to open IP2Location DB: %v", err))
	}
	defer db.Close()

    nodePeerID := CreateNodePeerIDMap()

    // Build peerID to IP map for participants only
    peerIDToIP := make(map[peer.ID]string)
    for _, p := range nodePeerID {
        if p == selfPeerID {
            // Get our own external IP from listen addresses
            for _, addr := range h.Addrs() {
                ip := addrToIP(addr.String())
                if ip != "" && !isPrivateIP(ip) {
                    peerIDToIP[p] = ip
                    break
                }
            }
        } else {
            addrs := h.Peerstore().Addrs(p)
            for _, addr := range addrs {
                ip := addrToIP(addr.String())
                if ip != "" && !isPrivateIP(ip) {
                    peerIDToIP[p] = ip
                    break
                }
            }
        }
    }

	clusterPeers := make(map[ClusterID][]peer.ID)
	selfClusterID := ClusterID(-1)
	for pid, ip := range peerIDToIP {
		res, err := db.Get_all(ip)
		if err != nil {
			fmt.Printf("Failed to geolocate IP %s: %v\n", ip, err)
			continue
		}
		cluster := regionToCluster(res.Country_short, res.Region, res.Longitude)
		clusterPeers[cluster] = append(clusterPeers[cluster], pid)
		if pid == selfPeerID {
			selfClusterID = cluster
		}
	}

	return &HierarchicalDataProvider{
		clusterPeers:  clusterPeers,
		selfClusterID: selfClusterID,
	}
}

func addrToIP(addr string) string {
	parts := strings.Split(addr, "/")
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] == "ip4" || parts[i] == "ip6" {
			return parts[i+1]
		}
	}
	return ""
}

func isPrivateIP(ip string) bool {
	netIP := net.ParseIP(ip)
	if netIP == nil {
		return false
	}
	if netIP.IsLoopback() || netIP.IsPrivate() {
		return true
	}
	return false
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