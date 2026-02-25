package pubsub

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	simnet "github.com/marcopolo/simnet"
)

// Usage: go test -tags simnet -run TestSimnetTwoNodesDirectLatency -v .
// Replace latency value to test different scenarios

const (
	SPREAD_SIMNET_TWO_NODES_LATENCY = 120 * time.Millisecond // Latency between the two nodes
	SPREAD_SIMNET_TWO_NODES_TIMEOUT = 90 * time.Second       // Overall timeout for the test
)

// Tests a network with 2 nodes and a direct link between them, with a configured latency.
// The test verifies that a message published on one node is received on the other node within the expected latency bounds.
func TestSimnetTwoNodesDirectLatency(t *testing.T) {

	// Sample topic
	const topicName = "spread-simnet-two-nodes"
	// Sample payload
	payload := []byte("two-nodes")

	latency := SPREAD_SIMNET_TWO_NODES_LATENCY

	netCreationStart := time.Now()

	// Map IP addresses to node idx
	idxByIP := map[string]int{
		simnet.IntToPublicIPv4(0).String(): 0,
		simnet.IntToPublicIPv4(1).String(): 1,
	}
	// Create network with 2 nodes and with the configured latency
	network, meta, err := simlibp2p.SimpleLibp2pNetwork([]simlibp2p.NodeLinkSettingsAndCount{
		{
			LinkSettings: simnet.NodeBiDiLinkSettings{
				Downlink: simnet.LinkSettings{BitsPerSecond: 20 * simlibp2p.OneMbps, MTU: 1400},
				Uplink:   simnet.LinkSettings{BitsPerSecond: 20 * simlibp2p.OneMbps, MTU: 1400},
			},
			Count: 2,
		},
	}, func(p *simnet.Packet) time.Duration {
		from, fok := p.From.(*net.UDPAddr)
		to, tok := p.To.(*net.UDPAddr)
		if !fok || !tok {
			return 0
		}
		a, aok := idxByIP[from.IP.String()]
		b, bok := idxByIP[to.IP.String()]
		if !aok || !bok || a == b {
			return 0
		}
		return latency
	}, simlibp2p.NetworkSettings{UseBlankHost: true})
	if err != nil {
		t.Fatalf("build two-nodes simnet network: %v", err)
	}

	// Start network
	network.Start()
	defer network.Close()
	defer func() {
		for _, h := range meta.Nodes {
			_ = h.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), SPREAD_SIMNET_TWO_NODES_TIMEOUT)
	defer cancel()

	// Build gossipsub instances on each node and join the topic
	ps0 := getGossipsub(ctx, meta.Nodes[0])
	ps1 := getGossipsub(ctx, meta.Nodes[1])

	// Join the topic on each node
	topic0, err := ps0.Join(topicName)
	if err != nil {
		t.Fatalf("join topic on node0: %v", err)
	}
	topic1, err := ps1.Join(topicName)
	if err != nil {
		t.Fatalf("join topic on node1: %v", err)
	}
	// Subscribe to the topic
	_, err = topic0.Subscribe()
	if err != nil {
		t.Fatalf("subscribe on node0: %v", err)
	}
	sub1, err := topic1.Subscribe()
	if err != nil {
		t.Fatalf("subscribe on node1: %v", err)
	}

	// Connect the nodes
	connectAll(t, meta.Nodes)

	netCreationTime := time.Since(netCreationStart)
	t.Logf("Network creation time: %s", netCreationTime)

	time.Sleep(2 * time.Second)

	// Publish a message on node0 and measure the time until it's received on node1
	start := time.Now()
	if err := topic0.Publish(ctx, payload); err != nil {
		t.Fatalf("publish from node0: %v", err)
	}

	// Wait for the message to be received on node1
	var observed time.Duration
	for {
		msg, err := sub1.Next(ctx)
		if err != nil {
			t.Fatalf("node1 receive: %v", err)
		}
		if !bytes.Equal(msg.Data, payload) {
			continue
		}
		observed = time.Since(start)
		break
	}

	stretch := float64(observed) / float64(latency)
	t.Logf("Latency: configured=%s observed=%s stretch=%.3f", latency, observed, stretch)
}
