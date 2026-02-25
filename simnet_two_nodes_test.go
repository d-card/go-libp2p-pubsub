package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/vivaldi"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	simnet "github.com/marcopolo/simnet"
)

// Usage: go test -tags simnet -run TestSimnetTwoNodesDirectLatency -v .
// Replace latency value to test different scenarios

const (
	SPREAD_SIMNET_TWO_NODES_LATENCY = 120 * time.Millisecond // Latency between the two nodes
	SPREAD_SIMNET_TWO_NODES_TIMEOUT = 90 * time.Second       // Overall timeout for the test
)

type twoNodeScenarioResult struct {
	observed time.Duration
	stretch  float64
}

// Tests two-node direct latency first with Gossipsub, then with Spread, using
// comparable configuration knobs from the larger spread experiment.
func TestSimnetTwoNodesDirectLatency(t *testing.T) {
	gs := runTwoNodeScenario(t, false)
	t.Logf("[gossipsub] configured=%s observed=%s stretch=%.3f",
		SPREAD_SIMNET_TWO_NODES_LATENCY, gs.observed, gs.stretch)

	sp := runTwoNodeScenario(t, true)
	t.Logf("[spread] configured=%s observed=%s stretch=%.3f",
		SPREAD_SIMNET_TWO_NODES_LATENCY, sp.observed, sp.stretch)
}

func runTwoNodeScenario(t *testing.T, useSpread bool) twoNodeScenarioResult {
	t.Helper()
	label := "gossipsub"
	if useSpread {
		label = "spread"
	}
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
		return SPREAD_SIMNET_TWO_NODES_LATENCY
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

	baseOpts := []Option{
		WithGossipSubParams(func() GossipSubParams {
			p := DefaultGossipSubParams()
			p.HeartbeatInitialDelay = 300 * time.Millisecond
			p.HeartbeatInterval = 1000 * time.Millisecond
			return p
		}()),
	}
	opts0 := append([]Option{}, baseOpts...)
	opts1 := append([]Option{}, baseOpts...)
	if useSpread {
		v0 := vivaldi.NewService(meta.Nodes[0], &vivaldi.Config{Timeout: 500 * time.Millisecond})
		v1 := vivaldi.NewService(meta.Nodes[1], &vivaldi.Config{Timeout: 500 * time.Millisecond})
		opts0 = append(opts0,
			WithProtocolChoice(SPREAD),
			withSpreadExtensionAdvertise(),
			WithVivaldi(v0, &VivaldiConfig{
				Cc:               0.25,
				Ce:               0.25,
				Newton:           true,
				OutlierThreshold: 0,
				Samples:          1,
				Interval:         150 * time.Millisecond,
				NeighborSetSize:  24,
				IN1ThresholdMS:   20,
				IN2ThresholdMS:   35,
				IN3MADKRandom:    5,
				IN3MADKClose:     8,
				IN3MinSamples:    4,
			}),
		)
		opts1 = append(opts1,
			WithProtocolChoice(SPREAD),
			withSpreadExtensionAdvertise(),
			WithVivaldi(v1, &VivaldiConfig{
				Cc:               0.25,
				Ce:               0.25,
				Newton:           true,
				OutlierThreshold: 0,
				Samples:          1,
				Interval:         150 * time.Millisecond,
				NeighborSetSize:  24,
				IN1ThresholdMS:   20,
				IN2ThresholdMS:   35,
				IN3MADKRandom:    5,
				IN3MADKClose:     8,
				IN3MinSamples:    4,
			}),
		)
	}

	ps0 := getGossipsub(ctx, meta.Nodes[0], opts0...)
	ps1 := getGossipsub(ctx, meta.Nodes[1], opts1...)

	topicName := fmt.Sprintf("spread-simnet-two-nodes-%s", label)
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
	t.Logf("[%s] network creation time: %s", label, netCreationTime)

	time.Sleep(2 * time.Second)

	payload := []byte(fmt.Sprintf("two-nodes-%s", label))
	start := time.Now()
	if useSpread {
		if err := publishWithSpread(ctx, topic0, payload); err != nil {
			t.Fatalf("publish spread from node0: %v", err)
		}
	} else {
		if err := topic0.Publish(ctx, payload); err != nil {
			t.Fatalf("publish gossipsub from node0: %v", err)
		}
	}

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

	stretch := float64(observed) / float64(SPREAD_SIMNET_TWO_NODES_LATENCY)
	return twoNodeScenarioResult{observed: observed, stretch: stretch}
}
