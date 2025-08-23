package pubsub

const (
	GOSSIPSUB = 1
	HIERARCHICAL_GOSSIP = 2
	SPATIAL_GOSSIP = 3
	DANDELION_GOSSIP = 4
)

type GossipProtocolChoice int