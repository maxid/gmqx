package gmqx

import "context"

// ClusterEventType identifies a cross-node cluster event.
type ClusterEventType string

const (
	ClusterPublish ClusterEventType = "publish"
	ClusterKick    ClusterEventType = "kick"
)

// ClusterMessage is a cross-node cluster event.
type ClusterMessage struct {
	Type     ClusterEventType
	NodeID   string
	ClientID string
	Topic    string
	Payload  []byte
	QoS      byte
	Retain   bool
	Props    []byte
}

// ClusterHandler receives remote events on a broker.
type ClusterHandler interface {
	OnRemotePublish(msg ClusterMessage)
	OnRemoteKick(msg ClusterMessage)
}

// Cluster is a pluggable cluster backend (e.g. Redis pub/sub, gossip).
type Cluster interface {
	Start(ctx context.Context, nodeID string, handler ClusterHandler) error
	Stop(ctx context.Context) error
	Spread(ctx context.Context, msg ClusterMessage) error
}
