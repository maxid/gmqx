package gmqx

import (
	"context"
	"time"
)

// ClusterEventType identifies a cross-node cluster event.
type ClusterEventType string

const (
	ClusterPublish ClusterEventType = "publish"
	ClusterKick    ClusterEventType = "kick"
)

// ClusterMessage is a cross-node cluster event.
//
// Every event carries the identity of the node that produced it: NodeID is the
// node that handed the event to the cluster, Origin is the node that first
// produced it and Seq is that origin's event sequence. A cluster backend stamps
// Origin and Seq on locally produced events and drops an event whose Origin is
// the receiving node, so a re-broadcast topology cannot loop. Received events
// are never forwarded: they die at the node that receives them.
type ClusterMessage struct {
	Type     ClusterEventType
	NodeID   string
	ClientID string
	Topic    string
	Payload  []byte
	QoS      byte
	Retain   bool
	Props    []byte

	// Origin is the node id of the node that first produced the event. An empty
	// Origin means "produced locally": the built-in TCP cluster fills it in
	// before the event leaves the node and never rewrites a received one.
	Origin string
	// Seq is the origin's monotonically increasing event sequence number,
	// starting at 1. Receivers use (Origin, Seq) to drop duplicates. Seq 0 means
	// "not stamped" and is delivered without deduplication.
	Seq uint64
}

// ClusterHandler receives remote events on a broker.
type ClusterHandler interface {
	OnRemotePublish(msg ClusterMessage)
	OnRemoteKick(msg ClusterMessage)
}

// Cluster is a pluggable cluster backend (e.g. Redis pub/sub, gossip).
//
// Spread must only be called with locally produced events: a backend may drop
// an event whose Origin belongs to another node instead of forwarding it.
//
// Start may refuse a nodeID that another live process already owns by returning
// an error that wraps ErrNodeIDInUse: two processes sharing an id would apply
// every event twice. A backend without shared state (the built-in TCP mesh)
// cannot arbitrate that and collapses duplicate links into one member instead.
type Cluster interface {
	Start(ctx context.Context, nodeID string, handler ClusterHandler) error
	Stop(ctx context.Context) error
	Spread(ctx context.Context, msg ClusterMessage) error
}

// Member is one node in a cluster backend's member view.
type Member struct {
	// NodeID is the node id the peer announced in its handshake.
	NodeID string
	// Addr is the peer's advertised cluster listen address.
	Addr string
	// Incarnation identifies the peer process; a restart produces a new one.
	Incarnation string
	// Since is when this incarnation became a member.
	Since time.Time
}

// MemberView is implemented by cluster backends that expose their member view.
// It is an optional interface: Cluster stays small so third-party backends do
// not have to implement membership.
type MemberView interface {
	Members() []Member
}

// subscriptionProvider is implemented by a ClusterHandler that can describe the
// topic filters its local sessions are subscribed to. The built-in TCP cluster
// advertises that summary to its peers for directed routing. A handler without
// it never advertises a summary, so peers treat it as unknown and send every
// publish to it (availability over precision).
type subscriptionProvider interface {
	subscriptionFilters() []string
}

// subscriptionChangeNotifier is implemented by cluster backends that want to
// push a fresh subscription summary as soon as local subscriptions change
// instead of waiting for the next periodic refresh. The broker calls it after a
// successful SUBSCRIBE/UNSUBSCRIBE and after a session with subscriptions goes
// away.
type subscriptionChangeNotifier interface {
	subscriptionsChanged()
}
