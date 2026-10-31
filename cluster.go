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
	// ClusterTarget is a directed publish: deliver one publication to one client
	// id, on whichever node holds it (see Broker.PublishToClient). It carries
	// ClientID plus the publication fields of ClusterPublish. A receiving node
	// applies it only when the client id is a session of its own
	// ClusterMessage.Namespace; a node that does not hold the id drops it and
	// counts it (Metrics.RecordDroppedMessage("target_unknown")).
	//
	// Routing a target event is the backend's choice: a backend with a client-id
	// presence view (the Redis backend) sends it to the one node that holds the
	// client, and the built-in TCP mesh, which has no presence, broadcasts it to
	// every peer and lets each node apply it only if it holds that client id (see
	// the TCP backend's Spread doc comment). Namespace scoping and (Origin, Seq)
	// deduplication are the ones every cluster event uses. A peer running an older
	// build that does not know the type drops the frame in its receive path instead
	// of misapplying it, so a rolling upgrade degrades a directed publish to a lost
	// one — which is the transport's at-most-once behaviour anyway.
	ClusterTarget ClusterEventType = "target"
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
	// Namespace is the isolation scope the event belongs to: the broker stamps
	// Config.Namespace on every event it produces and a backend drops an event
	// whose namespace is not its own, so two namespaces sharing one cluster never
	// apply each other's publishes, retained state or kicks. The empty string is
	// the default namespace, which keeps the pre-namespace wire behaviour.
	Namespace string

	// Shared names the shared subscription groups this event must deliver, one
	// entry per node: the entry whose Node is the receiving node names the groups
	// that node has to deliver one member of (see shared.go). The producing node
	// elects them, so a group is delivered on exactly one node even when its
	// members live on several. An event that matches no shared group carries none,
	// and a peer that does not know the field (an older build) ignores it and
	// delivers no shared copy, which degrades a shared subscription to "no message
	// across nodes" rather than to a duplicate.
	Shared []SharedDelivery

	// Origin is the node id of the node that first produced the event. An empty
	// Origin means "produced locally": the built-in TCP cluster fills it in
	// before the event leaves the node and never rewrites a received one.
	Origin string
	// Seq is the origin's monotonically increasing event sequence number,
	// starting at 1. Receivers use (Origin, Seq) to drop duplicates. Seq 0 means
	// "not stamped" and is delivered without deduplication.
	Seq uint64

	// ConnectTime is the epoch of a kick: when the connection that produced the
	// event was registered on its own node, in Unix nanoseconds from that node's
	// wall clock. It is set on ClusterKick only, and it is what lets a receiving
	// node tell a delayed or reordered kick from a current one instead of closing
	// a connection that is newer than the one the kick was meant to replace (see
	// Broker.OnRemoteKick).
	//
	// Clock assumption: the value is a wall-clock reading, because a monotonic
	// reading means nothing on another process. Cross-node ordering therefore
	// assumes the clocks of a cluster are NTP-synchronised to within the
	// Config.NotKickSeconds anti-flap window, which is what the receive-side
	// window bounds: skew larger than that window cannot revive an already
	// displaced connection, it can only make the peer refuse (or accept) a kick it
	// would have decided differently, and a refused kick never closes a younger
	// connection. Within one node nothing is compared across clocks: the local
	// anti-flap window uses the monotonic reading of the connection's start time.
	//
	// Zero means "not stamped": a peer running a build from before kick epochs
	// sends none, and its kick is applied with the pre-epoch behaviour (see
	// Broker.OnRemoteKick).
	ConnectTime int64
}

// ClusterHandler receives the remote events every backend delivers: a publish and
// a client-id kick. A directed publish arrives through the optional targetHandler
// (Broker implements it); a backend that receives a ClusterTarget hands it to that
// interface when the handler provides it.
type ClusterHandler interface {
	OnRemotePublish(msg ClusterMessage)
	OnRemoteKick(msg ClusterMessage)
}

// targetHandler is implemented by a ClusterHandler that can receive a directed
// publish (ClusterTarget). It is an optional interface so the handler contract
// stays additive: a backend delivers a target event to a handler that implements
// it, and counts the event as undeliverable otherwise. Broker implements it.
type targetHandler interface {
	OnRemoteTarget(msg ClusterMessage)
}

// clientPresence is implemented by a cluster backend that tracks which node holds
// a client id, so a directed publish can be sent to that node instead of to every
// node. The broker tells the backend about every admitted CONNECT and every
// connection that leaves without a replacement, outside the broker's lock: an
// implementation must not block, and must tolerate a lost or late notification
// (presence is a TTL'd hint, refreshed by the backend, not a source of truth).
type clientPresence interface {
	clientConnected(clientID string)
	clientDisconnected(clientID string)
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

// SharedDelivery is the shared-subscription half of one cluster publish: the
// shared subscription groups one node must deliver one member of (see shared.go).
// The groups are full shared filters ("$share/{ShareName}/{filter}"), because that
// is what identifies a group; the node picks the member and attaches the member's
// own subscription identifiers.
type SharedDelivery struct {
	// Node is the node id that must deliver the groups.
	Node string `json:"node"`
	// Groups are the shared subscription group keys it must deliver.
	Groups []string `json:"groups"`
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

// sharedSubscriptionProvider is implemented by a ClusterHandler that can describe
// the shared subscription groups its local sessions hold a live member of. It is
// the shared half of the advertised summary: a peer needs it to elect one node per
// shared group (see shared.go), so a handler without it leaves shared
// subscriptions local to the node that produced the publication.
type sharedSubscriptionProvider interface {
	sharedSubscriptionGroups() []string
}

// namespaceScoped is implemented by cluster backends that scope their traffic to
// one broker namespace. The broker calls setNamespace with its Config.Namespace
// before Cluster.Start, so the broker's namespace is authoritative for a backend
// attached to it. A backend that does not implement it is not scoped by the
// broker: it still receives ClusterMessage.Namespace and may filter on it.
type namespaceScoped interface {
	setNamespace(namespace string)
}

// subscriptionChangeNotifier is implemented by cluster backends that want to
// push a fresh subscription summary as soon as local subscriptions change
// instead of waiting for the next periodic refresh. The broker calls it after a
// successful SUBSCRIBE/UNSUBSCRIBE and after a session with subscriptions goes
// away.
type subscriptionChangeNotifier interface {
	subscriptionsChanged()
}
