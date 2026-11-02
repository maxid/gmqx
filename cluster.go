package gmqx

// The cluster contract lives in gmqx/cluster (docs/design/source-layout.md §2, rule
// L3): the root package re-exports every type and constant as an alias, so an import
// of either path names the same type and existing code needs no change. The
// implementation still lives here; the backends move next.

import "github.com/maxid/gmqx/cluster"

type (
	// ClusterEventType identifies a cross-node cluster event.
	ClusterEventType = cluster.ClusterEventType
	// ClusterMessage is a cross-node cluster event.
	ClusterMessage = cluster.ClusterMessage
	// ClusterHandler receives the remote events every backend delivers.
	ClusterHandler = cluster.ClusterHandler
	// Cluster is a pluggable cluster backend.
	Cluster = cluster.Cluster
	// SharedDelivery is the shared-subscription half of one cluster publish.
	SharedDelivery = cluster.SharedDelivery
	// MemberState is the liveness of one node in a member view.
	MemberState = cluster.MemberState
	// Member is one node in a cluster backend's member view.
	Member = cluster.Member
	// MemberView is implemented by backends that expose their member view.
	MemberView = cluster.MemberView
	// ClusterStats is a snapshot of one backend's counters.
	ClusterStats = cluster.ClusterStats
	// StatsView is implemented by backends that expose their counters.
	StatsView = cluster.StatsView
	// Link is one transport connection of a cluster node.
	Link = cluster.Link
	// LinkView is implemented by backends that expose their transport links.
	LinkView = cluster.LinkView
	// PresenceView is implemented by backends that track client-id presence.
	PresenceView = cluster.PresenceView
	// Routed is one peer as a backend's routing view holds it.
	Routed = cluster.Routed
	// RoutingView is implemented by backends that route through a cached view.
	RoutingView = cluster.RoutingView
)

const (
	ClusterPublish = cluster.ClusterPublish
	ClusterKick    = cluster.ClusterKick
	ClusterTarget  = cluster.ClusterTarget
)

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

const (
	MemberAlive   = cluster.MemberAlive
	MemberSuspect = cluster.MemberSuspect
	MemberDead    = cluster.MemberDead
)

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
