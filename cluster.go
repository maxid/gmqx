package gmqx

// The cluster contract lives in gmqx/cluster (docs/design/source-layout.md §2, rule
// L3): the root package re-exports every type and constant as an alias, so an import
// of either path names the same type and existing code needs no change. The
// implementation and its configuration types live there too; this file is the facade
// the broker and existing consumers keep importing.

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

	// GossipClusterConfig configures the built-in gossip cluster backend.
	GossipClusterConfig = cluster.GossipClusterConfig
	// TCPClusterConfig configures the built-in TCP mesh cluster backend.
	TCPClusterConfig = cluster.TCPClusterConfig
	// RedisClusterConfig configures the built-in Redis cluster backend.
	RedisClusterConfig = cluster.RedisClusterConfig
	// SubscriptionSummary is the routing metadata one gossip node advertises.
	SubscriptionSummary = cluster.SubscriptionSummary
)

const (
	ClusterPublish = cluster.ClusterPublish
	ClusterKick    = cluster.ClusterKick
	ClusterTarget  = cluster.ClusterTarget
)

const (
	MemberAlive   = cluster.MemberAlive
	MemberSuspect = cluster.MemberSuspect
	MemberDead    = cluster.MemberDead
)

// NewGossipCluster builds the gossip backend from cfg. It is a forwarder: the
// backend lives in gmqx/cluster, and the root name stays so existing code compiles
// unchanged (rule L3).
func NewGossipCluster(cfg GossipClusterConfig) Cluster { return cluster.NewGossipCluster(cfg) }

// NewTCPCluster builds the TCP mesh backend from cfg.
func NewTCPCluster(cfg TCPClusterConfig) Cluster { return cluster.NewTCPCluster(cfg) }

// NewRedisCluster builds the Redis backend from cfg.
func NewRedisCluster(cfg RedisClusterConfig) Cluster { return cluster.NewRedisCluster(cfg) }

// backendHandler is the ClusterHandler the broker hands a backend at Start. It
// forwards the event sinks to the broker and adds the exported method names the
// backends of gmqx/cluster type-assert (cluster.SubscriptionProvider and
// cluster.SharedSubscriptionProvider). It exists as an adapter instead of growing
// those methods on Broker: the remote-event hooks are the broker's public surface,
// while the two summary halves are a collaboration detail only a backend reads.
type backendHandler struct{ b *Broker }

func (h backendHandler) OnRemotePublish(msg ClusterMessage) { h.b.OnRemotePublish(msg) }
func (h backendHandler) OnRemoteKick(msg ClusterMessage)    { h.b.OnRemoteKick(msg) }
func (h backendHandler) OnRemoteTarget(msg ClusterMessage)  { h.b.OnRemoteTarget(msg) }

// SubscriptionFilters implements cluster.SubscriptionProvider.
func (h backendHandler) SubscriptionFilters() []string { return h.b.subscriptionFilters() }

// SharedSubscriptionGroups implements cluster.SharedSubscriptionProvider.
func (h backendHandler) SharedSubscriptionGroups() []string { return h.b.sharedSubscriptionGroups() }

// The collaboration seams below are implemented by a cluster backend and called by
// the broker. The interfaces stay in the root package because they are what the
// broker asks for; their methods are exported because the backend that implements
// them lives in gmqx/cluster. A backend that does not implement one is simply not
// given that job.

// namespaceScoped is implemented by cluster backends that scope their traffic to
// one broker namespace. The broker calls SetNamespace with its Config.Namespace
// before Cluster.Start, so the broker's namespace is authoritative for a backend
// attached to it. A backend that does not implement it is not scoped by the
// broker: it still receives ClusterMessage.Namespace and may filter on it.
type namespaceScoped interface {
	SetNamespace(namespace string)
}

// subscriptionChangeNotifier is implemented by cluster backends that want to
// push a fresh subscription summary as soon as local subscriptions change
// instead of waiting for the next periodic refresh. The broker calls it after a
// successful SUBSCRIBE/UNSUBSCRIBE and after a session with subscriptions goes
// away.
type subscriptionChangeNotifier interface {
	SubscriptionsChanged()
}

// clientPresence is implemented by a cluster backend that tracks which node holds
// a client id, so a directed publish can be sent to that node instead of to every
// node. The broker tells the backend about every admitted CONNECT and every
// connection that leaves without a replacement, outside the broker's lock: an
// implementation must not block, and must tolerate a lost or late notification
// (presence is a TTL'd hint, refreshed by the backend, not a source of truth).
type clientPresence interface {
	ClientConnected(clientID string)
	ClientDisconnected(clientID string)
}
