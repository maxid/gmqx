package gmqx

// The cluster contract lives in gmqx/cluster (docs/design/source-layout.md §2, rule
// L3): the root package re-exports every type and constant as an alias, so an import
// of either path names the same type and existing code needs no change. The
// backends and the broker-side adapter live in gmqx/cluster and gmqx/server.

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

// NewGossipCluster builds the gossip backend from cfg.
func NewGossipCluster(cfg GossipClusterConfig) Cluster { return cluster.NewGossipCluster(cfg) }

// NewTCPCluster builds the TCP mesh backend from cfg.
func NewTCPCluster(cfg TCPClusterConfig) Cluster { return cluster.NewTCPCluster(cfg) }

// NewRedisCluster builds the Redis backend from cfg.
func NewRedisCluster(cfg RedisClusterConfig) Cluster { return cluster.NewRedisCluster(cfg) }
