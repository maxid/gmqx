// Package gmqx is a small, embeddable MQTT 3.1.1/5 broker and client library.
//
// Status: experimental scaffold (v0.0.0-dev). The API is not stable yet.
//
// This package is the facade: it declares no behavior of its own, it re-exports the
// engine and the role packages under one import path, because that is the path
// every consumer and every example uses. The implementations live in
//   - internal/persist  — the one persistence seam: sessions and retained messages in one gcache store;
//   - internal/engine   — the broker: lifecycle, conn.Host, clientId table,
//     session bookkeeping, pending wills, plugin host;
//   - conn/             — one physical connection;
//   - routing/          — the routing table and the shared-subscription election;
//   - session/          — the live session state (its persisted form lives in internal/persist);
//   - cluster/          — the gossip / TCP mesh / Redis backends;
//   - hook/             — the extension contracts;
//   - retained/         — the retained-message store;
//   - client/           — the MQTT client library.
//
// Every name below is a type alias, a constant alias or a forwarding function
// (source-layout rule L3), so `gmqx.Broker` and `engine.Broker` are the same type;
// the engine package is internal, so this is the only public path to it.
package gmqx

import (
	"context"

	"github.com/maxid/gmqx/cluster"
	"github.com/maxid/gmqx/hook"
	engine "github.com/maxid/gmqx/internal/engine"
)

// The broker engine lives in gmqx/internal/engine (docs/design/source-layout.md
// §4.3): its assembly layer is internal, and the root package re-exports its public
// types as aliases (rule L3), so `gmqx.Broker` and `engine.Broker` are the same type
// and the engine is reachable through exactly one public path.

type (
	// Broker is a single-node MQTT 3.1.1/5 broker.
	Broker = engine.Broker
	// Publication is one accepted publication the broker routes.
	Publication = engine.Publication
	// LocalTarget is one local session a publication matches.
	LocalTarget = engine.LocalTarget
	// Dispatcher is the routing policy of an accepted publication.
	Dispatcher = engine.Dispatcher
	// DispatcherBinder is implemented by a Dispatcher that needs the routing seam.
	DispatcherBinder = engine.DispatcherBinder
	// DefaultDispatcher is the built-in routing policy.
	DefaultDispatcher = engine.DefaultDispatcher
	// ByteRateMetrics is the optional byte-rate extension of MetricsManager.
	ByteRateMetrics = engine.ByteRateMetrics
	// ConnectionID identifies one physical connection.
	ConnectionID = engine.ConnectionID
)

// New builds a broker from cfg.
func New(cfg Config) (*Broker, error) { return engine.New(cfg) }

// NewDefaultDispatcher builds the built-in routing policy.
func NewDefaultDispatcher() *DefaultDispatcher { return engine.NewDefaultDispatcher() }

// The configuration lives with the engine in gmqx/internal/engine (docs/design/
// source-layout.md §4.1): Config, ClusterConfig and TLSConfig are one unit with the
// code that reads them. The root names are aliases, so a consumer keeps importing
// gmqx and holding server types (rule L3).

type (
	// Config is the broker configuration.
	Config = engine.Config
	// ClusterConfig holds cluster settings.
	ClusterConfig = engine.ClusterConfig
	// TLSConfig holds the listener TLS configuration.
	TLSConfig = engine.TLSConfig
	// ConnectMode selects what a CONNECT with a client id in use does.
	ConnectMode = engine.ConnectMode
)

const (
	ConnectModeUnique = engine.ConnectModeUnique
	ConnectModeKick   = engine.ConnectModeKick
)

// The sentinels are defined with the engine in gmqx/internal/engine (docs/design/
// source-layout.md §4.1); the root names are aliases, so errors.Is matches either
// import path.

var (
	ErrUnauthorized         = engine.ErrUnauthorized
	ErrBadAuthMethod        = engine.ErrBadAuthMethod
	ErrMalformedPacket      = engine.ErrMalformedPacket
	ErrUnsupportedProtocol  = engine.ErrUnsupportedProtocol
	ErrClientIDInUse        = engine.ErrClientIDInUse
	ErrMaxConnections       = engine.ErrMaxConnections
	ErrTakeoverRateExceeded = engine.ErrTakeoverRateExceeded
	ErrClientIDRequired     = engine.ErrClientIDRequired
	// ErrBadTopic reports a topic name that is empty or contains a wildcard.
	ErrBadTopic           = engine.ErrBadTopic
	ErrNamespaceMismatch  = engine.ErrNamespaceMismatch
	ErrClusterUnavailable = engine.ErrClusterUnavailable
	ErrNodeIDInUse        = engine.ErrNodeIDInUse
)

// The extension contracts live in ./hook; the engine that implements the host side
// lives in ./engine. Every name below is an alias, so a consumer keeps importing
// this package and still gets the hook type: one definition, two import paths. New
// code may import github.com/maxid/gmqx/hook directly — both spellings are the same
// type, so they mix freely.

type (
	ConnectInfo    = hook.ConnectInfo
	AuthManager    = hook.AuthManager
	AuthStatus     = hook.AuthStatus
	AuthStep       = hook.AuthStep
	AuthExchange   = hook.AuthExchange
	AclAction      = hook.AclAction
	SessionInfo    = hook.SessionInfo
	AclManager     = hook.AclManager
	ConnInfo       = hook.ConnInfo
	Disposition    = hook.Disposition
	PublishResult  = hook.PublishResult
	PublishEvent   = hook.PublishEvent
	EventSink      = hook.EventSink
	MetricsManager = hook.MetricsManager
	NoopMetrics    = hook.NoopMetrics
	Interceptor    = hook.Interceptor

	// AllowAll permits every connection/subscription/publish.
	AllowAll = engine.AllowAll
	// DenyAll rejects every connection.
	DenyAll = engine.DenyAll
)

const (
	// MaxAuthSteps is the number of AUTH challenges one exchange may run.
	MaxAuthSteps = engine.MaxAuthSteps

	AuthContinue  = hook.AuthContinue
	AuthSucceeded = hook.AuthSucceeded
	AuthFailed    = hook.AuthFailed

	AclSubscribe = hook.AclSubscribe
	AclPublish   = hook.AclPublish

	PublishAccepted  = hook.PublishAccepted
	PublishRetryable = hook.PublishRetryable
	PublishRejected  = hook.PublishRejected

	ReasonUnspecified            = hook.ReasonUnspecified
	ReasonProtocolError          = hook.ReasonProtocolError
	ReasonNotAuthorized          = hook.ReasonNotAuthorized
	ReasonBadAuthMethod          = hook.ReasonBadAuthMethod
	ReasonSessionTakenOver       = hook.ReasonSessionTakenOver
	ReasonQuotaExceeded          = hook.ReasonQuotaExceeded
	ReasonConnectionRateExceeded = hook.ReasonConnectionRateExceeded
)

// ContextWithConn forwards to hook.ContextWithConn, so a consumer needs one import.
func ContextWithConn(ctx context.Context, info ConnInfo) context.Context {
	return hook.ContextWithConn(ctx, info)
}

// ConnFromContext forwards to hook.ConnFromContext.
func ConnFromContext(ctx context.Context) (ConnInfo, bool) { return hook.ConnFromContext(ctx) }

// The cluster contract lives in gmqx/cluster (docs/design/source-layout.md §2, rule
// L3): the root package re-exports every type and constant as an alias, so an import
// of either path names the same type and existing code needs no change. The
// backends and the broker-side adapter live in gmqx/cluster and gmqx/engine.

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
	// ClusterSessionReleased tells the peers that this node took a stored session
	// out of the shared store, so the node that wrote it drops its offline route.
	ClusterSessionReleased = cluster.ClusterSessionReleased
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
