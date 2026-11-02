package server

// The session store lives in gmqx/session (docs/design/source-layout.md §2, rule L3):
// the root package re-exports its types as aliases and forwards the constructors, so
// Config.SessionStore, an implementation outside this module (contrib/sessionstore-
// gcache) and the stores this package ships all name the same types as before.

import sessionstore "github.com/maxid/gmqx/session"

type (
	// SessionStore is the persistence seam of persistent-session state.
	SessionStore = sessionstore.SessionStore
	// SharedSessionStore is a SessionStore that several broker nodes share.
	SharedSessionStore = sessionstore.SharedSessionStore
	// SessionState is the state of one persistent session with no live connection.
	SessionState = sessionstore.SessionState
	// InflightStage is where a stored outbound exchange stood when its connection
	// ended.
	InflightStage = sessionstore.InflightStage
	// SessionInflight is one outbound QoS 1/2 exchange a session owes its client.
	SessionInflight = sessionstore.SessionInflight
	// SessionSubscription is one subscription of a stored sessionstore.
	SessionSubscription = sessionstore.SessionSubscription
	// QueuedMessage is one message queued for an offline persistent sessionstore.
	QueuedMessage = sessionstore.QueuedMessage
	// MemorySessionStore is the default node-local store.
	MemorySessionStore = sessionstore.MemorySessionStore
	// RedisSessionStore is the store shared by the nodes of a Redis cluster.
	RedisSessionStore = sessionstore.RedisSessionStore
	// RedisSessionStoreConfig configures the Redis-backed store.
	RedisSessionStoreConfig = sessionstore.RedisSessionStoreConfig
)

const (
	InflightAwaitPuback  = sessionstore.InflightAwaitPuback
	InflightAwaitPubrec  = sessionstore.InflightAwaitPubrec
	InflightAwaitPubcomp = sessionstore.InflightAwaitPubcomp
)

// NewMemorySessionStore builds the default node-local store.
func NewMemorySessionStore() *MemorySessionStore { return sessionstore.NewMemorySessionStore() }

// NewRedisSessionStore builds the Redis-backed store and validates its
// configuration.
func NewRedisSessionStore(cfg RedisSessionStoreConfig) (*RedisSessionStore, error) {
	return sessionstore.NewRedisSessionStore(cfg)
}

// NewRedisClusterWithSessionStore builds the Redis cluster backend and the Redis
// session store of one cluster from one RedisClusterConfig: both use the same
// address, database and key prefix, so the sessions a node stores are the ones its
// peers take over.
func NewRedisClusterWithSessionStore(cfg RedisClusterConfig) (Cluster, *RedisSessionStore, error) {
	return sessionstore.NewRedisClusterWithSessionStore(cfg)
}

// sessionTaker is implemented by a SessionStore that can remove a stored session
// atomically and hand it to one caller. A shared store needs it: Load followed by
// Delete is two round trips, so two nodes reconnecting the same client id at the
// same time could both see the session and both serve it. The method is exported
// so an implementation outside this module can provide it; the interface is
// unexported because only the broker decides when to take a sessionstore.
type sessionTaker interface {
	Take(key string) (SessionState, bool)
}
