package gmqx

import (
	"sync"
	"time"
)

// SessionStore is the persistence seam of persistent-session state: what a
// session keeps while no connection owns it, namely its subscriptions, the
// messages published to it while it was offline, and the deadline its Session
// Expiry Interval sets.
//
// New installs MemorySessionStore, so a broker that does not configure a store
// keeps the previous behavior. Config.SessionStore accepts another
// implementation, which is how a deployment moves session state out of the
// process without touching the broker. This package ships two: MemorySessionStore
// (the default, node-local) and RedisSessionStore (opt-in, shared by the nodes of
// a Redis cluster, see NewRedisSessionStore).
//
// The broker calls the store with its own session lock held, so an implementation
// must not call back into the broker. Save receives a state the broker no longer
// references, and Load must return a state the caller may keep: a store must
// never hand out a map it still writes to. The payload and property bytes of a
// QueuedMessage are immutable, so sharing them is enough.
//
// The interface stays this small on purpose; the two behaviors a store that
// several nodes share must have are optional interfaces, exactly like the
// optional cluster interfaces:
//
//   - SharedSessionStore says that peers of a cluster read and write the same
//     store, which changes how a connection that a peer displaced is torn down
//     (see Broker.unregister);
//   - sessionTaker (implement the exported Take method) makes "load and remove"
//     atomic, which is what keeps two nodes from taking the same session at the
//     same time. A store without it is taken with Load followed by Delete.
//
// NewRedisSessionStore is the shared implementation this package ships.
type SessionStore interface {
	// Load returns the state stored under key.
	Load(key string) (SessionState, bool)
	// Save stores state under key, replacing any previous value.
	Save(key string, state SessionState)
	// Delete removes key. Deleting a missing key is a no-op.
	Delete(key string)
	// Keys returns the keys that currently hold a session, in any order. Routing
	// does not use it: the broker indexes the subscriptions of a session when it
	// saves it, so a publish never calls the store. Keys is used once by New, to
	// index the sessions a deployment restored into the store before the broker
	// existed, and by the expiry sweeper, which has to enumerate the deadlines to
	// enforce; a store that could not enumerate its sessions would make those two
	// impossible. A store that another process keeps writing while this broker runs
	// is not re-read.
	Keys() []string
}

// SharedSessionStore is the optional interface of a SessionStore that several
// broker nodes read and write at the same time: the store is the single copy of a
// session, not a node-local one. The Redis-backed store implements it.
//
// The broker uses it when a connection is displaced by a peer's kick. A kick means
// the client id now belongs to another node, and with a shared store that node is
// the owner of the stored session too: the displaced node must neither save its
// own copy back (which would resurrect a session the other node is serving, and
// let two nodes serve one session) nor delete the key (which would erase the state
// the other node just saved when its client disconnected). With a node-local store
// the old behavior stays: the state of a kicked persistent session is kept where
// it is, because it is only ever reachable through this node.
//
// A store that reports itself shared should also implement Take: a shared store is
// written by several processes, so the Load-then-Delete fallback can hand one
// session to two nodes.
type SharedSessionStore interface {
	// SharedAcrossNodes reports that more than one broker node may read and write
	// this store at the same time.
	SharedAcrossNodes() bool
}

// sessionTaker is implemented by a SessionStore that can remove a stored session
// atomically and hand it to one caller. A shared store needs it: Load followed by
// Delete is two round trips, so two nodes reconnecting the same client id at the
// same time could both see the session and both serve it. The method is exported
// so an implementation outside this package can provide it; the interface is
// unexported because only the broker decides when to take a session.
type sessionTaker interface {
	Take(key string) (SessionState, bool)
}

// SessionState is the state of one persistent session that has no live
// connection. The key it is stored under is namespaceKey(Config.Namespace,
// ClientID).
type SessionState struct {
	// ClientID is the MQTT client identifier of the session.
	ClientID string
	// Subscriptions are the session's topic filters, keyed by filter.
	Subscriptions map[string]SessionSubscription
	// Queue holds the messages published to the session while it was offline, in
	// publication order. The broker drains them FIFO right after the CONNACK and
	// assigns packet ids at drain time, so a queued message keeps no packet id.
	Queue []QueuedMessage
	// ExpiresAt is when the session expires. The zero time means the session does
	// not expire: that is the MQTT 3.1.1 case, where the protocol has no Session
	// Expiry Interval, and it is what a deployment that never sets one keeps.
	ExpiresAt time.Time
}

// SessionSubscription is one stored subscription of a session.
//
// It carries every option that decides what the subscription delivers, the
// Subscription Identifier included: that identifier is state of the subscription,
// not of the connection, so it survives a session that expires out and is resumed
// elsewhere (MQTT 5 §3.8.3.1). A store written by a build that predates the field
// decodes it as "no identifier", which is what such a subscription had.
type SessionSubscription struct {
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
	// SubscriptionIdentifiers are the identifiers attached to every PUBLISH this
	// subscription causes, deduplicated and capped (see subscription_id.go).
	SubscriptionIdentifiers []uint32
}

// QueuedMessage is one message a persistent session owes its client. It carries
// what a PUBLISH needs — topic, QoS, retain flag, payload and MQTT 5 properties —
// but no packet id: that is assigned when the queue drains.
type QueuedMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	// Props is the MQTT 5 PUBLISH property block without its length prefix, the
	// same shape Publication.Props uses. It is nil for MQTT 3.1.1 traffic.
	Props []byte
}

// MemorySessionStore is the default SessionStore: an in-process map guarded by a
// RWMutex. Its lifetime is the broker's, so session state does not survive the
// process — that is the behavior every deployment had before the seam existed. It
// is not a SharedSessionStore: one broker owns it, so its state is node-local and
// a peer's kick leaves the stored session where it is.
type MemorySessionStore struct {
	mu       sync.RWMutex
	sessions map[string]SessionState
}

// NewMemorySessionStore returns an empty in-memory session store. A broker
// created without Config.SessionStore gets one of its own; constructing the store
// explicitly is only needed to share or preload it.
func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{sessions: make(map[string]SessionState)}
}

// Load implements SessionStore.
func (s *MemorySessionStore) Load(key string) (SessionState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.sessions[key]
	if !ok {
		return SessionState{}, false
	}
	return copySessionState(state), true
}

// Save implements SessionStore.
func (s *MemorySessionStore) Save(key string, state SessionState) {
	s.mu.Lock()
	if s.sessions == nil {
		s.sessions = make(map[string]SessionState)
	}
	s.sessions[key] = copySessionState(state)
	s.mu.Unlock()
}

// Delete implements SessionStore.
func (s *MemorySessionStore) Delete(key string) {
	s.mu.Lock()
	delete(s.sessions, key)
	s.mu.Unlock()
}

// Keys implements SessionStore.
func (s *MemorySessionStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.sessions))
	for key := range s.sessions {
		keys = append(keys, key)
	}
	return keys
}

// copySessionState detaches a state from the caller: the subscription map and the
// queue slice are new, while the payload and property bytes are shared because
// nothing mutates them once a message is queued.
func copySessionState(state SessionState) SessionState {
	out := SessionState{ClientID: state.ClientID, ExpiresAt: state.ExpiresAt}
	if len(state.Subscriptions) > 0 {
		out.Subscriptions = make(map[string]SessionSubscription, len(state.Subscriptions))
		for filter, sub := range state.Subscriptions {
			// The identifier slice is copied with the subscription: a store must
			// never share mutable state with its caller.
			sub.SubscriptionIdentifiers = append([]uint32(nil), sub.SubscriptionIdentifiers...)
			out.Subscriptions[filter] = sub
		}
	}
	if len(state.Queue) > 0 {
		out.Queue = append([]QueuedMessage(nil), state.Queue...)
	}
	return out
}
