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
// process without touching the broker; this package deliberately ships no
// network or disk backend.
//
// The broker calls the store with its own session lock held, so an implementation
// must not call back into the broker. Save receives a state the broker no longer
// references, and Load must return a state the caller may keep: a store must
// never hand out a map it still writes to. The payload and property bytes of a
// QueuedMessage are immutable, so sharing them is enough.
type SessionStore interface {
	// Load returns the state stored under key.
	Load(key string) (SessionState, bool)
	// Save stores state under key, replacing any previous value.
	Save(key string, state SessionState)
	// Delete removes key. Deleting a missing key is a no-op.
	Delete(key string)
	// Keys returns the keys that currently hold a session, in any order. The
	// broker uses it to route a publication to the persistent sessions that are
	// offline and to find the ones whose expiry elapsed; a store that could not
	// enumerate its sessions would make both impossible.
	Keys() []string
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
type SessionSubscription struct {
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
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
// process — that is the behavior every deployment had before the seam existed.
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
			out.Subscriptions[filter] = sub
		}
	}
	if len(state.Queue) > 0 {
		out.Queue = append([]QueuedMessage(nil), state.Queue...)
	}
	return out
}
