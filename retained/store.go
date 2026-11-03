// Package retained holds the retained-message table of a broker.
//
// A retained message is the value a new subscriber receives for a topic: the broker
// keeps one per (namespace, topic) and replays it when a subscription matches. The
// table is the in-memory copy of that state — bounded, keyed by the broker's
// namespace key and carrying the MQTT 5 Message Expiry deadline — while the policy
// that decides when to store, drop or replay a message stays with the broker, which
// is the only place that knows the configuration, the metrics and the sessions to
// deliver to.
//
// Persistence is not this package's business: gmqx/internal/persist writes the same
// messages into the configured gcache store, and the broker keeps this table as the
// hot copy that a SUBSCRIBE scans.
package retained

import (
	"sync"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// Message is one retained publication as the store keeps it.
type Message struct {
	// Namespace scopes the message: a replay only matches messages of the
	// subscriber's namespace.
	Namespace string
	// Topic is the topic the message was published to.
	Topic string
	// Payload is the retained value; the store owns the copy it is given.
	Payload []byte
	// QoS is the QoS of the publish that set the value.
	QoS byte
	// Props is the typed MQTT 5 property set, minus the Message Expiry Interval,
	// which is kept as ExpiresAt instead.
	Props mqtt.Properties
	// ExpiresAt is when the store may drop the message: the time it was stored plus
	// its Message Expiry Interval. The zero time means it never expires — no
	// interval on the publish, or MQTT 3.1.1, which has none.
	ExpiresAt time.Time
}

// Table is the retained-message table. The zero value is not usable; use NewTable.
// It is safe for concurrent use.
type Table struct {
	mu   sync.Mutex
	msgs map[string]Message
}

// NewStore builds an empty retained store.
func NewTable() *Table { return &Table{msgs: make(map[string]Message)} }

// Len reports how many topics the store holds.
func (s *Table) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

// Get returns the message stored under key.
func (s *Table) Get(key string) (Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.msgs[key]
	return m, ok
}

// Delete removes key. Deleting a missing key is a no-op.
func (s *Table) Delete(key string) {
	s.mu.Lock()
	delete(s.msgs, key)
	s.mu.Unlock()
}

// Put stores m under key. max bounds the number of topics the store keeps; a
// message for a key the store already holds is always accepted, because replacing
// a value does not grow the table. Put reports false and leaves the store
// unchanged when key is new and the table already holds max topics.
func (s *Table) Put(key string, m Message, max int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.msgs[key]; !exists && max > 0 && int64(len(s.msgs)) >= max {
		return false
	}
	s.msgs[key] = m
	return true
}

// Range calls fn for every message in the store. fn may request the removal of the
// message it was given by returning false; any other value keeps it. Range holds
// the store's lock, so fn must not call back into the store.
func (s *Table) Range(fn func(key string, m Message) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, m := range s.msgs {
		if !fn(key, m) {
			delete(s.msgs, key)
		}
	}
}
