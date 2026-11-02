// Package clusterkit is the machinery the cluster backends share: the
// duplicate-suppression table and the subscription summary they advertise.
//
// It is internal because no consumer of this package can use these types: they exist
// for the TCP, gossip and Redis backends in the root package, and keeping them here is
// what stops three backends from each growing their own copy.
package clusterkit

import (
	"sync"
	"time"
)

// Key identifies one cluster event: the origin node id and that origin's
// monotonically increasing sequence number.
type Key struct {
	Origin string
	Seq    uint64
}

// Table remembers recently applied event identities so a redelivered
// event — a reconnected peer replaying its queue, a duplicate link delivering
// the same frame twice — is applied once.
//
// The table is bounded twice: by slots (at most size entries are remembered;
// the oldest is overwritten first) and by TTL (an entry stops suppressing
// duplicates after ttl). Memory is therefore O(size) regardless of traffic, and
// the expiry sweep is lazy: an insert reclaims a small batch of expired slots
// instead of running a timer or a background goroutine. Because the TTL is
// constant, slots expire in insertion order, which is why the sweep can stop at
// the first live slot.
type Table struct {
	mu    sync.Mutex
	slots []slot
	index map[Key]int
	next  int // slot the next insert overwrites
	sweep int // lazy sweep cursor
	ttl   time.Duration
}

type slot struct {
	key Key
	exp time.Time
}

// Seen reports whether key was already applied, and remembers it when it was
// new.
func (t *Table) Seen(key Key) bool {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if i, ok := t.index[key]; ok {
		if now.Before(t.slots[i].exp) {
			return true
		}
		// Expired: forget it so the same identity can be applied again.
		delete(t.index, key)
	}
	t.insert(key, now.Add(t.ttl), now)
	return false
}

func (t *Table) insert(key Key, exp, now time.Time) {
	i := t.next
	if old := t.slots[i].key; old != (Key{}) {
		if j, ok := t.index[old]; ok && j == i {
			delete(t.index, old)
		}
	}
	t.slots[i] = slot{key: key, exp: exp}
	t.index[key] = i
	t.next = (i + 1) % len(t.slots)
	t.sweepExpired(now)
}

// sweepExpired reclaims up to sweepBatch expired slots starting at the sweep
// cursor. Slots expire in insertion order, so the scan stops at the first live
// slot of the current lap.
func (t *Table) sweepExpired(now time.Time) {
	const sweepBatch = 16
	for n := 0; n < sweepBatch; n++ {
		s := &t.slots[t.sweep]
		if s.key == (Key{}) {
			return
		}
		if now.Before(s.exp) {
			return
		}
		if j, ok := t.index[s.key]; ok && j == t.sweep {
			delete(t.index, s.key)
		}
		s.key, s.exp = Key{}, time.Time{}
		t.sweep = (t.sweep + 1) % len(t.slots)
	}
}

func NewTable(size int, ttl time.Duration) *Table {
	if size <= 0 {
		size = defaultSize
	}
	if ttl <= 0 {
		ttl = defaultTTL
	}
	return &Table{
		slots: make([]slot, size),
		index: make(map[Key]int, size),
		ttl:   ttl,
	}
}

// The bounds a backend uses when it configures no deduplication of its own.
const (
	defaultSize = 4096
	defaultTTL  = 30 * time.Second
)

// Len reports how many identities are currently remembered.
func (t *Table) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.index)
}

// Keys returns the identities currently remembered, for diagnostics and tests.
func (t *Table) Keys() []Key {
	t.mu.Lock()
	defer t.mu.Unlock()
	keys := make([]Key, 0, len(t.index))
	for key := range t.index {
		keys = append(keys, key)
	}
	return keys
}
