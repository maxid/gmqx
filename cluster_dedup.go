package gmqx

import (
	"sync"
	"time"
)

// dedupKey identifies one cluster event: the origin node id and that origin's
// monotonically increasing sequence number.
type dedupKey struct {
	origin string
	seq    uint64
}

// dedupTable remembers recently applied event identities so a redelivered
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
type dedupTable struct {
	mu    sync.Mutex
	slots []dedupSlot
	index map[dedupKey]int
	next  int // slot the next insert overwrites
	sweep int // lazy sweep cursor
	ttl   time.Duration
}

type dedupSlot struct {
	key dedupKey
	exp time.Time
}

// seen reports whether key was already applied, and remembers it when it was
// new.
func (t *dedupTable) seen(key dedupKey) bool {
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

func (t *dedupTable) insert(key dedupKey, exp, now time.Time) {
	i := t.next
	if old := t.slots[i].key; old != (dedupKey{}) {
		if j, ok := t.index[old]; ok && j == i {
			delete(t.index, old)
		}
	}
	t.slots[i] = dedupSlot{key: key, exp: exp}
	t.index[key] = i
	t.next = (i + 1) % len(t.slots)
	t.sweepExpired(now)
}

// sweepExpired reclaims up to sweepBatch expired slots starting at the sweep
// cursor. Slots expire in insertion order, so the scan stops at the first live
// slot of the current lap.
func (t *dedupTable) sweepExpired(now time.Time) {
	const sweepBatch = 16
	for n := 0; n < sweepBatch; n++ {
		s := &t.slots[t.sweep]
		if s.key == (dedupKey{}) {
			return
		}
		if now.Before(s.exp) {
			return
		}
		if j, ok := t.index[s.key]; ok && j == t.sweep {
			delete(t.index, s.key)
		}
		s.key, s.exp = dedupKey{}, time.Time{}
		t.sweep = (t.sweep + 1) % len(t.slots)
	}
}

// len reports how many identities are currently remembered (used by tests).
func (t *dedupTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.index)
}

func newDedupTable(size int, ttl time.Duration) *dedupTable {
	if size <= 0 {
		size = defaultDedupSize
	}
	if ttl <= 0 {
		ttl = defaultDedupTTL
	}
	return &dedupTable{
		slots: make([]dedupSlot, size),
		index: make(map[dedupKey]int, size),
		ttl:   ttl,
	}
}
