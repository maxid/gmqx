package routing

// Shared-group node election
//
// A shared subscription ("$share/{ShareName}/{filter}", MQTT 5 §4.8.2) is a group:
// a publication that matches it is sent to exactly one of its sessions. Inside one
// node the index answers that on its own (Index.SharedTarget picks the member round
// robin). Across a cluster every node deciding for itself would be wrong — each
// node would deliver its own copy — so the node that produces the publication picks
// the delivering node first, from the node ids that hold a live member of the group
// (its own index plus the peers that advertise the group).
//
// The election needs no shared state and no round trip:
//
//	candidates[(fnv64(groupKey) + Seat) % len(candidates)]
//
// over the sorted, de-duplicated candidate list. The group hash is the group's
// affinity: different groups spread over the nodes without any coordination, and
// two origins publishing to the same group do not have to agree. Seat is a
// per-group counter of this origin, so successive publications of one group visit
// every candidate instead of pinning the group to one node while it lives; it is
// per group because a single counter shared by all groups would keep the parity of
// a group constant whenever two groups match the same publication.
//
// Election is a value: its zero value is ready to use, and its counters are
// per-broker, so a broker that never routes a shared publication pays nothing.

import (
	"hash/fnv"
	"sort"
	"sync"
)

// maxRotationEntries bounds the per-group election counters. Past it the table is
// reset: the group hash still spreads the groups, so the only cost is that a
// group's rotation restarts from its affinity, never a correctness problem.
const maxRotationEntries = 1024

// Election decides which node delivers the publication of a shared subscription
// group. It is safe for concurrent use; the zero value is an empty election.
type Election struct {
	mu       sync.Mutex
	rotation map[string]uint64
}

// Choose elects one node for group out of candidates and reports whether there is
// a candidate at all. candidates need not be sorted or unique; the choice is made
// over the sorted, de-duplicated set. The group's rotation advances by one on every
// call, whether the elected node is this one or a peer.
func (e *Election) Choose(group string, candidates []string) (string, bool) {
	candidates = uniqueSorted(candidates)
	if len(candidates) == 0 {
		return "", false
	}
	seat := (groupHash(group) + e.next(group)) % uint64(len(candidates))
	return candidates[seat], true
}

// next advances the rotation of one group and returns the seat to elect with.
func (e *Election) next(group string) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.rotation == nil {
		e.rotation = make(map[string]uint64, 8)
	}
	if len(e.rotation) >= maxRotationEntries {
		clear(e.rotation)
	}
	seat := e.rotation[group]
	e.rotation[group] = seat + 1
	return seat
}

// groupHash is the affinity of a shared group: FNV-1a over its key. It is stable
// across processes and restarts, so two nodes that elect for the same group start
// from the same candidate.
func groupHash(group string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(group))
	return h.Sum64()
}

// uniqueSorted copies ids, sorts the copy and removes duplicates.
func uniqueSorted(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	out := append([]string(nil), ids...)
	sort.Strings(out)
	kept := out[:1]
	for _, id := range out[1:] {
		if id != kept[len(kept)-1] {
			kept = append(kept, id)
		}
	}
	return kept
}
