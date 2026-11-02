package clusterkit

import "sort"

// Set tracks the subscription summary a node advertises to its cluster
// peers: the topic filters its local sessions are subscribed to, oldest first.
//
// Order is the eviction policy, not cosmetics: over the advertised cap the
// filters present longest are left out so the newest subscriptions keep
// routing. A filter that disappears locally disappears from the summary, and a
// filter that survives keeps its position, so re-advertising the same set does
// not reshuffle the list and the cap evicts a stable set instead of a random one.
//
// The set is not safe for concurrent use; every backend guards it with its own
// lock.
type Set struct {
	order []string
	set   map[string]struct{}
}

func NewSet() *Set {
	return &Set{set: map[string]struct{}{}}
}

// Refresh folds the current local filter list into the set and returns the
// filters to advertise, oldest first and capped at max. A max <= 0 applies no
// cap.
func (s *Set) Refresh(current []string, max int) []string {
	present := make(map[string]struct{}, len(current))
	for _, f := range current {
		if f != "" {
			present[f] = struct{}{}
		}
	}
	kept := s.order[:0]
	for _, f := range s.order {
		if _, ok := present[f]; ok {
			kept = append(kept, f)
		}
	}
	s.order = kept
	for f := range s.set {
		if _, ok := present[f]; !ok {
			delete(s.set, f)
		}
	}
	// New filters are appended in a stable order so the advertised list does not
	// depend on map iteration.
	var added []string
	for f := range present {
		if _, ok := s.set[f]; !ok {
			added = append(added, f)
		}
	}
	sort.Strings(added)
	for _, f := range added {
		s.order = append(s.order, f)
		s.set[f] = struct{}{}
	}
	return s.Snapshot(max)
}

// Snapshot returns the filters to advertise, oldest first and capped at max.
func (s *Set) Snapshot(max int) []string {
	order := s.order
	if max > 0 && len(order) > max {
		order = order[len(order)-max:]
	}
	return append(make([]string, 0, len(order)), order...)
}

// Cap applies the per-node cap to a summary received from a peer: the
// newest filters survive, exactly as in the local cap.
func Cap(filters []string, max int) []string {
	if max > 0 && len(filters) > max {
		filters = filters[len(filters)-max:]
	}
	return append(make([]string, 0, len(filters)), filters...)
}
