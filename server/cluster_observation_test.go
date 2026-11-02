package server

// Helpers that observe a cluster backend only through its public views
// (StatsView, LinkView, MemberView, PresenceView). The backends live in
// gmqx/cluster (see docs/design/source-layout.md §4.1), so a test in the root
// package may not reach into one: what a test cannot observe here, a consumer
// cannot observe either.

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// clusterWaitCeiling bounds every wait below. It is generous because each wait is
// on an observable condition and returns as soon as the condition holds.
const clusterWaitCeiling = 10 * time.Second

// clusterLinkQuiet is how long a node's link set must be unchanged before a test
// treats it as settled.
const clusterLinkQuiet = 250 * time.Millisecond

// clusterStats returns a backend's counters, failing the test when it has no
// StatsView.
func clusterStats(t *testing.T, c Cluster) ClusterStats {
	t.Helper()
	view, ok := c.(StatsView)
	if !ok {
		t.Fatalf("%T does not implement StatsView", c)
	}
	return view.Stats()
}

// clusterLinks returns a backend's physical links, failing the test when it has no
// LinkView.
func clusterLinks(t *testing.T, c Cluster) []Link {
	t.Helper()
	view, ok := c.(LinkView)
	if !ok {
		t.Fatalf("%T does not implement LinkView", c)
	}
	return view.Links()
}

// clusterMembers returns a backend's member view, failing the test when it has no
// MemberView.
func clusterMembers(t *testing.T, c Cluster) []Member {
	t.Helper()
	view, ok := c.(MemberView)
	if !ok {
		t.Fatalf("%T does not implement MemberView", c)
	}
	return view.Members()
}

// clusterMemberIDs returns the sorted peer ids of a node.
func clusterMemberIDs(t *testing.T, c Cluster) []string {
	t.Helper()
	out := []string{}
	for _, m := range clusterMembers(t, c) {
		out = append(out, m.NodeID)
	}
	return out
}

// clusterMemberSummary returns the subscription summary a peer advertised, and
// whether that summary is known at all.
func clusterMemberSummary(t *testing.T, c Cluster, nodeID string) ([]string, bool) {
	t.Helper()
	for _, m := range clusterMembers(t, c) {
		if m.NodeID == nodeID {
			if !m.SummaryKnown {
				return nil, false
			}
			return m.Filters, true
		}
	}
	return nil, false
}

// waitClusterSummary waits until this node's view of a peer advertises exactly the
// given filters. A publication is routed to a peer only when that peer's advertised
// summary matches or when the summary is unknown or stale, so a test that publishes
// right after a subscription waits for the advertisement instead of assuming it has
// arrived.
func waitClusterSummary(t *testing.T, c Cluster, nodeID string, want ...string) {
	t.Helper()
	deadline := time.Now().Add(clusterWaitCeiling)
	var got []string
	for time.Now().Before(deadline) {
		if filters, ok := clusterMemberSummary(t, c, nodeID); ok {
			got = filters
			if equalStringsUnordered(got, want) {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("summary of %s = %v, want %v", nodeID, got, want)
}

// clusterMemberShared returns the shared subscription group keys a peer advertised
// and whether its summary is known at all. It is the shared half of the advertised
// summary, which is what elects one node per shared group.
func clusterMemberShared(t *testing.T, c Cluster, nodeID string) ([]string, bool) {
	t.Helper()
	for _, m := range clusterMembers(t, c) {
		if m.NodeID == nodeID {
			if !m.SummaryKnown {
				return nil, false
			}
			return m.Shared, true
		}
	}
	return nil, false
}

// clusterRouted returns a backend's routing view, failing the test when it has no
// RoutingView.
func clusterRouted(t *testing.T, c Cluster) []Routed {
	t.Helper()
	view, ok := c.(RoutingView)
	if !ok {
		t.Fatalf("%T does not implement RoutingView", c)
	}
	return view.Routed()
}

// clusterRoutedSummary returns the summary a backend's routing view holds for a
// node, which is the summary a spread would route by right now.
func clusterRoutedSummary(t *testing.T, c Cluster, nodeID string) ([]string, bool) {
	t.Helper()
	for _, r := range clusterRouted(t, c) {
		if r.NodeID == nodeID {
			if !r.SummaryKnown {
				return nil, false
			}
			return r.Filters, true
		}
	}
	return nil, false
}

// clusterRoutedShared returns the shared subscription groups a backend's routing
// view holds for a node, which is what the shared-group election elects over.
func clusterRoutedShared(t *testing.T, c Cluster, nodeID string) ([]string, bool) {
	t.Helper()
	for _, r := range clusterRouted(t, c) {
		if r.NodeID == nodeID {
			if !r.SummaryKnown {
				return nil, false
			}
			return r.Shared, true
		}
	}
	return nil, false
}

// clusterPresence returns a backend's presence view, failing the test when it has
// none.
func clusterPresence(t *testing.T, c Cluster) PresenceView {
	t.Helper()
	view, ok := c.(PresenceView)
	if !ok {
		t.Fatalf("%T does not implement PresenceView", c)
	}
	return view
}

// waitClusterMembers waits until the node sees exactly the given peers (itself
// excluded by Members).
func waitClusterMembers(t *testing.T, c Cluster, want ...string) {
	t.Helper()
	sort.Strings(want)
	deadline := time.Now().Add(clusterWaitCeiling)
	var got []string
	for time.Now().Before(deadline) {
		got = clusterMemberIDs(t, c)
		if equalStrings(got, want) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("member view = %v, want %v", got, want)
}

// waitClusterLink waits until a link backs the peer, which is what a send needs;
// the member view alone can be one gossip frame ahead of the link.
func waitClusterLink(t *testing.T, c Cluster, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(clusterWaitCeiling)
	for time.Now().Before(deadline) {
		for _, l := range clusterLinks(t, c) {
			if l.Peer == nodeID {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no link to %s in %v", nodeID, clusterLinks(t, c))
}

// waitClusterLinks waits until want physical links exist. A link alone is not yet
// routable (the handshake has to identify it), which is what waitClusterMembers
// covers on top.
func waitClusterLinks(t *testing.T, c Cluster, want int) {
	t.Helper()
	deadline := time.Now().Add(clusterWaitCeiling)
	var got []Link
	for time.Now().Before(deadline) {
		got = clusterLinks(t, c)
		if len(got) >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("links = %d (%v), want %d", len(got), got, want)
}

// equalStrings reports whether two sorted string slices are equal.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// equalStringsUnordered compares two string slices ignoring order.
func equalStringsUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	return equalStrings(as, bs)
}

// clusterLinkSignature is the identity of a node's link set: one token per link
// with its connection id, its peer, its addresses and its direction. A replaced
// link produces a new token even when the replacement has the same peer and
// direction, because the connection id is never reused.
func clusterLinkSignature(t *testing.T, c Cluster) string {
	t.Helper()
	parts := make([]string, 0, 4)
	for _, l := range clusterLinks(t, c) {
		dir := "in"
		if l.Outbound {
			dir = "out"
		}
		parts = append(parts, fmt.Sprintf("%d/%s/%s/%s/%s", l.ID, l.Peer, l.Addr, l.DialAddr, dir))
	}
	return strings.Join(parts, ",")
}

// waitClusterSettled waits until the link sets of all given nodes have been
// unchanged for clusterLinkQuiet, so a broadcast made right after it is not sent
// into a link that is being superseded.
//
// Two nodes that dial each other reconcile to one physical connection just after
// startup: the superseded link is closed and the survivor is the other dial, so for
// a few milliseconds a peer has no link at all. A send in that window is dropped
// and a cluster event is never retried, so a test that asserts a delivery waits for
// the reconciliation to finish rather than for a link to exist. The wait is an
// observation of the cluster, not a fixed sleep: it is bounded by
// clusterWaitCeiling and returns as soon as the sets are proven stable.
func waitClusterSettled(t *testing.T, cs ...Cluster) {
	t.Helper()
	deadline := time.Now().Add(clusterWaitCeiling)
	sigs := make([]string, len(cs))
	since := make([]time.Time, len(cs))
	start := time.Now()
	for i, c := range cs {
		sigs[i], since[i] = clusterLinkSignature(t, c), start
	}
	var last []string
	for time.Now().Before(deadline) {
		settled := true
		now := time.Now()
		for i, c := range cs {
			sig := clusterLinkSignature(t, c)
			if sig != sigs[i] {
				sigs[i], since[i] = sig, now
				settled = false
				continue
			}
			if now.Sub(since[i]) < clusterLinkQuiet {
				settled = false
			}
		}
		last = sigs
		if settled {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("link sets did not settle within %v (last: %v)", clusterWaitCeiling, last)
}
