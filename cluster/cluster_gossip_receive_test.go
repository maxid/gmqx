package cluster

// Receive-path tests: what a backend does with one event it received, driven
// through its own receive function and asserted with a recorder. They need no
// broker, which is why they moved here with the implementation
// (docs/design/source-layout.md §4.1): the root package's broker × cluster tests
// keep the end-to-end path, and these keep the exact (Origin, Seq) and presence
// rules that an end-to-end test can only observe by luck.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gmqx/hook"
)

// gossipRecorder is a cluster handler that records every event kind, including a
// directed publish (which recClusterHandler does not implement).
type gossipRecorder struct {
	recClusterHandler
	tmu     sync.Mutex
	targets []ClusterMessage
}

func (h *gossipRecorder) OnRemoteTarget(msg ClusterMessage) {
	h.tmu.Lock()
	h.targets = append(h.targets, msg)
	h.tmu.Unlock()
}

func (h *gossipRecorder) targetCount() int {
	h.tmu.Lock()
	defer h.tmu.Unlock()
	return len(h.targets)
}

func (h *gossipRecorder) waitTargets(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(gossipWaitCeiling)
	for time.Now().Before(deadline) {
		if h.targetCount() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("target events = %d, want %d", h.targetCount(), want)
}

// dropRecorder records the drop reasons a backend reports. The backend counts a
// foreign-namespace event as "cluster_namespace", which is the observable reason a
// reader of the metrics sees; Drops alone would not distinguish it from a duplicate.
type dropRecorder struct {
	hook.NoopMetrics
	mu      sync.Mutex
	reasons map[string]int
}

func (r *dropRecorder) RecordDroppedMessage(reason string) {
	r.mu.Lock()
	if r.reasons == nil {
		r.reasons = make(map[string]int)
	}
	r.reasons[reason]++
	r.mu.Unlock()
}

func (r *dropRecorder) count(reason string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reasons[reason]
}

// TestGossipNamespaceIsolationAndDedup pins the two receive-side filters: an event
// of another namespace never reaches the handler (and is counted), and the same
// (Origin, Seq) applied twice reaches it once.
func TestGossipNamespaceIsolationAndDedup(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	metrics := &dropRecorder{}
	cfgA := gossipTestConfig(aAddr, bAddr)
	cfgA.Namespace = "tenant-a"
	cfgB := gossipTestConfig(bAddr, aAddr)
	cfgB.Namespace = "tenant-b"
	cfgB.Metrics = metrics
	ha, hb := &gossipRecorder{}, &gossipRecorder{}
	a := startGossipTestCluster(t, "a", cfgA, ha)
	b := startGossipTestCluster(t, "b", cfgB, hb)
	waitClusterMembers(t, a, "b")
	waitClusterMembers(t, b, "a")
	waitClusterLink(t, a, "b")
	waitClusterLink(t, b, "a")
	waitClusterSettled(t, a, b)

	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Topic: "ns/x", Payload: []byte("a"),
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the foreign-namespace event to be counted", func() bool {
		return metrics.count("cluster_namespace") > 0
	})
	if n, _ := hb.counts(); n != 0 {
		t.Fatalf("b applied %d foreign-namespace publishes", n)
	}

	// Two copies of one event: the second is suppressed by (Origin, Seq).
	event := ClusterMessage{
		Type: ClusterPublish, NodeID: "a", Origin: "a", Seq: 41,
		Namespace: "tenant-b", Topic: "dedup/x", Payload: []byte("once"),
	}
	link := &gossipLink{nodeID: "a"}
	b.handleEvent(link, event)
	b.handleEvent(link, event)
	if n, _ := hb.counts(); n != 1 {
		t.Fatalf("b applied %d copies of one event, want 1", n)
	}
	if got := b.Stats().Dedup; got != 1 {
		t.Fatalf("dedup identities = %d, want 1", got)
	}
	if got := b.Stats().Drops; got == 0 {
		t.Fatal("the duplicate was not counted")
	}
}

// TestGossipDirectedPublishPresenceFallback pins the availability rule of presence
// routing: with a live claim the origin routes the target event to the claiming
// node, and with no claim at all it falls back to the epidemic and the node that
// still holds the client id delivers it. The claim is cleared through the backend's
// own writer, so the test does not depend on the TTL expiring.
func TestGossipDirectedPublishPresenceFallback(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ha, hb := &gossipRecorder{}, &gossipRecorder{}
	a := startGossipTestCluster(t, "a", gossipTestConfig(aAddr, bAddr), ha)
	b := startGossipTestCluster(t, "b", gossipTestConfig(bAddr, aAddr), hb)
	waitClusterMembers(t, a, "b")
	waitClusterMembers(t, b, "a")
	waitClusterLink(t, a, "b")
	waitClusterSettled(t, a, b)

	b.ClientConnected("gossip-target")
	waitFor(t, "the presence claim to reach a", func() bool {
		nodeID, ok := a.Presence("gossip-target")
		return ok && nodeID == "b"
	})
	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterTarget, ClientID: "gossip-target", Topic: "direct/x", Payload: []byte("once"),
	}); err != nil {
		t.Fatal(err)
	}
	hb.waitTargets(t, 1)
	if got := a.Stats().TargetRouted; got != 1 {
		t.Fatalf("directed publishes routed by presence = %d, want 1", got)
	}
	if got := a.Stats().TargetFlooded; got != 0 {
		t.Fatalf("directed publishes flooded = %d, want 0", got)
	}

	// The owner left and released its claim: the origin must not swallow the event,
	// it floods it and the node that still holds the client id delivers it. The
	// release travels as a tombstone, so the test waits for the origin to observe
	// it instead of clearing a local table.
	b.ClientDisconnected("gossip-target")
	waitFor(t, "the released claim to reach a", func() bool {
		_, ok := a.Presence("gossip-target")
		return !ok
	})
	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterTarget, ClientID: "gossip-target", Topic: "direct/x", Payload: []byte("fallback"),
	}); err != nil {
		t.Fatal(err)
	}
	hb.waitTargets(t, 2)
	if got := a.Stats().TargetFlooded; got != 1 {
		t.Fatalf("directed publishes flooded = %d, want 1", got)
	}
	if got := a.Stats().TargetRouted; got != 1 {
		t.Fatalf("directed publishes routed by presence = %d, want 1", got)
	}
}
