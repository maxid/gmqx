package gmqx

import (
	"context"
	"sync"
	"testing"
	"time"
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
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.targetCount() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("target events = %d, want %d", h.targetCount(), want)
}

func (h *gossipRecorder) waitPublishes(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := h.counts(); n >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	n, _ := h.counts()
	t.Fatalf("publishes = %d, want %d", n, want)
}

func (h *gossipRecorder) waitKicks(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, n := h.counts(); n >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	_, n := h.counts()
	t.Fatalf("kicks = %d, want %d", n, want)
}

// gossipFilterHandler is a recorder that advertises a filter set, so a test can
// make one node an unknown-summary peer and another a matching one.
type gossipFilterHandler struct {
	gossipRecorder
	fmu     sync.Mutex
	filters []string
}

func (h *gossipFilterHandler) subscriptionFilters() []string {
	h.fmu.Lock()
	defer h.fmu.Unlock()
	return append([]string(nil), h.filters...)
}

func (h *gossipFilterHandler) setFilters(filters ...string) {
	h.fmu.Lock()
	h.filters = filters
	h.fmu.Unlock()
}

func waitGossipSummary(t *testing.T, c *gossipCluster, nodeID string, want ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		if filters, ok := c.memberSummary(nodeID); ok {
			got = filters
			if equalStringsUnordered(got, want) {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("summary of %s on %s = %v, want %v", nodeID, c.nodeID, got, want)
}

func equalStringsUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sortStrings(as)
	sortStrings(bs)
	return equalStrings(as, bs)
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// TestGossipDisseminationForwardsThroughRelay pins the epidemic with a chain: the
// origin knows a matching peer (b) and an unknown one (c), but Fanout is one, so it
// writes only the matching peer. b forwards to the peer it did not receive the event
// from (c), and c applies the event exactly once: the dedup table stops the copy the
// origin would otherwise have had to send itself.
func TestGossipDisseminationForwardsThroughRelay(t *testing.T) {
	aAddr, bAddr, cAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	fanout := 1
	cfgA := gossipTestConfig(aAddr, bAddr)
	cfgA.Fanout = fanout
	cfgB := gossipTestConfig(bAddr, aAddr, cAddr)
	cfgB.Fanout = fanout
	cfgC := gossipTestConfig(cAddr, bAddr)
	cfgC.Fanout = fanout

	ha, hb, hc := &gossipRecorder{}, &gossipFilterHandler{}, &gossipRecorder{}
	hb.setFilters("t/#")
	a := startGossipCluster(t, "a", cfgA, ha)
	b := startGossipCluster(t, "b", cfgB, hb)
	c := startGossipCluster(t, "c", cfgC, hc)

	waitGossipMembers(t, a, "b", "c")
	waitGossipMembers(t, b, "a", "c")
	waitGossipMembers(t, c, "a", "b")
	// The origin must know that b matches before it can prefer it over c, and
	// both hops must have a link.
	waitGossipSummary(t, a, "b", "t/#")
	waitGossipLink(t, a, "b")
	waitGossipLink(t, b, "c")

	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Topic: "t/x", Payload: []byte("relayed"),
	}); err != nil {
		t.Fatal(err)
	}
	hc.waitPublishes(t, 1)
	// A short settle: the point is that c got one copy, not two.
	time.Sleep(30 * time.Millisecond)
	if n, _ := hc.counts(); n != 1 {
		t.Fatalf("c received %d publishes, want 1", n)
	}
	if n, _ := ha.counts(); n != 0 {
		t.Fatalf("the origin received its own publish %d times", n)
	}
	if got := b.forwardCount(); got == 0 {
		t.Fatal("b did not forward the event")
	}
	if got := a.forwardCount(); got != 0 {
		t.Fatalf("the origin forwarded %d events, want 0", got)
	}
	if got := a.eventWriteCount(); got != 1 {
		t.Fatalf("the origin wrote the event to %d peers, want only the matching relay", got)
	}
	if got := c.dedupLen(); got != 1 {
		t.Fatalf("c dedup table has %d identities, want 1", got)
	}
	if got := c.dropCount(); got != 0 {
		t.Fatalf("c dropped %d events, want 0", got)
	}
}

// TestGossipDisseminationFanoutBroadcasts pins the small-cluster degeneration:
// with Fanout at or above the member count the origin writes every peer. The peers
// then forward to each other, and the duplicate is suppressed by (Origin, Seq).
func TestGossipDisseminationFanoutBroadcasts(t *testing.T) {
	aAddr, bAddr, cAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	mk := func(addr string, seeds ...string) GossipClusterConfig {
		cfg := gossipTestConfig(addr, seeds...)
		cfg.Fanout = 5
		return cfg
	}
	ha, hb, hc := &gossipRecorder{}, &gossipRecorder{}, &gossipRecorder{}
	a := startGossipCluster(t, "a", mk(aAddr, bAddr, cAddr), ha)
	b := startGossipCluster(t, "b", mk(bAddr, aAddr, cAddr), hb)
	c := startGossipCluster(t, "c", mk(cAddr, aAddr, bAddr), hc)
	_ = b
	_ = c
	waitGossipMembers(t, a, "b", "c")
	waitGossipMembers(t, b, "a", "c")
	waitGossipMembers(t, c, "a", "b")
	waitGossipLink(t, a, "b")
	waitGossipLink(t, a, "c")

	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Topic: "broadcast/x", Payload: []byte("all"),
	}); err != nil {
		t.Fatal(err)
	}
	hb.waitPublishes(t, 1)
	hc.waitPublishes(t, 1)
	time.Sleep(30 * time.Millisecond)
	if n, _ := hb.counts(); n != 1 {
		t.Fatalf("b received %d publishes, want 1", n)
	}
	if n, _ := hc.counts(); n != 1 {
		t.Fatalf("c received %d publishes, want 1", n)
	}
	if got := b.dedupLen(); got != 1 {
		t.Fatalf("b dedup identities = %d, want 1", got)
	}
	if got := c.dedupLen(); got != 1 {
		t.Fatalf("c dedup identities = %d, want 1", got)
	}
	if got := a.forwardCount(); got != 0 {
		t.Fatalf("the origin forwarded %d events, want 0", got)
	}
	if got := a.eventWriteCount(); got != 2 {
		t.Fatalf("the origin wrote the broadcast to %d peers, want both", got)
	}
}

// TestGossipRoutingSummaryPrecision pins directed routing on top of the gossip: a
// peer that advertised a matching filter receives the publish and a peer that
// advertised a different filter does not. Neither summary is unknown, so no
// epidemic fallback is involved.
func TestGossipRoutingSummaryPrecision(t *testing.T) {
	aAddr, bAddr, cAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	cfg := func(addr string, seeds ...string) GossipClusterConfig {
		c := gossipTestConfig(addr, seeds...)
		c.Fanout = 5
		return c
	}
	ha, hb, hc := &gossipRecorder{}, &gossipFilterHandler{}, &gossipFilterHandler{}
	hb.setFilters("routed/#")
	hc.setFilters("other/#")
	a := startGossipCluster(t, "a", cfg(aAddr, bAddr, cAddr), ha)
	b := startGossipCluster(t, "b", cfg(bAddr, aAddr, cAddr), hb)
	c := startGossipCluster(t, "c", cfg(cAddr, aAddr, bAddr), hc)
	waitGossipMembers(t, a, "b", "c")
	waitGossipMembers(t, b, "a", "c")
	waitGossipMembers(t, c, "a", "b")
	waitGossipSummary(t, a, "b", "routed/#")
	waitGossipSummary(t, a, "c", "other/#")
	waitGossipLink(t, a, "b")
	waitGossipLink(t, a, "c")

	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Topic: "routed/x", Payload: []byte("b"),
	}); err != nil {
		t.Fatal(err)
	}
	hb.waitPublishes(t, 1)
	time.Sleep(40 * time.Millisecond)
	if n, _ := hc.counts(); n != 0 {
		t.Fatalf("a non-matching peer received %d publishes, want 0", n)
	}

	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Topic: "other/x", Payload: []byte("c"),
	}); err != nil {
		t.Fatal(err)
	}
	hc.waitPublishes(t, 1)
	time.Sleep(40 * time.Millisecond)
	if n, _ := hb.counts(); n != 1 {
		t.Fatalf("the non-matching peer received a total of %d publishes, want 1", n)
	}
}

// TestGossipRoutingUnknownSummaryFallsBack pins the availability rule: a peer whose
// summary is unknown is not skipped. With Fanout one the origin writes the matching
// peer only, and that relay forwards to the unknown peer, which is how a subscriber
// that connected before its summary reached the origin is still reached.
func TestGossipRoutingUnknownSummaryFallsBack(t *testing.T) {
	aAddr, bAddr, cAddr, dAddr := freeAddr(t), freeAddr(t), freeAddr(t), freeAddr(t)
	cfg := func(addr string, seeds ...string) GossipClusterConfig {
		c := gossipTestConfig(addr, seeds...)
		c.Fanout = 1
		return c
	}
	ha, hb, hc, hd := &gossipRecorder{}, &gossipFilterHandler{}, &gossipFilterHandler{}, &gossipRecorder{}
	hb.setFilters("fallback/#")
	hc.setFilters("unmatched/#")
	a := startGossipCluster(t, "a", cfg(aAddr, bAddr, cAddr, dAddr), ha)
	b := startGossipCluster(t, "b", cfg(bAddr, aAddr, cAddr, dAddr), hb)
	c := startGossipCluster(t, "c", cfg(cAddr, aAddr, bAddr, dAddr), hc)
	d := startGossipCluster(t, "d", cfg(dAddr, aAddr, bAddr, cAddr), hd)
	waitGossipMembers(t, a, "b", "c", "d")
	waitGossipMembers(t, b, "a", "c", "d")
	waitGossipMembers(t, c, "a", "b", "d")
	waitGossipMembers(t, d, "a", "b", "c")
	waitGossipSummary(t, a, "b", "fallback/#")
	waitGossipSummary(t, a, "c", "unmatched/#")
	// b must know c does not match, otherwise c is a legitimate unknown-summary
	// relay target and could consume the one fan-out slot meant for d.
	waitGossipSummary(t, b, "c", "unmatched/#")
	waitGossipLink(t, a, "b")
	waitGossipLink(t, b, "c")
	waitGossipLink(t, b, "d")
	// d advertises nothing at all: its summary is unknown.

	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Topic: "fallback/x", Payload: []byte("unknown"),
	}); err != nil {
		t.Fatal(err)
	}
	hd.waitPublishes(t, 1)
	time.Sleep(40 * time.Millisecond)
	if n, _ := hc.counts(); n != 0 {
		t.Fatalf("the non-matching peer received %d publishes, want 0", n)
	}
	if b.forwardCount() == 0 {
		t.Fatal("the relay did not forward to the unknown-summary peer")
	}
}

// TestGossipDisseminationKickBroker runs the connect-time kick end to end: a client
// connects to b and then to a, and the kick a spreads closes the connection b held.
func TestGossipDisseminationKickBroker(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewGossipCluster(gossipTestConfig(aAddr, bAddr))
	cb := NewGossipCluster(gossipTestConfig(bAddr, aAddr))
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca, ConnectMode: ConnectModeKick, NotKickSeconds: -1})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb, ConnectMode: ConnectModeKick, NotKickSeconds: -1})
	waitGossipMembers(t, ca.(*gossipCluster), "b")
	waitGossipMembers(t, cb.(*gossipCluster), "a")
	waitGossipLink(t, ca.(*gossipCluster), "b")
	waitGossipLink(t, cb.(*gossipCluster), "a")

	remote := dialTestClient(t, b.Addr(), "dup")
	// Let the kick b spread on the CONNECT reach a before the same client id
	// connects there; an older kick would be refused by the epoch ordering.
	time.Sleep(30 * time.Millisecond)
	_ = dialTestClient(t, a.Addr(), "dup")

	_ = remote.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := remote.r.ReadByte(); err == nil {
		t.Fatal("expected the remote session to be kicked")
	}
}

// TestGossipDisseminationTargetedPublish pins presence routing: a directed publish
// for a client id held by b is written to b and delivered once, and no other node
// applies it.
func TestGossipDisseminationTargetedPublish(t *testing.T) {
	aAddr, bAddr, cAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	ca := NewGossipCluster(gossipTestConfig(aAddr, bAddr, cAddr))
	cb := NewGossipCluster(gossipTestConfig(bAddr, aAddr, cAddr))
	cc := NewGossipCluster(gossipTestConfig(cAddr, aAddr, bAddr))
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb})
	startBroker(t, Config{NodeID: "c", ClusterBackend: cc})
	gca, gcb, gcc := ca.(*gossipCluster), cb.(*gossipCluster), cc.(*gossipCluster)
	waitGossipMembers(t, gca, "b", "c")
	waitGossipMembers(t, gcb, "a", "c")
	waitGossipMembers(t, gcc, "a", "b")
	waitGossipLink(t, gca, "b")

	target, _ := mustConnect(t, b.Addr(), confConnect{clientID: "gossip-target", clean: true})
	waitFor(t, "the presence claim to reach a", func() bool {
		return gca.presenceOwner("gossip-target") == "b"
	})

	if err := a.PublishToClient(context.Background(), "gossip-target", directPublication(a, "direct/x", "once", 0)); err != nil {
		t.Fatalf("PublishToClient: %v", err)
	}
	pub := expectPublish(t, target)
	if pub.Topic != "direct/x" || string(pub.Payload) != "once" {
		t.Fatalf("directed delivery = %q %q", pub.Topic, pub.Payload)
	}
	expectNoMessage(t, target, 150*time.Millisecond)
	if got := gca.targetRoutedCount(); got != 1 {
		t.Fatalf("directed publishes routed by presence = %d, want 1", got)
	}
	if got := gca.targetFloodedCount(); got != 0 {
		t.Fatalf("directed publishes flooded = %d, want 0", got)
	}
	// c only ever received the connect-time kick of b's client, never the
	// directed publish (which dedup would have made at most one identity).
	if got := gcc.dedupLen(); got > 1 {
		gcc.dedup.mu.Lock()
		keys := make([]dedupKey, 0, len(gcc.dedup.index))
		for k := range gcc.dedup.index {
			keys = append(keys, k)
		}
		gcc.dedup.mu.Unlock()
		t.Fatalf("the bystander applied %d events %v, want at most the kick", got, keys)
	}
	if got := gcc.targetForwardCount(); got != 0 {
		t.Fatalf("the bystander forwarded %d directed publishes, want 0", got)
	}
	gcb.mu.Lock()
	claims := len(gcb.presence)
	gcb.mu.Unlock()
	if claims == 0 {
		t.Fatal("b did not keep a presence claim for its client")
	}

	// A claim that is gone (the owner died or left) must not swallow the event:
	// the origin falls back to the epidemic and the node that still holds the
	// client id delivers it.
	gca.mu.Lock()
	gca.claimPresenceLocked("gossip-target", "")
	gca.mu.Unlock()
	if err := a.PublishToClient(context.Background(), "gossip-target", directPublication(a, "direct/x", "fallback", 0)); err != nil {
		t.Fatalf("PublishToClient without a presence claim: %v", err)
	}
	pub = expectPublish(t, target)
	if string(pub.Payload) != "fallback" {
		t.Fatalf("fallback delivery = %q", pub.Payload)
	}
	expectNoMessage(t, target, 100*time.Millisecond)
	if got := gca.targetFloodedCount(); got != 1 {
		t.Fatalf("directed publishes flooded = %d, want 1", got)
	}
}

// TestGossipNamespaceIsolationAndDedup pins the two receive-side filters: an event
// of another namespace never reaches the handler (and is counted), and the same
// (Origin, Seq) applied twice reaches it once.
func TestGossipNamespaceIsolationAndDedup(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	metrics := newDeliveryMetrics()
	cfgA := gossipTestConfig(aAddr, bAddr)
	cfgA.Namespace = "tenant-a"
	cfgB := gossipTestConfig(bAddr, aAddr)
	cfgB.Namespace = "tenant-b"
	cfgB.Metrics = metrics
	ha, hb := &gossipRecorder{}, &gossipRecorder{}
	a := startGossipCluster(t, "a", cfgA, ha)
	b := startGossipCluster(t, "b", cfgB, hb)
	waitGossipMembers(t, a, "b")
	waitGossipMembers(t, b, "a")
	waitGossipLink(t, a, "b")
	waitGossipLink(t, b, "a")

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
	if got := b.dedupLen(); got != 1 {
		t.Fatalf("dedup identities = %d, want 1", got)
	}
	if got := b.dropCount(); got == 0 {
		t.Fatal("the duplicate was not counted")
	}
}

// TestGossipSharedGroupsFromSummary pins the shared half of the advertised summary:
// the groups a peer advertises become its shared candidates, which is what lets the
// broker elect one node per shared group over the gossip backend.
func TestGossipSharedGroupsFromSummary(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	group := "$share/g/t/x"
	ha, hb := &gossipRecorder{}, &summaryProvider{filters: []string{"t/#"}, groups: []string{group}}
	a := startGossipCluster(t, "a", gossipTestConfig(aAddr, bAddr), ha)
	startGossipCluster(t, "b", gossipTestConfig(bAddr, aAddr), hb)
	waitGossipMembers(t, a, "b")
	waitFor(t, "the shared group advertisement", func() bool {
		groups := a.sharedGroups()
		return len(groups[group]) == 1 && groups[group][0] == "b"
	})
}
