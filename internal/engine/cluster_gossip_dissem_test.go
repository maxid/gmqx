package engine

// Wait policy for this file (and the rule a future change must keep): every wait is
// on an observable condition with a generous ceiling (gossipWaitCeiling, 10 s), and
// a delivery assertion waits for the cluster to be *settled*, not merely for a peer
// to be in the member view or for a link to exist.
//
// Two nodes that dial each other reconcile to one physical connection just after
// startup: the link that loses is closed, and for a few milliseconds the peer has no
// link at all. A gossip event queued on a closing link is dropped and is never
// retried, so a test that spreads into that window loses the event it then waits
// for. waitGossipLink covers "a link exists"; waitGossipSettled covers "the link set
// has stopped changing", which is what a one-shot delivery needs. The evidence for
// this was a flake whose drop counter said "cluster_unreachable: no link to d" while
// the same link existed again a millisecond later.
//
// Do not replace these waits with a sleep: the failure they prevent is a dropped
// event, and no amount of sleeping makes a dropped event arrive.

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
	deadline := time.Now().Add(gossipWaitCeiling)
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
	deadline := time.Now().Add(gossipWaitCeiling)
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
	deadline := time.Now().Add(gossipWaitCeiling)
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

// SubscriptionFilters implements cluster.SubscriptionProvider.
func (h *gossipFilterHandler) SubscriptionFilters() []string {
	h.fmu.Lock()
	defer h.fmu.Unlock()
	return append([]string(nil), h.filters...)
}

func (h *gossipFilterHandler) setFilters(filters ...string) {
	h.fmu.Lock()
	h.filters = filters
	h.fmu.Unlock()
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

	waitClusterMembers(t, a, "b", "c")
	waitClusterMembers(t, b, "a", "c")
	waitClusterMembers(t, c, "a", "b")
	// The origin must know that b matches before it can prefer it over c, and
	// both hops must have a link.
	waitClusterSummary(t, a, "b", "t/#")
	waitClusterLink(t, a, "b")
	waitClusterLink(t, b, "c")
	waitClusterSettled(t, a, b)

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
	if got := clusterStats(t, b).Forwards; got == 0 {
		t.Fatal("b did not forward the event")
	}
	if got := clusterStats(t, a).Forwards; got != 0 {
		t.Fatalf("the origin forwarded %d events, want 0", got)
	}
	if got := clusterStats(t, a).EventWrites; got != 1 {
		t.Fatalf("the origin wrote the event to %d peers, want only the matching relay", got)
	}
	if got := clusterStats(t, c).Dedup; got != 1 {
		t.Fatalf("c dedup table has %d identities, want 1", got)
	}
	if got := clusterStats(t, c).Drops; got != 0 {
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
	waitClusterMembers(t, a, "b", "c")
	waitClusterMembers(t, b, "a", "c")
	waitClusterMembers(t, c, "a", "b")
	waitClusterLink(t, a, "b")
	waitClusterLink(t, a, "c")
	waitClusterSettled(t, a, b, c)

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
	if got := clusterStats(t, b).Dedup; got != 1 {
		t.Fatalf("b dedup identities = %d, want 1", got)
	}
	if got := clusterStats(t, c).Dedup; got != 1 {
		t.Fatalf("c dedup identities = %d, want 1", got)
	}
	if got := clusterStats(t, a).Forwards; got != 0 {
		t.Fatalf("the origin forwarded %d events, want 0", got)
	}
	if got := clusterStats(t, a).EventWrites; got != 2 {
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
	waitClusterMembers(t, a, "b", "c")
	waitClusterMembers(t, b, "a", "c")
	waitClusterMembers(t, c, "a", "b")
	waitClusterSummary(t, a, "b", "routed/#")
	waitClusterSummary(t, a, "c", "other/#")
	waitClusterLink(t, a, "b")
	waitClusterLink(t, a, "c")
	waitClusterSettled(t, a)

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
	waitClusterMembers(t, a, "b", "c", "d")
	waitClusterMembers(t, b, "a", "c", "d")
	waitClusterMembers(t, c, "a", "b", "d")
	waitClusterMembers(t, d, "a", "b", "c")
	waitClusterSummary(t, a, "b", "fallback/#")
	waitClusterSummary(t, a, "c", "unmatched/#")
	// b must know c does not match, otherwise c is a legitimate unknown-summary
	// relay target and could consume the one fan-out slot meant for d.
	waitClusterSummary(t, b, "c", "unmatched/#")
	waitClusterLink(t, a, "b")
	waitClusterLink(t, b, "c")
	waitClusterLink(t, b, "d")
	// The relay's link to d must survive startup: a frame written into a link that
	// is being superseded is dropped and the event is not retried.
	waitClusterSettled(t, a, b)
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
	if clusterStats(t, b).Forwards == 0 {
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
	waitClusterMembers(t, ca, "b")
	waitClusterMembers(t, cb, "a")
	waitClusterLink(t, ca, "b")
	waitClusterLink(t, cb, "a")
	waitClusterSettled(t, ca, cb)

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
	waitClusterMembers(t, ca, "b", "c")
	waitClusterMembers(t, cb, "a", "c")
	waitClusterMembers(t, cc, "a", "b")
	waitClusterLink(t, ca, "b")
	waitClusterSettled(t, ca, cb)

	target, _ := mustConnect(t, b.Addr(), confConnect{clientID: "gossip-target", clean: true})
	waitFor(t, "the presence claim to reach a", func() bool {
		nodeID, ok := clusterPresence(t, ca).Presence("gossip-target")
		return ok && nodeID == "b"
	})

	if err := a.PublishToClient(context.Background(), "gossip-target", directPublication(a, "direct/x", "once", 0)); err != nil {
		t.Fatalf("PublishToClient: %v", err)
	}
	pub := expectPublish(t, target)
	if pub.Topic != "direct/x" || string(pub.Payload) != "once" {
		t.Fatalf("directed delivery = %q %q", pub.Topic, pub.Payload)
	}
	expectNoMessage(t, target, 150*time.Millisecond)
	if got := clusterStats(t, ca).TargetRouted; got != 1 {
		t.Fatalf("directed publishes routed by presence = %d, want 1", got)
	}
	if got := clusterStats(t, ca).TargetFlooded; got != 0 {
		t.Fatalf("directed publishes flooded = %d, want 0", got)
	}
	// c only ever received the connect-time kick of b's client, never the
	// directed publish (which dedup would have made at most one identity).
	if got := clusterStats(t, cc).Dedup; got > 1 {
		t.Fatalf("the bystander applied %d events, want at most the kick", got)
	}
	if got := clusterStats(t, cc).TargetForwards; got != 0 {
		t.Fatalf("the bystander forwarded %d directed publishes, want 0", got)
	}
	if nodeID, ok := clusterPresence(t, cb).Presence("gossip-target"); !ok || nodeID != "b" {
		t.Fatalf("b presence claim = (%q, %v), want itself", nodeID, ok)
	}

	// Without a live claim the origin falls back to the epidemic; that receive
	// path is tested in gmqx/cluster, where the claim can be cleared directly.
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
	waitClusterMembers(t, a, "b")
	waitFor(t, "the shared group advertisement", func() bool {
		groups, ok := clusterMemberShared(t, a, "b")
		return ok && len(groups) == 1 && groups[0] == group
	})
}
