package gmqx

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety"
)

// discardLogger keeps test output readable: membership decisions are logged at
// Info/Debug and the tests below do not assert on them.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// captureHandler records log records so tests can assert that a membership
// decision was logged.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *captureHandler) WithGroup(string) slog.Handler            { return h }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) count(msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Message == msg {
			n++
		}
	}
	return n
}

func startTestCluster(t *testing.T, nodeID string, cfg TCPClusterConfig, handler ClusterHandler) *tcpCluster {
	t.Helper()
	if cfg.Logger == nil {
		cfg.Logger = discardLogger()
	}
	c, ok := NewTCPCluster(cfg).(*tcpCluster)
	if !ok {
		t.Fatalf("NewTCPCluster returned %T", c)
	}
	if err := c.Start(context.Background(), nodeID, handler); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

// memberIDs returns the sorted member node ids.
func memberIDs(c *tcpCluster) []string {
	out := []string{}
	for _, m := range c.Members() {
		out = append(out, m.NodeID)
	}
	return out
}

func waitMembers(t *testing.T, c *tcpCluster, want ...string) {
	t.Helper()
	sort.Strings(want)
	deadline := time.Now().Add(5 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got = memberIDs(c)
		if equalStrings(got, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("members = %v, want %v", got, want)
}

// waitMembersOf waits until the node sees exactly the given nodes, minus itself.
func waitMembersOf(t *testing.T, c *tcpCluster, nodes ...string) {
	t.Helper()
	want := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n != c.nodeID {
			want = append(want, n)
		}
	}
	waitMembers(t, c, want...)
}

func waitLinks(t *testing.T, c *tcpCluster, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.peerCount() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("links = %d, want %d", c.peerCount(), want)
}

func waitMemberSummary(t *testing.T, c *tcpCluster, nodeID string, want ...string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		if filters, ok := c.memberSummary(nodeID); ok {
			got = filters
			if equalStrings(got, want) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("summary of %q = %v, want %v", nodeID, got, want)
}

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

func portOf(t *testing.T, addr string) int {
	t.Helper()
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split %q: %v", addr, err)
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		t.Fatalf("port %q: %v", p, err)
	}
	return n
}

// orderedAddrs returns two free addresses whose ports ascend, which they must
// for the lexicographic dial rule to hold.
func orderedAddrs(t *testing.T) (string, string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		first, second := freeAddr(t), freeAddr(t)
		if first != second && portOf(t, first) < portOf(t, second) {
			return first, second
		}
	}
	t.Fatal("could not allocate two ordered free addresses")
	return "", ""
}

// fakePeer is a raw cluster peer: it dials a cluster node, performs the
// handshake and exchanges internal frames without any broker behind it.
type fakePeer struct {
	t      *testing.T
	client *gnety.Client
	nodeID string
	addr   string
	inc    string
	frames chan clusterFrame
}

func newFakePeer(t *testing.T, target, nodeID, addr string) *fakePeer {
	t.Helper()
	return newFakePeerWithOptions(t, target, nodeID, addr, "inc-"+nodeID, true)
}

func newFakePeerWithOptions(t *testing.T, target, nodeID, addr, inc string, read bool) *fakePeer {
	t.Helper()
	client, err := gnety.Dial(context.Background(), gnety.ClientConfig{
		Network: "tcp", Addr: target, Codec: gnety.LengthFieldCodec{}, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial cluster %s: %v", target, err)
	}
	p := &fakePeer{t: t, client: client, nodeID: nodeID, addr: addr, inc: inc, frames: make(chan clusterFrame, 128)}
	t.Cleanup(func() { _ = client.Close() })
	p.send(clusterFrame{Kind: frameHello, Version: clusterWireVersion, NodeID: nodeID, Addr: addr, Incarnation: inc})
	if read {
		go p.readLoop()
	}
	return p
}

// readLoop answers heartbeats (otherwise the cluster would evict the peer) and
// forwards only the frames the tests assert on.
func (p *fakePeer) readLoop() {
	for {
		payload, err := p.client.Receive(context.Background())
		if err != nil {
			return
		}
		raw, ok := payload.([]byte)
		if !ok {
			continue
		}
		var f clusterFrame
		if err := json.Unmarshal(raw, &f); err != nil {
			continue
		}
		if f.Kind == framePing {
			if pong, err := json.Marshal(clusterFrame{Kind: framePong, Version: clusterWireVersion, NodeID: p.nodeID}); err == nil {
				_ = p.client.Send(pong)
			}
			continue
		}
		if f.Kind != frameEvent && f.Kind != frameSubs {
			continue
		}
		select {
		case p.frames <- f:
		default:
		}
	}
}

func (p *fakePeer) send(f clusterFrame) {
	p.t.Helper()
	raw, err := json.Marshal(f)
	if err != nil {
		p.t.Fatalf("marshal frame: %v", err)
	}
	if err := p.client.Send(raw); err != nil {
		p.t.Fatalf("send frame: %v", err)
	}
}

func (p *fakePeer) sendEvent(origin string, seq uint64, topic, payload string) {
	p.t.Helper()
	p.send(clusterFrame{Kind: frameEvent, Version: clusterWireVersion, NodeID: origin, Event: &ClusterMessage{
		Type: ClusterPublish, NodeID: origin, Origin: origin, Seq: seq, Topic: topic, Payload: []byte(payload),
	}})
}

// waitEvent waits for a publish event and returns it.
func (p *fakePeer) waitEvent(t *testing.T, timeout time.Duration) ClusterMessage {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case f := <-p.frames:
			if f.Kind == frameEvent && f.Event != nil {
				return *f.Event
			}
		case <-deadline:
			t.Fatal("timed out waiting for a cluster event")
		}
	}
}

// expectNoEvent asserts that no publish event arrives within d.
func (p *fakePeer) expectNoEvent(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case f := <-p.frames:
			if f.Kind == frameEvent && f.Event != nil {
				t.Fatalf("unexpected cluster event: %+v", *f.Event)
			}
		case <-deadline:
			return
		}
	}
}

// waitSubs waits for an advertised summary with exactly the wanted filters.
func (p *fakePeer) waitSubs(t *testing.T, want ...string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case f := <-p.frames:
			if f.Kind == frameSubs && equalStrings(f.Filters, want) {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for summary %v", want)
		}
	}
}

func testClusterConfig(addr string, seeds []string) TCPClusterConfig {
	return TCPClusterConfig{
		Addr:                addr,
		Seeds:               seeds,
		ReconnectInterval:   20 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		HeartbeatMissBudget: 3,
		SummaryInterval:     50 * time.Millisecond,
		Logger:              discardLogger(),
	}
}

// TestTCPClusterMembershipConverges checks that three nodes agree on one member
// per node id, and that each member carries the address and incarnation of the
// handshake.
func TestTCPClusterMembershipConverges(t *testing.T) {
	aAddr, bAddr, cAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	others := func(self string, all ...string) []string {
		var out []string
		for _, s := range all {
			if s != self {
				out = append(out, s)
			}
		}
		return out
	}
	ca := startTestCluster(t, "a", testClusterConfig(aAddr, others(aAddr, aAddr, bAddr, cAddr)), &recClusterHandler{})
	cb := startTestCluster(t, "b", testClusterConfig(bAddr, others(bAddr, aAddr, bAddr, cAddr)), &recClusterHandler{})
	cc := startTestCluster(t, "c", testClusterConfig(cAddr, others(cAddr, aAddr, bAddr, cAddr)), &recClusterHandler{})

	for _, c := range []*tcpCluster{ca, cb, cc} {
		waitMembersOf(t, c, "a", "b", "c")
		// One link per member: no duplicates survive the handshake.
		waitLinks(t, c, 2)
	}
	for _, c := range []*tcpCluster{ca, cb, cc} {
		members := c.Members()
		if len(members) != 2 {
			t.Fatalf("%s members = %+v", c.nodeID, members)
		}
		for _, m := range members {
			if m.NodeID == c.nodeID {
				t.Fatalf("%s sees itself as a member", c.nodeID)
			}
			if m.Addr == "" || m.Incarnation == "" || m.Since.IsZero() {
				t.Fatalf("%s member %+v is incomplete", c.nodeID, m)
			}
		}
	}
	// Addresses are the advertised listen addresses, not socket addresses.
	if got, want := ca.Members()[1].Addr, cAddr; got != want {
		t.Fatalf("member c addr = %q, want %q", got, want)
	}
}

// TestTCPClusterDuplicateLinkCollapse makes one node dial a peer twice (through
// two seed spellings that resolve to the same process) and checks that both ends
// settle on one logical member and one link, and that the collapse is logged.
func TestTCPClusterDuplicateLinkCollapse(t *testing.T) {
	aAddr, bAddr := orderedAddrs(t)
	_, port, err := net.SplitHostPort(bAddr)
	if err != nil {
		t.Fatal(err)
	}
	alias := "localhost:" + port
	logs := &captureHandler{}
	cfgA := testClusterConfig(aAddr, []string{bAddr, alias})
	cfgA.Logger = slog.New(logs)
	ca := startTestCluster(t, "a", cfgA, &recClusterHandler{})
	cb := startTestCluster(t, "b", testClusterConfig(bAddr, []string{aAddr}), &recClusterHandler{})

	waitMembers(t, ca, "b")
	waitMembers(t, cb, "a")

	// Both ends settle on one member and one link, and stay there: the losing
	// dial never adds a second member even while it retries.
	stable := time.Time{}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(ca.Members()) == 1 && ca.peerCount() == 1 && len(cb.Members()) == 1 && cb.peerCount() == 1 {
			if stable.IsZero() {
				stable = time.Now()
			}
			if time.Since(stable) > 300*time.Millisecond {
				break
			}
		} else {
			stable = time.Time{}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if stable.IsZero() {
		t.Fatalf("duplicate link did not collapse: a members=%v links=%d, b members=%v links=%d",
			memberIDs(ca), ca.peerCount(), memberIDs(cb), cb.peerCount())
	}
	if n := logs.count("gmqx cluster link rejected"); n == 0 {
		t.Fatal("duplicate link collapse was not logged")
	}
}

// TestTCPClusterMemberEviction checks the heartbeat miss budget: a peer that
// handshakes and then goes silent is evicted and its link closed.
func TestTCPClusterMemberEviction(t *testing.T) {
	aAddr := freeAddr(t)
	cfg := testClusterConfig(aAddr, nil)
	cfg.HeartbeatInterval = 20 * time.Millisecond
	cfg.HeartbeatMissBudget = 2
	ca := startTestCluster(t, "a", cfg, &recClusterHandler{})

	// A silent peer: it sends the handshake and never answers a ping.
	newFakePeerWithOptions(t, aAddr, "quiet", "127.0.0.1:9001", "inc-quiet", false)
	waitMembers(t, ca, "quiet")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(ca.Members()) == 0 && ca.peerCount() == 0 {
			time.Sleep(100 * time.Millisecond)
			if len(ca.Members()) != 0 {
				t.Fatal("evicted member came back without a handshake")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("member not evicted: members=%v links=%d", memberIDs(ca), ca.peerCount())
}

// TestTCPClusterRestartRejoin checks that a stopped node leaves the view, that
// the survivors stay connected, and that a restarted node rejoins with a new
// incarnation.
func TestTCPClusterRestartRejoin(t *testing.T) {
	aAddr, bAddr, cAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	cfgFor := func(self string) TCPClusterConfig {
		var seeds []string
		for _, s := range []string{aAddr, bAddr, cAddr} {
			if s != self {
				seeds = append(seeds, s)
			}
		}
		return testClusterConfig(self, seeds)
	}
	ca := startTestCluster(t, "a", cfgFor(aAddr), &recClusterHandler{})
	cb := startTestCluster(t, "b", cfgFor(bAddr), &recClusterHandler{})
	cc, _ := NewTCPCluster(cfgFor(cAddr)).(*tcpCluster)
	if err := cc.Start(context.Background(), "c", &recClusterHandler{}); err != nil {
		t.Fatal(err)
	}
	waitMembersOf(t, ca, "a", "b", "c")
	waitMembersOf(t, cb, "a", "b", "c")
	waitMembersOf(t, cc, "a", "b", "c")

	oldInc := func() string {
		for _, m := range ca.Members() {
			if m.NodeID == "c" {
				return m.Incarnation
			}
		}
		t.Fatal("c is not a member")
		return ""
	}()
	if err := cc.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitMembersOf(t, ca, "a", "b")
	waitMembersOf(t, cb, "a", "b")

	// Restart c on the same address with the same node id.
	cc2, _ := NewTCPCluster(cfgFor(cAddr)).(*tcpCluster)
	if err := cc2.Start(context.Background(), "c", &recClusterHandler{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc2.Stop(context.Background()) })
	waitMembersOf(t, ca, "a", "b", "c")
	waitMembersOf(t, cc2, "a", "b", "c")

	var newInc string
	for _, m := range ca.Members() {
		if m.NodeID == "c" {
			newInc = m.Incarnation
		}
	}
	if newInc == "" || newInc == oldInc {
		t.Fatalf("incarnation after restart = %q, previous %q", newInc, oldInc)
	}
}

// TestTCPClusterDedup checks duplicate suppression, the local-origin loop guard
// and the refusal to forward a received event.
func TestTCPClusterDedup(t *testing.T) {
	aAddr := freeAddr(t)
	ca := startTestCluster(t, "a", testClusterConfig(aAddr, nil), &recClusterHandler{})
	h := ca.handler.(*recClusterHandler)
	peer := newFakePeer(t, aAddr, "peer", "127.0.0.1:9001")
	waitMembers(t, ca, "peer")

	peer.sendEvent("peer", 7, "t/x", "one")
	peer.sendEvent("peer", 7, "t/x", "one")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := h.counts(); n >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if n, _ := h.counts(); n != 1 {
		t.Fatalf("deliveries = %d, want 1", n)
	}
	if ca.dedup.len() != 1 {
		t.Fatalf("dedup table = %d, want 1", ca.dedup.len())
	}

	// An event stamped with this node as its origin must not be applied.
	beforeLoop := ca.dropCount()
	peer.sendEvent("a", 99, "t/x", "loop")
	time.Sleep(100 * time.Millisecond)
	if n, _ := h.counts(); n != 1 {
		t.Fatalf("loop event was delivered: %d", n)
	}
	if ca.dropCount() <= beforeLoop {
		t.Fatal("loop event was not counted as a drop")
	}

	// A received event must never be forwarded.
	beforeForward := ca.dropCount()
	received := ClusterMessage{Type: ClusterPublish, Origin: "peer", NodeID: "peer", Seq: 8, Topic: "t/x"}
	if err := ca.Spread(context.Background(), received); err != nil {
		t.Fatal(err)
	}
	if ca.dropCount() <= beforeForward {
		t.Fatal("forwarded event was not dropped")
	}
}

// TestTCPClusterDedupBounds checks that the table never grows beyond its slot
// count and that the oldest identity is the one evicted.
func TestTCPClusterDedupBounds(t *testing.T) {
	aAddr := freeAddr(t)
	cfg := testClusterConfig(aAddr, nil)
	cfg.DedupSize = 4
	cfg.DedupTTL = time.Minute
	ca := startTestCluster(t, "a", cfg, &recClusterHandler{})
	h := ca.handler.(*recClusterHandler)

	publish := func(seq uint64) {
		ca.dispatchEvent(ClusterMessage{Type: ClusterPublish, NodeID: "peer", Origin: "peer", Seq: seq, Topic: "t/x"})
	}
	for seq := uint64(1); seq <= 10; seq++ {
		publish(seq)
	}
	if n, _ := h.counts(); n != 10 {
		t.Fatalf("deliveries = %d, want 10", n)
	}
	if got := ca.dedup.len(); got != 4 {
		t.Fatalf("dedup table = %d, want 4", got)
	}
	// The oldest identities are gone, so they apply again — and the table stays
	// at its cap.
	publish(1)
	if n, _ := h.counts(); n != 11 {
		t.Fatalf("evicted identity was suppressed: deliveries = %d, want 11", n)
	}
	if got := ca.dedup.len(); got != 4 {
		t.Fatalf("dedup table = %d, want 4", got)
	}
}

// TestTCPClusterDedupTTL checks that an identity stops suppressing duplicates
// once its TTL elapsed.
func TestTCPClusterDedupTTL(t *testing.T) {
	aAddr := freeAddr(t)
	cfg := testClusterConfig(aAddr, nil)
	cfg.DedupTTL = 30 * time.Millisecond
	ca := startTestCluster(t, "a", cfg, &recClusterHandler{})
	h := ca.handler.(*recClusterHandler)

	ev := ClusterMessage{Type: ClusterPublish, NodeID: "peer", Origin: "peer", Seq: 3, Topic: "t/x"}
	ca.dispatchEvent(ev)
	ca.dispatchEvent(ev)
	if n, _ := h.counts(); n != 1 {
		t.Fatalf("deliveries = %d, want 1", n)
	}
	time.Sleep(80 * time.Millisecond)
	ca.dispatchEvent(ev)
	if n, _ := h.counts(); n != 2 {
		t.Fatalf("expired identity was suppressed: deliveries = %d, want 2", n)
	}
}

// TestTCPClusterDirectedRouting checks that publishes follow the peer's
// subscription summary, that an unknown summary falls back to sending to that
// peer only, and that kicks and retained publishes go everywhere.
func TestTCPClusterDirectedRouting(t *testing.T) {
	aAddr := freeAddr(t)
	ca := startTestCluster(t, "a", testClusterConfig(aAddr, nil), &recClusterHandler{})

	subscribed := newFakePeer(t, aAddr, "sub", "127.0.0.1:9001")
	quiet := newFakePeer(t, aAddr, "quiet", "127.0.0.1:9002")
	waitMembers(t, ca, "quiet", "sub")

	subscribed.send(clusterFrame{Kind: frameSubs, Version: clusterWireVersion, NodeID: "sub", Filters: []string{"plant/+/temp"}})
	waitMemberSummary(t, ca, "sub", "plant/+/temp")

	spread := func(msg ClusterMessage) {
		t.Helper()
		if err := ca.Spread(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	}

	// A matching topic reaches the subscribed peer.
	spread(ClusterMessage{Type: ClusterPublish, Topic: "plant/1/temp", Payload: []byte("21")})
	got := subscribed.waitEvent(t, 3*time.Second)
	if got.Topic != "plant/1/temp" || string(got.Payload) != "21" || got.Origin != "a" || got.Seq == 0 {
		t.Fatalf("event = %+v", got)
	}
	// ... and the peer with an unknown summary gets it too: one peer, not a
	// flood to everyone indiscriminately.
	if got := quiet.waitEvent(t, 3*time.Second); got.Topic != "plant/1/temp" {
		t.Fatalf("unknown-summary peer event = %+v", got)
	}

	// A non-matching topic reaches only the peer whose summary is unknown.
	spread(ClusterMessage{Type: ClusterPublish, Topic: "plant/1/hum", Payload: []byte("60")})
	if got := quiet.waitEvent(t, 3*time.Second); got.Topic != "plant/1/hum" {
		t.Fatalf("unknown-summary peer event = %+v", got)
	}
	subscribed.expectNoEvent(t, 200*time.Millisecond)

	// Kicks reach every node: a kick names a client id, not a topic, so only the
	// node holding that session can act on it.
	spread(ClusterMessage{Type: ClusterKick, ClientID: "device-1"})
	for _, p := range []*fakePeer{subscribed, quiet} {
		if got := p.waitEvent(t, 3*time.Second); got.Type != ClusterKick || got.ClientID != "device-1" {
			t.Fatalf("kick = %+v", got)
		}
	}

	// Retained publishes reach every node so retained state exists wherever a
	// later subscriber connects.
	spread(ClusterMessage{Type: ClusterPublish, Topic: "plant/1/hum", Retain: true, Payload: []byte("61")})
	for _, p := range []*fakePeer{subscribed, quiet} {
		if got := p.waitEvent(t, 3*time.Second); got.Topic != "plant/1/hum" || !got.Retain {
			t.Fatalf("retained publish = %+v", got)
		}
	}
}

// filterHandler advertises a mutable filter set, like a broker with local
// subscriptions.
type filterHandler struct {
	recClusterHandler
	mu      sync.Mutex
	filters []string
}

func (h *filterHandler) subscriptionFilters() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.filters...)
}

func (h *filterHandler) setFilters(filters ...string) {
	h.mu.Lock()
	h.filters = filters
	h.mu.Unlock()
}

// TestTCPClusterSummaryCap checks that a node advertises at most
// MaxSummaryFilters filters and evicts the oldest ones first.
func TestTCPClusterSummaryCap(t *testing.T) {
	aAddr := freeAddr(t)
	cfg := testClusterConfig(aAddr, nil)
	cfg.MaxSummaryFilters = 3
	h := &filterHandler{}
	h.setFilters("a/1", "a/2", "a/3", "a/4", "a/5")
	ca := startTestCluster(t, "a", cfg, h)

	peer := newFakePeer(t, aAddr, "peer", "127.0.0.1:9001")
	waitMembers(t, ca, "peer")
	peer.waitSubs(t, "a/3", "a/4", "a/5")

	h.setFilters("a/4", "a/5", "a/6")
	ca.subscriptionsChanged()
	peer.waitSubs(t, "a/4", "a/5", "a/6")

	if filters, ok := ca.memberSummary("peer"); ok {
		t.Fatalf("summary of a peer that advertised nothing = %v", filters)
	}
}

// TestTCPClusterBrokerDirectedRouting checks the broker path: a publish is
// delivered to the node with a matching subscriber and not to a node without
// one.
func TestTCPClusterBrokerDirectedRouting(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(testClusterConfig(aAddr, []string{bAddr}))
	cb := NewTCPCluster(testClusterConfig(bAddr, []string{aAddr}))
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)
	waitMembers(t, ca.(*tcpCluster), "b")

	sub := dialTestClient(t, b.Addr(), "sub-b")
	subBody := []byte{0, 1}
	subBody = appendString(subBody, "plant/+/temp")
	subBody = append(subBody, 0)
	sub.writePacket(0x82, subBody)
	if h, p, err := sub.readPacket(); err != nil || h>>4 != 9 || len(p) < 3 || p[2] != 0 {
		t.Fatalf("suback: h=%x p=%v err=%v", h, p, err)
	}
	// The broker pushes its summary before it acknowledges the subscription.
	waitMemberSummary(t, ca.(*tcpCluster), "b", "plant/+/temp")

	pub := dialTestClient(t, a.Addr(), "pub-a")
	pubBody := appendString(nil, "plant/1/temp")
	pubBody = append(pubBody, []byte("21")...)
	pub.writePacket(0x30, pubBody)

	h, p, err := sub.readPacket()
	if err != nil {
		t.Fatal(err)
	}
	if h>>4 != 3 {
		t.Fatalf("expected PUBLISH, got type %d", h>>4)
	}
	topic, rest, err := readString(p)
	if err != nil || topic != "plant/1/temp" || string(rest) != "21" {
		t.Fatalf("topic=%q payload=%q err=%v", topic, rest, err)
	}

	// A topic nobody subscribed to is not routed to b at all.
	_ = sub.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	pubBody = appendString(nil, "plant/1/hum")
	pubBody = append(pubBody, []byte("60")...)
	pub.writePacket(0x30, pubBody)
	if _, _, err := sub.readPacket(); err == nil {
		t.Fatal("unexpected publish for an unsubscribed topic")
	}
}

// TestTCPClusterEventFromUnknownPeer checks the availability rule: events from a
// peer that never handshaked are still handled, but cannot create a member.
func TestTCPClusterEventFromUnknownPeer(t *testing.T) {
	aAddr := freeAddr(t)
	ca := startTestCluster(t, "a", testClusterConfig(aAddr, nil), &recClusterHandler{})
	h := ca.handler.(*recClusterHandler)

	client, err := gnety.Dial(context.Background(), gnety.ClientConfig{
		Network: "tcp", Addr: aAddr, Codec: gnety.LengthFieldCodec{}, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	raw, err := json.Marshal(clusterFrame{Kind: frameEvent, Version: clusterWireVersion, NodeID: "ghost", Event: &ClusterMessage{
		Type: ClusterPublish, NodeID: "ghost", Origin: "ghost", Seq: 1, Topic: "t/ghost", Payload: []byte("boo"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Send(raw); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := h.counts(); n == 1 {
			if members := memberIDs(ca); len(members) != 0 {
				t.Fatalf("event resurrected a member: %v", members)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("event from an unknown peer was not handled")
}

// TestTCPClusterWireVersionRejected checks that a peer speaking another frame
// version is refused: a version mismatch means different ordering rules, so it
// must not become a member (and cannot produce a duplicate logical member).
func TestTCPClusterWireVersionRejected(t *testing.T) {
	aAddr := freeAddr(t)
	ca := startTestCluster(t, "a", testClusterConfig(aAddr, nil), &recClusterHandler{})

	client, err := gnety.Dial(context.Background(), gnety.ClientConfig{
		Network: "tcp", Addr: aAddr, Codec: gnety.LengthFieldCodec{}, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	raw, err := json.Marshal(clusterFrame{
		Kind: frameHello, Version: clusterWireVersion + 1,
		NodeID: "future", Addr: "127.0.0.1:9001", Incarnation: "inc-future",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Send(raw); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(ca.Members()) == 0 && ca.peerCount() == 0 {
			// Stayed out: the refusal is not a transient state.
			time.Sleep(100 * time.Millisecond)
			if len(ca.Members()) != 0 || ca.peerCount() != 0 {
				t.Fatalf("refused peer was admitted: members=%v links=%d", memberIDs(ca), ca.peerCount())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("peer with a foreign wire version was not refused: members=%v links=%d", memberIDs(ca), ca.peerCount())
}
