package gmqx

// Namespace isolation tests: sessions, subscriptions, retained state and cluster
// traffic are partitioned by Config.Namespace, and the default namespace keeps
// the pre-namespace behavior (covered by the rest of the suite, which never sets
// it).

import (
	"bufio"
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// dropRecorder implements MetricsManager and counts the drop reasons a component
// reports.
type dropRecorder struct {
	NoopMetrics
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

// injectCluster is a Cluster backend that keeps its handler so a test can inject
// a remote event.
type injectCluster struct {
	mu      sync.Mutex
	handler ClusterHandler
}

func (c *injectCluster) Start(_ context.Context, _ string, h ClusterHandler) error {
	c.mu.Lock()
	c.handler = h
	c.mu.Unlock()
	return nil
}

func (c *injectCluster) Stop(context.Context) error { return nil }

func (c *injectCluster) Spread(context.Context, ClusterMessage) error { return nil }

func (c *injectCluster) inject(msg ClusterMessage) {
	c.mu.Lock()
	h := c.handler
	c.mu.Unlock()
	if h == nil {
		return
	}
	switch msg.Type {
	case ClusterPublish:
		h.OnRemotePublish(msg)
	case ClusterKick:
		h.OnRemoteKick(msg)
	}
}

// retainedIn returns the retained message a namespace holds for topic.
func retainedIn(b *Broker, namespace, topic string) (retainedMessage, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m, ok := b.retained[namespaceKey(namespace, topic)]
	return m, ok
}

func waitRetainedIn(t *testing.T, b *Broker, namespace, topic, payload string) retainedMessage {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m, ok := retainedIn(b, namespace, topic); ok && string(m.payload) == payload {
			return m
		}
		time.Sleep(5 * time.Millisecond)
	}
	m, ok := retainedIn(b, namespace, topic)
	t.Fatalf("retained[%s/%q] = (%+v, %t), want payload %q", namespace, topic, m, ok, payload)
	return retainedMessage{}
}

// waitDrop waits until a metrics recorder saw at least one drop of reason.
func waitDrop(t *testing.T, r *dropRecorder, reason string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if r.count(reason) > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no %q drop was recorded", reason)
}

// expectNoPublish fails when a client receives a PUBLISH within the window.
func expectNoPublish(t *testing.T, c *testClient, window time.Duration, what string) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(window))
	if h, p, err := c.readPacket(); err == nil {
		t.Fatalf("%s: unexpected packet %#x %x", what, h, p)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
}

// TestNamespaceKeyQualification pins the partition-key rules: the default
// namespace keeps bare names, and a non-empty namespace cannot collide with
// another one.
func TestNamespaceKeyQualification(t *testing.T) {
	if got := namespaceKey("", "plant/1/temp"); got != "plant/1/temp" {
		t.Fatalf("default namespace key = %q, want the bare name", got)
	}
	if got, want := namespaceKey("tenant-a", "plant/1/temp"), "tenant-a\x00plant/1/temp"; got != want {
		t.Fatalf("namespaced key = %q, want %q", got, want)
	}
	if namespaceKey("tenant-a", "x") == namespaceKey("tenant-b", "x") {
		t.Fatal("two namespaces produced the same key")
	}
	if namespaceKey("tenant-a", "x") == namespaceKey("", "x") {
		t.Fatal("a namespace produced the default-namespace key")
	}
}

// TestNamespaceRetainedAndSubscriptionIsolation checks that two brokers with the
// same topic but different namespaces never deliver to each other and keep
// separate retained state.
func TestNamespaceRetainedAndSubscriptionIsolation(t *testing.T) {
	a := startBroker(t, Config{Namespace: "tenant-a"})
	b := startBroker(t, Config{Namespace: "tenant-b"})

	pub := dialTestClient(t, a.Addr(), "ns-pub")
	publishRetained(t, pub, "state/x", "a-value")
	waitRetainedIn(t, a, "tenant-a", "state/x", "a-value")

	// The retained value exists only in tenant-a.
	if _, ok := retainedIn(a, "tenant-b", "state/x"); ok {
		t.Fatal("tenant-a broker retained the topic under tenant-b")
	}
	if retainedCount(b) != 0 {
		t.Fatalf("tenant-b broker retained %d topics, want 0", retainedCount(b))
	}

	// A subscriber in tenant-b sees no retained state for the topic.
	subB := dialTestClient(t, b.Addr(), "ns-sub-b")
	subscribeTestClient(t, subB, "state/#")
	expectNoPublish(t, subB, 300*time.Millisecond, "retained delivery crossed namespaces")

	// A subscriber in tenant-a sees it.
	subA := dialTestClient(t, a.Addr(), "ns-sub-a")
	subscribeTestClient(t, subA, "state/#")
	h, topic, payload := readPublish(t, subA)
	if h&0x01 == 0 || topic != "state/x" || string(payload) != "a-value" {
		t.Fatalf("namespaced retained delivery = %#x %q %q", h, topic, payload)
	}
}

// TestRemoteEventsOfAnotherNamespaceAreDropped checks the broker-side guard that
// complements the backend filters: a foreign namespace publication never reaches
// a session or the retained store, and a foreign kick never closes a session.
func TestRemoteEventsOfAnotherNamespaceAreDropped(t *testing.T) {
	cluster := &injectCluster{}
	metrics := &dropRecorder{}
	b := startBroker(t, Config{Namespace: "tenant-a", ClusterBackend: cluster, Metrics: metrics})

	sub := dialTestClient(t, b.Addr(), "ns-remote-sub")
	subscribeTestClient(t, sub, "iso/#")

	// The routing seam itself is namespace-scoped: a publication of another
	// namespace has no local target even when the topic matches.
	if targets := b.LocalTargets(Publication{Namespace: "tenant-b", Topic: "iso/x"}); len(targets) != 0 {
		t.Fatalf("LocalTargets returned %d targets for a foreign namespace", len(targets))
	}
	if targets := b.LocalTargets(Publication{Namespace: "tenant-a", Topic: "iso/x"}); len(targets) != 1 {
		t.Fatalf("LocalTargets returned %d targets, want 1", len(targets))
	}

	cluster.inject(ClusterMessage{
		Type: ClusterPublish, NodeID: "peer", Namespace: "tenant-b",
		Topic: "iso/x", Payload: []byte("foreign"), Retain: true,
	})
	expectNoPublish(t, sub, 300*time.Millisecond, "foreign-namespace publish was delivered")
	if _, ok := retainedIn(b, "tenant-a", "iso/x"); ok {
		t.Fatal("foreign-namespace publish was retained for tenant-a")
	}
	if _, ok := retainedIn(b, "tenant-b", "iso/x"); ok {
		t.Fatal("foreign-namespace publish was retained for tenant-b")
	}
	if metrics.count("cluster_namespace") == 0 {
		t.Fatal("the dropped foreign publish was not counted")
	}

	// A kick for another namespace must not close a local session that happens to
	// share the client id.
	cluster.inject(ClusterMessage{Type: ClusterKick, NodeID: "peer", Namespace: "tenant-b", ClientID: "ns-remote-sub"})

	// An event of this namespace is still applied after the foreign ones.
	cluster.inject(ClusterMessage{
		Type: ClusterPublish, NodeID: "peer", Namespace: "tenant-a",
		Topic: "iso/local", Payload: []byte("local"), Retain: true,
	})
	_, topic, payload := readPublish(t, sub)
	if topic != "iso/local" || string(payload) != "local" {
		t.Fatalf("local-namespace delivery = %q %q", topic, payload)
	}
	waitRetainedIn(t, b, "tenant-a", "iso/local", "local")
}

// TestNamespaceSessionTableIsPartitioned seeds a persistent session in one
// namespace and checks that a connection only resumes the session stored under
// its broker's namespace.
func TestNamespaceSessionTableIsPartitioned(t *testing.T) {
	b := startBroker(t, Config{Namespace: "tenant-a"})
	b.mu.Lock()
	b.sessions[namespaceKey("tenant-a", "resumed")] = &session{subs: map[string]subscription{
		"ns/#": {qos: 0},
	}}
	b.sessions[namespaceKey("tenant-b", "foreign")] = &session{subs: map[string]subscription{
		"foreign/#": {qos: 0},
	}}
	b.mu.Unlock()

	resumed, sessionPresent := dialPersistentTestClient(t, b.Addr(), "resumed")
	if sessionPresent != 1 {
		t.Fatal("a session stored in the broker's namespace was not resumed")
	}

	foreign, sessionPresent := dialPersistentTestClient(t, b.Addr(), "foreign")
	if sessionPresent != 0 {
		t.Fatal("a session stored in another namespace was resumed")
	}
	_ = foreign

	pub := dialTestClient(t, b.Addr(), "ns-session-pub")
	publishPlain(t, pub, "ns/1", "resumed")

	_, topic, payload := readPublish(t, resumed)
	if topic != "ns/1" || string(payload) != "resumed" {
		t.Fatalf("resumed subscription delivery = %q %q", topic, payload)
	}
}

// TestTCPClusterNamespaceIsolation drives the built-in TCP backend pair in one
// process: each node serves a different namespace, so a summary of the other
// namespace is not trusted and a received event of another namespace is dropped.
func TestTCPClusterNamespaceIsolation(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ma, mb := &dropRecorder{}, &dropRecorder{}
	ha := &recClusterHandler{}
	// A provider is needed so b actually advertises a summary for a to filter.
	hb := &filterHandler{}
	hb.setFilters("t/#")
	ca := startTestCluster(t, "a", TCPClusterConfig{
		Addr: aAddr, Seeds: []string{bAddr}, ReconnectInterval: 20 * time.Millisecond,
		Namespace: "tenant-a", Metrics: ma,
	}, ha)
	cb := startTestCluster(t, "b", TCPClusterConfig{
		Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond,
		Namespace: "tenant-b", Metrics: mb,
	}, hb)
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)

	// b advertises its summary with tenant-b; a ignores it, so b stays unknown.
	waitDrop(t, ma, "cluster_namespace")
	if filters, ok := ca.memberSummary("b"); ok {
		t.Fatalf("summary of another namespace was trusted: %v", filters)
	}

	// A locally produced event is stamped with a's namespace and dropped by b on
	// the receive path.
	if err := ca.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Topic: "t/x", Payload: []byte("foreign"),
	}); err != nil {
		t.Fatal(err)
	}
	waitDrop(t, mb, "cluster_namespace")
	if n, _ := hb.counts(); n != 0 {
		t.Fatalf("b applied %d foreign-namespace publishes, want 0", n)
	}

	// An event that explicitly names another namespace cannot be produced here.
	before := ca.dropCount()
	if err := ca.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Namespace: "tenant-b", Topic: "t/y", Payload: []byte("nope"),
	}); err != nil {
		t.Fatal(err)
	}
	if got := ca.dropCount(); got != before+1 {
		t.Fatalf("foreign-namespace spread drops = %d, want %d", got-before, 1)
	}
	if n, _ := hb.counts(); n != 0 {
		t.Fatalf("b applied %d foreign-namespace publishes, want 0", n)
	}
}

// TestTCPClusterSameNamespaceDelivers is the control: with the same namespace on
// both nodes, an event stamped by the sending backend is applied by the peer.
func TestTCPClusterSameNamespaceDelivers(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := startTestCluster(t, "a", TCPClusterConfig{
		Addr: aAddr, Seeds: []string{bAddr}, ReconnectInterval: 20 * time.Millisecond,
		Namespace: "tenant-a",
	}, &recClusterHandler{})
	hb := &recClusterHandler{}
	cb := startTestCluster(t, "b", TCPClusterConfig{
		Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond,
		Namespace: "tenant-a",
	}, hb)
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)

	if err := ca.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Topic: "t/x", Payload: []byte("same"),
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := hb.counts(); n == 1 {
			hb.mu.Lock()
			got := hb.publish[0]
			hb.mu.Unlock()
			if got.Namespace != "tenant-a" || got.Topic != "t/x" || string(got.Payload) != "same" {
				t.Fatalf("message = %+v", got)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("same-namespace publish was not delivered")
}

// TestBrokerNamespaceIsolationOverTCP runs two brokers with different namespaces
// on the in-process TCP cluster pair: a publish and a retained value of one
// namespace never reach a subscriber of the other.
func TestBrokerNamespaceIsolationOverTCP(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(TCPClusterConfig{
		Addr: aAddr, Seeds: []string{bAddr}, ReconnectInterval: 20 * time.Millisecond, Logger: discardLogger(),
	})
	cb := NewTCPCluster(TCPClusterConfig{
		Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond, Logger: discardLogger(),
	})
	a := startBroker(t, Config{NodeID: "a", Namespace: "tenant-a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", Namespace: "tenant-b", ClusterBackend: cb})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)

	subA := dialTestClient(t, a.Addr(), "iso-sub-a")
	subscribeTestClient(t, subA, "iso/#")
	subB := dialTestClient(t, b.Addr(), "iso-sub-b")
	subscribeTestClient(t, subB, "iso/#")

	pub := dialTestClient(t, a.Addr(), "iso-pub")
	publishPlain(t, pub, "iso/x", "local")

	_, topic, payload := readPublish(t, subA)
	if topic != "iso/x" || string(payload) != "local" {
		t.Fatalf("local delivery = %q %q", topic, payload)
	}
	expectNoPublish(t, subB, 400*time.Millisecond, "publish crossed broker namespaces over the TCP cluster")

	// Retained state is replicated per namespace only.
	publishRetained(t, pub, "keep/x", "a-value")
	waitRetainedIn(t, a, "tenant-a", "keep/x", "a-value")

	subB2 := dialTestClient(t, b.Addr(), "iso-sub-b2")
	subscribeTestClient(t, subB2, "keep/#")
	expectNoPublish(t, subB2, 400*time.Millisecond, "retained state crossed broker namespaces over the TCP cluster")

	if retainedCount(b) != 0 {
		t.Fatalf("broker b retained %d topics, want 0", retainedCount(b))
	}
	if _, ok := retainedIn(b, "tenant-b", "keep/x"); ok {
		t.Fatal("broker b retained a topic of another namespace")
	}
}

// TestRedisClusterNamespaceIsolation covers the second built-in backend: it
// ignores a peer's summary of another namespace and drops a received event of
// another namespace.
func TestRedisClusterNamespaceIsolation(t *testing.T) {
	mr := runMiniRedis(t)
	cfgA := redisTestConfig(mr)
	cfgA.Namespace = "tenant-a"
	cfgB := redisTestConfig(mr)
	cfgB.Namespace = "tenant-b"

	hb := &filterHandler{}
	hb.setFilters("t/#")
	a := startRedisCluster(t, mr, "a", &recClusterHandler{}, cfgA)
	b := startRedisCluster(t, mr, "b", hb, cfgB)
	waitRedisMembers(t, a, "b")

	// b advertised its summary with tenant-b; a must not use it for routing.
	deadline := time.Now().Add(3 * time.Second)
	for !mr.Exists(b.summaryKey("b")) {
		if time.Now().After(deadline) {
			t.Fatal("node b did not advertise a subscription summary")
		}
		time.Sleep(5 * time.Millisecond)
	}
	view, err := a.loadView()
	if err != nil {
		t.Fatal(err)
	}
	peer, ok := view["b"]
	if !ok {
		t.Fatal("node b is missing from a's routing view")
	}
	if peer.filtersKnown {
		t.Fatalf("summary of another namespace was trusted: %v", peer.filters)
	}

	// a stamps the event with tenant-a; b drops it on the receive path.
	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Topic: "t/x", Payload: []byte("foreign"),
	}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for b.dropCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("b did not drop the foreign-namespace event")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n, _ := hb.counts(); n != 0 {
		t.Fatalf("b applied %d foreign-namespace publishes, want 0", n)
	}

	// An event that explicitly names another namespace cannot leave a either.
	before := a.dropCount()
	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Namespace: "tenant-b", Topic: "t/y", Payload: []byte("nope"),
	}); err != nil {
		t.Fatal(err)
	}
	if got := a.dropCount(); got != before+1 {
		t.Fatalf("foreign-namespace spread drops = %d, want 1", got-before)
	}
}

// connectPayloadSession builds a CONNECT body with an explicit clean-session
// flag.
func connectPayloadSession(clientID string, clean bool) []byte {
	var b []byte
	b = appendString(b, "MQTT")
	flags := byte(0)
	if clean {
		flags = 0x02
	}
	b = append(b, 4, flags, 0, 30) // level, flags, keepalive 30
	b = appendString(b, clientID)
	return b
}

// dialPersistentTestClient connects with Clean Session cleared and returns the
// session-present flag of the CONNACK.
func dialPersistentTestClient(t *testing.T, addr, clientID string) (*testClient, byte) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	c := &testClient{conn: conn, r: bufio.NewReader(conn)}
	t.Cleanup(func() { _ = conn.Close() })
	c.writePacket(0x10, connectPayloadSession(clientID, false))
	header, payload, err := c.readPacket()
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if header>>4 != 2 || len(payload) < 2 || payload[1] != 0 {
		t.Fatalf("unexpected connack: %#x %v", header, payload)
	}
	return c, payload[0] & 0x01
}
