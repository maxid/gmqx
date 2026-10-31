package gmqx

// Directed-publish tests: Broker.PublishToClient delivers to one client id — a
// live local connection, a stored local session, or a client that lives on
// another node — and a target event never crosses a namespace or gets applied
// twice.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/maxid/gnety/codec/mqtt"
	"github.com/redis/go-redis/v9"
)

// directPublication builds a directed publication on the broker's namespace.
func directPublication(b *Broker, topic, payload string, qos byte) Publication {
	return Publication{Namespace: b.Namespace(), Topic: topic, Payload: []byte(payload), QoS: qos}
}

// TestPublishToClientLocalOnline pins the local half: a connected client receives
// the publication without holding any subscription, at the publication's QoS, and
// the acknowledgement path works as for any other delivery.
func TestPublishToClientLocalOnline(t *testing.T) {
	// A retry interval far beyond the test's lifetime keeps the assertion below
	// deterministic: under load a retransmitted QoS 1 delivery could otherwise
	// arrive between the ack and the next expectation.
	b := startBroker(t, Config{QoSRetryInterval: time.Minute})
	target, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "direct-target", clean: true})

	if err := b.PublishToClient(context.Background(), "direct-target", directPublication(b, "direct/x", "hello", 1)); err != nil {
		t.Fatalf("PublishToClient: %v", err)
	}
	pub := expectPublish(t, target)
	if pub.Topic != "direct/x" || string(pub.Payload) != "hello" {
		t.Fatalf("directed delivery = %q %q", pub.Topic, pub.Payload)
	}
	if pub.QoS != 1 || pub.PacketID == 0 {
		t.Fatalf("directed delivery = QoS %d packet id %d, want QoS 1 with an id", pub.QoS, pub.PacketID)
	}
	if pub.Retain {
		t.Fatal("a non-retained directed publish arrived with RETAIN set")
	}
	target.ack(pub.PacketID)

	// A QoS 0 publication is delivered at QoS 0, without a packet id.
	if err := b.PublishToClient(context.Background(), "direct-target", directPublication(b, "direct/y", "unacked", 0)); err != nil {
		t.Fatalf("PublishToClient QoS 0: %v", err)
	}
	pub = expectPublish(t, target)
	if pub.QoS != 0 || pub.PacketID != 0 || string(pub.Payload) != "unacked" {
		t.Fatalf("QoS 0 directed delivery = QoS %d id %d payload %q", pub.QoS, pub.PacketID, pub.Payload)
	}
	// A directed publish is not a broadcast: it does not update the retained
	// store, so a later subscriber sees nothing.
	if n := retainedCount(b); n != 0 {
		t.Fatalf("directed publish stored %d retained topics, want 0", n)
	}
}

// TestPublishToClientQueuesForOfflineSession pins the stored-session half: the
// publication is queued even though it matches no subscription, and the reconnect
// drains it right after the CONNACK.
func TestPublishToClientQueuesForOfflineSession(t *testing.T) {
	b := startBroker(t, Config{})
	offlineSession(t, b, "direct-offline", "queued/#", 1)

	// The topic deliberately does not match the session's filter: a directed
	// publish addresses the session, not a subscription.
	err := b.PublishToClient(context.Background(), "direct-offline", directPublication(b, "elsewhere/x", "for-you", 1))
	if err != nil {
		t.Fatalf("PublishToClient: %v", err)
	}
	if depth := offlineDepth(b); depth != 1 {
		t.Fatalf("offline depth = %d, want 1", depth)
	}

	c, ack := mustConnect(t, b.Addr(), confConnect{clientID: "direct-offline", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the reconnecting session did not report Session Present")
	}
	pub := expectPublish(t, c)
	if pub.Topic != "elsewhere/x" || string(pub.Payload) != "for-you" {
		t.Fatalf("queued directed delivery = %q %q", pub.Topic, pub.Payload)
	}
	if pub.QoS != 1 {
		t.Fatalf("queued directed delivery QoS = %d, want 1", pub.QoS)
	}
	c.ack(pub.PacketID)
	expectNoMessage(t, c, 100*time.Millisecond)
}

// TestPublishToClientUnknownClientID pins the fire-and-forget rule: a client id
// nobody holds is not an error, it is counted.
func TestPublishToClientUnknownClientID(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics})

	if err := b.PublishToClient(context.Background(), "ghost", directPublication(b, "nowhere/x", "lost", 1)); err != nil {
		t.Fatalf("PublishToClient for an unknown client id = %v, want nil", err)
	}
	if got := metrics.count("target_unknown"); got != 1 {
		t.Fatalf("target_unknown = %d, want 1", got)
	}

	// A caller mistake is an error, not a counted drop.
	if err := b.PublishToClient(context.Background(), "", directPublication(b, "nowhere/x", "lost", 0)); !errors.Is(err, ErrClientIDRequired) {
		t.Fatalf("PublishToClient without a client id = %v, want ErrClientIDRequired", err)
	}
	// A publication of another namespace is refused instead of being delivered to
	// a session of this broker or silently dropped.
	foreign := Publication{Namespace: "tenant-b", Topic: "nowhere/x"}
	if err := b.PublishToClient(context.Background(), "ghost", foreign); !errors.Is(err, ErrNamespaceMismatch) {
		t.Fatalf("PublishToClient for a foreign namespace = %v, want ErrNamespaceMismatch", err)
	}
	if got := metrics.count("target_unknown"); got != 1 {
		t.Fatalf("target_unknown after refused calls = %d, want 1", got)
	}
}

// directedCluster is a Cluster backend that records what the broker hands it, so a
// test can assert the target event and the error the broker reports.
type directedCluster struct {
	mu     sync.Mutex
	events []ClusterMessage
	err    error
}

func (c *directedCluster) Start(context.Context, string, ClusterHandler) error { return nil }

func (c *directedCluster) Stop(context.Context) error { return nil }

func (c *directedCluster) Spread(_ context.Context, msg ClusterMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, msg)
	return c.err
}

func (c *directedCluster) snapshot() []ClusterMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ClusterMessage(nil), c.events...)
}

func (c *directedCluster) setError(err error) {
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
}

// TestPublishToClientHandsRemoteClientToCluster pins the remote half: a client id
// that is not local becomes a ClusterTarget event with the publication in it, and a
// backend failure is reported to the caller instead of being swallowed.
func TestPublishToClientHandsRemoteClientToCluster(t *testing.T) {
	cluster := &directedCluster{}
	b := startBroker(t, Config{Namespace: "tenant-a", ClusterBackend: cluster})

	err := b.PublishToClient(context.Background(), "remote-client", Publication{
		Topic:   "direct/x",
		Payload: []byte("remote"),
		QoS:     1,
		Retain:  true,
	})
	if err != nil {
		t.Fatalf("PublishToClient: %v", err)
	}
	events := cluster.snapshot()
	if len(events) != 1 {
		t.Fatalf("cluster received %d events, want 1", len(events))
	}
	ev := events[0]
	if ev.Type != ClusterTarget {
		t.Fatalf("event type = %q, want %q", ev.Type, ClusterTarget)
	}
	if ev.ClientID != "remote-client" || ev.Topic != "direct/x" || string(ev.Payload) != "remote" || ev.QoS != 1 || !ev.Retain {
		t.Fatalf("target event = %+v", ev)
	}
	if ev.Namespace != "tenant-a" {
		t.Fatalf("target event namespace = %q, want the broker's", ev.Namespace)
	}

	cluster.setError(ErrClusterUnavailable)
	if err := b.PublishToClient(context.Background(), "remote-client", directPublication(b, "direct/x", "remote", 0)); !errors.Is(err, ErrClusterUnavailable) {
		t.Fatalf("PublishToClient with an unavailable cluster = %v, want ErrClusterUnavailable", err)
	}
}

// TestOnRemoteTargetDeliversAndQueues pins the receive half of a target event: the
// client id is delivered to when it is connected here, queued when it is an
// offline persistent session, and counted when this node does not hold it.
func TestOnRemoteTargetDeliversAndQueues(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics})

	live, _ := mustConnect(t, b.Addr(), confConnect{clientID: "remote-live", clean: true})
	b.OnRemoteTarget(ClusterMessage{
		Type: ClusterTarget, NodeID: "peer", Origin: "peer", Seq: 1,
		ClientID: "remote-live", Topic: "direct/x", Payload: []byte("hi"), QoS: 1,
	})
	pub := expectPublish(t, live)
	if pub.Topic != "direct/x" || string(pub.Payload) != "hi" || pub.QoS != 1 {
		t.Fatalf("remote directed delivery = %q %q QoS %d", pub.Topic, pub.Payload, pub.QoS)
	}
	live.ack(pub.PacketID)

	offlineSession(t, b, "remote-offline", "queue/#", 1)
	b.OnRemoteTarget(ClusterMessage{
		Type: ClusterTarget, NodeID: "peer", Origin: "peer", Seq: 2,
		ClientID: "remote-offline", Topic: "direct/x", Payload: []byte("queued"), QoS: 1,
	})
	if depth := offlineDepth(b); depth != 1 {
		t.Fatalf("offline depth after a remote target = %d, want 1", depth)
	}

	before := metrics.count("target_unknown")
	b.OnRemoteTarget(ClusterMessage{
		Type: ClusterTarget, NodeID: "peer", Origin: "peer", Seq: 3,
		ClientID: "not-here", Topic: "direct/x", Payload: []byte("lost"),
	})
	if got := metrics.count("target_unknown"); got != before+1 {
		t.Fatalf("target_unknown after a target for another node = %d, want %d", got, before+1)
	}

	// A target event of another namespace never reaches a session, even when the
	// client id matches one here.
	b.OnRemoteTarget(ClusterMessage{
		Type: ClusterTarget, NodeID: "peer", Origin: "peer", Seq: 4, Namespace: "tenant-z",
		ClientID: "remote-live", Topic: "direct/x", Payload: []byte("foreign"),
	})
	expectNoMessage(t, live, 150*time.Millisecond)
	if got := metrics.count("cluster_namespace"); got == 0 {
		t.Fatal("a foreign-namespace target event was not counted")
	}
}

// TestRedisClusterDirectedPublishUsesPresence pins the Redis flow: a client
// connected to node b is found through its presence record, the event is written to
// b's channel only, and it is delivered there. An offline persistent session keeps
// its presence, so a directed publish is still queued for it on the right node.
func TestRedisClusterDirectedPublishUsesPresence(t *testing.T) {
	mr := runMiniRedis(t)
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: NewRedisCluster(redisTestConfig(mr)), Logger: discardLogger()})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: NewRedisCluster(redisTestConfig(mr)), Logger: discardLogger()})
	ca := a.cfg.ClusterBackend.(*redisCluster)
	cb := b.cfg.ClusterBackend.(*redisCluster)
	waitRedisMembers(t, ca, "b")
	waitRedisMembers(t, cb, "a")

	target, _ := mustConnect(t, b.Addr(), confConnect{clientID: "redis-target", clean: true})
	waitFor(t, "the presence record of redis-target", func() bool {
		return mr.Exists(cb.presenceKey("", "redis-target"))
	})

	// Watch both node channels: the point of presence routing is that the origin
	// node does not publish a directed event to itself or to everyone.
	rdb := redisTestClient(t, mr)
	sub := rdb.Subscribe(context.Background(), ca.nodeChannel("a"), cb.nodeChannel("b"))
	t.Cleanup(func() { _ = sub.Close() })
	for i := 0; i < 2; i++ {
		if _, err := sub.ReceiveTimeout(context.Background(), 2*time.Second); err != nil {
			t.Fatalf("subscribe confirmation: %v", err)
		}
	}

	if err := a.PublishToClient(context.Background(), "redis-target", directPublication(a, "direct/x", "redis", 1)); err != nil {
		t.Fatalf("PublishToClient: %v", err)
	}
	pub := expectPublish(t, target)
	if pub.Topic != "direct/x" || string(pub.Payload) != "redis" || pub.QoS != 1 {
		t.Fatalf("redis directed delivery = %q %q QoS %d", pub.Topic, pub.Payload, pub.QoS)
	}
	target.ack(pub.PacketID)

	msg, err := sub.ReceiveTimeout(context.Background(), 2*time.Second)
	if err != nil {
		t.Fatalf("read the node channel: %v", err)
	}
	received, ok := msg.(*redis.Message)
	if !ok || received.Channel != cb.nodeChannel("b") {
		t.Fatalf("directed publish went to %v, want node b's channel", msg)
	}
	if extra, err := sub.ReceiveTimeout(context.Background(), 150*time.Millisecond); err == nil {
		t.Fatalf("a directed publish was published more than once: %v", extra)
	}

	// A client id with no presence record is fire-and-forget: nil, and counted by
	// the backend that tried to route it (node a).
	drops := ca.dropCount()
	if err := a.PublishToClient(context.Background(), "redis-ghost", directPublication(a, "direct/x", "lost", 0)); err != nil {
		t.Fatalf("PublishToClient for an unknown client id = %v, want nil", err)
	}
	if got := ca.dropCount(); got <= drops {
		t.Fatalf("presence miss was not counted: %d drops, was %d", got, drops)
	}

	// An offline persistent session stays reachable: its presence record is kept
	// (the node owns its queue) and the publication is queued on node b.
	offlineSession(t, b, "redis-offline", "queue/#", 1)
	waitFor(t, "the presence record of the offline session", func() bool {
		return mr.Exists(cb.presenceKey("", "redis-offline"))
	})
	if err := a.PublishToClient(context.Background(), "redis-offline", directPublication(a, "direct/x", "queued", 1)); err != nil {
		t.Fatalf("PublishToClient for the offline session: %v", err)
	}
	waitFor(t, "the directed publish to be queued on node b", func() bool { return offlineDepth(b) == 1 })

	back, ack := mustConnect(t, b.Addr(), confConnect{clientID: "redis-offline", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the offline session did not resume")
	}
	pub = expectPublish(t, back)
	if string(pub.Payload) != "queued" || pub.QoS != 1 {
		t.Fatalf("queued directed delivery = %q QoS %d", pub.Payload, pub.QoS)
	}
	back.ack(pub.PacketID)
}

// TestTCPClusterDirectedPublishBroadcasts pins the TCP trade-off: the mesh has no
// presence, so a directed publish is broadcast to the peer, which applies it only
// because it holds the client id, and the broadcast is counted.
func TestTCPClusterDirectedPublishBroadcasts(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(testClusterConfig(aAddr, []string{bAddr}))
	cb := NewTCPCluster(testClusterConfig(bAddr, []string{aAddr}))
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)

	target, _ := mustConnect(t, b.Addr(), confConnect{clientID: "tcp-target", clean: true})
	bystander, _ := mustConnect(t, a.Addr(), confConnect{clientID: "tcp-bystander", clean: true})

	if err := a.PublishToClient(context.Background(), "tcp-target", directPublication(a, "direct/x", "tcp", 1)); err != nil {
		t.Fatalf("PublishToClient: %v", err)
	}
	pub := expectPublish(t, target)
	if pub.Topic != "direct/x" || string(pub.Payload) != "tcp" || pub.QoS != 1 {
		t.Fatalf("tcp directed delivery = %q %q QoS %d", pub.Topic, pub.Payload, pub.QoS)
	}
	target.ack(pub.PacketID)
	expectNoMessage(t, bystander, 150*time.Millisecond)
	if got := ca.(*tcpCluster).targetFanoutCount(); got == 0 {
		t.Fatal("the directed publish was not counted as a broadcast")
	}

	// A peer that holds the id but has it offline and persistent queues it, so a
	// directed publish to an offline session behaves like a normal publish.
	offlineSession(t, b, "tcp-offline", "queue/#", 1)
	if err := a.PublishToClient(context.Background(), "tcp-offline", directPublication(a, "direct/x", "queued", 1)); err != nil {
		t.Fatalf("PublishToClient for the offline session: %v", err)
	}
	waitFor(t, "the directed publish to be queued on node b", func() bool { return offlineDepth(b) == 1 })
}

// TestTCPClusterDirectedPublishDeduplicates pins that a redelivered target event is
// applied once: the backends deduplicate on (Origin, Seq) like every other event.
func TestTCPClusterDirectedPublishDeduplicates(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(testClusterConfig(aAddr, []string{bAddr}))
	cb := NewTCPCluster(testClusterConfig(bAddr, []string{aAddr}))
	startBroker(t, Config{NodeID: "a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)

	target, _ := mustConnect(t, b.Addr(), confConnect{clientID: "dedup-target", clean: true})
	backend := cb.(*tcpCluster)
	event := ClusterMessage{
		Type: ClusterTarget, NodeID: "a", Origin: "a", Seq: 7,
		ClientID: "dedup-target", Topic: "direct/x", Payload: []byte("once"), QoS: 0,
	}
	backend.dispatchEvent(event)
	pub := expectPublish(t, target)
	if string(pub.Payload) != "once" {
		t.Fatalf("directed delivery = %q", pub.Payload)
	}
	backend.dispatchEvent(event)
	expectNoMessage(t, target, 200*time.Millisecond)
}

// TestDirectedPublishStaysInNamespace pins namespace isolation for the directed
// path: two brokers of different namespaces may hold the same client id, a directed
// publish reaches only the broker of its own scope, and a target event of another
// namespace is dropped and counted on the receiving node.
func TestDirectedPublishStaysInNamespace(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	metricsB := newDeliveryMetrics()
	cfgB := testClusterConfig(bAddr, []string{aAddr})
	cfgB.Metrics = metricsB
	ca := NewTCPCluster(testClusterConfig(aAddr, []string{bAddr}))
	cb := NewTCPCluster(cfgB)
	a := startBroker(t, Config{NodeID: "a", Namespace: "tenant-a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", Namespace: "tenant-b", ClusterBackend: cb, Metrics: metricsB})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)

	localA, _ := mustConnect(t, a.Addr(), confConnect{clientID: "shared-id", clean: true})
	remoteB, _ := mustConnect(t, b.Addr(), confConnect{clientID: "shared-id", clean: true})

	// The client id is local in tenant-a, so only tenant-a's session receives it.
	if err := a.PublishToClient(context.Background(), "shared-id", directPublication(a, "direct/x", "tenant-a", 0)); err != nil {
		t.Fatalf("PublishToClient: %v", err)
	}
	pub := expectPublish(t, localA)
	if string(pub.Payload) != "tenant-a" {
		t.Fatalf("tenant-a directed delivery = %q", pub.Payload)
	}
	expectNoMessage(t, remoteB, 200*time.Millisecond)

	// A client id that only tenant-b holds is not found locally in tenant-a, so the
	// event is spread; tenant-b drops it because the event belongs to tenant-a.
	onlyB, _ := mustConnect(t, b.Addr(), confConnect{clientID: "only-in-b", clean: true})
	if err := a.PublishToClient(context.Background(), "only-in-b", directPublication(a, "direct/x", "cross", 0)); err != nil {
		t.Fatalf("PublishToClient for a remote client id: %v", err)
	}
	expectNoMessage(t, onlyB, 300*time.Millisecond)
	waitFor(t, "the foreign-namespace target event to be counted", func() bool {
		return metricsB.count("cluster_namespace") > 0
	})

	// An explicit namespace of the other scope is refused by the publishing broker.
	foreign := Publication{Namespace: "tenant-b", Topic: "direct/x"}
	if err := a.PublishToClient(context.Background(), "only-in-b", foreign); !errors.Is(err, ErrNamespaceMismatch) {
		t.Fatalf("PublishToClient with a foreign namespace = %v, want ErrNamespaceMismatch", err)
	}
}

// TestRedisClusterForgetsExpiredSessionPresence pins the other half of the
// presence lifetime: a persistent session keeps its record (so a directed publish
// is queued for it), and the record is removed when the session expires, so peers
// stop routing it to a node that no longer holds it. The PresenceTTL is longer
// than the test, so the removal cannot be a natural TTL expiry.
func TestRedisClusterForgetsExpiredSessionPresence(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	cfg.PresenceTTL = 500 * time.Millisecond
	b := startBroker(t, Config{
		NodeID:               "b",
		ClusterBackend:       NewRedisCluster(cfg),
		Logger:               discardLogger(),
		unit:                 testSecond,
		SessionSweepInterval: 5 * time.Millisecond,
	})
	cb := b.cfg.ClusterBackend.(*redisCluster)

	c, _ := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "expiring-presence",
		clean:    false,
		props:    expiryProps(1),
	})
	subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "q/#", QoS: 0})
	c.disconnect()
	waitDisconnected(t, b, "expiring-presence")

	key := cb.presenceKey("", "expiring-presence")
	waitFor(t, "the presence record of the offline session", func() bool { return mr.Exists(key) })
	waitFor(t, "the stored session to expire", func() bool {
		_, ok := b.store.Load("expiring-presence")
		return !ok
	})
	waitFor(t, "the presence record of the expired session to be removed", func() bool {
		return !mr.Exists(key)
	})
}

// presenceNode reads the node id a presence record names (used by the takeover
// case).
func presenceNode(t *testing.T, mr *miniredis.Miniredis, key string) string {
	t.Helper()
	value, err := mr.Get(key)
	if err != nil {
		return ""
	}
	if i := strings.IndexByte(value, 0); i >= 0 {
		return value[:i]
	}
	return value
}

// TestRedisClusterPresenceFollowsTakeover pins the presence lifetime across a
// node change: a client that reconnects to another node is kicked on the old one,
// the old node stops refreshing the record (otherwise the two nodes would fight
// over it every refresh), and a directed publish follows the client to its new
// node. NotKickSeconds -1 disables the anti-flap window, which the receive side
// applies as well and which would refuse a kick for a connection this young: this
// case is about presence following the client, not about the takeover rate.
func TestRedisClusterPresenceFollowsTakeover(t *testing.T) {
	mr := runMiniRedis(t)
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: NewRedisCluster(redisTestConfig(mr)), Logger: discardLogger(), NotKickSeconds: -1})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: NewRedisCluster(redisTestConfig(mr)), Logger: discardLogger(), NotKickSeconds: -1})
	ca := a.cfg.ClusterBackend.(*redisCluster)
	cb := b.cfg.ClusterBackend.(*redisCluster)
	waitRedisMembers(t, ca, "b")
	waitRedisMembers(t, cb, "a")

	key := ca.presenceKey("", "roamer")
	mustConnect(t, a.Addr(), confConnect{clientID: "roamer", clean: true})
	waitFor(t, "the presence record of the first node", func() bool {
		return mr.Exists(key) && presenceNode(t, mr, key) == "a"
	})

	// The same client id connects to node b; b's connect-time kick displaces the
	// connection on a, which must give the presence record up.
	second, _ := mustConnect(t, b.Addr(), confConnect{clientID: "roamer", clean: true})
	waitDisconnected(t, a, "roamer")
	waitFor(t, "the presence record to move to node b", func() bool {
		return presenceNode(t, mr, key) == "b"
	})
	waitFor(t, "the old node to drop the routing entry", func() bool {
		_, ok := a.index.session("roamer")
		return !ok
	})
	// Several refresh periods pass; the old node must not take the record back.
	time.Sleep(5 * 20 * time.Millisecond)
	if node := presenceNode(t, mr, key); node != "b" {
		t.Fatalf("presence record names node %q after the takeover, want b", node)
	}

	if err := a.PublishToClient(context.Background(), "roamer", directPublication(a, "direct/x", "follow", 1)); err != nil {
		t.Fatalf("PublishToClient: %v", err)
	}
	pub := expectPublish(t, second)
	if pub.Topic != "direct/x" || string(pub.Payload) != "follow" || pub.QoS != 1 {
		t.Fatalf("directed delivery after the takeover = %q %q QoS %d", pub.Topic, pub.Payload, pub.QoS)
	}
	second.ack(pub.PacketID)
}
