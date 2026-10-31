package gmqx

// Redis-backed session store: the opt-in SessionStore that lets the nodes of a
// cluster share persistent sessions, so a client that reconnects to another node
// resumes its subscriptions and the messages queued for it instead of starting from
// nothing. The cases below pin the stored layout, the TTL rules, the migration path
// between two nodes, the atomic take that keeps two nodes from serving one session,
// and what a kick does to a session the peer may already own.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/maxid/gnety/codec/mqtt"
)

// newTestRedisSessionStore builds the store against the test miniredis and closes
// it at the end of the test.
func newTestRedisSessionStore(t *testing.T, mr *miniredis.Miniredis, metrics MetricsManager) *RedisSessionStore {
	t.Helper()
	store, err := NewRedisSessionStore(RedisSessionStoreConfig{
		Addr:    mr.Addr(),
		Metrics: metrics,
		Logger:  discardLogger(),
	})
	if err != nil {
		t.Fatalf("new redis session store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// sessionNode starts a broker of nodeID that keeps its sessions in store.
func sessionNode(t *testing.T, nodeID string, store SessionStore) *Broker {
	t.Helper()
	return startBroker(t, Config{NodeID: nodeID, SessionStore: store, Logger: discardLogger()})
}

// sessionStoreKey is the Redis key a store with the default prefix keeps for a
// broker session key.
func sessionStoreKey(key string) string { return defaultRedisChannelPrefix + ":session:" + key }

// offlineSessionOn subscribes a persistent session of b to filter at qos,
// disconnects it and waits until b stored it. The key is the namespace-qualified one
// the broker uses, so it works for a broker of any namespace.
func offlineSessionOn(t *testing.T, b *Broker, clientID, filter string, qos byte) {
	t.Helper()
	key := namespaceKey(b.namespace, clientID)
	c, ack := mustConnect(t, b.Addr(), confConnect{clientID: clientID, clean: false})
	if ack.SessionPresent {
		t.Fatalf("client %q resumed a session before it had one", clientID)
	}
	if codes := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: filter, QoS: qos}).ReasonCodes; len(codes) != 1 || codes[0] != qos {
		t.Fatalf("SUBACK codes %v, want [%d]", codes, qos)
	}
	c.disconnect()
	waitDisconnected(t, b, clientID)
	waitFor(t, "client "+clientID+" to be stored", func() bool {
		_, ok := b.store.Load(key)
		return ok
	})
}

// TestRedisSessionStoreRoundTrip pins the stored encoding: a SessionState comes back
// with its subscriptions and their options, its queue with payload, QoS, retain flag
// and property block, and its deadline — through Save/Load and through Take, which
// also removes the key.
func TestRedisSessionStoreRoundTrip(t *testing.T) {
	mr := runMiniRedis(t)
	store := newTestRedisSessionStore(t, mr, nil)

	props, err := encodePropertyBlock(mqtt.Properties{
		ContentType:    "application/json",
		UserProperties: []mqtt.UserProperty{{Key: "device", Value: "42"}},
	})
	if err != nil {
		t.Fatalf("encode properties: %v", err)
	}
	want := SessionState{
		ClientID: "round-trip",
		Subscriptions: map[string]SessionSubscription{
			"sensors/+/temp": {QoS: 1, RetainAsPublished: true},
			"plant/#":        {QoS: 2, NoLocal: true, RetainHandling: 2},
		},
		Queue: []QueuedMessage{
			{Topic: "sensors/a/temp", Payload: []byte{0, 1, 2}, QoS: 1, Retain: true, Props: props},
			{Topic: "plant/1", Payload: []byte("dry"), QoS: 0},
		},
		// A future deadline without a monotonic reading, which is what the store
		// reconstructs: an equal instant that DeepEqual can compare.
		ExpiresAt: time.Unix(time.Now().Unix()+3600, 42),
	}
	store.Save("round-trip", want)

	if !mr.Exists(sessionStoreKey("round-trip")) {
		t.Fatalf("the session is not under %q", sessionStoreKey("round-trip"))
	}
	got, ok := store.Load("round-trip")
	if !ok {
		t.Fatal("Load reported no session after Save")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loaded session = %+v, want %+v", got, want)
	}
	if keys := store.Keys(); len(keys) != 1 || keys[0] != "round-trip" {
		t.Fatalf("Keys = %v, want [round-trip]", keys)
	}

	taken, ok := store.Take("round-trip")
	if !ok || !reflect.DeepEqual(taken, want) {
		t.Fatalf("Take = %+v (ok=%v), want the saved state", taken, ok)
	}
	if mr.Exists(sessionStoreKey("round-trip")) {
		t.Fatal("Take left the key behind")
	}
	if _, ok := store.Take("round-trip"); ok {
		t.Fatal("a second Take returned a session that was already taken")
	}

	store.Save("delete-me", SessionState{ClientID: "delete-me"})
	store.Delete("delete-me")
	if mr.Exists(sessionStoreKey("delete-me")) {
		t.Fatal("Delete left the key behind")
	}
	// Deleting a missing key is a no-op.
	store.Delete("never-stored")
}

// TestRedisSessionStoreTTL pins the TTL rule: the key expires with the session's own
// deadline, a session without a deadline has no TTL, and a state whose deadline has
// already passed is not written at all.
func TestRedisSessionStoreTTL(t *testing.T) {
	mr := runMiniRedis(t)
	store := newTestRedisSessionStore(t, mr, nil)

	store.Save("expiring", SessionState{ClientID: "expiring", ExpiresAt: time.Now().Add(500 * time.Millisecond)})
	ttl := mr.TTL(sessionStoreKey("expiring"))
	if ttl <= 0 || ttl > 500*time.Millisecond {
		t.Fatalf("TTL of an expiring session = %v, want (0, 500ms]", ttl)
	}

	store.Save("forever", SessionState{ClientID: "forever"})
	if ttl := mr.TTL(sessionStoreKey("forever")); ttl != 0 {
		t.Fatalf("TTL of a session without a deadline = %v, want none", ttl)
	}

	store.Save("already-over", SessionState{ClientID: "already-over", ExpiresAt: time.Now().Add(-time.Second)})
	if mr.Exists(sessionStoreKey("already-over")) {
		t.Fatal("a session whose deadline passed was stored")
	}

	// Redis removes the key when the deadline arrives, so the sweeper is not the only
	// thing that ends an abandoned session.
	mr.FastForward(time.Second)
	if mr.Exists(sessionStoreKey("expiring")) {
		t.Fatal("the expiring session outlived its TTL")
	}
	if !mr.Exists(sessionStoreKey("forever")) {
		t.Fatal("the session without a deadline expired")
	}
}

// TestRedisSessionStoreExpiryFromBroker pins the whole TTL path: the deadline a
// broker stamps from an MQTT 5 Session Expiry Interval becomes the key's TTL, so a
// shared session disappears on its own.
func TestRedisSessionStoreExpiryFromBroker(t *testing.T) {
	mr := runMiniRedis(t)
	store := newTestRedisSessionStore(t, mr, newDeliveryMetrics())
	b := startBroker(t, Config{SessionStore: store, Logger: discardLogger(), unit: testSecond})

	// The protocol second is 20 ms here, so a Session Expiry Interval of 5 is 100 ms.
	c, _ := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "expiring-shared",
		clean:    false,
		props:    mqtt.Properties{SessionExpiryInterval: 5},
	})
	subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "expiring/#", QoS: 0})
	c.disconnect()
	waitDisconnected(t, b, "expiring-shared")

	key := sessionStoreKey("expiring-shared")
	if !mr.Exists(key) {
		t.Fatal("the session was not stored")
	}
	if ttl := mr.TTL(key); ttl <= 0 || ttl > 5*testSecond {
		t.Fatalf("TTL of the session = %v, want (0, %v]", ttl, 5*testSecond)
	}
	mr.FastForward(6 * testSecond)
	if mr.Exists(key) {
		t.Fatal("the session outlived its Session Expiry Interval")
	}
}

// TestRedisSessionStoreCorruptValueDropped pins the failure mode of a value that does
// not decode: it is dropped and counted, the key is removed, and the broker keeps
// serving its other sessions instead of failing the CONNECT.
func TestRedisSessionStoreCorruptValueDropped(t *testing.T) {
	mr := runMiniRedis(t)
	metrics := newDeliveryMetrics()
	store := newTestRedisSessionStore(t, mr, metrics)
	rdb := redisTestClient(t, mr)
	ctx := context.Background()

	// A value from another build or another writer: not a session record at all.
	if err := rdb.Set(ctx, sessionStoreKey("broken"), "not-a-session", 0).Err(); err != nil {
		t.Fatal(err)
	}
	b := sessionNode(t, "a", store)
	offlineSessionOn(t, b, "healthy", "ok/#", 1)

	if state, ok := store.Load("broken"); ok {
		t.Fatalf("an undecodable value was loaded as %+v", state)
	}
	if metrics.count("session_store_corrupt") == 0 {
		t.Fatal("the undecodable value was not counted")
	}
	if mr.Exists(sessionStoreKey("broken")) {
		t.Fatal("the undecodable value was not removed")
	}

	// A key that only holds a broken record is not a session: a CONNECT for that id
	// starts a new one instead of failing.
	_, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "broken", clean: false})
	if ack.SessionPresent {
		t.Fatal("the undecodable session was resumed")
	}

	// The broker is unaffected: the healthy session still resumes with its queue.
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "corrupt-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "ok/x", payload: "fine", qos: 1, pid: 1})
	back, backAck := mustConnect(t, b.Addr(), confConnect{clientID: "healthy", clean: false})
	if !backAck.SessionPresent {
		t.Fatal("the healthy session did not resume")
	}
	if got := expectPublish(t, back); string(got.Payload) != "fine" {
		t.Fatalf("queued delivery = %q, want fine", got.Payload)
	}
}

// TestSessionMigrationAcrossNodes is the feature: a persistent client subscribes on
// node a and goes away, messages are published while it is away, and it reconnects on
// node b, which takes the session out of the shared store — Session Present set, the
// subscriptions restored and the queued messages in publication order, properties
// included.
func TestSessionMigrationAcrossNodes(t *testing.T) {
	mr := runMiniRedis(t)
	store := newTestRedisSessionStore(t, mr, newDeliveryMetrics())
	a, b := sessionNode(t, "a", store), sessionNode(t, "b", store)

	// The session is created on a and stored there when the client leaves.
	migrate, ack := mustConnect(t, a.Addr(), confConnect{
		version: mqtt.Version5, clientID: "roamer", clean: false,
		props: mqtt.Properties{SessionExpiryInterval: 3600},
	})
	if ack.SessionPresent {
		t.Fatal("the first CONNECT reported a session it could not have")
	}
	subscribe(t, migrate, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "roam/#", QoS: 1, RetainAsPublished: true})
	migrate.disconnect()
	waitDisconnected(t, a, "roamer")
	if !mr.Exists(sessionStoreKey("roamer")) {
		t.Fatal("node a did not store the session in Redis")
	}

	// Messages published on a while the session is away are queued in the store.
	pub, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "roam-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "roam/one", payload: "one", qos: 1, pid: 1})
	pub.mustPublish(t, confPublish{
		topic: "roam/two", payload: "two", qos: 1, pid: 2,
		props: mqtt.Properties{UserProperties: []mqtt.UserProperty{{Key: "seq", Value: "2"}}},
	})

	// The reconnect on b takes the session over from the shared store.
	back, backAck := mustConnect(t, b.Addr(), confConnect{
		version: mqtt.Version5, clientID: "roamer", clean: false,
		props: mqtt.Properties{SessionExpiryInterval: 3600},
	})
	if !backAck.SessionPresent {
		t.Fatal("node b did not resume the session stored by node a")
	}
	first := expectPublish(t, back)
	second := expectPublish(t, back)
	if first.Topic != "roam/one" || string(first.Payload) != "one" || first.QoS != 1 {
		t.Fatalf("first queued delivery = %q %q QoS %d", first.Topic, first.Payload, first.QoS)
	}
	if second.Topic != "roam/two" || string(second.Payload) != "two" || second.QoS != 1 {
		t.Fatalf("second queued delivery = %q %q QoS %d", second.Topic, second.Payload, second.QoS)
	}
	if len(second.Properties.UserProperties) != 1 || second.Properties.UserProperties[0].Key != "seq" {
		t.Fatalf("the properties of the queued message did not survive: %+v", second.Properties.UserProperties)
	}
	back.ack(first.PacketID)
	back.ack(second.PacketID)

	// The subscriptions were restored too, so a live publish on the node that now
	// owns the session reaches it.
	livePub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "roam-pub-b", clean: true})
	livePub.mustPublish(t, confPublish{topic: "roam/three", payload: "live", qos: 1, pid: 1})
	live := expectPublish(t, back)
	if live.Topic != "roam/three" || string(live.Payload) != "live" {
		t.Fatalf("delivery over a restored subscription = %q %q", live.Topic, live.Payload)
	}
	back.ack(live.PacketID)

	// The session moved: the store no longer holds it while b owns the connection.
	if mr.Exists(sessionStoreKey("roamer")) {
		t.Fatal("the resumed session is still stored while a connection owns it")
	}
	if _, ok := store.Load("roamer"); ok {
		t.Fatal("the store still hands out the resumed session")
	}
}

// TestSharedSessionCleanConnectDiscards pins that a clean CONNECT discards the shared
// session, exactly as MQTT requires and as the in-memory store does: whichever node
// receives the CONNECT deletes the stored state the other node wrote.
func TestSharedSessionCleanConnectDiscards(t *testing.T) {
	mr := runMiniRedis(t)
	store := newTestRedisSessionStore(t, mr, newDeliveryMetrics())
	a, b := sessionNode(t, "a", store), sessionNode(t, "b", store)

	offlineSessionOn(t, a, "discarded", "state/#", 1)
	if !mr.Exists(sessionStoreKey("discarded")) {
		t.Fatal("node a did not store the session")
	}

	clean, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "discarded", clean: true})
	if ack.SessionPresent {
		t.Fatal("a clean CONNECT reported a session it just discarded")
	}
	if mr.Exists(sessionStoreKey("discarded")) {
		t.Fatal("the clean CONNECT left the stored session behind")
	}
	clean.disconnect()
	waitDisconnected(t, b, "discarded")

	// The next persistent CONNECT starts from nothing.
	_, ack = mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "discarded", clean: false})
	if ack.SessionPresent {
		t.Fatal("the discarded session was resumed")
	}
}

// TestSharedSessionTakeIsAtomic pins that one stored session is handed out once:
// concurrent takes of the same key (the store seam) and concurrent persistent CONNECTs
// of the same client id on two nodes (the broker path) must produce exactly one
// session owner, never two.
func TestSharedSessionTakeIsAtomic(t *testing.T) {
	mr := runMiniRedis(t)
	store := newTestRedisSessionStore(t, mr, newDeliveryMetrics())

	store.Save("atomic", SessionState{
		ClientID:      "atomic",
		Subscriptions: map[string]SessionSubscription{"atomic/#": {QoS: 1}},
	})

	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := store.Take("atomic"); ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d concurrent takes returned the session, want exactly 1", wins)
	}

	// The broker path: two nodes reconnect the same client id at the same time.
	a, b := sessionNode(t, "a", store), sessionNode(t, "b", store)
	store.Save("racing", SessionState{
		ClientID:      "racing",
		Subscriptions: map[string]SessionSubscription{"racing/#": {QoS: 1}},
	})
	type attempt struct {
		present bool
		err     error
	}
	attempts := make(chan attempt, 2)
	for _, node := range []*Broker{a, b} {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := connectSessionPresent(node.Addr(), "racing")
			attempts <- attempt{present: got, err: err}
		}()
	}
	wg.Wait()
	close(attempts)
	resumed := 0
	for got := range attempts {
		if got.err != nil {
			t.Fatalf("concurrent persistent CONNECT: %v", got.err)
		}
		if got.present {
			resumed++
		}
	}
	if resumed != 1 {
		t.Fatalf("%d nodes resumed the same session, want exactly 1", resumed)
	}
	if mr.Exists(sessionStoreKey("racing")) {
		t.Fatal("the taken session is still stored")
	}
}

// TestSharedSessionKeptWhenPeerTookOver pins the takeover rule of a store the nodes
// share: a connection displaced by a peer's kick must not write its own copy of the
// session back (which would resurrect a session the peer is serving) and must not
// delete the stored state (which would erase the session the peer saved).
func TestSharedSessionKeptWhenPeerTookOver(t *testing.T) {
	for _, tc := range []struct {
		name    string
		clean   bool
		persist SessionState
	}{
		{
			// A clean connection would delete the key on its way out.
			name:    "clean connection",
			clean:   true,
			persist: SessionState{ClientID: "handover", Subscriptions: map[string]SessionSubscription{"peer/#": {QoS: 1}}},
		},
		{
			// A persistent connection would save its own copy over the peer's state.
			name:  "persistent connection",
			clean: false,
			persist: SessionState{
				ClientID:      "handover",
				Subscriptions: map[string]SessionSubscription{"peer/#": {QoS: 1}},
				Queue:         []QueuedMessage{{Topic: "peer/x", Payload: []byte("peer"), QoS: 1}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mr := runMiniRedis(t)
			store := newRecordingSharedStore(t, mr)
			backend := &recordingCluster{}
			b := startBroker(t, Config{
				NodeID: "a", SessionStore: store, ClusterBackend: backend,
				Metrics: newDeliveryMetrics(), NotKickSeconds: -1, Logger: discardLogger(),
			})

			conn, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "handover", clean: tc.clean})
			if !tc.clean {
				subscribe(t, conn, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "handover/#", QoS: 1})
			}
			// The peer that kicks this connection took the session over and stored its
			// own state, which is what a shared store holds after a takeover.
			store.RedisSessionStore.Save("handover", tc.persist)
			store.resetWrites()

			backend.deliver(t, kickFor("handover", "b", time.Now().Add(time.Second)))
			expectDisconnect(t, conn, ReasonSessionTakenOver)
			waitDisconnected(t, b, "handover")

			if writes := store.writesFor("handover"); len(writes) != 0 {
				t.Fatalf("the kicked node wrote the session of a peer: %v", writes)
			}
			got, ok := store.RedisSessionStore.Load("handover")
			if !ok {
				t.Fatal("the kicked node deleted the session the peer owns")
			}
			if !reflect.DeepEqual(got, tc.persist) {
				t.Fatalf("the stored session = %+v, want the peer's %+v", got, tc.persist)
			}
			// The session is no longer a routing target here: nothing on this node may
			// queue for a session the peer owns.
			if _, indexed := b.index.session("handover"); indexed {
				t.Fatal("the session that moved away is still a local routing target")
			}
		})
	}
}

// TestSharedSessionStoreNamespaceIsolation pins namespace isolation on one shared
// store: the same client id in two namespaces is two sessions under two Redis keys,
// and neither broker resumes, queues for or expires the other's.
func TestSharedSessionStoreNamespaceIsolation(t *testing.T) {
	mr := runMiniRedis(t)
	store := newTestRedisSessionStore(t, mr, newDeliveryMetrics())
	a := startBroker(t, Config{Namespace: "tenant-a", SessionStore: store, Logger: discardLogger()})
	b := startBroker(t, Config{Namespace: "tenant-b", SessionStore: store, Logger: discardLogger()})

	keyA := sessionStoreKey(namespaceKey("tenant-a", "shared-id"))
	keyB := sessionStoreKey(namespaceKey("tenant-b", "shared-id"))
	if keyA == keyB {
		t.Fatalf("two namespaces share one key: %q", keyA)
	}
	offlineSessionOn(t, a, "shared-id", "a/#", 1)
	offlineSessionOn(t, b, "shared-id", "b/#", 1)
	if !mr.Exists(keyA) || !mr.Exists(keyB) {
		t.Fatalf("the two namespaces do not hold two keys (%v %v)", mr.Exists(keyA), mr.Exists(keyB))
	}
	if keys := store.Keys(); len(keys) != 2 {
		t.Fatalf("Keys = %v, want the two session keys", keys)
	}

	// A publication of one namespace is queued only for the session of that namespace.
	pub, _ := mustConnect(t, a.Addr(), confConnect{clientID: "ns-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "a/x", payload: "tenant-a", qos: 1, pid: 1})

	storedA, ok := store.Load(namespaceKey("tenant-a", "shared-id"))
	if !ok || len(storedA.Queue) != 1 {
		t.Fatalf("tenant-a queue = %+v (ok=%v), want one message", storedA.Queue, ok)
	}
	storedB, ok := store.Load(namespaceKey("tenant-b", "shared-id"))
	if !ok || len(storedB.Queue) != 0 {
		t.Fatalf("tenant-b queue = %+v (ok=%v), want it untouched", storedB.Queue, ok)
	}

	// Each broker resumes only its own session.
	resumedA, ackA := mustConnect(t, a.Addr(), confConnect{clientID: "shared-id", clean: false})
	if !ackA.SessionPresent {
		t.Fatal("tenant-a did not resume its own session")
	}
	if got := expectPublish(t, resumedA); string(got.Payload) != "tenant-a" {
		t.Fatalf("tenant-a delivery = %q", got.Payload)
	}
	resumedB, ackB := mustConnect(t, b.Addr(), confConnect{clientID: "shared-id", clean: false})
	if !ackB.SessionPresent {
		t.Fatal("tenant-b lost its own session")
	}
	expectNoMessage(t, resumedB, 100*time.Millisecond)
	if keys := store.Keys(); len(keys) != 0 {
		t.Fatalf("the resumed sessions are still stored: %v", keys)
	}
}

// TestNewRedisClusterWithSessionStore pins the opt-in wiring: one RedisClusterConfig
// builds the cluster backend and the session store, both under the configured prefix,
// and the store's keys do not collide with the cluster's own keys.
func TestNewRedisClusterWithSessionStore(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	cfg.ChannelPrefix = "tenant:cluster"
	cfg.Metrics = newDeliveryMetrics()
	cluster, store, err := NewRedisClusterWithSessionStore(cfg)
	if err != nil {
		t.Fatalf("build cluster and store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	backend := cluster.(*redisCluster)
	if err := cluster.Start(context.Background(), "a", &recClusterHandler{}); err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	t.Cleanup(func() { _ = cluster.Stop(context.Background()) })

	store.Save("collide", SessionState{ClientID: "collide"})
	want := "tenant:cluster:session:collide"
	if !mr.Exists(want) {
		t.Fatalf("the session is not under %q", want)
	}
	// The layout of the two kinds of key, under one prefix, shares no segment.
	for _, other := range []string{backend.nodeChannel("collide"), backend.ownerKey("collide"), backend.memberKey("collide"), backend.summaryKey("collide"), backend.presenceKey("", "collide")} {
		if other == want || strings.HasPrefix(other, store.sessionKeyPrefix()) {
			t.Fatalf("a session key collides with the cluster key %q", other)
		}
	}

	// A configuration a deployment has to fail on is reported, not ignored.
	if _, err := NewRedisSessionStore(RedisSessionStoreConfig{}); err == nil {
		t.Fatal("a store without an address was accepted")
	}
	if _, err := NewRedisSessionStore(RedisSessionStoreConfig{Addr: mr.Addr(), ChannelPrefix: "bad*prefix"}); err == nil {
		t.Fatal("a prefix with a SCAN glob was accepted")
	}
	if _, _, err := NewRedisClusterWithSessionStore(RedisClusterConfig{ChannelPrefix: "bad*prefix"}); err == nil {
		t.Fatal("the cluster helper ignored an invalid prefix")
	}
}

// TestMemorySessionStoreIsTheDefault pins that the shared store is opt-in: a broker
// without Config.SessionStore keeps the in-memory store, which does not claim to be
// shared.
func TestMemorySessionStoreIsTheDefault(t *testing.T) {
	b := startBroker(t, Config{})
	if _, ok := b.store.(*MemorySessionStore); !ok {
		t.Fatalf("the default store is %T, want *MemorySessionStore", b.store)
	}
	if b.sharedStore() {
		t.Fatal("the in-memory store claims to be shared across nodes")
	}
}

// connectSessionPresent sends a persistent MQTT 3.1.1 CONNECT and reports the Session
// Present flag of the CONNACK. It touches no *testing.T, so a case can run it from a
// goroutine.
func connectSessionPresent(addr, clientID string) (bool, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	var body []byte
	body = appendString(body, "MQTT")
	body = append(body, 4, 0, 0, 30) // level 4, clean session cleared, keep-alive 30
	body = appendString(body, clientID)
	frame := appendRemainingLength([]byte{0x10}, len(body))
	frame = append(frame, body...)
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		return false, err
	}
	r := bufio.NewReader(conn)
	header, err := r.ReadByte()
	if err != nil {
		return false, err
	}
	n, err := readRemainingLength(r)
	if err != nil {
		return false, err
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return false, err
	}
	if header>>4 != 2 || len(payload) < 2 {
		return false, fmt.Errorf("unexpected CONNACK: %#x %v", header, payload)
	}
	return payload[0]&0x01 != 0, nil
}

// recordingSharedStore wraps the Redis store and records the writes one broker makes,
// so a test can attribute a Save or a Delete to the node that issued it.
type recordingSharedStore struct {
	*RedisSessionStore

	mu     sync.Mutex
	writes map[string][]string
}

func newRecordingSharedStore(t *testing.T, mr *miniredis.Miniredis) *recordingSharedStore {
	t.Helper()
	return &recordingSharedStore{
		RedisSessionStore: newTestRedisSessionStore(t, mr, newDeliveryMetrics()),
		writes:            make(map[string][]string),
	}
}

func (s *recordingSharedStore) record(op, key string) {
	s.mu.Lock()
	s.writes[key] = append(s.writes[key], op)
	s.mu.Unlock()
}

// Save implements SessionStore.
func (s *recordingSharedStore) Save(key string, state SessionState) {
	s.record("save", key)
	s.RedisSessionStore.Save(key, state)
}

// Delete implements SessionStore.
func (s *recordingSharedStore) Delete(key string) {
	s.record("delete", key)
	s.RedisSessionStore.Delete(key)
}

// resetWrites forgets the writes recorded so far.
func (s *recordingSharedStore) resetWrites() {
	s.mu.Lock()
	s.writes = make(map[string][]string)
	s.mu.Unlock()
}

// writesFor returns the writes the broker issued for key.
func (s *recordingSharedStore) writesFor(key string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.writes[key]...)
}
