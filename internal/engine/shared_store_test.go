package engine

// Shared persistence: the broker's two states written through the gcache seam, so a
// client that reconnects to another node resumes its session instead of starting
// from nothing. These cases pin the broker behavior a shared store enables — the
// migration path, the atomic take, what a kick does to a session a peer owns, the
// namespace isolation and the failure modes — while the store contract itself
// (encoding, TTL, corruption) is pinned by gmqx/internal/persist's own tests.

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

	"github.com/maxid/gcache"
	"github.com/maxid/gmqx/session"
	"github.com/maxid/gnety/codec/mqtt"
)

// sharedStore is a gcache.RemoteStore with TTL and enumeration, shared by the
// brokers of one test. It also records the writes of one key, so a case can
// attribute a save or a delete to the node that issued it.
type sharedStore struct {
	mu     sync.Mutex
	items  map[string]storeItem
	now    func() time.Time
	writes map[string][]string
}

type storeItem struct {
	value     []byte
	expiresAt time.Time
}

func newSharedStore() *sharedStore {
	return &sharedStore{items: map[string]storeItem{}, now: time.Now, writes: map[string][]string{}}
}

func (s *sharedStore) Get(_ context.Context, key string) (gcache.Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.items[key]
	if !ok {
		return gcache.Entry{}, false, nil
	}
	if !it.expiresAt.IsZero() && !s.now().Before(it.expiresAt) {
		delete(s.items, key)
		return gcache.Entry{}, false, nil
	}
	return gcache.Entry{Value: it.value, ExpiresAt: it.expiresAt}, true, nil
}

func (s *sharedStore) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	it := storeItem{value: append([]byte(nil), value...)}
	if ttl > 0 {
		it.expiresAt = s.now().Add(ttl)
	}
	s.items[key] = it
	s.writes[key] = append(s.writes[key], "save")
	return nil
}

func (s *sharedStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
	s.writes[key] = append(s.writes[key], "delete")
	return nil
}

func (s *sharedStore) Close() error { return nil }

// Scan implements gcache.Scanner.
func (s *sharedStore) Scan(_ context.Context, prefix string, fn func(key string) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.items {
		if strings.HasPrefix(key, prefix) {
			if err := fn(key); err != nil {
				return err
			}
		}
	}
	return nil
}

// exists reports whether key is stored.
func (s *sharedStore) exists(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.items[key]
	if !ok || (!it.expiresAt.IsZero() && !s.now().Before(it.expiresAt)) {
		return false
	}
	return true
}

// putRaw writes a value the broker did not produce, for the corruption cases.
func (s *sharedStore) putRaw(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[key] = storeItem{value: value}
}

// resetWrites forgets the writes recorded so far.
func (s *sharedStore) resetWrites() {
	s.mu.Lock()
	s.writes = map[string][]string{}
	s.mu.Unlock()
}

// writesFor returns the writes the broker issued for key.
func (s *sharedStore) writesFor(key string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.writes[key]...)
}

// sharedLocker is a gcache.Locker for one process, enough for the atomic-take
// cases: one holder per key.
type sharedLocker struct {
	mu   sync.Mutex
	held map[string]string
}

func newSharedLocker() *sharedLocker { return &sharedLocker{held: map[string]string{}} }

func (l *sharedLocker) TryAcquire(_ context.Context, key string, _ time.Duration) (gcache.Lock, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.held[key]; ok {
		return nil, false, nil
	}
	token := fmt.Sprintf("%s#%d", key, time.Now().UnixNano())
	l.held[key] = token
	return &sharedLock{locker: l, key: key, token: token}, true, nil
}

func (l *sharedLocker) Acquire(ctx context.Context, key string, ttl time.Duration, _ gcache.WaitPolicy) (gcache.Lock, error) {
	lock, ok, err := l.TryAcquire(ctx, key, ttl)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("sharedLocker: %q is held", key)
	}
	return lock, nil
}

type sharedLock struct {
	locker *sharedLocker
	key    string
	token  string
}

func (l *sharedLock) Token() string        { return l.token }
func (l *sharedLock) FencingToken() uint64 { return 0 }
func (l *sharedLock) ExpiresAt() time.Time { return time.Time{} }
func (l *sharedLock) Renew(context.Context, time.Duration) error {
	return nil
}
func (l *sharedLock) Unlock(context.Context) error {
	l.locker.mu.Lock()
	defer l.locker.mu.Unlock()
	if l.locker.held[l.key] == l.token {
		delete(l.locker.held, l.key)
	}
	return nil
}

// persistKey is the key the seam writes for one broker session key.
func persistKey(key string) string { return "gmqx:session:" + key }

// sessionNode starts a broker of nodeID that keeps its state in store.
func sessionNode(t *testing.T, nodeID string, store *sharedStore) *Broker {
	t.Helper()
	return sessionNodeWithLocker(t, nodeID, store, newSharedLocker())
}

// sessionNodeWithLocker is sessionNode with the locker supplied: a take can only be
// atomic when the nodes contend on the same lock, so the case that proves it has to
// hand several nodes one locker instead of giving each its own.
func sessionNodeWithLocker(t *testing.T, nodeID string, store *sharedStore, locker *sharedLocker) *Broker {
	t.Helper()
	return startBroker(t, Config{
		NodeID: nodeID, Store: store, Locker: locker, Logger: discardLogger(),
	})
}

// offlineSessionOn subscribes a persistent session of b to filter at qos,
// disconnects it and waits until b stored it.
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
		_, ok := b.persist.LoadSession(key)
		return ok
	})
}

// TestSessionMigrationAcrossNodes is the feature: a persistent client subscribes on
// node a and goes away, messages are published while it is away, and it reconnects
// on node b, which takes the session out of the shared store — Session Present set,
// the subscriptions restored and the queued messages in publication order,
// properties included.
func TestSessionMigrationAcrossNodes(t *testing.T) {
	store := newSharedStore()
	a, b := sessionNode(t, "a", store), sessionNode(t, "b", store)

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
	if !store.exists(persistKey("roamer")) {
		t.Fatal("node a did not store the session")
	}

	pub, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "roam-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "roam/one", payload: "one", qos: 1, pid: 1})
	pub.mustPublish(t, confPublish{
		topic: "roam/two", payload: "two", qos: 1, pid: 2,
		props: mqtt.Properties{UserProperties: []mqtt.UserProperty{{Key: "seq", Value: "2"}}},
	})

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

	livePub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "roam-pub-b", clean: true})
	livePub.mustPublish(t, confPublish{topic: "roam/three", payload: "live", qos: 1, pid: 1})
	live := expectPublish(t, back)
	if live.Topic != "roam/three" || string(live.Payload) != "live" {
		t.Fatalf("delivery over a restored subscription = %q %q", live.Topic, live.Payload)
	}
	back.ack(live.PacketID)

	if store.exists(persistKey("roamer")) {
		t.Fatal("the resumed session is still stored while a connection owns it")
	}
}

// TestSharedSessionCleanConnectDiscards pins that a clean CONNECT discards the
// shared session, exactly as MQTT requires: whichever node receives the CONNECT
// deletes the stored state the other node wrote.
func TestSharedSessionCleanConnectDiscards(t *testing.T) {
	store := newSharedStore()
	a, b := sessionNode(t, "a", store), sessionNode(t, "b", store)

	offlineSessionOn(t, a, "discarded", "state/#", 1)
	if !store.exists(persistKey("discarded")) {
		t.Fatal("node a did not store the session")
	}

	clean, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "discarded", clean: true})
	if ack.SessionPresent {
		t.Fatal("a clean CONNECT reported a session it just discarded")
	}
	if store.exists(persistKey("discarded")) {
		t.Fatal("the clean CONNECT left the stored session behind")
	}
	clean.disconnect()
	waitDisconnected(t, b, "discarded")

	_, ack = mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "discarded", clean: false})
	if ack.SessionPresent {
		t.Fatal("the discarded session was resumed")
	}
}

// TestSharedSessionTakeIsAtomic pins that one stored session is handed out once:
// concurrent persistent CONNECTs of the same client id on two nodes produce exactly
// one session owner, and the take removed the stored copy.
func TestSharedSessionTakeIsAtomic(t *testing.T) {
	store := newSharedStore()
	locker := newSharedLocker()
	a, b := sessionNodeWithLocker(t, "a", store, locker), sessionNodeWithLocker(t, "b", store, locker)
	if !a.persist.SaveSession("racing", &session.State{
		ClientID: "racing",
		Subs:     map[string]session.Subscription{"racing/#": {QoS: 1}},
	}) {
		t.Fatal("could not seed the session")
	}

	type attempt struct {
		present bool
		conn    net.Conn
		err     error
	}
	attempts := make(chan attempt, 2)
	var wg sync.WaitGroup
	for _, node := range []*Broker{a, b} {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			present, conn, err := connectSessionPresent(node.Addr(), "racing")
			attempts <- attempt{present: present, conn: conn, err: err}
		}()
	}
	wg.Wait()
	close(attempts)
	resumed := 0
	var conns []net.Conn
	for got := range attempts {
		if got.err != nil {
			t.Fatalf("concurrent persistent CONNECT: %v", got.err)
		}
		conns = append(conns, got.conn)
		if got.present {
			resumed++
		}
	}
	if resumed != 1 {
		t.Fatalf("%d nodes resumed the same session, want exactly 1", resumed)
	}
	// Both connections are still open, so no disconnect has stored a session again
	// yet: what the store holds is what the take left.
	if store.exists(persistKey("racing")) {
		t.Fatal("the taken session is still stored")
	}
	for _, conn := range conns {
		conn.Close()
	}
}

// TestSharedSessionKeptWhenPeerTookOver pins the takeover rule of a store the nodes
// share: a connection displaced by a peer's kick must not write its own copy of the
// session back and must not delete the state the peer saved.
func TestSharedSessionKeptWhenPeerTookOver(t *testing.T) {
	for _, tc := range []struct {
		name    string
		clean   bool
		persist *session.State
	}{
		{
			// A clean connection would delete the key on its way out.
			name:    "clean connection",
			clean:   true,
			persist: &session.State{ClientID: "handover", Subs: map[string]session.Subscription{"peer/#": {QoS: 1}}},
		},
		{
			// A persistent connection would save its own copy over the peer's state.
			name:  "persistent connection",
			clean: false,
			persist: &session.State{
				ClientID: "handover",
				Subs:     map[string]session.Subscription{"peer/#": {QoS: 1}},
				Queue:    []session.Pending{{Topic: "peer/x", Payload: []byte("peer"), QoS: 1}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newSharedStore()
			backend := &recordingCluster{}
			b := startBroker(t, Config{
				NodeID: "a", Store: store, Locker: newSharedLocker(), ClusterBackend: backend,
				Metrics: newDeliveryMetrics(), NotKickSeconds: -1, Logger: discardLogger(),
			})

			conn, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "handover", clean: tc.clean})
			if !tc.clean {
				subscribe(t, conn, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "handover/#", QoS: 1})
			}
			// The peer that kicks this connection took the session over and stored its
			// own state, which is what a shared store holds after a takeover.
			b.persist.SaveSession("handover", tc.persist)
			store.resetWrites()

			backend.deliver(t, kickFor("handover", "b", time.Now().Add(time.Second)))
			expectDisconnect(t, conn, ReasonSessionTakenOver)
			waitDisconnected(t, b, "handover")

			if writes := store.writesFor(persistKey("handover")); len(writes) != 0 {
				t.Fatalf("the kicked node wrote the session of a peer: %v", writes)
			}
			got, ok := b.persist.LoadSession("handover")
			if !ok {
				t.Fatal("the kicked node deleted the session the peer owns")
			}
			if !reflect.DeepEqual(got, tc.persist) {
				t.Fatalf("the stored session = %+v, want the peer's %+v", got, tc.persist)
			}
			if indexed := b.index.Has("handover"); indexed {
				t.Fatal("the session that moved away is still a local routing target")
			}
		})
	}
}

// TestSharedSessionStoreNamespaceIsolation pins namespace isolation on one shared
// store: the same client id in two namespaces is two sessions under two keys, and
// neither broker resumes, queues for or expires the other's.
func TestSharedSessionStoreNamespaceIsolation(t *testing.T) {
	store := newSharedStore()
	a := startBroker(t, Config{Namespace: nsValue("tenant-a"), Store: store, Locker: newSharedLocker(), Logger: discardLogger()})
	b := startBroker(t, Config{Namespace: nsValue("tenant-b"), Store: store, Locker: newSharedLocker(), Logger: discardLogger()})

	keyA := persistKey(namespaceKey("tenant-a", "shared-id"))
	keyB := persistKey(namespaceKey("tenant-b", "shared-id"))
	if keyA == keyB {
		t.Fatalf("two namespaces share one key: %q", keyA)
	}
	offlineSessionOn(t, a, "shared-id", "a/#", 1)
	offlineSessionOn(t, b, "shared-id", "b/#", 1)
	if !store.exists(keyA) || !store.exists(keyB) {
		t.Fatalf("the two namespaces do not hold two keys (%v %v)", store.exists(keyA), store.exists(keyB))
	}

	pub, _ := mustConnect(t, a.Addr(), confConnect{clientID: "ns-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "a/x", payload: "tenant-a", qos: 1, pid: 1})

	storedA, ok := a.persist.LoadSession(namespaceKey("tenant-a", "shared-id"))
	if !ok || len(storedA.Queue) != 1 {
		t.Fatalf("tenant-a queue = %+v (ok=%v), want one message", storedA.Queue, ok)
	}
	storedB, ok := b.persist.LoadSession(namespaceKey("tenant-b", "shared-id"))
	if !ok || len(storedB.Queue) != 0 {
		t.Fatalf("tenant-b queue = %+v (ok=%v), want it untouched", storedB.Queue, ok)
	}

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
	if !ok || store.exists(keyA) || store.exists(keyB) {
		t.Fatalf("the resumed sessions are still stored (%v %v)", store.exists(keyA), store.exists(keyB))
	}
}

// TestStoredSessionExpiryIsSwept pins the deadline for a shared store: the record is
// written with its TTL, and the broker's sweeper removes the expired session from
// the store and the routing table.
func TestStoredSessionExpiryIsSwept(t *testing.T) {
	store := newSharedStore()
	b := startBroker(t, Config{
		Store: store, Locker: newSharedLocker(),
		Logger: discardLogger(), unit: testSecond,
		SessionSweepInterval: 10 * time.Millisecond,
	})

	c, _ := mustConnect(t, b.Addr(), confConnect{
		version: mqtt.Version5, clientID: "expiring-shared", clean: false,
		props: mqtt.Properties{SessionExpiryInterval: 5},
	})
	subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "expiring/#", QoS: 0})
	c.disconnect()
	waitDisconnected(t, b, "expiring-shared")

	if !store.exists(persistKey("expiring-shared")) {
		t.Fatal("the session was not stored")
	}
	waitFor(t, "the session to be swept after its expiry", func() bool {
		return !store.exists(persistKey("expiring-shared")) && !b.index.Has("expiring-shared")
	})
}

// TestCorruptStoredSessionIsDropped pins the failure mode of a value that does not
// decode: it is dropped and counted, and the broker keeps serving its other
// sessions instead of failing the CONNECT.
func TestCorruptStoredSessionIsDropped(t *testing.T) {
	store := newSharedStore()
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Store: store, Locker: newSharedLocker(), Metrics: metrics, Logger: discardLogger()})
	offlineSessionOn(t, b, "healthy", "ok/#", 1)

	store.putRaw(persistKey("broken"), []byte("not-a-session"))

	if _, ok := b.persist.LoadSession("broken"); ok {
		t.Fatal("an undecodable value was loaded")
	}
	if metrics.count("session_store_corrupt") == 0 {
		t.Fatal("the undecodable value was not counted")
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

// TestRetainedSurvivesRestart is the other half of the seam: a retained message
// written by one broker is replayed by the next one over the same store.
func TestRetainedSurvivesRestart(t *testing.T) {
	store := newSharedStore()
	first := startBroker(t, Config{Store: store, Logger: discardLogger()})
	pub, _ := mustConnect(t, first.Addr(), confConnect{clientID: "retained-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "state/light", payload: "on", qos: 1, pid: 1, retain: true})
	waitFor(t, "the retained message to be stored", func() bool {
		return store.exists("gmqx:retained:state/light")
	})
	if err := first.Stop(context.Background()); err != nil {
		t.Fatalf("stop first broker: %v", err)
	}

	second := startBroker(t, Config{Store: store, Logger: discardLogger()})
	sub, _ := mustConnect(t, second.Addr(), confConnect{clientID: "retained-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "state/#", QoS: 1})
	got := expectPublish(t, sub)
	if string(got.Payload) != "on" || !got.Retain {
		t.Fatalf("replayed retained message = %q retain=%v, want on/true", got.Payload, got.Retain)
	}
}

// connectSessionPresent sends a persistent MQTT 3.1.1 CONNECT, reports the Session
// Present flag of the CONNACK and returns the still open connection. The caller
// decides when the session ends, which is what lets a case observe the store before
// the disconnect writes the session back. It touches no *testing.T, so a case can
// run it from a goroutine.
func connectSessionPresent(addr, clientID string) (bool, net.Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return false, nil, err
	}

	var body []byte
	body = appendString(body, "MQTT")
	body = append(body, 4, 0, 0, 30) // level 4, clean session cleared, keep-alive 30
	body = appendString(body, clientID)
	frame := appendRemainingLength([]byte{0x10}, len(body))
	frame = append(frame, body...)
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		conn.Close()
		return false, nil, err
	}
	r := bufio.NewReader(conn)
	header, err := r.ReadByte()
	if err != nil {
		conn.Close()
		return false, nil, err
	}
	n, err := readRemainingLength(r)
	if err != nil {
		conn.Close()
		return false, nil, err
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		conn.Close()
		return false, nil, err
	}
	if header>>4 != 2 || len(payload) < 2 {
		conn.Close()
		return false, nil, fmt.Errorf("unexpected CONNACK: %#x %v", header, payload)
	}
	return payload[0]&0x01 != 0, conn, nil
}
