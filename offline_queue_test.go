package gmqx

// Persistent-session offline queue tests: a session that has no live connection
// keeps the messages its subscriptions match, in order, and receives them right
// after the CONNACK of its next CONNECT. The queue is bounded twice — per session
// and globally — and only persistent sessions queue at all.

import (
	"bufio"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// offlineSession subscribes a persistent session to filter at qos, disconnects it
// and waits until the broker stored it, so a publication finds it offline.
func offlineSession(t *testing.T, b *Broker, clientID, filter string, qos byte) {
	t.Helper()
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
		_, ok := b.store.Load(clientID)
		return ok
	})
}

// offlineDepth is the number of messages the broker holds for offline sessions.
// The counter belongs to the broker, so a test reads it under the broker's lock.
func offlineDepth(b *Broker) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.offlineQueued
}

// publishQoS1 sends one QoS 1 PUBLISH and waits for its PUBACK, so the test knows
// the broker finished routing it.
func publishQoS1(t *testing.T, b *Broker, topic, payload string, pid uint16) {
	t.Helper()
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "queue-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: topic, payload: payload, qos: 1, pid: pid})
	pub.disconnect()
	waitDisconnected(t, b, "queue-pub")
}

// TestOfflineQueueDrainsAfterConnack is the core of the feature: a persistent
// subscriber disconnects, QoS 1 messages are published, and the reconnect with the
// same client id reports Session Present and receives them in publication order.
func TestOfflineQueueDrainsAfterConnack(t *testing.T) {
	b := startBroker(t, Config{})
	offlineSession(t, b, "queue-sub", "queue/#", 1)

	stored, ok := b.store.Load("queue-sub")
	if !ok || len(stored.Queue) != 0 || len(stored.Subscriptions) != 1 {
		t.Fatalf("stored session before publishing = %+v (ok=%v)", stored, ok)
	}

	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "queue-pub", clean: true})
	for i, payload := range []string{"one", "two", "three"} {
		pub.mustPublish(t, confPublish{topic: "queue/x", payload: payload, qos: 1, pid: uint16(i + 1)})
	}

	stored, ok = b.store.Load("queue-sub")
	if !ok || len(stored.Queue) != 3 {
		t.Fatalf("stored queue = %d messages (ok=%v), want 3", len(stored.Queue), ok)
	}
	if stored.Queue[0].Topic != "queue/x" || string(stored.Queue[0].Payload) != "one" || stored.Queue[0].QoS != 1 {
		t.Fatalf("first queued message = %+v", stored.Queue[0])
	}

	sub, ack := mustConnect(t, b.Addr(), confConnect{clientID: "queue-sub", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the reconnecting persistent session did not report Session Present")
	}
	seen := make(map[uint16]struct{}, 3)
	for _, want := range []string{"one", "two", "three"} {
		got := expectPublish(t, sub)
		if got.Topic != "queue/x" || string(got.Payload) != want {
			t.Fatalf("queued delivery = %q %q, want queue/x %q", got.Topic, got.Payload, want)
		}
		if got.QoS != 1 {
			t.Fatalf("queued delivery %q arrived at QoS %d, want 1", want, got.QoS)
		}
		if got.PacketID == 0 {
			t.Fatalf("queued delivery %q has no packet id", want)
		}
		if got.Dup {
			t.Fatalf("a first delivery of %q carries DUP", want)
		}
		if _, dup := seen[got.PacketID]; dup {
			t.Fatalf("packet id %d was assigned twice during the drain", got.PacketID)
		}
		seen[got.PacketID] = struct{}{}
	}
	expectNoMessage(t, sub, 100*time.Millisecond)

	// The queue moved to the connection: the stored session no longer holds it.
	if stored, ok := b.store.Load("queue-sub"); ok {
		t.Fatalf("the drained session is still stored: %+v", stored)
	}
	if depth := offlineDepth(b); depth != 0 {
		t.Fatalf("global offline counter after the drain = %d, want 0", depth)
	}
}

// TestOfflineQueueOrderAcrossReconnects pins that the queue is FIFO per session
// and that a drained message is gone: the second offline period starts an empty
// queue, so the reconnect sees exactly the messages of that period, in order.
func TestOfflineQueueOrderAcrossReconnects(t *testing.T) {
	b := startBroker(t, Config{})
	offlineSession(t, b, "order-sub", "order/#", 1)
	publishQoS1(t, b, "order/x", "first", 1)
	publishQoS1(t, b, "order/x", "second", 2)

	sub, ack := mustConnect(t, b.Addr(), confConnect{clientID: "order-sub", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the session was not resumed")
	}
	for _, want := range []string{"first", "second"} {
		if got := expectPublish(t, sub); string(got.Payload) != want {
			t.Fatalf("drained delivery = %q, want %q", got.Payload, want)
		}
	}
	expectNoMessage(t, sub, 50*time.Millisecond)
	sub.disconnect()
	waitDisconnected(t, b, "order-sub")

	publishQoS1(t, b, "order/x", "third", 3)
	publishQoS1(t, b, "order/x", "fourth", 4)
	sub2, ack := mustConnect(t, b.Addr(), confConnect{clientID: "order-sub", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the session was not resumed a second time")
	}
	for _, want := range []string{"third", "fourth"} {
		if got := expectPublish(t, sub2); string(got.Payload) != want {
			t.Fatalf("second drain delivery = %q, want %q", got.Payload, want)
		}
	}
	expectNoMessage(t, sub2, 50*time.Millisecond)
}

// TestResumedSessionWithoutSubscriptionsCanSubscribe is a regression test for the
// stored shape: a session that had no subscription when it disconnected is stored
// with none, and resuming it must still leave a usable subscription map (a
// SUBSCRIBE on a resumed session is not new, and it must not hit a nil map).
func TestResumedSessionWithoutSubscriptionsCanSubscribe(t *testing.T) {
	b := startBroker(t, Config{})
	first, _ := mustConnect(t, b.Addr(), confConnect{clientID: "empty-session", clean: false})
	if _, ok := b.store.Load("empty-session"); ok {
		t.Fatal("a connected session was stored")
	}
	first.disconnect()
	waitDisconnected(t, b, "empty-session")
	stored, ok := b.store.Load("empty-session")
	if !ok || len(stored.Subscriptions) != 0 {
		t.Fatalf("stored session = %+v (ok=%v), want one without subscriptions", stored, ok)
	}

	second, ack := mustConnect(t, b.Addr(), confConnect{clientID: "empty-session", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the session was not resumed")
	}
	if codes := subscribe(t, second, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "empty/#", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v, want [1]", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "empty-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "empty/x", payload: "after", qos: 1, pid: 1})
	if got := expectPublish(t, second); string(got.Payload) != "after" {
		t.Fatalf("delivery after the resumed SUBSCRIBE = %q", got.Payload)
	}
}

// TestOfflineQueuePerSessionCapDropsOldest pins the per-session bound: when the
// queue is full the oldest message is dropped, and the drop is observable.
func TestOfflineQueuePerSessionCapDropsOldest(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics, MaxOfflineQueueSize: 2})
	offlineSession(t, b, "cap-sub", "cap/#", 1)

	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "cap-pub", clean: true})
	for i, payload := range []string{"one", "two", "three"} {
		pub.mustPublish(t, confPublish{topic: "cap/x", payload: payload, qos: 1, pid: uint16(i + 1)})
	}
	stored, ok := b.store.Load("cap-sub")
	if !ok || len(stored.Queue) != 2 {
		t.Fatalf("stored queue = %d messages (ok=%v), want the 2 newest", len(stored.Queue), ok)
	}
	if got := string(stored.Queue[0].Payload); got != "two" {
		t.Fatalf("oldest queued message = %q, want \"two\" (the oldest one is dropped)", got)
	}
	if n := metrics.count("offline_queue_full"); n != 1 {
		t.Fatalf("offline_queue_full drops = %d, want 1", n)
	}

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "cap-sub", clean: false})
	for _, want := range []string{"two", "three"} {
		if got := expectPublish(t, sub); string(got.Payload) != want {
			t.Fatalf("drained delivery = %q, want %q", got.Payload, want)
		}
	}
	expectNoMessage(t, sub, 100*time.Millisecond)
}

// TestOfflineQueueGlobalCapRejectsNewest pins the global bound: at the total cap a
// new message is rejected instead of evicting a queued one, and the rejection is
// observable.
func TestOfflineQueueGlobalCapRejectsNewest(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics, MaxTotalOfflineMessages: 2})
	offlineSession(t, b, "total-a", "total/#", 1)
	offlineSession(t, b, "total-b", "other/#", 1)

	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "total-pub", clean: true})
	for i, payload := range []string{"one", "two", "three"} {
		pub.mustPublish(t, confPublish{topic: "total/x", payload: payload, qos: 1, pid: uint16(i + 1)})
	}
	if n := metrics.count("offline_queue_total"); n != 1 {
		t.Fatalf("offline_queue_total rejections = %d, want 1", n)
	}
	if depth := offlineDepth(b); depth != 2 {
		t.Fatalf("global offline counter = %d, want 2", depth)
	}
	// The cap rejects the newest message, so the two accepted ones are the oldest.
	stored, ok := b.store.Load("total-a")
	if !ok || len(stored.Queue) != 2 {
		t.Fatalf("stored queue = %d messages (ok=%v), want 2", len(stored.Queue), ok)
	}
	if got := string(stored.Queue[1].Payload); got != "two" {
		t.Fatalf("newest queued message = %q, want \"two\"", got)
	}

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "total-a", clean: false})
	for _, want := range []string{"one", "two"} {
		if got := expectPublish(t, sub); string(got.Payload) != want {
			t.Fatalf("drained delivery = %q, want %q", got.Payload, want)
		}
	}
	expectNoMessage(t, sub, 100*time.Millisecond)
}

// TestOfflineQueueSkipsCleanSessions pins that a clean session neither queues nor
// resumes: it is deleted with its connection, so a publication has no target (the
// reference broker queues for clean sessions too, which delivers messages to a
// session that asked for none).
func TestOfflineQueueSkipsCleanSessions(t *testing.T) {
	b := startBroker(t, Config{})
	clean, _ := mustConnect(t, b.Addr(), confConnect{clientID: "clean-queue", clean: true})
	subscribe(t, clean, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "cleanq/#", QoS: 1})
	clean.disconnect()
	waitDisconnected(t, b, "clean-queue")

	if _, ok := b.store.Load("clean-queue"); ok {
		t.Fatal("a clean session was stored")
	}
	publishQoS1(t, b, "cleanq/x", "unwanted", 1)
	if depth := offlineDepth(b); depth != 0 {
		t.Fatalf("a clean session queued %d messages", depth)
	}

	again, ack := mustConnect(t, b.Addr(), confConnect{clientID: "clean-queue", clean: true})
	if ack.SessionPresent {
		t.Fatal("a clean session resumed a stored session")
	}
	expectNoMessage(t, again, 100*time.Millisecond)

	// A clean CONNECT also discards the queue a persistent session had collected.
	offlineSession(t, b, "clean-drop", "cleandrop/#", 1)
	publishQoS1(t, b, "cleandrop/x", "queued", 2)
	if depth := offlineDepth(b); depth != 1 {
		t.Fatalf("global offline counter = %d, want 1", depth)
	}
	dropped, ack := mustConnect(t, b.Addr(), confConnect{clientID: "clean-drop", clean: true})
	if ack.SessionPresent {
		t.Fatal("a clean CONNECT resumed the stored session")
	}
	expectNoMessage(t, dropped, 100*time.Millisecond)
	if _, ok := b.store.Load("clean-drop"); ok {
		t.Fatal("the clean CONNECT did not delete the stored session")
	}
	if depth := offlineDepth(b); depth != 0 {
		t.Fatalf("global offline counter after the clean connect = %d, want 0", depth)
	}
}

// TestOfflineQueueDrainDegradesToQoS0 pins the drain degradation: the queue is a
// stored obligation, so a message that cannot get a slot in the inflight window is
// delivered at QoS 0 in order rather than stalling the session (the reference
// broker applies the same degradation when its packet ids run out).
func TestOfflineQueueDrainDegradesToQoS0(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics, MaxInflightQoS1: 1})
	offlineSession(t, b, "degrade-sub", "degrade/#", 1)

	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "degrade-pub", clean: true})
	for i, payload := range []string{"one", "two", "three"} {
		pub.mustPublish(t, confPublish{topic: "degrade/x", payload: payload, qos: 1, pid: uint16(i + 1)})
	}

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "degrade-sub", clean: false})
	first := expectPublish(t, sub)
	if string(first.Payload) != "one" || first.QoS != 1 || first.PacketID == 0 {
		t.Fatalf("first drained delivery = %q qos%d pid%d, want the QoS 1 one", first.Payload, first.QoS, first.PacketID)
	}
	for _, want := range []string{"two", "three"} {
		got := expectPublish(t, sub)
		if string(got.Payload) != want {
			t.Fatalf("drained delivery = %q, want %q", got.Payload, want)
		}
		if got.QoS != 0 || got.PacketID != 0 {
			t.Fatalf("degraded delivery %q = qos%d pid%d, want QoS 0 without a packet id", want, got.QoS, got.PacketID)
		}
	}
	expectNoMessage(t, sub, 100*time.Millisecond)
}

// TestWritePublishDegradesWhenPacketIDsExhausted covers the other exhaustion case
// at the unit that decides it: when every packet id of the connection is in use,
// a QoS 1 delivery goes out at QoS 0 instead of being dropped or sent with a
// packet id a retransmission would collide with.
func TestWritePublishDegradesWhenPacketIDsExhausted(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := &Broker{cfg: Config{QoSRetryInterval: time.Second, QoSMaxRetries: 5}, metrics: metrics}
	server, peer := net.Pipe()
	defer func() { _ = server.Close(); _ = peer.Close() }()

	c := newClient(b, server)
	c.version = mqtt.Version311
	for id := 1; id <= 65535; id++ {
		c.inflight[uint16(id)] = &inflightMessage{packetID: uint16(id), qos: 1, state: awaitPuback}
	}
	go func() { _ = c.writePublish("ids/x", []byte("payload"), 1, false, mqtt.Properties{}) }()

	_ = peer.SetReadDeadline(time.Now().Add(3 * time.Second))
	decoder := mqtt.Decoder{Version: mqtt.Version311}
	msg, err := decoder.Decode(bufio.NewReader(peer))
	if err != nil {
		t.Fatalf("decode degraded PUBLISH: %v", err)
	}
	pub, ok := msg.(*mqtt.Publish)
	if !ok {
		t.Fatalf("read %T, want a PUBLISH", msg)
	}
	if pub.QoS != 0 || pub.PacketID != 0 || string(pub.Payload) != "payload" {
		t.Fatalf("degraded delivery = qos%d pid%d payload %q, want QoS 0 without a packet id", pub.QoS, pub.PacketID, pub.Payload)
	}
	if n := metrics.overflows("packet_ids"); n != 1 {
		t.Fatalf("packet_ids overflows = %d, want 1", n)
	}
}

// recordingSessionStore counts the calls the broker makes through the
// SessionStore seam, so a test can prove the seam is used rather than bypassed.
type recordingSessionStore struct {
	*MemorySessionStore
	mu      sync.Mutex
	loads   int
	saves   int
	deletes int
}

func (s *recordingSessionStore) Load(key string) (SessionState, bool) {
	s.mu.Lock()
	s.loads++
	s.mu.Unlock()
	return s.MemorySessionStore.Load(key)
}

func (s *recordingSessionStore) Save(key string, state SessionState) {
	s.mu.Lock()
	s.saves++
	s.mu.Unlock()
	s.MemorySessionStore.Save(key, state)
}

func (s *recordingSessionStore) Delete(key string) {
	s.mu.Lock()
	s.deletes++
	s.mu.Unlock()
	s.MemorySessionStore.Delete(key)
}

func (s *recordingSessionStore) counts() (loads, saves, deletes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loads, s.saves, s.deletes
}

// TestSessionStoreSeamIsUsed pins that Config.SessionStore is the store the
// broker reads and writes: a preloaded session resumes and delivers, a disconnect
// saves through the seam, and a clean CONNECT deletes through it.
func TestSessionStoreSeamIsUsed(t *testing.T) {
	store := &recordingSessionStore{MemorySessionStore: NewMemorySessionStore()}
	b := startBroker(t, Config{SessionStore: store})

	store.Save("seam", SessionState{
		ClientID:      "seam",
		Subscriptions: map[string]SessionSubscription{"seam/#": {QoS: 1}},
	})
	c, ack := mustConnect(t, b.Addr(), confConnect{clientID: "seam", clean: false})
	if !ack.SessionPresent {
		t.Fatal("a session preloaded in the configured store was not resumed")
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "seam-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "seam/x", payload: "stored", qos: 1, pid: 1})
	if got := expectPublish(t, c); string(got.Payload) != "stored" {
		t.Fatalf("delivery through the stored subscription = %q", got.Payload)
	}

	c.disconnect()
	waitDisconnected(t, b, "seam")
	waitFor(t, "the session to be saved through the seam", func() bool {
		_, ok := store.Load("seam")
		return ok
	})
	if _, saves, _ := store.counts(); saves == 0 {
		t.Fatal("the broker did not save through Config.SessionStore")
	}

	clean, ack := mustConnect(t, b.Addr(), confConnect{clientID: "seam", clean: true})
	if ack.SessionPresent {
		t.Fatal("a clean CONNECT resumed the stored session")
	}
	expectNoMessage(t, clean, 50*time.Millisecond)
	waitFor(t, "the session to be deleted through the seam", func() bool {
		_, ok := store.Load("seam")
		return !ok
	})
	if _, _, deletes := store.counts(); deletes == 0 {
		t.Fatal("the broker did not delete through Config.SessionStore")
	}
}

// deliveryMetrics records everything the delivery limits report: the drop reasons
// and the overflow names, which are the two observability seams the offline queue
// and the inflight windows use.
type deliveryMetrics struct {
	NoopMetrics
	mu       sync.Mutex
	drops    map[string]int
	overflow map[string]int
	depths   map[string]int
}

func newDeliveryMetrics() *deliveryMetrics {
	return &deliveryMetrics{
		drops:    make(map[string]int),
		overflow: make(map[string]int),
		depths:   make(map[string]int),
	}
}

func (m *deliveryMetrics) RecordDroppedMessage(reason string) {
	m.mu.Lock()
	m.drops[reason]++
	m.mu.Unlock()
}

func (m *deliveryMetrics) RecordOverflow(name string) {
	m.mu.Lock()
	m.overflow[name]++
	m.mu.Unlock()
}

func (m *deliveryMetrics) RecordQueueDepth(name string, depth int) {
	m.mu.Lock()
	m.depths[name] = depth
	m.mu.Unlock()
}

func (m *deliveryMetrics) count(reason string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.drops[reason]
}

func (m *deliveryMetrics) overflows(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.overflow[name]
}

func (m *deliveryMetrics) depth(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.depths[name]
}
