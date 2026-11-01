package gmqx

// A full inflight queue is not a loss point for a persistent session. The delivery
// that finds the window and the queue behind it full joins the queue and the
// session is taken offline — the connection is closed the way a lost one is, so the
// normal close → unregister → SessionStore path stores the session with everything
// it still owes — and the next CONNECT of that client id drains it. The only loss
// points left are the offline queue's own capacity limits
// (MaxOfflineQueueSize/MaxTotalOfflineMessages), which are a documented capacity
// limit and are counted with the reasons they already had.
//
// A clean session has nowhere to queue, because its state ends with the connection:
// there the delivery is still dropped, on one explicit path with its own reason
// ("inflight_overflow_clean").
//
// The cases below drive a subscriber that stops reading — the only way its inflight
// window stays full — and check the metrics, the log, the stored session, the
// resumed delivery order of every acknowledged message, the Redis-backed store
// across two nodes, the offline queue's cap, and that the hand-off leaves neither a
// goroutine nor a write after the close behind.

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// overflowBroker starts a broker whose inflight queue overflows after one window
// slot and one queued delivery, so three acknowledged deliveries are enough to
// drive a session offline deterministically.
func overflowBroker(t *testing.T, cfg Config) *Broker {
	t.Helper()
	cfg.MaxInflightQoS1 = 1
	cfg.MaxInflightQueueSize = 1
	cfg.QoSRetryInterval = time.Minute
	// An immediate reconnect after the broker closed the connection is legal in
	// these cases: the anti-flap window would otherwise refuse the CONNECT.
	cfg.NotKickSeconds = -1
	if cfg.Logger == nil {
		cfg.Logger = discardLogger()
	}
	return startBroker(t, cfg)
}

// stalledSession connects a persistent session of b, subscribes it to filter at
// QoS 1 and never reads from it again: every delivery the broker writes stays
// unconfirmed, so the inflight window stays full. The connection is returned so the
// case can close it, and it is closed by the cleanup as well.
func stalledSession(t *testing.T, b *Broker, clientID, filter string) *confClient {
	t.Helper()
	return stalledSessionAt(t, b, clientID, filter, 1)
}

// stalledSessionAt is stalledSession with an explicit subscription QoS.
func stalledSessionAt(t *testing.T, b *Broker, clientID, filter string, qos byte) *confClient {
	t.Helper()
	c, ack := mustConnect(t, b.Addr(), confConnect{clientID: clientID, clean: false})
	if ack.SessionPresent {
		t.Fatalf("client %q resumed a session before it had one", clientID)
	}
	if codes := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: filter, QoS: qos}).ReasonCodes; len(codes) != 1 || codes[0] != qos {
		t.Fatalf("SUBACK codes %v, want [%d]", codes, qos)
	}
	return c
}

// publishQoS2 publishes one message at QoS 2 and completes the exchange with the
// broker, so the publisher knows the broker accepted it.
func publishQoS2(t *testing.T, c *confClient, p confPublish) {
	t.Helper()
	c.publish(p)
	expectPubrec(t, c, p.pid, 0)
	c.write(&mqtt.Pubrel{MessageHeader: mqtt.MessageHeader{Level: c.version}, PacketID: p.pid})
	expectPubcomp(t, c, p.pid)
}

// drainResumed reads exactly want deliveries from a resumed session in payload
// order ("m1", "m2", ...) and acknowledges the QoS 1 ones. It returns the payloads
// and the QoS of each delivery.
//
// A retransmission of a delivery from the connection that was lost is allowed — the
// protocol is at-least-once — but it has to carry DUP, and a sequence number may
// never arrive twice without it or arrive out of order.
//
// A queued delivery that finds the resumed inflight window full arrives at QoS 0:
// that is the drain rule of the offline queue (a stored obligation is delivered
// rather than stalling the session, see client.drainOfflineQueue and
// TestOfflineQueueDrainDegradesToQoS0), and it is not what these cases are about —
// they assert that nothing is lost and that the order holds.
func drainResumed(t *testing.T, c *confClient, want int) ([]string, []byte) {
	t.Helper()
	var got []string
	var qos []byte
	seen := make(map[string]bool, want)
	for len(got) < want {
		pub := expectPublish(t, c)
		payload := string(pub.Payload)
		switch pub.QoS {
		case 0:
			if pub.PacketID != 0 {
				t.Fatalf("degraded delivery %q carries packet id %d, want none", payload, pub.PacketID)
			}
		case 1:
			if pub.PacketID == 0 {
				t.Fatalf("QoS 1 delivery %q has no packet id", payload)
			}
			c.ack(pub.PacketID)
		default:
			t.Fatalf("resumed delivery %q = qos%d, want QoS 1 or the documented QoS 0 drain", payload, pub.QoS)
		}
		if seen[payload] {
			if !pub.Dup {
				t.Fatalf("delivery %q arrived twice without DUP", payload)
			}
			continue
		}
		if len(got) > 0 && payload <= got[len(got)-1] {
			t.Fatalf("resumed delivery %q arrived after %q: the queue is not drained in order", payload, got[len(got)-1])
		}
		seen[payload] = true
		got = append(got, payload)
		qos = append(qos, pub.QoS)
	}
	expectNoMessage(t, c, 100*time.Millisecond)
	return got, qos
}

// expectEventuallyClosed reads until the connection ends. A client that stopped
// reading may still have buffered packets, so the first read after the broker hangs
// up can still return one; the connection must end behind it.
func expectEventuallyClosed(t *testing.T, c *confClient) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := c.readFor(deadline); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatal("the overflowing connection stayed open")
			}
			return
		}
	}
}

// wantPayloads builds the payload of message i: "m1" .. "mN".
func wantPayloads(n int) []string {
	out := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, fmt.Sprintf("m%d", i))
	}
	return out
}

// TestInflightOverflowTakesPersistentSessionOffline pins the new policy end to end:
// a persistent subscriber that stops reading is disconnected with
// "inflight_overflow", nothing is dropped, the session is stored with every
// delivery it still owed (the unconfirmed exchange plus the queue and the pending
// ones behind it), and the reconnect receives every acknowledged message, in order
// and without a gap.
func TestInflightOverflowTakesPersistentSessionOffline(t *testing.T) {
	metrics := newDeliveryMetrics()
	logger, logs := captureLogger()
	b := overflowBroker(t, Config{Metrics: metrics, Logger: logger, MaxOfflineQueueSize: 64})

	stalled := stalledSession(t, b, "overflow-persist", "overflow/#")
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-persist-pub", clean: true})

	// Every publication is acknowledged to the publisher, so every one of them is a
	// promise about the sink. m1 fills the window, m2 the queue, m3 overflows.
	const messages = 8
	for i := 1; i <= messages; i++ {
		pub.mustPublish(t, confPublish{topic: "overflow/x", payload: fmt.Sprintf("m%d", i), qos: 1, pid: uint16(i)})
	}

	if n := metrics.overflows("inflight_overflow"); n != 1 {
		t.Fatalf("RecordOverflow(\"inflight_overflow\") = %d, want 1: the session is taken offline once", n)
	}
	if n := metrics.count("inflight_overflow"); n != 0 {
		t.Fatalf("inflight_overflow drops = %d, want 0: a persistent session queues instead of dropping", n)
	}
	if n := metrics.count("inflight_overflow_clean"); n != 0 {
		t.Fatalf("inflight_overflow_clean drops = %d, want 0: the session is persistent", n)
	}
	if n := metrics.count("offline_queue_full") + metrics.count("offline_queue_total"); n != 0 {
		t.Fatalf("offline queue dropped %d messages, want 0 with a queue of 64", n)
	}
	logged := logs.String()
	if !strings.Contains(logged, "inflight_overflow") || !strings.Contains(logged, "overflow-persist") {
		t.Fatalf("the hand-off was not logged with the reason and the client id: %q", logged)
	}

	// The connection is closed, exactly as it is for a lost one, and the session is
	// stored with everything it still owes.
	expectEventuallyClosed(t, stalled)
	waitDisconnected(t, b, "overflow-persist")
	key := namespaceKey(b.namespace, "overflow-persist")
	waitFor(t, "the overflowing session to be stored", func() bool {
		_, ok := b.store.Load(key)
		return ok
	})
	stored, ok := b.store.Load(key)
	if !ok {
		t.Fatal("the session of the overflowing connection was not stored")
	}
	// m1 is still an unconfirmed exchange (it was written to a client that stopped
	// reading), m2..m8 are the queue. The count is exact: nothing was dropped and
	// nothing was duplicated into both halves.
	if len(stored.Inflight) != 1 {
		t.Fatalf("stored inflight exchanges = %d, want 1 (m1)", len(stored.Inflight))
	}
	if got := len(stored.Queue); got != messages-1 {
		t.Fatalf("stored queue = %d messages, want %d", got, messages-1)
	}
	for i, msg := range stored.Queue {
		if want := fmt.Sprintf("m%d", i+2); string(msg.Payload) != want {
			t.Fatalf("stored queue[%d] = %q, want %q", i, msg.Payload, want)
		}
		if msg.QoS != 1 {
			t.Fatalf("stored queue[%d] = qos%d, want 1", i, msg.QoS)
		}
	}
	if depth := offlineDepth(b); depth != messages-1 {
		t.Fatalf("offline queue depth = %d, want %d", depth, messages-1)
	}

	// The client reconnects (immediately — the session is stored, but a reconnect
	// that races the unregister takes the session over directly) and receives every
	// acknowledged message in order, m1 first as a retransmission with DUP.
	resumed, ack := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-persist", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the resumed session did not report Session Present")
	}
	got, qos := drainResumed(t, resumed, messages)
	want := wantPayloads(messages)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resumed delivery %d = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
	// The first delivery is the exchange that was unconfirmed when the connection
	// was lost: re-sent under its original packet id with DUP set (MQTT §4.4).
	if qos[0] != 1 {
		t.Fatalf("the retransmitted exchange arrived at QoS %d, want 1", qos[0])
	}
	if stored, ok := b.store.Load(key); ok {
		t.Fatalf("the drained session is still stored: %+v", stored)
	}
	if depth := offlineDepth(b); depth != 0 {
		t.Fatalf("offline queue depth after the drain = %d, want 0", depth)
	}
}

// TestInflightOverflowImmediateReconnect pins the race the hand-off could lose: the
// client reconnects before the broker has stored the session, so the CONNECT takes
// the session over from the live connection (register) instead of from the store.
// Nothing may be lost in either order, so the client connects straight after the
// publication that overflowed — no wait for the disconnect — and the reconnect is
// retried until it wins the client id.
func TestInflightOverflowImmediateReconnect(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := overflowBroker(t, Config{Metrics: metrics, MaxOfflineQueueSize: 64})

	stalled := stalledSession(t, b, "overflow-race", "race/#")
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-race-pub", clean: true})
	const messages = 6
	for i := 1; i <= messages; i++ {
		pub.mustPublish(t, confPublish{topic: "race/x", payload: fmt.Sprintf("m%d", i), qos: 1, pid: uint16(i)})
		// The third publication takes the session offline; from there the client is
		// free to reconnect, which is what the next line does as early as possible.
	}

	// Do not wait for the disconnect: connect straight away, repeatedly, until one
	// of the attempts wins the client id (the first CONNECT may race the close).
	var resumed *confClient
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, dialed := dialConf(t, b.Addr(), confConnect{clientID: "overflow-race", clean: false})
		if dialed.ReasonCode == 0 {
			resumed = c
			break
		}
		if dialed.ReasonCode != ReasonConnectionRateExceeded {
			t.Fatalf("reconnect CONNACK %#x, want success", dialed.ReasonCode)
		}
		_ = c.conn.Close()
		time.Sleep(5 * time.Millisecond)
	}
	if resumed == nil {
		t.Fatal("the session could not be resumed after the overflow")
	}
	got, _ := drainResumed(t, resumed, messages)
	want := wantPayloads(messages)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resumed delivery %d = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
	if n := metrics.count("inflight_overflow"); n != 0 {
		t.Fatalf("inflight_overflow drops = %d, want 0", n)
	}
	if n := metrics.count("inflight_overflow_clean"); n != 0 {
		t.Fatalf("inflight_overflow_clean drops = %d, want 0", n)
	}
	_ = stalled.conn.Close()
}

// TestInflightOverflowQueuesForRedisStoreAcrossNodes pins the durable half of the
// policy: the queue the hand-off produces is written to the shared Redis session
// store, so the messages survive the disconnect of a broker node and are drained by
// the node the client reconnects to.
func TestInflightOverflowQueuesForRedisStoreAcrossNodes(t *testing.T) {
	mr := runMiniRedis(t)
	metrics := newDeliveryMetrics()
	store := newTestRedisSessionStore(t, mr, metrics)

	// Two independent brokers of one cluster: node A holds the connection, node B
	// resumes the session from the store.
	a := overflowBroker(t, Config{NodeID: "overflow-a", SessionStore: store, Metrics: metrics, MaxOfflineQueueSize: 64})
	bb := overflowBroker(t, Config{NodeID: "overflow-b", SessionStore: store, Metrics: metrics, MaxOfflineQueueSize: 64})

	stalled := stalledSession(t, a, "overflow-redis", "redis/#")
	pub, _ := mustConnect(t, a.Addr(), confConnect{clientID: "overflow-redis-pub", clean: true})
	const messages = 7
	for i := 1; i <= messages; i++ {
		pub.mustPublish(t, confPublish{topic: "redis/x", payload: fmt.Sprintf("m%d", i), qos: 1, pid: uint16(i)})
	}

	waitDisconnected(t, a, "overflow-redis")
	key := namespaceKey(a.namespace, "overflow-redis")
	waitFor(t, "the session to be stored in Redis", func() bool {
		raw, err := mr.Get(sessionStoreKey(key))
		return err == nil && len(raw) > 0
	})
	// Node A must not keep the session in its own table any more: the store is the
	// single copy (that is what keeps two nodes from serving one session).
	if _, ok := a.store.Load(key); !ok {
		t.Fatal("the session is not in the shared store")
	}
	if _, err := mr.Get(sessionStoreKey(key)); err != nil {
		t.Fatalf("redis session key: %v", err)
	}
	if n := metrics.overflows("inflight_overflow"); n != 1 {
		t.Fatalf("RecordOverflow(\"inflight_overflow\") = %d, want 1", n)
	}
	if n := metrics.count("inflight_overflow"); n != 0 {
		t.Fatalf("inflight_overflow drops = %d, want 0", n)
	}

	// The client reconnects to the other node and drains what node A queued.
	resumed, ack := mustConnect(t, bb.Addr(), confConnect{clientID: "overflow-redis", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the session was not resumed on the second node")
	}
	got, _ := drainResumed(t, resumed, messages)
	want := wantPayloads(messages)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivery %d of the resumed session on node B = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
	waitFor(t, "the resumed session to leave the shared store", func() bool {
		_, err := mr.Get(sessionStoreKey(key))
		return err != nil
	})
	_ = stalled.conn.Close()
}

// TestInflightOverflowCleanSessionDropsWithoutQueueing pins the other half of the
// policy: a clean session has nowhere to queue — its state ends with the connection
// — so the overflow drops the delivery, counted with its own reason, and nothing is
// queued for the session id afterwards.
func TestInflightOverflowCleanSessionDropsWithoutQueueing(t *testing.T) {
	metrics := newDeliveryMetrics()
	logger, logs := captureLogger()
	b := overflowBroker(t, Config{Metrics: metrics, Logger: logger})

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-clean", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "clean/#", QoS: 1})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-clean-pub", clean: true})

	// m1 fills the window, m2 the queue; m3 and m4 find both full and are dropped.
	for i := 1; i <= 4; i++ {
		pub.mustPublish(t, confPublish{topic: "clean/x", payload: fmt.Sprintf("m%d", i), qos: 1, pid: uint16(i)})
	}
	if n := metrics.count("inflight_overflow_clean"); n != 2 {
		t.Fatalf("inflight_overflow_clean drops = %d, want 2", n)
	}
	if n := metrics.overflows("inflight_overflow"); n != 0 {
		t.Fatalf("RecordOverflow(\"inflight_overflow\") = %d, want 0: a clean session is not taken offline", n)
	}
	logged := logs.String()
	if !strings.Contains(logged, "inflight_overflow_clean") || !strings.Contains(logged, "overflow-clean") {
		t.Fatalf("the drop was not logged with the reason and the client id: %q", logged)
	}

	// Nothing is stored, and the connection stays up: the client is still reading
	// what fits, and its session is not part of any durable state.
	if _, ok := b.store.Load(namespaceKey(b.namespace, "overflow-clean")); ok {
		t.Fatal("a clean session was stored after an inflight overflow")
	}
	if depth := offlineDepth(b); depth != 0 {
		t.Fatalf("offline queue depth = %d, want 0: a clean session never queues", depth)
	}
	first := expectPublish(t, sub)
	if string(first.Payload) != "m1" {
		t.Fatalf("first delivery = %q, want m1", first.Payload)
	}
	sub.ack(first.PacketID)
	for _, want := range []string{"m2"} {
		got := expectPublish(t, sub)
		if string(got.Payload) != want {
			t.Fatalf("queued delivery = %q, want %q", got.Payload, want)
		}
		sub.ack(got.PacketID)
	}
	expectNoMessage(t, sub, 100*time.Millisecond)
}

// TestInflightOverflowOfflineQueueFullCountsTheLoss pins the only loss point that
// remains for a persistent session: the offline queue's own per-session cap. The
// session is taken offline once, the queue keeps its newest MaxOfflineQueueSize
// messages (drop-oldest), and every message that goes is counted with the reason
// that cap already had.
func TestInflightOverflowOfflineQueueFullCountsTheLoss(t *testing.T) {
	metrics := newDeliveryMetrics()
	cfg := Config{Metrics: metrics, MaxOfflineQueueSize: 2}
	b := overflowBroker(t, cfg)

	stalled := stalledSession(t, b, "overflow-cap", "cap/#")
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-cap-pub", clean: true})
	const messages = 6
	for i := 1; i <= messages; i++ {
		pub.mustPublish(t, confPublish{topic: "cap/x", payload: fmt.Sprintf("m%d", i), qos: 1, pid: uint16(i)})
	}
	if n := metrics.overflows("inflight_overflow"); n != 1 {
		t.Fatalf("RecordOverflow(\"inflight_overflow\") = %d, want 1", n)
	}
	if n := metrics.count("inflight_overflow") + metrics.count("inflight_overflow_clean"); n != 0 {
		t.Fatalf("inflight overflow drops = %d, want 0: the offline cap is the drop point", n)
	}
	if n := metrics.count("offline_queue_full"); n == 0 {
		t.Fatal("the per-session offline cap did not count its drops")
	}
	if n := metrics.count("offline_session_missing"); n != 0 {
		t.Fatalf("offline_session_missing = %d, want 0: the queue exists, it is full", n)
	}

	// Drop-oldest: the newest MaxOfflineQueueSize messages of the acknowledged ones
	// survive, so the drain ends with the last message and has no gaps at the tail.
	resumed, ack := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-cap", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the capped session was not resumed")
	}
	var got []string
	for {
		msg, err := resumed.readFor(time.Now().Add(150 * time.Millisecond))
		if err != nil {
			break
		}
		p, ok := msg.(*mqtt.Publish)
		if !ok {
			t.Fatalf("read %T while draining", msg)
		}
		got = append(got, string(p.Payload))
		if p.QoS == 1 {
			resumed.ack(p.PacketID)
		}
	}
	if len(got) == 0 || got[len(got)-1] != fmt.Sprintf("m%d", messages) {
		t.Fatalf("drained %v, want the newest messages, ending with m%d", got, messages)
	}
	if len(got) > messages {
		t.Fatalf("drained %d deliveries of %d published messages", len(got), messages)
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("drained %v, want increasing payloads", got)
		}
	}
	if preserved := len(got); preserved != 3 {
		// m1 is the unconfirmed exchange (it is always kept: it is session state,
		// not queue state) and two queue slots survive the per-session cap.
		t.Fatalf("drained %d deliveries, want 3 (m1 plus the two queue slots)", preserved)
	}
	_ = stalled.conn.Close()
}

// TestInflightOverflowQoS2SessionResumesExchanges pins the QoS 2 half of the same
// policy: the exchange that was unconfirmed when the session went offline is stored
// with its packet id and stage (await PUBREC) and resumed under that id with DUP,
// and the queue behind it is drained in order, so neither the QoS 2 handshake nor
// the messages are lost with the connection.
func TestInflightOverflowQoS2SessionResumesExchanges(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := overflowBroker(t, Config{Metrics: metrics, MaxInflightQoS2: 1, MaxOfflineQueueSize: 64})

	stalled := stalledSessionAt(t, b, "overflow-q2", "q2/#", 2)
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-q2-pub", clean: true})
	const messages = 5
	for i := 1; i <= messages; i++ {
		publishQoS2(t, pub, confPublish{topic: "q2/x", payload: fmt.Sprintf("m%d", i), qos: 2, pid: uint16(i)})
	}
	if n := metrics.overflows("inflight_overflow"); n != 1 {
		t.Fatalf("RecordOverflow(\"inflight_overflow\") = %d, want 1", n)
	}
	if n := metrics.count("inflight_overflow") + metrics.count("inflight_overflow_clean"); n != 0 {
		t.Fatalf("inflight overflow drops = %d, want 0", n)
	}

	waitDisconnected(t, b, "overflow-q2")
	key := namespaceKey(b.namespace, "overflow-q2")
	waitFor(t, "the QoS 2 session to be stored", func() bool {
		_, ok := b.store.Load(key)
		return ok
	})
	stored, ok := b.store.Load(key)
	if !ok {
		t.Fatal("the QoS 2 session was not stored")
	}
	if len(stored.Inflight) != 1 || stored.Inflight[0].QoS != 2 || stored.Inflight[0].Stage != InflightAwaitPubrec {
		t.Fatalf("stored exchanges = %+v, want one QoS 2 exchange awaiting PUBREC", stored.Inflight)
	}
	if got := len(stored.Queue); got != messages-1 {
		t.Fatalf("stored queue = %d messages, want %d", got, messages-1)
	}

	resumed, ack := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-q2", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the QoS 2 session was not resumed")
	}
	var got []string
	var first *mqtt.Publish
	pubrel := false
	deadline := time.Now().Add(3 * time.Second)
	for len(got) < messages || !pubrel {
		msg, err := resumed.readFor(deadline)
		if err != nil {
			t.Fatalf("read while resuming: %v (deliveries so far %v, PUBREL seen %v)", err, got, pubrel)
		}
		switch m := msg.(type) {
		case *mqtt.Publish:
			if first == nil {
				first = m
			}
			got = append(got, string(m.Payload))
			switch m.QoS {
			case 2:
				resumed.write(&mqtt.Pubrec{MessageHeader: mqtt.MessageHeader{Level: m.Version()}, PacketID: m.PacketID})
			case 1:
				resumed.ack(m.PacketID)
			}
		case *mqtt.Pubrel:
			resumed.write(&mqtt.Pubcomp{MessageHeader: mqtt.MessageHeader{Level: m.Version()}, PacketID: m.PacketID})
			pubrel = true
		}
	}
	want := wantPayloads(messages)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivery %d of the resumed QoS 2 session = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
	if first == nil || first.QoS != 2 || !first.Dup {
		t.Fatalf("the resumed exchange = %+v, want a QoS 2 PUBLISH with DUP", first)
	}
	if !pubrel {
		t.Fatal("the resumed QoS 2 exchange never completed its PUBREL")
	}
	_ = stalled.conn.Close()
}

// TestInflightOverflowLeavesNoGoroutineAndNoWriteAfterClose pins the teardown: the
// hand-off joins the writer goroutine it interrupts, no write happens on the closed
// socket after the session is stored, and the broker's goroutines are gone once it
// stops. The suite runs under -race, which is what makes the "no write after close"
// half of the claim observable: a write racing the close is a data race on the
// connection, not only a lost byte.
func TestInflightOverflowLeavesNoGoroutineAndNoWriteAfterClose(t *testing.T) {
	before := settledGoroutines()
	metrics := newDeliveryMetrics()
	b := overflowBroker(t, Config{Metrics: metrics, MaxOfflineQueueSize: 64})

	ids := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("overflow-leak-%d", i)
		ids = append(ids, id)
		stalledSession(t, b, id, "leak/#")
	}
	// Hold on to the broker-side connections: after the hand-off it must be
	// impossible to write to them.
	b.mu.RLock()
	conns := make([]*client, len(ids))
	for i, id := range ids {
		conns[i] = b.clients[namespaceKey(b.namespace, id)]
	}
	b.mu.RUnlock()

	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-leak-pub", clean: true})
	for round := 1; round <= 4; round++ {
		for i := range ids {
			pub.mustPublish(t, confPublish{topic: "leak/x", payload: fmt.Sprintf("m%d", round), qos: 1, pid: uint16(round*10 + i)})
		}
	}
	for i, id := range ids {
		waitDisconnected(t, b, id)
		// The session the hand-off stored is intact: one exchange plus the messages
		// that were waiting behind it.
		stored, ok := b.store.Load(namespaceKey(b.namespace, id))
		if !ok {
			t.Fatalf("session %q was not stored", id)
		}
		if len(stored.Queue) == 0 || len(stored.Inflight) == 0 {
			t.Fatalf("session %q stored %d exchanges and %d queued messages, want both",
				id, len(stored.Inflight), len(stored.Queue))
		}
		// Nothing may write to the socket any more: the queue is closed for writes
		// and the writer goroutine was joined by close, which runs before unregister.
		c := conns[i]
		if c == nil {
			t.Fatalf("connection %q was not registered", id)
		}
		if !c.writeClosed.Load() {
			t.Fatalf("connection %q still accepts writes after the hand-off", id)
		}
		select {
		case <-c.writerDone:
		default:
			t.Fatalf("the writer goroutine of %q is still running after the hand-off", id)
		}
		c.mu.Lock()
		left := len(c.pending)
		c.mu.Unlock()
		if left != 0 {
			t.Fatalf("connection %q still holds %d pending deliveries after the hand-off", id, left)
		}
	}
	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("broker stop: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines after the overflow churn = %d, want at most %d", runtime.NumGoroutine(), before+2)
}
