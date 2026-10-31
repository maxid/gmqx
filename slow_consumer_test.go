package gmqx

// Writer-queue tests: every packet of a connection leaves through that
// connection's single writer goroutine, which writes one packet at a time under
// Config.WriteTimeout. The point of the tests here is that no other goroutine —
// the publisher's read loop, a will publish inside close, the cluster receive
// goroutine — can be held by a peer that stopped reading.
//
// The stalled peer is a net.Pipe instead of a TCP socket: a pipe has no buffer at
// all, so the first packet the broker writes to a peer that does not read blocks
// in the writer goroutine. That is the worst case of a full socket buffer, made
// deterministic.
//
// The policy the cases pin down: a QoS 0 PUBLISH may be dropped at a full queue
// ("outbound_queue_full"), anything else disconnects the slow consumer
// ("slow_consumer") instead of losing an acknowledged message.

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

// pipeClient attaches a client to b over net.Pipe instead of the TCP listener:
// b.serve is the same per-connection goroutine the listener calls, only the
// transport differs.
func pipeClient(t *testing.T, b *Broker, cc confConnect) *confClient {
	t.Helper()
	server, peer := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	go b.serve(server)
	c := newConfClient(t, peer, cc.version, cc.clientID)
	c.sendConnect(cc)
	if ack := readConf[*mqtt.Connack](t, c); ack.ReasonCode != 0 {
		t.Fatalf("CONNACK reason code %#x over the pipe, want success", ack.ReasonCode)
	}
	return c
}

// waitWriterWedged waits until the writer goroutine of clientID is stuck in a
// write: a packet it already took from the queue never reaches a peer that does
// not read, so the packets behind it stay queued.
func waitWriterWedged(t *testing.T, b *Broker, clientID string) {
	t.Helper()
	waitFor(t, "the writer of "+clientID+" to be stuck on the socket", func() bool {
		b.mu.RLock()
		c := b.clients[clientID]
		b.mu.RUnlock()
		return c != nil && len(c.outq) > 0
	})
}

// expectClosedNow asserts that the broker closed the connection right now: a read
// fails with something other than a timeout.
func expectClosedNow(t *testing.T, c *confClient) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.r.ReadByte(); err == nil {
		t.Fatal("the broker sent a packet where the connection should be closed")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("the connection is still open")
	}
}

// TestStalledSubscriberDoesNotBlockPublisher pins the first of the three stalls:
// a subscriber that stops reading no longer holds the publisher. Every publish is
// acknowledged and a healthy subscriber receives every message while the stalled
// one sits on a wedged socket.
func TestStalledSubscriberDoesNotBlockPublisher(t *testing.T) {
	// The write deadline is far away: only the queue keeps the write off the
	// publisher's path, which is what this case is about.
	b := startBroker(t, Config{MaxOutboundQueueSize: 64, WriteTimeout: 30 * time.Second})

	stalled := pipeClient(t, b, confConnect{clientID: "stalled-sub", clean: true})
	subscribe(t, stalled, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "stall/#", QoS: 1})

	healthy, _ := mustConnect(t, b.Addr(), confConnect{clientID: "healthy-sub", clean: true})
	subscribe(t, healthy, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "stall/#", QoS: 1})

	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "stall-pub", clean: true})
	const messages = 5
	for i := 1; i <= messages; i++ {
		// mustPublish waits for the PUBACK of this publisher's own connection: if
		// the delivery to the stalled subscriber were written on the publishing
		// goroutine, this call would never return.
		pub.mustPublish(t, confPublish{topic: "stall/x", payload: fmt.Sprintf("m%d", i), qos: 1, pid: uint16(i)})
	}
	waitWriterWedged(t, b, "stalled-sub")

	for i := 1; i <= messages; i++ {
		got := expectPublish(t, healthy)
		if want := fmt.Sprintf("m%d", i); string(got.Payload) != want {
			t.Fatalf("healthy subscriber delivery %d = %q, want %q", i, got.Payload, want)
		}
	}
}

// TestStalledSubscriberDoesNotBlockClusterDispatch pins the second stall: the
// cluster receive goroutine of a node fans a remote publication out to its local
// subscribers, and a stalled one used to stop every cross-node delivery.
func TestStalledSubscriberDoesNotBlockClusterDispatch(t *testing.T) {
	b := startBroker(t, Config{MaxOutboundQueueSize: 64, WriteTimeout: 30 * time.Second})

	stalled := pipeClient(t, b, confConnect{clientID: "cluster-stalled", clean: true})
	subscribe(t, stalled, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "cluster/#", QoS: 1})
	// Two warm-up deliveries: the writer takes the first and blocks on the pipe,
	// the second stays in the queue, which is how the test observes the stall.
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "cluster-warmup", clean: true})
	pub.mustPublish(t, confPublish{topic: "cluster/warm", payload: "m0", qos: 1, pid: 1})
	pub.mustPublish(t, confPublish{topic: "cluster/warm", payload: "m1", qos: 1, pid: 2})
	waitWriterWedged(t, b, "cluster-stalled")

	// A subscriber that does read, attached after the stall so it only sees the
	// cross-node publication below.
	healthy, _ := mustConnect(t, b.Addr(), confConnect{clientID: "cluster-healthy", clean: true})
	subscribe(t, healthy, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "cluster/#", QoS: 1})

	// OnRemotePublish is what a cluster backend calls from its receive goroutine.
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.OnRemotePublish(ClusterMessage{
			Type:    ClusterPublish,
			NodeID:  "peer",
			Topic:   "cluster/x",
			Payload: []byte("from another node"),
			QoS:     1,
		})
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the cluster receive path is blocked by a stalled subscriber")
	}
	// The cross-node delivery still reached the subscriber that reads.
	got := expectPublish(t, healthy)
	if got.Topic != "cluster/x" || string(got.Payload) != "from another node" {
		t.Fatalf("cross-node delivery = %q %q, want cluster/x %q", got.Topic, got.Payload, "from another node")
	}
}

// TestStalledSubscriberDoesNotBlockWillPublish pins the third stall: the will of a
// connection that ends is published inside close, so a wedged subscriber would
// keep the dying connection (and its session) alive. The connection must
// unregister although the will cannot be written to the stalled peer.
func TestStalledSubscriberDoesNotBlockWillPublish(t *testing.T) {
	b := startBroker(t, Config{MaxOutboundQueueSize: 64, WriteTimeout: 30 * time.Second})

	stalled := pipeClient(t, b, confConnect{clientID: "will-stalled", clean: true})
	subscribe(t, stalled, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "will/#", QoS: 1})
	// Two warm-up deliveries: the writer takes the first and blocks on the pipe,
	// the second stays in the queue, which is how the test observes the stall.
	warm, _ := mustConnect(t, b.Addr(), confConnect{clientID: "will-warmup", clean: true})
	warm.mustPublish(t, confPublish{topic: "will/warm", payload: "m0", qos: 1, pid: 1})
	warm.mustPublish(t, confPublish{topic: "will/warm", payload: "m1", qos: 1, pid: 2})
	waitWriterWedged(t, b, "will-stalled")

	// The will of this connection is published to will/#, which the stalled
	// subscriber matches.
	src, _ := mustConnect(t, b.Addr(), confConnect{
		clientID: "will-source",
		clean:    true,
		will:     &mqtt.Will{Topic: "will/gone", Payload: []byte("gone"), QoS: 1},
	})
	_ = src.conn.Close()
	// unregister runs after the will publish returned, so this proves it did not
	// wait for the stalled socket.
	waitDisconnected(t, b, "will-source")
}

// TestFullOutboundQueueDropsQoS0WithMetric pins the documented policy for the one
// packet class the protocol lets the broker lose: beyond a full queue a QoS 0
// PUBLISH is dropped and counted, and the connection is kept.
func TestFullOutboundQueueDropsQoS0WithMetric(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{
		Metrics:              metrics,
		MaxOutboundQueueSize: 8,
		WriteTimeout:         30 * time.Second,
	})

	stalled := pipeClient(t, b, confConnect{clientID: "qos0-stalled", clean: true})
	subscribe(t, stalled, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "qos0/#", QoS: 0})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "qos0-pub", clean: true})

	for i := 0; i < 100; i++ {
		pub.publish(confPublish{topic: "qos0/x", payload: strings.Repeat("x", 256), qos: 0})
	}
	waitFor(t, "the QoS 0 drop to be counted", func() bool { return metrics.count("outbound_queue_full") >= 1 })
	if n := metrics.overflows("outbound_queue"); n < 1 {
		t.Fatalf("overflow(\"outbound_queue\") = %d, want at least 1", n)
	}
	if n := metrics.count("slow_consumer"); n != 0 {
		t.Fatalf("slow_consumer disconnects = %d, want 0: QoS 0 is dropped, not the connection", n)
	}
}

// TestFullOutboundQueueDisconnectsSlowConsumer pins the policy for everything else:
// a QoS 1 delivery that finds the writer queue full disconnects the slow consumer
// with a metric instead of being silently lost.
func TestFullOutboundQueueDisconnectsSlowConsumer(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{
		Metrics:              metrics,
		MaxOutboundQueueSize: 8,
		// The window does not queue anything here: every delivery goes straight to
		// the writer queue, which is what fills up.
		MaxInflightQoS1: -1,
		WriteTimeout:    30 * time.Second,
	})

	stalled := pipeClient(t, b, confConnect{clientID: "slow-stalled", clean: true})
	subscribe(t, stalled, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "slow/#", QoS: 1})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "slow-pub", clean: true})

	for i := 1; i <= 40; i++ {
		pub.mustPublish(t, confPublish{topic: "slow/x", payload: fmt.Sprintf("m%d", i), qos: 1, pid: uint16(i)})
	}
	waitFor(t, "the slow consumer to be counted", func() bool { return metrics.count("slow_consumer") == 1 })
	waitDisconnected(t, b, "slow-stalled")
	expectClosedNow(t, stalled)
	// One report per connection, however many packets hit the full queue.
	if n := metrics.count("slow_consumer"); n != 1 {
		t.Fatalf("slow_consumer reports = %d, want exactly 1", n)
	}
}

// TestWriteDeadlineClosesWedgedConnection pins the write deadline: a peer that
// stops reading cannot hold the writer goroutine forever, because every write
// carries Config.WriteTimeout and the connection is closed when it expires.
func TestWriteDeadlineClosesWedgedConnection(t *testing.T) {
	const timeout = 100 * time.Millisecond
	b := startBroker(t, Config{WriteTimeout: timeout})

	stalled := pipeClient(t, b, confConnect{clientID: "deadline-stalled", clean: true})
	subscribe(t, stalled, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "deadline/#", QoS: 1})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "deadline-pub", clean: true})

	start := time.Now()
	pub.mustPublish(t, confPublish{topic: "deadline/x", payload: "m1", qos: 1, pid: 1})
	// The writer takes the PUBLISH and blocks on the pipe; the deadline fires and
	// the broker closes the connection.
	waitDisconnected(t, b, "deadline-stalled")
	expectClosedNow(t, stalled)
	if elapsed := time.Since(start); elapsed < timeout {
		t.Fatalf("the connection was closed after %v, before the %v write deadline", elapsed, timeout)
	}
}

// TestNoGoroutineLeakWithQueuedWrites drives repeated connect/disconnect cycles
// over connections whose writer is stuck or has queued packets, and then checks
// that no goroutine of the broker survives. It runs under -race with the rest of
// the suite, which also covers the concurrent access to the queue, the inflight
// table and the writer lifecycle.
func TestNoGoroutineLeakWithQueuedWrites(t *testing.T) {
	before := settledGoroutines()
	b := startBroker(t, Config{
		MaxOutboundQueueSize: 4,
		WriteTimeout:         20 * time.Millisecond,
		QoSRetryInterval:     10 * time.Millisecond,
		QoSMaxRetries:        100,
	})

	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "leakw-pub", clean: true})
	const clients = 12
	for i := 0; i < clients; i++ {
		id := fmt.Sprintf("leakw-%d", i)
		sub := pipeClient(t, b, confConnect{clientID: id, clean: false})
		subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "leakw/#", QoS: 1})
		// Three deliveries per connection: one the writer takes (and loses to the
		// write deadline) and two that stay queued.
		for j := 0; j < 3; j++ {
			pub.mustPublish(t, confPublish{topic: "leakw/x", payload: "queued", qos: 1, pid: uint16(i*3 + j + 1)})
		}
		_ = sub.conn.Close()
	}
	waitFor(t, "the pipe clients to unregister", func() bool {
		b.mu.RLock()
		defer b.mu.RUnlock()
		for key := range b.clients {
			if strings.HasPrefix(key, "leakw-") && key != "leakw-pub" {
				return false
			}
		}
		return true
	})
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
	t.Fatalf("goroutines after the churn = %d, want at most %d", runtime.NumGoroutine(), before+2)
}
