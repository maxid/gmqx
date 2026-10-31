package gmqx

// Broker-side retransmission tests: an outbound QoS 1/2 delivery is not finished
// until the client confirms it, so the broker repeats it on one timer per
// connection — a PUBLISH with DUP set until PUBACK/PUBREC, a PUBREL until PUBCOMP
// — and stops at the confirmation or at the configured attempt cap. The inflight
// windows bound the outstanding exchanges in both directions.

import (
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// expectPubrelWithin reads until the PUBREL of pid arrives, tolerating the
// retransmitted PUBLISHes of the same exchange that may already be in flight.
func expectPubrelWithin(t *testing.T, c *confClient, pid uint16, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		msg, err := c.readFor(deadline)
		if err != nil {
			t.Fatalf("read PUBREL %d within %v: %v", pid, d, err)
		}
		switch m := msg.(type) {
		case *mqtt.Publish:
			if !m.Dup || m.PacketID != pid {
				t.Fatalf("PUBLISH while waiting for PUBREL %d: dup=%v pid=%d", pid, m.Dup, m.PacketID)
			}
		case *mqtt.Pubrel:
			if m.PacketID != pid {
				t.Fatalf("PUBREL packet id = %d, want %d", m.PacketID, pid)
			}
			return
		default:
			t.Fatalf("read %T while waiting for PUBREL %d", msg, pid)
		}
	}
}

// TestOutboundQoS1RetransmitsWithDup is the core of the outbound QoS 1 state
// machine: an unconfirmed PUBLISH is repeated with DUP and the same packet id
// after the retry interval, and a PUBACK frees the id and stops the repeats.
func TestOutboundQoS1RetransmitsWithDup(t *testing.T) {
	const interval = 40 * time.Millisecond
	b := startBroker(t, Config{QoSRetryInterval: interval})

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "retry-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "retry/x", QoS: 1})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "retry-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "retry/x", payload: "hello", qos: 1, pid: 1})

	first := expectPublish(t, sub)
	if first.Dup || first.QoS != 1 || first.PacketID == 0 {
		t.Fatalf("first delivery = dup%v qos%d pid%d, want a fresh QoS 1 PUBLISH", first.Dup, first.QoS, first.PacketID)
	}
	if string(first.Payload) != "hello" {
		t.Fatalf("first delivery payload = %q", first.Payload)
	}

	second := expectPublish(t, sub)
	if !second.Dup {
		t.Fatal("the retransmitted PUBLISH does not carry DUP")
	}
	if second.PacketID != first.PacketID {
		t.Fatalf("retransmission packet id = %d, want the original %d", second.PacketID, first.PacketID)
	}
	if string(second.Payload) != "hello" || second.QoS != 1 {
		t.Fatalf("retransmission = %q qos%d", second.Payload, second.QoS)
	}

	sub.write(&mqtt.Puback{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: first.PacketID})
	// PUBACK stops the exchange: no further copy arrives.
	expectNoMessage(t, sub, 6*interval)
}

// TestOutboundQoS1RetryCapStops pins the attempt cap: after QoSMaxRetries
// retransmissions the broker stops repeating the PUBLISH and reports it.
func TestOutboundQoS1RetryCapStops(t *testing.T) {
	const interval = 20 * time.Millisecond
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{QoSRetryInterval: interval, QoSMaxRetries: 2, Metrics: metrics})

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "cap-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "cap/x", QoS: 1})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "cap-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "cap/x", payload: "keep", qos: 1, pid: 1})

	first := expectPublish(t, sub)
	if first.Dup {
		t.Fatal("the first delivery carries DUP")
	}
	for attempt := 1; attempt <= 2; attempt++ {
		got := expectPublish(t, sub)
		if !got.Dup || got.PacketID != first.PacketID {
			t.Fatalf("retransmission %d = dup%v pid%d, want dup with pid %d", attempt, got.Dup, got.PacketID, first.PacketID)
		}
	}
	expectNoMessage(t, sub, 10*interval)
	waitFor(t, "the exhausted retries to be counted", func() bool { return metrics.count("retry_exhausted") >= 1 })
}

// TestOutboundQoS2HandshakeRetransmitsPubrel walks the full server-to-client QoS 2
// exchange: the PUBLISH is repeated until PUBREC, the PUBREL until PUBCOMP, and
// PUBCOMP ends the exchange.
func TestOutboundQoS2HandshakeRetransmitsPubrel(t *testing.T) {
	const interval = 40 * time.Millisecond
	b := startBroker(t, Config{QoSRetryInterval: interval})

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "q2out-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "q2out/x", QoS: 2})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "q2out-pub", clean: true})
	pub.publish(confPublish{topic: "q2out/x", payload: "once", qos: 2, pid: 9})
	expectPubrec(t, pub, 9, 0)
	pub.write(&mqtt.Pubrel{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: 9})
	expectPubcomp(t, pub, 9)

	first := expectPublish(t, sub)
	if first.QoS != 2 || first.Dup || first.PacketID == 0 {
		t.Fatalf("first delivery = qos%d dup%v pid%d, want a fresh QoS 2 PUBLISH", first.QoS, first.Dup, first.PacketID)
	}
	// No PUBREC from the subscriber: the PUBLISH is retransmitted, DUP set.
	retransmit := expectPublish(t, sub)
	if !retransmit.Dup || retransmit.PacketID != first.PacketID {
		t.Fatalf("PUBLISH retransmission = dup%v pid%d, want dup with pid %d", retransmit.Dup, retransmit.PacketID, first.PacketID)
	}

	// PUBREC moves the exchange to PUBREL, which the broker repeats until PUBCOMP.
	sub.write(&mqtt.Pubrec{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: first.PacketID})
	expectPubrelWithin(t, sub, first.PacketID, 2*time.Second)
	expectPubrelWithin(t, sub, first.PacketID, 10*interval)

	sub.write(&mqtt.Pubcomp{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: first.PacketID})
	expectNoMessage(t, sub, 6*interval)
}

// TestInflightWindowQueuesInsteadOfDropping pins the window rule: an outbound
// QoS 1 delivery that would exceed MaxInflightQoS1 waits in the connection's
// inflight queue instead of being skipped, the publishing goroutine is never
// blocked by a subscriber that does not confirm, and the acknowledgement that
// frees the slot delivers the queued message.
//
// This is the case that used to assert the defect: it required the second
// delivery to be dropped and counted as "inflight_full" even though the
// publisher had already been acknowledged. A dropped QoS 1 delivery is lost with
// nobody noticing, so the case now asserts that the delivery is queued and sent
// when the window opens.
func TestInflightWindowQueuesInsteadOfDropping(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics, MaxInflightQoS1: 1})

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "window-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "window/x", QoS: 1})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "window-pub", clean: true})

	// Both publishes are acknowledged to the publisher: the second one waits in the
	// subscriber's inflight queue, it is not lost.
	pub.mustPublish(t, confPublish{topic: "window/x", payload: "one", qos: 1, pid: 1})
	pub.mustPublish(t, confPublish{topic: "window/x", payload: "two", qos: 1, pid: 2})

	first := expectPublish(t, sub)
	if string(first.Payload) != "one" {
		t.Fatalf("delivered payload = %q, want the first message", first.Payload)
	}
	// The window is full, so nothing else goes out — and nothing is dropped.
	expectNoMessage(t, sub, 100*time.Millisecond)
	if n := metrics.count("inflight_overflow"); n != 0 {
		t.Fatalf("inflight_overflow drops = %d, want 0: a full window queues", n)
	}
	if n := metrics.count("inflight_full"); n != 0 {
		t.Fatalf("inflight_full drops = %d, want 0: the old skip is gone", n)
	}

	// Confirming the first message frees the window: the queued delivery goes out
	// by itself, still at QoS 1 and still in order.
	sub.write(&mqtt.Puback{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: first.PacketID})
	second := expectPublish(t, sub)
	if string(second.Payload) != "two" {
		t.Fatalf("delivery after the window opened = %q, want \"two\"", second.Payload)
	}
	if second.QoS != 1 || second.PacketID == 0 || second.Dup {
		t.Fatalf("queued delivery = qos%d pid%d dup%v, want a fresh QoS 1 PUBLISH", second.QoS, second.PacketID, second.Dup)
	}
	sub.write(&mqtt.Puback{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: second.PacketID})

	// The queue is empty again, so a new publish is delivered without delay.
	waitInflightEmpty(t, b, "window-sub")
	pub.mustPublish(t, confPublish{topic: "window/x", payload: "three", qos: 1, pid: 3})
	if got := expectPublish(t, sub); string(got.Payload) != "three" {
		t.Fatalf("delivery after the queue drained = %q, want \"three\"", got.Payload)
	}
}

// waitInflightEmpty waits until the broker's connection for clientID has no
// unconfirmed outbound exchange, which is when an acknowledgement it just sent has
// been processed (a PUBACK frees the window asynchronously to the test's reads).
func waitInflightEmpty(t *testing.T, b *Broker, clientID string) {
	t.Helper()
	waitFor(t, "connection "+clientID+" to have no inflight exchange", func() bool {
		b.mu.RLock()
		c := b.clients[clientID]
		b.mu.RUnlock()
		if c == nil {
			return false
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.inflight) == 0
	})
}

// TestInboundQoS2WindowAnswersQuotaExceeded pins the inbound half of the QoS 2
// window: over MaxInflightQoS2 pending exchanges a new PUBLISH is not accepted and
// MQTT 5 is told why with PUBREC 0x97 instead of the message being dropped
// silently.
func TestInboundQoS2WindowAnswersQuotaExceeded(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics, MaxInflightQoS2: 1})
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "in-q2", clean: true})

	c.publish(confPublish{topic: "in/1", payload: "one", qos: 2, pid: 1})
	expectPubrec(t, c, 1, 0)

	c.publish(confPublish{topic: "in/2", payload: "two", qos: 2, pid: 2})
	expectPubrec(t, c, 2, ReasonQuotaExceeded)
	waitFor(t, "the inbound window drop to be counted", func() bool { return metrics.count("inflight_qos2_full") == 1 })

	// Completing the first exchange frees the window for a new one.
	c.write(&mqtt.Pubrel{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}, PacketID: 1})
	expectPubcomp(t, c, 1)
	c.publish(confPublish{topic: "in/3", payload: "three", qos: 2, pid: 3})
	expectPubrec(t, c, 3, 0)
}
