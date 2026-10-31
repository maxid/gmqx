package gmqx

// Retained message expiry (MQTT 5 Message Expiry Interval): the interval counts
// from the publish, so a retained message must be replayed with what is left of
// it — not with the value the publisher used — and must disappear once it is
// gone. A retained message without an interval never expires.

import (
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// TestRetainedMessageExpiryReplaysRemainingInterval pins the replay: a retained
// message that still has time left is delivered with the interval it has left.
func TestRetainedMessageExpiryReplaysRemainingInterval(t *testing.T) {
	b := startBroker(t, Config{unit: testSecond})
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "ret-exp-pub", clean: true})
	pub.publish(confPublish{
		topic:   "ret/x",
		payload: "value",
		qos:     0,
		retain:  true,
		props:   mqtt.Properties{MessageExpiryInterval: 10}, // 10 protocol seconds = 200 ms here
	})

	stored := waitRetained(t, b, "ret/x", "value")
	if stored.props.MessageExpiryInterval != 10 {
		t.Fatalf("stored interval = %d, want the published 10", stored.props.MessageExpiryInterval)
	}
	if stored.expiresAt.IsZero() {
		t.Fatal("the stored retained message carries no expiry deadline")
	}

	// Half of the interval passes, so the replay has less left than published.
	time.Sleep(5 * testSecond)
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "ret-exp-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "ret/#", QoS: 0})
	got := expectPublish(t, sub)
	if string(got.Payload) != "value" || !got.Retain {
		t.Fatalf("retained replay = (%q, retain %t), want (value, retain true)", got.Payload, got.Retain)
	}
	if remaining := got.Properties.MessageExpiryInterval; remaining >= 10 || remaining == 0 {
		t.Fatalf("replayed interval = %d, want it decremented into (0, 10)", remaining)
	}
}

// TestRetainedMessageExpiredIsNotReplayed pins the other end: past its interval a
// retained message is not delivered, and the store deletes it on the way.
func TestRetainedMessageExpiredIsNotReplayed(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{unit: testSecond, Metrics: metrics})
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "ret-old-pub", clean: true})
	pub.publish(confPublish{
		topic:   "ret/old",
		payload: "stale",
		qos:     0,
		retain:  true,
		props:   mqtt.Properties{MessageExpiryInterval: 5}, // 100 ms
	})
	waitRetained(t, b, "ret/old", "stale")

	time.Sleep(8 * testSecond) // well past the interval

	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "ret-old-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "ret/#", QoS: 0})
	expectNoMessage(t, sub, 150*time.Millisecond)
	if m, ok := retainedSnapshot(b, "ret/old"); ok {
		t.Fatalf("the expired retained message is still stored: %q", m.payload)
	}
	if got := metrics.count("retained_expired"); got == 0 {
		t.Fatal("the expired retained message was not counted")
	}
}

// TestRetainedWithoutIntervalNeverExpires pins the third rule: no interval means
// no expiry, however long the message stays in the store.
func TestRetainedWithoutIntervalNeverExpires(t *testing.T) {
	b := startBroker(t, Config{unit: testSecond})
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "ret-forever-pub", clean: true})
	pub.publish(confPublish{topic: "ret/forever", payload: "kept", qos: 0, retain: true})
	stored := waitRetained(t, b, "ret/forever", "kept")
	if !stored.expiresAt.IsZero() {
		t.Fatalf("a retained message without an interval got a deadline: %v", stored.expiresAt)
	}

	time.Sleep(10 * testSecond)
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "ret-forever-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "ret/forever", QoS: 0})
	got := expectPublish(t, sub)
	if string(got.Payload) != "kept" {
		t.Fatalf("retained replay = %q, want kept", got.Payload)
	}
	if got.Properties.MessageExpiryInterval != 0 {
		t.Fatalf("replayed interval = %d, want none", got.Properties.MessageExpiryInterval)
	}
}
