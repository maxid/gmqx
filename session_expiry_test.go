package gmqx

// Session expiry tests: an MQTT 5 session carries a deadline, a bounded sweeper
// deletes the stored state when it passes, and a CONNECT never resumes an expired
// session even before the sweeper reaches it. MQTT 3.1.1 has no Session Expiry
// Interval, so its persistent sessions keep the pre-feature behavior and never
// expire.

import (
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// expiryProps is the CONNECT property block of an MQTT 5 session expiry interval.
func expiryProps(seconds uint32) mqtt.Properties {
	return mqtt.Properties{SessionExpiryInterval: seconds}
}

// TestSessionExpirySweepSessions verifies the whole path: the deadline is stamped
// on disconnect, the sweeper deletes the session with its subscriptions and its
// queue, and the client id starts fresh afterwards.
func TestSessionExpirySweepSessions(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics, SessionSweepInterval: 10 * time.Millisecond})

	c, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "expiring",
		clean:    false,
		props:    expiryProps(1),
	})
	if ack.SessionPresent {
		t.Fatal("a new session reported Session Present")
	}
	subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "expire/#", QoS: 1})
	c.disconnect()
	waitDisconnected(t, b, "expiring")

	stored, ok := b.store.Load("expiring")
	if !ok {
		t.Fatal("the persistent MQTT 5 session was not stored")
	}
	if stored.ExpiresAt.IsZero() {
		t.Fatal("the stored session has no expiry deadline")
	}
	if wait := time.Until(stored.ExpiresAt); wait < 500*time.Millisecond || wait > 2*time.Second {
		t.Fatalf("expiry deadline is %v away, want about the 1 s interval", wait)
	}

	// A message queued while the session is offline must disappear with it.
	publishQoS1(t, b, "expire/x", "queued", 1)
	if depth := offlineDepth(b); depth != 1 {
		t.Fatalf("global offline counter = %d, want 1", depth)
	}

	waitFor(t, "the session to expire", func() bool {
		_, ok := b.store.Load("expiring")
		return !ok
	})
	waitFor(t, "the expiry to be counted", func() bool { return metrics.count("session_expired") > 0 })
	if depth := offlineDepth(b); depth != 0 {
		t.Fatalf("global offline counter after the expiry = %d, want 0", depth)
	}

	// The subscriptions are gone, so a fresh session receives nothing.
	fresh, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "expiring",
		clean:    false,
		props:    expiryProps(60),
	})
	if ack.SessionPresent {
		t.Fatal("an expired session was resumed")
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "expire-pub", clean: true})
	pub.publish(confPublish{topic: "expire/x", payload: "after", qos: 1, pid: 1})
	expectPuback(t, pub, 1, 0)
	expectNoMessage(t, fresh, 100*time.Millisecond)
}

// TestSessionExpiryResumeInsideWindow pins that a session resumes as long as its
// deadline has not passed, and that the deadline is re-stamped on every
// disconnect.
func TestSessionExpiryResumeInsideWindow(t *testing.T) {
	b := startBroker(t, Config{SessionSweepInterval: 10 * time.Millisecond})
	c, _ := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "renewing",
		clean:    false,
		props:    expiryProps(60),
	})
	subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "renew/#", QoS: 1})
	c.disconnect()
	waitDisconnected(t, b, "renewing")

	// Several sweeper ticks pass; a 60 s deadline must survive them.
	time.Sleep(50 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	second, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "renewing",
		clean:    false,
		props:    expiryProps(60),
	})
	if !ack.SessionPresent {
		t.Fatal("a session inside its expiry window did not resume")
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "renew-pub", clean: true})
	pub.publish(confPublish{topic: "renew/x", payload: "resumed", qos: 0})
	if got := expectPublish(t, second); string(got.Payload) != "resumed" {
		t.Fatalf("resumed delivery = %q", got.Payload)
	}

	// The resumed session gets a new deadline when it disconnects again.
	second.disconnect()
	waitDisconnected(t, b, "renewing")
	stored, ok := b.store.Load("renewing")
	if !ok || stored.ExpiresAt.IsZero() || !stored.ExpiresAt.After(time.Now().Add(50*time.Second)) {
		t.Fatalf("re-stamped deadline = %v (ok=%v), want about 60 s from now", stored.ExpiresAt, ok)
	}
}

// TestSessionExpiryZeroIntervalNeverExpires pins the documented compatibility
// case: without an interval there is no deadline. MQTT 3.1.1 has no Session Expiry
// Interval at all, so its persistent sessions never expire; an MQTT 5 CONNECT
// without one does not keep a session after the connection ends (that is the
// interval's meaning), which is the behavior the broker had before the sweeper
// existed.
func TestSessionExpiryZeroIntervalNeverExpires(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics, SessionSweepInterval: 5 * time.Millisecond})

	c, _ := mustConnect(t, b.Addr(), confConnect{clientID: "v311-forever", clean: false})
	subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "forever/#", QoS: 1})
	c.disconnect()
	waitDisconnected(t, b, "v311-forever")

	stored, ok := b.store.Load("v311-forever")
	if !ok {
		t.Fatal("the MQTT 3.1.1 persistent session was not stored")
	}
	if !stored.ExpiresAt.IsZero() {
		t.Fatalf("an MQTT 3.1.1 session got the deadline %v, want none", stored.ExpiresAt)
	}
	// Many sweeper ticks pass and the session stays.
	time.Sleep(50 * time.Millisecond)
	if _, ok := b.store.Load("v311-forever"); !ok {
		t.Fatal("a session without an expiry interval was swept")
	}
	if n := metrics.count("session_expired"); n != 0 {
		t.Fatalf("session_expired = %d, want 0", n)
	}
	again, ack := mustConnect(t, b.Addr(), confConnect{clientID: "v311-forever", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the session did not resume after many sweeper ticks")
	}
	_ = again

	// MQTT 5 with an absent interval: the session ends with the connection, so
	// there is nothing to expire either.
	c5, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-zero", clean: false})
	subscribe(t, c5, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "zero/#", QoS: 1})
	c5.disconnect()
	waitDisconnected(t, b, "v5-zero")
	if _, ok := b.store.Load("v5-zero"); ok {
		t.Fatal("an MQTT 5 session with a zero Session Expiry Interval was stored")
	}
}

// TestSessionExpiryCheckedOnResume pins that the deadline is enforced by the
// CONNECT path itself: a sweeper tick is bounded and may lag, so a session that
// expired must not be resumed in the meantime.
func TestSessionExpiryCheckedOnResume(t *testing.T) {
	b := startBroker(t, Config{SessionSweepInterval: time.Hour})
	c, _ := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "stale",
		clean:    false,
		props:    expiryProps(60),
	})
	subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "stale/#", QoS: 1})
	c.disconnect()
	waitDisconnected(t, b, "stale")

	// Move the deadline into the past; the sweeper is effectively disabled, so
	// only the CONNECT path can reject the session.
	stored, ok := b.store.Load("stale")
	if !ok {
		t.Fatal("the session was not stored")
	}
	stored.ExpiresAt = time.Now().Add(-time.Second)
	b.store.Save("stale", stored)

	again, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "stale",
		clean:    false,
		props:    expiryProps(60),
	})
	if ack.SessionPresent {
		t.Fatal("an expired session was resumed")
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "stale-pub", clean: true})
	pub.publish(confPublish{topic: "stale/x", payload: "no", qos: 0})
	expectNoMessage(t, again, 100*time.Millisecond)
	if _, ok := b.store.Load("stale"); ok {
		t.Fatal("the expired session is still stored")
	}
}
