package gmqx

// Will Delay Interval (MQTT 5): a will is published when the earlier of two
// things happens — the delay elapses, or the session ends. A CONNECT that resumes
// the session cancels the pending will.
//
// The wire carries the delay in whole seconds, so the cases below shorten the
// broker's protocol second (Config.unit, an unexported test seam) and use small
// intervals: a case about seconds runs in tens of milliseconds instead of sleeping
// for real ones.

import (
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// testSecond is the protocol second the will-delay cases run with: one MQTT second is
// 20 ms, so a delay of 3 is 60 ms.
const testSecond = 20 * time.Millisecond

// willConnect builds an MQTT 5 CONNECT of a persistent session that registers a
// will with the given delay, in protocol seconds.
func willConnect(clientID, topic, payload string, delay, sessionExpiry uint32) confConnect {
	return confConnect{
		version:  mqtt.Version5,
		clientID: clientID,
		clean:    false,
		props:    mqtt.Properties{SessionExpiryInterval: sessionExpiry},
		will: &mqtt.Will{
			Topic:      topic,
			Payload:    []byte(payload),
			QoS:        0,
			Properties: mqtt.Properties{WillDelayInterval: delay},
		},
	}
}

// pendingWillCount reports how many delayed wills the broker is holding.
func pendingWillCount(b *Broker) int {
	b.willMu.Lock()
	defer b.willMu.Unlock()
	return len(b.wills)
}

// waitPendingWill waits until the broker holds want delayed wills.
func waitPendingWill(t *testing.T, b *Broker, want int) {
	t.Helper()
	waitFor(t, "the broker to hold the delayed will", func() bool { return pendingWillCount(b) == want })
}

// willSubscriber connects a plain subscriber to filter and returns it.
func willSubscriber(t *testing.T, b *Broker, clientID, filter string) *confClient {
	t.Helper()
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: clientID, clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: filter, QoS: 0})
	return sub
}

// TestWillDelayPublishesAfterDelay pins the plain case: the will of a session
// that asked for a delay is published once the delay elapsed, not when the
// connection ended.
func TestWillDelayPublishesAfterDelay(t *testing.T) {
	b := startBroker(t, Config{unit: testSecond})
	sub := willSubscriber(t, b, "will-delay-sub", "will/#")

	const delay = 3 // 60 ms
	client, _ := mustConnect(t, b.Addr(), willConnect("will-delay", "will/delay", "gone", delay, 100))
	start := time.Now()
	_ = client.conn.Close() // an abrupt close, so the will is due

	got := expectPublish(t, sub)
	elapsed := time.Since(start)
	if string(got.Payload) != "gone" || got.Topic != "will/delay" {
		t.Fatalf("will delivery = %q %q, want will/delay gone", got.Topic, got.Payload)
	}
	if want := delay * testSecond; elapsed < want {
		t.Fatalf("will published after %v, want at least the %v delay", elapsed, want)
	}
	if elapsed > time.Second {
		t.Fatalf("will published after %v, want about the %v delay", elapsed, delay*testSecond)
	}
	if n := pendingWillCount(b); n != 0 {
		t.Fatalf("broker still holds %d delayed wills after the publish, want 0", n)
	}
}

// TestWillDelayCancelledByReconnect pins the cancellation: a CONNECT that resumes
// the session before the delay elapsed means the session did not die, so the will
// is never published.
func TestWillDelayCancelledByReconnect(t *testing.T) {
	b := startBroker(t, Config{unit: testSecond})
	sub := willSubscriber(t, b, "will-resume-sub", "will/#")

	const delay = 5 // 100 ms
	client, _ := mustConnect(t, b.Addr(), willConnect("will-resume", "will/resume", "gone", delay, 200))
	_ = client.conn.Close()
	waitDisconnected(t, b, "will-resume")
	waitPendingWill(t, b, 1)

	// The session comes back inside the delay.
	again, ack := mustConnect(t, b.Addr(), willConnect("will-resume", "will/resume", "gone", delay, 200))
	if !ack.SessionPresent {
		t.Fatal("the resumed session reported no Session Present")
	}
	waitPendingWill(t, b, 0)

	// Well past the delay: the will of the closed connection never shows up.
	expectNoMessage(t, sub, 3*delay*testSecond)
	_ = again
}

// TestWillDelayFlushedByCleanStart pins the complement of the cancellation: a
// CONNECT with Clean Start set discards the session, so the session ends there and
// the pending will of the previous connection is published at that moment.
func TestWillDelayFlushedByCleanStart(t *testing.T) {
	b := startBroker(t, Config{unit: testSecond})
	sub := willSubscriber(t, b, "will-cleanstart-sub", "will/#")

	const delay = 20 // 400 ms
	client, _ := mustConnect(t, b.Addr(), willConnect("will-cleanstart", "will/cleanstart", "gone", delay, 200))
	_ = client.conn.Close()
	waitDisconnected(t, b, "will-cleanstart")
	waitPendingWill(t, b, 1)

	clean := willConnect("will-cleanstart", "will/cleanstart", "next", delay, 200)
	clean.clean = true
	start := time.Now()
	_, ack := mustConnect(t, b.Addr(), clean)
	if ack.SessionPresent {
		t.Fatal("a Clean Start connection reported Session Present")
	}
	got := expectPublish(t, sub)
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("the will of a discarded session was published after %v, want it at the CONNECT", elapsed)
	}
	if string(got.Payload) != "gone" {
		t.Fatalf("will delivery = %q, want gone", got.Payload)
	}
	waitPendingWill(t, b, 0)
}

// TestWillDelayCutShortBySessionExpiry pins the interaction with session expiry:
// the will is published when the session ends at the latest, so a session that
// expires inside the delay publishes its will at the expiry.
func TestWillDelayCutShortBySessionExpiry(t *testing.T) {
	b := startBroker(t, Config{unit: testSecond, SessionSweepInterval: 5 * time.Millisecond})
	sub := willSubscriber(t, b, "will-expire-sub", "will/#")

	// A 400 ms delay on a session that expires after 60 ms.
	client, _ := mustConnect(t, b.Addr(), willConnect("will-expire", "will/expire", "gone", 20, 3))
	start := time.Now()
	_ = client.conn.Close()

	got := expectPublish(t, sub)
	elapsed := time.Since(start)
	if string(got.Payload) != "gone" {
		t.Fatalf("will delivery = %q, want gone", got.Payload)
	}
	if elapsed > 250*time.Millisecond {
		t.Fatalf("will published after %v, want about the 60 ms session expiry rather than the 400 ms delay", elapsed)
	}
	if elapsed < 2*testSecond {
		t.Fatalf("will published after %v, want it to wait for the session expiry (about %v)", elapsed, 3*testSecond)
	}
}

// TestWillDelayClampedByConfig pins the configured bound: a delay above
// Config.MaxWillDelayInterval is published at the bound and counted.
func TestWillDelayClampedByConfig(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{unit: testSecond, MaxWillDelayInterval: 50 * time.Millisecond, Metrics: metrics})
	sub := willSubscriber(t, b, "will-clamp-sub", "will/#")

	client, _ := mustConnect(t, b.Addr(), willConnect("will-clamp", "will/clamp", "gone", 50, 1000)) // 1 s requested
	start := time.Now()
	_ = client.conn.Close()

	got := expectPublish(t, sub)
	elapsed := time.Since(start)
	if string(got.Payload) != "gone" {
		t.Fatalf("will delivery = %q, want gone", got.Payload)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("will published after %v, want about the 50 ms bound", elapsed)
	}
	if got := metrics.overflows("will_delay"); got == 0 {
		t.Fatal("the clamped will delay was not counted as an overflow")
	}
}

// TestWillDelayIgnoredWhenSessionEnds pins the other half of §3.1.3.2.2: a session
// that ends with its connection has no time to delay into, so a clean session
// publishes its will at once even when it asked for a delay.
func TestWillDelayIgnoredWhenSessionEnds(t *testing.T) {
	b := startBroker(t, Config{unit: testSecond})
	sub := willSubscriber(t, b, "will-clean-sub", "will/#")

	clean := willConnect("will-clean", "will/clean", "gone", 50, 0) // 1 s delay, no session expiry
	clean.clean = true
	client, _ := mustConnect(t, b.Addr(), clean)
	start := time.Now()
	_ = client.conn.Close()

	got := expectPublish(t, sub)
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("will of a session that ended with its connection published after %v, want it immediately", elapsed)
	}
	if string(got.Payload) != "gone" {
		t.Fatalf("will delivery = %q, want gone", got.Payload)
	}
}
