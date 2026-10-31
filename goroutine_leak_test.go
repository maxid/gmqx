package gmqx

// Leak test: connections that carry queued and inflight messages must not leave
// goroutines or timers behind. Every connection gets exactly one retransmission
// timer and the broker one session sweeper; both end with the connection and the
// broker. The test runs under -race with the rest of the suite, so the same churn
// also covers the concurrent access to the queue and the inflight table.

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// settledGoroutines returns the current goroutine count once it has been stable
// for a moment, which keeps unrelated stragglers of earlier tests out of the
// baseline.
func settledGoroutines() int {
	previous := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		time.Sleep(10 * time.Millisecond)
		current := runtime.NumGoroutine()
		if current == previous {
			return current
		}
		previous = current
	}
	return previous
}

// TestNoGoroutineLeakAfterConnectChurn drives repeated connect/disconnect cycles
// with queued and unconfirmed messages and then checks that the broker's goroutines
// are gone.
func TestNoGoroutineLeakAfterConnectChurn(t *testing.T) {
	before := settledGoroutines()

	b := startBroker(t, Config{
		QoSRetryInterval:     5 * time.Millisecond,
		QoSMaxRetries:        100,
		SessionSweepInterval: 5 * time.Millisecond,
		MaxInflightQoS1:      4,
	})
	const clients = 10
	for i := 0; i < clients; i++ {
		id := "leak-" + string(rune('a'+i))
		sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: id, clean: false})
		subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "leak/#", QoS: 1})
		sub.disconnect()
		waitDisconnected(t, b, id)
	}

	// Queue messages for every offline session.
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "leak-pub", clean: true})
	for i := 0; i < 2*clients; i++ {
		pub.mustPublish(t, confPublish{topic: "leak/x", payload: "queued", qos: 1, pid: uint16(i + 1)})
	}

	// Reconnect every session: the queue drains, the deliveries stay unconfirmed
	// and the retransmission timers fire while the connections are open.
	conns := make([]*confClient, 0, clients)
	for i := 0; i < clients; i++ {
		id := "leak-" + string(rune('a'+i))
		sub, ack := mustConnect(t, b.Addr(), confConnect{clientID: id, clean: false})
		if !ack.SessionPresent {
			t.Fatalf("session %q was not resumed", id)
		}
		conns = append(conns, sub)
	}
	for _, sub := range conns {
		for i := 0; i < 2; i++ {
			if got := expectPublish(t, sub); string(got.Payload) != "queued" {
				t.Fatalf("drained delivery = %q", got.Payload)
			}
		}
	}
	// Let at least one retransmission pass happen on every connection.
	time.Sleep(50 * time.Millisecond)

	// Close abruptly while exchanges are unconfirmed: the timer goroutines must
	// end with their connections.
	for _, sub := range conns {
		_ = sub.conn.Close()
	}
	for i := 0; i < clients; i++ {
		waitDisconnected(t, b, "leak-"+string(rune('a'+i)))
	}
	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("broker stop: %v", err)
	}

	// The broker's own goroutines are gone before Stop returned (the sweeper is
	// waited for); the connection goroutines need a moment to unwind.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines after the churn = %d, want at most %d", runtime.NumGoroutine(), before+2)
}
