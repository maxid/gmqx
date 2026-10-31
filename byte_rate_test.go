package gmqx

// Byte-rate shaping.
//
// Config.MaxReadByteRate/MaxWriteByteRate bound one connection and
// MaxTotalReadByteRate/MaxTotalWriteByteRate bound the whole broker. The policy of
// the excess is delay: a transfer waits for its tokens, every byte still arrives in
// order, and the delayed bytes are counted (ByteRateMetrics.RecordThrottledBytes).
// The defaults (8 MiB/s read, 16 MiB/s write per connection and 256 MiB/s each in
// total) are high enough that normal traffic is not shaped at all, which the last
// case pins.

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// shapingMetrics is a MetricsManager that also implements the optional
// ByteRateMetrics extension, so a case can assert on the bytes that had to wait.
type shapingMetrics struct {
	*deliveryMetrics
	mu        sync.Mutex
	throttled map[string]int
}

func newShapingMetrics() *shapingMetrics {
	return &shapingMetrics{deliveryMetrics: newDeliveryMetrics(), throttled: make(map[string]int)}
}

func (m *shapingMetrics) RecordThrottledBytes(direction string, n int) {
	m.mu.Lock()
	m.throttled[direction] += n
	m.mu.Unlock()
}

func (m *shapingMetrics) throttledBytes(direction string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.throttled[direction]
}

// ratePayload is comfortably above the one-second burst of the limits below
// (64 KiB), so the excess — and therefore the delay — is unambiguous.
const ratePayload = 96 << 10

// TestByteRateReadShaping delays the bytes a client sends: the broker's read loop
// spends the tokens the bucket has and slows the peer down, while the whole
// publication still arrives intact.
func TestByteRateReadShaping(t *testing.T) {
	metrics := newShapingMetrics()
	b := startBroker(t, Config{Metrics: metrics, MaxReadByteRate: 64 << 10})
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "rate-read-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "rate/read", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "rate-read-pub", clean: true})
	payload := strings.Repeat("r", ratePayload)

	start := time.Now()
	pub.publish(confPublish{topic: "rate/read", payload: payload, qos: 0})
	got := expectPublish(t, sub)
	elapsed := time.Since(start)

	if string(got.Payload) != payload {
		t.Fatalf("delivery was altered: %d bytes, want %d", len(got.Payload), len(payload))
	}
	if elapsed < 250*time.Millisecond {
		t.Fatalf("an over-rate publish was delivered after %v, want the shaping delay (32 KiB over a 64 KiB/s bucket)", elapsed)
	}
	if throttled := metrics.throttledBytes("read"); throttled < 16<<10 {
		t.Fatalf("throttled read bytes = %d, want at least the 16 KiB the bucket could not pay for", throttled)
	}
	if n := metrics.overflows("byte_rate_read"); n == 0 {
		t.Fatal("the read shaping wait was not counted as a limit hit")
	}
}

// TestByteRateWriteShaping delays the bytes the broker sends to one connection.
func TestByteRateWriteShaping(t *testing.T) {
	metrics := newShapingMetrics()
	b := startBroker(t, Config{Metrics: metrics, MaxWriteByteRate: 64 << 10})
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "rate-write-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "rate/write", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "rate-write-pub", clean: true})
	payload := strings.Repeat("w", ratePayload)

	start := time.Now()
	pub.publish(confPublish{topic: "rate/write", payload: payload, qos: 0})
	got := expectPublish(t, sub)
	elapsed := time.Since(start)

	if string(got.Payload) != payload {
		t.Fatalf("delivery was altered: %d bytes, want %d", len(got.Payload), len(payload))
	}
	if elapsed < 250*time.Millisecond {
		t.Fatalf("an over-rate delivery was written after %v, want the shaping delay", elapsed)
	}
	if throttled := metrics.throttledBytes("write"); throttled < 32<<10 {
		t.Fatalf("throttled write bytes = %d, want at least the excess over the 64 KiB burst", throttled)
	}
	if n := metrics.overflows("byte_rate_write"); n == 0 {
		t.Fatal("the write shaping wait was not counted as a limit hit")
	}
}

// TestByteRateGlobalLimit pins the broker-wide bucket: a per-connection limit that
// is effectively unlimited does not help when the global one is exhausted.
func TestByteRateGlobalLimit(t *testing.T) {
	metrics := newShapingMetrics()
	b := startBroker(t, Config{Metrics: metrics, MaxTotalReadByteRate: 64 << 10})
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "rate-global-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "rate/global", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "rate-global-pub", clean: true})
	payload := strings.Repeat("g", ratePayload)

	start := time.Now()
	pub.publish(confPublish{topic: "rate/global", payload: payload, qos: 0})
	got := expectPublish(t, sub)
	elapsed := time.Since(start)

	if string(got.Payload) != payload {
		t.Fatalf("delivery was altered: %d bytes, want %d", len(got.Payload), len(payload))
	}
	if elapsed < 250*time.Millisecond {
		t.Fatalf("an over-rate publish was delivered after %v with a 64 KiB/s global read limit", elapsed)
	}
	if throttled := metrics.throttledBytes("read"); throttled < 16<<10 {
		t.Fatalf("throttled read bytes = %d, want at least the 16 KiB over the global burst", throttled)
	}
}

// TestByteRateDefaultsDoNotThrottle is the other half of the policy: with the
// documented defaults the same 96 KiB publication is not delayed and not counted as
// throttled, so ordinary traffic pays nothing for the feature.
func TestByteRateDefaultsDoNotThrottle(t *testing.T) {
	metrics := newShapingMetrics()
	b := startBroker(t, Config{Metrics: metrics})
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "rate-default-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "rate/default", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "rate-default-pub", clean: true})
	payload := strings.Repeat("d", ratePayload)

	start := time.Now()
	pub.publish(confPublish{topic: "rate/default", payload: payload, qos: 0})
	got := expectPublish(t, sub)
	elapsed := time.Since(start)

	if string(got.Payload) != payload {
		t.Fatalf("delivery was altered: %d bytes, want %d", len(got.Payload), len(payload))
	}
	if elapsed > 2*time.Second {
		t.Fatalf("a default-configured broker delayed %d bytes by %v", len(payload), elapsed)
	}
	if read, write := metrics.throttledBytes("read"), metrics.throttledBytes("write"); read != 0 || write != 0 {
		t.Fatalf("default limits throttled %d read and %d write bytes, want none", read, write)
	}
}
