package gmqx

// The inflight queue is the second half of the outbound window: a QoS 1/2
// delivery that finds the window full waits for a free slot instead of being
// dropped, is sent in arrival order when the acknowledgement of an older message
// frees one, and is only dropped when the queue itself is full — a drop that is
// counted twice and logged with the client id, so a lost delivery can never go
// unnoticed.

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// logCapture collects the log output of one broker so a test can assert on the
// record of a drop.
type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logCapture) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logCapture) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// captureLogger returns a logger writing into a capture buffer.
func captureLogger() (*slog.Logger, *logCapture) {
	capture := &logCapture{}
	return slog.New(slog.NewTextHandler(capture, &slog.HandlerOptions{Level: slog.LevelDebug})), capture
}

// TestInflightQueueDeliversInOrder is the core of the queued-window behaviour: a
// window of two accepts two deliveries, the rest wait in the connection's queue,
// and every PUBACK releases exactly the next message — the subscriber observes
// the publication order, never a reordering and never a gap.
func TestInflightQueueDeliversInOrder(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics, MaxInflightQoS1: 2, MaxInflightQueueSize: 8})

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "fifo-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "fifo/x", QoS: 1})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "fifo-pub", clean: true})

	const messages = 6
	for i := 1; i <= messages; i++ {
		// The publisher is acknowledged for every message: the window of the
		// subscriber never blocks it.
		pub.mustPublish(t, confPublish{topic: "fifo/x", payload: fmt.Sprintf("m%d", i), qos: 1, pid: uint16(i)})
	}

	// One acknowledgement in, one delivery out: the order is the publication order.
	for i := 1; i <= messages; i++ {
		got := expectPublish(t, sub)
		if want := fmt.Sprintf("m%d", i); string(got.Payload) != want {
			t.Fatalf("delivery %d = %q, want %q", i, got.Payload, want)
		}
		if got.QoS != 1 || got.PacketID == 0 || got.Dup {
			t.Fatalf("delivery %q = qos%d pid%d dup%v, want a fresh QoS 1 PUBLISH", got.Payload, got.QoS, got.PacketID, got.Dup)
		}
		sub.write(&mqtt.Puback{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: got.PacketID})
	}
	expectNoMessage(t, sub, 100*time.Millisecond)
	if n := metrics.count("inflight_overflow"); n != 0 {
		t.Fatalf("inflight_overflow drops = %d, want 0: a window of 2 with a queue of 8 loses nothing", n)
	}
}

// TestInflightQueueOverflowDropsWithMetricAndLog pins the only case that may lose
// a QoS 1/2 delivery: the window and the queue behind it are both full. The drop
// is counted twice and logged with the client id, because the publisher was
// already acknowledged and nothing else would make the loss visible.
func TestInflightQueueOverflowDropsWithMetricAndLog(t *testing.T) {
	metrics := newDeliveryMetrics()
	logger, logs := captureLogger()
	b := startBroker(t, Config{
		Metrics:              metrics,
		Logger:               logger,
		MaxInflightQoS1:      1,
		MaxInflightQueueSize: 2,
	})

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "overflow/x", QoS: 1})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "overflow-pub", clean: true})

	// One delivery occupies the window, two wait in the queue, the fourth finds
	// both full.
	for i := 1; i <= 4; i++ {
		pub.mustPublish(t, confPublish{topic: "overflow/x", payload: fmt.Sprintf("m%d", i), qos: 1, pid: uint16(i)})
	}

	got := expectPublish(t, sub)
	if string(got.Payload) != "m1" {
		t.Fatalf("first delivery = %q, want m1", got.Payload)
	}
	if n := metrics.count("inflight_overflow"); n != 1 {
		t.Fatalf("inflight_overflow drops = %d, want 1", n)
	}
	if n := metrics.overflows("inflight"); n < 1 {
		t.Fatalf("overflow(\"inflight\") = %d, want at least 1", n)
	}
	logged := logs.String()
	if !strings.Contains(logged, "inflight_overflow") || !strings.Contains(logged, "overflow-sub") {
		t.Fatalf("the drop was not logged with the reason and the client id: %q", logged)
	}

	// The queued messages are still delivered in order once the window opens; the
	// dropped one is gone.
	sub.write(&mqtt.Puback{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: got.PacketID})
	var drained []string
	for i := 2; i <= 3; i++ {
		got := expectPublish(t, sub)
		drained = append(drained, string(got.Payload))
		sub.write(&mqtt.Puback{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: got.PacketID})
	}
	if len(drained) != 2 || drained[0] != "m2" || drained[1] != "m3" {
		t.Fatalf("drained deliveries = %v, want [m2 m3]", drained)
	}
	expectNoMessage(t, sub, 100*time.Millisecond)
}

// TestInflightQueueIsPerQoS pins that the two windows are independent: a full
// QoS 1 window does not hold back a QoS 2 delivery that has a slot of its own.
func TestInflightQueueIsPerQoS(t *testing.T) {
	b := startBroker(t, Config{MaxInflightQoS1: 1, MaxInflightQoS2: 1})

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "perqos-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "perqos/#", QoS: 2})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "perqos-pub", clean: true})

	// Fill the QoS 1 window and queue one more QoS 1 delivery behind it.
	pub.mustPublish(t, confPublish{topic: "perqos/one", payload: "q1-a", qos: 1, pid: 1})
	pub.mustPublish(t, confPublish{topic: "perqos/one", payload: "q1-b", qos: 1, pid: 2})
	first := expectPublish(t, sub)
	if first.QoS != 1 || string(first.Payload) != "q1-a" {
		t.Fatalf("first delivery = qos%d %q, want the QoS 1 one", first.QoS, first.Payload)
	}

	// The QoS 2 window is still empty, so a QoS 2 publication goes out now, ahead
	// of the QoS 1 delivery that waits for its own window.
	pub.publish(confPublish{topic: "perqos/two", payload: "q2-a", qos: 2, pid: 3})
	expectPubrec(t, pub, 3, 0)
	pub.write(&mqtt.Pubrel{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: 3})
	expectPubcomp(t, pub, 3)

	got := expectPublish(t, sub)
	if got.QoS != 2 || string(got.Payload) != "q2-a" {
		t.Fatalf("delivery while the QoS 1 window is full = qos%d %q, want the QoS 2 one", got.QoS, got.Payload)
	}
	sub.write(&mqtt.Pubrec{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: got.PacketID})
	expectPubrelWithin(t, sub, got.PacketID, 2*time.Second)
	sub.write(&mqtt.Pubcomp{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: got.PacketID})

	// The QoS 1 queue drains once its own window opens.
	sub.write(&mqtt.Puback{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: first.PacketID})
	if queued := expectPublish(t, sub); string(queued.Payload) != "q1-b" {
		t.Fatalf("queued QoS 1 delivery = %q, want q1-b", queued.Payload)
	}
}
