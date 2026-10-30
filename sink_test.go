package gmqx

import (
	"context"
	"encoding/binary"
	"testing"
	"time"
)

// sinkFunc adapts a function to EventSink.
type sinkFunc func(context.Context, PublishEvent) PublishResult

func (sinkFunc) OnConnect(context.Context, ConnectInfo)          {}
func (sinkFunc) OnDisconnect(context.Context, string, error)     {}
func (sinkFunc) OnConnectionLost(context.Context, string, error) {}
func (sinkFunc) OnPing(context.Context, string)                  {}
func (f sinkFunc) OnPublish(ctx context.Context, ev PublishEvent) PublishResult {
	return f(ctx, ev)
}

func qos1Publish(topic string, pid uint16, payload string) (byte, []byte) {
	body := appendString(nil, topic)
	body = append(body, byte(pid>>8), byte(pid))
	body = append(body, payload...)
	return 0x32, body
}

// TestSinkAcceptedAcknowledges pins the H1 contract: the PUBACK is sent only
// after the sink accepted the message, and it carries the packet id.
func TestSinkAcceptedAcknowledges(t *testing.T) {
	var events []PublishEvent
	b := startBroker(t, Config{Sink: sinkFunc(func(_ context.Context, ev PublishEvent) PublishResult {
		events = append(events, ev)
		return PublishResult{Disposition: PublishAccepted}
	})})

	pub := dialTestClient(t, b.Addr(), "pub")
	header, body := qos1Publish("plant/a/temp", 7, "42")
	pub.writePacket(header, body)

	h, p, err := pub.readPacket()
	if err != nil {
		t.Fatalf("read puback: %v", err)
	}
	if h != 0x40 || len(p) != 2 || binary.BigEndian.Uint16(p) != 7 {
		t.Fatalf("puback = %#x %x", h, p)
	}
	if len(events) != 1 {
		t.Fatalf("sink saw %d events, want 1", len(events))
	}
	ev := events[0]
	if ev.Topic != "plant/a/temp" || string(ev.Payload) != "42" || ev.QoS != 1 || ev.PacketID != 7 || ev.Retain {
		t.Fatalf("event = %+v", ev)
	}
	if ev.ConnectionID == "" || ev.ClientID != "pub" {
		t.Fatalf("identity missing: %+v", ev)
	}
}

// TestSinkRetryableSendsNoAck pins the other half: MQTT 3.1.1 has no negative
// PUBACK, so a retryable verdict must stay silent instead of faking success.
func TestSinkRetryableSendsNoAck(t *testing.T) {
	b := startBroker(t, Config{Sink: sinkFunc(func(context.Context, PublishEvent) PublishResult {
		return PublishResult{Disposition: PublishRetryable}
	})})
	pub := dialTestClient(t, b.Addr(), "pub")
	_ = pub.conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))

	header, body := qos1Publish("plant/a/temp", 9, "1")
	pub.writePacket(header, body)

	if h, _, err := pub.readPacket(); err == nil {
		t.Fatalf("expected no PUBACK for a retryable publish, got header %#x", h)
	}
}

// TestSinkTimeoutIsRetryable bounds the wait: a sink that never answers must not
// turn into an acknowledgement.
func TestSinkTimeoutIsRetryable(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	b := startBroker(t, Config{
		PublishCallbackTimeout: 40 * time.Millisecond,
		Sink: sinkFunc(func(context.Context, PublishEvent) PublishResult {
			<-release
			return PublishResult{Disposition: PublishAccepted}
		}),
	})
	pub := dialTestClient(t, b.Addr(), "pub")
	_ = pub.conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))

	header, body := qos1Publish("plant/a/temp", 11, "1")
	pub.writePacket(header, body)

	if h, _, err := pub.readPacket(); err == nil {
		t.Fatalf("expected no PUBACK after the sink wait expired, got header %#x", h)
	}
}
