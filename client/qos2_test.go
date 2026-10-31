package client_test

import (
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/mqtt"
)

// TestClientQoS2Handshake asserts the QoS 2 exchange from both sides: the broker
// sees exactly one QoS 2 PUBLISH from the publisher, the publisher's Publish
// returns only after PUBCOMP, and the subscriber — whose wire PUBLISH is
// duplicated by the proxy with DUP set — receives the message exactly once and
// completes the inbound PUBREC/PUBREL/PUBCOMP handshake.
func TestClientQoS2Handshake(t *testing.T) {
	sink := &recordingSink{}
	b := startBroker(t, gmqx.Config{Sink: sink})
	ctx := testContext(t)

	proxy := newTestProxy(t, b.Addr())
	proxy.duplicateQoS2.Store(true)

	rec := newRecorder(4)
	sub := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Dialer:     proxy.DialContext,
		ClientID:   "qos2-sub",
		Version:    mqtt.Version5,
		CleanStart: true,
		OnMessage:  rec.OnMessage,
	})
	codes, err := sub.Subscribe(ctx, client.Subscription{Filter: "qos2/topic", QoS: 2})
	if err != nil || len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("Subscribe = %v, %v; want granted QoS 2", codes, err)
	}

	pub := dialClient(t, client.Config{Addr: b.Addr(), ClientID: "qos2-pub", Version: mqtt.Version5, CleanStart: true})
	if err := pub.Publish(ctx, "qos2/topic", []byte("exactly-once"), 2, false); err != nil {
		t.Fatalf("Publish(qos2): %v", err)
	}

	msg := rec.next(t)
	if msg.QoS != 2 || string(msg.Payload) != "exactly-once" {
		t.Fatalf("delivery = QoS %d %q", msg.QoS, msg.Payload)
	}

	// The proxy duplicated the PUBLISH on the wire; a correct deduplication by
	// packet id means the callback still ran once.
	rec.none(t, 400*time.Millisecond)

	if got := sink.count(); got != 1 {
		t.Fatalf("the broker accepted %d publishes, want 1", got)
	}
	event := sink.event(0)
	if event.QoS != 2 || event.Topic != "qos2/topic" {
		t.Fatalf("broker-side publish = QoS %d on %q, want QoS 2 on qos2/topic", event.QoS, event.Topic)
	}
	if !sub.Connected() || !pub.Connected() {
		t.Fatal("a QoS 2 handshake must leave both connections up")
	}
}
