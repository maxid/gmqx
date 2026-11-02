package gmqx

import (
	"context"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"

	mqttclient "github.com/maxid/gmqx/client"
)

// This file closes the coverage gaps in the directed-publish and wildcard paths:
// the QoS 2 handshake end to end, MQTT 5 property propagation, the "overlapping
// subscriptions still deliver once" rule, the documented retained behaviour of a
// directed publish, and two wildcard cases that only exist once a real broker is
// involved (a wildcard inside a shared subscription, and a wildcard retained
// replay).

// TestPublishToClientQoS2 runs the full inbound QoS 2 exchange against a real
// client: the delivery must be confirmed (PUBREC/PUBREL/PUBCOMP) and delivered
// exactly once.
func TestPublishToClientQoS2(t *testing.T) {
	b := startBroker(t, Config{})

	deliveries := make(chan *mqttclient.Message, 4)
	c, err := mqttclient.Dial(context.Background(), mqttclient.Config{
		Addr:           b.Addr(),
		ClientID:       "direct-qos2",
		Version:        mqtt.Version5,
		CleanStart:     true,
		ConnectTimeout: 2 * time.Second,
		OnMessage:      func(m *mqttclient.Message) { deliveries <- m },
	})
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	// No subscription: a directed publish does not need one.
	pub := directPublication(b, "direct/qos2", "once", 2)
	if err := b.PublishToClient(context.Background(), "direct-qos2", pub); err != nil {
		t.Fatalf("PublishToClient QoS 2: %v", err)
	}

	select {
	case m := <-deliveries:
		if string(m.Payload) != "once" || m.QoS != 2 || m.Topic != "direct/qos2" {
			t.Fatalf("QoS 2 directed delivery = %+v", m)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("QoS 2 directed delivery never arrived")
	}

	// Exactly once: a second publication must be the only one that follows, and a
	// retransmission of the first must not appear.
	pub2 := directPublication(b, "direct/qos2", "second", 2)
	if err := b.PublishToClient(context.Background(), "direct-qos2", pub2); err != nil {
		t.Fatalf("PublishToClient QoS 2 (second): %v", err)
	}
	select {
	case m := <-deliveries:
		if string(m.Payload) != "second" {
			t.Fatalf("second delivery = %q, want the second publication (a duplicate of the first was delivered)", m.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second QoS 2 delivery never arrived")
	}
	select {
	case m := <-deliveries:
		t.Fatalf("duplicate delivery of %q for a QoS 2 publication", m.Payload)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestPublishToClientProperties pins that the MQTT 5 properties a caller sets on a
// directed publication reach the client unchanged.
func TestPublishToClientProperties(t *testing.T) {
	b := startBroker(t, Config{})
	target, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "direct-props", clean: true})

	if err := b.PublishToClient(context.Background(), "direct-props", Publication{
		Namespace: b.Namespace(),
		Topic:     "direct/props",
		Payload:   []byte("with-properties"),
		QoS:       1,
		Props:     publishPropsBlock(),
	}); err != nil {
		t.Fatalf("PublishToClient with properties: %v", err)
	}

	got := expectPublish(t, target)
	if string(got.Payload) != "with-properties" {
		t.Fatalf("payload = %q", got.Payload)
	}
	if got.Properties.ContentType != "text/plain" {
		t.Fatalf("content type = %q, want text/plain", got.Properties.ContentType)
	}
	if got.Properties.ResponseTopic != "direct/replies" {
		t.Fatalf("response topic = %q", got.Properties.ResponseTopic)
	}
	if len(got.Properties.UserProperties) != 1 ||
		got.Properties.UserProperties[0].Key != "trace" ||
		got.Properties.UserProperties[0].Value != "abc" {
		t.Fatalf("user properties = %+v", got.Properties.UserProperties)
	}
	if got.Properties.PayloadFormat == nil || *got.Properties.PayloadFormat != 1 {
		t.Fatalf("payload format = %v, want 1", got.Properties.PayloadFormat)
	}
	target.ack(got.PacketID)
}

// TestPublishToClientOverlappingSubscriptionsDeliverOnce pins that a directed
// publication is one delivery even when the target holds several subscriptions
// that would each match its topic.
func TestPublishToClientOverlappingSubscriptionsDeliverOnce(t *testing.T) {
	b := startBroker(t, Config{QoSRetryInterval: time.Minute})
	target, _ := mustConnect(t, b.Addr(), confConnect{clientID: "direct-overlap", clean: true})
	if codes := subscribe(t, target, 3, mqtt.Properties{},
		mqtt.Subscription{Filter: "overlap/#", QoS: 1},
		mqtt.Subscription{Filter: "overlap/+/x", QoS: 1},
		mqtt.Subscription{Filter: "overlap/a/x", QoS: 1},
	).ReasonCodes; len(codes) != 3 {
		t.Fatalf("SUBACK codes %v", codes)
	}

	if err := b.PublishToClient(context.Background(), "direct-overlap", directPublication(b, "overlap/a/x", "once", 1)); err != nil {
		t.Fatalf("PublishToClient: %v", err)
	}
	got := expectPublish(t, target)
	if string(got.Payload) != "once" {
		t.Fatalf("delivery = %q", got.Payload)
	}
	target.ack(got.PacketID)
	expectNoMessage(t, target, 150*time.Millisecond)
}

// TestDirectedPublishRetainIsDeliveredNotStored pins the documented deviation: a
// directed publication carries the RETAIN flag it was given but does not become
// retained state, because it targets one session rather than being a broadcast a
// future subscriber should see. The test exists so the behaviour cannot change
// silently.
func TestDirectedPublishRetainIsDeliveredNotStored(t *testing.T) {
	b := startBroker(t, Config{QoSRetryInterval: time.Minute})
	target, _ := mustConnect(t, b.Addr(), confConnect{clientID: "direct-retain", clean: true})

	pub := directPublication(b, "direct/retain", "flagged", 0)
	pub.Retain = true
	if err := b.PublishToClient(context.Background(), "direct-retain", pub); err != nil {
		t.Fatalf("PublishToClient: %v", err)
	}
	got := expectPublish(t, target)
	if !got.Retain {
		t.Fatal("a directed publish marked retained was delivered without the RETAIN flag")
	}
	if n := retainedCount(b); n != 0 {
		t.Fatalf("directed publish stored %d retained topics, want 0 (documented deviation)", n)
	}

	// A later subscriber therefore sees nothing.
	late, _ := mustConnect(t, b.Addr(), confConnect{clientID: "direct-retain-late", clean: true})
	subscribe(t, late, 4, mqtt.Properties{}, mqtt.Subscription{Filter: "direct/#", QoS: 0})
	expectNoMessage(t, late, 100*time.Millisecond)
}

// TestSharedSubscriptionWithWildcardReceives pins that a wildcard filter inside a
// shared subscription routes like any other shared filter.
func TestSharedSubscriptionWithWildcardReceives(t *testing.T) {
	b := startBroker(t, Config{QoSRetryInterval: time.Minute})
	member, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "shared-wild", clean: true})
	if codes := subscribe(t, member, 1, mqtt.Properties{},
		mqtt.Subscription{Filter: "$share/g/sport/+/result", QoS: 1},
	).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v for a shared wildcard filter", codes)
	}

	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "shared-wild-pub", clean: true})
	pub.publish(confPublish{topic: "sport/tennis/result", payload: "won", qos: 1, pid: 7})
	got := expectPublish(t, member)
	if string(got.Payload) != "won" {
		t.Fatalf("shared wildcard delivery = %q", got.Payload)
	}
	member.ack(got.PacketID)

	// A topic the wildcard does not match must not reach the group.
	pub.publish(confPublish{topic: "sport/tennis/live", payload: "nope", qos: 0})
	expectNoMessage(t, member, 150*time.Millisecond)
}

// TestWildcardRetainedReplay pins that a retained message published on a concrete
// topic reaches a later subscriber through a wildcard filter, and not one whose
// wildcard does not cover it.
func TestWildcardRetainedReplay(t *testing.T) {
	b := startBroker(t, Config{QoSRetryInterval: time.Minute})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "retain-pub", clean: true})
	pub.publish(confPublish{topic: "plant/a/temp", payload: "21", qos: 0, retain: true})

	hit, _ := mustConnect(t, b.Addr(), confConnect{clientID: "retain-hit", clean: true})
	subscribe(t, hit, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "plant/+/temp", QoS: 0})
	got := expectPublish(t, hit)
	if string(got.Payload) != "21" || !got.Retain {
		t.Fatalf("wildcard retained replay = %q retain=%v", got.Payload, got.Retain)
	}

	miss, _ := mustConnect(t, b.Addr(), confConnect{clientID: "retain-miss", clean: true})
	subscribe(t, miss, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "plant/+/humidity", QoS: 0})
	expectNoMessage(t, miss, 150*time.Millisecond)
}

func ptrByte(v byte) *byte { return &v }

// publishPropsBlock builds the MQTT 5 PUBLISH property block used by the
// property test: payload format, content type, response topic and one user
// property, prefixed with its varint length as the wire format requires.
func publishPropsBlock() []byte {
	var body []byte
	body = append(body, 0x01, 0x01) // payload format indicator = 1
	body = append(body, 0x03, 0x00, byte(len("text/plain")))
	body = append(body, "text/plain"...)
	body = append(body, 0x08, 0x00, byte(len("direct/replies")))
	body = append(body, "direct/replies"...)
	body = append(body, 0x26, 0x00, byte(len("trace")), 't', 'r', 'a', 'c', 'e')
	body = append(body, 0x00, byte(len("abc")), 'a', 'b', 'c')
	// Publication.Props carries the block alone: the encoder prepends the varint
	// length, so the prefix must not be repeated here.
	return body
}
