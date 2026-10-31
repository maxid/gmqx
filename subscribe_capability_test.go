package gmqx

// Capabilities the CONNACK advertises as unavailable must be refused, not
// silently accepted. props.go advertises exactly two of them: Shared Subscription
// Available 0 and Subscription Identifier Available 0 (Retain, Wildcard
// Subscriptions and Topic Alias are advertised as available and are implemented).
// These tests pin the refusal of each.

import (
	"strings"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// TestSharedSubscriptionRejected pins that a "$share/..." filter is answered with
// the reason code that names the missing capability — per filter, in the SUBACK,
// as MQTT 5 §3.9 requires — and is not stored as a literal filter that could
// never match.
func TestSharedSubscriptionRejected(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics})

	t.Run("mqtt5", func(t *testing.T) {
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "share-v5", clean: true})
		sub := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "$share/g/t", QoS: 1})
		if len(sub.ReasonCodes) != 1 || sub.ReasonCodes[0] != 0x9E {
			t.Fatalf("SUBACK codes %#v for \"$share/g/t\", want [0x9E] (Shared Subscriptions not supported)", sub.ReasonCodes)
		}
		// The refusal is per filter and does not end the connection: a plain
		// filter in the same SUBSCRIBE is still granted.
		sub = subscribe(t, c, 2, mqtt.Properties{},
			mqtt.Subscription{Filter: "g/t", QoS: 1},
			mqtt.Subscription{Filter: "$share/g/t", QoS: 1},
		)
		if len(sub.ReasonCodes) != 2 || sub.ReasonCodes[0] != 1 || sub.ReasonCodes[1] != 0x9E {
			t.Fatalf("SUBACK codes %#v, want [1 0x9E]", sub.ReasonCodes)
		}
		c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
		readConf[*mqtt.Pingresp](t, c)

		// The shared filter is not part of the session: a publish to its literal
		// text reaches nobody, and the routing summary never advertises it.
		pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "share-v5-pub", clean: true})
		pub.publish(confPublish{topic: "$share/g/t", payload: "literal", qos: 0})
		expectNoMessage(t, c, 150*time.Millisecond)
		for _, filter := range b.subscriptionFilters() {
			if strings.HasPrefix(filter, "$share/") {
				t.Fatalf("routing summary advertises the rejected shared filter %q", filter)
			}
		}
	})

	t.Run("mqtt311", func(t *testing.T) {
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version311, clientID: "share-v311", clean: true})
		sub := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "$share/g/t", QoS: 0})
		if len(sub.ReasonCodes) != 1 || sub.ReasonCodes[0] != 0x80 {
			t.Fatalf("SUBACK codes %#v for \"$share/g/t\", want [0x80]", sub.ReasonCodes)
		}
		c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}})
		readConf[*mqtt.Pingresp](t, c)
	})

	if got := metrics.count("shared_subscription"); got != 3 {
		t.Fatalf("shared_subscription drops = %d, want 3", got)
	}
}

// TestSubscriptionIdentifierRejected pins the other advertised capability: a
// SUBSCRIBE that carries a Subscription Identifier asks for something the CONNACK
// said is unavailable, so MQTT 5 answers with DISCONNECT 0x9A and closes.
//
// MQTT 3.1.1 has no SUBSCRIBE properties at all, so there is nothing to refuse
// there and the broker cannot be asked the question.
func TestSubscriptionIdentifierRejected(t *testing.T) {
	b := startBroker(t, Config{})
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "sub-id", clean: true})
	c.write(&mqtt.Subscribe{
		MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5},
		PacketID:      1,
		Properties:    mqtt.Properties{SubscriptionIdentifiers: []uint32{7}},
		Subscriptions: []mqtt.Subscription{{Filter: "si/x", QoS: 0}},
	})
	expectDisconnect(t, c, 0x9A)
	waitDisconnected(t, b, "sub-id")
	if filters := b.subscriptionFilters(); len(filters) != 0 {
		t.Fatalf("rejected SUBSCRIBE left filters behind: %v", filters)
	}
}
