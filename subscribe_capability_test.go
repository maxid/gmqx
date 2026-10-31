package gmqx

// Capabilities the CONNACK advertises must be real. props.go advertises Retain,
// Wildcard Subscriptions, Subscription Identifiers and Shared Subscriptions as
// available, and all four are implemented. This file pins the two that used to be
// refused — a "$share/..." filter and a SUBSCRIBE carrying a Subscription
// Identifier — from the capability side: granted, not refused, and with the
// connection still usable afterwards. The routing and property behaviour of both
// lives in shared_subscription_test.go, subscription_identifier_test.go and
// shared_cluster_test.go.

import (
	"testing"

	"github.com/maxid/gnety/codec/mqtt"
)

// TestSharedSubscriptionCapability pins that a shared subscription is a real
// subscription on both protocol versions: the filter is granted with the requested
// QoS, and the routing summary advertises the group (never the filter as a literal
// topic that could never match).
func TestSharedSubscriptionCapability(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics})

	// MQTT 5: granted, and the group is part of the advertised summary.
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "cap-v5", clean: true})
	if codes := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "$share/g/cap/t", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %#v for \"$share/g/cap/t\", want [1]", codes)
	}
	if groups := b.sharedSubscriptionGroups(); len(groups) != 1 || groups[0] != "$share/g/cap/t" {
		t.Fatalf("advertised shared groups = %v, want the subscribed group", groups)
	}
	for _, filter := range b.subscriptionFilters() {
		if filter == "$share/g/cap/t" {
			t.Fatal("the routing summary advertises the shared filter as a normal one")
		}
	}

	// MQTT 3.1.1 has no shared subscription concept, but the filter syntax is the
	// broker's, so the subscription is granted there too (0 is the granted QoS; the
	// 0x80 failure code the old refusal used is gone).
	legacy, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version311, clientID: "cap-v311", clean: true})
	if codes := subscribe(t, legacy, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "$share/g/cap/t", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %#v for \"$share/g/cap/t\" on MQTT 3.1.1, want [0]", codes)
	}
	legacy.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}})
	readConf[*mqtt.Pingresp](t, legacy)

	// Nothing was refused, so nothing is counted as dropped.
	if got := metrics.count("shared_subscription"); got != 0 {
		t.Fatalf("shared_subscription drops = %d, want 0: accepted filters are not refusals", got)
	}
}

// TestSubscriptionIdentifierCapability pins the other advertised capability: a
// SUBSCRIBE carrying a Subscription Identifier is accepted (the DISCONNECT 0x9A
// of the unavailable-capability build is gone), the identifier is stored with the
// subscription, and the connection keeps working.
func TestSubscriptionIdentifierCapability(t *testing.T) {
	b := startBroker(t, Config{})
	c, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "cap-si", clean: true})
	if ack.Properties.SubscriptionIdentifierAvailable == nil || !*ack.Properties.SubscriptionIdentifierAvailable {
		t.Fatalf("CONNACK subscription identifier available = %v, want true", ack.Properties.SubscriptionIdentifierAvailable)
	}
	if codes := subscribe(t, c, 1, mqtt.Properties{SubscriptionIdentifiers: []uint32{7}},
		mqtt.Subscription{Filter: "si/cap", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %#v for a SUBSCRIBE carrying an identifier, want [0]", codes)
	}
	// The connection is still usable and the subscription is routable.
	c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	readConf[*mqtt.Pingresp](t, c)
	if filters := b.subscriptionFilters(); len(filters) != 1 || filters[0] != "si/cap" {
		t.Fatalf("routing summary = %v, want the accepted subscription", filters)
	}
}
