package gmqx

// Subscription identifier tests (MQTT 5 §3.8.2.1.2, §3.8.3.1, §3.3.2.3.8): the
// identifier a SUBSCRIBE carries is state of the subscription, is stored with it,
// and is copied into every PUBLISH that subscription causes — including a retained
// replay, an offline-queue drain and a shared-group delivery.

import (
	"testing"

	"github.com/maxid/gnety/codec/mqtt"
)

// subscribeID subscribes one filter with the given subscription identifiers and
// requires the broker to grant qos.
func subscribeID(t *testing.T, c *confClient, pid uint16, filter string, qos byte, ids ...uint32) {
	t.Helper()
	codes := subscribe(t, c, pid, mqtt.Properties{SubscriptionIdentifiers: ids},
		mqtt.Subscription{Filter: filter, QoS: qos}).ReasonCodes
	if len(codes) != 1 || codes[0] != qos {
		t.Fatalf("SUBACK codes %v for %q, want [%d]", codes, filter, qos)
	}
}

// TestSubscriptionIdentifierTagsDeliveries is the basic association: a SUBSCRIBE
// carrying an identifier tags every delivery the subscription causes.
func TestSubscriptionIdentifierTagsDeliveries(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-one", clean: true})
	subscribeID(t, sub, 1, "si/one/#", 1, 7)

	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-one-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "si/one/x", payload: "tagged", qos: 1, pid: 1})
	got := expectPublish(t, sub)
	if string(got.Payload) != "tagged" {
		t.Fatalf("delivery = %q", got.Payload)
	}
	if ids := got.Properties.SubscriptionIdentifiers; len(ids) != 1 || ids[0] != 7 {
		t.Fatalf("subscription identifiers = %v, want [7]", ids)
	}
	// A second publication is tagged the same way: the identifier belongs to the
	// subscription, not to one publication.
	pub.mustPublish(t, confPublish{topic: "si/one/y", payload: "again", qos: 1, pid: 2})
	if ids := expectPublish(t, sub).Properties.SubscriptionIdentifiers; len(ids) != 1 || ids[0] != 7 {
		t.Fatalf("second delivery identifiers = %v, want [7]", ids)
	}
}

// TestSubscriptionIdentifierMergesSubscriptions pins §3.3.2.3.8: one PUBLISH that
// matches several subscriptions of a session carries the identifier of each,
// deduplicated, and a subscription without an identifier adds nothing.
func TestSubscriptionIdentifierMergesSubscriptions(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-merge", clean: true})
	subscribeID(t, sub, 1, "si/merge/#", 1, 1)
	subscribeID(t, sub, 2, "si/merge/x", 1, 2)
	// The same identifier on a third matching subscription must not be repeated.
	subscribeID(t, sub, 3, "si/+/x", 1, 1)
	// A matching subscription without an identifier contributes none.
	if codes := subscribe(t, sub, 4, mqtt.Properties{}, mqtt.Subscription{Filter: "+/merge/x", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v for the untagged subscription", codes)
	}

	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-merge-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "si/merge/x", payload: "both", qos: 1, pid: 1})
	ids := expectPublish(t, sub).Properties.SubscriptionIdentifiers
	if len(ids) != 2 || !sameIDs(ids, []uint32{1, 2}) {
		t.Fatalf("subscription identifiers = %v, want [1 2] once each", ids)
	}
}

// TestSubscriptionIdentifierRetainedReplay pins that a retained replay is a delivery
// like any other: the subscription's identifier is on it.
func TestSubscriptionIdentifierRetainedReplay(t *testing.T) {
	b := startBroker(t, Config{})
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-ret-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "si/ret/x", payload: "stored", qos: 1, pid: 1, retain: true})

	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-ret-sub", clean: true})
	subscribeID(t, sub, 1, "si/ret/#", 1, 42)
	replay := expectPublish(t, sub)
	if string(replay.Payload) != "stored" || !replay.Retain {
		t.Fatalf("retained replay = %q retain=%v", replay.Payload, replay.Retain)
	}
	if ids := replay.Properties.SubscriptionIdentifiers; len(ids) != 1 || ids[0] != 42 {
		t.Fatalf("retained replay identifiers = %v, want [42]", ids)
	}
}

// TestSubscriptionIdentifierOfflineQueue pins that a message queued for a
// persistent session while it is away carries the identifier into the drain after
// the next CONNACK.
func TestSubscriptionIdentifierOfflineQueue(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{
		version: mqtt.Version5, clientID: "si-offline", clean: false, props: expiryProps(60),
	})
	subscribeID(t, sub, 1, "si/offline/#", 1, 21)
	sub.disconnect()
	waitDisconnected(t, b, "si-offline")
	waitFor(t, "the session to be stored", func() bool {
		_, ok := b.store.Load("si-offline")
		return ok
	})

	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-offline-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "si/offline/x", payload: "queued", qos: 1, pid: 1})

	back, ack := mustConnect(t, b.Addr(), confConnect{
		version: mqtt.Version5, clientID: "si-offline", clean: false, props: expiryProps(60),
	})
	if !ack.SessionPresent {
		t.Fatal("the resumed session does not report Session Present")
	}
	queued := expectPublish(t, back)
	if string(queued.Payload) != "queued" {
		t.Fatalf("queued delivery = %q", queued.Payload)
	}
	if ids := queued.Properties.SubscriptionIdentifiers; len(ids) != 1 || ids[0] != 21 {
		t.Fatalf("queued delivery identifiers = %v, want [21]", ids)
	}
}

// TestSubscriptionIdentifierSurvivesResume pins the round trip through the session
// store: the identifier is written with the stored subscription, read back on the
// resume, and still tags the deliveries of the resumed session.
func TestSubscriptionIdentifierSurvivesResume(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{
		version: mqtt.Version5, clientID: "si-resume", clean: false, props: expiryProps(60),
	})
	subscribeID(t, sub, 1, "si/resume/#", 1, 11)
	sub.disconnect()
	waitDisconnected(t, b, "si-resume")
	waitFor(t, "the session to be stored", func() bool {
		_, ok := b.store.Load("si-resume")
		return ok
	})

	stored, ok := b.store.Load("si-resume")
	if !ok {
		t.Fatal("the session was not stored")
	}
	keep, ok := stored.Subscriptions["si/resume/#"]
	if !ok {
		t.Fatalf("the stored session lost the subscription: %+v", stored.Subscriptions)
	}
	if len(keep.SubscriptionIdentifiers) != 1 || keep.SubscriptionIdentifiers[0] != 11 {
		t.Fatalf("stored subscription identifiers = %v, want [11]", keep.SubscriptionIdentifiers)
	}

	back, ack := mustConnect(t, b.Addr(), confConnect{
		version: mqtt.Version5, clientID: "si-resume", clean: false, props: expiryProps(60),
	})
	if !ack.SessionPresent {
		t.Fatal("the resumed session does not report Session Present")
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-resume-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "si/resume/x", payload: "after", qos: 1, pid: 1})
	if ids := expectPublish(t, back).Properties.SubscriptionIdentifiers; len(ids) != 1 || ids[0] != 11 {
		t.Fatalf("identifiers after the resume = %v, want [11]", ids)
	}
}

// TestSubscriptionIdentifierZeroIsAProtocolError pins §3.8.2.1.2: a Subscription
// Identifier of 0 is a Protocol Error, answered with DISCONNECT 0x82. The codec
// refuses the value while parsing (it is the only place the zero is visible, gmqx
// keeps no property table of its own), so the case is sent as a raw frame.
func TestSubscriptionIdentifierZeroIsAProtocolError(t *testing.T) {
	b := startBroker(t, Config{})
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-zero", clean: true})
	// SUBSCRIBE, packet id 1, property block [0x0B 0x00], filter "si/zero" at QoS 0.
	body := []byte{0, 1}
	body = append(body, 0x02, 0x0b, 0x00)
	body = mqtt.AppendString(body, "si/zero")
	body = append(body, 0)
	c.writeRaw(append(appendRemainingLength([]byte{0x82}, len(body)), body...))
	expectDisconnect(t, c, 0x82)
	waitDisconnected(t, b, "si-zero")
	if filters := b.subscriptionFilters(); len(filters) != 0 {
		t.Fatalf("a refused SUBSCRIBE left filters behind: %v", filters)
	}
}

// TestSubscriptionIdentifierDeliveryCap pins the documented bound: one PUBLISH
// carries at most maxSubscriptionIdentifiersPerDelivery identifiers, and the excess
// is counted instead of silently dropped.
func TestSubscriptionIdentifierDeliveryCap(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics})
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-cap", clean: true})
	// Ten distinct filters that all match "cap/a/b", one identifier each. The codec
	// allows one Subscription Identifier per SUBSCRIBE (§3.8.2.1.2), so the session
	// collects them across packets.
	filters := []string{
		"cap/a/b", "#", "cap/#", "cap/a/#", "cap/+/b",
		"+/a/b", "cap/a/+", "+/+/b", "cap/+/+", "+/a/+",
	}
	for i, filter := range filters {
		subscribeID(t, sub, uint16(i+1), filter, 1, uint32(i+1))
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-cap-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "cap/a/b", payload: "many", qos: 1, pid: 1})

	ids := expectPublish(t, sub).Properties.SubscriptionIdentifiers
	if len(ids) != maxSubscriptionIdentifiersPerDelivery {
		t.Fatalf("identifiers on one delivery = %d (%v), want the documented cap of %d",
			len(ids), ids, maxSubscriptionIdentifiersPerDelivery)
	}
	seen := make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 || id > uint32(len(filters)) {
			t.Fatalf("delivery carries an identifier that is not subscribed: %v", ids)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("delivery repeats an identifier: %v", ids)
		}
		seen[id] = struct{}{}
	}
	if got := metrics.count("subscription_identifier_limit"); got != 1 {
		t.Fatalf("subscription_identifier_limit drops = %d, want 1 for the capped delivery", got)
	}
}

// TestSubscriptionIdentifierSharedGroup pins that a shared subscription may carry an
// identifier and that it applies to the delivery of the member the group picks.
func TestSubscriptionIdentifierSharedGroup(t *testing.T) {
	b := startBroker(t, Config{})
	first, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-shared-1", clean: true})
	second, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-shared-2", clean: true})
	subscribeID(t, first, 1, "$share/g/si/shared", 1, 21)
	subscribeID(t, second, 1, "$share/g/si/shared", 1, 22)

	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-shared-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "si/shared", payload: "one", qos: 1, pid: 1})
	one := expectPublish(t, first)
	if ids := one.Properties.SubscriptionIdentifiers; len(ids) != 1 || ids[0] != 21 {
		t.Fatalf("first member identifiers = %v, want [21]", ids)
	}
	pub.mustPublish(t, confPublish{topic: "si/shared", payload: "two", qos: 1, pid: 2})
	two := expectPublish(t, second)
	if ids := two.Properties.SubscriptionIdentifiers; len(ids) != 1 || ids[0] != 22 {
		t.Fatalf("second member identifiers = %v, want [22]", ids)
	}
}

// TestSubscriptionIdentifierFromPublisherIsAProtocolError pins MQTT 5 §3.3.4: a
// PUBLISH from a client must not carry a Subscription Identifier. The broker used
// to strip it silently, which let a publisher smuggle a delivery tag past the
// protocol; it is a protocol error and now ends the connection with 0x82.
func TestSubscriptionIdentifierFromPublisherIsAProtocolError(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-hop", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "si/hop", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "si-hop-pub", clean: true})
	pub.write(&mqtt.Publish{
		MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5},
		Topic:         "si/hop",
		Payload:       []byte("from-publisher"),
		QoS:           1,
		PacketID:      9,
		Properties:    mqtt.Properties{SubscriptionIdentifiers: []uint32{99}},
	})
	expectDisconnect(t, pub, 0x82)
}

// sameIDs reports whether got holds exactly the values of want, in any order.
func sameIDs(got, want []uint32) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[uint32]int, len(got))
	for _, id := range got {
		seen[id]++
	}
	for _, id := range want {
		if seen[id] != 1 {
			return false
		}
	}
	return true
}
