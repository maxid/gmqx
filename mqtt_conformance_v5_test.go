package gmqx

// MQTT 5 rows of the conformance matrix. The harness lives in
// mqtt_conformance_test.go; these cases only add the packets MQTT 5 introduces:
// property blocks, topic aliases, subscription options and reason codes.

import (
	"reflect"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

func testConformance5(t *testing.T) {
	t.Run("connect_properties", test5ConnectProperties)
	t.Run("publish_properties", test5PublishProperties)
	t.Run("subscribe_properties", test5SubscribeProperties)
	t.Run("connack_properties", test5ConnackProperties)
	t.Run("topic_alias", test5TopicAlias)
	t.Run("subscription_identifiers", test5SubscriptionIdentifiers)
	t.Run("subscription_options", test5SubscriptionOptions)
	t.Run("reason_codes", test5ReasonCodes)
	t.Run("session_expiry", test5SessionExpiry)
	t.Run("receive_maximum", test5ReceiveMaximum)
	t.Run("maximum_packet_size", test5MaximumPacketSize)
	t.Run("shared_subscriptions", test5SharedSubscriptions)
	t.Run("auth", test5Auth)
	t.Run("protocol_error_disconnect", test5ProtocolErrorDisconnect)
	t.Run("subscribe_invalid_filter", test5SubscribeInvalidFilter)
	t.Run("qos_downgrade", test5QoSDowngrade)
	t.Run("qos2_flow", test5QoS2Flow)
}

func boolPtr(v bool) *bool { return &v }

func test5ConnectProperties(t *testing.T) {
	b := startBroker(t, Config{})
	format := byte(1)
	props := mqtt.Properties{
		SessionExpiryInterval: 60,
		ReceiveMaximum:        10,
		MaximumPacketSize:     4096,
		TopicAliasMaximum:     5,
		RequestProblemInfo:    boolPtr(false),
		UserProperties:        []mqtt.UserProperty{{Key: "device", Value: "42"}},
	}
	c, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "v5-connect-props",
		clean:    false,
		props:    props,
	})
	if ack.SessionPresent {
		t.Fatal("a new MQTT 5 session reports Session Present")
	}
	_ = c

	// The will property block of CONNECT reaches subscribers as PUBLISH
	// properties when the will is published.
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-will-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "will/v5", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	willProps := mqtt.Properties{
		PayloadFormat:         &format,
		MessageExpiryInterval: 300,
		ContentType:           "text/plain",
		UserProperties:        []mqtt.UserProperty{{Key: "reason", Value: "offline"}},
	}
	willClient, _ := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "v5-will-client",
		clean:    true,
		will: &mqtt.Will{
			Topic:      "will/v5",
			Payload:    []byte("bye"),
			QoS:        0,
			Properties: willProps,
		},
	})
	_ = willClient.conn.Close()

	got := expectPublish(t, sub)
	if got.Topic != "will/v5" || string(got.Payload) != "bye" {
		t.Fatalf("v5 will delivery = %q %q", got.Topic, got.Payload)
	}
	wantPublishProps := mqtt.Properties{
		PayloadFormat:         &format,
		MessageExpiryInterval: 300,
		ContentType:           "text/plain",
		UserProperties:        []mqtt.UserProperty{{Key: "reason", Value: "offline"}},
	}
	if !reflect.DeepEqual(got.Properties, wantPublishProps) {
		t.Fatalf("will properties = %+v, want %+v", got.Properties, wantPublishProps)
	}
}

func test5PublishProperties(t *testing.T) {
	b := startBroker(t, Config{})
	format := byte(1)
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-props-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "props/x", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-props-pub", clean: true})

	props := mqtt.Properties{
		PayloadFormat:         &format,
		MessageExpiryInterval: 60,
		ContentType:           "application/json",
		ResponseTopic:         "props/reply",
		CorrelationData:       []byte{1, 2, 3},
		UserProperties:        []mqtt.UserProperty{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}},
	}
	pub.publish(confPublish{topic: "props/x", payload: `{"v":1}`, qos: 1, pid: 5, props: props})
	expectPuback(t, pub, 5, 0)

	got := expectPublish(t, sub)
	if got.Topic != "props/x" || string(got.Payload) != `{"v":1}` {
		t.Fatalf("v5 delivery = %q %q", got.Topic, got.Payload)
	}
	if !reflect.DeepEqual(got.Properties, props) {
		t.Fatalf("forwarded properties = %+v, want %+v", got.Properties, props)
	}

	// The same publish to an MQTT 3.1.1 subscriber carries no property block and
	// therefore the unchanged payload.
	v3, _ := mustConnect(t, b.Addr(), confConnect{clientID: "v3-props-sub", clean: true})
	subscribe(t, v3, 2, mqtt.Properties{}, mqtt.Subscription{Filter: "props/x", QoS: 1})
	pub.publish(confPublish{topic: "props/x", payload: `{"v":2}`, qos: 1, pid: 6, props: props})
	expectPuback(t, pub, 6, 0)
	v3Got := expectPublish(t, v3)
	if string(v3Got.Payload) != `{"v":2}` {
		t.Fatalf("v3 payload = %q", v3Got.Payload)
	}
	if !reflect.DeepEqual(v3Got.Properties, mqtt.Properties{}) {
		t.Fatalf("v3 delivery carries MQTT 5 properties: %+v", v3Got.Properties)
	}
}

func test5SubscribeProperties(t *testing.T) {
	b := startBroker(t, Config{})
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-sub-props", clean: true})
	ack := subscribe(t, c, 3, mqtt.Properties{
		UserProperties: []mqtt.UserProperty{{Key: "client", Value: "suite"}},
	}, mqtt.Subscription{Filter: "subprops/#", QoS: 1})
	if ack.PacketID != 3 {
		t.Fatalf("SUBACK packet id %d, want 3", ack.PacketID)
	}
	if len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] != 1 {
		t.Fatalf("SUBACK codes %v, want [1]", ack.ReasonCodes)
	}
	// The property block is accepted; the filter is installed.
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-sub-props-pub", clean: true})
	pub.publish(confPublish{topic: "subprops/x", payload: "ok", qos: 0})
	if got := expectPublish(t, c); string(got.Payload) != "ok" {
		t.Fatalf("delivery = %q", got.Payload)
	}
}

func test5ConnackProperties(t *testing.T) {
	b := startBroker(t, Config{MaxPacketSize: 1024})
	_, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "", clean: true})
	if ack.SessionPresent {
		t.Fatal("new session reports Session Present")
	}
	p := ack.Properties
	if p.AssignedClientID == "" {
		t.Fatal("CONNACK does not assign a client id for an empty one")
	}
	// Absent Receive Maximum means 65535, the default flow-control window.
	if p.ReceiveMaximum != 0 {
		t.Fatalf("CONNACK receive maximum = %d, want the absent default", p.ReceiveMaximum)
	}
	// Absent Maximum QoS means QoS 2.
	if p.MaximumQoS != nil {
		t.Fatalf("CONNACK caps maximum QoS at %d, want absent (QoS 2)", *p.MaximumQoS)
	}
	if p.RetainAvailable == nil || !*p.RetainAvailable {
		t.Fatalf("retain available = %v, want true", p.RetainAvailable)
	}
	if p.MaximumPacketSize != 1024 {
		t.Fatalf("maximum packet size = %d, want 1024", p.MaximumPacketSize)
	}
	if p.TopicAliasMaximum != topicAliasMaximum {
		t.Fatalf("topic alias maximum = %d, want %d", p.TopicAliasMaximum, topicAliasMaximum)
	}
	if p.WildcardSubscriptionAvailable == nil || !*p.WildcardSubscriptionAvailable {
		t.Fatalf("wildcard subscription available = %v, want true", p.WildcardSubscriptionAvailable)
	}
	if p.SubscriptionIdentifierAvailable == nil || !*p.SubscriptionIdentifierAvailable {
		t.Fatalf("subscription identifier available = %v, want true", p.SubscriptionIdentifierAvailable)
	}
	if p.SharedSubscriptionAvailable == nil || !*p.SharedSubscriptionAvailable {
		t.Fatalf("shared subscription available = %v, want true", p.SharedSubscriptionAvailable)
	}
}

func test5TopicAlias(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-alias-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{},
		mqtt.Subscription{Filter: "alias/topic", QoS: 0},
		mqtt.Subscription{Filter: "alias/other", QoS: 0},
	).ReasonCodes; len(codes) != 2 || codes[0] != 0 || codes[1] != 0 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-alias-pub", clean: true})

	// Create an alias.
	pub.publish(confPublish{topic: "alias/topic", payload: "one", qos: 0, props: mqtt.Properties{TopicAlias: 1}})
	one := expectPublish(t, sub)
	if one.Topic != "alias/topic" || string(one.Payload) != "one" {
		t.Fatalf("alias create delivery = %q %q", one.Topic, one.Payload)
	}
	if one.Properties.TopicAlias != 0 {
		t.Fatal("topic alias is forwarded to the subscriber instead of being hop-local")
	}

	// Reuse it with an empty topic name.
	pub.publish(confPublish{topic: "", payload: "two", qos: 0, props: mqtt.Properties{TopicAlias: 1}})
	two := expectPublish(t, sub)
	if two.Topic != "alias/topic" || string(two.Payload) != "two" {
		t.Fatalf("alias reuse delivery = %q %q", two.Topic, two.Payload)
	}

	// An alias may be reassigned to a new topic.
	pub.publish(confPublish{topic: "alias/other", payload: "three", qos: 0, props: mqtt.Properties{TopicAlias: 2}})
	pub.publish(confPublish{topic: "", payload: "four", qos: 0, props: mqtt.Properties{TopicAlias: 2}})
	three := expectPublish(t, sub)
	four := expectPublish(t, sub)
	if three.Topic != "alias/other" || four.Topic != "alias/other" {
		t.Fatalf("alias reassignment deliveries = %q %q", three.Topic, four.Topic)
	}

	// An alias above the advertised Topic Alias Maximum is a protocol error.
	outOfRange, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-alias-big", clean: true})
	outOfRange.publish(confPublish{topic: "alias/topic", payload: "x", qos: 0, props: mqtt.Properties{TopicAlias: topicAliasMaximum + 1}})
	expectDisconnect(t, outOfRange, 0x94)

	// An empty topic with an alias that was never created cannot resolve.
	unknown, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-alias-unknown", clean: true})
	unknown.publish(confPublish{topic: "", payload: "x", qos: 0, props: mqtt.Properties{TopicAlias: 3}})
	expectDisconnect(t, unknown, 0x82)
}

func test5SubscriptionIdentifiers(t *testing.T) {
	b := startBroker(t, Config{})
	_, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-sub-id", clean: true})
	if ack.Properties.SubscriptionIdentifierAvailable == nil || !*ack.Properties.SubscriptionIdentifierAvailable {
		t.Fatalf("CONNACK subscription identifier available = %v, want true", ack.Properties.SubscriptionIdentifierAvailable)
	}
	t.Run("association_round_trip", func(t *testing.T) {
		// The identifier is stored with the subscription it was sent on and copied
		// into every PUBLISH that subscription causes; a publication matching two
		// tagged subscriptions carries both identifiers (§3.3.2.3.8).
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-sub-id-request", clean: true})
		if codes := subscribe(t, c, 1, mqtt.Properties{SubscriptionIdentifiers: []uint32{1}},
			mqtt.Subscription{Filter: "si/x", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
			t.Fatalf("SUBACK codes %v", codes)
		}
		if codes := subscribe(t, c, 2, mqtt.Properties{SubscriptionIdentifiers: []uint32{268435455}},
			mqtt.Subscription{Filter: "si/#", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
			t.Fatalf("SUBACK codes %v for the second tagged subscription", codes)
		}
		pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-sub-id-pub", clean: true})
		pub.mustPublish(t, confPublish{topic: "si/x", payload: "tagged", qos: 1, pid: 1})
		got := expectPublish(t, c)
		if ids := got.Properties.SubscriptionIdentifiers; !sameIDs(ids, []uint32{1, 268435455}) {
			t.Fatalf("delivery identifiers = %v, want [1 268435455]", ids)
		}
	})
	t.Run("zero_identifier_is_a_protocol_error", func(t *testing.T) {
		// MQTT 5 §3.8.2.1.2: a Subscription Identifier of 0 is a Protocol Error.
		// It is sent as a raw frame because the codec refuses to encode it.
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-sub-id-zero", clean: true})
		body := []byte{0, 1}
		body = append(body, 0x02, 0x0b, 0x00)
		body = mqtt.AppendString(body, "si/zero")
		body = append(body, 0)
		c.writeRaw(append(appendRemainingLength([]byte{0x82}, len(body)), body...))
		expectDisconnect(t, c, 0x82)
	})
}

func test5SubscriptionOptions(t *testing.T) {
	t.Run("no_local", func(t *testing.T) {
		b := startBroker(t, Config{})
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-nolocal", clean: true})
		if codes := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "nl/x", QoS: 1, NoLocal: true}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
			t.Fatalf("SUBACK codes %v", codes)
		}
		c.publish(confPublish{topic: "nl/x", payload: "self", qos: 1, pid: 1})
		expectPuback(t, c, 1, 0)
		expectNoMessage(t, c, 150*time.Millisecond)

		other, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-nolocal-other", clean: true})
		other.publish(confPublish{topic: "nl/x", payload: "other", qos: 0})
		if got := expectPublish(t, c); string(got.Payload) != "other" {
			t.Fatalf("delivery from another client = %q", got.Payload)
		}
	})

	t.Run("retain_as_published", func(t *testing.T) {
		b := startBroker(t, Config{})
		rap, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-rap", clean: true})
		plain, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-rap-plain", clean: true})
		subscribe(t, rap, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "rap/x", QoS: 1, RetainAsPublished: true})
		subscribe(t, plain, 2, mqtt.Properties{}, mqtt.Subscription{Filter: "rap/x", QoS: 1})

		pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-rap-pub", clean: true})
		pub.publish(confPublish{topic: "rap/x", payload: "kept", qos: 0, retain: true})
		if got := expectPublish(t, rap); !got.Retain {
			t.Fatal("retain-as-published subscription received RETAIN=0")
		}
		if got := expectPublish(t, plain); got.Retain {
			t.Fatal("plain subscription received RETAIN=1 on an established subscription")
		}
	})

	t.Run("retain_handling_0", func(t *testing.T) {
		b := startBroker(t, Config{})
		seedRetained(t, b, "rh/x", "stored")
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-rh0", clean: true})
		subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "rh/x", QoS: 1, RetainHandling: 0})
		if got := expectPublish(t, c); string(got.Payload) != "stored" || !got.Retain {
			t.Fatalf("retain handling 0 delivery = %q retain=%v", got.Payload, got.Retain)
		}
	})

	t.Run("retain_handling_1", func(t *testing.T) {
		b := startBroker(t, Config{})
		seedRetained(t, b, "rh/x", "stored")
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-rh1", clean: true})
		subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "rh/x", QoS: 1, RetainHandling: 1})
		if got := expectPublish(t, c); string(got.Payload) != "stored" {
			t.Fatalf("first retain handling 1 delivery = %q", got.Payload)
		}
		// The filter already exists, so a repeat subscription sends no retained
		// message.
		subscribe(t, c, 2, mqtt.Properties{}, mqtt.Subscription{Filter: "rh/x", QoS: 1, RetainHandling: 1})
		expectNoMessage(t, c, 150*time.Millisecond)
	})

	t.Run("retain_handling_2", func(t *testing.T) {
		b := startBroker(t, Config{})
		seedRetained(t, b, "rh/x", "stored")
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-rh2", clean: true})
		subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "rh/x", QoS: 1, RetainHandling: 2})
		expectNoMessage(t, c, 150*time.Millisecond)
	})
}

func seedRetained(t *testing.T, b *Broker, topic, payload string) {
	t.Helper()
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "seed-" + topic, clean: true})
	pub.publish(confPublish{topic: topic, payload: payload, qos: 1, pid: 1, retain: true})
	expectPuback(t, pub, 1, 0)
}

func test5ReasonCodes(t *testing.T) {
	t.Run("puback_success", func(t *testing.T) {
		b := startBroker(t, Config{})
		pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-puback-ok", clean: true})
		pub.publish(confPublish{topic: "rc/x", payload: "ok", qos: 1, pid: 7})
		expectPuback(t, pub, 7, 0)
	})

	t.Run("puback_failure", func(t *testing.T) {
		b := startBroker(t, Config{Sink: &confSink{publish: func(PublishEvent) PublishResult {
			return PublishResult{Disposition: PublishRejected, ReasonCode: ReasonNotAuthorized}
		}}})
		sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-puback-bad-sub", clean: true})
		subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "rc/x", QoS: 1})
		pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-puback-bad", clean: true})
		pub.publish(confPublish{topic: "rc/x", payload: "rejected", qos: 1, pid: 8})
		expectPuback(t, pub, 8, ReasonNotAuthorized)
		expectNoMessage(t, sub, 150*time.Millisecond)
	})

	t.Run("pubrec_failure", func(t *testing.T) {
		b := startBroker(t, Config{Sink: &confSink{publish: func(PublishEvent) PublishResult {
			return PublishResult{Disposition: PublishRejected, ReasonCode: ReasonNotAuthorized}
		}}})
		pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-pubrec-bad", clean: true})
		pub.publish(confPublish{topic: "rc/x", payload: "rejected", qos: 2, pid: 9})
		expectPubrec(t, pub, 9, ReasonNotAuthorized)
	})

	t.Run("suback_success_and_failure", func(t *testing.T) {
		acl := confACL(func(_ SessionInfo, topic string, action AclAction) bool {
			return !(action == AclSubscribe && topic == "rc/denied")
		})
		b := startBroker(t, Config{ACL: acl})
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-suback", clean: true})
		ack := subscribe(t, c, 11, mqtt.Properties{},
			mqtt.Subscription{Filter: "rc/allowed", QoS: 2},
			mqtt.Subscription{Filter: "rc/denied", QoS: 1},
		)
		want := []byte{2, 0x87}
		if len(ack.ReasonCodes) != len(want) || ack.ReasonCodes[0] != want[0] || ack.ReasonCodes[1] != want[1] {
			t.Fatalf("SUBACK codes %v, want %v", ack.ReasonCodes, want)
		}

		// A malformed filter is a per-filter failure on MQTT 5.
		bad := subscribe(t, c, 12, mqtt.Properties{}, mqtt.Subscription{Filter: "a/#/b", QoS: 1})
		if len(bad.ReasonCodes) != 1 || bad.ReasonCodes[0] != 0x8f {
			t.Fatalf("SUBACK codes for a malformed filter = %v, want [0x8f]", bad.ReasonCodes)
		}

		// The subscription cap is not an authorization failure: MQTT 5 has a
		// dedicated quota code.
		capped := startBroker(t, Config{MaxTopicSubscriptions: 1})
		quota, _ := mustConnect(t, capped.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-suback-cap", clean: true})
		qack := subscribe(t, quota, 13, mqtt.Properties{},
			mqtt.Subscription{Filter: "cap/a", QoS: 1},
			mqtt.Subscription{Filter: "cap/b", QoS: 1},
		)
		if len(qack.ReasonCodes) != 2 || qack.ReasonCodes[0] != 1 || qack.ReasonCodes[1] != 0x97 {
			t.Fatalf("SUBACK codes for a subscription cap = %v, want [1 0x97]", qack.ReasonCodes)
		}
	})

	t.Run("unsuback", func(t *testing.T) {
		b := startBroker(t, Config{})
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-unsuback", clean: true})
		subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "rc/unsub", QoS: 1})
		ack := c.unsubscribe(2, "rc/unsub", "rc/never")
		if ack.PacketID != 2 {
			t.Fatalf("UNSUBACK packet id %d, want 2", ack.PacketID)
		}
		if len(ack.ReasonCodes) != 2 {
			t.Fatalf("UNSUBACK reason codes %v, want one per filter", ack.ReasonCodes)
		}
		for i, code := range ack.ReasonCodes {
			// 0x00 Success and 0x11 No subscription existed are both valid here.
			if code != 0 && code != 0x11 {
				t.Fatalf("UNSUBACK reason code %d of %v is not a valid answer", i, ack.ReasonCodes)
			}
		}
	})

	t.Run("disconnect_reason", func(t *testing.T) {
		b := startBroker(t, Config{})
		sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-disc-sub", clean: true})
		subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "disc/will", QoS: 1})
		c, _ := mustConnect(t, b.Addr(), confConnect{
			version:  mqtt.Version5,
			clientID: "v5-disc",
			clean:    true,
			will:     &mqtt.Will{Topic: "disc/will", Payload: []byte("must-not-appear")},
		})
		c.write(&mqtt.Disconnect{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}, ReasonCode: 0x80})
		waitDisconnected(t, b, "v5-disc")
		// A client DISCONNECT, whatever its reason code, ends the session
		// without publishing the will (0x04 is the one exception).
		expectNoMessage(t, sub, 150*time.Millisecond)
	})

	t.Run("disconnect_with_will", func(t *testing.T) {
		b := startBroker(t, Config{})
		sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-disc-will-sub", clean: true})
		subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "disc/will", QoS: 1})
		c, _ := mustConnect(t, b.Addr(), confConnect{
			version:  mqtt.Version5,
			clientID: "v5-disc-will",
			clean:    true,
			will:     &mqtt.Will{Topic: "disc/will", Payload: []byte("requested")},
		})
		// 0x04: disconnect with will message.
		c.write(&mqtt.Disconnect{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}, ReasonCode: 0x04})
		if got := expectPublish(t, sub); string(got.Payload) != "requested" {
			t.Fatalf("will after DISCONNECT 0x04 = %q", got.Payload)
		}
	})
}

func test5SessionExpiry(t *testing.T) {
	b := startBroker(t, Config{})
	props := mqtt.Properties{SessionExpiryInterval: 60}
	first, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "v5-expiry",
		clean:    false,
		props:    props,
	})
	if ack.SessionPresent {
		t.Fatal("new session reports Session Present")
	}
	subscribe(t, first, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "expiry/x", QoS: 1})
	first.disconnect()
	waitDisconnected(t, b, "v5-expiry")

	second, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "v5-expiry",
		clean:    false,
		props:    props,
	})
	if !ack.SessionPresent {
		t.Fatal("a session with a non-zero Session Expiry Interval did not resume")
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-expiry-pub", clean: true})
	pub.publish(confPublish{topic: "expiry/x", payload: "resumed", qos: 0})
	if got := expectPublish(t, second); string(got.Payload) != "resumed" {
		t.Fatalf("resumed subscription delivery = %q", got.Payload)
	}
	second.disconnect()
	waitDisconnected(t, b, "v5-expiry")

	// A CONNECT without a Session Expiry Interval still resumes a stored
	// session; it only expires again when that connection ends.
	third, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "v5-expiry",
		clean:    false,
	})
	if !ack.SessionPresent {
		t.Fatal("a CONNECT without Session Expiry Interval did not resume the stored session")
	}
	third.disconnect()
	waitDisconnected(t, b, "v5-expiry")
	fourth, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "v5-expiry",
		clean:    false,
		props:    props,
	})
	if ack.SessionPresent {
		t.Fatal("a session with Session Expiry Interval 0 survived its connection")
	}
	_ = fourth

	t.Run("expires_after_interval", func(t *testing.T) {
		t.Skip("this row would have to wait out the 60 s Session Expiry Interval it connected with; the sweeper is covered with a 1 s interval and a short sweep period by TestSessionExpirySweepSessions")
	})
}

func test5ReceiveMaximum(t *testing.T) {
	b := startBroker(t, Config{})
	_, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-recvmax", clean: true})
	if ack.Properties.ReceiveMaximum != 0 {
		t.Fatalf("CONNACK receive maximum = %d; absent means the 65535 default", ack.Properties.ReceiveMaximum)
	}
	t.Run("flow_control", func(t *testing.T) {
		t.Skip("receive-maximum flow control is not implemented: ROADMAP.md lists \"Full MQTT 5 properties/semantics (session expiry, receive maximum, packet limits)\" as open")
	})
}

func test5MaximumPacketSize(t *testing.T) {
	b := startBroker(t, Config{MaxPacketSize: 128})
	c, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-maxpacket", clean: true})
	if ack.Properties.MaximumPacketSize != 128 {
		t.Fatalf("CONNACK maximum packet size = %d, want 128", ack.Properties.MaximumPacketSize)
	}
	c.publish(confPublish{topic: "big/topic", payload: string(make([]byte, 256)), qos: 0})
	expectDisconnect(t, c, 0x95)

	t.Run("client_limit", func(t *testing.T) {
		t.Skip("a client Maximum Packet Size in CONNECT is not honoured: ROADMAP.md lists \"Full MQTT 5 properties/semantics (session expiry, receive maximum, packet limits)\" as open")
	})
}

func test5SharedSubscriptions(t *testing.T) {
	b := startBroker(t, Config{})
	_, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-shared", clean: true})
	if ack.Properties.SharedSubscriptionAvailable == nil || !*ack.Properties.SharedSubscriptionAvailable {
		t.Fatalf("CONNACK shared subscription available = %v, want true", ack.Properties.SharedSubscriptionAvailable)
	}
	t.Run("round_robin", func(t *testing.T) {
		// A publication matching a shared subscription goes to exactly one member of
		// the group; the group alternates, and a normal subscriber of the same filter
		// gets its own copy (§4.8.2).
		first, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-shared-1", clean: true})
		second, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-shared-2", clean: true})
		plain, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-shared-plain", clean: true})
		for _, c := range []*confClient{first, second} {
			if codes := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "$share/g/v5/shared", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
				t.Fatalf("SUBACK codes %v for the shared filter", codes)
			}
		}
		if codes := subscribe(t, plain, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "v5/shared", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
			t.Fatalf("SUBACK codes %v for the normal filter", codes)
		}
		pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-shared-pub", clean: true})
		for i, payload := range []string{"one", "two"} {
			pub.publish(confPublish{topic: "v5/shared", payload: payload, qos: 0})
			want := first
			if i == 1 {
				want = second
			}
			if got := expectPublish(t, want); string(got.Payload) != payload {
				t.Fatalf("group member received %q for publication %d, want %q", got.Payload, i, payload)
			}
			if got := expectPublish(t, plain); string(got.Payload) != payload {
				t.Fatalf("normal subscriber received %q for publication %d", got.Payload, i)
			}
		}
	})
	t.Run("malformed_filter_is_refused_per_filter", func(t *testing.T) {
		// A "$share/..." filter without a ShareName and a Topic Filter is refused
		// with 0x8F, per filter, and the connection stays usable (§3.9).
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-shared-bad", clean: true})
		ack := subscribe(t, c, 1, mqtt.Properties{},
			mqtt.Subscription{Filter: "$share/g/v5/ok", QoS: 1},
			mqtt.Subscription{Filter: "$share//v5/ok", QoS: 1},
		)
		if len(ack.ReasonCodes) != 2 || ack.ReasonCodes[0] != 1 || ack.ReasonCodes[1] != 0x8f {
			t.Fatalf("SUBACK codes %v, want [1 0x8F]", ack.ReasonCodes)
		}
		c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
		readConf[*mqtt.Pingresp](t, c)
	})
}

func test5Auth(t *testing.T) {
	// Enhanced authentication is not negotiated: the CONNACK carries no
	// Authentication Method, so an AUTH packet is a protocol error (MQTT 5
	// 3.15.2) and the broker must say so rather than ignore the packet.
	b := startBroker(t, Config{})
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-auth", clean: true})
	c.write(&mqtt.Auth{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	expectDisconnect(t, c, 0x82)
}

func test5ProtocolErrorDisconnect(t *testing.T) {
	t.Run("malformed_property", func(t *testing.T) {
		b := startBroker(t, Config{})
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-bad-prop", clean: true})
		// PUBLISH with a one-byte property block holding unknown identifier 0x7f.
		body := mqtt.AppendString(nil, "bad/x")
		body = append(body, 0x01, 0x7f)
		body = append(body, 'x')
		frame := appendRemainingLength([]byte{0x30}, len(body))
		c.writeRaw(append(frame, body...))
		expectDisconnect(t, c, 0x81)
	})

	t.Run("wildcard_topic", func(t *testing.T) {
		b := startBroker(t, Config{})
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-bad-topic", clean: true})
		c.publish(confPublish{topic: "a/+/b", payload: "x", qos: 0})
		expectDisconnect(t, c, 0x90)
	})

	t.Run("empty_topic_without_alias", func(t *testing.T) {
		b := startBroker(t, Config{})
		c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-empty-topic", clean: true})
		c.publish(confPublish{topic: "", payload: "x", qos: 0})
		expectDisconnect(t, c, 0x82)
	})
}

func test5SubscribeInvalidFilter(t *testing.T) {
	b := startBroker(t, Config{})
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-bad-filter", clean: true})
	ack := subscribe(t, c, 4, mqtt.Properties{},
		mqtt.Subscription{Filter: "a/#/b", QoS: 1},
		mqtt.Subscription{Filter: "ok/filter", QoS: 1},
	)
	want := []byte{0x8f, 1}
	if len(ack.ReasonCodes) != len(want) || ack.ReasonCodes[0] != want[0] || ack.ReasonCodes[1] != want[1] {
		t.Fatalf("SUBACK codes %v, want %v", ack.ReasonCodes, want)
	}
	// The malformed filter is not a connection error on MQTT 5: the valid
	// filter in the same packet still works.
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-bad-filter-pub", clean: true})
	pub.publish(confPublish{topic: "ok/filter", payload: "works", qos: 0})
	if got := expectPublish(t, c); string(got.Payload) != "works" {
		t.Fatalf("delivery after a rejected filter = %q", got.Payload)
	}
}

func test5QoSDowngrade(t *testing.T) {
	b := startBroker(t, Config{})
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-downgrade", clean: true})
	if codes := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "down/x", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-downgrade-pub", clean: true})
	pub.publish(confPublish{topic: "down/x", payload: "one", qos: 1, pid: 3})
	expectPuback(t, pub, 3, 0)
	got := expectPublish(t, c)
	if got.QoS != 0 || got.PacketID != 0 {
		t.Fatalf("QoS 1 publication to a QoS 0 subscription = QoS %d packet id %d", got.QoS, got.PacketID)
	}
}

func test5QoS2Flow(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-q2-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "q2/v5", QoS: 2}).ReasonCodes; len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-q2-pub", clean: true})

	pub.publish(confPublish{topic: "q2/v5", payload: "once", qos: 2, pid: 31})
	expectPubrec(t, pub, 31, 0)
	if got := expectPublish(t, sub); string(got.Payload) != "once" || got.QoS != 2 {
		t.Fatalf("v5 QoS 2 delivery = %q qos%d", got.Payload, got.QoS)
	}
	pub.publish(confPublish{topic: "q2/v5", payload: "once", qos: 2, pid: 31, dup: true})
	expectPubrec(t, pub, 31, 0)
	expectNoMessage(t, sub, 100*time.Millisecond)
	pub.write(&mqtt.Pubrel{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}, PacketID: 31})
	expectPubcomp(t, pub, 31)
}

// TestConformanceAuthNotNegotiated pins the enhanced-authentication rule: the
// broker negotiates no Authentication Method, so an AUTH packet is a protocol
// error and must be answered with DISCONNECT 0x82 rather than ignored.
func TestConformanceAuthNotNegotiated(t *testing.T) {
	b := startBroker(t, Config{})
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "v5-auth-rule", clean: true})
	c.write(&mqtt.Auth{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}, ReasonCode: 0x18})
	expectDisconnect(t, c, 0x82)
}
