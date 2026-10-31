package gmqx

// The MQTT 5 client Maximum Packet Size (§3.1.2.11.4).
//
// A CONNECT can tell the broker the largest packet it accepts. Every packet the
// broker writes to that connection is measured against it, and one that does not
// fit is dropped and counted instead of being split or truncated: a PUBLISH, a
// retained replay and a control packet (a SUBACK here) take the same path, and the
// connection stays usable. The broker's own inbound limit (Config.MaxPacketSize)
// is independent of it and is still advertised in the CONNACK.

import (
	"strings"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// smallPacketLimit is comfortably larger than a CONNACK and a one-filter SUBACK
// and comfortably smaller than the payloads the cases below deliver.
const smallPacketLimit = 64

func TestClientMaximumPacketSizeDropsOversizedPublish(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics})
	sub, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "maxpacket-sub",
		clean:    true,
		props:    mqtt.Properties{MaximumPacketSize: smallPacketLimit},
	})
	if ack.Properties.MaximumPacketSize == 0 {
		t.Fatal("the broker stopped advertising its own inbound maximum packet size")
	}
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "maxpacket/x", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "maxpacket-pub", clean: true})

	// The oversized PUBLISH is suppressed, and the metric says why.
	pub.publish(confPublish{topic: "maxpacket/x", payload: strings.Repeat("x", 200), qos: 0})
	expectNoMessage(t, sub, 200*time.Millisecond)
	waitFor(t, "the oversized PUBLISH to be counted", func() bool {
		return metrics.count("max_packet_size_too_large") == 1
	})

	// A message that fits still arrives: the connection is not closed and nothing
	// about it was degraded.
	pub.publish(confPublish{topic: "maxpacket/x", payload: "small", qos: 0})
	if got := expectPublish(t, sub); string(got.Payload) != "small" {
		t.Fatalf("delivery after the dropped one = %q, want \"small\"", got.Payload)
	}
	sub.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	readConf[*mqtt.Pingresp](t, sub)
}

func TestClientMaximumPacketSizeDropsOversizedControlPacket(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics})
	sub, _ := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "maxpacket-control",
		clean:    true,
		props:    mqtt.Properties{MaximumPacketSize: smallPacketLimit},
	})

	// A SUBACK carries one reason code per filter: 200 of them do not fit in 64
	// bytes. The broker must not send a SUBACK over the limit, so the packet is
	// dropped and counted instead.
	filters := make([]mqtt.Subscription, 0, 200)
	for i := 0; i < 200; i++ {
		filters = append(filters, mqtt.Subscription{Filter: "maxpacket/control/" + strings.Repeat("f", 4), QoS: 0})
	}
	sub.write(&mqtt.Subscribe{
		MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5},
		PacketID:      2,
		Subscriptions: filters,
	})
	expectNoMessage(t, sub, 200*time.Millisecond)
	waitFor(t, "the oversized SUBACK to be counted", func() bool {
		return metrics.count("max_packet_size_too_large") == 1
	})

	// The subscription itself was applied and the connection is still usable: a
	// PINGREQ is answered, and a small SUBSCRIBE gets its small SUBACK.
	sub.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	readConf[*mqtt.Pingresp](t, sub)
	if codes := subscribe(t, sub, 3, mqtt.Properties{}, mqtt.Subscription{Filter: "maxpacket/control/one", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v after the dropped one", codes)
	}
}

func TestClientMaximumPacketSizeDropsOversizedRetainedReplay(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics})
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "maxpacket-ret-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "maxpacket/ret", payload: strings.Repeat("r", 300), qos: 1, pid: 1, retain: true})

	sub, _ := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "maxpacket-ret-sub",
		clean:    true,
		props:    mqtt.Properties{MaximumPacketSize: smallPacketLimit},
	})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "maxpacket/ret", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	// The replay is a delivery like any other: it does not fit, so it is dropped
	// and counted rather than truncated to the limit.
	expectNoMessage(t, sub, 200*time.Millisecond)
	waitFor(t, "the oversized retained replay to be counted", func() bool {
		return metrics.count("max_packet_size_too_large") == 1
	})
}

func TestClientMaximumPacketSizeAbsentSendsEverything(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics})
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "maxpacket-none", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "maxpacket/none", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "maxpacket-none-pub", clean: true})
	payload := strings.Repeat("n", 100<<10)
	pub.publish(confPublish{topic: "maxpacket/none", payload: payload, qos: 0})
	if got := expectPublish(t, sub); string(got.Payload) != payload {
		t.Fatalf("delivery truncated: %d bytes, want %d", len(got.Payload), len(payload))
	}
	if n := metrics.count("max_packet_size_too_large"); n != 0 {
		t.Fatalf("a client without a limit counted %d oversized packets", n)
	}
}

// TestClientMaximumPacketSizeKeepsQoS2ExchangeUsable pins that a suppressed QoS 2
// delivery does not leave a packet id reserved: the client is not sent a PUBREL for
// a PUBLISH it never received.
func TestClientMaximumPacketSizeKeepsQoS2ExchangeUsable(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{Metrics: metrics})
	sub, _ := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "maxpacket-qos2",
		clean:    true,
		props:    mqtt.Properties{MaximumPacketSize: smallPacketLimit},
	})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "maxpacket/qos2", QoS: 2}).ReasonCodes; len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "maxpacket-qos2-pub", clean: true})
	pub.publish(confPublish{topic: "maxpacket/qos2", payload: strings.Repeat("q", 200), qos: 2, pid: 1})
	expectPubrec(t, pub, 1, 0)
	expectNoMessage(t, sub, 200*time.Millisecond)
	waitFor(t, "the oversized QoS 2 PUBLISH to be counted", func() bool {
		return metrics.count("max_packet_size_too_large") == 1
	})
	// No inflight exchange survived the drop, so nothing is retransmitted either.
	b.mu.RLock()
	c := b.clients["maxpacket-qos2"]
	b.mu.RUnlock()
	waitFor(t, "the dropped delivery to leave no inflight exchange", func() bool {
		if c == nil {
			return false
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.inflight) == 0
	})
}
