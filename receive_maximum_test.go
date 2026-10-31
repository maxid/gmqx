package gmqx

// MQTT 5 Receive Maximum (§3.3.4) in both directions.
//
// Outbound: the Receive Maximum a client sends in its CONNECT bounds how many QoS
// 1/2 PUBLISHes the broker may have outstanding toward it at once. It caps the
// configured inflight window of each QoS (the effective window is the smaller of
// the two), the two QoS levels share it, and a delivery that cannot enter it waits
// in the connection's inflight queue instead of being dropped.
//
// Inbound: the broker's own receive maximum (Config.ReceiveMaximum, lowered by
// MaxInflightQoS2) is what its CONNACK advertises and what it enforces on the
// publishes a client sends it.

import (
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// TestReceiveMaximumCapsOutboundWindow drives the outbound window over the wire:
// a client that asks for N unconfirmed QoS 1 PUBLISHes never sees N+1, and the
// extra delivery is released — not lost — by the PUBACK that frees a slot.
func TestReceiveMaximumCapsOutboundWindow(t *testing.T) {
	cases := []struct {
		name            string
		maxInflightQoS1 int
		receiveMax      uint16 // the CONNECT property; 0 means absent
		wantUnacked     int
	}{
		// The client's window is smaller than the broker's default: the client wins.
		{name: "client_asks_for_one", maxInflightQoS1: defaultMaxInflightQoS1, receiveMax: 1, wantUnacked: 1},
		// The broker's window is smaller than the client's: the broker wins.
		{name: "broker_window_is_smaller", maxInflightQoS1: 2, receiveMax: 10, wantUnacked: 2},
		// The absent property is the protocol default 65535 and does not narrow
		// anything.
		{name: "absent_property", maxInflightQoS1: defaultMaxInflightQoS1, receiveMax: 0, wantUnacked: 3},
	}
	const total = 3
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := startBroker(t, Config{MaxInflightQoS1: tc.maxInflightQoS1})
			sub, _ := mustConnect(t, b.Addr(), confConnect{
				version:  mqtt.Version5,
				clientID: "recvmax-" + tc.name,
				clean:    true,
				props:    mqtt.Properties{ReceiveMaximum: tc.receiveMax},
			})
			if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "recvmax/cap", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
				t.Fatalf("SUBACK codes %v", codes)
			}
			pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "recvmax-cap-pub", clean: true})
			payloads := []string{"one", "two", "three"}
			for i, payload := range payloads {
				pub.mustPublish(t, confPublish{topic: "recvmax/cap", payload: payload, qos: 1, pid: uint16(i + 1)})
			}

			var received []uint16
			for i := 0; i < tc.wantUnacked; i++ {
				got := expectPublish(t, sub)
				if string(got.Payload) != payloads[i] {
					t.Fatalf("delivery %d = %q, want %q", i, got.Payload, payloads[i])
				}
				received = append(received, got.PacketID)
			}
			// The window is full: the remaining publications are queued, not sent.
			expectNoMessage(t, sub, 200*time.Millisecond)

			// Every acknowledgement releases exactly one queued publication.
			for i := tc.wantUnacked; i < total; i++ {
				sub.ack(received[0])
				received = received[1:]
				got := expectPublish(t, sub)
				if string(got.Payload) != payloads[i] {
					t.Fatalf("delivery after a PUBACK = %q, want %q", got.Payload, payloads[i])
				}
				received = append(received, got.PacketID)
			}
			expectNoMessage(t, sub, 100*time.Millisecond)
			for _, pid := range received {
				sub.ack(pid)
			}
			waitInflightEmpty(t, b, "recvmax-"+tc.name)
		})
	}
}

// TestReceiveMaximumIsSharedByBothQoSLevels pins the part of §3.3.4 that a
// per-QoS window would get wrong: the Receive Maximum counts QoS 1 and QoS 2
// PUBLISHes together, so a client that asks for 2 does not get two of each.
func TestReceiveMaximumIsSharedByBothQoSLevels(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "recvmax-shared",
		clean:    true,
		props:    mqtt.Properties{ReceiveMaximum: 2},
	})
	if codes := subscribe(t, sub, 1, mqtt.Properties{},
		mqtt.Subscription{Filter: "recvmax/shared/one", QoS: 1},
		mqtt.Subscription{Filter: "recvmax/shared/two", QoS: 2},
	).ReasonCodes; len(codes) != 2 || codes[0] != 1 || codes[1] != 2 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "recvmax-shared-pub", clean: true})

	pub.mustPublish(t, confPublish{topic: "recvmax/shared/one", payload: "q1", qos: 1, pid: 1})
	pub.publish(confPublish{topic: "recvmax/shared/two", payload: "q2", qos: 2, pid: 2})
	expectPubrec(t, pub, 2, 0)
	pub.mustPublish(t, confPublish{topic: "recvmax/shared/one", payload: "third", qos: 1, pid: 3})

	first := expectPublish(t, sub)
	second := expectPublish(t, sub)
	if string(first.Payload) != "q1" || string(second.Payload) != "q2" {
		t.Fatalf("first two deliveries = %q, %q", first.Payload, second.Payload)
	}
	expectNoMessage(t, sub, 200*time.Millisecond)

	// The QoS 1 PUBACK confirms one PUBLISH and frees one of the two slots.
	sub.ack(first.PacketID)
	if got := expectPublish(t, sub); string(got.Payload) != "third" {
		t.Fatalf("delivery after one acknowledgement = %q, want \"third\"", got.Payload)
	}
}

// TestReceiveMaximumInboundWindow covers the broker's own receive maximum: what
// its CONNACK advertises is what it enforces, a QoS 1 publish over it is left
// unacknowledged (so the client retransmits later) and a QoS 2 publish is refused
// with PUBREC 0x97 (Quota exceeded).
func TestReceiveMaximumInboundWindow(t *testing.T) {
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{ReceiveMaximum: 2, Metrics: metrics})
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "recvmax-in-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "recvmax/in", QoS: 2}).ReasonCodes; len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "recvmax-in-pub", clean: true})
	if ack.Properties.ReceiveMaximum != 2 {
		t.Fatalf("CONNACK receive maximum = %d, want the enforced 2", ack.Properties.ReceiveMaximum)
	}

	// Two QoS 2 exchanges fill the window; both are delivered.
	for _, pid := range []uint16{10, 11} {
		pub.publish(confPublish{topic: "recvmax/in", payload: "two-slots", qos: 2, pid: pid})
		expectPubrec(t, pub, pid, 0)
		if got := expectPublish(t, sub); string(got.Payload) != "two-slots" {
			t.Fatalf("delivery of packet %d = %q", pid, got.Payload)
		}
	}

	// Over the window: a QoS 1 publish is neither acknowledged nor delivered...
	pub.publish(confPublish{topic: "recvmax/in", payload: "over", qos: 1, pid: 12})
	expectNoMessage(t, pub, 200*time.Millisecond)
	expectNoMessage(t, sub, 100*time.Millisecond)
	// ...and a QoS 2 publish is answered with PUBREC 0x97, without starting an
	// exchange.
	pub.publish(confPublish{topic: "recvmax/in", payload: "over", qos: 2, pid: 13})
	expectPubrec(t, pub, 13, ReasonQuotaExceeded)
	expectNoMessage(t, sub, 100*time.Millisecond)
	waitFor(t, "both over-limit publishes to be counted", func() bool {
		return metrics.count("receive_maximum_full") == 2
	})

	// Releasing one exchange makes room: the retransmitted QoS 1 publish is
	// accepted and delivered once.
	pub.write(&mqtt.Pubrel{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}, PacketID: 10})
	expectPubcomp(t, pub, 10)
	pub.publish(confPublish{topic: "recvmax/in", payload: "over", qos: 1, pid: 12, dup: true})
	expectPuback(t, pub, 12, 0)
	if got := expectPublish(t, sub); string(got.Payload) != "over" {
		t.Fatalf("retransmitted delivery = %q, want \"over\"", got.Payload)
	}
}

// TestReceiveMaximumAdvertised pins the CONNACK value against the configuration:
// the broker advertises the window it enforces, never a larger one.
func TestReceiveMaximumAdvertised(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want uint16
	}{
		{name: "default", cfg: Config{}, want: defaultReceiveMaximum},
		{name: "configured", cfg: Config{ReceiveMaximum: 7}, want: 7},
		// MaxInflightQoS2 still bounds the inbound exchanges, so the smaller of
		// the two is what the broker can honour.
		{name: "qos2_window_is_smaller", cfg: Config{MaxInflightQoS2: 3}, want: 3},
		// Unlimited on both sides is the protocol default, not a zero property.
		{name: "unlimited", cfg: Config{ReceiveMaximum: -1, MaxInflightQoS2: -1}, want: maxReceiveMaximum},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := startBroker(t, tc.cfg)
			_, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "recvmax-adv-" + tc.name, clean: true})
			if ack.Properties.ReceiveMaximum != tc.want {
				t.Fatalf("CONNACK receive maximum = %d, want %d", ack.Properties.ReceiveMaximum, tc.want)
			}
			if got := b.receiveMaximum(); got != int(tc.want) {
				t.Fatalf("receiveMaximum() = %d, want %d", got, tc.want)
			}
		})
	}
}
