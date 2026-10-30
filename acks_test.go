package gmqx

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"
)

// The acknowledgement paths are the ones the typed packet layer decides on its
// own: which packet type carries the answer, whether a reason code fits and how
// many reason codes a SUBACK/UNSUBACK returns. These tests read the bytes.

// TestQoS2Handshake walks the inbound QoS 2 exchange: PUBLISH is answered with
// PUBREC, PUBREL with PUBCOMP, both carrying the same packet id.
func TestQoS2Handshake(t *testing.T) {
	b := startBroker(t, Config{})
	pub := dialTestClient(t, b.Addr(), "qos2-pub")

	body := appendString(nil, "qos2/topic")
	body = append(body, 0, 3) // packet id 3
	body = append(body, []byte("x")...)
	pub.writePacket(0x34, body) // PUBLISH, QoS 2

	h, p, err := pub.readPacket()
	if err != nil {
		t.Fatalf("read pubrec: %v", err)
	}
	if h != 0x50 || len(p) != 2 || binary.BigEndian.Uint16(p) != 3 {
		t.Fatalf("pubrec = %#x %x", h, p)
	}

	pub.writePacket(0x62, []byte{0, 3}) // PUBREL
	h, p, err = pub.readPacket()
	if err != nil {
		t.Fatalf("read pubcomp: %v", err)
	}
	if h != 0x70 || len(p) != 2 || binary.BigEndian.Uint16(p) != 3 {
		t.Fatalf("pubcomp = %#x %x", h, p)
	}
}

// TestMQTT5RejectedPublishReasonCode pins the MQTT 5 negative acknowledgement:
// a rejected publish is answered with PUBACK and the sink's reason code, while
// the packet id stays the one the client sent.
func TestMQTT5RejectedPublishReasonCode(t *testing.T) {
	b := startBroker(t, Config{Sink: sinkFunc(func(context.Context, PublishEvent) PublishResult {
		return PublishResult{Disposition: PublishRejected, ReasonCode: ReasonNotAuthorized}
	})})
	conn, r, reason := dialV5(t, b.Addr(), "v5-reject", nil)
	defer conn.Close()
	if reason != 0 {
		t.Fatalf("connack reason = %d", reason)
	}

	body := appendString(nil, "deny/topic")
	body = append(body, 0, 7) // packet id 7
	body = append(body, 0)    // empty property block
	body = append(body, []byte("x")...)
	frame := appendRemainingLength([]byte{0x32}, len(body))
	frame = append(frame, body...)
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}

	h, p, err := readPacketFrom(r)
	if err != nil {
		t.Fatalf("read puback: %v", err)
	}
	if h != 0x40 || len(p) < 3 || binary.BigEndian.Uint16(p) != 7 || p[2] != ReasonNotAuthorized {
		t.Fatalf("puback = %#x %x", h, p)
	}
}

// TestMQTT5UnsubackReasonCodes pins that MQTT 5 UNSUBACK answers every filter:
// the packet id, an empty property block and one success code per filter.
func TestMQTT5UnsubackReasonCodes(t *testing.T) {
	b := startBroker(t, Config{})
	conn, r, reason := dialV5(t, b.Addr(), "v5-unsub", nil)
	defer conn.Close()
	if reason != 0 {
		t.Fatalf("connack reason = %d", reason)
	}
	// SUBSCRIBE pid 1, no properties, filter "unsub/topic" at QoS 0.
	sub := []byte{0, 1, 0}
	sub = appendString(sub, "unsub/topic")
	sub = append(sub, 0)
	frame := appendRemainingLength([]byte{0x82}, len(sub))
	frame = append(frame, sub...)
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	if h, _, err := readPacketFrom(r); err != nil || h != 0x90 {
		t.Fatalf("suback = %#x, %v", h, err)
	}

	unsub := []byte{0, 2, 0}
	unsub = appendString(unsub, "unsub/topic")
	unsub = appendString(unsub, "other/topic")
	frame = appendRemainingLength([]byte{0xA2}, len(unsub))
	frame = append(frame, unsub...)
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	h, p, err := readPacketFrom(r)
	if err != nil {
		t.Fatalf("read unsuback: %v", err)
	}
	want := []byte{0, 2, 0, 0, 0}
	if h != 0xB0 || string(p) != string(want) {
		t.Fatalf("unsuback = %#x %x, want 0xb0 %x", h, p, want)
	}
}

// TestMQTT5PingAndDisconnect covers the two bodyless packets a client sends and
// the answer to PINGREQ.
func TestMQTT5PingAndDisconnect(t *testing.T) {
	b := startBroker(t, Config{})
	conn, r, _ := dialV5(t, b.Addr(), "v5-ping", nil)
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	defer conn.Close()

	if _, err := conn.Write([]byte{0xC0, 0}); err != nil { // PINGREQ
		t.Fatal(err)
	}
	h, p, err := readPacketFrom(r)
	if err != nil {
		t.Fatalf("read pingresp: %v", err)
	}
	if h != 0xD0 || len(p) != 0 {
		t.Fatalf("pingresp = %#x %x", h, p)
	}

	if _, err := conn.Write([]byte{0xE0, 0}); err != nil { // DISCONNECT
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected the broker to close after DISCONNECT, got %v", err)
	}
}
