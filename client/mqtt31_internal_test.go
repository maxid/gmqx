package client

// The wire-shape half of MQTT 3.1 support: the CONNECT the client writes must carry
// the 3.1 protocol name ("MQIsdp", level 3) and a remaining length that matches the
// longer body. A codec change that stops encoding 3.1.1 would break the rewrite, and
// this case says so in bytes instead of through a broker.

import (
	"testing"

	"github.com/maxid/gnety/codec/mqtt"
)

func TestMQTT31ConnectFrame(t *testing.T) {
	c := newClient(Config{ClientID: "frame", Version: Version31})
	frame, err := encodeConnect(c)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if frame[0]>>4 != mqtt.TypeConnect {
		t.Fatalf("first byte %#x is not CONNECT", frame[0])
	}

	length, body, err := readConnectLength(frame[1:])
	if err != nil {
		t.Fatalf("remaining length: %v", err)
	}
	if length != len(body) {
		t.Fatalf("remaining length %d does not match the body (%d)", length, len(body))
	}
	if len(body) < 2+6+1 {
		t.Fatalf("body too short: %x", body)
	}
	nameLen := int(body[0])<<8 | int(body[1])
	if nameLen != 6 || string(body[2:2+nameLen]) != "MQIsdp" {
		t.Fatalf("protocol name %q (length %d), want MQIsdp", string(body[2:2+nameLen]), nameLen)
	}
	if level := body[2+nameLen]; level != Version31 {
		t.Fatalf("level %d, want %d", level, Version31)
	}

	// And the 3.1.1 spelling is untouched: same client, other version, "MQTT"/4.
	c = newClient(Config{ClientID: "frame", Version: mqtt.Version311})
	frame, err = encodeConnect(c)
	if err != nil {
		t.Fatalf("encode 3.1.1: %v", err)
	}
	if _, body, err = readConnectLength(frame[1:]); err != nil {
		t.Fatalf("remaining length: %v", err)
	}
	nameLen = int(body[0])<<8 | int(body[1])
	if string(body[2:2+nameLen]) != "MQTT" || body[2+nameLen] != mqtt.Version311 {
		t.Fatalf("3.1.1 CONNECT names %q level %d", string(body[2:2+nameLen]), body[2+nameLen])
	}
}
