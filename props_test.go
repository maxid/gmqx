package gmqx

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/maxid/gnety/codec/mqtt"
)

// TestPropertyBlockRoundTrip pins the seam between the typed property set and
// the property bytes PublishEvent.Properties / ClusterMessage.Props carry: what
// gmqx re-encodes must parse back to the same set.
func TestPropertyBlockRoundTrip(t *testing.T) {
	format := byte(1)
	want := mqtt.Properties{
		PayloadFormat:         &format,
		MessageExpiryInterval: 30,
		ContentType:           "text/plain",
		ResponseTopic:         "reply/topic",
		CorrelationData:       []byte{1, 2, 3},
		UserProperties:        []mqtt.UserProperty{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}},
	}
	raw, err := encodePropertyBlock(want)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodePropertyBlock(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

// TestPropertyBlockBytes pins the wire bytes of a forwarded property block: a
// user property is identifier 0x26, two UTF-8 strings, in ascending identifier
// order.
func TestPropertyBlockBytes(t *testing.T) {
	raw, err := encodePropertyBlock(mqtt.Properties{
		UserProperties: []mqtt.UserProperty{{Key: "key", Value: "v"}},
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := []byte{0x26, 0, 3, 'k', 'e', 'y', 0, 1, 'v'}
	if !bytes.Equal(raw, want) {
		t.Fatalf("property bytes = %x, want %x", raw, want)
	}
}

func TestPropertyBlockEmpty(t *testing.T) {
	raw, err := encodePropertyBlock(mqtt.Properties{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(raw) != 0 {
		t.Fatalf("empty property set encodes %d bytes: %x", len(raw), raw)
	}
	got, err := decodePropertyBlock(nil)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if !reflect.DeepEqual(got, mqtt.Properties{}) {
		t.Fatalf("decoded empty = %+v", got)
	}
}

// TestPropertyBlockRejectsBadBytes covers bytes from a sink or a peer node: they
// are parsed with the PUBLISH property rules, so a truncated value and a
// property that does not belong on a PUBLISH are both rejected.
func TestPropertyBlockRejectsBadBytes(t *testing.T) {
	truncated := []byte{0x26, 0, 3, 'k'}
	if _, err := decodePropertyBlock(truncated); err == nil {
		t.Fatal("expected an error for a truncated user property")
	}
	// 0x12 is Assigned Client Identifier, CONNACK only.
	connackOnly := []byte{0x12, 0, 1, 'c'}
	if _, err := decodePropertyBlock(connackOnly); err == nil {
		t.Fatal("expected an error for a CONNACK-only property")
	}
}
