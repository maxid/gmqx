package mqtt5

import (
	"bytes"
	"testing"
)

// FuzzDecodePropertyBlock feeds arbitrary bytes to the MQTT 5 property decoder.
//
// The decoder runs on every inbound PUBLISH and on every cluster event that
// carries properties (ClusterMessage.Props is one property block without its
// length prefix), so its input is a peer's bytes: a panic is a remote crash, and
// an unbounded allocation is a remote memory exhaustion. The round-trip half pins
// the encoder against it: whatever decodes must re-encode to bytes that decode to
// the same property set.
func FuzzDecodePropertyBlock(f *testing.F) {
	seeds := [][]byte{
		nil,
		{},
		{0x01, 0x01},                      // payload format indicator
		{0x02, 0x00, 0x00, 0x01},          // message expiry
		{0x0b, 0x05},                      // subscription identifier
		{0x26, 0x00, 0x01, 'a'},           // one user property pair
		{0x09, 0x00, 0x0d, 'a', '/', 'b'}, // correlation data
		{0xff, 0xff, 0xff, 0xff},          // unknown / hostile identifier
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		props, err := DecodePropertyBlock(raw)
		if err != nil {
			return
		}
		encoded, err := EncodePropertyBlock(props)
		if err != nil {
			t.Fatalf("decoded a block that does not encode: %v (raw %x)", err, raw)
		}
		again, err := DecodePropertyBlock(encoded)
		if err != nil {
			t.Fatalf("re-decoding our own encoding failed: %v (raw %x, encoded %x)", err, raw, encoded)
		}
		// Idempotence is the property that matters on the wire: the second encoding
		// must be byte-identical to the first. Comparing the property structs would
		// compare pointers (the decoder allocates fresh ones for the optional
		// fields), which the wire format cannot distinguish.
		reencoded, err := EncodePropertyBlock(again)
		if err != nil {
			t.Fatalf("re-encoding the decoded block failed: %v", err)
		}
		if !bytes.Equal(encoded, reencoded) {
			t.Fatalf("encode is not idempotent: %x -> %x -> %x (raw %x)", encoded, reencoded, encoded, raw)
		}
	})
}
