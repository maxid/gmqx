package gmqx

// Raw wire helpers for the black-box wire tests: they build and parse frames
// byte by byte so an assertion can talk about the bytes the broker actually
// puts on the socket. The names are the ones the tests always used; the
// implementations are the framer primitives of gnety/codec/mqtt, which is also
// what production code sits on — production goes through the typed layer
// (mqtt.Decoder, mqtt.Encode) and never touches these.

import (
	"bufio"

	"github.com/maxid/gnety/codec/mqtt"
)

func appendRemainingLength(dst []byte, n int) []byte { return mqtt.AppendRemainingLength(dst, n) }

func readRemainingLength(r *bufio.Reader) (int, error) { return mqtt.ReadRemainingLength(r) }

func appendString(dst []byte, s string) []byte { return mqtt.AppendString(dst, s) }

func readString(b []byte) (string, []byte, error) { return mqtt.ReadString(b) }

// prefixPropertyLength prefixes an MQTT 5 property block with its length.
func prefixPropertyLength(props []byte) []byte {
	return append(mqtt.AppendRemainingLength(nil, len(props)), props...)
}

// readPropertyBlock splits a length-prefixed MQTT 5 property block.
func readPropertyBlock(b []byte) (props, rest []byte, err error) {
	n, rest, err := mqtt.ReadVariableByteInt(b)
	if err != nil {
		return nil, nil, err
	}
	if len(rest) < n {
		return nil, nil, mqtt.ErrMalformedPacket
	}
	return rest[:n], rest[n:], nil
}
