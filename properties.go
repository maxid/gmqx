package gmqx

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// MQTT 5 property identifiers.
const (
	propPayloadFormat        = 0x01
	propMessageExpiry        = 0x02
	propContentType          = 0x03
	propResponseTopic        = 0x08
	propCorrelationData      = 0x09
	propSubscriptionID       = 0x0B
	propSessionExpiry        = 0x11
	propAssignedClientID     = 0x12
	propServerKeepAlive      = 0x13
	propAuthMethod           = 0x15
	propAuthData             = 0x16
	propRequestProblemInfo   = 0x17
	propWillDelay            = 0x18
	propRequestResponseInfo  = 0x19
	propResponseInfo         = 0x1A
	propServerReference      = 0x1C
	propReasonString         = 0x1F
	propReceiveMaximum       = 0x21
	propTopicAliasMaximum    = 0x22
	propTopicAlias           = 0x23
	propMaximumQoS           = 0x24
	propRetainAvailable      = 0x25
	propUserProperty         = 0x26
	propMaximumPacketSize    = 0x27
	propWildcardSubAvailable = 0x28
	propSubIDAvailable       = 0x29
	propSharedSubAvailable   = 0x2A
)

type connectProperties struct {
	SessionExpiry     uint32
	ReceiveMaximum    uint16
	MaximumPacketSize uint32
	TopicAliasMaximum uint16
}

// readPropertyBlock reads a property-length prefixed block, returning the raw
// properties and the remaining bytes.
func readPropertyBlock(b []byte) (props, rest []byte, err error) {
	n, rest, err := readVariableByteInt(b)
	if err != nil {
		return nil, nil, err
	}
	if len(rest) < n {
		return nil, nil, ErrMalformedPacket
	}
	return rest[:n], rest[n:], nil
}

// lengthOfProperty reports the byte length of one property value.
func lengthOfProperty(id byte, b []byte) (int, error) {
	fixed := func(n int) (int, error) {
		if len(b) < n {
			return 0, ErrMalformedPacket
		}
		return n, nil
	}
	switch id {
	case propPayloadFormat, propRequestProblemInfo, propRequestResponseInfo,
		propRetainAvailable, propWildcardSubAvailable, propSubIDAvailable,
		propSharedSubAvailable, propMaximumQoS:
		return fixed(1)
	case propServerKeepAlive, propReceiveMaximum, propTopicAliasMaximum, propTopicAlias:
		return fixed(2)
	case propMessageExpiry, propSessionExpiry, propWillDelay, propMaximumPacketSize:
		return fixed(4)
	case propSubscriptionID:
		_, rest, err := readVariableByteInt(b)
		if err != nil {
			return 0, err
		}
		return len(b) - len(rest), nil
	case propContentType, propResponseTopic, propAssignedClientID, propAuthMethod,
		propResponseInfo, propServerReference, propReasonString:
		if len(b) < 2 {
			return 0, ErrMalformedPacket
		}
		n := int(binary.BigEndian.Uint16(b[:2]))
		if len(b) < 2+n {
			return 0, ErrMalformedPacket
		}
		return 2 + n, nil
	case propCorrelationData, propAuthData:
		if len(b) < 2 {
			return 0, ErrMalformedPacket
		}
		n := int(binary.BigEndian.Uint16(b[:2]))
		if len(b) < 2+n {
			return 0, ErrMalformedPacket
		}
		return 2 + n, nil
	case propUserProperty:
		if len(b) < 4 {
			return 0, ErrMalformedPacket
		}
		k := int(binary.BigEndian.Uint16(b[:2]))
		if len(b) < 2+k+2 {
			return 0, ErrMalformedPacket
		}
		v := int(binary.BigEndian.Uint16(b[2+k : 4+k]))
		if len(b) < 4+k+v {
			return 0, ErrMalformedPacket
		}
		return 4 + k + v, nil
	default:
		return 0, fmt.Errorf("%w: unknown property 0x%02x", ErrMalformedPacket, id)
	}
}

func parseConnectProperties(block []byte) (connectProperties, error) {
	var out connectProperties
	for len(block) > 0 {
		id := block[0]
		block = block[1:]
		n, err := lengthOfProperty(id, block)
		if err != nil {
			return out, err
		}
		val := block[:n]
		switch id {
		case propSessionExpiry:
			out.SessionExpiry = binary.BigEndian.Uint32(val)
		case propReceiveMaximum:
			out.ReceiveMaximum = binary.BigEndian.Uint16(val)
		case propMaximumPacketSize:
			out.MaximumPacketSize = binary.BigEndian.Uint32(val)
		case propTopicAliasMaximum:
			out.TopicAliasMaximum = binary.BigEndian.Uint16(val)
		}
		block = block[n:]
	}
	return out, nil
}

// filterPublishProperties copies publish properties except Topic Alias and
// Subscription Identifier (both are hop-local), and extracts the Topic Alias.
func filterPublishProperties(block []byte) (filtered []byte, alias *uint16, err error) {
	filtered = make([]byte, 0, len(block))
	for len(block) > 0 {
		id := block[0]
		block = block[1:]
		n, err := lengthOfProperty(id, block)
		if err != nil {
			return nil, nil, err
		}
		val := block[:n]
		switch id {
		case propTopicAlias:
			v := binary.BigEndian.Uint16(val)
			alias = &v
		case propSubscriptionID:
			// hop-local, not forwarded
		default:
			filtered = append(filtered, id)
			filtered = append(filtered, val...)
		}
		block = block[n:]
	}
	return filtered, alias, nil
}

func appendPropertyByte(dst []byte, id, value byte) []byte {
	return append(dst, id, value)
}

func appendPropertyUint16(dst []byte, id byte, value uint16) []byte {
	return append(dst, id, byte(value>>8), byte(value))
}

func appendPropertyUint32(dst []byte, id byte, value uint32) []byte {
	return append(dst, id, byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
}

func appendPropertyString(dst []byte, id byte, value string) []byte {
	dst = append(dst, id, byte(len(value)>>8), byte(len(value)))
	return append(dst, value...)
}

func prefixPropertyLength(props []byte) []byte {
	out := make([]byte, 0, 4+len(props))
	out = appendRemainingLength(out, len(props))
	return append(out, props...)
}

func buildConnackProperties(assignedClientID string, maxPacket uint32, topicAliasMax uint16) []byte {
	var props []byte
	props = appendPropertyByte(props, propRetainAvailable, 1)
	props = appendPropertyByte(props, propWildcardSubAvailable, 1)
	props = appendPropertyByte(props, propSubIDAvailable, 0)
	props = appendPropertyByte(props, propSharedSubAvailable, 0)
	props = appendPropertyByte(props, propMaximumQoS, 2)
	props = appendPropertyUint32(props, propMaximumPacketSize, maxPacket)
	props = appendPropertyUint16(props, propTopicAliasMaximum, topicAliasMax)
	if assignedClientID != "" {
		props = appendPropertyString(props, propAssignedClientID, assignedClientID)
	}
	return prefixPropertyLength(props)
}

var errNoAlias = errors.New("gmqx: topic alias not found")
