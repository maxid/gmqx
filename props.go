package gmqx

import (
	"bufio"
	"bytes"
	"fmt"

	"github.com/maxid/gnety/codec/mqtt"
)

// This file is the seam between the typed MQTT 5 property set of
// gnety/codec/mqtt and the opaque property bytes gmqx's public SPI exposes:
// PublishEvent.Properties and ClusterMessage.Props carry one property block
// without its length prefix, the shape they had before the typed codec existed.
//
// Both directions run through the codec's own encoder and decoder, so gmqx
// keeps no property table, no property identifier and no value layout of its
// own: the only thing this file knows is that a property block ends up in the
// variable header of a PUBLISH.

// topicAliasMaximum is the highest topic alias this broker accepts, and the
// value its MQTT 5 CONNACK advertises.
const topicAliasMaximum uint16 = 16

// connackProperties builds the MQTT 5 CONNACK capability set gmqx advertises.
//
// Maximum QoS is deliberately absent: the specification allows only 0 or 1
// there, and its absence already means "QoS 2", which is what this broker
// offers. Retain Available, Wildcard/Subscription Identifier/Shared
// Subscription Available state the other capabilities; the four of them are
// implemented (shared subscription routing lives in shared.go, subscription
// identifiers in subscription_id.go), so all four are advertised as available.
//
// Maximum Packet Size and Receive Maximum are the broker's own inbound limits,
// advertised so a client can size its packets and its QoS 1/2 pipeline: the first
// is Config.MaxPacketSize (the codec enforces it on every inbound packet, see
// client.readMessage), the second is Broker.receiveMaximum, the number of
// unacknowledged inbound QoS 1/2 exchanges the broker really allows (see
// client.handlePublish). Advertising either as a larger number than the broker
// enforces would invite a client into a window it cannot use.
//
// authMethod is the enhanced authentication method the connection negotiated and
// authData the final data of its exchange; both are empty for a connection that
// authenticated any other way. MQTT 5 §3.2.2.3.8 makes the Authentication Method
// an echo of the client's CONNECT: it may only be sent when the client named a
// method, and it must be that method, so this function never advertises a method
// the client did not use. Authentication Data without a method is a Protocol
// Error, which is why it is written only together with one.
func connackProperties(assignedClientID string, maxPacketSize uint32, receiveMaximum uint16, authMethod string, authData []byte) mqtt.Properties {
	yes := true
	props := mqtt.Properties{
		RetainAvailable:                 &yes,
		WildcardSubscriptionAvailable:   &yes,
		SubscriptionIdentifierAvailable: &yes,
		SharedSubscriptionAvailable:     &yes,
		MaximumPacketSize:               maxPacketSize,
		ReceiveMaximum:                  receiveMaximum,
		TopicAliasMaximum:               topicAliasMaximum,
		AssignedClientID:                assignedClientID,
	}
	if authMethod != "" {
		props.AuthenticationMethod = authMethod
		if len(authData) > 0 {
			props.AuthenticationData = authData
		}
	}
	return props
}

// encodePropertyBlock returns the PUBLISH property bytes of p, without the
// property length prefix. Properties the codec refuses for a PUBLISH are
// reported as an error instead of being dropped.
func encodePropertyBlock(p mqtt.Properties) ([]byte, error) {
	var buf bytes.Buffer
	pub := &mqtt.Publish{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}, Properties: p}
	if err := mqtt.Encode(&buf, pub); err != nil {
		return nil, err
	}
	_, body, err := mqtt.ReadPacket(bufio.NewReaderSize(&buf, 16), 0)
	if err != nil {
		return nil, err
	}
	// body is the empty topic name of pub followed by the property block:
	// property length, then that many property bytes.
	n, rest, err := mqtt.ReadVariableByteInt(body[2:])
	if err != nil {
		return nil, err
	}
	if n != len(rest) {
		return nil, fmt.Errorf("%w: property block of %d bytes with %d trailing", ErrMalformedPacket, n, len(rest)-n)
	}
	return rest, nil
}

// decodePropertyBlock parses property bytes as they arrive from a sink, a
// retained message or a peer node.
func decodePropertyBlock(raw []byte) (mqtt.Properties, error) {
	body := make([]byte, 0, 2+4+len(raw))
	body = append(body, 0, 0) // empty topic name
	body = mqtt.AppendRemainingLength(body, len(raw))
	body = append(body, raw...)
	frame := mqtt.AppendRemainingLength([]byte{mqtt.TypePublish << 4}, len(body))
	frame = append(frame, body...)

	d := mqtt.Decoder{Version: mqtt.Version5}
	msg, err := d.Decode(bufio.NewReaderSize(bytes.NewReader(frame), len(frame)))
	if err != nil {
		return mqtt.Properties{}, err
	}
	pub, ok := msg.(*mqtt.Publish)
	if !ok {
		return mqtt.Properties{}, fmt.Errorf("%w: unexpected %T", ErrMalformedPacket, msg)
	}
	return pub.Properties, nil
}
