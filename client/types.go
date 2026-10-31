package client

import (
	"errors"
	"fmt"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// Errors a caller can test with errors.Is. A failure that carries an MQTT 5
// reason code is a *ReasonError instead.
var (
	// ErrClosed reports a call on a client that Close ended.
	ErrClosed = errors.New("gmqx/client: client is closed")
	// ErrNotConnected reports a call that needs a live connection while the
	// client is disconnected (the initial connect failed, a reconnect is running,
	// or the client gave up reconnecting).
	ErrNotConnected = errors.New("gmqx/client: not connected")
	// ErrDisconnected reports a call whose connection ended before the exchange
	// finished and that will not be resumed (a reconnect is disabled, the
	// retransmission limit was reached, or the exchange cannot be replayed).
	ErrDisconnected = errors.New("gmqx/client: connection lost")
	// ErrPacketTooLarge reports a packet larger than the Maximum Packet Size the
	// server advertised in its CONNACK. It is returned to the caller; the packet
	// is never silently dropped or truncated.
	ErrPacketTooLarge = errors.New("gmqx/client: packet exceeds the server maximum packet size")
	// ErrRetryLimit reports a QoS 1/2 exchange that used up Config.MaxRetries
	// retransmissions without being confirmed.
	ErrRetryLimit = errors.New("gmqx/client: retransmission limit reached")
	// ErrNoPacketID reports that all 65535 packet identifiers are in use.
	ErrNoPacketID = errors.New("gmqx/client: no packet identifier available")
	// ErrInvalidConfig reports a Config a Client cannot be built from.
	ErrInvalidConfig = errors.New("gmqx/client: invalid configuration")
	// ErrBadQoS reports a QoS outside 0..2.
	ErrBadQoS = errors.New("gmqx/client: invalid QoS")
	// ErrBadTopic reports an empty topic or a topic that contains a wildcard
	// where a topic name (not a filter) is required.
	ErrBadTopic = errors.New("gmqx/client: invalid topic")
	// ErrProtocol reports an MQTT protocol violation by the server, such as a
	// SUBACK with the wrong number of reason codes. The connection is closed
	// after it.
	ErrProtocol = errors.New("gmqx/client: protocol violation")
)

// errServerDisconnect marks a connection a server ended with DISCONNECT. Such a
// connection is not a transport failure: the client does not reconnect after it.
var errServerDisconnect = errors.New("gmqx/client: server sent DISCONNECT")

// errKeepAliveTimeout reports a PINGRESP that did not arrive in time.
var errKeepAliveTimeout = errors.New("gmqx/client: keepalive timeout, PINGRESP not received")

// ReasonError is an MQTT reason code a server sent. Packet names the packet that
// carried it ("CONNACK", "PUBACK", "PUBREC", "UNSUBACK", "DISCONNECT"), Code is
// the raw reason code (a 3.1.1 return code for CONNACK) and Reason is the
// server's Reason String property when it sent one.
type ReasonError struct {
	// Packet is the control packet that carried the reason code.
	Packet string
	// Code is the MQTT reason code, or the 3.1.1 return code for CONNACK.
	Code byte
	// Reason is the server's Reason String, "" when it sent none.
	Reason string
}

// Error implements error.
func (e *ReasonError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("gmqx/client: %s %s 0x%02x (%s): %s", e.Packet, e.intro(), e.Code, e.name(), e.Reason)
	}
	return fmt.Sprintf("gmqx/client: %s %s 0x%02x (%s)", e.Packet, e.intro(), e.Code, e.name())
}

// intro is "return code" for the MQTT 3.1.1 CONNACK return codes and "reason"
// for every MQTT 5 reason code.
func (e *ReasonError) intro() string {
	if e.Packet == "CONNACK" && e.Code >= 1 && e.Code <= 5 {
		return "return code"
	}
	return "reason"
}

// name is the specification name of the code, aware of the MQTT 3.1.1 CONNACK
// return codes, which overlap numerically with the MQTT 5 granted-QoS codes.
func (e *ReasonError) name() string {
	if e.Packet == "CONNACK" && e.Code >= 1 && e.Code <= 5 {
		switch e.Code {
		case 1:
			return "unacceptable protocol version"
		case 2:
			return "identifier rejected"
		case 3:
			return "server unavailable"
		case 4:
			return "bad user name or password"
		case 5:
			return "not authorized"
		}
	}
	return reasonName(e.Code)
}

// reasonName is the specification name of a reason code. It is best-effort: an
// unknown or vendor-specific code keeps its numeric form in the message.
func reasonName(code byte) string {
	switch code {
	case 0x00:
		return "success"
	case 0x01:
		return "granted QoS 1"
	case 0x02:
		return "granted QoS 2"
	case 0x04:
		return "disconnect with will"
	case 0x10:
		return "no matching subscribers"
	case 0x11:
		return "no subscription existed"
	case 0x18:
		return "continue authentication"
	case 0x19:
		return "re-authenticate"
	case 0x80:
		return "unspecified error"
	case 0x81:
		return "malformed packet"
	case 0x82:
		return "protocol error"
	case 0x83:
		return "implementation specific error"
	case 0x84:
		return "unsupported protocol version"
	case 0x85:
		return "client identifier not valid"
	case 0x86:
		return "bad user name or password"
	case 0x87:
		return "not authorized"
	case 0x88:
		return "server unavailable"
	case 0x89:
		return "server busy"
	case 0x8A:
		return "banned"
	case 0x8B:
		return "server shutting down"
	case 0x8C:
		return "bad authentication method"
	case 0x8D:
		return "keep alive timeout"
	case 0x8E:
		return "session taken over"
	case 0x8F:
		return "topic filter invalid"
	case 0x90:
		return "topic name invalid"
	case 0x91:
		return "packet identifier in use"
	case 0x92:
		return "packet identifier not found"
	case 0x93:
		return "receive maximum exceeded"
	case 0x94:
		return "topic alias invalid"
	case 0x95:
		return "packet too large"
	case 0x96:
		return "message rate too high"
	case 0x97:
		return "quota exceeded"
	case 0x98:
		return "administrative action"
	case 0x99:
		return "payload format invalid"
	case 0x9A:
		return "retain not supported"
	case 0x9B:
		return "QoS not supported"
	case 0x9C:
		return "use another server"
	case 0x9D:
		return "server moved"
	case 0x9E:
		return "shared subscriptions not supported"
	case 0x9F:
		return "connection rate exceeded"
	case 0xA0:
		return "maximum connect time"
	case 0xA1:
		return "subscription identifiers not supported"
	case 0xA2:
		return "wildcard subscriptions not supported"
	}
	return "reason"
}

// serverDisconnectError is the error OnDisconnect receives when a server ended
// the connection with DISCONNECT. It matches errServerDisconnect and exposes the
// reason code as a *ReasonError.
type serverDisconnectError struct {
	Reason *ReasonError
}

// Error implements error.
func (e *serverDisconnectError) Error() string {
	if e.Reason == nil {
		return errServerDisconnect.Error()
	}
	return fmt.Sprintf("%s: %v", errServerDisconnect, e.Reason)
}

// Unwrap exposes both the sentinel and the reason error to errors.Is/As.
func (e *serverDisconnectError) Unwrap() []error {
	if e.Reason == nil {
		return []error{errServerDisconnect}
	}
	return []error{errServerDisconnect, e.Reason}
}

// Message is one inbound application message.
type Message struct {
	// Topic is the topic name the server delivered the message to.
	Topic string
	// Payload is the message body. It is owned by the client and only valid for
	// the duration of the OnMessage call; a callback that keeps it must copy it.
	Payload []byte
	// QoS is the delivery quality: 0, 1 or 2.
	QoS byte
	// Retain reports the retain flag of the packet (a retained replay).
	Retain bool
	// Duplicate reports the DUP flag: the server is retransmitting the PUBLISH.
	Duplicate bool
	// Properties holds the MQTT 5 PUBLISH properties. It is empty on MQTT 3.1.1.
	// Topic Alias is resolved and cleared before delivery; Subscription
	// Identifiers are kept.
	Properties mqtt.Properties
}

// Subscription is one filter of a Subscribe call.
type Subscription struct {
	// Filter is the topic filter, wildcards included.
	Filter string
	// QoS is the maximum delivery quality requested, 0 to 2. The SUBACK reason
	// code reports what the server granted.
	QoS byte
	// NoLocal asks the server not to send the client its own publications (MQTT 5
	// only).
	NoLocal bool
	// RetainAsPublished asks the server to keep the retain flag as published (MQTT
	// 5 only).
	RetainAsPublished bool
	// RetainHandling selects the retained-message behaviour at subscribe time:
	// 0 send always, 1 send only on a new subscription, 2 never (MQTT 5 only).
	RetainHandling byte
	// SubscriptionIdentifier is the MQTT 5 Subscription Identifier to register
	// (0 means none). It is a SUBSCRIBE property, so a call whose subscriptions
	// carry different identifiers is split into one SUBSCRIBE packet per
	// identifier, preserving the order of the filters.
	SubscriptionIdentifier uint32
	// Properties carries the MQTT 5 SUBSCRIBE properties of this filter. Only
	// UserProperties is used; a packet is built from the first subscription of
	// each SubscriptionIdentifier group. It is ignored on MQTT 3.1.1.
	Properties mqtt.Properties
}

// Will is the last will and testament a client registers in CONNECT.
type Will struct {
	// Topic is the topic the will is published to.
	Topic string
	// Payload is the will body.
	Payload []byte
	// QoS is the delivery quality of the published will.
	QoS byte
	// Retain asks the server to keep the will as a retained message.
	Retain bool
	// Properties holds the MQTT 5 will properties (will delay interval, content
	// type, response topic, correlation data, user properties, message expiry).
	Properties mqtt.Properties
}

// PublishOptions is one publication for PublishMessage.
type PublishOptions struct {
	// Topic is the topic name; it must not contain wildcards.
	Topic string
	// Payload is the message body.
	Payload []byte
	// QoS is 0, 1 or 2.
	QoS byte
	// Retain asks the server to keep the message as the retained message.
	Retain bool
	// Properties holds the MQTT 5 PUBLISH properties a client may set: content
	// type, response topic, correlation data, user properties, payload format and
	// message expiry. Topic Alias and Subscription Identifiers are not client-set
	// properties; this client clears them instead of sending them.
	Properties mqtt.Properties
}

// ServerInfo is what the last CONNACK advertised. It is a snapshot, so a caller
// may keep it after the connection has moved on.
type ServerInfo struct {
	// SessionPresent is the Session Present flag of the CONNACK: the server
	// resumed the session instead of starting a new one.
	SessionPresent bool
	// AssignedClientID is the client identifier a server assigned to an empty
	// CONNECT client id (MQTT 5 only), "" when the client supplied its own.
	AssignedClientID string
	// ReceiveMaximum is the server's Receive Maximum, the number of
	// unacknowledged QoS 1/2 PUBLISHes it accepts. It is 65535 when the property
	// was absent.
	ReceiveMaximum uint16
	// MaximumPacketSize is the largest packet the server accepts, 0 when it
	// advertised no limit.
	MaximumPacketSize uint32
	// ServerKeepAlive is the keepalive the server overrode, 0 when it did not.
	ServerKeepAlive time.Duration
	// MaximumQoS is the highest QoS the server supports; it is 2 when the
	// property was absent (the protocol default).
	MaximumQoS byte
	// RetainAvailable reports whether the server supports retained messages.
	RetainAvailable bool
	// WildcardSubscriptionAvailable reports whether wildcard filters are
	// supported.
	WildcardSubscriptionAvailable bool
	// SubscriptionIdentifierAvailable reports whether subscription identifiers
	// are supported.
	SubscriptionIdentifierAvailable bool
	// SharedSubscriptionAvailable reports whether shared subscriptions are
	// supported.
	SharedSubscriptionAvailable bool
	// TopicAliasMaximum is the highest topic alias the server accepts, 0 when it
	// accepts none.
	TopicAliasMaximum uint16
	// ResponseInfo is the server's Response Information, "" when it sent none.
	ResponseInfo string
	// ServerReference is the server's Server Reference (use another server), ""
	// when it sent none.
	ServerReference string
}
