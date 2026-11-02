package hook

import "context"

// Disposition is the sink's verdict on an inbound PUBLISH.
type Disposition byte

const (
	// PublishAccepted means the message is durable enough to acknowledge.
	PublishAccepted Disposition = iota
	// PublishRetryable means the broker must not acknowledge; the client may
	// retry later. A bounded wait that expires is Retryable.
	PublishRetryable
	// PublishRejected means the message must be neither delivered nor
	// acknowledged.
	PublishRejected
)

// PublishResult is what the sink hands back. ReasonCode 0 lets the broker choose
// a default for the disposition.
type PublishResult struct {
	Disposition Disposition
	ReasonCode  byte
}

// Accepted reports whether the publish may be acknowledged and delivered.
func (r PublishResult) Accepted() bool { return r.Disposition == PublishAccepted }

// PublishEvent describes one inbound PUBLISH.
type PublishEvent struct {
	ConnectionID ConnectionID
	ClientID     string
	Topic        string
	Payload      []byte
	QoS          byte
	PacketID     uint16
	Retain       bool
	Dup          bool
	Properties   []byte
}

// EventSink receives broker lifecycle and message events.
//
// OnPublish returns the verdict that decides the acknowledgement: the broker
// sends PUBACK/PUBREC only after an Accepted result. MQTT 3.1.1 has no negative
// PUBACK, so a retryable or rejected publish is simply not acknowledged — never
// acknowledged optimistically.
type EventSink interface {
	OnConnect(ctx context.Context, info ConnectInfo)
	OnDisconnect(ctx context.Context, clientID string, err error)
	OnConnectionLost(ctx context.Context, clientID string, err error)
	OnPublish(ctx context.Context, ev PublishEvent) PublishResult
	OnPing(ctx context.Context, clientID string)
}

// SinkFuncs implements EventSink with function fields, so a consumer implements only the
// events it cares about instead of five methods. A nil field is skipped:
//
//	sink := hook.SinkFuncs{
//		OnPublishFunc: func(ctx context.Context, ev hook.PublishEvent) hook.PublishResult {
//			return hook.PublishResult{Disposition: hook.PublishRejected, ReasonCode: 0x87}
//		},
//	}
//
// The field names carry a Func suffix because a field and a method cannot share a name on
// one type.
type SinkFuncs struct {
	OnConnectFunc        func(ctx context.Context, info ConnectInfo)
	OnDisconnectFunc     func(ctx context.Context, clientID string, err error)
	OnConnectionLostFunc func(ctx context.Context, clientID string, err error)
	OnPublishFunc        func(ctx context.Context, ev PublishEvent) PublishResult
	OnPingFunc           func(ctx context.Context, clientID string)
}

// OnConnect calls OnConnectFunc when it is set.
func (s SinkFuncs) OnConnect(ctx context.Context, info ConnectInfo) {
	if s.OnConnectFunc != nil {
		s.OnConnectFunc(ctx, info)
	}
}

// OnDisconnect calls OnDisconnectFunc when it is set.
func (s SinkFuncs) OnDisconnect(ctx context.Context, clientID string, err error) {
	if s.OnDisconnectFunc != nil {
		s.OnDisconnectFunc(ctx, clientID, err)
	}
}

// OnConnectionLost calls OnConnectionLostFunc when it is set.
func (s SinkFuncs) OnConnectionLost(ctx context.Context, clientID string, err error) {
	if s.OnConnectionLostFunc != nil {
		s.OnConnectionLostFunc(ctx, clientID, err)
	}
}

// OnPublish calls OnPublishFunc when it is set, and accepts the publication otherwise: a
// sink that declares no publish hook has no opinion on delivery.
func (s SinkFuncs) OnPublish(ctx context.Context, ev PublishEvent) PublishResult {
	if s.OnPublishFunc == nil {
		return PublishResult{Disposition: PublishAccepted}
	}
	return s.OnPublishFunc(ctx, ev)
}

// OnPing calls OnPingFunc when it is set.
func (s SinkFuncs) OnPing(ctx context.Context, clientID string) {
	if s.OnPingFunc != nil {
		s.OnPingFunc(ctx, clientID)
	}
}

// MQTT 5 reason codes used when a sink does not pick one.
const (
	ReasonUnspecified byte = 0x80
	// ReasonProtocolError is the DISCONNECT of a packet the specification calls a
	// Protocol Error rather than a malformed one: a Subscription Identifier of 0
	// on a SUBSCRIBE (§3.8.2.1.2) is the case this package answers with it.
	ReasonProtocolError byte = 0x82
	ReasonNotAuthorized byte = 0x87
	// ReasonBadAuthMethod is the CONNACK of a CONNECT that named an MQTT 5
	// Authentication Method this broker does not support, and the DISCONNECT of a
	// connection whose negotiated method it can no longer build an exchange for
	// (MQTT 5 §3.2.2.2, §3.14.2.1).
	ReasonBadAuthMethod byte = 0x8C
	// ReasonSessionTakenOver is the DISCONNECT a server sends to the connection a
	// new CONNECT displaced (MQTT 5 §3.1.2.5, 4.13).
	ReasonSessionTakenOver byte = 0x8E
	ReasonQuotaExceeded    byte = 0x97
	// ReasonConnectionRateExceeded is the CONNACK of a CONNECT refused by the
	// Config.NotKickSeconds anti-flap window (MQTT 5 §3.2.2.2).
	ReasonConnectionRateExceeded byte = 0x9F
)
