package gmqx

import "context"

// ConnectInfo describes an MQTT CONNECT request.
type ConnectInfo struct {
	ClientID     string
	Username     string
	Password     []byte
	KeepAlive    uint16
	CleanSession bool
	// SessionExpiry is the MQTT 5 Session Expiry Interval in seconds. It is
	// zero for MQTT 3.1.1 and for MQTT 5 clients that omit the property.
	SessionExpiry uint32
	WillTopic     string
	WillPayload   []byte
	RemoteAddr    string
}

// AuthManager validates a connecting client.
type AuthManager interface {
	Authenticate(ctx context.Context, info ConnectInfo) error
	// RequiresOffload is kept for SPI compatibility; Go runs each connection in
	// its own goroutine, so callers may ignore it.
	RequiresOffload() bool
}

// AclAction is a topic permission action.
type AclAction int

const (
	AclSubscribe AclAction = iota
	AclPublish
)

// SessionInfo identifies the session for ACL checks.
type SessionInfo struct {
	ClientID string
	Username string
}

// AclManager authorizes subscribe/publish.
type AclManager interface {
	Check(ctx context.Context, session SessionInfo, topic string, action AclAction) bool
	RequiresOffload() bool
}

// ConnectionID identifies one physical connection. A client that reconnects
// gets a new ConnectionID even when it keeps its ClientID.
type ConnectionID string

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

// MQTT 5 reason codes used when a sink does not pick one.
const (
	ReasonUnspecified   byte = 0x80
	ReasonNotAuthorized byte = 0x87
	ReasonQuotaExceeded byte = 0x97
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

// MetricsManager is the metrics SPI.
type MetricsManager interface {
	IncrementConnections()
	DecrementConnections()
	IncrementPublishedMessages()
	IncrementSubscribeEvents()
	DecrementSubscribeEvents()
	IncrementDisconnectEvents()
	RecordDroppedMessage(reason string)
	RecordOverflow(sinkName string)
	RecordQueueDepth(queueName string, depth int)
	RecordConnections(count int)
}

// Interceptor can observe/drop a publish before routing.
type Interceptor interface {
	OnPublish(ctx context.Context, clientID, topic string, payload []byte, qos byte) bool // false = drop
}

// AllowAll permits every connection/subscription/publish.
type AllowAll struct{}

func (AllowAll) Authenticate(context.Context, ConnectInfo) error            { return nil }
func (AllowAll) RequiresOffload() bool                                      { return false }
func (AllowAll) Check(context.Context, SessionInfo, string, AclAction) bool { return true }

// DenyAll rejects every connection.
type DenyAll struct{}

func (DenyAll) Authenticate(context.Context, ConnectInfo) error { return ErrUnauthorized }
func (DenyAll) RequiresOffload() bool                           { return false }

// NoopMetrics implements MetricsManager.
type NoopMetrics struct{}

func (NoopMetrics) IncrementConnections()        {}
func (NoopMetrics) DecrementConnections()        {}
func (NoopMetrics) IncrementPublishedMessages()  {}
func (NoopMetrics) IncrementSubscribeEvents()    {}
func (NoopMetrics) DecrementSubscribeEvents()    {}
func (NoopMetrics) IncrementDisconnectEvents()   {}
func (NoopMetrics) RecordDroppedMessage(string)  {}
func (NoopMetrics) RecordOverflow(string)        {}
func (NoopMetrics) RecordQueueDepth(string, int) {}
func (NoopMetrics) RecordConnections(int)        {}
