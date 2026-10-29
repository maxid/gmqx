package gmqx

import "context"

// ConnectInfo describes an MQTT CONNECT request.
type ConnectInfo struct {
	ClientID     string
	Username     string
	Password     []byte
	KeepAlive    uint16
	CleanSession bool
	WillTopic    string
	WillPayload  []byte
	RemoteAddr   string
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

// EventSink receives broker lifecycle and message events.
type EventSink interface {
	OnConnect(ctx context.Context, info ConnectInfo)
	OnDisconnect(ctx context.Context, clientID string, err error)
	OnConnectionLost(ctx context.Context, clientID string, err error)
	OnPublish(ctx context.Context, clientID, topic string, payload []byte, qos byte)
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
