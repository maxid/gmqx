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

// Authenticator validates a connecting client.
type Authenticator interface {
	Authenticate(ctx context.Context, info ConnectInfo) error
}

// ACL authorizes subscribe/publish operations.
type ACL interface {
	CanSubscribe(ctx context.Context, clientID, filter string) bool
	CanPublish(ctx context.Context, clientID, topic string) bool
}

// EventSink receives broker lifecycle and message events.
type EventSink interface {
	OnConnect(ctx context.Context, info ConnectInfo)
	OnDisconnect(ctx context.Context, clientID string, err error)
	OnPublish(ctx context.Context, clientID, topic string, payload []byte, qos byte)
}

// AllowAll permits every connection/subscription/publish.
type AllowAll struct{}

// Authenticate implements Authenticator.
func (AllowAll) Authenticate(context.Context, ConnectInfo) error { return nil }

// CanSubscribe implements ACL.
func (AllowAll) CanSubscribe(context.Context, string, string) bool { return true }

// CanPublish implements ACL.
func (AllowAll) CanPublish(context.Context, string, string) bool { return true }

// DenyAll rejects every connection.
type DenyAll struct{}

// Authenticate implements Authenticator.
func (DenyAll) Authenticate(context.Context, ConnectInfo) error { return ErrUnauthorized }
