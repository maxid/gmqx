package hook

import "context"

// ConnectionID identifies one physical connection. A client that reconnects
// gets a new ConnectionID even when it keeps its ClientID.
type ConnectionID string

// ConnInfo is the identity of the connection a hook is running for.
//
// The reference broker in this broker family keeps the same information in a
// request-scoped holder bound to the executing thread. Go has no thread-local
// state, so the Go equivalent is a context value: the broker attaches a ConnInfo
// to the connection-scoped context it hands to every hook, and ConnFromContext
// reads it back. The context is created when the connection is accepted, is
// enriched once at CONNECT (Config.ConnContext), is reused for the whole
// connection — CONNECT, every PUBLISH, SUBSCRIBE, ping and the disconnect hooks —
// and is cancelled when the connection ends.
type ConnInfo struct {
	// ClientID is the MQTT client identifier of the connection.
	ClientID string
	// ConnectionID is the physical connection, unique per reconnect.
	ConnectionID string
	// Namespace is the isolated scope the connection belongs to; the empty
	// string is the default namespace.
	Namespace string
	// RemoteAddr is the peer address as the transport reports it.
	RemoteAddr string
	// ProtocolVersion is the negotiated protocol level: mqtt.Version311 (4) or
	// mqtt.Version5 (5). MQTT 3.1 clients use the 3.1.1 layout and report 4.
	ProtocolVersion byte
	// Transport is the listener that accepted the connection: "tcp", "tls", "ws"
	// or "wss" — the names Addrs() reports. A hook can therefore tell a device on
	// the TLS port from one on the WebSocket port without inspecting the broker's
	// configuration. It is empty when the connection was not accepted by a broker
	// listener (an embedder that handed one over itself).
	Transport string
}

type connInfoKey struct{}

// ContextWithConn returns a copy of ctx carrying info. The broker calls it when
// it builds the connection-scoped context; a Dispatcher, EventSink, Interceptor,
// AuthManager or AclManager implementation reads the value with
// ConnFromContext. The value is informational: it never replaces the
// cancellation or the deadlines of ctx.
func ContextWithConn(ctx context.Context, info ConnInfo) context.Context {
	return context.WithValue(ctx, connInfoKey{}, info)
}

// ConnFromContext returns the ConnInfo the broker attached to ctx, and reports
// whether ctx carries one. A context the broker did not create (a background or
// shutdown context) has no ConnInfo.
func ConnFromContext(ctx context.Context) (ConnInfo, bool) {
	info, ok := ctx.Value(connInfoKey{}).(ConnInfo)
	return info, ok
}
