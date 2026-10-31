package gmqx

import (
	"context"

	"github.com/maxid/gnety/codec/mqtt"
)

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
	// ReasonSessionTakenOver is the DISCONNECT a server sends to the connection a
	// new CONNECT displaced (MQTT 5 §3.1.2.5, 4.13).
	ReasonSessionTakenOver byte = 0x8E
	ReasonQuotaExceeded    byte = 0x97
	// ReasonSubscriptionIdentifiersNotSupported is the DISCONNECT a SUBSCRIBE
	// carrying a Subscription Identifier gets, because the CONNACK advertised
	// Subscription Identifier Available 0.
	ReasonSubscriptionIdentifiersNotSupported byte = 0x9A
	// ReasonSharedSubscriptionsNotSupported is the per-filter SUBACK code of a
	// "$share/..." filter, because the CONNACK advertised Shared Subscription
	// Available 0 (MQTT 5 §3.9.3).
	ReasonSharedSubscriptionsNotSupported byte = 0x9E
	// ReasonConnectionRateExceeded is the CONNACK of a CONNECT refused by the
	// Config.NotKickSeconds anti-flap window (MQTT 5 §3.2.2.2).
	ReasonConnectionRateExceeded byte = 0x9F
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

// MetricsManager is the metrics SPI.
//
// RecordDroppedMessage observes something the broker refused to deliver or keep.
// The reasons this package records are "acl_publish", "acl_subscribe",
// "acl_timeout" (an AclManager.Check call exceeded Config.AuthTimeout and counts
// as a denial), "auth_timeout" (an AuthManager.Authenticate call exceeded it and
// counts as a failed authentication), "interceptor", "sink_rejected",
// "sink_retryable", "max_retain", "retained_expired" (a retained message was not
// replayed because its Message Expiry Interval elapsed, and it was deleted),
// "max_connections", "max_subscriptions", "connection_rate_limit",
// "takeover_rate_limit" (a CONNECT inside the Config.NotKickSeconds window would
// have displaced a live connection), "shared_subscription" (a "$share/..."
// filter was refused), "subscription_identifier" (a SUBSCRIBE carrying one was
// rejected), "cluster_namespace", "inflight_overflow" (a QoS 1/2 delivery found
// the connection's inflight window and the queue behind it both full, so it was
// dropped), "inflight_qos2_full" (an inbound QoS 2 PUBLISH over the window),
// "retry_exhausted" (an outbound exchange used up its retransmissions),
// "outbound_queue_full" (a QoS 0 PUBLISH was dropped because the connection's
// writer queue was full), "slow_consumer" (a packet that must not be dropped — a
// QoS 1/2 PUBLISH, an acknowledgement — hit a full writer queue, so the
// connection is disconnected), "offline_queue_full" (the per-session cap dropped
// the oldest message), "offline_queue_total" (the global cap rejected the newest
// one), "offline_queue_corrupt", "offline_session_missing" and
// "session_expired".
// RecordQueueDepth("offline", n) reports the messages currently queued for
// offline sessions across the broker; RecordOverflow reports a limit that was hit
// without losing a message ("connections", "inflight" — a delivery was dropped
// as well, see "inflight_overflow" —, "inflight_qos2", "outbound_queue",
// "will_delay" — a Will Delay Interval was clamped to Config.MaxWillDelayInterval
// — and "packet_ids" — a delivery degraded to QoS 0 because no packet id was
// free).
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

// Publication is one accepted publication the broker routes. It is the payload
// of the routing SPI: QoS and Retain are the values the client published, Dup
// reports a retransmission, and FromClientID/FromConnectionID name the session
// that produced it (both are empty for an event that came from a peer node).
// Props is the MQTT 5 PUBLISH property block without its length prefix, nil for
// MQTT 3.1.1 traffic.
type Publication struct {
	Namespace        string
	Topic            string
	Payload          []byte
	QoS              byte
	Retain           bool
	Dup              bool
	FromClientID     string
	FromConnectionID string
	Props            []byte

	// props is the typed mirror of Props for publications the broker built
	// itself. SendLocal uses it directly instead of decoding Props once per
	// local target; a Publication built by a dispatcher only carries Props.
	props    mqtt.Properties
	propsSet bool
}

// Dispatcher is the single decision point for where an accepted publication
// goes. The broker authorizes, runs every Interceptor and calls the EventSink
// first; only a publication the sink accepted reaches Dispatch. Suppressing it
// here therefore suppresses delivery, not the acknowledgement the sink already
// granted.
//
// A Dispatcher must not re-implement topic matching. It routes through the
// broker's read-only seam instead:
//
//	targets := broker.LocalTargets(pub)        // sessions whose subscriptions match
//	for _, t := range targets {
//	    _ = broker.SendLocal(ctx, t, pub)      // one PUBLISH to one of them
//	}
//	return broker.SpreadPublication(ctx, pub)  // forward to the cluster backend
//
// A dispatcher that needs the seam implements DispatcherBinder; the broker binds
// itself before the first publication is routed. Retained state is not part of
// routing: the broker stores an accepted retained publication before Dispatch, so
// a dispatcher decides where live publications go, not what is retained.
//
// Config.Dispatcher == nil selects DefaultDispatcher, which is the behavior of a
// broker without the SPI: local delivery plus a cluster spread.
type Dispatcher interface {
	Dispatch(ctx context.Context, pub Publication) error
}

// DispatcherBinder is implemented by a Dispatcher that needs the broker's
// routing seam (LocalTargets, SendLocal, SpreadPublication). The broker calls
// Bind once, from New, before any connection can publish.
type DispatcherBinder interface {
	Bind(b *Broker)
}

// LocalTarget is one local session a publication matches. Its fields are
// read-only reporting; pass the value back to Broker.SendLocal to write the
// PUBLISH. A LocalTarget is only valid for the broker that produced it.
type LocalTarget struct {
	ClientID     string
	ConnectionID ConnectionID
	// QoS is min(publication QoS, highest matching subscription QoS).
	QoS byte
	// Retain is the retain flag to send: it is set only for a retained
	// publication delivered to a subscription with retain-as-published.
	Retain bool

	client     *client
	sessionKey string
}

// Offline reports whether the target is a persistent session with no live
// connection. SendLocal queues the publication for such a target instead of
// writing it, and the queue drains right after the session's next CONNACK;
// ConnectionID is empty for it, because there is no connection to name.
func (t LocalTarget) Offline() bool { return t.client == nil && t.sessionKey != "" }

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
