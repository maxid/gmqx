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
	// AuthMethod is the MQTT 5 Authentication Method the CONNECT named, or ""
	// when the connection negotiates none (always "" on MQTT 3.1.1, which has no
	// property block). A non-empty value selects the enhanced-authentication path:
	// Config.Authenticator builds the AuthExchange of the connection and
	// AuthManager.Authenticate is skipped for it (see AuthExchange).
	AuthMethod string
	// AuthData is the MQTT 5 Authentication Data the CONNECT carried: the client's
	// first message of the exchange, nil when there is none. It is handed to
	// AuthExchange.Begin and means nothing to AuthManager.
	AuthData []byte
}

// AuthManager validates a connecting client.
//
// It is the authentication hook of a CONNECT that carries no MQTT 5
// Authentication Method. A connection that does negotiate a method is
// authenticated by its AuthExchange instead and this hook is not called at all
// (see AuthExchange for the precedence); AllowAll and DenyAll therefore keep
// their meaning for every other connection.
type AuthManager interface {
	Authenticate(ctx context.Context, info ConnectInfo) error
	// RequiresOffload is kept for SPI compatibility; Go runs each connection in
	// its own goroutine, so callers may ignore it.
	RequiresOffload() bool
}

// AuthStatus is the verdict of one step of an AuthExchange.
type AuthStatus int

const (
	// AuthContinue asks for another step: the broker sends AUTH 0x18 (Continue
	// authentication) carrying the step's data and waits for the client's answer,
	// which it hands to AuthExchange.Continue. It is the zero value, so a step an
	// implementation left unset keeps the exchange going instead of admitting the
	// client; the exchange is bounded by MaxAuthSteps.
	AuthContinue AuthStatus = iota
	// AuthSucceeded completes the exchange and admits the connection. Data is the
	// final Authentication Data the broker sends with the CONNACK (or with the
	// AUTH 0x00 of a re-authentication) and Info, when set, is the authenticated
	// identity.
	AuthSucceeded
	// AuthFailed refuses the connection: the broker answers with CONNACK (during
	// CONNECT) or DISCONNECT (on a live connection) carrying ReasonCode and closes
	// it. Data is ignored on a failure.
	AuthFailed
)

// AuthStep is one step of an AuthExchange: what the exchange decided and the
// Authentication Data that goes with it.
type AuthStep struct {
	// Status is what the broker does next.
	Status AuthStatus
	// Data is the Authentication Data of this step: the server's challenge when
	// Status is AuthContinue, the final data that accompanies a successful CONNACK
	// or AUTH 0x00 when it is AuthSucceeded. Empty data omits the property.
	Data []byte
	// Info, on a step that succeeds, is the identity of the authenticated
	// connection: its Username and ClientID replace the CONNECT's when they are
	// set, and an empty field keeps the value the client sent. Every other field
	// is not used, because an AUTH packet can only carry Authentication Method and
	// Authentication Data (see AuthExchange). Info on a continuing or a failing
	// step is ignored.
	Info *ConnectInfo
	// ReasonCode is the reason code of a failed step: the CONNACK reason code
	// during CONNECT, the DISCONNECT reason code on a live connection. 0 selects
	// 0x87 (Not authorized); 0x86 (Bad User Name or Password), 0x87 and 0x8C (Bad
	// authentication method) are the codes that describe an authentication
	// failure.
	ReasonCode byte
}

// AuthExchange is one MQTT 5 enhanced-authentication exchange (AUTH) of one
// connection, in the spirit of the other hooks: the broker owns the packet flow,
// the deadline and the limits, and the implementation only decides the next step.
//
// Lifetime and calls. The broker obtains an exchange from Config.Authenticator
// and drives it from the connection's read path, one call at a time: it waits for
// each step before it reads the next packet, so no step of an exchange ever runs
// concurrently with another packet of the same client:
//
//   - CONNECT: Begin receives the CONNECT's ConnectInfo — with AuthMethod and
//     AuthData filled from the authentication properties of the packet — and its
//     step is the exchange's first verdict. A step that continues makes the
//     broker send AUTH 0x18 with the step's data; each client AUTH 0x18 then calls
//     Continue with the client's Authentication Data. Success sends CONNACK 0x00
//     with the method and the step's data, failure sends CONNACK with
//     step.ReasonCode and closes.
//   - Re-authentication: an AUTH 0x19 on a live connection starts a fresh
//     exchange (a new value from Config.Authenticator, then Begin with the
//     connection's ConnectInfo and the AUTH's Authentication Data, then Continue
//     exactly as above). Success answers AUTH 0x00 and leaves the session — its
//     subscriptions, its queue and its inflight state — untouched; failure ends
//     the connection with a DISCONNECT.
//
// Bounds. One exchange runs at most MaxAuthSteps challenges, and the whole
// CONNECT exchange is inside Config.ConnectTimeout. Every call into the exchange
// is bounded by Config.AuthTimeout: it receives a context carrying the deadline
// (a positive timeout may run the call on a helper goroutine the broker waits
// for), and an expired call is a failed authentication — never a success. An
// error returned by Begin or Continue is a failed authentication answered with
// 0x87; only a returned step can name another reason code.
//
// Identity. A successful step may return a ConnectInfo; that is how an exchange
// authenticates a device under a name the client did not send. The returned
// Username and ClientID win over the CONNECT's, and because the exchange *is* the
// authentication of this connection, AuthManager.Authenticate is skipped for it
// (see AuthManager). Authorization (AclManager) still runs for every publish and
// subscribe, against the identity the exchange established.
//
// The Authentication Method a connection negotiated cannot change afterwards: an
// AUTH that names another method is a Protocol Error (DISCONNECT 0x82), and the
// broker only ever sends the negotiated method in its own AUTH and CONNACK
// packets.
type AuthExchange interface {
	// Method is the MQTT 5 Authentication Method this exchange implements. It must
	// be the method Config.Authenticator was asked for and the one the client
	// named in its CONNECT: the broker writes it into every AUTH and CONNACK packet
	// of the connection. A factory that returns an exchange for another method is
	// a misconfiguration, and the broker treats the method as unsupported (CONNACK
	// 0x8C).
	Method() string
	// Begin starts the exchange. info carries the CONNECT's identity and its
	// authentication properties (AuthMethod, AuthData). On a re-authentication,
	// info is the connection's ConnectInfo with AuthMethod set to the negotiated
	// method and AuthData set to the data of the client's AUTH 0x19.
	Begin(ctx context.Context, info ConnectInfo) (AuthStep, error)
	// Continue advances the exchange with the Authentication Data of the client's
	// AUTH 0x18. data is nil when the packet carried no Authentication Data.
	Continue(ctx context.Context, data []byte) (AuthStep, error)
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
// as a denial), "auth_timeout" (an AuthManager.Authenticate or AuthExchange call
// exceeded it and counts as a failed authentication), "auth_method" (a CONNECT
// named an MQTT 5 Authentication Method this broker has no exchange for, so the
// connection was refused with 0x8C), "auth_steps" (an AuthExchange asked for more
// than MaxAuthSteps challenges and was cut off), "interceptor", "sink_rejected",
// "sink_retryable", "max_retain", "retained_expired" (a retained message was not
// replayed because its Message Expiry Interval elapsed, and it was deleted),
// "max_connections", "max_subscriptions", "connection_rate_limit",
// "takeover_rate_limit" (a CONNECT inside the Config.NotKickSeconds window would
// have displaced a live connection), "shared_subscription" (a malformed
// "$share/..." filter, or one that sets No Local, was refused), "shared_no_member"
// (a shared publication reached a node with no live member of the group, so it was
// dropped instead of being handed to another node's member),
// "subscription_identifier" (a SUBSCRIBE carried an out-of-range Subscription
// Identifier, which is a Protocol Error), "subscription_identifier_limit" (a
// delivery carried more Subscription Identifiers than the documented cap, or a
// subscription stored more), "cluster_namespace", "inflight_overflow" (a QoS 1/2 delivery found
// the connection's inflight window and the queue behind it both full, so it was
// dropped), "receive_maximum_full" (an inbound QoS 1/2 PUBLISH over the broker's own
// receive maximum: it was neither delivered nor acknowledged),
// "retry_exhausted" (an outbound exchange used up its retransmissions),
// "outbound_queue_full" (a QoS 0 PUBLISH was dropped because the connection's
// writer queue was full), "slow_consumer" (a packet that must not be dropped — a
// QoS 1/2 PUBLISH, an acknowledgement — hit a full writer queue, so the
// connection is disconnected), "offline_queue_full" (the per-session cap dropped
// the oldest message), "offline_queue_total" (the global cap rejected the newest
// one), "offline_queue_corrupt", "offline_session_missing", "session_expired",
// "session_store_corrupt" (a stored session in a SessionStore did not decode, so
// it was dropped), "session_store_error" (a SessionStore command failed, so the
// operation degraded to "no session"), "cluster_kick_stale" (a peer's kick is not
// newer than the connection it would close, so it was refused and the connection
// kept), "cluster_kick_rate_limit" (a peer's kick arrived inside the
// Config.NotKickSeconds window of the connection it would close, so it was refused
// and the connection kept), "cluster_kick_legacy" (a peer's kick carried no epoch
// because its sender predates kick epochs, so it was applied with the pre-epoch
// behavior; it is counted although it is not dropped, so an operator can see a
// mixed-version cluster)
// and "target_unknown" (a directed publish for a client id nobody holds: no
// cluster backend is configured, the Redis presence view has no record for it, or
// a TCP peer received the broadcast without holding the id — a directed publish is
// fire-and-forget, so this is counted instead of returned as an error).
// RecordQueueDepth("offline", n) reports the messages currently queued for
// offline sessions across the broker; RecordOverflow reports a limit that was hit
// without losing a message ("connections", "inflight" — a delivery was dropped
// as well, see "inflight_overflow" —, "receive_maximum", "outbound_queue",
// "will_delay" — a Will Delay Interval was clamped to Config.MaxWillDelayInterval
// —, "packet_ids" — a delivery degraded to QoS 0 because no packet id was free —
// and "byte_rate_read"/"byte_rate_write" — a byte-rate limit delayed a transfer;
// the throttled byte count itself is reported through the optional
// ByteRateMetrics interface).
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
	// subscriptionIDs are the Subscription Identifiers of the subscriptions the
	// target was reached through, deduplicated. SendLocal attaches them to the
	// PUBLISH it writes (or queues), which is what makes the identifier a property
	// of the subscription rather than of the publication.
	subscriptionIDs []uint32
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
