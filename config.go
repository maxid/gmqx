package gmqx

import (
	"context"
	"log/slog"
	"time"
)

// ConnectMode controls what happens when a client connects with an existing
// clientId. It controls session takeover behavior.
type ConnectMode string

const (
	// ConnectModeUnique rejects the new connection (default).
	ConnectModeUnique ConnectMode = "UNIQUE"
	// ConnectModeKick closes the old connection and accepts the new one.
	ConnectModeKick ConnectMode = "KICK"
)

// TLSConfig holds TLS listener settings.
type TLSConfig struct {
	Enabled           bool
	CA                string
	Cert              string
	Key               string
	RequireClientCert bool
}

// ClusterConfig holds cluster settings. ClusterBackend selects the optional
// cross-node implementation; when it is nil and Enabled is set, the built-in
// gossip backend is created from these fields (see New and NewGossipCluster).
// The fields that have no meaning for the gossip backend are not here: the
// backend is selected by Enabled/ClusterBackend, its listen address by Addr and
// its peer list by Seeds.
type ClusterConfig struct {
	// Enabled turns the cluster on. An explicit Config.ClusterBackend always wins
	// over it: Enabled is what selects the default (gossip) backend when no
	// backend object was supplied.
	Enabled bool
	// Addr is the gossip cluster listen address this node advertises, e.g.
	// "10.0.0.1:7946". It must be explicit (no ":0") and must differ from every
	// MQTT listener address, because peers dial it and compare it with their own
	// view of this node. It is required when Enabled is set and ClusterBackend is
	// nil. The empty string is an error at broker Start, exactly as for
	// NewTCPCluster.
	Addr string
	// Seeds are the cluster addresses of other nodes to contact. A node learns
	// the rest of the cluster from their gossip, so one reachable seed is enough
	// to join; a node with no seeds starts alone and still serves local traffic.
	Seeds []string
	// Node is the node id this broker announces in the cluster. It is used only
	// when Config.NodeID is empty; an explicit Config.NodeID wins.
	Node string
	// Namespace scopes the gossip traffic. It is used only when Config.Namespace
	// is empty; the broker's namespace is authoritative for a backend attached to
	// it. The empty string is the default namespace.
	Namespace string
	// PingInterval is how often a node probes one random member (default 1s).
	PingInterval time.Duration
	// PingTimeout is how long a direct probe may take before the member is probed
	// indirectly through the other members (default 500ms).
	PingTimeout time.Duration
	// SuspicionMultiplier is how many PingIntervals a suspected member may stay
	// silent before it is declared dead (default 3). A negative value is an
	// error.
	SuspicionMultiplier int
	// Fanout is how many peers one node writes one event to (default
	// min(len(members), 3), computed at send time). Receivers forward to Fanout
	// peers they did not receive the event from, so the whole cluster is reached
	// with O(N·Fanout) frames instead of N². A value at or above the member count
	// makes every event a full broadcast, which is what a 2- or 3-node cluster
	// wants. A negative value is an error.
	Fanout int
}

// Config is the broker configuration. Transport-specific tuning knobs are
// intentionally small and portable.
type Config struct {
	// Network
	Addr                string // TCP listen address, default :1883
	Port                int    // used when Addr is empty
	SecureAddr          string // TLS listen address (optional override)
	SecurePort          int    // TLS port when SecureAddr is empty
	WebsocketAddr       string // WebSocket listen address (optional override)
	WebsocketPort       int    // WebSocket port when WebsocketAddr is empty
	WebsocketSecureAddr string // WSS listen address (optional override)
	WebsocketSecurePort int    // WSS port when WebsocketSecureAddr is empty
	WebsocketPath       string // WebSocket path, default /mqtt
	TLS                 TLSConfig
	MaxPacketSize       int

	// Protocol semantics
	ConnectMode ConnectMode
	// NotKickSeconds is the anti-flap window of a client-id takeover, in
	// seconds: two ends of one client id cannot displace each other in a loop.
	// In ConnectModeKick a CONNECT that would displace a live connection is
	// refused when that connection began less than NotKickSeconds ago, exactly
	// as the reference implementation of this broker family does it; the refusal
	// is CONNACK 0x9F (Connection rate exceeded) on MQTT 5 and return code 3
	// (Server unavailable) on MQTT 3.1.1 — a rate limit on this client id, not an
	// administrative ban (0x8A) and not a whole-server overload (0x89). Zero
	// selects the default (30 s) and a negative value disables the window, which
	// takes the session over on every CONNECT — the behaviour before the window
	// existed and the one MQTT itself prescribes.
	//
	// The window protects a connection on both ends of a cluster. On the node that
	// receives the CONNECT it is enforced against that node's connections, as
	// above. On a node that receives a peer's connect-time kick it is enforced
	// against the connection the kick would close: a kick that arrives while the
	// incumbent is younger than the window is refused (counted with
	// Metrics.RecordDroppedMessage("cluster_kick_rate_limit")) instead of closing
	// a connection that cannot be the one the peer replaced. A kick that carries no
	// epoch, because its sender predates kick epochs, is applied without the
	// window: a mixed cluster keeps the pre-epoch behaviour instead of refusing
	// every kick.
	NotKickSeconds int
	// ConnectTimeout bounds the CONNECT phase of one connection: a read deadline
	// is armed before the first packet is read and cleared once the handshake is
	// through, when the read loop arms the deadline it owns (keep-alive, or none).
	// A client that opens the socket and sends nothing, or sends its CONNECT too
	// slowly, is therefore disconnected instead of holding a connection slot, a
	// goroutine and a session forever. The deadline also covers the authentication
	// call (see AuthTimeout, which bounds that call on its own) and the whole MQTT
	// 5 enhanced-authentication exchange, so a client that stalls on an AUTH is
	// disconnected at this deadline instead of being waited on. Default 10 s;
	// a non-positive value selects the default and there is no unlimited setting.
	ConnectTimeout time.Duration
	// AuthTimeout bounds one call into the authentication/authorization SPI:
	// AuthManager.Authenticate at CONNECT, each AuthExchange.Begin/Continue call of
	// an enhanced-authentication exchange, and AclManager.Check for every SUBSCRIBE
	// and PUBLISH. On a live connection it is also the read deadline of a whole
	// re-authentication exchange, because no CONNECT deadline exists any more. The
	// hook receives a context carrying the deadline and is abandoned when it
	// expires; an expired call is a refusal — a failed authentication or a denied
	// authorization — never a success. The built-in AllowAll (the default of both
	// SPIs) cannot block and always takes the synchronous path, so a broker that
	// keeps the defaults pays no goroutine per check. Default 5 s; a negative value
	// disables the bound and calls the hook synchronously, which then has to
	// respect its context itself.
	AuthTimeout time.Duration
	// MaxWillDelayInterval bounds the MQTT 5 Will Delay Interval this broker
	// honours. A will that asks for a longer delay is published after this bound
	// instead and the clamp is counted with Metrics.RecordOverflow("will_delay").
	// The delay is also cut short by the Session Expiry Interval of the session
	// that ended, because the will is published when the session ends at the
	// latest; and it does not apply at all to a session that ends with its
	// connection (Clean Start / Clean Session set, or a zero Session Expiry
	// Interval), whose will is published as soon as the connection is gone. The
	// delay is cancelled by a CONNECT that resumes the session (see the
	// pending-will scheduling in will.go). Default 5 min; a negative value removes
	// the bound, leaving the uint32 wire field (about 136 years) as the limit.
	MaxWillDelayInterval time.Duration
	// Namespace is the isolation scope of this broker: its session table,
	// subscription matching, retained store and advertised subscription summaries
	// only ever hold and match entries of this namespace. A publication produced
	// in another namespace is never delivered to a session here, locally or
	// through a cluster backend. The empty string is the default namespace and is
	// the zero value, so a deployment that does not set it behaves exactly as it
	// did before namespaces existed.
	Namespace string

	// Limits (0 = unlimited)
	MaxConnections        int
	MaxTopicSubscriptions int
	MaxRetainMessages     int64
	ConnectionRateLimit   int

	// Outbound delivery. MaxInflightQoS1 bounds the unconfirmed QoS 1 PUBLISHes of
	// one connection and MaxInflightQoS2 the QoS 2 exchanges of one connection, in
	// each direction. Zero selects the default (64); a negative value means
	// unlimited. A QoS 1/2 delivery that would exceed its window is queued for the
	// connection — it is not dropped and does not block the publishing goroutine —
	// and it is sent, in arrival order, as soon as a slot frees (see
	// MaxInflightQueueSize). An inbound QoS 2 PUBLISH over the window is answered
	// with PUBREC 0x97 (Quota exceeded) on MQTT 5.
	MaxInflightQoS1 int
	MaxInflightQoS2 int
	// MaxInflightQueueSize bounds the QoS 1/2 deliveries one connection may have
	// waiting for a free inflight slot (default 256). The queue preserves the
	// connection's delivery order and is drained by the acknowledgements that free
	// a slot; it is never unbounded, because a client that never confirms anything
	// would otherwise cost unbounded broker memory. Only a delivery that finds the
	// queue full is dropped, and that drop is counted twice
	// (Metrics.RecordDroppedMessage("inflight_overflow") and
	// Metrics.RecordOverflow("inflight")) and logged at Warn level with the client
	// id. A non-positive value selects the default; there is no unlimited setting.
	MaxInflightQueueSize int
	// MaxOutboundQueueSize bounds the writer queue of one connection (default 256
	// packets). Every packet of a live connection — PUBLISH, PUBACK, PUBREC,
	// PUBREL, PUBCOMP, SUBACK, PINGRESP — is appended to it and written by the
	// connection's single writer goroutine in that order, so no caller waits on a
	// socket. Only the CONNACK (written before the writer exists) and a server
	// DISCONNECT (the last packet, written before close) bypass it. The bound is
	// in packets, not bytes: the worst case is this many times MaxPacketSize per
	// connection. A non-positive value selects the default; there is no unlimited
	// setting.
	MaxOutboundQueueSize int
	// WriteTimeout is the write deadline of one packet (default 5 s). A peer that
	// stops reading therefore cannot hold a connection's writer goroutine, the
	// publisher that feeds it, the will publish of a closing connection or the
	// cluster receive goroutine: the write fails, the connection is closed and the
	// expiry is logged at Warn level. A non-positive value selects the default.
	WriteTimeout time.Duration
	// QoSRetryInterval is the base interval of the outbound retransmission timer
	// (default 5 s): the first retransmission of a QoS 1/2 exchange is sent after
	// one interval, and every further attempt doubles the wait. QoSMaxRetries caps
	// how many retransmissions one exchange gets (default 5); zero selects the
	// default and a negative value disables retransmission. Retransmitted
	// PUBLISHes carry the DUP flag; a PUBREL retransmission is a PUBREL again.
	QoSRetryInterval time.Duration
	QoSMaxRetries    int

	// Offline queue of persistent sessions. MaxOfflineQueueSize bounds the
	// messages one session keeps while it has no live connection (default 1000); a
	// full queue drops its oldest message. MaxTotalOfflineMessages bounds them
	// across all sessions of this broker (default 100000); at the global cap a new
	// message is rejected. Both caps drop/reject through
	// Metrics.RecordDroppedMessage and both are disabled by a negative value. Only
	// a persistent session (Clean Session / Clean Start cleared) queues; a clean
	// session never does.
	MaxOfflineQueueSize     int
	MaxTotalOfflineMessages int
	// SessionSweepInterval is how often the broker looks for sessions whose
	// Session Expiry Interval elapsed (default 30 s); each tick is bounded. A
	// negative value disables the sweeper, which only means an expired session is
	// then removed when it is next loaded — a CONNECT never resumes one either way.
	SessionSweepInterval time.Duration

	// SessionStore is where persistent-session state lives while no connection
	// owns it (subscriptions, queued messages, expiry deadline). Nil installs
	// MemorySessionStore, which is exactly the broker's behavior before the seam
	// existed and stays the default. A deployment can supply another
	// implementation; this package ships two, the node-local MemorySessionStore and
	// the shared RedisSessionStore (NewRedisSessionStore), the latter being what
	// lets a client that reconnects to another node of a cluster resume its
	// session.
	SessionStore SessionStore

	// Auth is the authentication hook of a CONNECT that carries no MQTT 5
	// Authentication Method. A connection that negotiates an enhanced
	// authentication method is authenticated by Config.Authenticator instead and
	// this hook is skipped for it; every other connection goes through it, so
	// AllowAll and DenyAll keep working for the non-AUTH path.
	Auth AuthManager
	// Authenticator builds the MQTT 5 enhanced-authentication exchange (AUTH) of
	// one connection, given the Authentication Method the client named in its
	// CONNECT. Returning nil means "this method is not supported" and the CONNECT
	// is refused with CONNACK 0x8C (Bad authentication method). A nil factory
	// advertises no method at all: the previous behavior, in which a connection
	// that somehow has no negotiated method and sends AUTH is a Protocol Error
	// (DISCONNECT 0x82). The returned exchange must report the method it was asked
	// for (AuthExchange.Method).
	//
	// The exchange replaces AuthManager for the connection it authenticates: on
	// success its ConnectInfo is the authoritative identity (ClientID/Username)
	// and AuthManager.Authenticate is not called. AclManager still authorizes every
	// publish and subscribe of the connection. See AuthExchange for the flow, the
	// deadline (Config.AuthTimeout per call) and the step limit (MaxAuthSteps).
	Authenticator func(method string) AuthExchange
	// AuthMethods lists the MQTT 5 Authentication Methods this broker advertises.
	// A CONNECT that names a method outside the list is refused with CONNACK 0x8C
	// even when Config.Authenticator would build an exchange for it; an empty list
	// means "whatever the factory supports".
	//
	// MQTT 5 has no field for a list of methods in CONNACK (§3.2.2.3.8 makes the
	// Authentication Method an echo of the client's, and including one the client
	// did not send is a Protocol Error), so the list is what the CONNACK echo can
	// name and what a CONNECT is checked against: a client that guesses a method
	// learns from 0x8C that it guessed wrong. When a method is negotiated, the
	// CONNACK Authentication Method is exactly the one the client used.
	AuthMethods []string
	ACL         AclManager
	Sink        EventSink
	// PublishCallbackTimeout bounds how long a publish waits for the sink. When
	// it expires the publish is Retryable and no success ACK is sent. Zero waits
	// without a bound (the sink must respect ctx).
	PublishCallbackTimeout time.Duration
	Metrics                MetricsManager
	Interceptors           []Interceptor
	// Dispatcher decides where an accepted publication goes. Nil selects
	// DefaultDispatcher, which preserves the broker's built-in behavior: deliver
	// to the local matching sessions and spread to the cluster backend.
	Dispatcher Dispatcher
	// ConnContext, when set, enriches the per-connection context at CONNECT. It is
	// called once for every CONNECT the broker parses, before authentication, with
	// the context that is cancelled when the connection ends plus the ConnectInfo
	// read from the packet (an empty ClientID means the server will assign one
	// afterwards). The returned context becomes the parent of every hook call for
	// that connection, so values and deadlines put here reach the auth, ACL, sink,
	// interceptor and dispatcher calls. Returning nil keeps the context passed in.
	ConnContext func(ctx context.Context, info ConnectInfo) context.Context
	Cluster     ClusterConfig
	// ClusterBackend enables cross-node routing (optional).
	ClusterBackend Cluster
	// NodeID identifies this broker in the cluster (default random).
	NodeID string
	Logger *slog.Logger

	// unit is the length of one MQTT protocol second for the intervals this
	// broker turns into timers or deadlines (the Session Expiry Interval, the
	// Will Delay Interval, the Message Expiry Interval of a retained message and
	// NotKickSeconds). Production always runs with time.Second; the package's own
	// tests shorten it so a case about a seconds-resolution interval completes in
	// milliseconds instead of sleeping. It is unexported because it is not part of
	// the API.
	unit time.Duration
}
