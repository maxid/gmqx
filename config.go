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
// cross-node implementation; the built-in TCP backend uses these settings where applicable.
type ClusterConfig struct {
	Enabled                  bool
	URL                      string
	Port                     int
	Node                     string
	Namespace                string
	PingTimeout              time.Duration
	SuspicionMultiplier      int
	ClusterMessageBufferSize int
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
	ConnectMode    ConnectMode
	NotKickSeconds int
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
	// unlimited. A QoS 1/2 delivery that would exceed its window is skipped and
	// counted with Metrics.RecordDroppedMessage("inflight_full") instead of
	// blocking the publishing goroutine, and an inbound QoS 2 PUBLISH over the
	// window is answered with PUBREC 0x97 (Quota exceeded) on MQTT 5.
	MaxInflightQoS1 int
	MaxInflightQoS2 int
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
	// existed. A deployment can supply another implementation; this package ships
	// only the in-memory one.
	SessionStore SessionStore

	// SPI
	Auth AuthManager
	ACL  AclManager
	Sink EventSink
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
}
