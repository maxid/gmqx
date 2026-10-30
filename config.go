package gmqx

import (
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
	Namespace      string

	// Limits (0 = unlimited)
	MaxConnections        int
	MaxTopicSubscriptions int
	MaxRetainMessages     int64
	MaxInflightQoS2       int
	ConnectionRateLimit   int

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
	Cluster                ClusterConfig
	// ClusterBackend enables cross-node routing (optional).
	ClusterBackend Cluster
	// NodeID identifies this broker in the cluster (default random).
	NodeID string
	Logger *slog.Logger
}
