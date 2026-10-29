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

// TLSConfig holds TLS listener settings. TLS is not implemented yet (roadmap).
type TLSConfig struct {
	Enabled bool
	CA      string
	Cert    string
	Key     string
}

// ClusterConfig holds cluster settings. Clustering is not implemented yet
// (roadmap); the interface is provided so a backend can be plugged in without
// changing the broker API.
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
	SecurePort          int    // reserved (TLS, not implemented)
	WebsocketPort       int    // reserved (WS, not implemented)
	WebsocketSecurePort int    // reserved (WSS, not implemented)
	WebsocketPath       string // reserved, default /mqtt
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
	Auth         AuthManager
	ACL          AclManager
	Sink         EventSink
	Metrics      MetricsManager
	Interceptors []Interceptor
	Cluster      ClusterConfig
	Logger       *slog.Logger
}
