package gmqx

// The configuration lives with the engine in gmqx/server (docs/design/
// source-layout.md §4.1): Config, ClusterConfig and TLSConfig are one unit with the
// code that reads them. The root names are aliases, so a consumer keeps importing
// gmqx and holding server types (rule L3).

import "github.com/maxid/gmqx/server"

type (
	// Config is the broker configuration.
	Config = server.Config
	// ClusterConfig holds cluster settings.
	ClusterConfig = server.ClusterConfig
	// TLSConfig holds the listener TLS configuration.
	TLSConfig = server.TLSConfig
	// ConnectMode selects what a CONNECT with a client id in use does.
	ConnectMode = server.ConnectMode
)

const (
	ConnectModeUnique = server.ConnectModeUnique
	ConnectModeKick   = server.ConnectModeKick
)
