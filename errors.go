package gmqx

// The sentinels are defined with the engine in gmqx/server (docs/design/
// source-layout.md §4.1); the root names are aliases, so errors.Is matches either
// import path.

import "github.com/maxid/gmqx/server"

var (
	ErrUnauthorized         = server.ErrUnauthorized
	ErrBadAuthMethod        = server.ErrBadAuthMethod
	ErrMalformedPacket      = server.ErrMalformedPacket
	ErrUnsupportedProtocol  = server.ErrUnsupportedProtocol
	ErrClientIDInUse        = server.ErrClientIDInUse
	ErrMaxConnections       = server.ErrMaxConnections
	ErrTakeoverRateExceeded = server.ErrTakeoverRateExceeded
	ErrClientIDRequired     = server.ErrClientIDRequired
	ErrNamespaceMismatch    = server.ErrNamespaceMismatch
	ErrClusterUnavailable   = server.ErrClusterUnavailable
	ErrNodeIDInUse          = server.ErrNodeIDInUse
)
