package gmqx

import "errors"

var (
	// ErrUnauthorized is returned when authentication fails.
	ErrUnauthorized = errors.New("gmqx: unauthorized")
	// ErrMalformedPacket indicates an invalid MQTT packet.
	ErrMalformedPacket = errors.New("gmqx: malformed packet")
	// ErrUnsupportedProtocol indicates a non-MQTT-3.1.1 CONNECT.
	ErrUnsupportedProtocol = errors.New("gmqx: unsupported protocol level")
	// ErrClientIDInUse is returned in ConnectModeUnique when the clientId exists.
	ErrClientIDInUse = errors.New("gmqx: client id already in use")
	// ErrMaxConnections is returned when the configured connection limit is reached.
	ErrMaxConnections = errors.New("gmqx: max connections reached")
)
