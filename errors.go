package gmqx

import "errors"

var (
	// ErrUnauthorized is returned when authentication fails.
	ErrUnauthorized = errors.New("gmqx: unauthorized")
	// ErrMalformedPacket indicates an invalid MQTT packet.
	ErrMalformedPacket = errors.New("gmqx: malformed packet")
	// ErrUnsupportedProtocol indicates a non-MQTT-3.1.1 CONNECT.
	ErrUnsupportedProtocol = errors.New("gmqx: unsupported protocol level")
)
