package gmqx

import (
	"errors"

	"github.com/maxid/gnety/codec/mqtt"
)

var (
	// ErrUnauthorized is returned when authentication fails.
	ErrUnauthorized = errors.New("gmqx: unauthorized")
	// ErrMalformedPacket indicates an invalid MQTT packet.
	// It aliases the wire codec error so callers can match either sentinel.
	ErrMalformedPacket = mqtt.ErrMalformedPacket
	// ErrUnsupportedProtocol indicates a CONNECT whose protocol name or level
	// this broker does not speak. MQTT 3.1, 3.1.1 and 5 are supported; the codec
	// reports any other version and gmqx keeps the sentinel on top.
	ErrUnsupportedProtocol = errors.New("gmqx: unsupported protocol level")
	// ErrClientIDInUse is returned in ConnectModeUnique when the clientId exists.
	ErrClientIDInUse = errors.New("gmqx: client id already in use")
	// ErrMaxConnections is returned when the configured connection limit is reached.
	ErrMaxConnections = errors.New("gmqx: max connections reached")
)
