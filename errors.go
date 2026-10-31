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
	// ErrTakeoverRateExceeded is returned in ConnectModeKick when a CONNECT would
	// displace a live connection that began less than Config.NotKickSeconds ago:
	// the anti-flap window refuses the take-over instead of letting two ends of one
	// client id displace each other in a loop. The CONNACK that accompanies it is
	// 0x9F (Connection rate exceeded) on MQTT 5 and return code 3 (Server
	// unavailable) on MQTT 3.1.1.
	ErrTakeoverRateExceeded = errors.New("gmqx: takeover inside the NotKickSeconds window")
	// ErrClusterUnavailable reports that a cluster backend cannot reach the
	// transport it fans out on (for example Redis). It is a retryable condition:
	// the backend keeps its own loops running and recovers by itself, so a caller
	// may treat the event as undelivered and move on instead of retrying in a
	// loop. Spread wraps it with the underlying cause, so both stay matchable
	// with errors.Is.
	ErrClusterUnavailable = errors.New("gmqx: cluster backend unavailable")
	// ErrNodeIDInUse reports that another live process owns this cluster node id.
	// The Redis backend enforces one owner per node id: Start takes the node's
	// ownership key with SET NX and fails with this error when a live process
	// still holds it, because two processes sharing an id would share one channel,
	// apply every event twice and collapse into a single member. The error is not
	// retryable while that process lives; a process that died without releasing
	// the key blocks the id only until the key's TTL expires.
	ErrNodeIDInUse = errors.New("gmqx: cluster node id already in use")
	// ErrClientIDRequired reports that Broker.PublishToClient was called without a
	// client id: a directed publish has no other way to name its target.
	ErrClientIDRequired = errors.New("gmqx: client id is required")
	// ErrNamespaceMismatch reports that Broker.PublishToClient was handed a
	// publication stamped with a namespace other than the broker's. A client id is
	// only unique inside its namespace, so such a publication is refused instead of
	// being delivered to a session of a different scope or silently dropped. An
	// empty Publication.Namespace means the broker's own namespace.
	ErrNamespaceMismatch = errors.New("gmqx: publication namespace does not match the broker's")
)
