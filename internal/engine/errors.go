package engine

import (
	"errors"

	"github.com/maxid/gmqx/cluster"
	"github.com/maxid/gmqx/conn"
)

var (
	// The connect-refusal errors are gmqx/conn's: the connection maps them onto
	// MQTT reason codes, the broker returns them, and re-exporting them here keeps
	// one identity under the gmqx name (errors.Is works across both spellings).
	ErrUnauthorized = conn.ErrUnauthorized
	// ErrBadAuthMethod is returned when a CONNECT names an MQTT 5 Authentication
	// Method the broker has no exchange for. The CONNACK that accompanies it is
	// 0x8C (Bad authentication method) on MQTT 5.
	ErrBadAuthMethod = conn.ErrBadAuthMethod
	// ErrMalformedPacket indicates an invalid MQTT packet.
	ErrMalformedPacket = conn.ErrMalformedPacket
	// ErrUnsupportedProtocol indicates a CONNECT whose protocol name or level this
	// broker does not speak. MQTT 3.1, 3.1.1 and 5 are supported.
	ErrUnsupportedProtocol = conn.ErrUnsupportedProtocol
	// ErrClientIDInUse is returned in ConnectModeUnique when the clientId exists.
	ErrClientIDInUse = conn.ErrClientIDInUse
	// ErrMaxConnections is returned when the configured connection limit is reached.
	ErrMaxConnections = conn.ErrMaxConnections
	// ErrTakeoverRateExceeded is returned in ConnectModeKick when a CONNECT would
	// displace a live connection that began less than Config.NotKickSeconds ago:
	// the anti-flap window refuses the take-over instead of letting two ends of one
	// client id displace each other in a loop. The CONNACK that accompanies it is
	// 0x9F (Connection rate exceeded) on MQTT 5 and return code 3 (Server
	// unavailable) on MQTT 3.1.1.
	ErrTakeoverRateExceeded = conn.ErrTakeoverRateExceeded
	// ErrClientIDRequired reports that Broker.PublishToClient was called without a
	// client id: a directed publish has no other way to name its target.
	ErrClientIDRequired = errors.New("gmqx: client id is required")
	// ErrBadTopic reports a topic that is empty or contains a wildcard, which is
	// not a topic name a publication can carry.
	ErrBadTopic = errors.New("gmqx: invalid topic name")
	// ErrNamespaceMismatch reports that Broker.PublishToClient was handed a
	// publication stamped with a namespace other than the broker's. A client id is
	// only unique inside its namespace, so such a publication is refused instead of
	// being delivered to a session of a different scope or silently dropped. An
	// empty Publication.Namespace means the broker's own namespace.
	ErrNamespaceMismatch = errors.New("gmqx: publication namespace does not match the broker's")

	// ErrClusterUnavailable reports that a cluster backend cannot reach the
	// transport it fans out on (for example Redis). The cluster backends live in
	// gmqx/cluster, so the sentinel is defined there and aliased here: both import
	// paths name the same error and errors.Is matches either.
	ErrClusterUnavailable = cluster.ErrClusterUnavailable
	// ErrNodeIDInUse reports that another live process owns this cluster node id.
	// It is defined in gmqx/cluster with the Redis backend that enforces it (see
	// the doc comment there).
	ErrNodeIDInUse = cluster.ErrNodeIDInUse
)
