package cluster

import "errors"

var (
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
)
