package conn

// The connection state machine, and the seam it runs against
//
// One Client is one physical connection: the MQTT handshake (CONNECT, optional
// AUTH), the read loop that dispatches the packets the client sends, the
// write-behind queue that owns the socket, the outbound QoS 1/2 window with its
// retransmission timer, and the teardown that hands the session back to the broker.
//
// The broker is not imported here. Everything a connection needs from it is the
// Host interface below: the identity and namespace, the resolved per-connection
// limits, the swappable extension snapshots (metrics, sink, logger), the
// authentication and ACL entry points, the routing/publish path, and the session
// handoff (register, unregister, will scheduling, offline queueing). A Broker
// implements it; a test can implement the few methods a case needs.
//
// The direction is what makes the split worth having: the broker owns identity,
// routing and persistence, the connection owns the protocol state machine, and the
// dependency arrow points one way (gmqx/internal/engine → gmqx/conn → gmqx/{hook,routing,session}).

import (
	"context"
	"log/slog"
	"time"

	"github.com/maxid/gnety/codec/mqtt"

	"github.com/maxid/gmqx/hook"
	"github.com/maxid/gmqx/routing"
	"github.com/maxid/gmqx/session"
)

// ConnectionID identifies one physical connection. It is the hook package's type,
// so the value a connection reports to its hooks and one a Target carries are the
// same.
type ConnectionID = hook.ConnectionID

// Message is one outgoing publication as a connection hands it to its host: what
// the offline queue needs, with the MQTT 5 property block in typed form.
type Message struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	Props   mqtt.Properties
}

// Origin is the identity a connection's publications are made under: the context
// they run with (the connection's, or the broker's for a will that outlived it),
// the ids the event carries and the namespace it belongs to.
type Origin struct {
	Context   context.Context
	ClientID  string
	ConnID    ConnectionID
	Username  string
	Namespace string
}

// Limits are the broker settings one connection resolves at construction, with the
// broker's defaults already applied. A zero value is the documented default of
// every limit.
type Limits struct {
	// MaxPacketSize is the largest packet the broker accepts from a client, 0 for
	// no limit.
	MaxPacketSize int
	// ReceiveMaximum is the inbound flow-control window: how many QoS 1/2
	// publications this broker accepts unacknowledged at once.
	ReceiveMaximum int
	// MaxInflightQoS1 and MaxInflightQoS2 bound the outbound exchanges of each QoS
	// that may be unacknowledged at once.
	MaxInflightQoS1 int
	MaxInflightQoS2 int
	// MaxInflightQueueSize bounds the deliveries waiting for a free slot in the
	// outbound window.
	MaxInflightQueueSize int
	// MaxOfflineQueueSize and MaxTotalOfflineMessages bound the queue of a
	// persistent session that has no connection.
	MaxOfflineQueueSize     int
	MaxTotalOfflineMessages int
	// MaxOutboundQueueSize bounds the connection's write-behind queue.
	MaxOutboundQueueSize int
	// WriteTimeout bounds one socket write.
	WriteTimeout time.Duration
	// ConnectTimeout bounds the CONNECT (and the AUTH exchange of a handshake).
	ConnectTimeout time.Duration
	// AuthTimeout bounds one authentication or ACL call.
	AuthTimeout time.Duration
	// QoSRetryInterval and QoSMaxRetries are the outbound retransmission policy.
	// A negative QoSMaxRetries disables retransmission.
	QoSRetryInterval time.Duration
	QoSMaxRetries    int
	// MaxWillDelayInterval caps a commanded Will Delay Interval. Negative disables
	// delayed wills, zero means the broker's default.
	MaxWillDelayInterval time.Duration
	// ConnContext, when set, derives the connection context from the CONNECT.
	ConnContext func(context.Context, hook.ConnectInfo) context.Context
}

// Host is everything a connection needs from the broker that owns it.
//
// Every method runs on the connection's goroutine or on a goroutine the
// connection started, and the connection never holds its own lock across a Host
// call unless the method's documentation says so (Register and Unregister are
// called without it; the session handoff callbacks run under it and must not call
// back into the connection).
type Host interface {
	// Namespace is the isolation scope the connection belongs to.
	Namespace() string
	// Second is the broker's protocol second: time.Second outside tests, where a
	// broker may shorten it to keep timing cases fast.
	Second() time.Duration
	// Limits are the resolved per-connection settings.
	Limits() Limits
	// Metrics is the current metrics snapshot (a plugin can swap it at run time).
	Metrics() hook.MetricsManager
	// Sink is the current event-sink snapshot, nil when none is configured.
	Sink() hook.EventSink
	// Logger is the broker's logger; it may be nil.
	Logger() *slog.Logger
	// LifecycleContext outlives every connection. It is what a will that outlived
	// its connection is published under.
	LifecycleContext() context.Context

	// AuthExchange returns the enhanced-authentication exchange a CONNECT asked
	// for, nil when the broker does not offer that method.
	AuthExchange(method string) hook.AuthExchange
	// Authenticate runs the plain authentication of a CONNECT.
	Authenticate(ctx context.Context, info hook.ConnectInfo) error

	// Register installs the connection's session (taking over a displaced one),
	// enforces the connect mode and the connection limits, and reports the CONNACK
	// decision.
	Register(c *Client) error
	// Unregister tears the session down: it stores a persistent session, drops a
	// clean one, and tells the cluster and the hooks.
	Unregister(c *Client)

	// Publish accepts one publication of this connection.
	Publish(from Origin, ev hook.PublishEvent, props mqtt.Properties) hook.PublishResult
	// Subscribe records one subscription and reports the SUBACK code and whether
	// the filter was new.
	Subscribe(c *Client, filter string, sub session.Subscription) (byte, bool)
	// Unsubscribe removes one subscription.
	Unsubscribe(c *Client, filter string)
	// SendRetained replays the retained publications of one filter to the
	// connection.
	SendRetained(c *Client, filter string, qos byte, retainAsPublished bool, ids []uint32)
	// ReceiveMaxFull reports that an inbound publication was refused because the
	// receive maximum was reached.
	ReceiveMaxFull(topic string)

	// EnqueueOffline queues one publication for the stored session of a target that
	// has no live connection.
	EnqueueOffline(target routing.Target, msg Message)
	// ScheduleWill hands a delayed will to the broker's pending-will scheduler.
	ScheduleWill(c *Client, delay time.Duration, ev hook.PublishEvent, props mqtt.Properties)
}

// Defaults for the limits a zero Limits leaves unset. They are bounded on purpose:
// an unbounded inflight window, outbound queue or retry loop all turn a slow or
// absent client into unbounded broker memory or an endless retransmission.
const (
	defaultMaxInflightQoS1      = 64
	defaultMaxInflightQoS2      = 64
	defaultMaxInflightQueueSize = 256
	defaultMaxOutboundQueueSize = 256
	defaultWriteTimeout         = 5 * time.Second
	defaultQoSRetryInterval     = 5 * time.Second
	defaultQoSMaxRetries        = 5
	defaultConnectTimeout       = 10 * time.Second
	defaultAuthTimeout          = 5 * time.Second
	defaultMaxWillDelayInterval = 5 * time.Minute
	// maxReceiveMaximum is the MQTT 5 limit of the Receive Maximum property.
	maxReceiveMaximum = 65535
)
