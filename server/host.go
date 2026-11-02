package server

// The host side of the connection seam
//
// gmqx/conn owns the protocol state machine; everything it cannot know — the
// namespace, the resolved per-connection limits, the swappable extension
// snapshots, the publish/subscribe/retained path, the session handoff and the
// delayed-will scheduler — is this file. Most methods are one-line adapters over
// the broker's own helpers, so the engine keeps a single implementation of each
// behavior and the connection only sees the interface.
//
// Naming: the interface method is the exported spelling (Metrics, Sink, Register),
// the broker's internal helper keeps its lower-case name (metrics, sink, register).
// A method pair that differs only in case is deliberate here: the exported one is
// the seam, the unexported one is what the rest of the engine calls.

import (
	"context"
	"log/slog"
	"time"

	"github.com/maxid/gnety/codec/mqtt"

	"github.com/maxid/gmqx/conn"
	"github.com/maxid/gmqx/hook"
	"github.com/maxid/gmqx/routing"
	"github.com/maxid/gmqx/session"
)

// MaxAuthSteps is the number of AUTH challenges one exchange may run; the limit is
// enforced by the connection's exchange loop, and the constant lives with it.
const MaxAuthSteps = conn.MaxAuthSteps

// Second implements conn.Host: the protocol second a connection measures
// keep-alive, session expiry and will delay in.
func (b *Broker) Second() time.Duration { return b.second() }

// Limits implements conn.Host: the per-connection settings, with the broker's
// defaults already applied by the helpers that own them.
func (b *Broker) Limits() conn.Limits {
	return conn.Limits{
		MaxPacketSize:           b.cfg.MaxPacketSize,
		ReceiveMaximum:          b.receiveMaximum(),
		MaxInflightQoS1:         b.cfg.MaxInflightQoS1,
		MaxInflightQoS2:         b.cfg.MaxInflightQoS2,
		MaxInflightQueueSize:    b.cfg.MaxInflightQueueSize,
		MaxOfflineQueueSize:     b.cfg.MaxOfflineQueueSize,
		MaxTotalOfflineMessages: b.cfg.MaxTotalOfflineMessages,
		MaxOutboundQueueSize:    b.cfg.MaxOutboundQueueSize,
		WriteTimeout:            b.cfg.WriteTimeout,
		// The two resolved settings: a negative will delay disables delayed wills
		// and a zero connect timeout means the documented default, so the broker
		// decides them once (see connectTimeout, maxWillDelayInterval).
		ConnectTimeout:       b.connectTimeout(),
		MaxWillDelayInterval: b.maxWillDelayInterval(),
		AuthTimeout:          b.cfg.AuthTimeout,
		QoSRetryInterval:     b.cfg.QoSRetryInterval,
		QoSMaxRetries:        b.cfg.QoSMaxRetries,
		ConnContext:          b.cfg.ConnContext,
	}
}

// Metrics implements conn.Host.
func (b *Broker) Metrics() hook.MetricsManager { return b.metrics() }

// Sink implements conn.Host.
func (b *Broker) Sink() hook.EventSink { return b.sink() }

// Logger implements conn.Host.
func (b *Broker) Logger() *slog.Logger { return b.log }

// LifecycleContext implements conn.Host.
func (b *Broker) LifecycleContext() context.Context { return b.lifecycleContext() }

// AuthExchange implements conn.Host.
func (b *Broker) AuthExchange(method string) hook.AuthExchange { return b.authExchange(method) }

// Authenticate implements conn.Host.
func (b *Broker) Authenticate(ctx context.Context, info hook.ConnectInfo) error {
	return b.authenticate(ctx, info)
}

// Register implements conn.Host.
func (b *Broker) Register(c *conn.Client) error { return b.register(c) }

// Unregister implements conn.Host.
func (b *Broker) Unregister(c *conn.Client) { b.unregister(c) }

// Publish implements conn.Host.
func (b *Broker) Publish(from conn.Origin, ev hook.PublishEvent, props mqtt.Properties) hook.PublishResult {
	return b.publish(from, ev, props)
}

// Subscribe implements conn.Host.
func (b *Broker) Subscribe(c *conn.Client, filter string, sub session.Subscription) (byte, bool) {
	return b.subscribe(c, filter, sub)
}

// Unsubscribe implements conn.Host.
func (b *Broker) Unsubscribe(c *conn.Client, filter string) { b.unsubscribe(c, filter) }

// SendRetained implements conn.Host.
func (b *Broker) SendRetained(c *conn.Client, filter string, qos byte, retainAsPublished bool, ids []uint32) {
	b.sendRetained(c, filter, qos, retainAsPublished, ids)
}

// ReceiveMaxFull implements conn.Host.
func (b *Broker) ReceiveMaxFull(topic string) { b.receiveMaxFull(topic) }

// EnqueueOffline implements conn.Host.
func (b *Broker) EnqueueOffline(target routing.Target, msg conn.Message) {
	b.enqueueOffline(target, Publication{
		Topic:    msg.Topic,
		Payload:  msg.Payload,
		QoS:      msg.QoS,
		Retain:   msg.Retain,
		props:    msg.Props,
		propsSet: true,
	}, msg.Props)
}

// ScheduleWill implements conn.Host.
func (b *Broker) ScheduleWill(c *conn.Client, delay time.Duration, ev hook.PublishEvent, props mqtt.Properties) {
	b.scheduleWill(c, namespaceKey(b.namespace, c.ID()), delay, ev, props)
}
