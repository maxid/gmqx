package conn

// Delayed wills, the connection's half
//
// A will is the last message a client asks the server to publish for it, and MQTT
// 5 lets the client ask for a delay so a device that reconnects after a network
// blip does not announce its own death. The connection decides the two things
// that depend on it — how long to wait (bounded by the session it belongs to) and
// which identity the will is published under once the connection's own context is
// cancelled — and hands the result to its host, which owns the timers and the
// map that outlives the connection (see gmqx/internal/engine's will.go).

import (
	"time"

	"github.com/maxid/gnety/codec/mqtt"

	"github.com/maxid/gmqx/hook"
	mqtt5 "github.com/maxid/gmqx/internal/mqtt5"
)

// origin returns the identity a publication of this connection is made under
// while the connection is still live.
func (c *Client) origin() Origin {
	return Origin{
		Context:   c.context(),
		ClientID:  c.id,
		ConnID:    c.connID,
		Username:  c.username,
		Namespace: c.namespace,
	}
}

// willOrigin returns the origin a will that outlives its connection is published
// under. close cancels the connection context, so the delayed publish runs under
// the broker's lifetime context with the connection's hook.ConnInfo attached: the
// hooks still see who published the will, and none of them is handed a context
// that expired before the will was even due.
// WillOrigin returns the identity a will that outlives its connection is
// published under: the broker's lifetime context with the connection's ConnInfo,
// because the connection context is cancelled by the time the will is due.
func (c *Client) WillOrigin() Origin {
	o := c.origin()
	o.Context = hook.ContextWithConn(c.host.LifecycleContext(), c.connInfo(c.info))
	return o
}

// publishWill publishes the will of this closed connection, or hands it to the
// broker's pending-will scheduler when the client asked for a delay.
func (c *Client) publishWill(will *mqtt.Will) {
	props := willPublishProperties(will.Properties)
	wireProps, err := mqtt5.EncodePropertyBlock(props)
	if err != nil {
		// A will whose properties do not belong on a PUBLISH is published
		// without them rather than not at all.
		props, wireProps = mqtt.Properties{}, nil
	}
	ev := hook.PublishEvent{
		Topic:      will.Topic,
		Payload:    will.Payload,
		QoS:        will.QoS,
		Retain:     will.Retain,
		Properties: wireProps,
	}
	if delay := c.willDelay(will); delay > 0 {
		c.host.ScheduleWill(c, delay, ev, props)
		return
	}
	_ = c.host.Publish(c.origin(), ev, props)
}

// willDelay is how long the broker waits before it publishes this connection's
// will: the requested Will Delay Interval, clamped to
// Config.MaxWillDelayInterval and cut short by what is left of the session.
//
// It returns 0 — publish now — for MQTT 3.1.1 (no will properties), for a will
// without an interval, and for a session that ends with this connection, because
// then the session ends first and the will goes with it.
func (c *Client) willDelay(will *mqtt.Will) time.Duration {
	if c.version != mqtt.Version5 {
		return 0
	}
	seconds := will.Properties.WillDelayInterval
	if seconds == 0 {
		return 0
	}
	if !c.persistentSession() {
		return 0
	}
	delay := time.Duration(seconds) * c.host.Second()
	if limit := c.host.Limits().MaxWillDelayInterval; limit > 0 && delay > limit {
		delay = limit
		c.host.Metrics().RecordOverflow("will_delay")
	}
	// The will is published when the session ends at the latest, so a session
	// that expires inside the delay publishes its will at the expiry.
	if deadline := c.SessionExpiryDeadline(); !deadline.IsZero() {
		if remaining := time.Until(deadline); remaining < delay {
			delay = remaining
		}
	}
	if delay <= 0 {
		return 0
	}
	return delay
}
