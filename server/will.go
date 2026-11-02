package server

// Delayed wills (MQTT 5 Will Delay Interval)
//
// A will is the last message a client asks the server to publish for it. MQTT
// 3.1.1 publishes it as soon as the connection is gone; MQTT 5 lets the client
// ask for a delay, so a device that reconnects after a network blip does not
// announce its own death to the world.
//
// The rule is the one MQTT 5 states in §3.1.3.2.2: the will is published when the
// earlier of two things happens — the Will Delay Interval elapses, or the session
// ends. Everything else follows from that:
//
//   - A session that ends with its connection (Clean Start set, or a zero Session
//     Expiry Interval) publishes its will immediately, because the delay cannot
//     outlive the session it belongs to.
//   - A session with a Session Expiry Interval publishes its will at the expiry
//     when that comes first, so a delayed will is never left waiting for a session
//     that no longer exists.
//   - A CONNECT that resumes the session (same client id) cancels the pending
//     will [MQTT-3.1.2-28]: a session that comes back must not announce that it
//     died. That CONNECT is either local (register cancels directly) or on another
//     node, where the cluster kick it spreads cancels it here (OnRemoteKick).
//   - A CONNECT with Clean Start set discards the session instead of resuming it,
//     so the session ends there and the will is published at that moment.
//   - A broker that stops drops what is pending: it is shutting down, so there is
//     nowhere to publish the will and no timer may outlive Stop.
//
// Scheduling is one timer per pending will, not a broker-wide ticker: a delayed
// will is rare, and a timer that exists only while the will waits costs nothing
// when nobody asks for one. The timer fires on its own goroutine, publishes under
// the broker's lifetime context (the connection's own context is cancelled by
// then) and removes itself from the map before publishing, so a will is published
// at most once even when a cancellation and the timer race.
//
// What the connection decides (the delay, the identity the will is published
// under) lives in gmqx/conn; what outlives the connection — the map and its
// timers — stays here.

import (
	"time"

	"github.com/maxid/gnety/codec/mqtt"

	"github.com/maxid/gmqx/conn"
	"github.com/maxid/gmqx/hook"
)

// pendingWill is a will waiting for its Will Delay Interval to elapse. It is
// stored under the namespace-scoped client id it belongs to, which is what a
// CONNECT of the same session cancels.
type pendingWill struct {
	// origin is the identity the will is published under once it fires: the
	// connection is closed by then, so it carries the broker's lifetime context
	// with the connection's hook.ConnInfo instead of the cancelled connection context.
	origin conn.Origin
	ev     hook.PublishEvent
	props  mqtt.Properties
	timer  *time.Timer
	// delay is the wait that was scheduled, for the log line.
	delay time.Duration
}

// scheduleWill records a delayed will and arms its timer. A will already pending
// for the same client id is replaced: a session has one will, and the newest
// connection ended with it.
func (b *Broker) scheduleWill(c *conn.Client, key string, delay time.Duration, ev hook.PublishEvent, props mqtt.Properties) {
	pw := &pendingWill{origin: c.WillOrigin(), ev: ev, props: props, delay: delay}
	// A broker that is stopping has nowhere left to publish the will and must not
	// arm a timer behind Stop's back; the wills Stop was already holding are
	// dropped the same way.
	b.mu.RLock()
	stopped := b.closed
	b.mu.RUnlock()
	if stopped {
		if b.log != nil {
			b.log.Debug("gmqx delayed will dropped: broker is stopping", "client", pw.origin.ClientID,
				"topic", ev.Topic)
		}
		return
	}
	b.willMu.Lock()
	if old, ok := b.wills[key]; ok {
		old.timer.Stop()
	}
	// The timer is created under the lock and its callback takes the same lock
	// first, so the callback cannot observe the map before pw is in it.
	pw.timer = time.AfterFunc(delay, func() { b.firePendingWill(key, pw) })
	b.wills[key] = pw
	b.willMu.Unlock()
	if b.log != nil {
		b.log.Debug("gmqx will delayed", "client", pw.origin.ClientID,
			"connection", string(pw.origin.ConnID), "topic", ev.Topic, "delay", delay)
	}
}

// firePendingWill publishes a delayed will whose interval elapsed. It removes the
// entry first, so a will is published at most once even when a cancellation and
// the timer race.
func (b *Broker) firePendingWill(key string, want *pendingWill) {
	b.willMu.Lock()
	pw, ok := b.wills[key]
	if !ok || pw != want {
		b.willMu.Unlock()
		return
	}
	delete(b.wills, key)
	b.willMu.Unlock()
	_ = b.publish(pw.origin, pw.ev, pw.props)
}

// flushPendingWill publishes a delayed will right away, because the session it
// belongs to is ending before the delay elapsed: a CONNECT with Clean Start set
// discards the session [MQTT-3.1.2-29], and the will is published when the
// session ends.
func (b *Broker) flushPendingWill(key string) {
	b.willMu.Lock()
	pw, ok := b.wills[key]
	if ok {
		delete(b.wills, key)
	}
	b.willMu.Unlock()
	if !ok {
		return
	}
	pw.timer.Stop()
	_ = b.publish(pw.origin, pw.ev, pw.props)
}

// cancelPendingWill drops the will a closed connection left waiting for its
// delay. A CONNECT that resumes the session calls it, because a session that
// comes back must not announce that it died.
func (b *Broker) cancelPendingWill(key string) {
	b.willMu.Lock()
	pw, ok := b.wills[key]
	if ok {
		delete(b.wills, key)
	}
	b.willMu.Unlock()
	if ok {
		pw.timer.Stop()
		if b.log != nil {
			b.log.Debug("gmqx delayed will cancelled by a reconnect", "client", pw.origin.ClientID,
				"connection", string(pw.origin.ConnID), "delay", pw.delay)
		}
	}
}

// dropPendingWills forgets every delayed will. Stop calls it: the broker is
// shutting down, so there is no subscriber left to publish to, and no timer may
// outlive the broker.
func (b *Broker) dropPendingWills() {
	b.willMu.Lock()
	pending := b.wills
	b.wills = make(map[string]*pendingWill)
	b.willMu.Unlock()
	for _, pw := range pending {
		pw.timer.Stop()
	}
}
