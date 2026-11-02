package server

// Persistent-session bookkeeping
//
// The state itself — the subscriptions to restore, the messages queued while the
// session was offline, the QoS 2 state of both directions and the expiry deadline —
// lives in gmqx/session (State, with Stored/StateFromStored as the projection to
// the SessionStore form). This file is the broker side of it: when the state is
// taken from a store, written back, swept, and how the offline queue and its caps
// follow it.
//
// The broker stores a session only when it has no live connection. While a
// connection owns the session its subscriptions live on the connection and the
// connection has taken the queued messages over (they drain right after the
// CONNACK), so the store and Broker.clients never hold the same key at the same
// time. That invariant is what makes a publication reach an offline session
// exactly once: LocalTargets reads the offline half, SendLocal re-checks it, and a
// session that reconnected in between receives the message on its live connection
// instead of a stale queue.
//
// The reference broker of this family also queues for clean sessions; gmqx does
// not, because a clean session is deleted with its connection and recreated empty
// by the next CONNECT — a queued message would be delivered to a session that
// declared it wanted none.

import (
	"time"

	"github.com/maxid/gnety/codec/mqtt"

	mqtt5 "github.com/maxid/gmqx/internal/mqtt5"
	sessionstate "github.com/maxid/gmqx/session"
)

// restoreQoS2State hands the QoS 2 state of a resumed session to the connection
// that takes it over: the inbound packet ids the client has not released and the
// outbound exchanges it has not confirmed. The outbound exchanges are scheduled
// for retransmission exactly like a freshly registered one, and resumeInflight
// re-sends them after the CONNACK.
func (c *client) restoreQoS2State(s *sessionstate.State) {
	for _, id := range s.InboundQoS2 {
		c.inboundQoS2[id] = struct{}{}
	}
	retry := c.broker.cfg.QoSRetryInterval
	if retry <= 0 {
		retry = defaultQoSRetryInterval
	}
	now := time.Now()
	for _, stored := range s.Inflight {
		m := &inflightMessage{
			packetID: stored.PacketID,
			topic:    stored.Topic,
			payload:  stored.Payload,
			qos:      stored.QoS,
			retain:   stored.Retain,
			props:    stored.Props,
			state:    liveInflightState(stored.Stage),
		}
		if c.broker.cfg.QoSMaxRetries >= 0 {
			m.due = now.Add(retry)
		}
		c.addInflightLocked(m.packetID, m)
		c.restoredInflight = append(c.restoredInflight, m.packetID)
	}
}

// snapshotQoS2 returns the connection's QoS 2 state for a store in the projection
// a session carries. The caller holds c.mu, which is what makes the snapshot
// consistent with the acknowledgements the connection is processing.
func (c *client) snapshotQoS2Locked() ([]uint16, []sessionstate.Inflight) {
	inbound := make([]uint16, 0, len(c.inboundQoS2))
	for id := range c.inboundQoS2 {
		inbound = append(inbound, id)
	}
	inflight := make([]sessionstate.Inflight, 0, len(c.inflight))
	for _, m := range c.inflight {
		inflight = append(inflight, sessionstate.Inflight{
			PacketID: m.packetID,
			Topic:    m.topic,
			Payload:  m.payload,
			QoS:      m.qos,
			Retain:   m.retain,
			Props:    m.props,
			Stage:    inflightStage(m.state),
		})
	}
	return inbound, inflight
}

// takeStoredSession removes the stored session of key and hands it to a
// connection. The caller holds b.mu. The global offline-queue counter follows the
// messages that leave the store, because those messages are now owned by the
// connection.
//
// An expired session is never handed over, even when the sweeper has not reached
// it yet: the Session Expiry Interval is a deadline, not a hint.
//
// The removal is atomic when the store provides Take (a shared store must: see
// sessionTaker), so two nodes that reconnect the same client id cannot both be
// handed the session. A node-local store is taken with Load followed by Delete,
// which its own broker lock already serialises.
func (b *Broker) takeStoredSession(key string) (*sessionstate.State, bool) {
	state, ok := b.takeSessionState(key)
	if !ok {
		return nil, false
	}
	b.offlineQueued = max(0, b.offlineQueued-len(state.Queue))
	if !state.ExpiresAt.IsZero() && !time.Now().Before(state.ExpiresAt) {
		b.metrics().RecordDroppedMessage("session_expired")
		return nil, false
	}
	s, dropped := sessionstate.StateFromStored(state)
	if dropped > 0 {
		b.log.Warn("gmqx dropped queued messages with an undecodable property block",
			"client", s.ClientID, "count", dropped)
		b.metrics().RecordDroppedMessage("offline_queue_corrupt")
	}
	return s, true
}

// takeSessionState removes the state stored under key and returns it: one atomic
// operation for a store that implements Take, Load and Delete otherwise.
func (b *Broker) takeSessionState(key string) (SessionState, bool) {
	if taker, ok := b.store.(sessionTaker); ok {
		return taker.Take(key)
	}
	state, ok := b.store.Load(key)
	if !ok {
		return SessionState{}, false
	}
	b.store.Delete(key)
	return state, true
}

// sharedStore reports whether Config.SessionStore is read and written by several
// broker nodes, which is what decides whether a connection displaced by a peer's
// kick may still write to it (see unregister).
func (b *Broker) sharedStore() bool {
	shared, ok := b.store.(SharedSessionStore)
	return ok && shared.SharedAcrossNodes()
}

// deleteStoredSession removes key and forgets its queued messages. The caller
// holds b.mu. The session leaves the routing index with it: an expired or
// discarded session must not be a target any more.
func (b *Broker) deleteStoredSession(key string) {
	if state, ok := b.store.Load(key); ok {
		b.offlineQueued = max(0, b.offlineQueued-len(state.Queue))
	}
	b.store.Delete(key)
	b.index.Remove(key)
}

// saveStoredSession writes the state the connection c leaves behind: its
// subscriptions, the messages of the queue it did not manage to drain, and its
// expiry deadline. The caller holds b.mu and c.mu, and c.queue is handed over to
// the store.
//
// The session's filters move from live to stored in the routing index here: from
// this point a matching publication is queued for it instead of written to a
// connection, and no store call is needed to find out that it is a target.
func (b *Broker) saveStoredSession(key string, c *client) {
	if stale, ok := b.store.Load(key); ok {
		// Defensive: a stored session of a live connection should not exist, but
		// the counter must follow what the store actually holds.
		b.offlineQueued = max(0, b.offlineQueued-len(stale.Queue))
	}
	// One deadline for both halves: the store the sweeper enforces and the index
	// that decides whether a publication still finds a target.
	expiresAt := c.sessionExpiryDeadline()
	inboundQoS2, inflight := c.snapshotQoS2Locked()
	state := (&sessionstate.State{
		ClientID:    c.id,
		Subs:        c.subs,
		Queue:       c.queue,
		ExpiresAt:   expiresAt,
		InboundQoS2: inboundQoS2,
		Inflight:    inflight,
	}).Stored()
	b.store.Save(key, state)
	b.offlineQueued += len(c.queue)
	c.queue = nil
	// The index is fed the same subscription set the store just got, so a publish
	// that matches the session is routed exactly like a restored one.
	b.index.SetStored(key, state.ClientID, state.Subscriptions, expiresAt)
}

// loadStoredSessions indexes the sessions a store already held when the broker
// was created: a deployment restores session state before New, and those sessions
// have to be local targets from the first publication on. A session whose deadline
// already passed is skipped — nothing is queued for it, and the sweeper deletes
// it. A store that is written behind the broker's back while it runs is not
// re-read: the broker indexes what it saves, hands over and expires.
func (b *Broker) loadStoredSessions() {
	now := time.Now()
	for _, key := range b.store.Keys() {
		if !keyInNamespace(b.namespace, key) {
			// A store may be shared with a broker of another namespace.
			continue
		}
		state, ok := b.store.Load(key)
		if !ok {
			continue
		}
		if !state.ExpiresAt.IsZero() && !now.Before(state.ExpiresAt) {
			continue
		}
		b.index.SetStored(key, state.ClientID, state.Subscriptions, state.ExpiresAt)
	}
}

// sessionExpiryDeadline is the deadline the sweeper enforces for the session a
// closing connection leaves behind.
//
// MQTT 3.1.1 has no Session Expiry Interval, so its persistent sessions keep the
// previous behavior and never expire (the zero time). MQTT 5 stamps
// now + Session Expiry Interval; a zero interval does not reach this point
// because such a session is not persistent at all — the session ends with the
// connection, which is also why a CONNECT with a zero interval can resume a
// stored session but never creates one. The interval is counted in the broker's
// protocol second (Config.unit), which is time.Second outside the tests.
func (c *client) sessionExpiryDeadline() time.Time {
	if c.version != mqtt.Version5 || c.info.SessionExpiry == 0 {
		return time.Time{}
	}
	return time.Now().Add(time.Duration(c.info.SessionExpiry) * c.broker.second())
}

// maxSessionSweepPerTick bounds how many stored sessions one sweep tick examines,
// so the sweeper's cost per tick stays bounded no matter how many sessions a
// broker holds. Map iteration order is randomized, so a session that is not
// reached in one tick is reached by a later one.
const maxSessionSweepPerTick = 512

// sessionSweeper deletes the sessions whose Session Expiry Interval elapsed. One
// goroutine per broker, started by Start and ended by Stop; no timer per session.
func (b *Broker) sessionSweeper() {
	interval := b.cfg.SessionSweepInterval
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.done:
			return
		case <-ticker.C:
			b.sweepExpiredSessions()
		}
	}
}

// sweepExpiredSessions removes the stored sessions whose deadline has passed.
// Subscriptions and queued messages go with them: the session is gone.
func (b *Broker) sweepExpiredSessions() {
	now := time.Now()
	checked := 0
	for _, key := range b.store.Keys() {
		if checked >= maxSessionSweepPerTick {
			return
		}
		if !keyInNamespace(b.namespace, key) {
			// A store may be shared with a broker of another namespace; its
			// sessions are that broker's to expire.
			continue
		}
		checked++
		stored, ok := b.store.Load(key)
		if !ok || stored.ExpiresAt.IsZero() || now.Before(stored.ExpiresAt) {
			continue
		}
		b.mu.Lock()
		if _, connected := b.clients[key]; connected {
			// The session has an owner; its next disconnect stamps a new deadline.
			b.mu.Unlock()
			continue
		}
		// Re-read under the lock: a reconnect or a new publish may have changed
		// the entry since the check above.
		current, ok := b.store.Load(key)
		if ok && !current.ExpiresAt.IsZero() && !now.Before(current.ExpiresAt) {
			b.deleteStoredSession(key)
			// The session is gone, so no directed publish can be queued for it
			// here any more: a presence-tracking cluster backend stops routing it to
			// this node.
			b.forgetClientPresence(current.ClientID)
			b.metrics().RecordDroppedMessage("session_expired")
			b.metrics().RecordQueueDepth("offline", b.offlineQueued)
		}
		b.mu.Unlock()
	}
}

// absorbPendingLocked moves the deliveries a connection still owed its inflight
// window into the session queue it leaves behind, in the order they were queued.
// They were acknowledged to their publisher and never written, so a persistent
// session must not lose them with the connection: they take their place behind
// whatever the offline queue still holds, which is where the next CONNECT drains
// them from.
//
// This is the queue a persistent session gets instead of a drop when its inflight
// queue overflows (see client.sendPublish), and it is also what a connection that
// simply went away leaves behind. The queue's own bounds apply exactly as they do
// to a message published while the session is offline — Config.MaxOfflineQueueSize
// drops the oldest messages of the combined queue, Config.MaxTotalOfflineMessages
// rejects the newest — and they are counted with the same reasons
// ("offline_queue_full", "offline_queue_total"), so the offline queue's capacity
// stays the single, documented loss point of a persistent session.
//
// The caller holds b.mu and c.mu, and has not saved the session yet: the counter
// of queued messages follows len(c.queue) when saveStoredSession stores it.
func (b *Broker) absorbPendingLocked(c *client) {
	if len(c.pending) == 0 {
		return
	}
	pending := c.pending
	c.pending = nil
	rejected := 0
	for _, m := range pending {
		if limit := b.cfg.MaxTotalOfflineMessages; limit > 0 && b.offlineQueued+len(c.queue) >= limit {
			rejected++
			continue
		}
		c.queue = append(c.queue, sessionstate.Pending{
			Topic:   m.topic,
			Payload: m.payload,
			QoS:     m.qos,
			Retain:  m.retain,
			Props:   m.props,
		})
	}
	if maxQueue := b.cfg.MaxOfflineQueueSize; maxQueue > 0 && len(c.queue) > maxQueue {
		c.queue = c.queue[len(c.queue)-maxQueue:]
		b.metrics().RecordDroppedMessage("offline_queue_full")
	}
	for i := 0; i < rejected; i++ {
		b.metrics().RecordDroppedMessage("offline_queue_total")
	}
}

// dropPendingLocked discards the deliveries a connection still owed, counting each
// one with reason. It is the last resort: a session that another node owns (or that
// was taken over here) has no queue on this node to keep them in. The caller holds
// b.mu and c.mu.
func (b *Broker) dropPendingLocked(c *client, reason string) {
	for range c.pending {
		b.metrics().RecordDroppedMessage(reason)
	}
	c.pending = nil
}

// enqueueOffline appends one publication to the queue of the persistent session
// target names. It is the write half of an offline LocalTarget.
//
// Routing is a snapshot, so the session may have reconnected since LocalTargets
// ran: then the message goes to the live connection instead of a queue nobody
// would drain. A session that is gone (cleaned or expired) has nothing to queue
// for. The global cap rejects the newest message, the per-session cap drops the
// oldest one, and both are counted through the metrics drop hook.
func (b *Broker) enqueueOffline(target LocalTarget, pub Publication, props mqtt.Properties) {
	key := target.SessionKey()
	b.mu.Lock()
	if live, ok := b.clients[key]; ok {
		b.mu.Unlock()
		// A live connection owns the session now, so delivery is live again.
		_ = live.sendPublish(pub.Topic, pub.Payload, target.QoS, target.Retain, props)
		return
	}
	state, ok := b.store.Load(key)
	if !ok {
		b.mu.Unlock()
		b.metrics().RecordDroppedMessage("offline_session_missing")
		return
	}
	if !state.ExpiresAt.IsZero() && !time.Now().Before(state.ExpiresAt) {
		// The session is past its deadline and only waits for the sweeper.
		b.mu.Unlock()
		b.metrics().RecordDroppedMessage("offline_session_missing")
		return
	}
	if limit := b.cfg.MaxTotalOfflineMessages; limit > 0 && b.offlineQueued >= limit {
		b.mu.Unlock()
		b.metrics().RecordDroppedMessage("offline_queue_total")
		return
	}
	queue := make([]QueuedMessage, 0, len(state.Queue)+1)
	queue = append(queue, state.Queue...)
	// The queued message keeps the MQTT 5 property block of the delivery, which
	// carries the target's subscription identifiers when it has any: they are part
	// of the subscription state and have to be there when the session drains its
	// queue after the next CONNACK. Without them the block the publisher sent is
	// stored unchanged, so an ordinary message is not re-encoded.
	wireProps := pub.Props
	if len(props.SubscriptionIdentifiers) > 0 {
		if encoded, err := mqtt5.EncodePropertyBlock(props); err == nil {
			wireProps = encoded
		}
	}
	queue = append(queue, QueuedMessage{
		Topic:   pub.Topic,
		Payload: append([]byte(nil), pub.Payload...),
		QoS:     target.QoS,
		Retain:  target.Retain,
		Props:   append([]byte(nil), wireProps...),
	})
	queued := 1
	evicted := false
	if maxQueue := b.cfg.MaxOfflineQueueSize; maxQueue > 0 && len(queue) > maxQueue {
		dropped := len(queue) - maxQueue
		queue = queue[dropped:]
		queued -= dropped
		evicted = true
	}
	b.offlineQueued += queued
	state.Queue = queue
	b.store.Save(key, state)
	depth := b.offlineQueued
	b.mu.Unlock()

	if evicted {
		b.metrics().RecordDroppedMessage("offline_queue_full")
	}
	b.metrics().RecordQueueDepth("offline", depth)
}
