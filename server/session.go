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

	"github.com/maxid/gmqx/conn"
	mqtt5 "github.com/maxid/gmqx/internal/mqtt5"
	sessionstate "github.com/maxid/gmqx/session"
)

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

// saveStoredSession writes the state the connection c handed over: its
// subscriptions, the queue it did not manage to drain plus the deliveries it
// still owed, and its expiry deadline. The caller holds b.mu, and the connection
// runs it from inside conn.Client.HandOff, so no delivery can observe the handoff
// before the state is stored.
//
// The session's filters move from live to stored in the routing index here: from
// this point a matching publication is queued for it instead of written to a
// connection, and no store call is needed to find out that it is a target.
func (b *Broker) saveStoredSession(key string, h *conn.Handoff) {
	if stale, ok := b.store.Load(key); ok {
		// Defensive: a stored session of a live connection should not exist, but
		// the counter must follow what the store actually holds.
		b.offlineQueued = max(0, b.offlineQueued-len(stale.Queue))
	}
	// One deadline for both halves: the store the sweeper enforces and the index
	// that decides whether a publication still finds a target.
	queue := b.adoptQueue(h)
	state := (&sessionstate.State{
		ClientID:    h.ClientID,
		Subs:        h.Subs,
		Queue:       queue,
		ExpiresAt:   h.ExpiresAt,
		InboundQoS2: h.InboundQoS2,
		Inflight:    h.Inflight,
	}).Stored()
	b.store.Save(key, state)
	b.offlineQueued += len(queue)
	// The index is fed the same subscription set the store just got, so a publish
	// that matches the session is routed exactly like a restored one.
	b.index.SetStored(key, state.ClientID, state.Subscriptions, h.ExpiresAt)
}

// adoptQueue merges the deliveries a connection still owed its inflight window
// into the queue it hands over, in the order they were queued. They were
// acknowledged to their publisher and never written, so a persistent session must
// not lose them with the connection: they take their place behind whatever the
// offline queue still holds, which is where the next CONNECT drains them from.
//
// This queue is what a persistent session gets instead of a drop when its inflight
// queue overflows (see conn's sendPublish), and it is also what a connection that
// simply went away leaves behind. The queue's own bounds apply exactly as they do
// to a message published while the session is offline — Config.MaxOfflineQueueSize
// drops the oldest messages of the combined queue, Config.MaxTotalOfflineMessages
// rejects the newest — and they are counted with the same reasons
// ("offline_queue_full", "offline_queue_total"), so the offline queue's capacity
// stays the single, documented loss point of a persistent session.
//
// The caller holds b.mu.
func (b *Broker) adoptQueue(h *conn.Handoff) []sessionstate.Pending {
	queue := h.Queue
	rejected := 0
	for _, m := range h.Pending {
		if limit := b.cfg.MaxTotalOfflineMessages; limit > 0 && b.offlineQueued+len(queue) >= limit {
			rejected++
			continue
		}
		queue = append(queue, m)
	}
	if maxQueue := b.cfg.MaxOfflineQueueSize; maxQueue > 0 && len(queue) > maxQueue {
		queue = queue[len(queue)-maxQueue:]
		b.metrics().RecordDroppedMessage("offline_queue_full")
	}
	for i := 0; i < rejected; i++ {
		b.metrics().RecordDroppedMessage("offline_queue_total")
	}
	return queue
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
		_ = live.DeliverPublish(pub.Topic, pub.Payload, target.QoS, target.Retain, props)
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
