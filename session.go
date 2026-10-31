package gmqx

// Persistent-session state
//
// A session is the state a client asked the server to keep while it is away:
// the subscriptions to restore, the messages published to those subscriptions
// while there was no connection, and the deadline after which the state ends.
//
// The broker stores that state only for a session with no live connection. While
// a connection owns the session its subscriptions live on the client and the
// client has taken the queued messages over (they drain right after the CONNACK),
// so the store and Broker.clients never hold the same key at the same time. That
// invariant is what makes a publication reach an offline session exactly once:
// LocalTargets reads the offline half, SendLocal re-checks it, and a session that
// reconnected in between receives the message on its live connection instead of
// a stale queue.
//
// The reference broker of this family also queues for clean sessions; gmqx does
// not, because a clean session is deleted with its connection and recreated empty
// by the next CONNECT — a queued message would be delivered to a session that
// declared it wanted none.

import (
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// session is the broker's in-memory form of the stored state.
type session struct {
	clientID  string
	subs      map[string]subscription
	queue     []queuedMessage
	expiresAt time.Time
}

// queuedMessage is one message the broker owed a persistent session while it was
// offline, with its MQTT 5 properties in typed form (the stored form keeps the
// property block instead).
type queuedMessage struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
	props   mqtt.Properties
}

// sessionState converts the broker's form into the stored form.
func sessionState(s *session) SessionState {
	state := SessionState{ClientID: s.clientID, ExpiresAt: s.expiresAt}
	if len(s.subs) > 0 {
		state.Subscriptions = make(map[string]SessionSubscription, len(s.subs))
		for filter, sub := range s.subs {
			state.Subscriptions[filter] = SessionSubscription{
				QoS:               sub.qos,
				NoLocal:           sub.noLocal,
				RetainAsPublished: sub.retainAsPublished,
				RetainHandling:    sub.retainHandling,
			}
		}
	}
	if len(s.queue) > 0 {
		state.Queue = make([]QueuedMessage, 0, len(s.queue))
		for _, msg := range s.queue {
			props, err := encodePropertyBlock(msg.props)
			if err != nil {
				// A message whose properties cannot be encoded still travels; it
				// simply carries none, exactly like a will with invalid properties.
				props = nil
			}
			state.Queue = append(state.Queue, QueuedMessage{
				Topic:   msg.topic,
				Payload: msg.payload,
				QoS:     msg.qos,
				Retain:  msg.retain,
				Props:   props,
			})
		}
	}
	return state
}

// sessionFromState converts the stored form into the broker's form and reports
// how many queued messages had to be dropped because their property block no
// longer decodes.
func sessionFromState(state SessionState) (*session, int) {
	s := &session{clientID: state.ClientID, expiresAt: state.ExpiresAt, subs: make(map[string]subscription, len(state.Subscriptions))}
	for filter, sub := range state.Subscriptions {
		s.subs[filter] = subscription{
			qos:               sub.QoS,
			noLocal:           sub.NoLocal,
			retainAsPublished: sub.RetainAsPublished,
			retainHandling:    sub.RetainHandling,
		}
	}
	dropped := 0
	if len(state.Queue) > 0 {
		s.queue = make([]queuedMessage, 0, len(state.Queue))
		for _, msg := range state.Queue {
			props, err := decodePropertyBlock(msg.Props)
			if err != nil {
				dropped++
				continue
			}
			s.queue = append(s.queue, queuedMessage{
				topic:   msg.Topic,
				payload: msg.Payload,
				qos:     msg.QoS,
				retain:  msg.Retain,
				props:   props,
			})
		}
	}
	return s, dropped
}

// takeStoredSession removes the stored session of key and hands it to a
// connection. The caller holds b.mu. The global offline-queue counter follows the
// messages that leave the store, because those messages are now owned by the
// connection.
//
// An expired session is never handed over, even when the sweeper has not reached
// it yet: the Session Expiry Interval is a deadline, not a hint.
func (b *Broker) takeStoredSession(key string) (*session, bool) {
	state, ok := b.store.Load(key)
	if !ok {
		return nil, false
	}
	b.store.Delete(key)
	b.offlineQueued = max(0, b.offlineQueued-len(state.Queue))
	if !state.ExpiresAt.IsZero() && !time.Now().Before(state.ExpiresAt) {
		b.metrics.RecordDroppedMessage("session_expired")
		return nil, false
	}
	s, dropped := sessionFromState(state)
	if dropped > 0 {
		b.log.Warn("gmqx dropped queued messages with an undecodable property block",
			"client", s.clientID, "count", dropped)
		b.metrics.RecordDroppedMessage("offline_queue_corrupt")
	}
	return s, true
}

// deleteStoredSession removes key and forgets its queued messages. The caller
// holds b.mu.
func (b *Broker) deleteStoredSession(key string) {
	if state, ok := b.store.Load(key); ok {
		b.offlineQueued = max(0, b.offlineQueued-len(state.Queue))
	}
	b.store.Delete(key)
}

// saveStoredSession writes the state the connection c leaves behind: its
// subscriptions, the messages of the queue it did not manage to drain, and its
// expiry deadline. The caller holds b.mu and c.mu, and c.queue is handed over to
// the store.
func (b *Broker) saveStoredSession(key string, c *client) {
	if stale, ok := b.store.Load(key); ok {
		// Defensive: a stored session of a live connection should not exist, but
		// the counter must follow what the store actually holds.
		b.offlineQueued = max(0, b.offlineQueued-len(stale.Queue))
	}
	state := sessionState(&session{
		clientID:  c.id,
		subs:      c.subs,
		queue:     c.queue,
		expiresAt: c.sessionExpiryDeadline(),
	})
	b.store.Save(key, state)
	b.offlineQueued += len(c.queue)
	c.queue = nil
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
			b.metrics.RecordDroppedMessage("session_expired")
			b.metrics.RecordQueueDepth("offline", b.offlineQueued)
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
	key := target.sessionKey
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
		b.metrics.RecordDroppedMessage("offline_session_missing")
		return
	}
	if !state.ExpiresAt.IsZero() && !time.Now().Before(state.ExpiresAt) {
		// The session is past its deadline and only waits for the sweeper.
		b.mu.Unlock()
		b.metrics.RecordDroppedMessage("offline_session_missing")
		return
	}
	if limit := b.cfg.MaxTotalOfflineMessages; limit > 0 && b.offlineQueued >= limit {
		b.mu.Unlock()
		b.metrics.RecordDroppedMessage("offline_queue_total")
		return
	}
	queue := make([]QueuedMessage, 0, len(state.Queue)+1)
	queue = append(queue, state.Queue...)
	queue = append(queue, QueuedMessage{
		Topic:   pub.Topic,
		Payload: append([]byte(nil), pub.Payload...),
		QoS:     target.QoS,
		Retain:  target.Retain,
		Props:   append([]byte(nil), pub.Props...),
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
		b.metrics.RecordDroppedMessage("offline_queue_full")
	}
	b.metrics.RecordQueueDepth("offline", depth)
}
