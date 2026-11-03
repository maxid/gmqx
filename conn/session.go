package conn

// The session a connection leaves behind, and the host-facing view of a
// connection
//
// A connection owns its session while it lives: the subscriptions, the queue it
// drained after the CONNACK, the QoS 2 state of both directions and the outbound
// exchanges it has not confirmed. When it ends — normally, by a take-over, or as a
// peer's kick — that state moves to the broker, which decides what to store, what
// to hand to the replacement and what to drop.
//
// The handoff runs under the connection's own lock (HandOff, TakeOver) and that is
// the whole point: the state is taken and the connection stops owning its queue in
// one critical section, so a delivery that arrives at the same moment is either
// still the connection's (it is delivered, or collected into pending) or already
// the host's (it goes through Host.EnqueueOffline). Without that, a publication
// could be written to a connection whose session was just stored, and be lost with
// the socket.
//
// The state is handed over in two parts on purpose: the queue the session already
// had, and the deliveries the connection still owed. The host merges them, because
// the merge is where its capacity limits (MaxTotalOfflineMessages,
// MaxOfflineQueueSize) apply, and those are broker policy, not protocol.

import (
	"context"
	"time"

	"github.com/maxid/gmqx/hook"
	"github.com/maxid/gmqx/session"
)

// Handoff is the session one connection hands to its host when it ends or is
// displaced.
type Handoff struct {
	// ClientID is the MQTT client identifier of the session.
	ClientID string
	// Subs are the session's subscriptions, detached from the connection.
	Subs map[string]session.Subscription
	// Queue is what the session queued while it was offline and has not drained
	// yet, in publication order.
	Queue []session.Pending
	// Pending are the deliveries the connection still owed its inflight window.
	// They were acknowledged to their publisher and never written, so the host
	// appends them to the queue it stores (subject to its own capacity limits).
	Pending []session.Pending
	// InboundQoS2 are the packet ids the client published at QoS 2 whose PUBREL has
	// not arrived yet.
	InboundQoS2 []uint16
	// Inflight are the outbound exchanges the client has not confirmed.
	Inflight []session.Inflight
	// ExpiresAt is the deadline of the session this connection leaves behind.
	ExpiresAt time.Time
}

// State turns a handoff into the session form the persistence seam stores, with the
// pending deliveries appended to the queue in arrival order.
func (h *Handoff) State() *session.State {
	queue := h.Queue
	if len(h.Pending) > 0 {
		queue = make([]session.Pending, 0, len(h.Queue)+len(h.Pending))
		queue = append(queue, h.Queue...)
		queue = append(queue, h.Pending...)
	}
	return &session.State{
		ClientID:    h.ClientID,
		Subs:        h.Subs,
		Queue:       queue,
		ExpiresAt:   h.ExpiresAt,
		InboundQoS2: h.InboundQoS2,
		Inflight:    h.Inflight,
	}
}

// HandOff runs fn under the connection's session lock with the session this
// connection leaves behind, and marks the connection as handing it over: after fn
// returns, a delivery that still arrives with a target of this connection goes
// through Host.EnqueueOffline instead of the socket.
//
// fn runs with the lock held and must not call back into the connection. It is the
// host's chance to write the state out before any delivery can observe the
// handoff.
func (c *Client) HandOff(fn func(*Handoff)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn(c.detachLocked())
	c.queue = nil
	c.pending = nil
	c.handoff = handoffStored
}

// TakeOver is HandOff for a connection a replacement is displacing. It marks the
// connection superseded first, so its own teardown knows the session already
// belongs to another connection.
//
// fn reports whether the host took the session; when it did not (a stored session
// won, which is the defensive case), the connection keeps its queue and pending
// deliveries: its teardown then drops them the way it always did, and counting
// them stays where it was.
func (c *Client) TakeOver(fn func(*Handoff) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.superseded = true
	if !fn(c.detachLocked()) {
		return
	}
	c.queue = nil
	c.pending = nil
	c.handoff = handoffStored
}

// Discard drops the deliveries the connection still owed and marks the connection
// as no longer owning them, because the session lives on another node (a peer's
// kick) or on another connection here. It returns how many deliveries were
// dropped, so the host counts them with its own reason.
func (c *Client) Discard() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.pending)
	c.pending = nil
	c.handoff = handoffStored
	return n
}

// DropPending drops the deliveries the connection still owed without changing how
// a later delivery is handled: it is the clean-session case, whose state ends with
// the connection.
func (c *Client) DropPending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.pending)
	c.pending = nil
	return n
}

// detachLocked builds the handoff state of the connection. The caller holds c.mu.
func (c *Client) detachLocked() *Handoff {
	inbound, inflight := c.snapshotQoS2()
	pending := make([]session.Pending, 0, len(c.pending))
	for _, m := range c.pending {
		pending = append(pending, session.Pending{
			Topic:   m.topic,
			Payload: m.payload,
			QoS:     m.qos,
			Retain:  m.retain,
			Props:   m.props,
		})
	}
	return &Handoff{
		ClientID:    c.id,
		Subs:        session.CopySubscriptions(c.subs),
		Queue:       c.queue,
		Pending:     pending,
		InboundQoS2: inbound,
		Inflight:    inflight,
		ExpiresAt:   c.SessionExpiryDeadline(),
	}
}

// Adopt installs the session a CONNECT resumed (or took over from a displaced
// connection): its subscriptions, its queue and the QoS 2 state of both
// directions, whose outbound exchanges are scheduled for retransmission right
// after the CONNACK.
func (c *Client) Adopt(state *session.State) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs = state.Subs
	c.queue = state.Queue
	c.restoreQoS2Locked(state)
	c.sessionPresent = true
}

// EnsureSubscriptions gives a connection that resumed a session without
// subscriptions an empty set, so a SUBSCRIBE has a map to write into.
func (c *Client) EnsureSubscriptions() {
	c.mu.Lock()
	if c.subs == nil {
		c.subs = make(map[string]session.Subscription)
	}
	c.mu.Unlock()
}

// Subscriptions returns a detached copy of the live subscription set: what the
// routing index installs when the connection is registered.
func (c *Client) Subscriptions() map[string]session.Subscription {
	c.mu.Lock()
	defer c.mu.Unlock()
	return session.CopySubscriptions(c.subs)
}

// InstallSubscription adds one filter to the live session. A max above zero is the
// broker's per-connection subscription budget: a new filter over it is refused
// (granted false) while an existing one is always updated. It reports whether the
// filter was already subscribed, which is what the SUBACK and the cluster summary
// care about.
func (c *Client) InstallSubscription(filter string, sub session.Subscription, max int) (granted, existed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, existed = c.subs[filter]
	if max > 0 && len(c.subs) >= max && !existed {
		return false, existed
	}
	if c.subs == nil {
		c.subs = make(map[string]session.Subscription, 1)
	}
	c.subs[filter] = sub
	return true, existed
}

// RemoveSubscription drops one filter and reports whether it was subscribed.
func (c *Client) RemoveSubscription(filter string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, existed := c.subs[filter]
	delete(c.subs, filter)
	return existed
}

// InflightDepth is the number of outbound exchanges the connection has not
// confirmed yet.
func (c *Client) InflightDepth() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.inflight)
}

// PendingDepth is the number of deliveries waiting for a free inflight slot.
func (c *Client) PendingDepth() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// QueueDepth is the number of messages the session queued while it had no
// connection and has not drained yet.
func (c *Client) QueueDepth() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queue)
}

// WritesClosed reports whether the connection refuses further packets: it is
// closing, its socket failed or it was displaced.
func (c *Client) WritesClosed() bool { return c.writeClosed.Load() }

// WriterStopped reports whether the writer goroutine has ended. A connection
// whose writer is still running may still write to the socket.
func (c *Client) WriterStopped() bool {
	select {
	case <-c.writerDone:
		return true
	default:
		return false
	}
}

// ID is the MQTT client identifier of the connection.
func (c *Client) ID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.id
}

// Namespace is the isolation scope the connection belongs to.
func (c *Client) Namespace() string { return c.namespace }

// ConnID is the identity of the physical connection.
func (c *Client) ConnID() ConnectionID { return c.connID }

// Context is the connection's context: what every hook of this connection
// receives. It is cancelled when the connection ends.
func (c *Client) Context() context.Context { return c.context() }

// Info is the CONNECT the client sent, as the connection settled it (an assigned
// client id, a re-authenticated identity).
func (c *Client) Info() hook.ConnectInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.info
}

// Username is the username the connection authenticated with.
func (c *Client) Username() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.username
}

// Version is the MQTT protocol level of the connection.
func (c *Client) Version() byte { return c.version }

// ConnectedAt is when the session of this connection was registered. It is the
// reference point of the broker's anti-flap window.
func (c *Client) ConnectedAt() time.Time { return c.connectedAt }

// MarkRegistered records that the session of this connection is installed, and
// when.
func (c *Client) MarkRegistered(now time.Time) {
	c.registered = true
	c.connectedAt = now
}

// MarkSuperseded records that another connection on this node owns the session.
func (c *Client) MarkSuperseded() {
	c.mu.Lock()
	c.superseded = true
	c.mu.Unlock()
}

// HasSubscriptions reports whether the live session has at least one filter.
func (c *Client) HasSubscriptions() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.subs) > 0
}

// MarkUnregistered clears the registered flag and reports whether it was set.
func (c *Client) MarkUnregistered() bool {
	was := c.registered
	c.registered = false
	return was
}

// Registered reports whether the session of this connection is installed.
func (c *Client) Registered() bool { return c.registered }

// SessionPresent reports whether the CONNACK of this connection carries Session
// Present: the session was resumed or taken over.
func (c *Client) SessionPresent() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionPresent
}

// Kicked reports whether a peer's connect-time kick ended this connection.
func (c *Client) Kicked() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.kicked
}

// Superseded reports whether another connection on this node displaced this one.
func (c *Client) Superseded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.superseded
}

// Lost reports whether the connection ended without a DISCONNECT from the client.
func (c *Client) Lost() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lost
}

// CloseErr is why the connection ended, nil for a clean DISCONNECT.
func (c *Client) CloseErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}

// PersistentSession reports whether the session outlives the connection.
func (c *Client) PersistentSession() bool { return c.persistentSession() }

// ResumesSession reports whether the CONNECT asks to resume a stored session
// (Clean Session / Clean Start cleared).
func (c *Client) ResumesSession() bool { return c.resumesSession() }

// Displace ends the connection because a replacement took its session: the client
// gets a DISCONNECT 0x8E (MQTT 5) before the socket closes, and the teardown treats
// the connection as kicked.
func (c *Client) Displace() { c.displace() }

// Close ends the connection without publishing its will.
func (c *Client) Close() { c.close(false) }

// restoreQoS2Locked hands the QoS 2 state of a resumed session to the connection:
// the inbound packet ids the client has not released and the outbound exchanges it
// has not confirmed. The outbound exchanges are scheduled for retransmission
// exactly like a freshly registered one, and resumeInflight re-sends them after the
// CONNACK. The caller holds c.mu.
func (c *Client) restoreQoS2Locked(s *session.State) {
	for _, id := range s.InboundQoS2 {
		c.inboundQoS2[id] = struct{}{}
	}
	retry := c.host.Limits().QoSRetryInterval
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
		if c.host.Limits().QoSMaxRetries >= 0 {
			m.due = now.Add(retry)
		}
		c.addInflightLocked(m.packetID, m)
		c.restoredInflight = append(c.restoredInflight, m.packetID)
	}
}

// snapshotQoS2 returns the connection's QoS 2 state in the projection a session
// carries. The caller holds c.mu, which is what makes the snapshot consistent with
// the acknowledgements the connection is processing.
func (c *Client) snapshotQoS2() ([]uint16, []session.Inflight) {
	inbound := make([]uint16, 0, len(c.inboundQoS2))
	for id := range c.inboundQoS2 {
		inbound = append(inbound, id)
	}
	inflight := make([]session.Inflight, 0, len(c.inflight))
	for _, m := range c.inflight {
		inflight = append(inflight, session.Inflight{
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
