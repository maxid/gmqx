package conn

// Outbound QoS 1/2 delivery
//
// An outbound QoS 1 or QoS 2 PUBLISH is not fire-and-forget: the client has to
// confirm it, and the broker has to keep the message (and its packet id) until it
// does. This file is that bookkeeping.
//
// The table is per connection and keyed by packet id, because a packet id is only
// unique on one connection. It holds the three states of the two exchanges:
//
//	QoS 1: PUBLISH ──PUBACK──▶ done
//	QoS 2: PUBLISH ──PUBREC──▶ PUBREL ──PUBCOMP──▶ done
//
// One timer per connection drives every retransmission: it is armed with the
// earliest deadline of the table and re-armed after each pass, so a thousand
// inflight messages cost one timer, not a thousand. Every schedule also wakes the
// timer, so a newly registered exchange is never behind a long wait of an older
// one. The goroutine ends with the connection context, which close cancels, so a
// closed connection leaves neither a goroutine nor a timer behind.
//
// The window of a connection is a promise and a budget: MaxInflightQoS1/2 bound
// how many exchanges may be unconfirmed at once. A delivery that would exceed its
// window waits in the connection's pending queue (pending, bounded by
// Config.MaxInflightQueueSize) instead of blocking the publishing goroutine — a
// publisher has already been acknowledged by the sink and must not be stalled by
// a slow subscriber — or being skipped: an acknowledgement that frees a slot
// drains the queue in arrival order. The pending queue belongs to the live
// connection, unlike the stored offline queue of a persistent session, and it
// ends with the connection.
//
// A pending queue that is full is where the two kinds of session part. A
// persistent session is not dropped from at all: the delivery joins the queue and
// the connection is closed, so the session — its subscriptions, the exchanges it
// has not confirmed and everything still waiting in pending — is stored and the
// next CONNECT drains it. That hand-off reuses the machinery a lost connection
// already has (close → unregister → store → drain after the next CONNACK);
// the only loss points that remain are the offline queue's own capacity limits
// (Config.MaxOfflineQueueSize, Config.MaxTotalOfflineMessages), which are a
// documented capacity limit and not a defect. A clean session has nowhere to
// queue — its state ends with the connection — so the delivery is still dropped
// there, on one explicit path with its own reason ("inflight_overflow_clean").
// See sendPublish.
//
// The reference broker of this family retries every unacked PUBLISH but never
// sets DUP and has no window at all; both are defects this implementation does
// not copy. Inbound QoS 2 state does survive the connection when the session does:
// the packet ids a client published at QoS 2 and has not released with PUBREL are
// part of the stored session (see SessionState.InboundQoS2), so a publisher that
// reconnects and retransmits gets the same acknowledgement and no second delivery.
// Outbound exchanges survive the same way (SessionState.Inflight): the packets a
// session still owes its client are re-sent on resume, with DUP set for a PUBLISH
// and as a PUBREL for a confirmed QoS 2 publish, exactly as MQTT §4.4 requires.

import (
	"errors"
	"sort"
	"time"

	"github.com/maxid/gnety/codec/mqtt"

	ns "github.com/maxid/gmqx/internal/namespace"
	"github.com/maxid/gmqx/routing"
	"github.com/maxid/gmqx/session"
)

var (
	// errClientClosed reports a write to a connection that is already gone.
	errClientClosed = errors.New("gmqx: connection closed")
	// errInflightFull reports an outbound QoS 1/2 delivery that was dropped: a
	// clean session's inflight window and the queue waiting behind it are both
	// full, and a clean session has no offline queue to keep the delivery in.
	errInflightFull = errors.New("gmqx: inflight window full")
)

// handoffState is where a persistent session of a live connection stands while it
// moves to its offline queue. The value is kept under c.mu, so a delivery and the
// unregister that stores the session cannot disagree about which side of the move
// the delivery belongs to.
type handoffState byte

const (
	// handoffNone is a connection that delivers normally. Every clean session
	// stays here: it has no offline queue to move to.
	handoffNone handoffState = iota
	// handoffCollecting is a persistent connection that is closing. Its session is
	// not stored yet, so a delivery that still arrives waits in pending, in arrival
	// order, next to the deliveries that were already waiting there.
	handoffCollecting
	// handoffStored is a connection whose session has been stored (unregister ran)
	// or handed to a new connection (a take-over). A delivery that still arrives
	// with a stale target of this connection is queued through the offline path.
	handoffStored
)

// inflightState is where an outbound exchange stands.
type inflightState byte

const (
	// awaitPuback is a QoS 1 PUBLISH waiting for its PUBACK.
	awaitPuback inflightState = iota
	// awaitPubrec is a QoS 2 PUBLISH waiting for its PUBREC.
	awaitPubrec
	// awaitPubcomp is a PUBREL waiting for its PUBCOMP.
	awaitPubcomp
)

// inflightStage is the projection of a live exchange's stage into the form a
// session carries across a disconnect.
func inflightStage(state inflightState) session.Stage {
	switch state {
	case awaitPubrec:
		return session.AwaitPubrec
	case awaitPubcomp:
		return session.AwaitPubcomp
	default:
		return session.AwaitPuback
	}
}

// liveInflightState is the inverse of inflightStage. session.State records the
// unknown stored stage to InflightAwaitPuback, so the default is only a guard.
func liveInflightState(stage session.Stage) inflightState {
	switch stage {
	case session.AwaitPubrec:
		return awaitPubrec
	case session.AwaitPubcomp:
		return awaitPubcomp
	default:
		return awaitPuback
	}
}

// inflightMessage is one outbound QoS 1/2 exchange the connection owes a
// confirmation for. It keeps everything a retransmission needs, so the
// retransmitter never depends on broker state that may have changed.
type inflightMessage struct {
	topic    string
	payload  []byte
	props    mqtt.Properties
	packetID uint16
	qos      byte
	state    inflightState
	retain   bool
	// attempts counts the retransmissions already sent. due is when the next one
	// is due; the zero time means the exchange is not scheduled (retransmission
	// disabled, or the attempt cap was reached).
	attempts int
	due      time.Time
}

// pendingMessage is one outbound QoS 1/2 delivery that found no free slot in the
// connection's inflight window. It keeps what a later PUBLISH needs — topic, QoS,
// payload, retain flag, MQTT 5 properties — but no packet id: the id is assigned
// when the delivery finally enters the window, so a queued message can never
// collide with an exchange that is already retransmitting.
type pendingMessage struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
	props   mqtt.Properties
}

// retryDelay is the wait before retransmission number attempt (1-based): the base
// interval doubled per earlier attempt, bounded so a long-lived exchange cannot
// overflow a timer.
func retryDelay(base time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 10 {
		shift = 10
	}
	const ceiling = 4 * time.Hour
	if delay := base << shift; delay > 0 && delay < ceiling {
		return delay
	}
	return ceiling
}

// nextPacketIDLocked reserves the next packet id no inflight exchange of this
// connection holds, so a retransmission can never collide with a new message. It
// reports false when every one of the 65535 ids is in use. The caller holds c.mu.
func (c *Client) nextPacketIDLocked() (uint16, bool) {
	for i := 0; i < 65536; i++ {
		c.outPID++
		if c.outPID == 0 {
			// Packet id 0 is not a valid identifier.
			c.outPID = 1
		}
		if _, used := c.inflight[c.outPID]; !used {
			return c.outPID, true
		}
	}
	return 0, false
}

// inflightLimit is the configured window of qos; zero or less means unlimited.
func (c *Client) inflightLimit(qos byte) int {
	if qos == 2 {
		return c.host.Limits().MaxInflightQoS2
	}
	return c.host.Limits().MaxInflightQoS1
}

// clientReceiveMaximum is the MQTT 5 Receive Maximum of this connection: how many
// QoS 1/2 PUBLISHes may be outstanding toward the client at once. The absent
// property means the protocol default 65535, which the handshake already folded
// into the field.
func (c *Client) clientReceiveMaximum() int {
	if c.receiveMaximum == 0 {
		return maxReceiveMaximum
	}
	return int(c.receiveMaximum)
}

// inflightQueueLimit is how many deliveries of this connection may wait for a
// free inflight slot. It is always bounded: a non-positive configuration (or a
// broker built without the Config defaulting) selects the documented default.
func (c *Client) inflightQueueLimit() int {
	if limit := c.host.Limits().MaxInflightQueueSize; limit > 0 {
		return limit
	}
	return defaultMaxInflightQueueSize
}

// addInflightLocked registers an outbound exchange and keeps the window counters
// in step with the map. The caller holds c.mu.
func (c *Client) addInflightLocked(id uint16, m *inflightMessage) {
	c.inflight[id] = m
	switch m.qos {
	case 1:
		c.inflightQoS1++
	case 2:
		c.inflightQoS2++
	}
	if m.state != awaitPubcomp {
		c.inflightUnconfirmed++
	}
}

// deleteInflightLocked removes an outbound exchange and keeps the window counters
// in step. It reports whether the entry was there. The caller holds c.mu.
func (c *Client) deleteInflightLocked(id uint16) bool {
	m, ok := c.inflight[id]
	if !ok {
		return false
	}
	delete(c.inflight, id)
	switch m.qos {
	case 1:
		c.inflightQoS1--
	case 2:
		c.inflightQoS2--
	}
	if m.state != awaitPubcomp {
		c.inflightUnconfirmed--
	}
	return true
}

// confirmInflightLocked marks a QoS 2 exchange as confirmed by its PUBREC: the
// PUBREL that follows is a separate exchange and no longer occupies the client's
// Receive Maximum. The caller holds c.mu.
func (c *Client) confirmInflightLocked(m *inflightMessage) {
	if m.state == awaitPubcomp {
		return
	}
	m.state = awaitPubcomp
	c.inflightUnconfirmed--
}

// windowFullLocked reports whether an outbound PUBLISH of qos would exceed a
// window of this connection: the configured per-QoS window (MaxInflightQoS1/2)
// or the Receive Maximum the client asked for in its CONNECT (§3.3.4), whichever
// is hit first. The Receive Maximum counts the QoS 1 and QoS 2 PUBLISHes together
// and only while neither their PUBACK nor their PUBREC has arrived — once PUBREC
// is in, the PUBLISH is confirmed and the PUBREL that follows is a separate
// exchange, so an awaitPubcomp entry does not occupy the client's window. The
// caller holds c.mu.
//
// Both answers come from the counters addInflightLocked/deleteInflightLocked
// maintain, not from a scan of the inflight table: this runs once per delivery,
// under the connection lock every publisher writing to the connection shares.
func (c *Client) windowFullLocked(qos byte) bool {
	if limit := c.inflightLimit(qos); limit > 0 {
		if qos == 2 {
			if c.inflightQoS2 >= limit {
				return true
			}
		} else if c.inflightQoS1 >= limit {
			return true
		}
	}
	return c.receiveMaximumFullLocked()
}

// receiveMaximumFullLocked reports whether another QoS 1/2 PUBLISH would exceed
// the Receive Maximum the client announced. The caller holds c.mu.
func (c *Client) receiveMaximumFullLocked() bool {
	limit := c.clientReceiveMaximum()
	return limit < maxReceiveMaximum && c.inflightUnconfirmed >= limit
}

// inboundWindowFullLocked reports whether another inbound QoS 1/2 exchange would
// exceed the broker's receive maximum (see Host.Limits). The inbound
// QoS 2 exchanges still waiting for their PUBREL are the ones that occupy the
// window; a QoS 1 publish is acknowledged as soon as it is accepted, so it never
// holds a slot. The caller holds c.mu.
func (c *Client) inboundWindowFullLocked() bool {
	limit := c.host.Limits().ReceiveMaximum
	return limit > 0 && len(c.inboundQoS2) >= limit
}

// wakeRetransmitter asks the connection's retransmission timer to recompute its
// deadline. The channel is buffered and the send never blocks, so it is safe from
// any goroutine a delivery comes from.
func (c *Client) wakeRetransmitter() {
	select {
	case c.retryWake <- struct{}{}:
	default:
	}
}

// startRetransmitter starts the connection's single retransmission timer. It is
// called once, from the handshake, after the CONNACK has been written.
func (c *Client) startRetransmitter() {
	go c.retransmitLoop()
}

// retransmitLoop waits for the earliest retransmission deadline, sends what is
// due and re-arms itself. It returns when the connection ends: the context is
// cancelled by close, so a closed connection leaves neither a goroutine nor a
// timer behind.
//
// One timer is reused for the whole life of the connection, and it is only reset
// when the earliest deadline actually moves: a delivery whose due time is later
// than the armed one does not wake or rearm the loop (see writePublishLocked).
// Waking the loop per delivery cost a goroutine wakeup, a scan of the whole
// inflight table and a fresh time.Timer, all under the connection lock the
// delivery path contends on.
func (c *Client) retransmitLoop() {
	var timer *time.Timer
	var timerC <-chan time.Time
	var armed time.Time
	stopTimer := func() {
		if timer == nil || timerC == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
		armed = time.Time{}
	}
	for {
		deadline, ok := c.nextRetransmitDeadline()
		if !ok {
			stopTimer()
		} else if timer == nil {
			timer = time.NewTimer(time.Until(deadline))
			timerC = timer.C
			armed = deadline
		} else if !armed.Equal(deadline) {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(time.Until(deadline))
			timerC = timer.C
			armed = deadline
		}
		select {
		case <-timerC:
			c.retransmitDue()
		case <-c.retryWake:
		case <-c.connCtx.Done():
			stopTimer()
			return
		}
	}
}

// nextRetransmitDeadline is the earliest scheduled retransmission of the table,
// and whether there is one. It records the answer in retryAt, which is what lets
// a new exchange decide whether the sleeping loop has to be woken.
func (c *Client) nextRetransmitDeadline() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var earliest time.Time
	for _, m := range c.inflight {
		if m.due.IsZero() {
			continue
		}
		if earliest.IsZero() || m.due.Before(earliest) {
			earliest = m.due
		}
	}
	if earliest.IsZero() {
		c.retryAt = time.Time{}
		return time.Time{}, false
	}
	c.retryAt = earliest
	return earliest, true
}

// retransmitDue sends every retransmission that is due. A PUBLISH is resent with
// the same packet id and DUP set; a PUBREL is resent as it is. The next attempt of
// each exchange is scheduled before its write, so a slow peer cannot turn one pass
// into a burst. An exchange that reaches Config.QoSMaxRetries is left unscheduled:
// its packet id stays reserved until the client confirms it or the connection
// ends, which is what keeps a late confirmation from being mistaken for the
// confirmation of a newer message.
func (c *Client) retransmitDue() {
	now := time.Now()
	maxRetries := c.host.Limits().QoSMaxRetries
	var batch []inflightResend
	exhausted := 0

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	for id, m := range c.inflight {
		if m.due.IsZero() || now.Before(m.due) {
			continue
		}
		m.attempts++
		if maxRetries >= 0 && m.attempts > maxRetries {
			// The cap is reached: the exchange stays registered (its packet id
			// remains reserved) but is no longer retransmitted.
			m.due = time.Time{}
			exhausted++
			continue
		}
		batch = append(batch, inflightResend{
			topic:    m.topic,
			payload:  m.payload,
			props:    m.props,
			packetID: id,
			qos:      m.qos,
			state:    m.state,
			retain:   m.retain,
		})
		m.due = now.Add(retryDelay(c.host.Limits().QoSRetryInterval, m.attempts))
	}
	c.mu.Unlock()

	for i := 0; i < exhausted; i++ {
		c.host.Metrics().RecordDroppedMessage("retry_exhausted")
	}
	c.resendInflight(batch)
}

// inflightResend is one snapshot of an exchange that has to be written again: the
// message, its original packet id and the stage it reached. A snapshot is taken
// under c.mu and written outside it, so a slow socket never holds the lock that the
// acknowledgements of the connection need.
type inflightResend struct {
	topic    string
	payload  []byte
	props    mqtt.Properties
	packetID uint16
	qos      byte
	state    inflightState
	retain   bool
}

// resendInflight writes the snapshot of a retransmission or of a resumed session: a
// PUBLISH with DUP set and its original packet id while it is unconfirmed, a PUBREL
// once PUBREC arrived.
//
// A packet the client's Maximum Packet Size forbids can never be delivered, so the
// exchange is given up instead of being retried forever: the packet id is released
// (the drop itself was counted by enqueue) and the queue behind the window moves on.
func (c *Client) resendInflight(batch []inflightResend) {
	for _, r := range batch {
		var err error
		if r.state == awaitPubcomp {
			err = c.send(&mqtt.Pubrel{MessageHeader: c.header(), PacketID: r.packetID})
		} else {
			err = c.sendPublishFrame(&mqtt.Publish{
				MessageHeader: c.header(),
				Topic:         r.topic,
				Payload:       r.payload,
				QoS:           r.qos,
				PacketID:      r.packetID,
				Retain:        r.retain,
				Dup:           true,
				Properties:    r.props,
			})
		}
		if errors.Is(err, errPacketTooLarge) {
			c.mu.Lock()
			// The state check keeps a PUBREC that arrived in between from dropping
			// an exchange that is confirmed but not complete yet.
			if entry, ok := c.inflight[r.packetID]; ok && entry.state == r.state {
				c.deleteInflightLocked(r.packetID)
				c.drainPendingLocked()
			}
			c.mu.Unlock()
		}
	}
}

// resumeInflight re-sends the exchanges a resumed session left unconfirmed: every
// QoS 1/2 PUBLISH whose PUBACK or PUBREC never arrived goes out again with DUP set
// and its original packet id, and every confirmed QoS 2 publish whose PUBCOMP never
// arrived goes out as its PUBREL again. MQTT §4.4 requires exactly this — "both the
// Client and Server MUST resend any unacknowledged PUBLISH packets (where QoS > 0)
// and PUBREL packets using their original Packet Identifiers" — and it is the only
// way a delivery the broker already acknowledged to its publisher survives a
// reconnect without being lost or duplicated.
//
// It runs after the CONNACK (startWriter has opened the queue) and before the
// offline queue drains, so the packets the client was already sent keep their
// place ahead of messages it has never seen. Entries are sent in packet-id order
// instead of map order, so a resumed session is reproducible.
//
// A client that reconnects with a smaller Receive Maximum than the exchanges it
// left unconfirmed still gets all of them: they were sent under the previous
// connection and §4.4 requires the retransmission under the original packet ids.
// The new value applies to new deliveries, which wait in the inflight queue, not
// to the completion of an exchange that is already in flight.
func (c *Client) resumeInflight() {
	c.mu.Lock()
	batch := c.restoredBatchLocked()
	c.mu.Unlock()

	c.resendInflight(batch)
}

// restoredBatchLocked is the resend snapshot of the exchanges restoreQoS2State
// carried over from the previous connection: those still unconfirmed when the
// handshake finishes, in packet-id order, and only those. A delivery that reached
// this fresh connection between the CONNACK and this call is registered in the
// same table but was already written once; resending it would duplicate it with
// DUP set (the Windows CI failure that led to this split).
//
// The caller holds c.mu.
func (c *Client) restoredBatchLocked() []inflightResend {
	ids := c.restoredInflight
	c.restoredInflight = nil
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	batch := make([]inflightResend, 0, len(ids))
	for _, id := range ids {
		m, ok := c.inflight[id]
		if !ok {
			// Confirmed or given up between the CONNACK and now.
			continue
		}
		batch = append(batch, inflightResend{
			topic:    m.topic,
			payload:  m.payload,
			props:    m.props,
			packetID: id,
			qos:      m.qos,
			state:    m.state,
			retain:   m.retain,
		})
	}
	return batch
}

// writePublish sends one PUBLISH to this connection. For QoS 1 and 2 it registers
// the exchange (which the retransmission timer then owns) before writing, so an
// acknowledgement can never arrive for an unknown packet id. A QoS 1/2 message
// that finds no free packet id is downgraded to QoS 0 and delivered rather than
// dropped, which is also what the reference broker does when its ids run out.
func (c *Client) writePublish(topic string, payload []byte, qos byte, retain bool, props mqtt.Properties) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.writeClosed.Load() {
		return errClientClosed
	}
	return c.writePublishLocked(topic, payload, qos, retain, props)
}

// writePublishLocked is writePublish with the connection lock already held, so the
// window check and the registration of the exchange are one step and a delivery
// can never slip past a window that is full. The caller holds c.mu.
func (c *Client) writePublishLocked(topic string, payload []byte, qos byte, retain bool, props mqtt.Properties) error {
	packetID := uint16(0)
	var registered *inflightMessage
	if qos > 0 {
		id, ok := c.nextPacketIDLocked()
		if !ok {
			qos = 0
			c.host.Metrics().RecordOverflow("packet_ids")
		} else {
			packetID = id
			registered = &inflightMessage{
				topic:    topic,
				payload:  payload,
				props:    props,
				packetID: id,
				qos:      qos,
				state:    awaitPuback,
				retain:   retain,
			}
			if qos == 2 {
				registered.state = awaitPubrec
			}
			if c.host.Limits().QoSMaxRetries >= 0 {
				registered.due = time.Now().Add(c.host.Limits().QoSRetryInterval)
			}
			c.addInflightLocked(id, registered)
		}
	}

	err := c.sendPublishFrame(&mqtt.Publish{
		MessageHeader: c.header(),
		Topic:         topic,
		Payload:       payload,
		QoS:           qos,
		PacketID:      packetID,
		Retain:        retain,
		Properties:    props,
	})
	if err != nil {
		if registered != nil && c.inflight[packetID] == registered {
			c.deleteInflightLocked(packetID)
		}
		return err
	}
	if registered != nil && (c.retryAt.IsZero() || registered.due.Before(c.retryAt)) {
		// This exchange is due before the deadline the retransmission loop is
		// sleeping for (or there is no deadline at all), so the loop has to be
		// woken. A delivery whose due time is later than the armed one does not
		// wake it: the loop is already scheduled to look at the table earlier.
		c.wakeRetransmitter()
	}
	return nil
}

// sendPublish is the outbound delivery path of a live session. A QoS 1/2 delivery
// that would exceed the connection's inflight window is queued for the connection
// and sent, in arrival order, as soon as an acknowledgement frees a slot: the
// publisher is never blocked and the message is never lost.
//
// A pending queue that is full is the one place where the policy differs by kind
// of session, because only one of the two has somewhere to keep the delivery:
//
//   - a persistent session is not dropped from. The delivery joins the queue, the
//     session is taken offline — the socket is closed, exactly as it is for a lost
//     connection, so the read loop unwinds and unregister stores the session —
//     and everything the connection still owed is stored with it (see
//     conn.Handoff). The next CONNECT of that client id resumes the
//     session and drains the queue. Taking the session offline is counted with
//     Metrics.RecordOverflow("inflight_overflow") and logged at Warn level with
//     the client id and the reason "inflight_overflow"; nothing is dropped, so no
//     drop counter moves.
//   - a clean session has nowhere to queue, because its state ends with the
//     connection. The delivery is dropped on that one explicit path, counted with
//     Metrics.RecordDroppedMessage("inflight_overflow_clean") — the offline queue
//     of a persistent session does not exist for it — and logged with the same
//     reason.
//
// Deliveries that arrive while the hand-off is in flight are not a third case:
// they wait in the same pending queue (handoffCollecting) or, once the session is
// stored, are queued for it through the offline path the broker already has
// (handoffStored), so a publisher that holds a target of a connection that just
// went away loses nothing and never observes an error.
func (c *Client) sendPublish(topic string, payload []byte, qos byte, retain bool, props mqtt.Properties) error {
	c.mu.Lock()
	switch c.handoff {
	case handoffCollecting:
		// The session is on its way to the store: the delivery waits in the very
		// queue unregister moves into it, so it keeps its place behind everything
		// queued before it.
		c.collectLocked(pendingMessage{topic: topic, payload: payload, qos: qos, retain: retain, props: props})
		c.mu.Unlock()
		return nil
	case handoffStored:
		// The session is stored (or now belongs to another connection): the delivery
		// is queued exactly like one that arrives while the session is offline,
		// which may find the client already reconnected and deliver it live.
		key := ns.Key(c.namespace, c.id)
		clientID := c.id
		c.mu.Unlock()
		c.host.EnqueueOffline(
			routing.OfflineTarget(clientID, key, qos, retain),
			Message{Topic: topic, Payload: payload, QoS: qos, Retain: retain, Props: props})
		return nil
	}
	// writeClosed also covers a connection that is closing, whose socket failed or
	// which is being disconnected as a slow consumer: its queue is not growing any
	// more, so a delivery is refused instead of parked in it.
	if c.closed || c.writeClosed.Load() {
		c.mu.Unlock()
		return errClientClosed
	}
	if qos == 0 || !c.windowFullLocked(qos) {
		err := c.writePublishLocked(topic, payload, qos, retain, props)
		c.mu.Unlock()
		return err
	}
	limit := c.inflightQueueLimit()
	if len(c.pending) < limit {
		c.pending = append(c.pending, pendingMessage{topic: topic, payload: payload, qos: qos, retain: retain, props: props})
		c.mu.Unlock()
		return nil
	}
	if !c.persistentSession() {
		// A clean session trades durability for statelessness: there is no queue to
		// keep the delivery in, so it is dropped — visibly, and never silently.
		c.mu.Unlock()
		c.host.Metrics().RecordDroppedMessage("inflight_overflow_clean")
		c.host.Metrics().RecordOverflow("inflight")
		c.warn("gmqx dropped a delivery at a full inflight queue of a clean session",
			"client", c.id, "connection", string(c.connID), "reason", "inflight_overflow_clean",
			"qos", qos, "topic", topic, "queue", limit)
		return errInflightFull
	}
	// A persistent session is taken offline instead of losing a delivery its
	// publisher was already acknowledged for: the message joins the queue, the
	// connection is closed the way a lost one is, and unregister stores the session
	// with the queue in it.
	c.collectLocked(pendingMessage{topic: topic, payload: payload, qos: qos, retain: retain, props: props})
	c.handoff = handoffCollecting
	c.mu.Unlock()
	c.host.Metrics().RecordOverflow("inflight_overflow")
	c.warn("gmqx taking a session offline at a full inflight queue",
		"client", c.id, "connection", string(c.connID), "reason", "inflight_overflow",
		"qos", qos, "topic", topic, "queue", limit)
	c.closeOverflowed()
	return nil
}

// collectLocked queues one more delivery in the pending queue of a session that is
// being taken offline. The collection window is short — the socket is already
// closed, so the read loop that leads to unregister is on its way — but it is
// bounded like the queue these deliveries are going to: past the per-session
// offline cap the oldest one goes, counted exactly as that queue counts its own
// drops ("offline_queue_full"). A deployment that disabled the cap still gets the
// inflight queue's own bound, because a connection that is being taken offline
// must not grow without one. The caller holds c.mu.
func (c *Client) collectLocked(m pendingMessage) {
	c.pending = append(c.pending, m)
	limit := c.collectLimit()
	if len(c.pending) > limit {
		dropped := len(c.pending) - limit
		c.pending = c.pending[dropped:]
		for i := 0; i < dropped; i++ {
			c.host.Metrics().RecordDroppedMessage("offline_queue_full")
		}
	}
}

// collectLimit is how many deliveries may wait in the pending queue while the
// session is handed over to its offline queue.
func (c *Client) collectLimit() int {
	if limit := c.host.Limits().MaxOfflineQueueSize; limit > 0 {
		return max(limit, c.inflightQueueLimit())
	}
	return c.inflightQueueLimit()
}

// closeOverflowed ends a connection whose session is being taken offline because
// its inflight queue is full. It is the teardown of a lost connection: the socket
// is closed, the read loop unwinds and the connection's serve goroutine runs the
// normal unregister, which stores the session (see Host.Unregister). Nothing is
// written first on purpose — a client that stopped reading cannot receive a
// DISCONNECT, and the publishing goroutine must not wait for one to be written; it
// is the hang-up the client reconnects from, and the session queue, not the
// connection, is where the messages are.
func (c *Client) closeOverflowed() {
	c.writeClosed.Store(true)
	_ = c.conn.Close()
}

// drainPendingLocked sends the queued deliveries whose window has room, in the
// order they were queued, and stops at the first message that still has no slot:
// the connection never reorders its QoS 1/2 deliveries, not even when the QoS 1
// and QoS 2 windows free slots at different times. The caller holds c.mu and has
// just released an inflight slot.
//
// A session that is being taken offline is not drained at all: its pending queue
// belongs to the offline queue from now on, and a drain would compete with the
// hand-off for the same messages.
//
// A delivery that cannot be queued for the writer (the connection is closing, or
// the writer queue is full and the connection is being disconnected as a slow
// consumer) goes back to the front of the queue instead of being lost, so the
// hand-off to the offline queue — or the next reconnect — still has it. A packet
// the client's Maximum Packet Size forbids is the one exception: it can never be
// delivered, it was counted when it was queued, and it is skipped so it cannot
// wedge the queue.
func (c *Client) drainPendingLocked() {
	if c.handoff != handoffNone {
		return
	}
	for len(c.pending) > 0 {
		msg := c.pending[0]
		if c.windowFullLocked(msg.qos) {
			return
		}
		c.pending[0] = pendingMessage{}
		c.pending = c.pending[1:]
		if err := c.writePublishLocked(msg.topic, msg.payload, msg.qos, msg.retain, msg.props); err != nil {
			if errors.Is(err, errPacketTooLarge) {
				continue
			}
			c.pending = append([]pendingMessage{msg}, c.pending...)
			return
		}
	}
}

// handlePuback completes an outbound QoS 1 exchange: the packet id is free again,
// no retransmission follows, and the queue of deliveries that waited for the
// window moves on.
func (c *Client) handlePuback(m *mqtt.Puback) {
	c.mu.Lock()
	if entry, ok := c.inflight[m.PacketID]; ok && entry.state == awaitPuback {
		c.deleteInflightLocked(m.PacketID)
		c.drainPendingLocked()
	}
	c.mu.Unlock()
}

// handlePubrec advances an outbound QoS 2 exchange: the PUBLISH is confirmed, so
// the broker answers with PUBREL and the retransmission timer now repeats that
// PUBREL until PUBCOMP. A PUBREC with a reason code of 0x80 or above ends the
// exchange without a PUBREL (MQTT 5).
func (c *Client) handlePubrec(m *mqtt.Pubrec) {
	c.mu.Lock()
	entry, ok := c.inflight[m.PacketID]
	if !ok || entry.state != awaitPubrec {
		c.mu.Unlock()
		return
	}
	if m.ReasonCode >= 0x80 {
		c.deleteInflightLocked(m.PacketID)
		c.drainPendingLocked()
		c.mu.Unlock()
		return
	}
	c.confirmInflightLocked(entry)
	if c.host.Limits().QoSMaxRetries >= 0 {
		entry.attempts = 0
		entry.due = time.Now().Add(c.host.Limits().QoSRetryInterval)
	} else {
		entry.due = time.Time{}
	}
	c.mu.Unlock()

	_ = c.enqueueAck(outboundPubrel, m.PacketID)
	c.wakeRetransmitter()
}

// handlePubcomp completes an outbound QoS 2 exchange and frees its packet id,
// which also lets the queue of waiting deliveries move on.
func (c *Client) handlePubcomp(m *mqtt.Pubcomp) {
	c.mu.Lock()
	if entry, ok := c.inflight[m.PacketID]; ok && entry.state == awaitPubcomp {
		c.deleteInflightLocked(m.PacketID)
		c.drainPendingLocked()
	}
	c.mu.Unlock()
}

// drainOfflineQueue sends the messages a persistent session queued while it was
// offline, in FIFO order, immediately after the CONNACK. Packet ids are assigned
// here, at drain time, and not when the message was queued.
//
// The queue is a stored obligation, so it is always drained: a QoS 1/2 message
// that cannot get a slot in the inflight window or a free packet id is delivered
// at QoS 0 instead of stalling the session or being reordered. That is the same
// degradation the reference broker applies when its packet ids run out. If a write
// fails the message goes back to the head of the queue, and the connection's
// unregister stores the queue again for the next CONNECT.
func (c *Client) drainOfflineQueue() {
	for {
		c.mu.Lock()
		if c.closed || len(c.queue) == 0 {
			c.mu.Unlock()
			return
		}
		msg := c.queue[0]
		c.queue = c.queue[1:]
		c.mu.Unlock()

		qos := msg.QoS
		if qos > 0 {
			c.mu.Lock()
			full := c.windowFullLocked(qos)
			c.mu.Unlock()
			if full {
				qos = 0
				c.host.Metrics().RecordOverflow("inflight")
			}
		}
		if err := c.writePublish(msg.Topic, msg.Payload, qos, msg.Retain, msg.Props); err != nil {
			if errors.Is(err, errPacketTooLarge) {
				// The client's Maximum Packet Size forbids this stored message. It was
				// counted by enqueue and is skipped, so one oversize message cannot wedge
				// the session's queue forever.
				continue
			}
			c.mu.Lock()
			c.queue = append([]session.Pending{msg}, c.queue...)
			c.mu.Unlock()
			return
		}
	}
}
