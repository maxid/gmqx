package gmqx

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
// drains the queue in arrival order. Only a full pending queue drops a delivery,
// and that drop is counted twice and logged with the client id (see sendPublish).
// The pending queue belongs to the live connection, unlike the stored offline
// queue of a persistent session, and it ends with the connection.
//
// The reference broker of this family retries every unacked PUBLISH but never
// sets DUP and has no window at all; both are defects this implementation does
// not copy. What is deliberately not carried over either: an inbound QoS 2
// exchange does not survive the connection (the state is per connection, and the
// reference drops it too — see ROADMAP.md).

import (
	"errors"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

var (
	// errClientClosed reports a write to a connection that is already gone.
	errClientClosed = errors.New("gmqx: connection closed")
	// errInflightFull reports an outbound QoS 1/2 delivery that was dropped: the
	// connection's inflight window and the queue waiting behind it are both full.
	errInflightFull = errors.New("gmqx: inflight window full")
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
func (c *client) nextPacketIDLocked() (uint16, bool) {
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
func (c *client) inflightLimit(qos byte) int {
	if qos == 2 {
		return c.broker.cfg.MaxInflightQoS2
	}
	return c.broker.cfg.MaxInflightQoS1
}

// inflightQueueLimit is how many deliveries of this connection may wait for a
// free inflight slot. It is always bounded: a non-positive configuration (or a
// broker built without the Config defaulting) selects the documented default.
func (c *client) inflightQueueLimit() int {
	if limit := c.broker.cfg.MaxInflightQueueSize; limit > 0 {
		return limit
	}
	return defaultMaxInflightQueueSize
}

// windowFullLocked reports whether an outbound exchange of qos would exceed its
// window. The caller holds c.mu.
func (c *client) windowFullLocked(qos byte) bool {
	limit := c.inflightLimit(qos)
	if limit <= 0 {
		return false
	}
	count := 0
	for _, m := range c.inflight {
		if m.qos == qos {
			count++
		}
	}
	return count >= limit
}

// inboundWindowFullLocked reports whether another inbound QoS 2 exchange would
// exceed Config.MaxInflightQoS2. The caller holds c.mu.
func (c *client) inboundWindowFullLocked() bool {
	limit := c.broker.cfg.MaxInflightQoS2
	return limit > 0 && len(c.inboundQoS2) >= limit
}

// wakeRetransmitter asks the connection's retransmission timer to recompute its
// deadline. The channel is buffered and the send never blocks, so it is safe from
// any goroutine a delivery comes from.
func (c *client) wakeRetransmitter() {
	select {
	case c.retryWake <- struct{}{}:
	default:
	}
}

// startRetransmitter starts the connection's single retransmission timer. It is
// called once, from the handshake, after the CONNACK has been written.
func (c *client) startRetransmitter() {
	go c.retransmitLoop()
}

// retransmitLoop waits for the earliest retransmission deadline, sends what is
// due and re-arms itself. It returns when the connection ends: the context is
// cancelled by close, so a closed connection leaves neither a goroutine nor a
// timer behind.
func (c *client) retransmitLoop() {
	for {
		wait, ok := c.nextRetransmitDelay()
		var timer *time.Timer
		var timerC <-chan time.Time
		if ok {
			timer = time.NewTimer(wait)
			timerC = timer.C
		}
		select {
		case <-timerC:
			c.retransmitDue()
		case <-c.retryWake:
		case <-c.connCtx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// nextRetransmitDelay is how long the timer may sleep before the earliest
// scheduled retransmission of the table.
func (c *client) nextRetransmitDelay() (time.Duration, bool) {
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
		return 0, false
	}
	wait := time.Until(earliest)
	if wait < 0 {
		wait = 0
	}
	return wait, true
}

// retransmitDue sends every retransmission that is due. A PUBLISH is resent with
// the same packet id and DUP set; a PUBREL is resent as it is. The next attempt of
// each exchange is scheduled before its write, so a slow peer cannot turn one pass
// into a burst. An exchange that reaches Config.QoSMaxRetries is left unscheduled:
// its packet id stays reserved until the client confirms it or the connection
// ends, which is what keeps a late confirmation from being mistaken for the
// confirmation of a newer message.
func (c *client) retransmitDue() {
	type resend struct {
		topic    string
		payload  []byte
		props    mqtt.Properties
		packetID uint16
		qos      byte
		state    inflightState
		retain   bool
	}
	now := time.Now()
	maxRetries := c.broker.cfg.QoSMaxRetries
	var batch []resend
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
		batch = append(batch, resend{
			topic:    m.topic,
			payload:  m.payload,
			props:    m.props,
			packetID: id,
			qos:      m.qos,
			state:    m.state,
			retain:   m.retain,
		})
		m.due = now.Add(retryDelay(c.broker.cfg.QoSRetryInterval, m.attempts))
	}
	c.mu.Unlock()

	for i := 0; i < exhausted; i++ {
		c.broker.metrics.RecordDroppedMessage("retry_exhausted")
	}
	for _, r := range batch {
		if r.state == awaitPubcomp {
			_ = c.send(&mqtt.Pubrel{MessageHeader: c.header(), PacketID: r.packetID})
			continue
		}
		_ = c.sendPublishFrame(&mqtt.Publish{
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
}

// writePublish sends one PUBLISH to this connection. For QoS 1 and 2 it registers
// the exchange (which the retransmission timer then owns) before writing, so an
// acknowledgement can never arrive for an unknown packet id. A QoS 1/2 message
// that finds no free packet id is downgraded to QoS 0 and delivered rather than
// dropped, which is also what the reference broker does when its ids run out.
func (c *client) writePublish(topic string, payload []byte, qos byte, retain bool, props mqtt.Properties) error {
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
func (c *client) writePublishLocked(topic string, payload []byte, qos byte, retain bool, props mqtt.Properties) error {
	packetID := uint16(0)
	var registered *inflightMessage
	if qos > 0 {
		id, ok := c.nextPacketIDLocked()
		if !ok {
			qos = 0
			c.broker.metrics.RecordOverflow("packet_ids")
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
			if c.broker.cfg.QoSMaxRetries >= 0 {
				registered.due = time.Now().Add(c.broker.cfg.QoSRetryInterval)
			}
			c.inflight[id] = registered
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
			delete(c.inflight, packetID)
		}
		return err
	}
	if registered != nil {
		c.wakeRetransmitter()
	}
	return nil
}

// sendPublish is the outbound delivery path of a live session. A QoS 1/2 delivery
// that would exceed the connection's inflight window is queued for the connection
// and sent, in arrival order, as soon as an acknowledgement frees a slot: the
// publisher is never blocked and the message is never lost. Only a queue that is
// full drops the delivery, and that drop is counted twice and logged with the
// client id — a publisher's successful PUBACK is a promise about the sink, so a
// delivery the broker cannot keep has to be visible.
func (c *client) sendPublish(topic string, payload []byte, qos byte, retain bool, props mqtt.Properties) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// writeClosed also covers a connection that is closing, whose socket failed or
	// which is being disconnected as a slow consumer: its queue is not growing any
	// more, so a delivery is refused instead of parked in it.
	if c.closed || c.writeClosed.Load() {
		return errClientClosed
	}
	if qos > 0 && c.windowFullLocked(qos) {
		limit := c.inflightQueueLimit()
		if len(c.pending) >= limit {
			c.broker.metrics.RecordDroppedMessage("inflight_overflow")
			c.broker.metrics.RecordOverflow("inflight")
			c.warn("gmqx dropped a delivery at a full inflight queue",
				"client", c.id, "connection", string(c.connID), "reason", "inflight_overflow",
				"qos", qos, "topic", topic, "queue", limit)
			return errInflightFull
		}
		c.pending = append(c.pending, pendingMessage{topic: topic, payload: payload, qos: qos, retain: retain, props: props})
		return nil
	}
	return c.writePublishLocked(topic, payload, qos, retain, props)
}

// drainPendingLocked sends the queued deliveries whose window has room, in the
// order they were queued, and stops at the first message that still has no slot:
// the connection never reorders its QoS 1/2 deliveries, not even when the QoS 1
// and QoS 2 windows free slots at different times. The caller holds c.mu and has
// just released an inflight slot.
//
// A delivery that cannot be queued for the writer (the connection is closing, or
// the writer queue is full and the connection is being disconnected as a slow
// consumer) ends the drain: the rest stays in the queue and ends with the
// connection, exactly like the unconfirmed exchanges of the inflight table.
func (c *client) drainPendingLocked() {
	for len(c.pending) > 0 {
		msg := c.pending[0]
		if c.windowFullLocked(msg.qos) {
			return
		}
		c.pending[0] = pendingMessage{}
		c.pending = c.pending[1:]
		if err := c.writePublishLocked(msg.topic, msg.payload, msg.qos, msg.retain, msg.props); err != nil {
			return
		}
	}
}

// handlePuback completes an outbound QoS 1 exchange: the packet id is free again,
// no retransmission follows, and the queue of deliveries that waited for the
// window moves on.
func (c *client) handlePuback(m *mqtt.Puback) {
	c.mu.Lock()
	if entry, ok := c.inflight[m.PacketID]; ok && entry.state == awaitPuback {
		delete(c.inflight, m.PacketID)
		c.drainPendingLocked()
	}
	c.mu.Unlock()
}

// handlePubrec advances an outbound QoS 2 exchange: the PUBLISH is confirmed, so
// the broker answers with PUBREL and the retransmission timer now repeats that
// PUBREL until PUBCOMP. A PUBREC with a reason code of 0x80 or above ends the
// exchange without a PUBREL (MQTT 5).
func (c *client) handlePubrec(m *mqtt.Pubrec) {
	c.mu.Lock()
	entry, ok := c.inflight[m.PacketID]
	if !ok || entry.state != awaitPubrec {
		c.mu.Unlock()
		return
	}
	if m.ReasonCode >= 0x80 {
		delete(c.inflight, m.PacketID)
		c.drainPendingLocked()
		c.mu.Unlock()
		return
	}
	entry.state = awaitPubcomp
	if c.broker.cfg.QoSMaxRetries >= 0 {
		entry.attempts = 0
		entry.due = time.Now().Add(c.broker.cfg.QoSRetryInterval)
	} else {
		entry.due = time.Time{}
	}
	c.mu.Unlock()

	_ = c.send(&mqtt.Pubrel{MessageHeader: c.header(), PacketID: m.PacketID})
	c.wakeRetransmitter()
}

// handlePubcomp completes an outbound QoS 2 exchange and frees its packet id,
// which also lets the queue of waiting deliveries move on.
func (c *client) handlePubcomp(m *mqtt.Pubcomp) {
	c.mu.Lock()
	if entry, ok := c.inflight[m.PacketID]; ok && entry.state == awaitPubcomp {
		delete(c.inflight, m.PacketID)
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
func (c *client) drainOfflineQueue() {
	for {
		c.mu.Lock()
		if c.closed || len(c.queue) == 0 {
			c.mu.Unlock()
			return
		}
		msg := c.queue[0]
		c.queue = c.queue[1:]
		c.mu.Unlock()

		qos := msg.qos
		if qos > 0 {
			c.mu.Lock()
			full := c.windowFullLocked(qos)
			c.mu.Unlock()
			if full {
				qos = 0
				c.broker.metrics.RecordOverflow("inflight")
			}
		}
		if err := c.writePublish(msg.topic, msg.payload, qos, msg.retain, msg.props); err != nil {
			c.mu.Lock()
			c.queue = append([]queuedMessage{msg}, c.queue...)
			c.mu.Unlock()
			return
		}
	}
}
