package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// outgoing is one unconfirmed outbound QoS 1/2 exchange. It keeps everything a
// retransmission needs — the original PUBLISH (with its original packet id),
// the phase of the QoS 2 handshake and the schedule — so the retransmitter never
// depends on state that may have changed.
type outgoing struct {
	// msg is the PUBLISH as it was first sent. A retransmission is a copy with
	// Dup set, which keeps the packet id and every property identical.
	msg mqtt.Publish
	// pubrel reports that the PUBLISH was confirmed with PUBREC and the client
	// now retransmits PUBREL until PUBCOMP.
	pubrel bool
	// attempts counts the retransmissions already sent and next is when the next
	// one is due; a zero next means "not scheduled" (retransmission disabled).
	attempts int
	next     time.Time
	// done receives the result of the exchange exactly once: nil for PUBACK or
	// PUBCOMP, an error for a negative acknowledgement, a retransmission
	// exhaustion or a terminal connection loss.
	done chan error
}

// Publish publishes one application message at the given QoS. QoS 0 returns once
// the packet was handed to the writer; QoS 1 returns after PUBACK and QoS 2 after
// PUBCOMP, or with an error when the exchange cannot complete (see
// PublishMessage).
func (c *Client) Publish(ctx context.Context, topic string, payload []byte, qos byte, retain bool) error {
	return c.PublishMessage(ctx, PublishOptions{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
		Retain:  retain,
	})
}

// PublishMessage publishes one application message.
//
// QoS 0 returns once the packet is queued for the writer: the protocol allows a
// QoS 0 message to be lost, and the bounded queue is the only wait. QoS 1 blocks
// until the server's PUBACK and QoS 2 until PUBCOMP, which is what makes the
// inflight window a real window: with MaxInflight 1 a second QoS 1 publish waits
// for the first acknowledgement. The call is bounded by ctx, by Close and by
// Config.MaxRetries; cancelling ctx after the packet was sent abandons the wait
// (QoS 1/2 are at-least-once, so the message may still be delivered once).
//
// A server Maximum Packet Size smaller than the encoded packet yields
// ErrPacketTooLarge and the packet is not sent. A negative MQTT 5 acknowledgement
// yields a *ReasonError, and an exchange that used its retransmissions yields
// ErrRetryLimit.
func (c *Client) PublishMessage(ctx context.Context, opts PublishOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.QoS > 2 {
		return fmt.Errorf("%w: %d", ErrBadQoS, opts.QoS)
	}
	if !validTopicName(opts.Topic) {
		return fmt.Errorf("%w: %q", ErrBadTopic, opts.Topic)
	}
	pub := mqtt.Publish{
		MessageHeader: c.header(),
		Topic:         opts.Topic,
		Payload:       opts.Payload,
		QoS:           opts.QoS,
		Retain:        opts.Retain,
		Properties:    opts.Properties,
	}
	// Topic aliases are hop-local and this client does not create outbound ones;
	// a caller-provided alias would refer to a mapping the server does not have.
	// Subscription identifiers are server-to-client properties that a client
	// PUBLISH must not carry, so both are cleared instead of being sent.
	pub.Properties.TopicAlias = 0
	pub.Properties.SubscriptionIdentifiers = nil
	if err := c.checkPacketSize(&pub); err != nil {
		return err
	}
	if opts.QoS == 0 {
		return c.send(ctx, &pub)
	}

	// The window slot is taken before the packet id is assigned and the packet is
	// queued, so a full window blocks the caller instead of buffering an
	// unbounded number of publishes in the client.
	if err := c.acquireSlot(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	if c.closed.Load() {
		c.releaseSlotLocked()
		c.mu.Unlock()
		return ErrClosed
	}
	cs, connected := c.conn, c.connected
	if cs == nil || !connected {
		c.releaseSlotLocked()
		c.mu.Unlock()
		return ErrNotConnected
	}
	id, err := c.allocPIDLocked()
	if err != nil {
		c.releaseSlotLocked()
		c.mu.Unlock()
		return err
	}
	pub.PacketID = id
	entry := &outgoing{
		msg:  pub,
		done: make(chan error, 1),
	}
	if c.cfg.MaxRetries >= 0 {
		entry.next = time.Now().Add(c.cfg.RetryInterval)
	}
	c.inflight[id] = entry
	c.order = append(c.order, id)
	c.mu.Unlock()
	c.wakeRetry()

	// The packet id adds two bytes to the encoded packet; measure again so a
	// packet exactly at the server limit is judged by its real size.
	if err := c.checkPacketSize(&pub); err != nil {
		c.abortPublish(id)
		return err
	}
	if err := c.sendOn(ctx, cs, &pub); err != nil {
		c.abortPublish(id)
		return err
	}

	select {
	case err := <-entry.done:
		return err
	case <-ctx.Done():
		c.abortPublish(id)
		return ctx.Err()
	case <-c.closedCh:
		return ErrClosed
	}
}

// acquireSlot reserves one slot of the outbound inflight window. It waits — it
// never drops a publish — until a confirmation frees a slot, the caller's context
// ends or the client closes. The wait also ends with ErrNotConnected when the
// connection is lost: a publish that has not been sent yet is not replayed by a
// reconnect.
func (c *Client) acquireSlot(ctx context.Context) error {
	for {
		c.mu.Lock()
		if c.closed.Load() {
			c.mu.Unlock()
			return ErrClosed
		}
		if !c.connected {
			c.mu.Unlock()
			return ErrNotConnected
		}
		if c.inflightN < c.windowLocked() {
			c.inflightN++
			c.mu.Unlock()
			return nil
		}
		wake := c.slotWake
		c.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closedCh:
			return ErrClosed
		}
	}
}

// windowLocked is the effective outbound QoS 1/2 window: Config.MaxInflight,
// lowered by the Receive Maximum the server advertised in its CONNACK. The caller
// holds c.mu.
func (c *Client) windowLocked() int {
	window := c.cfg.MaxInflight
	if window <= 0 {
		window = maxReceiveMaximum
	}
	if c.server.ReceiveMaximum > 0 && int(c.server.ReceiveMaximum) < window {
		window = int(c.server.ReceiveMaximum)
	}
	if window < 1 {
		window = 1
	}
	return window
}

// releaseSlotLocked frees one window slot and wakes every waiter. The caller
// holds c.mu.
func (c *Client) releaseSlotLocked() {
	if c.inflightN > 0 {
		c.inflightN--
	}
	c.broadcastSlotsLocked()
}

// broadcastSlotsLocked wakes every acquireSlot waiter by replacing the channel
// they are sleeping on. The caller holds c.mu.
func (c *Client) broadcastSlotsLocked() {
	close(c.slotWake)
	c.slotWake = make(chan struct{})
}

// allocPIDLocked returns a packet id no unconfirmed PUBLISH and no pending
// SUBSCRIBE/UNSUBSCRIBE of this client holds. The caller holds c.mu.
func (c *Client) allocPIDLocked() (uint16, error) {
	for i := 0; i < 65535; i++ {
		c.nextPID++
		if c.nextPID == 0 {
			c.nextPID = 1
		}
		if _, used := c.inflight[c.nextPID]; used {
			continue
		}
		if _, used := c.requests[c.nextPID]; used {
			continue
		}
		return c.nextPID, nil
	}
	return 0, ErrNoPacketID
}

// removeOrderLocked drops id from the resend order list. The caller holds c.mu.
func (c *Client) removeOrderLocked(id uint16) {
	for i, other := range c.order {
		if other == id {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

// completeLocked removes an exchange, frees its window slot and reports whether
// it was still present. The caller holds c.mu.
func (c *Client) completeLocked(id uint16, e *outgoing) bool {
	current, ok := c.inflight[id]
	if !ok || current != e {
		return false
	}
	delete(c.inflight, id)
	c.removeOrderLocked(id)
	c.releaseSlotLocked()
	return true
}

// abortPublish removes an exchange the caller is abandoning (its context ended or
// its packet could not be queued) if it is still unconfirmed.
func (c *Client) abortPublish(id uint16) {
	c.mu.Lock()
	if e, ok := c.inflight[id]; ok {
		c.completeLocked(id, e)
	}
	c.mu.Unlock()
	c.wakeRetry()
}

// handlePuback completes an outbound QoS 1 exchange. A PUBACK for a QoS 2
// exchange is ignored: the server cannot legitimately send one.
func (c *Client) handlePuback(m *mqtt.Puback) {
	c.mu.Lock()
	e, ok := c.inflight[m.PacketID]
	if !ok || e.pubrel {
		c.mu.Unlock()
		return
	}
	c.completeLocked(m.PacketID, e)
	c.mu.Unlock()
	var err error
	if m.ReasonCode >= 0x80 && c.cfg.Version == mqtt.Version5 {
		err = &ReasonError{Packet: "PUBACK", Code: m.ReasonCode, Reason: m.Properties.ReasonString}
	}
	e.done <- err
	c.wakeRetry()
}

// handlePubrec advances an outbound QoS 2 exchange: the PUBLISH is confirmed, so
// the client answers PUBREL and from now on retransmits that PUBREL until PUBCOMP.
// A duplicate PUBREC (the server retransmitted it before it saw the PUBREL) is
// answered with the PUBREL again and does not change the exchange.
func (c *Client) handlePubrec(cs *connState, m *mqtt.Pubrec) error {
	c.mu.Lock()
	e, ok := c.inflight[m.PacketID]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	if e.pubrel {
		c.mu.Unlock()
		return c.sendControl(cs, &mqtt.Pubrel{MessageHeader: c.header(), PacketID: m.PacketID})
	}
	if m.ReasonCode >= 0x80 && c.cfg.Version == mqtt.Version5 {
		// A negative PUBREC ends the exchange: no PUBREL may be sent (MQTT 5
		// §3.5.2.2), so the publish fails with the server's reason.
		c.completeLocked(m.PacketID, e)
		c.mu.Unlock()
		e.done <- &ReasonError{Packet: "PUBREC", Code: m.ReasonCode, Reason: m.Properties.ReasonString}
		c.wakeRetry()
		return nil
	}
	e.pubrel = true
	e.attempts = 0
	if c.cfg.MaxRetries >= 0 {
		e.next = time.Now().Add(c.cfg.RetryInterval)
	} else {
		e.next = time.Time{}
	}
	c.mu.Unlock()
	c.wakeRetry()
	return c.sendControl(cs, &mqtt.Pubrel{MessageHeader: c.header(), PacketID: m.PacketID})
}

// handlePubrel completes an inbound QoS 2 exchange: the packet id is free again
// and PUBCOMP answers the release. The message itself was delivered when the
// PUBLISH first arrived, which is what makes a retransmitted PUBLISH (same packet
// id, DUP set) a duplicate that is acknowledged but not delivered twice.
func (c *Client) handlePubrel(cs *connState, m *mqtt.Pubrel) error {
	c.mu.Lock()
	delete(c.inboundQoS2, m.PacketID)
	c.mu.Unlock()
	return c.sendControl(cs, &mqtt.Pubcomp{MessageHeader: c.header(), PacketID: m.PacketID})
}

// handlePubcomp completes an outbound QoS 2 exchange.
func (c *Client) handlePubcomp(m *mqtt.Pubcomp) {
	c.mu.Lock()
	e, ok := c.inflight[m.PacketID]
	if !ok || !e.pubrel {
		c.mu.Unlock()
		return
	}
	c.completeLocked(m.PacketID, e)
	c.mu.Unlock()
	e.done <- nil
	c.wakeRetry()
}

// handlePublish processes one inbound PUBLISH: it resolves a topic alias,
// suppresses a QoS 2 duplicate, delivers the message through OnMessage and sends
// the acknowledgement the QoS requires.
func (c *Client) handlePublish(cs *connState, m *mqtt.Publish) error {
	if m.QoS > 2 {
		return fmt.Errorf("%w: PUBLISH QoS %d", ErrProtocol, m.QoS)
	}
	topic := m.Topic
	props := m.Properties
	if alias := props.TopicAlias; alias != 0 || topic == "" {
		c.mu.Lock()
		if topic == "" {
			resolved, ok := c.aliases[alias]
			if !ok {
				c.mu.Unlock()
				return fmt.Errorf("%w: PUBLISH uses unresolved topic alias %d", ErrProtocol, alias)
			}
			topic = resolved
		} else {
			c.aliases[alias] = topic
		}
		c.mu.Unlock()
	}
	props.TopicAlias = 0
	if topic == "" {
		return fmt.Errorf("%w: PUBLISH with an empty topic", ErrProtocol)
	}

	if m.QoS == 2 {
		c.mu.Lock()
		_, duplicate := c.inboundQoS2[m.PacketID]
		if !duplicate {
			c.inboundQoS2[m.PacketID] = struct{}{}
		}
		c.mu.Unlock()
		if duplicate {
			// A retransmission of a PUBLISH whose PUBREC may have been lost: the
			// message was already delivered, so only the acknowledgement is repeated.
			return c.sendControl(cs, &mqtt.Pubrec{MessageHeader: c.header(), PacketID: m.PacketID})
		}
	}

	msg := &Message{
		Topic:      topic,
		Payload:    m.Payload,
		QoS:        m.QoS,
		Retain:     m.Retain,
		Duplicate:  m.Dup,
		Properties: props,
	}
	if !c.deliver(cs, msg) {
		return ErrDisconnected
	}

	switch m.QoS {
	case 1:
		return c.sendControl(cs, &mqtt.Puback{MessageHeader: c.header(), PacketID: m.PacketID})
	case 2:
		return c.sendControl(cs, &mqtt.Pubrec{MessageHeader: c.header(), PacketID: m.PacketID})
	}
	return nil
}

// nextRetryIn reports how long the writer may sleep before the next
// retransmission is due, false when nothing is scheduled.
func (c *Client) nextRetryIn() (time.Duration, bool) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	var soonest time.Time
	for _, id := range c.order {
		e := c.inflight[id]
		if e == nil || e.next.IsZero() {
			continue
		}
		if soonest.IsZero() || e.next.Before(soonest) {
			soonest = e.next
		}
	}
	if soonest.IsZero() {
		return 0, false
	}
	delay := soonest.Sub(now)
	if delay < 0 {
		delay = 0
	}
	return delay, true
}

// retransmit sends every exchange whose retransmission is due: a PUBLISH with the
// original packet id and DUP set, or a PUBREL for a QoS 2 exchange already
// confirmed by PUBREC. An exchange that used Config.MaxRetries retransmissions
// fails with ErrRetryLimit instead.
func (c *Client) retransmit(cs *connState) error {
	now := time.Now()
	var due []mqtt.Message
	var failed []*outgoing

	c.mu.Lock()
	order := append([]uint16(nil), c.order...)
	for _, id := range order {
		e := c.inflight[id]
		if e == nil || e.next.IsZero() || e.next.After(now) {
			continue
		}
		if c.cfg.MaxRetries >= 0 && e.attempts >= c.cfg.MaxRetries {
			c.completeLocked(id, e)
			failed = append(failed, e)
			continue
		}
		e.attempts++
		e.next = now.Add(retryDelay(c.cfg.RetryInterval, e.attempts))
		if e.pubrel {
			due = append(due, &mqtt.Pubrel{MessageHeader: c.header(), PacketID: id})
			continue
		}
		dup := e.msg
		dup.Dup = true
		due = append(due, &dup)
	}
	c.mu.Unlock()

	for _, e := range failed {
		e.done <- ErrRetryLimit
	}
	if len(failed) > 0 {
		c.wakeRetry()
	}
	for _, m := range due {
		if err := c.writeOne(cs, m); err != nil {
			return err
		}
	}
	return nil
}

// failInflight fails every unconfirmed exchange and frees the window. It is used
// when the client gives up: Close, a lost connection with reconnection disabled,
// and the end of a reconnect policy.
func (c *Client) failInflight(err error) {
	c.mu.Lock()
	c.failInflightLocked(err)
	c.mu.Unlock()
	c.wakeRetry()
}

// failInflightLocked is failInflight without the lock. The caller holds c.mu.
func (c *Client) failInflightLocked(err error) {
	entries := c.inflight
	c.inflight = make(map[uint16]*outgoing)
	c.order = nil
	c.inflightN = 0
	c.broadcastSlotsLocked()
	for _, e := range entries {
		e.done <- err
	}
	if len(entries) > 0 {
		c.wakeRetry()
	}
}

// validTopicName reports whether topic may be published to: non-empty and free of
// wildcards.
func validTopicName(topic string) bool {
	return topic != "" && !strings.ContainsAny(topic, "+#")
}
