package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// connState is one live MQTT connection. A Client owns at most one of them, and
// Close and the supervisor own its end.
type connState struct {
	nc  net.Conn
	r   *bufio.Reader
	dec mqtt.Decoder
	// outq is the bounded writer FIFO; every outgoing packet of a caller method
	// enters it and is written by this connection's single writer goroutine.
	outq chan mqtt.Message
	// keepAlive is the effective keepalive of this connection: Config.KeepAlive,
	// or the server's Server Keep Alive override. Zero disables the PINGREQ and
	// the watchdog.
	keepAlive time.Duration
	// stop is closed when the connection ends. It is what wakes the writer, the
	// reader's queue sends and the delivery enqueue.
	stop       chan struct{}
	stopOnce   sync.Once
	writerDone chan struct{}
	readerDone chan struct{}
	// wmu serializes socket writes: the writer goroutine and the terminal
	// DISCONNECT Close writes synchronously.
	wmu sync.Mutex
	// pingAck is signalled by the reader on PINGRESP; the writer consumes it on
	// its keepalive tick. pingSent and pingOut are owned by the writer.
	pingAck  chan struct{}
	pingSent time.Time
	pingOut  bool
	// errMu guards the first failure of the connection, which is the error the
	// supervisor reports instead of the secondary "use of closed connection" the
	// reader sees after the socket is closed.
	errMu sync.Mutex
	err   error
}

// shutdown closes the connection's stop channel and its socket. It is safe to
// call more than once and from any goroutine.
func (cs *connState) shutdown() {
	cs.stopOnce.Do(func() { close(cs.stop) })
	_ = cs.nc.Close()
}

// fail records the first failure of the connection and shuts it down.
func (cs *connState) fail(err error) {
	if err == nil {
		err = ErrDisconnected
	}
	cs.errMu.Lock()
	if cs.err == nil {
		cs.err = err
	}
	cs.errMu.Unlock()
	cs.shutdown()
}

// lastError is the first recorded failure, or the fallback error.
func (cs *connState) lastError() error {
	cs.errMu.Lock()
	defer cs.errMu.Unlock()
	if cs.err != nil {
		return cs.err
	}
	return ErrDisconnected
}

// dialPeer opens the transport, applying Config.TLS when configured.
func (c *Client) dialPeer(ctx context.Context) (net.Conn, error) {
	raw, err := c.dial(ctx, "tcp", c.cfg.Addr)
	if err != nil {
		return nil, err
	}
	if c.cfg.TLS == nil {
		return raw, nil
	}
	conn := tls.Client(raw, c.cfg.TLS)
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return conn, nil
}

// inboundLimit is the decoder bound of inbound packet bodies: the Maximum Packet
// Size this client advertised, or a memory guard when it advertised none.
func (c *Client) inboundLimit() int {
	if c.cfg.MaximumPacketSize == 0 {
		return defaultMaxInboundPacket
	}
	if uint64(c.cfg.MaximumPacketSize) > uint64(mqtt.MaxRemainingLength) {
		return mqtt.MaxRemainingLength
	}
	return int(c.cfg.MaximumPacketSize)
}

// currentClientID is the identifier the next CONNECT carries: Config.ClientID or
// the identifier a server assigned to the first empty CONNECT.
func (c *Client) currentClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientID
}

// connectPacket builds the CONNECT of one attempt. The MQTT 5 properties are the
// ones a client legitimately sets: session expiry, its receive maximum and its
// maximum packet size.
func (c *Client) connectPacket() *mqtt.Connect {
	seconds := uint16(0)
	if c.cfg.KeepAlive > 0 {
		s := int64((c.cfg.KeepAlive + time.Second - 1) / time.Second)
		if s > 65535 {
			s = 65535
		}
		seconds = uint16(s)
	}
	m := &mqtt.Connect{
		MessageHeader: c.header(),
		ClientID:      []byte(c.currentClientID()),
		KeepAlive:     seconds,
		CleanSession:  c.cfg.CleanStart,
	}
	if c.cfg.Username != "" {
		m.Username = []byte(c.cfg.Username)
		// The codec refuses a password without a user name, which validate
		// already rejected.
		m.Password = c.cfg.Password
	}
	if c.cfg.Will != nil {
		m.Will = &mqtt.Will{
			Topic:      c.cfg.Will.Topic,
			Payload:    c.cfg.Will.Payload,
			QoS:        c.cfg.Will.QoS,
			Retain:     c.cfg.Will.Retain,
			Properties: c.cfg.Will.Properties,
		}
	}
	if c.cfg.Version == mqtt.Version5 {
		props := mqtt.Properties{}
		if c.cfg.SessionExpiry > 0 {
			props.SessionExpiryInterval = c.cfg.SessionExpiry
		}
		if c.cfg.ReceiveMaximum > 0 {
			props.ReceiveMaximum = c.cfg.ReceiveMaximum
		}
		if c.cfg.MaximumPacketSize > 0 {
			props.MaximumPacketSize = c.cfg.MaximumPacketSize
		}
		m.Properties = props
	}
	return m
}

// establish opens one connection: dial, CONNECT, CONNACK, then the reader and
// writer goroutines, the resumed state and the OnConnect report. It is the only
// place a connection is created, for Dial and for every reconnect attempt.
func (c *Client) establish(ctx context.Context) (*connState, bool, error) {
	dialCtx := ctx
	if c.cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, c.cfg.ConnectTimeout)
		defer cancel()
	}
	raw, err := c.dialPeer(dialCtx)
	if err != nil {
		return nil, false, fmt.Errorf("gmqx/client: dial %s: %w", c.cfg.Addr, err)
	}
	opened := false
	defer func() {
		if !opened {
			_ = raw.Close()
		}
	}()

	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return nil, false, ErrClosed
	}
	c.handshakeConn = raw
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.handshakeConn == raw {
			c.handshakeConn = nil
		}
		c.mu.Unlock()
	}()

	deadline := time.Now().Add(c.cfg.ConnectTimeout)
	if d, ok := dialCtx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = raw.SetDeadline(deadline)

	if err := mqtt.Encode(raw, c.connectPacket()); err != nil {
		return nil, false, fmt.Errorf("gmqx/client: write CONNECT: %w", err)
	}
	// The bufio.Reader may already hold bytes that follow the CONNACK, so it is
	// kept for the read loop instead of being recreated after the handshake.
	br := bufio.NewReaderSize(raw, 4096)
	dec := mqtt.Decoder{Version: c.cfg.Version, MaxPacketSize: c.inboundLimit()}
	msg, err := dec.Decode(br)
	if err != nil {
		return nil, false, fmt.Errorf("gmqx/client: read CONNACK: %w", err)
	}
	ack, ok := msg.(*mqtt.Connack)
	if !ok {
		return nil, false, fmt.Errorf("%w: first server packet is %s, want CONNACK", ErrProtocol, packetTypeName(msg.Type()))
	}
	if ack.ReasonCode != 0 {
		return nil, false, &ReasonError{Packet: "CONNACK", Code: ack.ReasonCode, Reason: ack.Properties.ReasonString}
	}
	info := connackInfo(ack)
	keepAlive := c.cfg.KeepAlive
	if ack.Properties.ServerKeepAlive != nil {
		keepAlive = time.Duration(*ack.Properties.ServerKeepAlive) * time.Second
	}
	clientID := c.currentClientID()
	if info.AssignedClientID != "" {
		clientID = info.AssignedClientID
	}
	_ = raw.SetDeadline(time.Time{})

	cs := &connState{
		nc:         raw,
		r:          br,
		dec:        dec,
		outq:       make(chan mqtt.Message, c.cfg.OutboundQueueSize),
		keepAlive:  keepAlive,
		stop:       make(chan struct{}),
		writerDone: make(chan struct{}),
		readerDone: make(chan struct{}),
		pingAck:    make(chan struct{}, 1),
	}
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return nil, false, ErrClosed
	}
	c.conn = cs
	c.server = info
	c.clientID = clientID
	c.mu.Unlock()
	opened = true

	go c.writeLoop(cs)
	go c.readLoop(cs)

	// Restore the session before the client is reported connected, so no new
	// publish can overtake the retransmissions of an unconfirmed exchange.
	c.restore(cs, info.SessionPresent)

	c.mu.Lock()
	if c.closed.Load() {
		if c.conn == cs {
			c.conn = nil
		}
		c.mu.Unlock()
		// Close raced the handshake: it has already shut the socket down. Join the
		// goroutines here so the connection leaves nothing behind even though the
		// supervisor never sees it.
		cs.shutdown()
		<-cs.writerDone
		<-cs.readerDone
		return nil, false, ErrClosed
	}
	c.connected = true
	c.sessionPresent = info.SessionPresent
	c.mu.Unlock()
	if !c.closed.Load() {
		c.notifyConnect(info.SessionPresent)
	}
	return cs, info.SessionPresent, nil
}

// connackInfo maps a CONNACK onto the public snapshot, applying the protocol
// defaults of the absent capability properties.
func connackInfo(ack *mqtt.Connack) ServerInfo {
	info := ServerInfo{
		SessionPresent:                  ack.SessionPresent,
		ReceiveMaximum:                  maxReceiveMaximum,
		MaximumQoS:                      2,
		RetainAvailable:                 true,
		WildcardSubscriptionAvailable:   true,
		SubscriptionIdentifierAvailable: true,
		SharedSubscriptionAvailable:     true,
	}
	p := ack.Properties
	info.AssignedClientID = p.AssignedClientID
	if p.ReceiveMaximum > 0 {
		info.ReceiveMaximum = p.ReceiveMaximum
	}
	info.MaximumPacketSize = p.MaximumPacketSize
	if p.ServerKeepAlive != nil {
		info.ServerKeepAlive = time.Duration(*p.ServerKeepAlive) * time.Second
	}
	if p.MaximumQoS != nil {
		info.MaximumQoS = *p.MaximumQoS
	}
	if p.RetainAvailable != nil {
		info.RetainAvailable = *p.RetainAvailable
	}
	if p.WildcardSubscriptionAvailable != nil {
		info.WildcardSubscriptionAvailable = *p.WildcardSubscriptionAvailable
	}
	if p.SubscriptionIdentifierAvailable != nil {
		info.SubscriptionIdentifierAvailable = *p.SubscriptionIdentifierAvailable
	}
	if p.SharedSubscriptionAvailable != nil {
		info.SharedSubscriptionAvailable = *p.SharedSubscriptionAvailable
	}
	info.TopicAliasMaximum = p.TopicAliasMaximum
	info.ResponseInfo = p.ResponseInfo
	info.ServerReference = p.ServerReference
	return info
}

// postConnError hands the failure of the current connection to the supervisor. It
// never blocks: the channel has one slot and only one reader goroutine posts.
func (c *Client) postConnError(err error) {
	select {
	case c.connErr <- err:
	case <-c.closedCh:
	}
}

// readLoop decodes server packets until the connection ends. It is the single
// reader goroutine of the connection.
func (c *Client) readLoop(cs *connState) {
	defer close(cs.readerDone)
	for {
		msg, err := cs.dec.Decode(cs.r)
		if err != nil {
			cs.fail(err)
			c.postConnError(cs.lastError())
			return
		}
		if err := c.handlePacket(cs, msg); err != nil {
			cs.fail(err)
			c.postConnError(cs.lastError())
			return
		}
	}
}

// handlePacket dispatches one decoded server packet. An error ends the
// connection.
func (c *Client) handlePacket(cs *connState, msg mqtt.Message) error {
	switch m := msg.(type) {
	case *mqtt.Publish:
		return c.handlePublish(cs, m)
	case *mqtt.Puback:
		c.handlePuback(m)
		return nil
	case *mqtt.Pubrec:
		return c.handlePubrec(cs, m)
	case *mqtt.Pubrel:
		return c.handlePubrel(cs, m)
	case *mqtt.Pubcomp:
		c.handlePubcomp(m)
		return nil
	case *mqtt.Suback:
		c.handleSuback(m)
		return nil
	case *mqtt.Unsuback:
		c.handleUnsuback(m)
		return nil
	case *mqtt.Pingresp:
		select {
		case cs.pingAck <- struct{}{}:
		default:
		}
		return nil
	case *mqtt.Disconnect:
		reason := &ReasonError{Packet: "DISCONNECT", Code: m.ReasonCode, Reason: m.Properties.ReasonString}
		if c.cfg.Version == mqtt.Version311 {
			return &serverDisconnectError{}
		}
		return &serverDisconnectError{Reason: reason}
	case *mqtt.Auth:
		// This client does not offer enhanced authentication, so an AUTH on a
		// live connection is a protocol error.
		return fmt.Errorf("%w: server sent AUTH without a negotiated method", ErrProtocol)
	default:
		// PINGREQ, CONNECT, SUBSCRIBE and the other client-only packets cannot
		// legally arrive from a server; ignoring them keeps the connection
		// tolerant while staying within the protocol.
		return nil
	}
}

// writeLoop is the single writer goroutine of one connection. It drains the
// outbound FIFO in order and owns the keepalive and the retransmission timer, so
// one connection costs one timer and one goroutine, never one per message.
func (c *Client) writeLoop(cs *connState) {
	defer close(cs.writerDone)

	var pingC <-chan time.Time
	if cs.keepAlive > 0 {
		ticker := time.NewTicker(cs.keepAlive)
		defer ticker.Stop()
		pingC = ticker.C
	}

	retry := time.NewTimer(time.Hour)
	if !retry.Stop() {
		<-retry.C
	}
	defer retry.Stop()
	armRetry := func() {
		if !retry.Stop() {
			select {
			case <-retry.C:
			default:
			}
		}
		if d, ok := c.nextRetryIn(); ok {
			retry.Reset(d)
		}
	}
	armRetry()

	for {
		select {
		case <-cs.stop:
			return
		case <-c.closedCh:
			return
		case pkt := <-cs.outq:
			if err := c.writeOne(cs, pkt); err != nil {
				cs.fail(err)
				return
			}
		case <-c.retryWake:
			armRetry()
		case <-pingC:
			if err := c.keepaliveTick(cs); err != nil {
				cs.fail(err)
				return
			}
			armRetry()
		case <-retry.C:
			if err := c.retransmit(cs); err != nil {
				cs.fail(err)
				return
			}
			armRetry()
		}
	}
}

// keepaliveTick sends one PINGREQ and enforces the PINGRESP deadline. A PINGRESP
// that has not arrived within one keepalive interval after the PINGREQ marks the
// connection dead: the server may be gone or the path may have black-holed, and
// either way a reconnect is what recovers.
func (c *Client) keepaliveTick(cs *connState) error {
	for {
		select {
		case <-cs.pingAck:
			cs.pingOut = false
			continue
		default:
		}
		break
	}
	if cs.pingOut && time.Since(cs.pingSent) >= cs.keepAlive {
		return errKeepAliveTimeout
	}
	if err := c.writeOne(cs, &mqtt.Pingreq{MessageHeader: c.header()}); err != nil {
		return err
	}
	cs.pingOut = true
	cs.pingSent = time.Now()
	return nil
}

// writeOne encodes one packet onto the socket under the write deadline. The
// caller must not hold c.mu.
func (c *Client) writeOne(cs *connState, m mqtt.Message) error {
	cs.wmu.Lock()
	defer cs.wmu.Unlock()
	if c.cfg.WriteTimeout > 0 {
		_ = cs.nc.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	}
	return mqtt.Encode(cs.nc, m)
}

// sendDisconnect writes the terminal DISCONNECT of a client-initiated close. It
// is best effort: a socket that is already gone simply fails the write.
func (c *Client) sendDisconnect(cs *connState) {
	_ = c.writeOne(cs, &mqtt.Disconnect{MessageHeader: c.header()})
}

// packetTypeName names a packet type for an error message.
func packetTypeName(t byte) string {
	switch t {
	case mqtt.TypeConnect:
		return "CONNECT"
	case mqtt.TypeConnack:
		return "CONNACK"
	case mqtt.TypePublish:
		return "PUBLISH"
	case mqtt.TypePuback:
		return "PUBACK"
	case mqtt.TypePubrec:
		return "PUBREC"
	case mqtt.TypePubrel:
		return "PUBREL"
	case mqtt.TypePubcomp:
		return "PUBCOMP"
	case mqtt.TypeSubscribe:
		return "SUBSCRIBE"
	case mqtt.TypeSuback:
		return "SUBACK"
	case mqtt.TypeUnsubscribe:
		return "UNSUBSCRIBE"
	case mqtt.TypeUnsuback:
		return "UNSUBACK"
	case mqtt.TypePingreq:
		return "PINGREQ"
	case mqtt.TypePingresp:
		return "PINGRESP"
	case mqtt.TypeDisconnect:
		return "DISCONNECT"
	case mqtt.TypeAuth:
		return "AUTH"
	default:
		return "packet"
	}
}
