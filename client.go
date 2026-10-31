package gmqx

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
	"github.com/maxid/gnety/codec/wire"
)

var (
	// errOutboundQueueFull reports a QoS 0 PUBLISH that was dropped because the
	// connection's writer queue is full. QoS 0 is the only droppable class.
	errOutboundQueueFull = errors.New("gmqx: outbound queue full")
	// errSlowConsumer reports a packet that must not be dropped and found the
	// connection's writer queue full: the connection is disconnected instead.
	errSlowConsumer = errors.New("gmqx: slow consumer disconnected")
)

// subscription stores MQTT 5 subscription options.
type subscription struct {
	qos               byte
	noLocal           bool
	retainAsPublished bool
	retainHandling    byte
}

type client struct {
	broker *Broker
	conn   net.Conn
	connID ConnectionID
	r      *bufio.Reader
	// wmu serializes every write on the socket: the writer goroutine, the
	// synchronous handshake writes and the final server DISCONNECT.
	wmu sync.Mutex

	// outq is the connection's write-behind queue. Every goroutine that wants to
	// send a packet appends it here and the writer goroutine writes it, in the
	// order it was appended, under Config.WriteTimeout. The queue is bounded by
	// Config.MaxOutboundQueueSize and is never closed, so an append that races
	// with close cannot panic on a send to a closed channel.
	outq chan outboundPacket
	// writerStop is closed once to end the writer goroutine, writerDone is closed
	// when it has returned. writerActive reports whether the writer owns outq yet:
	// before the handshake is through, enqueue writes synchronously, so the CONNACK
	// (and a CONNACK refusing the connection) reaches the wire before anything
	// else.
	writerStop    chan struct{}
	writerDone    chan struct{}
	writerActive  atomic.Bool
	writerMu      sync.Mutex
	writerStarted bool
	writerStopped bool
	// writeClosed is set when no packet may be queued any more: the connection is
	// closing, its socket failed or it is being disconnected as a slow consumer.
	// It is read without c.mu, from whichever goroutine produces a packet.
	writeClosed atomic.Bool
	// slowOnce makes the slow-consumer report happen once per connection.
	slowOnce sync.Once

	// namespace is the broker's isolation scope, fixed for the connection.
	namespace string
	// connCtx is the cancellable context of the connection. ctx is connCtx with
	// ConnInfo attached: it is what every hook of this connection receives. Both
	// are written during the handshake, before the client is registered.
	connCtx context.Context
	ctx     context.Context
	// cancel cancels the context derived from Config.ConnContext, parentCancel
	// the base context it was derived from. stopCtx cancels both, once.
	cancel       context.CancelFunc
	parentCancel context.CancelFunc
	stopOnce     sync.Once

	mu   sync.Mutex
	id   string
	info ConnectInfo
	// connectedAt is when the session of this connection was registered. It is
	// the reference point of the anti-flap window of a takeover
	// (Config.NotKickSeconds): written and read under Broker.mu, never under c.mu.
	connectedAt    time.Time
	subs           map[string]subscription
	keepAlive      time.Duration
	closed         bool
	closeErr       error
	will           *mqtt.Will
	username       string
	registered     bool
	lost           bool
	superseded     bool
	sessionPresent bool
	// kicked is set when a peer's connect-time kick ended this connection:
	// another node accepted the same client id, so the client itself is live
	// elsewhere and this node must stop claiming its presence, even when its
	// persistent session stays stored here (see Broker.unregister).
	kicked       bool
	inboundQoS2  map[uint16]struct{}
	topicAliases map[uint16]string
	outPID       uint16

	// inflight holds the outbound QoS 1/2 exchanges this connection has not
	// confirmed yet, keyed by packet id; retryWake asks retransmitLoop to
	// recompute its deadline. Both belong to the connection and die with it.
	inflight  map[uint16]*inflightMessage
	retryWake chan struct{}
	// pending is where an outbound QoS 1/2 delivery waits when the inflight window
	// of its QoS is full: arrival order, drained by the acknowledgement that frees
	// a slot, bounded by Config.MaxInflightQueueSize. It is a live-connection
	// queue, unlike the session queue below, and it ends with the connection.
	pending []pendingMessage
	// queue is what the session queued while it was offline, owned by this
	// connection between register (which takes it from the store) and unregister
	// (which stores what a failed drain left behind). Filled and read on the
	// connection's goroutine only.
	queue []queuedMessage

	// version is the protocol level of the connection: mqtt.Version311 or
	// mqtt.Version5. MQTT 3.1 clients decode and encode with the 3.1.1 layout,
	// which is byte-identical for everything except CONNECT.
	version byte
}

func newClient(b *Broker, conn net.Conn) *client {
	connCtx, cancel := context.WithCancel(b.lifecycleContext())
	queueSize := b.cfg.MaxOutboundQueueSize
	if queueSize <= 0 {
		queueSize = defaultMaxOutboundQueueSize
	}
	return &client{
		broker:       b,
		namespace:    b.namespace,
		connCtx:      connCtx,
		ctx:          connCtx,
		cancel:       cancel,
		parentCancel: cancel,
		connID:       nextConnectionID(),
		conn:         conn,
		r:            bufio.NewReaderSize(conn, 4096),
		subs:         make(map[string]subscription),
		inboundQoS2:  make(map[uint16]struct{}),
		topicAliases: make(map[uint16]string),
		inflight:     make(map[uint16]*inflightMessage),
		retryWake:    make(chan struct{}, 1),
		outq:         make(chan outboundPacket, queueSize),
		writerStop:   make(chan struct{}),
		writerDone:   make(chan struct{}),
		version:      mqtt.Version311,
	}
}

// context returns the connection-scoped context every hook of this connection
// receives. It carries ConnInfo and is cancelled by stopContext when the
// connection ends. It is never nil.
func (c *client) context() context.Context { return c.ctx }

// attachConnectContext builds the connection context from the parsed CONNECT:
// the optional Config.ConnContext hook enriches it once, and ConnInfo is attached
// so every hook can identify the connection without a thread-local request
// scope. The context it creates is cancelled by stopContext, even when the hook
// returns a context that is not derived from the one it was given.
func (c *client) attachConnectContext(info ConnectInfo) {
	ctx := c.connCtx
	if hook := c.broker.cfg.ConnContext; hook != nil {
		if enriched := hook(ctx, info); enriched != nil {
			enrichedCtx, cancel := context.WithCancel(enriched)
			c.connCtx, c.cancel = enrichedCtx, cancel
			ctx = enrichedCtx
		}
	}
	c.ctx = ContextWithConn(ctx, c.connInfo(info))
}

// setConnInfo refreshes the ConnInfo value after the broker assigned a client
// id. The connection context itself, and therefore its cancellation, is kept.
func (c *client) setConnInfo(info ConnectInfo) {
	c.ctx = ContextWithConn(c.connCtx, c.connInfo(info))
}

func (c *client) connInfo(info ConnectInfo) ConnInfo {
	return ConnInfo{
		ClientID:        info.ClientID,
		ConnectionID:    string(c.connID),
		Namespace:       c.namespace,
		RemoteAddr:      info.RemoteAddr,
		ProtocolVersion: c.version,
	}
}

// stopContext cancels the connection context. close calls it as soon as the
// connection is gone (after a will was published, before the disconnect hooks),
// and serve calls it again when its goroutine ends, so the cancellation happens
// even for a connection that never got past the handshake. It is safe to call
// more than once.
func (c *client) stopContext() {
	c.stopOnce.Do(func() {
		c.parentCancel()
		c.cancel()
	})
}

// persistentSession reports whether the session state survives the connection.
// MQTT 3.1.1 keys it off Clean Session; MQTT 5 keys it off Clean Start plus a
// non-zero Session Expiry Interval (the absent property means 0, so the session
// ends with the connection).
func (c *client) persistentSession() bool {
	if c.info.CleanSession {
		return false
	}
	if c.version == mqtt.Version5 {
		return c.info.SessionExpiry > 0
	}
	return true
}

// resumesSession reports whether a CONNECT asks to resume existing state. A
// zero Session Expiry Interval does not prevent resuming: it only means the
// session ends again when this connection closes.
func (c *client) resumesSession() bool { return !c.info.CleanSession }

// header is the protocol level every packet this client sends carries.
func (c *client) header() mqtt.MessageHeader {
	return mqtt.MessageHeader{Level: c.version}
}

// readMessage reads and decodes one control packet. The typed codec owns the
// wire format, so gmqx never branches on a fixed-header byte.
func (c *client) readMessage() (mqtt.Message, error) {
	d := mqtt.Decoder{Version: c.version, MaxPacketSize: c.broker.cfg.MaxPacketSize}
	return d.Decode(c.r)
}

// writeMessage encodes one packet and writes it to the socket on the calling
// goroutine, under the connection's write deadline. Only the handshake (CONNACK)
// and the terminal server DISCONNECT use it: both must be on the wire before the
// connection is closed, and neither may wait behind a queued delivery. Every
// other packet goes through enqueue.
func (c *client) writeMessage(m mqtt.Message) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.writeLocked(m)
}

// writeLocked writes one packet with the connection's write deadline applied.
// The caller holds wmu, which is what keeps the writer goroutine and a
// synchronous write from interleaving on the socket.
func (c *client) writeLocked(m mqtt.Message) error {
	// A transport without write-deadline support is still usable; the deadline is
	// the guard against a wedged peer, not a requirement of the protocol.
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout()))
	return mqtt.Encode(c.conn, m)
}

// writeTimeout is the write deadline of one packet: Config.WriteTimeout, or the
// documented default when the broker was built without the config defaulting
// (New applies it, a bare Broker literal may not).
func (c *client) writeTimeout() time.Duration {
	if timeout := c.broker.cfg.WriteTimeout; timeout > 0 {
		return timeout
	}
	return defaultWriteTimeout
}

// outboundPacket is one packet waiting for the writer goroutine. droppable marks
// the only packets a full queue may discard: QoS 0 PUBLISHes, which the protocol
// already allows to be lost.
//
// Trade-off of the queue model: the queue is bounded in packets and not in
// bytes, so one connection can hold at most MaxOutboundQueueSize packets (each
// of which MaxPacketSize bounds) — the memory of a slow consumer is capped
// without a byte-accounting pass on every send.
type outboundPacket struct {
	msg       mqtt.Message
	droppable bool
}

// startWriter starts the connection's single writer goroutine. It is called once,
// from the handshake, right after the CONNACK: from here on every packet of this
// connection is written by that goroutine, in the order the packets were queued,
// so no caller — the publisher's read loop, a will publish, the cluster receive
// goroutine — ever waits on this socket.
func (c *client) startWriter() {
	c.writerMu.Lock()
	if c.writerStarted || c.writerStopped {
		c.writerMu.Unlock()
		return
	}
	c.writerStarted = true
	c.writerMu.Unlock()
	c.writerActive.Store(true)
	go c.writeLoop()
}

// stopWriter ends the writer goroutine and waits for it to return, so a closed
// connection leaves no goroutine behind and Stop means every write is over. It
// is safe to call more than once and safe to call before startWriter.
func (c *client) stopWriter() {
	c.writerMu.Lock()
	if c.writerStopped {
		c.writerMu.Unlock()
		return
	}
	c.writerStopped = true
	started := c.writerStarted
	close(c.writerStop)
	c.writerMu.Unlock()
	if started {
		<-c.writerDone
	}
}

// writeLoop is the writer goroutine: it writes the queued packets one at a time,
// each under Config.WriteTimeout, and ends with the connection. A write that
// fails (a closed socket, an expired write deadline, a peer that reset the
// connection) ends the connection: close then runs on the read goroutine, which
// is what publishes the will and unregisters the session.
func (c *client) writeLoop() {
	defer close(c.writerDone)
	for {
		select {
		case <-c.writerStop:
			return
		case p := <-c.outq:
			if c.writeClosed.Load() {
				// The connection is closing (or was displaced): a server
				// DISCONNECT may already be on the wire and it has to be the last
				// packet, so the rest of the queue is not written.
				return
			}
			if err := c.writeMessage(p.msg); err != nil {
				c.writeFailed(err)
				return
			}
		}
	}
}

// writeFailed ends a connection whose socket write failed. It never calls close:
// closing the socket is enough to make the read goroutine return and run the
// connection teardown, which joins this goroutine.
func (c *client) writeFailed(err error) {
	c.writeClosed.Store(true)
	switch {
	case errors.Is(err, os.ErrDeadlineExceeded):
		c.warn("gmqx write deadline exceeded, closing connection",
			"client", c.id, "connection", string(c.connID), "remote", c.remoteAddr(),
			"timeout", c.writeTimeout())
	case c.broker.log != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe):
		c.broker.log.Debug("gmqx connection write failed",
			"client", c.id, "connection", string(c.connID), "remote", c.remoteAddr(), "err", err)
	}
	_ = c.conn.Close()
}

// remoteAddr is the peer address of the connection, "" when the transport has
// none. It is immutable for the life of the connection.
func (c *client) remoteAddr() string {
	if addr := c.conn.RemoteAddr(); addr != nil {
		return addr.String()
	}
	return ""
}

// warn logs at Warn level. A broker built without Config.Logger (only tests do
// that) has no logger, and a delivery guard must not panic there.
func (c *client) warn(msg string, args ...any) {
	if c.broker.log != nil {
		c.broker.log.Warn(msg, args...)
	}
}

// enqueue hands one packet to the writer goroutine. It never blocks on the
// socket and never blocks on a full queue:
//
//   - before the writer runs (the handshake) it writes synchronously, so the
//     CONNACK is on the wire before anything queued behind it;
//   - a full queue drops a QoS 0 PUBLISH (counted as "outbound_queue_full"),
//     because losing a QoS 0 message is what the protocol already permits;
//   - a full queue disconnects the connection for anything else — a QoS 1/2
//     PUBLISH, an acknowledgement, a PINGRESP — and counts it as
//     "slow_consumer". Dropping one of those would either lose an acknowledged
//     message or break the exchange, so the client is disconnected instead and
//     reconnects with a chance to catch up.
//
// Ordering: one FIFO carries every packet class, so a PUBLISH, a PUBACK, a
// PUBREC, a PUBREL, a SUBACK and a PINGRESP leave in exactly the order they were
// queued. An acknowledgement is never reordered behind a later PUBLISH and never
// overtakes an earlier one, which is what keeps a QoS 2 exchange in step; the
// price is that an acknowledgement waits behind at most MaxOutboundQueueSize
// queued packets, all of which the write deadline bounds in time. The only
// packets that do not travel through the queue are the CONNACK (before the writer
// exists) and a server DISCONNECT (the last packet of the connection, written
// before close).
func (c *client) enqueue(p outboundPacket) error {
	if c.writeClosed.Load() {
		return errClientClosed
	}
	if !c.writerActive.Load() {
		return c.writeMessage(p.msg)
	}
	select {
	case c.outq <- p:
		return nil
	default:
	}
	c.broker.metrics.RecordOverflow("outbound_queue")
	if p.droppable {
		c.broker.metrics.RecordDroppedMessage("outbound_queue_full")
		return errOutboundQueueFull
	}
	c.dropSlowConsumer()
	return errSlowConsumer
}

// send queues one packet that must not be dropped: an acknowledgement, a
// PINGRESP, a SUBACK, a retransmission. A full queue disconnects the connection.
func (c *client) send(m mqtt.Message) error {
	return c.enqueue(outboundPacket{msg: m})
}

// sendPublishFrame queues one PUBLISH. A QoS 0 PUBLISH is the one packet a full
// queue may discard; a QoS 1/2 one disconnects the slow consumer instead.
func (c *client) sendPublishFrame(m *mqtt.Publish) error {
	return c.enqueue(outboundPacket{msg: m, droppable: m.QoS == 0})
}

// dropSlowConsumer disconnects a connection whose writer queue is full and cannot
// take a packet that must not be lost. The report happens once per connection;
// closing the socket makes the read goroutine run the normal teardown, which
// publishes the will (the connection did not end in an orderly DISCONNECT) and
// unregisters the session.
func (c *client) dropSlowConsumer() {
	c.slowOnce.Do(func() {
		c.broker.metrics.RecordDroppedMessage("slow_consumer")
		c.warn("gmqx disconnecting slow consumer",
			"client", c.id, "connection", string(c.connID), "remote", c.remoteAddr(),
			"reason", "slow_consumer", "queue", cap(c.outq))
		c.writeClosed.Store(true)
		_ = c.conn.Close()
	})
}

// readConnect reads the opening packet of a connection, which must be CONNECT.
//
// The frame is read first because the typed codec models MQTT 3.1.1 and 5.0
// only: MQTT 3.1 (protocol name "MQIsdp", level 3) shares the CONNECT layout, so
// a 3.1 body is rewritten to its 3.1.1 spelling and decoded by the codec like
// any other packet. v31 reports that the client asked for 3.1, which only the
// client-id rule cares about.
func (c *client) readConnect() (connect *mqtt.Connect, v31 bool, err error) {
	header, body, err := mqtt.ReadPacket(c.r, c.broker.cfg.MaxPacketSize)
	if err != nil {
		return nil, false, err
	}
	if header>>4 != mqtt.TypeConnect {
		return nil, false, fmt.Errorf("%w: first packet is not CONNECT", ErrMalformedPacket)
	}
	body, v31 = normalizeV31Connect(body)
	frame := mqtt.AppendRemainingLength([]byte{header}, len(body))
	frame = append(frame, body...)
	d := mqtt.Decoder{MaxPacketSize: c.broker.cfg.MaxPacketSize}
	msg, err := d.Decode(bufio.NewReader(bytes.NewReader(frame)))
	if err != nil {
		return nil, false, connectError(err)
	}
	connect, ok := msg.(*mqtt.Connect)
	if !ok {
		return nil, false, fmt.Errorf("%w: first packet is not CONNECT", ErrMalformedPacket)
	}
	return connect, v31, nil
}

// normalizeV31Connect rewrites an MQTT 3.1 CONNECT body ("MQIsdp", level 3) to
// the 3.1.1 spelling ("MQTT", level 4). Both versions lay out the connect flags,
// the keep-alive and the payload identically, so the protocol name and the level
// are the whole difference.
func normalizeV31Connect(body []byte) ([]byte, bool) {
	const name = "MQIsdp"
	if len(body) < 2+len(name)+1 {
		return body, false
	}
	if binary.BigEndian.Uint16(body) != uint16(len(name)) || string(body[2:2+len(name)]) != name || body[2+len(name)] != 3 {
		return body, false
	}
	out := make([]byte, 0, len(body)-2)
	out = append(out, 0, 4)
	out = append(out, "MQTT"...)
	out = append(out, mqtt.Version311)
	return append(out, body[2+len(name)+1:]...), true
}

// connectError keeps "not an MQTT version this broker speaks" distinguishable
// from "malformed CONNECT", as ErrUnsupportedProtocol documents.
func connectError(err error) error {
	if errors.Is(err, wire.ErrUnsupported) {
		return fmt.Errorf("%w: %w", ErrUnsupportedProtocol, err)
	}
	return err
}

// connectInfo maps a decoded CONNECT onto the public ConnectInfo the SPI sees.
func connectInfo(m *mqtt.Connect) ConnectInfo {
	info := ConnectInfo{
		ClientID:      string(m.ClientID),
		Username:      string(m.Username),
		Password:      m.Password,
		KeepAlive:     m.KeepAlive,
		CleanSession:  m.CleanSession,
		SessionExpiry: m.Properties.SessionExpiryInterval,
	}
	if m.Will != nil {
		info.WillTopic = m.Will.Topic
		info.WillPayload = m.Will.Payload
	}
	return info
}

func (c *client) handshake() error {
	// The CONNECT phase runs under its own read deadline: a socket that is opened
	// and then says nothing must not hold a connection slot, a goroutine and a
	// session until the peer goes away. The read loop replaces this deadline with
	// the keep-alive one as soon as the handshake is through.
	_ = c.conn.SetReadDeadline(time.Now().Add(c.broker.connectTimeout()))
	connect, v31, err := c.readConnect()
	if err != nil {
		return err
	}
	c.version = connect.Version()
	info := connectInfo(connect)
	info.RemoteAddr = c.conn.RemoteAddr().String()
	c.attachConnectContext(info)
	if err := c.broker.authenticate(c.context(), info); err != nil {
		_ = c.writeConnack(connackNotAuthorized(c.version), "")
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	assigned := ""
	if info.ClientID == "" {
		if v31 || !info.CleanSession {
			_ = c.writeConnack(connackClientIDInvalid(c.version), "")
			return fmt.Errorf("%w: empty client id is not allowed", ErrMalformedPacket)
		}
		info.ClientID = randomID()
		assigned = info.ClientID
	}
	if v31 && len(info.ClientID) > 23 {
		_ = c.writeConnack(connackClientIDInvalid(c.version), "")
		return fmt.Errorf("%w: MQTT 3.1 client id exceeds 23 characters", ErrMalformedPacket)
	}
	if connect.Will != nil && !validTopic(connect.Will.Topic) {
		// A will topic is a topic name: no wildcards and not empty. MQTT 5 can
		// name the reason; MQTT 3.1.1 has no return code for it, so the
		// malformed CONNECT just closes the connection.
		if c.version == mqtt.Version5 {
			_ = c.writeConnack(connackTopicNameInvalid(c.version), "")
		}
		return fmt.Errorf("%w: invalid will topic %q", ErrMalformedPacket, connect.Will.Topic)
	}
	c.mu.Lock()
	c.id = info.ClientID
	c.info = info
	c.keepAlive = time.Duration(info.KeepAlive) * time.Second
	c.username = info.Username
	if connect.Will != nil {
		will := *connect.Will
		will.Payload = append([]byte(nil), connect.Will.Payload...)
		c.will = &will
	}
	c.mu.Unlock()
	// The client id can be assigned by the broker, so ConnInfo is refreshed with
	// the final identity before anything else observes the connection.
	c.setConnInfo(info)
	if err := c.broker.register(c); err != nil {
		code := byte(5)
		switch {
		case errors.Is(err, ErrClientIDInUse):
			code = connackClientIDInvalid(c.version)
		case errors.Is(err, ErrTakeoverRateExceeded):
			code = connackRateExceeded(c.version)
		case errors.Is(err, ErrMaxConnections):
			code = connackQuotaExceeded(c.version)
		}
		_ = c.writeConnack(code, "")
		return err
	}
	if err := c.writeConnack(0, assigned); err != nil {
		return err
	}
	// The handshake is through: the CONNECT deadline is cleared and the read loop
	// arms the deadline it owns (keep-alive, or none at all).
	_ = c.conn.SetReadDeadline(time.Time{})
	// Outbound delivery is owned by the connection from here on: one writer
	// goroutine that drains the packet queue in order, one retransmission timer
	// for QoS 1/2 confirmations, and the queue the session collected while it was
	// offline drained FIFO right behind the CONNACK. The writer starts after the
	// CONNACK, so nothing can be written ahead of it.
	c.startWriter()
	c.startRetransmitter()
	c.drainOfflineQueue()
	return nil
}

func (c *client) loop() {
	for {
		deadline := time.Time{}
		c.mu.Lock()
		if c.keepAlive > 0 {
			deadline = time.Now().Add(c.keepAlive * 3 / 2)
		}
		c.mu.Unlock()
		_ = c.conn.SetReadDeadline(deadline)

		msg, err := c.readMessage()
		if err != nil {
			c.mu.Lock()
			c.lost = true
			c.mu.Unlock()
			c.failProtocol(err)
			c.close(true)
			return
		}
		switch m := msg.(type) {
		case *mqtt.Publish:
			c.handlePublish(m)
		case *mqtt.Puback:
			c.handlePuback(m)
		case *mqtt.Pubrec:
			c.handlePubrec(m)
		case *mqtt.Pubcomp:
			c.handlePubcomp(m)
		case *mqtt.Pubrel:
			c.handlePubrel(m)
		case *mqtt.Subscribe:
			c.handleSubscribe(m)
		case *mqtt.Unsubscribe:
			c.handleUnsubscribe(m)
		case *mqtt.Pingreq:
			_ = c.send(&mqtt.Pingresp{MessageHeader: c.header()})
			if c.broker.cfg.Sink != nil {
				c.broker.cfg.Sink.OnPing(c.context(), c.id)
			}
		case *mqtt.Disconnect:
			// A DISCONNECT with reason code 0x04 asks the server to publish the
			// will even though the disconnect itself is orderly.
			withWill := c.version == mqtt.Version5 && m.ReasonCode == 0x04
			c.mu.Lock()
			if !withWill {
				c.will = nil
			}
			c.lost = false
			c.mu.Unlock()
			c.close(withWill)
			return
		case *mqtt.Auth:
			// Enhanced authentication was not negotiated: CONNECT carried no
			// Authentication Method, so an AUTH packet is a protocol error
			// (MQTT 5 3.15.2) rather than something to ignore.
			c.protocolError(0x82)
			return
		default:
			c.broker.log.Debug("gmqx ignoring packet type", "type", msg.Type())
		}
	}
}

// failProtocol tells an MQTT 5 client why the connection is being closed. An
// MQTT 3.1.1 client cannot be told: the protocol has no server DISCONNECT, so
// the connection is simply closed. Transport errors (a closed connection or an
// expired keep-alive deadline) are not protocol errors and stay silent.
func (c *client) failProtocol(err error) {
	if c.version != mqtt.Version5 {
		return
	}
	if !errors.Is(err, mqtt.ErrMalformedPacket) {
		return
	}
	reason := byte(0x81) // Malformed Packet
	if wire.KindOf(err) == wire.KindTooLarge {
		reason = 0x95 // Packet too large
	}
	c.sendDisconnect(reason)
}

// protocolError answers an MQTT 5 protocol violation with a DISCONNECT and ends
// the connection. MQTT 3.1.1 closes the connection without a packet.
func (c *client) protocolError(reason byte) {
	if c.version == mqtt.Version5 {
		c.sendDisconnect(reason)
	}
	c.close(true)
}

func (c *client) sendDisconnect(reason byte) {
	// The DISCONNECT is the last packet of the connection and close follows it
	// immediately, so it is written synchronously (under the write deadline)
	// instead of being queued behind deliveries that will never be sent.
	_ = c.writeMessage(&mqtt.Disconnect{MessageHeader: c.header(), ReasonCode: reason})
}

// handlePubrel releases an inbound QoS 2 publish: the client waits for the
// PUBCOMP before it reuses the packet id. Completing the flow forgets the packet
// id, so the next PUBLISH that reuses it is delivered as a new message.
func (c *client) handlePubrel(m *mqtt.Pubrel) {
	c.mu.Lock()
	delete(c.inboundQoS2, m.PacketID)
	c.mu.Unlock()
	_ = c.send(&mqtt.Pubcomp{MessageHeader: c.header(), PacketID: m.PacketID})
}

func (c *client) handlePublish(m *mqtt.Publish) {
	topic := m.Topic
	props := m.Properties
	// Topic Alias and Subscription Identifier are hop-local: the alias resolves
	// (and records) the topic name, and neither property is forwarded.
	alias := props.TopicAlias
	props.TopicAlias = 0
	props.SubscriptionIdentifiers = nil
	var wireProps []byte
	if c.version == mqtt.Version5 {
		if alias > topicAliasMaximum {
			// The alias is above the Topic Alias Maximum this broker advertised.
			c.protocolError(0x94)
			return
		}
		if topic == "" {
			if alias == 0 {
				c.protocolError(0x82) // empty topic and no alias to resolve
				return
			}
			resolved, ok := c.topicAliases[alias]
			if !ok {
				c.protocolError(0x82) // alias that was never created
				return
			}
			topic = resolved
		} else if alias != 0 {
			c.topicAliases[alias] = topic
		}
		var err error
		if wireProps, err = encodePropertyBlock(props); err != nil {
			c.protocolError(0x81)
			return
		}
	}
	if !validTopic(topic) {
		// Empty or wildcard topic name in a PUBLISH is a protocol violation.
		c.protocolError(0x90)
		return
	}
	if m.QoS == 2 {
		// A retransmitted QoS 2 PUBLISH while the first one is still pending
		// (publisher waiting for PUBREL) must be acknowledged again but not
		// delivered again.
		c.mu.Lock()
		_, pending := c.inboundQoS2[m.PacketID]
		full := !pending && c.inboundWindowFullLocked()
		c.mu.Unlock()
		if pending {
			_ = c.writePublishAck(2, m.PacketID, 0)
			return
		}
		if full {
			// The inbound QoS 2 window is full. MQTT 5 names the reason with PUBREC
			// 0x97 and the exchange never starts, so the message is not delivered and
			// the packet id is not remembered; MQTT 3.1.1 has no reason code, so the
			// PUBLISH is left unacknowledged and the client retries (the same rule
			// the broker applies to a publish it cannot accept).
			c.broker.metrics.RecordDroppedMessage("inflight_qos2_full")
			c.broker.metrics.RecordOverflow("inflight_qos2")
			if c.version == mqtt.Version5 {
				_ = c.writePublishAck(2, m.PacketID, ReasonQuotaExceeded)
			}
			return
		}
	}
	res := c.broker.publish(c.origin(), PublishEvent{
		ConnectionID: c.connID,
		ClientID:     c.id,
		Topic:        topic,
		Payload:      m.Payload,
		QoS:          m.QoS,
		PacketID:     m.PacketID,
		Retain:       m.Retain,
		Dup:          m.Dup,
		Properties:   wireProps,
	}, props)
	switch m.QoS {
	case 1:
		c.ackPublish(m.QoS, m.PacketID, res)
	case 2:
		if res.Accepted() {
			c.mu.Lock()
			c.inboundQoS2[m.PacketID] = struct{}{}
			c.mu.Unlock()
		}
		c.ackPublish(m.QoS, m.PacketID, res)
	}
}

func (c *client) handleSubscribe(m *mqtt.Subscribe) {
	if len(m.Properties.SubscriptionIdentifiers) > 0 {
		// The CONNACK this connection was admitted with advertises Subscription
		// Identifier Available 0 (props.go), so a SUBSCRIBE that carries one asks
		// for a capability this broker does not have. MQTT 5 §3.9.2.1.2 makes
		// that a protocol error, answered with DISCONNECT 0x9A (Subscription
		// Identifiers not supported); MQTT 3.1.1 has neither the property nor a
		// server DISCONNECT and is simply closed.
		c.broker.metrics.RecordDroppedMessage("subscription_identifier")
		c.protocolError(ReasonSubscriptionIdentifiersNotSupported)
		return
	}
	codes := make([]byte, 0, len(m.Subscriptions))
	existed := make([]bool, 0, len(m.Subscriptions))
	for _, req := range m.Subscriptions {
		if isSharedSubscription(req.Filter) {
			// A shared subscription is a filter starting with "$share/". The
			// CONNACK advertises Shared Subscription Available 0, and accepting it
			// as the literal filter it looks like would admit a subscription that
			// never matches anything — the client would "succeed" and silently
			// never receive a message. MQTT 5 names the refusal per filter (0x9E,
			// Shared Subscriptions not supported) and keeps the connection, as
			// §3.9 requires; MQTT 3.1.1 has only the 0x80 failure code.
			c.broker.metrics.RecordDroppedMessage("shared_subscription")
			codes = append(codes, subscribeFailure(c.version, ReasonSharedSubscriptionsNotSupported))
			existed = append(existed, false)
			continue
		}
		if !validFilter(req.Filter) {
			// MQTT 3.1.1 has no per-filter rejection code for a malformed
			// filter, so the protocol violation closes the connection. MQTT 5
			// answers it with Topic Filter invalid and keeps the session.
			if c.version != mqtt.Version5 {
				c.protocolError(0)
				return
			}
			codes = append(codes, 0x8f)
			existed = append(existed, false)
			continue
		}
		code, had := c.broker.subscribe(c, req.Filter, subscription{
			qos:               req.QoS,
			noLocal:           req.NoLocal,
			retainAsPublished: req.RetainAsPublished,
			retainHandling:    req.RetainHandling,
		})
		codes = append(codes, code)
		existed = append(existed, had)
	}
	_ = c.send(&mqtt.Suback{
		MessageHeader: c.header(),
		PacketID:      m.PacketID,
		ReasonCodes:   codes,
	})
	for i, req := range m.Subscriptions {
		if codes[i] > 2 { // 0, 1 and 2 are the granted QoS values
			continue
		}
		if req.RetainHandling == 2 || (req.RetainHandling == 1 && existed[i]) {
			continue
		}
		c.broker.sendRetained(c, req.Filter, req.QoS, req.RetainAsPublished)
	}
}

func (c *client) handleUnsubscribe(m *mqtt.Unsubscribe) {
	// MQTT 5 answers with one reason code per filter, all Success: a filter that
	// was not subscribed is a no-op for this broker. MQTT 3.1.1 carries none.
	codes := make([]byte, len(m.Filters))
	for i, filter := range m.Filters {
		c.broker.unsubscribe(c, filter)
		codes[i] = 0
	}
	_ = c.send(&mqtt.Unsuback{
		MessageHeader: c.header(),
		PacketID:      m.PacketID,
		ReasonCodes:   codes,
	})
}

func (c *client) writeConnack(code byte, assignedClientID string) error {
	ack := &mqtt.Connack{MessageHeader: c.header(), ReasonCode: code}
	if code == 0 {
		ack.SessionPresent = c.sessionPresent
	}
	if c.version == mqtt.Version5 && code == 0 {
		ack.Properties = connackProperties(assignedClientID, uint32(c.broker.cfg.MaxPacketSize))
	}
	return c.writeMessage(ack)
}

func (c *client) close(withWill bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	// No packet may be queued from here on: the socket is about to go away and an
	// enqueue must answer "closed" instead of growing a queue nobody drains.
	c.writeClosed.Store(true)
	will := c.will
	if !withWill {
		will = nil
	}
	c.mu.Unlock()
	_ = c.conn.Close()
	if will != nil && c.broker != nil {
		// The will is published right away, or scheduled when the client asked
		// for a Will Delay Interval (MQTT 5); see will.go for when the delay is
		// cut short or cancelled.
		c.publishWill(will)
	}
	// The will exchange above still runs on a live connection context; once the
	// connection is gone, every later observer (the disconnect hooks) sees the
	// cancelled context.
	c.stopContext()
	// The writer goroutine is joined last: closing the socket above already ends a
	// write in progress, and this makes close mean "no write of this connection is
	// still running".
	c.stopWriter()
}

// displace ends this connection because another CONNECT took its session over.
// MQTT 5 names that reason with a server DISCONNECT of 0x8E (Session taken over)
// so the client can tell a take-over from a network drop and not retry in a
// loop; MQTT 3.1.1 has no server DISCONNECT and is simply closed. The will is not
// published: the session moved to another connection, it did not die.
//
// The queue is closed before the DISCONNECT is written, so the writer goroutine
// cannot put a packet behind it — a server DISCONNECT is the last packet of the
// connection. The write (and close) is bounded by Config.WriteTimeout, whether
// the caller is the handshake of the new connection or the cluster receive
// goroutine.
func (c *client) displace() {
	c.mu.Lock()
	c.kicked = true
	c.mu.Unlock()
	c.writeClosed.Store(true)
	if c.version == mqtt.Version5 {
		c.sendDisconnect(ReasonSessionTakenOver)
	}
	c.close(false)
}

// willPublishProperties maps the will property block onto the PUBLISH properties
// the will message carries when it is published. Will Delay Interval is not a
// PUBLISH property: it decides when the will is published (see will.go), not what
// the published message looks like.
func willPublishProperties(p mqtt.Properties) mqtt.Properties {
	return mqtt.Properties{
		PayloadFormat:         p.PayloadFormat,
		MessageExpiryInterval: p.MessageExpiryInterval,
		ContentType:           p.ContentType,
		ResponseTopic:         p.ResponseTopic,
		CorrelationData:       p.CorrelationData,
		UserProperties:        p.UserProperties,
	}
}

func connackNotAuthorized(version byte) byte {
	if version == mqtt.Version5 {
		return 0x87
	}
	return 5
}

func connackTopicNameInvalid(version byte) byte {
	if version == mqtt.Version5 {
		return 0x90
	}
	return 2
}

func connackClientIDInvalid(version byte) byte {
	if version == mqtt.Version5 {
		return 0x85
	}
	return 2
}

func connackQuotaExceeded(version byte) byte {
	if version == mqtt.Version5 {
		return 0x97
	}
	return 5
}

// connackRateExceeded is the answer to a CONNECT refused because it arrived
// inside the Config.NotKickSeconds window of the connection it would displace.
// MQTT 5 has a reason code for exactly that (0x9F Connection rate exceeded, one
// of the CONNACK codes of §3.2.2.2); 0x8A (Banned) would claim an administrative
// ban, 0x89 (Server busy) a whole-server overload and 0x9C (Use another server) a
// redirection this broker cannot offer. MQTT 3.1.1 has no comparable return code
// and 3 (Server unavailable) is the closest true statement: the service for this
// connection is unavailable right now, and the client may retry.
func connackRateExceeded(version byte) byte {
	if version == mqtt.Version5 {
		return ReasonConnectionRateExceeded
	}
	return 3
}

// ackPublish acknowledges an accepted publish. A retryable or rejected publish
// gets no success ACK: MQTT 3.1.1 has no negative PUBACK, and an optimistic ACK
// would claim durability the broker does not have. MQTT 5 clients get the reason
// code instead.
func (c *client) ackPublish(qos byte, pid uint16, res PublishResult) {
	if res.Accepted() {
		_ = c.writePublishAck(qos, pid, 0)
		return
	}
	code := res.ReasonCode
	if code == 0 {
		if res.Disposition == PublishRetryable {
			code = ReasonQuotaExceeded
		} else {
			code = ReasonUnspecified
		}
	}
	if c.version == mqtt.Version5 {
		_ = c.writePublishAck(qos, pid, code)
	}
}

// writePublishAck writes the PUBACK of a QoS 1 publish or the PUBREC of a QoS 2
// one. The reason code is only encoded on MQTT 5.
func (c *client) writePublishAck(qos byte, pid uint16, reason byte) error {
	if qos == 2 {
		return c.send(&mqtt.Pubrec{MessageHeader: c.header(), PacketID: pid, ReasonCode: reason})
	}
	return c.send(&mqtt.Puback{MessageHeader: c.header(), PacketID: pid, ReasonCode: reason})
}

var connSeq atomic.Uint64

// nextConnectionID returns a process-unique connection identifier.
func nextConnectionID() ConnectionID {
	return ConnectionID(strconv.FormatUint(connSeq.Add(1), 10))
}
