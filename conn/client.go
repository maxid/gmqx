package conn

// The connection
//
// One Client is one physical connection to the broker, from the CONNECT to the
// socket closing. It owns the wire: the read loop and its reusable buffers, the
// write-behind queue and its writer goroutine, the outbound QoS 1/2 window with one
// retransmission timer, and the teardown that hands the session back. It does not
// own the broker: every decision that is not protocol — who this client is, what
// its limits are, whether it may publish, where its session goes — is a Host call
// (see host.go).

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
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

	"github.com/maxid/gmqx/hook"
	mqtt5 "github.com/maxid/gmqx/internal/mqtt5"
	topicmatch "github.com/maxid/gmqx/internal/topic"
	"github.com/maxid/gmqx/session"
)

// The connect-refusal errors a host returns and the connection maps onto MQTT
// reason codes. They are defined here because both sides compare against the same
// value, and the broker (gmqx/server) re-exports them so a consumer still checks
// gmqx.ErrClientIDInUse.
var (
	// ErrUnauthorized is returned when authentication fails.
	ErrUnauthorized = errors.New("gmqx: unauthorized")
	// ErrBadAuthMethod is returned by a CONNECT that names an MQTT 5 Authentication
	// Method the broker does not support.
	ErrBadAuthMethod = errors.New("gmqx: bad authentication method")
	// ErrMalformedPacket indicates an invalid MQTT packet.
	ErrMalformedPacket = mqtt.ErrMalformedPacket
	// ErrUnsupportedProtocol indicates a CONNECT whose protocol name or level the
	// broker does not speak.
	ErrUnsupportedProtocol = errors.New("gmqx: unsupported protocol level")
	// ErrClientIDInUse is returned in ConnectModeUnique when the client id exists.
	ErrClientIDInUse = errors.New("gmqx: client id already in use")
	// ErrMaxConnections is returned when the configured connection limit is reached.
	ErrMaxConnections = errors.New("gmqx: max connections reached")
	// ErrTakeoverRateExceeded is returned when a CONNECT would displace a connection
	// that is still inside the NotKickSeconds anti-flap window.
	ErrTakeoverRateExceeded = errors.New("gmqx: takeover inside the NotKickSeconds window")
)

var (
	// errOutboundQueueFull reports a QoS 0 PUBLISH that was dropped because the
	// connection's writer queue is full. QoS 0 is the only droppable class.
	errOutboundQueueFull = errors.New("gmqx: outbound queue full")
	// errSlowConsumer reports a packet that must not be dropped and found the
	// connection's writer queue full: the connection is disconnected instead.
	errSlowConsumer = errors.New("gmqx: slow consumer disconnected")
)

type Client struct {
	host   Host
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
	// hook.ConnInfo attached: it is what every hook of this connection receives. Both
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
	info hook.ConnectInfo
	// connectedAt is when the session of this connection was registered. It is
	// the reference point of the anti-flap window of a takeover
	// (Config.NotKickSeconds): written and read under the broker lock, never under c.mu.
	connectedAt    time.Time
	subs           map[string]session.Subscription
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
	// persistent session stays stored here (see Host.Unregister).
	kicked       bool
	inboundQoS2  map[uint16]struct{}
	topicAliases map[uint16]string
	outPID       uint16

	// inflight holds the outbound QoS 1/2 exchanges this connection has not
	// confirmed yet, keyed by packet id; retryWake asks retransmitLoop to
	// recompute its deadline. Both belong to the connection and die with it.
	//
	// inflightQoS1, inflightQoS2 and inflightUnconfirmed are the counts the window
	// checks need. They are maintained together with the map, under c.mu, so that
	// answering "is the window full?" is O(1): a scan of the table on every
	// delivery is what a saturated QoS 1/2 delivery path spent its lock on. A
	// QoS 2 entry stops being unconfirmed when its PUBREC arrives (it is then a
	// PUBREL exchange and no longer occupies the client's Receive Maximum), which
	// is the one state transition the counters follow.
	inflight            map[uint16]*inflightMessage
	inflightQoS1        int
	inflightQoS2        int
	inflightUnconfirmed int
	// restoredInflight is the packet ids restoreQoS2State carried over from the
	// previous connection, in restore order. resumeInflight re-sends exactly these
	// and nothing else: the CONNACK is already on the wire when it runs, so a
	// delivery made to the fresh connection can be registered in the meantime, and
	// re-sending the whole table would duplicate it with DUP set. It is filled
	// before the connection is served and emptied by resumeInflight.
	restoredInflight []uint16
	// retryAt is the deadline retransmitLoop is currently sleeping for, zero when
	// no deadline is armed. It is maintained under c.mu and is what lets a new
	// delivery skip the wakeup when its retransmission cannot be the earliest.
	retryAt   time.Time
	retryWake chan struct{}
	// pending is where an outbound QoS 1/2 delivery waits when the inflight window
	// of its QoS is full: arrival order, drained by the acknowledgement that frees
	// a slot, bounded by Config.MaxInflightQueueSize. It is a live-connection
	// queue, unlike the session queue below, and it ends with the connection —
	// except for a persistent session, whose pending deliveries move into the
	// session queue when the connection goes away (see conn.Handoff).
	pending []pendingMessage
	// handoff says where a persistent session of this connection stands while it
	// moves to its offline queue. A delivery that arrives in that window waits in
	// pending (handoffCollecting) or is queued through the offline path
	// (handoffStored) instead of being dropped (see client.sendPublish). It is
	// maintained under c.mu and is handoffNone for every clean session.
	handoff handoffState
	// queue is what the session queued while it was offline, owned by this
	// connection between register (which takes it from the store) and unregister
	// (which stores what a failed drain left behind). Filled and read on the
	// connection's goroutine only.
	queue []session.Pending

	// readBuf is this connection's inbound PUBLISH buffer: the topic bytes and the
	// payload of the packet being handled are slices of it, so a packet costs no
	// allocation for them (see mqtt.ReadPublishInto). It is owned by the read loop
	// and grows only: a packet larger than the buffer reallocates it once, and it
	// can never exceed the broker's MaxPacketSize, because the codec rejects a
	// larger remaining length before reading the body. Anything that keeps the
	// payload past the end of handlePublish copies it — exactly once, just before
	// the publication is routed (see handlePublish).
	readBuf []byte

	// version is the protocol level of the connection: mqtt.Version311 or
	// mqtt.Version5. MQTT 3.1 clients decode and encode with the 3.1.1 layout,
	// which is byte-identical for everything except CONNECT.
	version byte

	// receiveMaximum is the MQTT 5 Receive Maximum the client asked for in its
	// CONNECT, with the protocol default 65535 standing in for the absent
	// property (the codec refuses an explicit zero as a malformed CONNECT, which
	// is the Protocol Error the specification asks for). It caps how many QoS 1/2
	// PUBLISHes this connection may have unconfirmed by PUBACK or PUBREC at once
	// (see windowFullLocked). Written by the handshake before the connection is
	// registered and read under c.mu afterwards.
	receiveMaximum uint16
	// maxPacketSize is the Maximum Packet Size the client asked for in its
	// CONNECT, 0 when it did not ask for a limit: no packet larger than this is
	// ever encoded onto the wire for this connection (see enqueue). Written by the
	// handshake before the connection is registered and never changed afterwards,
	// so the send paths may read it without a lock.
	maxPacketSize uint32

	// authMethod is the MQTT 5 enhanced-authentication method the CONNECT
	// negotiated, "" when the connection authenticated any other way. It is written
	// by the handshake before the connection is registered and never changes: a
	// re-authentication must use the negotiated method. Only the connection's own
	// goroutine reads it afterwards (the AUTH handler and the read loop), so it
	// needs no lock.
	authMethod string
	// authData is the final Authentication Data of a successful CONNECT exchange;
	// it goes into the CONNACK. Written with authMethod, before the connection is
	// registered and the CONNACK is written.
	authData []byte
}

func New(h Host, netConn net.Conn) *Client {
	connCtx, cancel := context.WithCancel(h.LifecycleContext())
	queueSize := h.Limits().MaxOutboundQueueSize
	if queueSize <= 0 {
		queueSize = defaultMaxOutboundQueueSize
	}
	return &Client{
		host:         h,
		namespace:    h.Namespace(),
		connCtx:      connCtx,
		ctx:          connCtx,
		cancel:       cancel,
		parentCancel: cancel,
		connID:       nextConnectionID(),
		conn:         netConn,
		r:            bufio.NewReaderSize(netConn, 4096),
		subs:         make(map[string]session.Subscription),
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
// receives. It carries hook.ConnInfo and is cancelled by stopContext when the
// connection ends. It is never nil.
func (c *Client) context() context.Context { return c.ctx }

// attachConnectContext builds the connection context from the parsed CONNECT:
// the optional Config.ConnContext hook enriches it once, and hook.ConnInfo is attached
// so every hook can identify the connection without a thread-local request
// scope. The context it creates is cancelled by stopContext, even when the hook
// returns a context that is not derived from the one it was given.
func (c *Client) attachConnectContext(info hook.ConnectInfo) {
	ctx := c.connCtx
	if hook := c.host.Limits().ConnContext; hook != nil {
		if enriched := hook(ctx, info); enriched != nil {
			enrichedCtx, cancel := context.WithCancel(enriched)
			c.connCtx, c.cancel = enrichedCtx, cancel
			ctx = enrichedCtx
		}
	}
	c.ctx = hook.ContextWithConn(ctx, c.connInfo(info))
}

// setConnInfo refreshes the hook.ConnInfo value after the broker assigned a client
// id. The connection context itself, and therefore its cancellation, is kept.
func (c *Client) setConnInfo(info hook.ConnectInfo) {
	c.ctx = hook.ContextWithConn(c.connCtx, c.connInfo(info))
}

func (c *Client) connInfo(info hook.ConnectInfo) hook.ConnInfo {
	return hook.ConnInfo{
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
// StopContext cancels the connection context. It is safe to call more than once.
func (c *Client) StopContext() {
	c.stopOnce.Do(func() {
		c.parentCancel()
		c.cancel()
	})
}

// persistentSession reports whether the session state survives the connection.
// MQTT 3.1.1 keys it off Clean Session; MQTT 5 keys it off Clean Start plus a
// non-zero Session Expiry Interval (the absent property means 0, so the session
// ends with the connection).
func (c *Client) persistentSession() bool {
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
func (c *Client) resumesSession() bool { return !c.info.CleanSession }

// header is the protocol level every packet this client sends carries.
// SessionExpiryDeadline is the deadline the broker enforces for the session this
// connection leaves behind, and the one its own expiry sweeper compares against.
//
// MQTT 3.1.1 has no Session Expiry Interval, so its persistent sessions keep the
// previous behavior and never expire (the zero time). MQTT 5 stamps
// now + Session Expiry Interval; a zero interval does not reach this point
// because such a session is not persistent at all — the session ends with the
// connection, which is also why a CONNECT with a zero interval can resume a
// stored session but never creates one. The interval is counted in the broker's
// protocol second (Config.unit), which is time.Second outside the tests.
func (c *Client) SessionExpiryDeadline() time.Time {
	if c.version != mqtt.Version5 || c.info.SessionExpiry == 0 {
		return time.Time{}
	}
	return time.Now().Add(time.Duration(c.info.SessionExpiry) * c.host.Second())
}

func (c *Client) header() mqtt.MessageHeader {
	return mqtt.MessageHeader{Level: c.version}
}

// readMessage reads and decodes one control packet. The typed codec owns the
// wire format, so gmqx never branches on a fixed-header byte.
func (c *Client) readMessage() (mqtt.Message, error) {
	d := mqtt.Decoder{Version: c.version, MaxPacketSize: c.host.Limits().MaxPacketSize}
	return d.Decode(c.r)
}

// readPublish reads one PUBLISH into the connection's reusable buffer, so the
// topic and the payload of the packet cost no allocation. ErrNotPublish means
// the next packet is something else: the call consumed nothing and readMessage
// reads it.
func (c *Client) readPublish() (mqtt.PublishView, error) {
	view, err := mqtt.ReadPublishInto(c.r, c.version, c.host.Limits().MaxPacketSize, c.readBuf)
	if err != nil {
		return mqtt.PublishView{}, err
	}
	c.readBuf = view.Buffer
	return view, nil
}

// writeMessage encodes one packet and writes it to the socket on the calling
// goroutine, under the connection's write deadline. Only the handshake (CONNACK)
// and the terminal server DISCONNECT use it: both must be on the wire before the
// connection is closed, and neither may wait behind a queued delivery. Every
// other packet goes through enqueue.
func (c *Client) writeMessage(m mqtt.Message) error {
	if err := c.checkPacketSize(m); err != nil {
		return err
	}
	return c.writeMessageUnchecked(m)
}

// writeMessageUnchecked writes a packet whose size was already checked: the writer
// goroutine checks once, when the packet enters the queue (enqueue), and must not
// encode it a second time just to measure it again.
func (c *Client) writeMessageUnchecked(m mqtt.Message) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.writeLocked(m)
}

// errPacketTooLarge reports a packet the client's CONNECT Maximum Packet Size
// does not allow the broker to write. It is not a transport failure: the packet is
// dropped (and counted) while the connection stays usable.
var errPacketTooLarge = errors.New("gmqx: packet exceeds the client maximum packet size")

// countingWriter counts the bytes handed to it and discards them, which is how
// encodedPacketSize measures a packet without keeping it.
type countingWriter int

func (w *countingWriter) Write(p []byte) (int, error) {
	*w += countingWriter(len(p))
	return len(p), nil
}

// encodedPacketSize is the number of bytes m occupies on the wire, fixed header
// and remaining-length field included: the size the MQTT 5 Maximum Packet Size of
// a client applies to.
func encodedPacketSize(m mqtt.Message) (int, error) {
	var n countingWriter
	if err := mqtt.Encode(&n, m); err != nil {
		return 0, err
	}
	return int(n), nil
}

// checkPacketSize refuses a packet larger than the Maximum Packet Size the client
// announced in its CONNECT. The packet is dropped instead of being written: MQTT
// has no way to split or truncate a control packet, so a broker that sent it would
// be violating the limit the client asked for, and one that cut it would corrupt
// the stream. Every refused packet is counted
// (Metrics.RecordDroppedMessage("max_packet_size_too_large")) and logged with the
// connection and the packet type, so a client whose limit is too small for what it
// subscribed to is visible instead of silently starved. A client that announced no
// limit (the common case) pays nothing: the size is only computed when a limit
// exists.
func (c *Client) checkPacketSize(m mqtt.Message) error {
	limit := c.maxPacketSize
	if limit == 0 || m == nil {
		return nil
	}
	size, err := encodedPacketSize(m)
	if err != nil {
		// The encoder will report the real problem when the packet is written.
		return nil
	}
	if uint32(size) <= limit {
		return nil
	}
	c.host.Metrics().RecordDroppedMessage("max_packet_size_too_large")
	c.warn("gmqx dropped a packet over the client maximum packet size",
		"client", c.id, "connection", string(c.connID), "packet", m.Type(),
		"size", size, "limit", limit, "reason", "max_packet_size_too_large")
	return errPacketTooLarge
}

// writeAckSync writes one acknowledgement frame directly, for the window before
// the writer goroutine exists (see enqueue).
func (c *Client) writeAckSync(kind uint8, pid uint16) error {
	var frame [ackFrameLen]byte
	buf := appendAck(frame[:0], kind, pid)
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout()))
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.conn.Write(buf)
	return err
}

// writeLocked writes one packet with the connection's write deadline applied.
// The caller holds wmu, which is what keeps the writer goroutine and a
// synchronous write from interleaving on the socket.
func (c *Client) writeLocked(m mqtt.Message) error {
	// A transport without write-deadline support is still usable; the deadline is
	// the guard against a wedged peer, not a requirement of the protocol.
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout()))
	return mqtt.Encode(c.conn, m)
}

// writeTimeout is the write deadline of one packet: Config.WriteTimeout, or the
// documented default when the broker was built without the config defaulting
// (New applies it, a bare Broker literal may not).
func (c *Client) writeTimeout() time.Duration {
	if timeout := c.host.Limits().WriteTimeout; timeout > 0 {
		return timeout
	}
	return defaultWriteTimeout
}

// outboundPacket is one packet waiting for the writer goroutine. droppable marks
// the only packets a full queue may discard: QoS 0 PUBLISHes, which the protocol
// already allows to be lost.
//
// The PUBACK family with a default reason code and no properties — which is every
// acknowledgement this broker sends unless an MQTT 5 client is being told about a
// failure — is carried as its type and packet id instead of a typed message. Its
// wire form is four bytes, so the packet costs no allocation at all, and the
// writer builds it directly (see appendAck). Anything else sets kind to
// outboundMessage and travels as msg.
//
// Trade-off of the queue model: the queue is bounded in packets and not in
// bytes, so one connection can hold at most MaxOutboundQueueSize packets (each
// of which MaxPacketSize bounds) — the memory of a slow consumer is capped
// without a byte-accounting pass on every send.
type outboundPacket struct {
	msg       mqtt.Message
	packetID  uint16
	kind      uint8
	droppable bool
}

// Kinds of outboundPacket. outboundMessage means msg holds the packet; the other
// four are the acknowledgement family with a zero reason code and no properties.
const (
	outboundMessage uint8 = iota
	outboundPuback
	outboundPubrec
	outboundPubrel
	outboundPubcomp
)

// ackFrameLen is the wire size of an acknowledgement with no reason code and no
// properties: fixed header, remaining length 2, packet id.
const ackFrameLen = 4

// startWriter starts the connection's single writer goroutine. It is called once,
// from the handshake, right after the CONNACK: from here on every packet of this
// connection is written by that goroutine, in the order the packets were queued,
// so no caller — the publisher's read loop, a will publish, the cluster receive
// goroutine — ever waits on this socket.
func (c *Client) startWriter() {
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
// StopWriter ends the writer goroutine and joins it. It is safe to call more than once.
func (c *Client) StopWriter() {
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

// writeBatchMax is the largest number of packets one write of the writer
// goroutine carries, and writeBatchBytes the largest frame buffer it fills. Both
// only bound the memory and the time one batch may hold; a normal queue has far
// fewer packets waiting, so a batch is usually the whole backlog.
//
// writeBatchInitial is the buffer a connection starts with. It grows (up to the
// bound) only on a connection that actually has bursts, so a mostly idle device
// connection does not pay for a large buffer it never fills.
const (
	writeBatchMax     = 64
	writeBatchBytes   = 64 << 10
	writeBatchInitial = 4 << 10
)

// writeLoop is the writer goroutine: it writes the queued packets, in queue
// order, and ends with the connection. The packets that are already waiting when
// the goroutine wakes are encoded into one buffer and handed to the socket with
// one write, under Config.WriteTimeout: a PUBACK and the deliveries queued behind
// it cost one write instead of one each, and the order is still exactly the order
// they were queued in.
//
// A write that fails (a closed socket, an expired write deadline, a peer that
// reset the connection) ends the connection: close then runs on the read
// goroutine, which is what publishes the will and unregisters the session.
func (c *Client) writeLoop() {
	defer close(c.writerDone)
	batch := writeBatch{}
	batch.frame.buf = make([]byte, 0, writeBatchInitial)
	for {
		select {
		case <-c.writerStop:
			return
		case p := <-c.outq:
			if !c.appendOutbound(&batch, p) {
				return
			}
		}
		// Drain what is already queued, without waiting: the packets that arrived
		// while the writer was waking up travel with this one.
		for batch.packets < writeBatchMax && batch.frame.Len() < writeBatchBytes {
			var next outboundPacket
			var ok bool
			select {
			case <-c.writerStop:
				return
			case next, ok = <-c.outq:
			default:
			}
			if !ok {
				break
			}
			if !c.appendOutbound(&batch, next) {
				return
			}
		}
		if err := c.flushBatch(&batch); err != nil {
			c.writeFailed(err)
			return
		}
	}
}

// writeBatch is the reusable frame buffer of the writer goroutine: the encoded
// bytes of the packets of one write, and how many packets they came from. It is
// only touched by the writer goroutine, and the buffer is allocated once per
// connection, not per packet.
type writeBatch struct {
	frame   appendBuffer
	packets int
}

// appendBuffer is an io.Writer that appends to a byte slice. It is the writer
// side of mqtt.Encode in the write loop: the packet is encoded into the batch
// instead of straight onto the socket, so the packets that are already queued
// travel with one write. The buffer lives in the writeBatch, so passing its
// address to the encoder allocates nothing per packet.
type appendBuffer struct {
	buf []byte
}

func (b *appendBuffer) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

// Len is the number of encoded bytes waiting in the batch.
func (b *appendBuffer) Len() int { return len(b.buf) }

// Bytes is the encoded batch.
func (b *appendBuffer) Bytes() []byte { return b.buf }

// Reset empties the batch and keeps its capacity for the next one.
func (b *appendBuffer) Reset() { b.buf = b.buf[:0] }

// appendOutbound encodes one packet into the batch and reports whether the
// writer may continue. It refuses to work once writeClosed is set: the connection
// is closing (or was displaced), a server DISCONNECT may already be on the wire
// and it has to be the last packet, so the rest of the queue is not written.
func (c *Client) appendOutbound(batch *writeBatch, p outboundPacket) bool {
	if c.writeClosed.Load() {
		return false
	}
	if p.kind != outboundMessage {
		batch.frame.buf = appendAck(batch.frame.buf, p.kind, p.packetID)
		batch.packets++
		return true
	}
	_ = mqtt.Encode(&batch.frame, p.msg)
	batch.packets++
	return true
}

// appendAck appends one acknowledgement frame: fixed header, remaining length 2,
// packet id. The PUBREL fixed header carries its mandatory 0x02 flags.
func appendAck(dst []byte, kind uint8, pid uint16) []byte {
	header := ackType(kind) << 4
	if kind == outboundPubrel {
		header |= 0x02
	}
	return append(dst, header, 2, byte(pid>>8), byte(pid))
}

// flushBatch writes the encoded batch with one socket write, under the
// connection's write deadline and wmu, and resets the batch for the next one.
func (c *Client) flushBatch(batch *writeBatch) error {
	if batch.frame.Len() == 0 {
		return nil
	}
	pending := batch.frame.Bytes()
	batch.frame.Reset()
	batch.packets = 0
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout()))
	c.wmu.Lock()
	_, err := c.conn.Write(pending)
	c.wmu.Unlock()
	return err
}

// writeFailed ends a connection whose socket write failed. It never calls close:
// closing the socket is enough to make the read goroutine return and run the
// connection teardown, which joins this goroutine.
func (c *Client) writeFailed(err error) {
	c.writeClosed.Store(true)
	switch {
	case errors.Is(err, os.ErrDeadlineExceeded):
		c.warn("gmqx write deadline exceeded, closing connection",
			"client", c.id, "connection", string(c.connID), "remote", c.remoteAddr(),
			"timeout", c.writeTimeout())
	case c.host.Logger() != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe):
		c.host.Logger().Debug("gmqx connection write failed",
			"client", c.id, "connection", string(c.connID), "remote", c.remoteAddr(), "err", err)
	}
	_ = c.conn.Close()
}

// remoteAddr is the peer address of the connection, "" when the transport has
// none. It is immutable for the life of the connection.
func (c *Client) remoteAddr() string {
	if addr := c.conn.RemoteAddr(); addr != nil {
		return addr.String()
	}
	return ""
}

// warn logs at Warn level. A broker built without Config.Logger (only tests do
// that) has no logger, and a delivery guard must not panic there.
func (c *Client) warn(msg string, args ...any) {
	if c.host.Logger() != nil {
		c.host.Logger().Warn(msg, args...)
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
func (c *Client) enqueue(p outboundPacket) error {
	if c.writeClosed.Load() {
		return errClientClosed
	}
	// A packet the client's Maximum Packet Size forbids is refused before it is
	// queued: no queue slot is spent on something that can never be written, and a
	// dropped acknowledgement or SUBACK is reported instead of violating the limit.
	if err := c.checkPacketSize(p.msg); err != nil {
		return err
	}
	if !c.writerActive.Load() {
		// The size was checked above; the handshake path writes synchronously so the
		// CONNACK is on the wire before anything queued behind it.
		if p.kind != outboundMessage {
			return c.writeAckSync(p.kind, p.packetID)
		}
		return c.writeMessageUnchecked(p.msg)
	}
	select {
	case c.outq <- p:
		return nil
	default:
	}
	c.host.Metrics().RecordOverflow("outbound_queue")
	if p.droppable {
		c.host.Metrics().RecordDroppedMessage("outbound_queue_full")
		return errOutboundQueueFull
	}
	c.dropSlowConsumer()
	return errSlowConsumer
}

// send queues one packet that must not be dropped: an acknowledgement, a
// PINGRESP, a SUBACK, a retransmission. A full queue disconnects the connection.
func (c *Client) send(m mqtt.Message) error {
	return c.enqueue(outboundPacket{msg: m})
}

// enqueueAck queues one acknowledgement of the PUBACK family with a default
// reason code and no properties. It is the hot acknowledgement path (one per
// inbound QoS 1 publish and two per inbound QoS 2 publish) and it allocates
// nothing: the writer encodes the four-byte frame from the packet id.
func (c *Client) enqueueAck(kind uint8, pid uint16) error {
	if limit := c.maxPacketSize; limit != 0 && limit < ackFrameLen {
		// The client's Maximum Packet Size forbids even an acknowledgement. The
		// packet is dropped and counted, exactly like every other oversize packet
		// (see checkPacketSize); the connection stays usable.
		c.host.Metrics().RecordDroppedMessage("max_packet_size_too_large")
		c.warn("gmqx dropped a packet over the client maximum packet size",
			"client", c.id, "connection", string(c.connID), "packet", ackType(kind),
			"size", ackFrameLen, "limit", limit, "reason", "max_packet_size_too_large")
		return errPacketTooLarge
	}
	return c.enqueue(outboundPacket{kind: kind, packetID: pid})
}

// ackType is the mqtt control packet type of an outboundPacket kind.
func ackType(kind uint8) byte {
	switch kind {
	case outboundPuback:
		return mqtt.TypePuback
	case outboundPubrec:
		return mqtt.TypePubrec
	case outboundPubrel:
		return mqtt.TypePubrel
	}
	return mqtt.TypePubcomp
}

// sendPublishFrame queues one PUBLISH. A QoS 0 PUBLISH is the one packet a full
// queue may discard; a QoS 1/2 one disconnects the slow consumer instead.
func (c *Client) sendPublishFrame(m *mqtt.Publish) error {
	return c.enqueue(outboundPacket{msg: m, droppable: m.QoS == 0})
}

// dropSlowConsumer disconnects a connection whose writer queue is full and cannot
// take a packet that must not be lost. The report happens once per connection;
// closing the socket makes the read goroutine run the normal teardown, which
// publishes the will (the connection did not end in an orderly DISCONNECT) and
// unregisters the session.
func (c *Client) dropSlowConsumer() {
	c.slowOnce.Do(func() {
		c.host.Metrics().RecordDroppedMessage("slow_consumer")
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
func (c *Client) readConnect() (connect *mqtt.Connect, v31 bool, err error) {
	header, body, err := mqtt.ReadPacket(c.r, c.host.Limits().MaxPacketSize)
	if err != nil {
		return nil, false, err
	}
	if header>>4 != mqtt.TypeConnect {
		return nil, false, fmt.Errorf("%w: first packet is not CONNECT", ErrMalformedPacket)
	}
	body, v31 = normalizeV31Connect(body)
	frame := mqtt.AppendRemainingLength([]byte{header}, len(body))
	frame = append(frame, body...)
	d := mqtt.Decoder{MaxPacketSize: c.host.Limits().MaxPacketSize}
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

// connectInfo maps a decoded CONNECT onto the public hook.ConnectInfo the SPI sees.
// The MQTT 5 authentication properties travel with it: AuthMethod selects the
// enhanced-authentication path and AuthData is the client's first exchange
// message.
func connectInfo(m *mqtt.Connect) hook.ConnectInfo {
	info := hook.ConnectInfo{
		ClientID:      string(m.ClientID),
		Username:      string(m.Username),
		Password:      m.Password,
		KeepAlive:     m.KeepAlive,
		CleanSession:  m.CleanSession,
		SessionExpiry: m.Properties.SessionExpiryInterval,
		AuthMethod:    m.Properties.AuthenticationMethod,
		AuthData:      m.Properties.AuthenticationData,
	}
	if m.Will != nil {
		info.WillTopic = m.Will.Topic
		info.WillPayload = m.Will.Payload
	}
	return info
}

// Handshake runs the CONNECT exchange up to the point the connection is ready to
// serve: it reads and validates the CONNECT, authenticates, registers the session
// and writes the CONNACK.
func (c *Client) Handshake() error {
	// The CONNECT phase runs under its own read deadline: a socket that is opened
	// and then says nothing must not hold a connection slot, a goroutine and a
	// session until the peer goes away. The read loop replaces this deadline with
	// the keep-alive one as soon as the handshake is through.
	_ = c.conn.SetReadDeadline(time.Now().Add(c.host.Limits().ConnectTimeout))
	connect, v31, err := c.readConnect()
	if err != nil {
		return err
	}
	c.version = connect.Version()
	info := connectInfo(connect)
	info.RemoteAddr = c.conn.RemoteAddr().String()
	c.attachConnectContext(info)
	if info.AuthData != nil && info.AuthMethod == "" {
		// Authentication Data without an Authentication Method is a Protocol Error
		// (MQTT 5 §3.1.2.11.10): there is no exchange the data could belong to, and
		// AuthManager must not be handed bytes it never asked for.
		_ = c.writeConnack(connackProtocolError(c.version), "")
		return fmt.Errorf("%w: Authentication Data without an Authentication Method", ErrMalformedPacket)
	}
	switch {
	case info.AuthMethod != "":
		// MQTT 5 enhanced authentication: the exchange is the authentication of
		// this connection, so AuthManager.Authenticate is skipped for it (see
		// hook.AuthExchange). The whole exchange runs inside the CONNECT read deadline,
		// and each call into it is bounded by Config.AuthTimeout.
		exchange := c.host.AuthExchange(info.AuthMethod)
		if exchange == nil {
			c.host.Metrics().RecordDroppedMessage("auth_method")
			_ = c.writeConnack(connackBadAuthMethod(c.version), "")
			return fmt.Errorf("%w: %q", ErrBadAuthMethod, info.AuthMethod)
		}
		step, err := c.runAuth(exchange, info)
		if err != nil {
			var refused authReject
			if errors.As(err, &refused) {
				_ = c.writeConnack(refused.code, "")
				return fmt.Errorf("%w: %v", ErrUnauthorized, err)
			}
			return err
		}
		// The exchange is the authentication: the identity it returns wins over the
		// CONNECT's (a device key bound to the credential, for example), and the
		// CONNACK reports the negotiated method and the final authentication data.
		info = mergeAuthIdentity(info, step.Info)
		c.authMethod = info.AuthMethod
		c.authData = step.Data
	default:
		if err := c.host.Authenticate(c.context(), info); err != nil {
			_ = c.writeConnack(connackNotAuthorized(c.version), "")
			return fmt.Errorf("%w: %v", ErrUnauthorized, err)
		}
	}
	assigned := ""
	if info.ClientID == "" {
		if v31 || !info.CleanSession {
			_ = c.writeConnack(connackClientIDInvalid(c.version), "")
			return fmt.Errorf("%w: empty client id is not allowed", ErrMalformedPacket)
		}
		info.ClientID = RandomID()
		assigned = info.ClientID
	}
	if v31 && len(info.ClientID) > 23 {
		_ = c.writeConnack(connackClientIDInvalid(c.version), "")
		return fmt.Errorf("%w: MQTT 3.1 client id exceeds 23 characters", ErrMalformedPacket)
	}
	if connect.Will != nil && !topicmatch.ValidTopic(connect.Will.Topic) {
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
	// The two MQTT 5 flow-control properties of the CONNECT are the connection's
	// for its whole life: Receive Maximum caps the broker's outbound windows and
	// Maximum Packet Size bounds every packet the broker writes to it. An absent
	// property is the zero value; Receive Maximum means 65535 then, the protocol
	// default, while Maximum Packet Size means "no limit". An explicit zero is
	// already refused by the codec, which is the Protocol Error (0x82) the
	// specification prescribes.
	c.receiveMaximum = connect.Properties.ReceiveMaximum
	if c.receiveMaximum == 0 {
		c.receiveMaximum = maxReceiveMaximum
	}
	c.maxPacketSize = connect.Properties.MaximumPacketSize
	if connect.Will != nil {
		will := *connect.Will
		will.Payload = append([]byte(nil), connect.Will.Payload...)
		c.will = &will
	}
	c.mu.Unlock()
	// The client id can be assigned by the broker, so hook.ConnInfo is refreshed with
	// the final identity before anything else observes the connection.
	c.setConnInfo(info)
	if err := c.host.Register(c); err != nil {
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
	// CONNACK, so nothing can be written ahead of it. A resumed session re-sends
	// the QoS 1/2 exchanges it left unconfirmed before its offline queue drains, so
	// the client sees the packets it already knew about first (§4.4).
	c.startWriter()
	c.startRetransmitter()
	c.resumeInflight()
	c.drainOfflineQueue()
	return nil
}

// Loop serves the connection until it ends: it reads packets, dispatches them and
// runs the teardown.
func (c *Client) Loop() {
	for {
		deadline := time.Time{}
		c.mu.Lock()
		if c.keepAlive > 0 {
			deadline = time.Now().Add(c.keepAlive * 3 / 2)
		}
		c.mu.Unlock()
		_ = c.conn.SetReadDeadline(deadline)

		// PUBLISH has its own read path: it is the packet a publisher sends by the
		// thousand, and it decodes into the connection's reusable buffer instead of
		// allocating a topic string, a payload and a Properties value per packet.
		// Every other packet — and every read error, PUBLISH or not — is handled
		// exactly as before, by the typed decoder below.
		view, pubErr := c.readPublish()
		if pubErr == nil {
			c.handlePublish(view)
			continue
		}
		if !errors.Is(pubErr, mqtt.ErrNotPublish) {
			c.readFailed(pubErr)
			return
		}

		msg, err := c.readMessage()
		if err != nil {
			c.readFailed(err)
			return
		}
		// No PUBLISH reaches this switch: readPublish above has already read every
		// one of them.
		switch m := msg.(type) {
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
			if sink := c.host.Sink(); sink != nil {
				sink.OnPing(c.context(), c.id)
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
			// AUTH on an established connection is a re-authentication (0x19) or a
			// protocol error; handleAuth runs the exchange inline, on this goroutine,
			// so it is ordered with every other packet of the client.
			c.handleAuth(m)
		default:
			c.host.Logger().Debug("gmqx ignoring packet type", "type", msg.Type())
		}
	}
}

// readFailed ends the connection after a read that did not produce a packet: it
// marks the connection lost (so the will is published), tells an MQTT 5 client why
// when the error is a protocol violation, and closes.
func (c *Client) readFailed(err error) {
	c.mu.Lock()
	c.lost = true
	c.mu.Unlock()
	c.failProtocol(err)
	c.close(true)
}

// failProtocol tells an MQTT 5 client why the connection is being closed. An
// MQTT 3.1.1 client cannot be told: the protocol has no server DISCONNECT, so
// the connection is simply closed. Transport errors (a closed connection or an
// expired keep-alive deadline) are not protocol errors and stay silent.
func (c *Client) failProtocol(err error) {
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
	if zeroSubscriptionIdentifier(err) {
		reason = hook.ReasonProtocolError
	}
	c.sendDisconnect(reason)
}

// zeroSubscriptionIdentifier reports whether err is the codec refusing a
// Subscription Identifier whose value is 0. MQTT 5 §3.8.2.1.2 calls that a
// Protocol Error (DISCONNECT 0x82) while every other unparsable packet is a
// Malformed Packet (0x81); the codec exposes a dedicated sentinel so the broker
// can tell them apart without matching on error text.
func zeroSubscriptionIdentifier(err error) bool {
	return errors.Is(err, mqtt.ErrZeroSubscriptionIdentifier)
}

// protocolError answers an MQTT 5 protocol violation with a DISCONNECT and ends
// the connection. MQTT 3.1.1 closes the connection without a packet.
func (c *Client) protocolError(reason byte) {
	if c.version == mqtt.Version5 {
		c.sendDisconnect(reason)
	}
	c.close(true)
}

func (c *Client) sendDisconnect(reason byte) {
	// The DISCONNECT is the last packet of the connection and close follows it
	// immediately, so it is written synchronously (under the write deadline)
	// instead of being queued behind deliveries that will never be sent.
	_ = c.writeMessage(&mqtt.Disconnect{MessageHeader: c.header(), ReasonCode: reason})
}

// handlePubrel releases an inbound QoS 2 publish: the client waits for the
// PUBCOMP before it reuses the packet id. Completing the flow forgets the packet
// id, so the next PUBLISH that reuses it is delivered as a new message.
func (c *Client) handlePubrel(m *mqtt.Pubrel) {
	c.mu.Lock()
	delete(c.inboundQoS2, m.PacketID)
	c.mu.Unlock()
	_ = c.enqueueAck(outboundPubcomp, m.PacketID)
}

// handlePublish routes one inbound PUBLISH.
//
// The packet arrives as an mqtt.PublishView: its topic and payload are slices of
// the connection's reusable read buffer (c.readBuf), valid only until the read
// loop reads the next packet. Everything before the publication below only reads
// them — the topic name becomes a string (which is what routing indexes on) and
// the MQTT 5 properties are already owned by the view — so the payload the
// publication is given is the one copy this path makes: see the call to
// Host.Publish.
func (c *Client) handlePublish(v mqtt.PublishView) {
	topic := string(v.Topic)
	var props mqtt.Properties
	if v.Properties != nil {
		props = *v.Properties
	}
	// Topic Alias and Subscription Identifier are hop-local: the alias resolves
	// (and records) the topic name, and neither property is forwarded.
	alias := props.TopicAlias
	props.TopicAlias = 0
	if len(props.SubscriptionIdentifiers) > 0 {
		// A PUBLISH from a client must not carry a Subscription Identifier
		// (MQTT 5 §3.3.4); it is a protocol error, not something to strip.
		c.protocolError(0x82)
		return
	}
	var wireProps []byte
	if c.version == mqtt.Version5 {
		if alias > mqtt5.TopicAliasMaximum {
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
		if wireProps, err = mqtt5.EncodePropertyBlock(props); err != nil {
			c.protocolError(0x81)
			return
		}
	}
	if !topicmatch.ValidTopic(topic) {
		// Empty or wildcard topic name in a PUBLISH is a protocol violation.
		c.protocolError(0x90)
		return
	}
	if v.QoS == 2 {
		// A retransmitted QoS 2 PUBLISH while the first one is still pending
		// (publisher waiting for PUBREL) must be acknowledged again but not
		// delivered again.
		c.mu.Lock()
		_, pending := c.inboundQoS2[v.PacketID]
		full := !pending && c.inboundWindowFullLocked()
		c.mu.Unlock()
		if pending {
			_ = c.writePublishAck(2, v.PacketID, 0)
			return
		}
		if full {
			// The inbound window — the broker's own Receive Maximum — is full. MQTT 5
			// names the reason with PUBREC 0x97 and the exchange never starts, so the
			// message is not delivered and the packet id is not remembered; MQTT 3.1.1
			// has no reason code, so the PUBLISH is left unacknowledged and the client
			// retries (the same rule the broker applies to a publish it cannot accept).
			c.host.ReceiveMaxFull(topic)
			if c.version == mqtt.Version5 {
				_ = c.writePublishAck(2, v.PacketID, hook.ReasonQuotaExceeded)
			}
			return
		}
	} else if v.QoS == 1 {
		// QoS 1 has no handshake to hold a slot open: the broker acknowledges a
		// publish as soon as it is accepted, so the only way the inbound window can
		// be full for a QoS 1 PUBLISH is that the client is still waiting for the
		// PUBREL of enough QoS 2 publishes. In that case the publish is neither
		// delivered nor acknowledged — a PUBACK would claim it was accepted — and the
		// client retransmits it later (§4.4), which is exactly the behaviour the
		// broker already has for a publish it cannot accept. MQTT 5 deliberately
		// does not get a negative PUBACK here: the task of the window is to make the
		// client slow down, and a PUBACK means "done".
		c.mu.Lock()
		full := c.inboundWindowFullLocked()
		c.mu.Unlock()
		if full {
			c.host.ReceiveMaxFull(topic)
			return
		}
	}
	// This is where the payload stops being a view of the read buffer and becomes
	// an owned publication, and it is the only place the broker copies it: every
	// consumer below may read it after this connection has read the next packet —
	// the ACL, the interceptors and the hook.EventSink (which a configured
	// Config.PublishCallbackTimeout calls on its own goroutine, so a shared buffer
	// would be a data race and not merely a stale read), the retained store, the
	// offline queue of a persistent session, the cluster spread, and the writer
	// queue plus the QoS 1/2 inflight table of every target connection. Those keep
	// their own copies where they always made one; this one is what makes the
	// reuse of c.readBuf invisible to all of them. A publish that is refused above
	// — an unknown topic alias, a wildcard topic name, a full inbound window — is
	// never copied at all.
	payload := bytes.Clone(v.Payload)
	res := c.host.Publish(c.origin(), hook.PublishEvent{
		ConnectionID: c.connID,
		ClientID:     c.id,
		Topic:        topic,
		Payload:      payload,
		QoS:          v.QoS,
		PacketID:     v.PacketID,
		Retain:       v.Retain,
		Dup:          v.Dup,
		Properties:   wireProps,
	}, props)
	switch v.QoS {
	case 1:
		c.ackPublish(v.QoS, v.PacketID, res)
	case 2:
		if res.Accepted() {
			c.mu.Lock()
			c.inboundQoS2[v.PacketID] = struct{}{}
			c.mu.Unlock()
		}
		c.ackPublish(v.QoS, v.PacketID, res)
	}
}

// ClientID, ConnectionID and DeliverPublish implement routing.Endpoint: the three
// things the routing table needs from a live connection, and nothing else. They are
// exported on an unexported type on purpose — the interface is what gmqx/routing
// names, while the connection state machine stays in the engine.
func (c *Client) ClientID() string { return c.id }

// ConnectionID implements routing.Endpoint.
func (c *Client) ConnectionID() ConnectionID { return c.connID }

// DeliverPublish implements routing.Endpoint by writing (or, when its hand-off
// window has begun, queueing) one PUBLISH with the delivery rules of this
// connection.
func (c *Client) DeliverPublish(topic string, payload []byte, qos byte, retain bool, props mqtt.Properties) error {
	return c.sendPublish(topic, payload, qos, retain, props)
}

func (c *Client) handleSubscribe(m *mqtt.Subscribe) {
	// Subscription Identifiers are accepted: the CONNACK this connection was
	// admitted with advertises Subscription Identifier Available 1 (props.go). A
	// value of 0 is a Protocol Error (§3.8.2.1.2) and ends the connection; the
	// identifiers themselves are stored per subscription and attached to the
	// deliveries that subscription causes (subscription_id.go).
	ids, dropped, ok := mqtt5.NormalizeSubscriptionIDs(m.Properties.SubscriptionIdentifiers)
	if !ok {
		c.host.Metrics().RecordDroppedMessage("subscription_identifier")
		c.protocolError(hook.ReasonProtocolError)
		return
	}
	if dropped > 0 {
		c.host.Metrics().RecordDroppedMessage("subscription_identifier_limit")
	}
	codes := make([]byte, 0, len(m.Subscriptions))
	existed := make([]bool, 0, len(m.Subscriptions))
	for _, req := range m.Subscriptions {
		sub := session.Subscription{
			QoS:               req.QoS,
			NoLocal:           req.NoLocal,
			RetainAsPublished: req.RetainAsPublished,
			RetainHandling:    req.RetainHandling,
			SubscriptionIDs:   ids,
		}
		if _, isShared, wellFormed := topicmatch.ParseSharedFilter(req.Filter); isShared {
			// A "$share/<shareName>/<filter>" subscription, MQTT 5 §4.8.2. The
			// filter must be well formed (MQTT-4.8.2-1, -2) and must not set No
			// Local (MQTT-3.8.3-4): the two refusals are per filter in the SUBACK,
			// so the rest of the packet still applies (MQTT 5 §3.9). MQTT 3.1.1 has
			// no per-filter rejection code for them and closes the connection, like
			// its one failure code does for a malformed filter.
			if !wellFormed || req.NoLocal {
				if c.version != mqtt.Version5 {
					c.protocolError(0)
					return
				}
				c.host.Metrics().RecordDroppedMessage("shared_subscription")
				codes = append(codes, 0x8f)
				existed = append(existed, false)
				continue
			}
		}
		if !topicmatch.ValidFilter(req.Filter) {
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
		code, had := c.host.Subscribe(c, req.Filter, sub)
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
		// No retained message is sent to a shared subscription (§4.8.2: the replay
		// is skipped for the members of a group; later matching publications are
		// delivered normally). A malformed shared filter is already skipped above,
		// because its SUBACK code is not a granted QoS.
		if _, isShared, _ := topicmatch.ParseSharedFilter(req.Filter); isShared {
			continue
		}
		c.host.SendRetained(c, req.Filter, req.QoS, req.RetainAsPublished, ids)
	}
}

func (c *Client) handleUnsubscribe(m *mqtt.Unsubscribe) {
	// MQTT 5 answers with one reason code per filter, all Success: a filter that
	// was not subscribed is a no-op for this broker. MQTT 3.1.1 carries none.
	codes := make([]byte, len(m.Filters))
	for i, filter := range m.Filters {
		c.host.Unsubscribe(c, filter)
		codes[i] = 0
	}
	_ = c.send(&mqtt.Unsuback{
		MessageHeader: c.header(),
		PacketID:      m.PacketID,
		ReasonCodes:   codes,
	})
}

func (c *Client) writeConnack(code byte, assignedClientID string) error {
	ack := &mqtt.Connack{MessageHeader: c.header(), ReasonCode: code}
	if code == 0 {
		ack.SessionPresent = c.sessionPresent
	}
	if c.version == mqtt.Version5 && code == 0 {
		ack.Properties = mqtt5.ConnackProperties(assignedClientID, uint32(c.host.Limits().MaxPacketSize),
			uint16(c.host.Limits().ReceiveMaximum), c.authMethod, c.authData)
	}
	return c.writeMessage(ack)
}

func (c *Client) close(withWill bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	// A persistent session outlives this connection, so the deliveries it still
	// owes its inflight window are not lost with it: they wait in pending from here
	// on and unregister moves them into the session queue (see
	// conn.Handoff and the host's queue limits). A clean session keeps the previous behavior —
	// its state ends with the connection.
	if c.handoff == handoffNone && c.persistentSession() {
		c.handoff = handoffCollecting
	}
	// No packet may be queued from here on: the socket is about to go away and an
	// enqueue must answer "closed" instead of growing a queue nobody drains.
	c.writeClosed.Store(true)
	will := c.will
	if !withWill {
		will = nil
	}
	c.mu.Unlock()
	_ = c.conn.Close()
	if will != nil && c.host != nil {
		// The will is published right away, or scheduled when the client asked
		// for a Will Delay Interval (MQTT 5); see will.go for when the delay is
		// cut short or cancelled.
		c.publishWill(will)
	}
	// The will exchange above still runs on a live connection context; once the
	// connection is gone, every later observer (the disconnect hooks) sees the
	// cancelled context.
	c.StopContext()
	// The writer goroutine is joined last: closing the socket above already ends a
	// write in progress, and this makes close mean "no write of this connection is
	// still running".
	c.StopWriter()
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
func (c *Client) displace() {
	c.mu.Lock()
	c.kicked = true
	c.mu.Unlock()
	c.writeClosed.Store(true)
	if c.version == mqtt.Version5 {
		c.sendDisconnect(hook.ReasonSessionTakenOver)
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

// connackBadAuthMethod is the CONNACK of a CONNECT that named an MQTT 5
// Authentication Method this broker has no exchange for (§3.2.2.2: 0x8C Bad
// authentication method). MQTT 3.1.1 has no Authentication Method at all, so only
// a malformed CONNECT could ask; it is refused like any other failed
// authentication.
func connackBadAuthMethod(version byte) byte {
	if version == mqtt.Version5 {
		return hook.ReasonBadAuthMethod
	}
	return 5
}

// connackProtocolError is the CONNACK of a protocol violation the broker detects
// before the connection is admitted (MQTT 5 §3.2.2.2: 0x82 Protocol Error).
func connackProtocolError(version byte) byte {
	if version == mqtt.Version5 {
		return hook.ReasonProtocolError
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
		return hook.ReasonConnectionRateExceeded
	}
	return 3
}

// ackPublish acknowledges an accepted publish. A retryable or rejected publish
// gets no success ACK: MQTT 3.1.1 has no negative PUBACK, and an optimistic ACK
// would claim durability the broker does not have. MQTT 5 clients get the reason
// code instead.
func (c *Client) ackPublish(qos byte, pid uint16, res hook.PublishResult) {
	if res.Accepted() {
		_ = c.writePublishAck(qos, pid, 0)
		return
	}
	code := res.ReasonCode
	if code == 0 {
		if res.Disposition == hook.PublishRetryable {
			code = hook.ReasonQuotaExceeded
		} else {
			code = hook.ReasonUnspecified
		}
	}
	if c.version == mqtt.Version5 {
		_ = c.writePublishAck(qos, pid, code)
	}
}

// writePublishAck writes the PUBACK of a QoS 1 publish or the PUBREC of a QoS 2
// one. The reason code is only encoded on MQTT 5.
func (c *Client) writePublishAck(qos byte, pid uint16, reason byte) error {
	if reason == 0 {
		// The acknowledgement every accepted publish gets: four bytes, no typed
		// packet and no allocation (see enqueueAck).
		if qos == 2 {
			return c.enqueueAck(outboundPubrec, pid)
		}
		return c.enqueueAck(outboundPuback, pid)
	}
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

// RandomID returns a short random identifier, used for a node id and for the
// client id the broker assigns to an empty CONNECT.
func RandomID() string {
	buf := make([]byte, 8)
	_, _ = rand.Read(buf)
	return "gmqx-" + hex.EncodeToString(buf)
}
