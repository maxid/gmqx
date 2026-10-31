package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// Client is one MQTT client session. It is safe for concurrent use by multiple
// goroutines; a Client created by Dial must be closed with Close.
type Client struct {
	cfg  Config
	dial func(ctx context.Context, network, addr string) (net.Conn, error)

	// ctx is the lifetime context of the client: Close cancels it, which aborts a
	// dial, a TLS handshake, a reconnect backoff and the establishment of a new
	// connection.
	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
	closedCh  chan struct{}
	closed    atomic.Bool

	// connErr carries the failure of the current connection from its reader to
	// the supervisor. It has room for one error, so a failing reader never blocks.
	connErr chan error
	// deliverq is the bounded inbound queue of the delivery goroutine; OnMessage
	// is the only consumer. deliverStop ends that goroutine on a terminal
	// disconnect or Close.
	deliverq    chan *Message
	deliverStop chan struct{}
	deliverOnce sync.Once
	deliverDone chan struct{}
	sessionDone chan struct{}

	mu             sync.Mutex
	conn           *connState
	handshakeConn  net.Conn // the socket of an opening handshake, nil otherwise
	connected      bool
	sessionPresent bool
	clientID       string
	server         ServerInfo
	inflight       map[uint16]*outgoing
	order          []uint16
	inflightN      int
	slotWake       chan struct{}
	requests       map[uint16]*request
	inboundQoS2    map[uint16]struct{}
	aliases        map[uint16]string
	subs           map[string]Subscription
	subOrder       []string
	nextPID        uint16
	retryWake      chan struct{}
}

// newClient builds a Client from an already defaulted and validated Config.
func newClient(cfg Config) *Client {
	c := &Client{
		cfg:         cfg,
		closedCh:    make(chan struct{}),
		connErr:     make(chan error, 1),
		deliverq:    make(chan *Message, cfg.OutboundQueueSize),
		deliverStop: make(chan struct{}),
		deliverDone: make(chan struct{}),
		sessionDone: make(chan struct{}),
		slotWake:    make(chan struct{}),
		inflight:    make(map[uint16]*outgoing),
		requests:    make(map[uint16]*request),
		inboundQoS2: make(map[uint16]struct{}),
		aliases:     make(map[uint16]string),
		subs:        make(map[string]Subscription),
		retryWake:   make(chan struct{}, 1),
		clientID:    cfg.ClientID,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.dial = cfg.Dialer
	if c.dial == nil {
		dialer := &net.Dialer{}
		c.dial = dialer.DialContext
	}
	return c
}

// Dial opens a connection, completes the MQTT opening handshake and returns the
// connected client. ctx bounds the opening handshake in addition to
// Config.ConnectTimeout; it is not the lifetime of the client (Close is), so a
// deadline on ctx that expires after Dial returns does not end the connection.
//
// OnConnect, when configured, runs for the first connection before Dial returns.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	c := newClient(cfg)
	cs, _, err := c.establish(ctx)
	if err != nil {
		c.cancel()
		return nil, err
	}
	go c.deliverLoop()
	go c.run(cs)
	return c, nil
}

// Close sends DISCONNECT (when a connection is still up), closes the transport
// and joins every goroutine of the client: the reader, the writer, the delivery
// goroutine and the supervisor, including a reconnect attempt in progress. It
// never reconnects and is safe to call more than once; later calls return nil.
//
// Close must not be called from an OnMessage, OnConnect or OnDisconnect callback:
// those callbacks run on goroutines Close joins.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closedCh)
		c.cancel()
		c.mu.Lock()
		cs, hc := c.conn, c.handshakeConn
		c.mu.Unlock()
		if hc != nil {
			// A handshake in progress is aborted by closing its socket; the
			// supervisor sees the failure and exits because the client is closed.
			_ = hc.Close()
		}
		if cs != nil {
			// Best effort: the DISCONNECT tells the server not to publish the will.
			// A dead socket just makes the write fail.
			c.sendDisconnect(cs)
			cs.shutdown()
		}
		<-c.sessionDone
		c.stopDelivery()
		<-c.deliverDone
		c.failInflight(ErrClosed)
		c.failRequests(ErrClosed)
	})
	return nil
}

// Connected reports whether the client currently has a live MQTT connection.
// While a reconnect is in progress it is false.
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// SessionPresent reports the Session Present flag of the most recent CONNACK. It
// stays at its last value while the client is disconnected.
func (c *Client) SessionPresent() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionPresent
}

// ClientID reports the client identifier in use. It is Config.ClientID, or the
// identifier the server assigned in its CONNACK when Config.ClientID was empty
// (MQTT 5).
func (c *Client) ClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientID
}

// ServerInfo reports what the most recent CONNACK advertised: the assigned client
// id, the server's Receive Maximum and Maximum Packet Size, its capability flags
// and the session-present flag. It is a copy and stays valid after reconnects.
func (c *Client) ServerInfo() ServerInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.server
}

// header is the protocol level every packet of this client carries.
func (c *Client) header() mqtt.MessageHeader {
	return mqtt.MessageHeader{Level: c.cfg.Version}
}

// run is the supervisor goroutine: it waits for the active connection to fail,
// tears it down, reports the loss and reconnects while the policy allows it.
func (c *Client) run(cs *connState) {
	defer close(c.sessionDone)
	for {
		select {
		case err := <-c.connErr:
			c.teardown(cs, err)
			if c.closed.Load() {
				c.stopDelivery()
				return
			}
			c.notifyDisconnect(err)
			if !c.cfg.Reconnect.Enabled || errors.Is(err, errServerDisconnect) {
				// A server DISCONNECT is an orderly end, not a transport failure:
				// reconnecting after a session-taken-over DISCONNECT would fight
				// the connection that displaced this one.
				c.failInflight(err)
				c.stopDelivery()
				return
			}
			next, rerr := c.reconnect(err)
			if next == nil {
				c.failInflight(rerr)
				c.stopDelivery()
				return
			}
			cs = next
		case <-c.closedCh:
			c.teardown(cs, ErrClosed)
			c.stopDelivery()
			return
		}
	}
}

// teardown ends one connection and waits for its reader and writer goroutines.
// It fails the requests whose acknowledgement can no longer arrive (a SUBSCRIBE
// or UNSUBSCRIBE is not replayed across a reconnect); QoS 1/2 publishes stay in
// the inflight table when reconnection is enabled, because they are replayed.
func (c *Client) teardown(cs *connState, err error) {
	cs.shutdown()
	<-cs.writerDone
	<-cs.readerDone

	c.mu.Lock()
	if c.conn == cs {
		c.conn = nil
	}
	c.connected = false
	c.failRequestsLocked(err)
	c.broadcastSlotsLocked()
	if !c.cfg.Reconnect.Enabled || c.closed.Load() {
		c.failInflightLocked(err)
	}
	c.mu.Unlock()
	c.wakeRetry()
}

// notifyConnect reports a successful CONNECT. It is never called while the client
// holds a lock.
func (c *Client) notifyConnect(sessionPresent bool) {
	if fn := c.cfg.OnConnect; fn != nil {
		fn(sessionPresent)
	}
}

// notifyDisconnect reports an unexpected end of an established connection. It is
// never called while the client holds a lock.
func (c *Client) notifyDisconnect(err error) {
	if fn := c.cfg.OnDisconnect; fn != nil {
		fn(err)
	}
}

// stopDelivery ends the delivery goroutine once.
func (c *Client) stopDelivery() {
	c.deliverOnce.Do(func() { close(c.deliverStop) })
}

// deliverLoop invokes OnMessage for every queued inbound message. It is the only
// goroutine that runs the callback, so deliveries stay in order and never run
// while a client method holds a lock.
func (c *Client) deliverLoop() {
	defer close(c.deliverDone)
	for {
		select {
		case <-c.deliverStop:
			return
		case <-c.closedCh:
			return
		case msg := <-c.deliverq:
			if fn := c.cfg.OnMessage; fn != nil {
				fn(msg)
			}
		}
	}
}

// deliver queues one inbound message for OnMessage. It blocks while the delivery
// queue is full (that is the documented backpressure of a slow callback) and
// reports false when the connection or the client ended first.
func (c *Client) deliver(cs *connState, msg *Message) bool {
	if c.cfg.OnMessage == nil {
		return true
	}
	select {
	case c.deliverq <- msg:
		return true
	case <-cs.stop:
		return false
	case <-c.deliverStop:
		return false
	case <-c.closedCh:
		return false
	}
}

// send queues one caller packet on the current connection. It reports
// ErrNotConnected when no connection is up; a full outbound queue blocks until
// the writer drains it, the caller's context is done or the client closes.
func (c *Client) send(ctx context.Context, m mqtt.Message) error {
	c.mu.Lock()
	cs, connected := c.conn, c.connected
	c.mu.Unlock()
	if cs == nil || !connected {
		return ErrNotConnected
	}
	return c.sendOn(ctx, cs, m)
}

// sendOn queues one packet on a specific connection, after checking the server's
// Maximum Packet Size.
func (c *Client) sendOn(ctx context.Context, cs *connState, m mqtt.Message) error {
	if err := c.checkPacketSize(m); err != nil {
		return err
	}
	select {
	case cs.outq <- m:
		return nil
	case <-cs.stop:
		return ErrDisconnected
	case <-c.closedCh:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sendControl queues a protocol packet the reader produces (an acknowledgement,
// a retransmission, a restored SUBSCRIBE). It has no caller context: the packet
// must be sent or the connection must fail, so it only gives up when the
// connection is gone.
func (c *Client) sendControl(cs *connState, m mqtt.Message) error {
	if err := c.checkPacketSize(m); err != nil {
		return err
	}
	select {
	case cs.outq <- m:
		return nil
	case <-cs.stop:
		return ErrDisconnected
	case <-c.closedCh:
		return ErrClosed
	}
}

// countingWriter counts the bytes written to it without keeping them.
type countingWriter int

func (w *countingWriter) Write(p []byte) (int, error) {
	*w += countingWriter(len(p))
	return len(p), nil
}

// encodedPacketSize is the number of bytes m occupies on the wire, fixed header
// and remaining-length field included: the size the server's Maximum Packet Size
// applies to.
func encodedPacketSize(m mqtt.Message) (int, error) {
	var n countingWriter
	if err := mqtt.Encode(&n, m); err != nil {
		return 0, err
	}
	return int(n), nil
}

// checkPacketSize refuses a packet larger than the Maximum Packet Size the server
// advertised. The packet is not enqueued: the caller gets ErrPacketTooLarge, so
// the error is visible instead of the packet being silently dropped. A server
// that advertised no limit costs nothing, because the size is only measured when
// a limit exists.
func (c *Client) checkPacketSize(m mqtt.Message) error {
	c.mu.Lock()
	limit := c.server.MaximumPacketSize
	c.mu.Unlock()
	if limit == 0 || m == nil {
		return nil
	}
	size, err := encodedPacketSize(m)
	if err != nil {
		// The writer's encoder will report the real problem when the packet is
		// written; measuring must not turn an encoding error into a size error.
		return nil
	}
	if uint32(size) <= limit {
		return nil
	}
	return errPacketTooLarge(size, limit)
}

func errPacketTooLarge(size int, limit uint32) error {
	return fmt.Errorf("%w: packet of %d bytes exceeds the server maximum packet size %d", ErrPacketTooLarge, size, limit)
}

// wakeRetry nudges the writer to recompute its retransmission deadline.
func (c *Client) wakeRetry() {
	select {
	case c.retryWake <- struct{}{}:
	default:
	}
}

// retryDelay is the wait before retransmission number attempt (1-based): the base
// interval doubled for every earlier attempt, capped so a long-lived exchange
// cannot overflow a timer.
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
