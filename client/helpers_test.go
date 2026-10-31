package client_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/mqtt"
)

// startBroker starts an in-process gmqx broker on an ephemeral loopback port and
// stops it when the test ends.
func startBroker(t *testing.T, cfg gmqx.Config) *gmqx.Broker {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	b, err := gmqx.New(cfg)
	if err != nil {
		t.Fatalf("gmqx.New: %v", err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("broker.Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})
	return b
}

// testContext returns a context bounded by the test.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// dialClient dials a client and closes it when the test ends.
func dialClient(t *testing.T, cfg client.Config) *client.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, cfg)
	if err != nil {
		t.Fatalf("client.Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// recorder collects OnMessage deliveries, copying everything the callback hands
// over so the test may inspect the message after the callback returned.
type recorder struct {
	ch chan *client.Message
}

func newRecorder(size int) *recorder {
	return &recorder{ch: make(chan *client.Message, size)}
}

func (r *recorder) OnMessage(m *client.Message) {
	cp := *m
	cp.Payload = append([]byte(nil), m.Payload...)
	cp.Properties.UserProperties = append([]mqtt.UserProperty(nil), m.Properties.UserProperties...)
	cp.Properties.SubscriptionIdentifiers = append([]uint32(nil), m.Properties.SubscriptionIdentifiers...)
	cp.Properties.CorrelationData = append([]byte(nil), m.Properties.CorrelationData...)
	r.ch <- &cp
}

// next returns the next delivery or fails the test.
func (r *recorder) next(t *testing.T) *client.Message {
	t.Helper()
	select {
	case m := <-r.ch:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an inbound message")
		return nil
	}
}

// none fails the test when a message arrives within d.
func (r *recorder) none(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case m := <-r.ch:
		t.Fatalf("unexpected delivery on %q: %q", m.Topic, m.Payload)
	case <-time.After(d):
	}
}

// waitFor polls cond until it is true or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// recvError waits for an error on ch.
func recvError(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// recvBool waits for a bool on ch.
func recvBool(t *testing.T, ch <-chan bool, what string) bool {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return false
	}
}

// recordingSink is a gmqx.EventSink that records what the broker saw. It never
// blocks, so it can count the publishes that reached the broker without changing
// the timing of the acknowledgements.
type recordingSink struct {
	mu          sync.Mutex
	publishes   []gmqx.PublishEvent
	connects    int
	disconnects int
}

func (s *recordingSink) OnConnect(context.Context, gmqx.ConnectInfo) {
	s.mu.Lock()
	s.connects++
	s.mu.Unlock()
}

func (s *recordingSink) OnDisconnect(context.Context, string, error) {
	s.mu.Lock()
	s.disconnects++
	s.mu.Unlock()
}

func (s *recordingSink) OnConnectionLost(context.Context, string, error) {}

func (s *recordingSink) OnPublish(_ context.Context, ev gmqx.PublishEvent) gmqx.PublishResult {
	s.mu.Lock()
	s.publishes = append(s.publishes, ev)
	s.mu.Unlock()
	return gmqx.PublishResult{Disposition: gmqx.PublishAccepted}
}

func (s *recordingSink) OnPing(context.Context, string) {}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.publishes)
}

func (s *recordingSink) event(i int) gmqx.PublishEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.publishes[i]
}

// testProxy is a TCP proxy between a client and the real broker. The client is
// configured with proxy.DialContext, so every connection and every reconnect goes
// through it. It can kill connections (a network failure), drop PINGRESP packets
// (a server that stopped answering), hold PUBACKs (a server that is slow to
// acknowledge) and duplicate a QoS 2 PUBLISH (a retransmission on the wire). It
// is how the tests observe the client's behaviour end to end without replacing
// the broker with a mock.
type testProxy struct {
	t      *testing.T
	ln     net.Listener
	target string

	mu    sync.Mutex
	pairs map[*proxyPair]struct{}
	held  []heldFrame

	closed    atomic.Bool
	dials     atomic.Int64
	pings     atomic.Int64
	publishes atomic.Int64
	dups      atomic.Int64
	pubrels   atomic.Int64

	dropPingresp  atomic.Bool
	holdPuback    atomic.Bool
	holdPubcomp   atomic.Bool
	duplicateQoS2 atomic.Bool
}

// proxyPair is one accepted client connection and its upstream counterpart.
type proxyPair struct {
	client net.Conn
	server net.Conn
	once   sync.Once
}

// heldFrame is one server packet the proxy kept instead of forwarding it yet.
type heldFrame struct {
	dst   net.Conn
	frame []byte
}

func newTestProxy(t *testing.T, target string) *testProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &testProxy{t: t, ln: ln, target: target, pairs: make(map[*proxyPair]struct{})}
	go p.acceptLoop()
	t.Cleanup(p.Close)
	return p
}

func (p *testProxy) Addr() string { return p.ln.Addr().String() }

// DialContext is a client.Config.Dialer that dials the proxy.
func (p *testProxy) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	p.dials.Add(1)
	dialer := &net.Dialer{}
	return dialer.DialContext(ctx, network, p.Addr())
}

// Pings reports how many PINGREQ packets the proxy forwarded to the broker.
func (p *testProxy) Pings() int64 { return p.pings.Load() }

// Publishes reports how many PUBLISH packets the proxy forwarded to the broker.
func (p *testProxy) Publishes() int64 { return p.publishes.Load() }

// Dups reports how many of those PUBLISH packets carried the DUP flag.
func (p *testProxy) Dups() int64 { return p.dups.Load() }

// Pubrels reports how many PUBREL packets the proxy forwarded to the broker.
func (p *testProxy) Pubrels() int64 { return p.pubrels.Load() }

// Dials reports how many connections the client attempted through the proxy.
func (p *testProxy) Dials() int64 { return p.dials.Load() }

func (p *testProxy) acceptLoop() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		s, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		pair := &proxyPair{client: c, server: s}
		p.mu.Lock()
		p.pairs[pair] = struct{}{}
		p.mu.Unlock()
		go p.pipe(pair, s, c, true)
		go p.pipe(pair, c, s, false)
	}
}

func (p *testProxy) pipe(pair *proxyPair, src, dst net.Conn, serverToClient bool) {
	defer func() {
		pair.once.Do(func() {
			_ = pair.client.Close()
			_ = pair.server.Close()
		})
		p.mu.Lock()
		delete(p.pairs, pair)
		p.mu.Unlock()
	}()
	r := bufio.NewReader(src)
	for {
		header, err := r.ReadByte()
		if err != nil {
			return
		}
		remaining, err := readRemainingLength(r)
		if err != nil {
			return
		}
		body := make([]byte, remaining)
		if _, err := io.ReadFull(r, body); err != nil {
			return
		}
		kind := header >> 4
		if !serverToClient && kind == mqtt.TypePublish {
			p.publishes.Add(1)
			if header&0x08 != 0 {
				p.dups.Add(1)
			}
		}
		if !serverToClient && kind == mqtt.TypePubrel {
			p.pubrels.Add(1)
		}
		if !serverToClient && kind == mqtt.TypePingreq {
			p.pings.Add(1)
		}
		frame := appendFrame(nil, header, body)
		if serverToClient && kind == mqtt.TypePingresp && p.dropPingresp.Load() {
			continue
		}
		if serverToClient && ((kind == mqtt.TypePuback && p.holdPuback.Load()) ||
			(kind == mqtt.TypePubcomp && p.holdPubcomp.Load())) {
			p.mu.Lock()
			p.held = append(p.held, heldFrame{dst: dst, frame: frame})
			p.mu.Unlock()
			continue
		}
		if _, err := dst.Write(frame); err != nil {
			return
		}
		if serverToClient && kind == mqtt.TypePublish && (header>>1)&0x03 == 2 && p.duplicateQoS2.CompareAndSwap(false, true) {
			dup := appendFrame(nil, header|0x08, body)
			if _, err := dst.Write(dup); err != nil {
				return
			}
		}
	}
}

// releaseHeld forwards the packets the proxy held, in arrival order, and stops
// holding new ones when holdPuback is already false.
func (p *testProxy) releaseHeld() {
	p.mu.Lock()
	held := p.held
	p.held = nil
	p.mu.Unlock()
	for _, h := range held {
		_, _ = h.dst.Write(h.frame)
	}
}

// killAll closes every proxied connection abruptly, with no DISCONNECT: the
// broker sees the client socket die and the client sees the server socket die.
func (p *testProxy) killAll() {
	p.mu.Lock()
	pairs := make([]*proxyPair, 0, len(p.pairs))
	for pair := range p.pairs {
		pairs = append(pairs, pair)
	}
	p.mu.Unlock()
	for _, pair := range pairs {
		pair.once.Do(func() {
			_ = pair.client.Close()
			_ = pair.server.Close()
		})
	}
}

// Close stops the listener and kills every live pair.
func (p *testProxy) Close() {
	if p.closed.Swap(true) {
		return
	}
	_ = p.ln.Close()
	p.killAll()
}

func readRemainingLength(r *bufio.Reader) (int, error) {
	value, shift := 0, 0
	for i := 0; i < 4; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		value |= int(b&0x7f) << shift
		if b&0x80 == 0 {
			return value, nil
		}
		shift += 7
	}
	return 0, errors.New("proxy: malformed remaining length")
}

func appendFrame(dst []byte, header byte, body []byte) []byte {
	dst = append(dst, header)
	dst = mqtt.AppendRemainingLength(dst, len(body))
	return append(dst, body...)
}
