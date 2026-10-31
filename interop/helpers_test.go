package interop

// Shared helpers of the interoperability matrix.
//
// The matrix has three directions and one set of primitives: an in-process
// broker on an ephemeral port, a bounded wait, a delivery recorder that both
// client libraries can feed, and a way to kill a client's socket without closing
// the client (which would send DISCONNECT and cancel the will).

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/mqtt"
)

const (
	// connectTimeout bounds one client handshake: the TCP dial plus the MQTT
	// CONNECT/CONNACK exchange.
	connectTimeout = 10 * time.Second
	// deliveryTimeout bounds the wait for a message that must arrive.
	deliveryTimeout = 10 * time.Second
	// quietWindow is how long a negative assertion listens. A will, a duplicate
	// delivery or an extra queued message would be sent immediately, so this
	// window is only paid in wall time when the behaviour is wrong.
	quietWindow = 500 * time.Millisecond
	// stopTimeout bounds broker shutdown.
	stopTimeout = 5 * time.Second
)

// startBroker starts an in-process broker on an ephemeral port and stops it when
// the test ends. A zero Config listens on loopback; a direction-B case passes
// ":0" so a container can reach the broker through host.docker.internal.
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
		t.Fatalf("gmqx broker Start on %q: %v", cfg.Addr, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()
		_ = b.Stop(ctx)
	})
	return b
}

// brokerPort returns the port half of a broker address. Direction B needs it
// because the reference clients reach the host through host.docker.internal,
// which is a host name and not the listen address.
func brokerPort(t *testing.T, addr string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("broker address %q: %v", addr, err)
	}
	return port
}

// waitFor polls cond until it holds or the deadline passes. Every readiness
// check of the matrix goes through it, so no case ever waits without a bound.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// delivery is one inbound application message, copied out of the client
// callback so that the assertion can run after the callback returned.
type delivery struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
	dup     bool
}

// sink collects deliveries on a buffered channel. The buffer is larger than any
// case's message count, so a callback never blocks on it.
type sink struct {
	ch chan delivery
}

func newSink() *sink { return &sink{ch: make(chan delivery, 16)} }

func (s *sink) push(d delivery) {
	d.payload = append([]byte(nil), d.payload...)
	s.ch <- d
}

// next returns the next delivery or fails the test after deliveryTimeout.
func (s *sink) next(t *testing.T, what string) delivery {
	t.Helper()
	select {
	case d := <-s.ch:
		return d
	case <-time.After(deliveryTimeout):
		t.Fatalf("timed out after %s waiting for %s", deliveryTimeout, what)
		return delivery{}
	}
}

// expectNone fails the test when a message arrives within window.
func (s *sink) expectNone(t *testing.T, what string, window time.Duration) {
	t.Helper()
	select {
	case d := <-s.ch:
		t.Fatalf("%s: unexpected delivery on %q (%d bytes, qos %d, retain %v)",
			what, d.topic, len(d.payload), d.qos, d.retain)
	case <-time.After(window):
	}
}

// sinkHandler adapts a sink to the gmqx client callback.
func sinkHandler(s *sink) func(*client.Message) {
	return func(m *client.Message) {
		s.push(delivery{
			topic:   m.Topic,
			payload: m.Payload,
			qos:     m.QoS,
			retain:  m.Retain,
			dup:     m.Duplicate,
		})
	}
}

// connTap captures the transport connection a client opens, so a test can kill
// the socket instead of closing the client. Both libraries can be fed with it:
// client.Config.Dialer for the gmqx client and
// ClientOptions.SetCustomOpenConnectionFn for paho (see paho_test.go).
type connTap struct {
	mu     sync.Mutex
	conn   net.Conn
	dialer net.Dialer
}

// dial is the gmqx client's Dialer shape.
func (tap *connTap) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := tap.dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	tap.mu.Lock()
	tap.conn = conn
	tap.mu.Unlock()
	return conn, nil
}

// kill aborts the captured connection the way a dead network does: the socket
// is reset (SO_LINGER 0) instead of closed with a FIN, and no DISCONNECT is
// sent, so the server must publish the will.
func (tap *connTap) kill(t *testing.T) {
	t.Helper()
	tap.mu.Lock()
	conn := tap.conn
	tap.mu.Unlock()
	if conn == nil {
		t.Fatal("no connection was captured by the dialer")
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("kill the connection: %v", err)
	}
}

// dialGMQX connects the gmqx client and closes it when the test ends.
func dialGMQX(t *testing.T, cfg client.Config) *client.Client {
	t.Helper()
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = connectTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	c, err := client.Dial(ctx, cfg)
	if err != nil {
		t.Fatalf("gmqx client.Dial to %s: %v", cfg.Addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// subscribeGMQX subscribes one filter and asserts the granted QoS.
func subscribeGMQX(t *testing.T, c *client.Client, filter string, qos byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	codes, err := c.Subscribe(ctx, client.Subscription{Filter: filter, QoS: qos})
	if err != nil {
		t.Fatalf("gmqx client Subscribe %q: %v", filter, err)
	}
	if len(codes) != 1 || codes[0] != qos {
		t.Fatalf("gmqx client SUBACK for %q = %v, want one code %d", filter, codes, qos)
	}
}

// publishGMQX publishes one message and waits for the exchange to complete.
func publishGMQX(t *testing.T, c *client.Client, topic string, payload []byte, qos byte, retain bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
	defer cancel()
	if err := c.Publish(ctx, topic, payload, qos, retain); err != nil {
		t.Fatalf("gmqx client Publish %q at QoS %d: %v", topic, qos, err)
	}
}

// cleanPayload returns a payload of n bytes with a position-dependent pattern,
// so a truncated or reordered delivery cannot pass a later comparison.
func cleanPayload(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte('a' + i%26)
	}
	return out
}

// describe renders the first bytes of a payload for an assertion message.
func describe(p []byte) string {
	const max = 32
	if len(p) <= max {
		return fmt.Sprintf("%q", p)
	}
	return fmt.Sprintf("%q... (%d bytes)", p[:max], len(p))
}

// mqttVersionName is the label of a protocol version in a subtest name.
func mqttVersionName(version byte) string {
	if version == mqtt.Version5 {
		return "MQTT5"
	}
	return "MQTT3.1.1"
}
