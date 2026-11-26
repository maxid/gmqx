package engine

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/maxid/glimit"
)

func mustGate(t *testing.T, policies ...glimit.Policy) glimit.Limiter {
	t.Helper()
	gate, err := glimit.Compile(glimit.Spec{Policies: policies})
	if err != nil {
		t.Fatalf("glimit.Compile: %v", err)
	}
	return gate
}

// TestLimiterConnectQuota pins the protocol-native refusal of a CONNECT over the
// connection gauge: MQTT 3.1.1 gets the closest true return code, MQTT 5 gets
// 0x97 Quota exceeded, and the metric keeps the historical max_connections word.
func TestLimiterConnectQuota(t *testing.T) {
	metrics := &dropRecorder{}
	b := startBroker(t, Config{
		Metrics: metrics,
		Limiter: mustGate(t, glimit.Policy{Kind: glimit.KindConnections, Scope: glimit.ScopeGlobal, Max: 1}),
	})
	first := dialTestClient(t, b.Addr(), "first")
	defer func() { _ = first.conn.Close() }()

	if code := dialRawConnack(t, b.Addr(), "second"); code != 5 {
		t.Fatalf("3.1.1 CONNACK = %d, want 5 (server unavailable)", code)
	}
	conn, _, code := dialV5(t, b.Addr(), "third", nil)
	defer func() { _ = conn.Close() }()
	if code != 0x97 {
		t.Fatalf("MQTT 5 CONNACK = %#x, want 0x97 (quota exceeded)", code)
	}
	if got := metrics.count("max_connections"); got != 2 {
		t.Fatalf("max_connections drops = %d, want 2", got)
	}
}

// TestLimiterConnectRate pins the accept-rate refusal, which reads as the rate
// word rather than the quota word.
func TestLimiterConnectRate(t *testing.T) {
	metrics := &dropRecorder{}
	b := startBroker(t, Config{
		Metrics: metrics,
		Limiter: mustGate(t, glimit.Policy{Kind: glimit.KindRequests, Scope: glimit.ScopeGlobal, Rate: 1, Burst: 1}),
	})
	first := dialTestClient(t, b.Addr(), "first")
	defer func() { _ = first.conn.Close() }()

	if code := dialRawConnack(t, b.Addr(), "second"); code != 3 {
		t.Fatalf("CONNACK = %d, want 3 (server unavailable) for a refused accept rate", code)
	}
	if got := metrics.count("connection_rate_limit"); got != 1 {
		t.Fatalf("connection_rate_limit drops = %d, want 1", got)
	}
}

// TestLimiterCreditReleased proves the connection gauge is returned when a client
// goes away, so a refused peer is not locked out for the life of the process.
func TestLimiterCreditReleased(t *testing.T) {
	b := startBroker(t, Config{
		Limiter: mustGate(t, glimit.Policy{Kind: glimit.KindConnections, Scope: glimit.ScopeGlobal, Max: 1}),
	})
	first := dialTestClient(t, b.Addr(), "first")
	_ = first.conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, _, code := dialV5(t, b.Addr(), "second", nil)
		if code == 0 {
			_ = conn.Close()
			return
		}
		_ = conn.Close()
		if time.Now().After(deadline) {
			t.Fatalf("the released connection credit was never reusable (last CONNACK %#x)", code)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestLimiterFrameRateDisconnects pins the packet-rate refusal: the connection is
// a flood, so it ends, and MQTT 5 would have seen a DISCONNECT first.
func TestLimiterFrameRateDisconnects(t *testing.T) {
	metrics := &dropRecorder{}
	b := startBroker(t, Config{
		Metrics: metrics,
		Limiter: mustGate(t, glimit.Policy{Kind: glimit.KindRequests, Scope: glimit.ScopeSession, Rate: 2, Burst: 2}),
	})
	c := dialTestClient(t, b.Addr(), "chatty")
	defer func() { _ = c.conn.Close() }()

	for i := 0; i < 6; i++ {
		c.writePacket(0xC0, nil) // PINGREQ
	}
	c.armDeadline(2 * time.Second)
	for {
		if _, _, err := c.readPacket(); err != nil {
			break
		}
	}
	if got := metrics.count("rate_limited"); got == 0 {
		t.Fatal("the refused packet was not counted as rate_limited")
	}
}

// TestLimiterPublishBytesRefused pins the byte budget at the message boundary: the
// publication is refused the way an exhausted receive maximum refuses one, and the
// stream survives because an MQTT connection cannot survive a hole.
func TestLimiterPublishBytesRefused(t *testing.T) {
	metrics := &dropRecorder{}
	b := startBroker(t, Config{
		Metrics: metrics,
		Limiter: mustGate(t, glimit.Policy{Kind: glimit.KindBytes, Scope: glimit.ScopeSession, Rate: 1, Burst: 1}),
	})
	c := dialTestClient(t, b.Addr(), "publisher")
	defer func() { _ = c.conn.Close() }()

	c.writePacket(0x32, publishBody("t", []byte("0123456789"), 1)) // QoS 1
	c.armDeadline(150 * time.Millisecond)
	if _, _, err := c.readPacket(); err == nil {
		t.Fatal("a refused QoS 1 publication must not be acknowledged")
	} else if !errors.Is(err, io.EOF) && !isTimeout(err) {
		t.Fatalf("read after refusal = %v, want a timeout or a closed connection", err)
	}

	// The stream is still usable: the refusal answered one packet, not the socket.
	c.armDeadline(2 * time.Second)
	c.writePacket(0xC0, nil) // PINGREQ
	for {
		header, _, err := c.readPacket()
		if err != nil {
			t.Fatalf("the connection must survive a refused publication: %v", err)
		}
		if header>>4 == 13 { // PINGRESP
			break
		}
	}
	if got := metrics.count("byte_limited"); got == 0 {
		t.Fatal("the refused publication was not counted as byte_limited")
	}
}

// TestLimiterPublishBytesMQTT5ReasonCode pins PUBREC 0x97, the negative
// acknowledgement MQTT 5 has for a publication the broker will not accept.
func TestLimiterPublishBytesMQTT5ReasonCode(t *testing.T) {
	b := startBroker(t, Config{
		Limiter: mustGate(t, glimit.Policy{Kind: glimit.KindBytes, Scope: glimit.ScopeSession, Rate: 1, Burst: 1}),
	})
	conn, r, code := dialV5(t, b.Addr(), "publisher5", nil)
	defer func() { _ = conn.Close() }()
	if code != 0 {
		t.Fatalf("CONNACK = %#x, want success", code)
	}
	// MQTT 5 PUBLISH: topic, packet id, an empty property block, payload.
	body := appendString(nil, "t")
	body = append(body, 0x00, 0x07, 0x00)
	body = append(body, []byte("0123456789")...)
	frame := appendRemainingLength([]byte{0x34}, len(body)) // QoS 2 PUBLISH
	frame = append(frame, body...)
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write publish: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	header, err := r.ReadByte()
	if err != nil {
		t.Fatalf("read pubrec: %v", err)
	}
	if header>>4 != 5 {
		n, _ := readRemainingLength(r)
		body := make([]byte, n)
		_, _ = io.ReadFull(r, body)
		t.Fatalf("packet type = %d body %v, want PUBREC (5)", header>>4, body)
	}
	n, err := readRemainingLength(r)
	if err != nil {
		t.Fatalf("read pubrec length: %v", err)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		t.Fatalf("read pubrec body: %v", err)
	}
	if len(payload) < 3 || payload[2] != 0x97 {
		t.Fatalf("PUBREC reason = %v, want 0x97 (quota exceeded)", payload)
	}
}

func publishBody(topic string, payload []byte, pid uint16) []byte {
	var b []byte
	b = appendString(b, topic)
	if pid != 0 {
		b = append(b, byte(pid>>8), byte(pid))
	}
	return append(b, payload...)
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
