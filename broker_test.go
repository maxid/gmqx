package gmqx

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

type testClient struct {
	conn net.Conn
	r    *bufio.Reader
}

func dialTestClient(t *testing.T, addr, clientID string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	c := &testClient{conn: conn, r: bufio.NewReader(conn)}
	c.writePacket(0x10, connectPayload(clientID))
	header, payload, err := c.readPacket()
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if header>>4 != 2 || len(payload) != 2 || payload[1] != 0 {
		t.Fatalf("unexpected connack: %x %v", header, payload)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return c
}

func (c *testClient) writePacket(header byte, body []byte) {
	buf := []byte{header}
	buf = appendRemainingLength(buf, len(body))
	buf = append(buf, body...)
	_, _ = c.conn.Write(buf)
}

func (c *testClient) readPacket() (byte, []byte, error) {
	header, err := c.r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, err := readRemainingLength(c.r)
	if err != nil {
		return 0, nil, err
	}
	body := make([]byte, n)
	if _, err := c.r.Read(body); err != nil {
		return 0, nil, err
	}
	return header, body, nil
}

func connectPayload(clientID string) []byte {
	var b []byte
	b = appendString(b, "MQTT")
	b = append(b, 4, 0x02, 0, 30) // level, clean session, keepalive 30
	b = appendString(b, clientID)
	return b
}

func startBroker(t *testing.T, cfg Config) *Broker {
	t.Helper()
	cfg.Addr = "127.0.0.1:0"
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	return b
}

func TestConnectSubscribePublish(t *testing.T) {
	b := startBroker(t, Config{})
	sub := dialTestClient(t, b.Addr(), "sub-1")
	// SUBSCRIBE sensors/+/temp qos0, packet id 1
	body := []byte{0, 1}
	body = appendString(body, "sensors/+/temp")
	body = append(body, 0)
	sub.writePacket(0x82, body)
	if h, p, err := sub.readPacket(); err != nil || h>>4 != 9 || len(p) < 3 || p[2] != 0 {
		t.Fatalf("suback: h=%x p=%v err=%v", h, p, err)
	}
	pub := dialTestClient(t, b.Addr(), "pub-1")
	pubBody := appendString(nil, "sensors/a/temp")
	pubBody = append(pubBody, []byte("42")...)
	pub.writePacket(0x30, pubBody)

	h, p, err := sub.readPacket()
	if err != nil {
		t.Fatalf("read publish: %v", err)
	}
	if h>>4 != 3 {
		t.Fatalf("expected PUBLISH, got type %d", h>>4)
	}
	topic, rest, err := readString(p)
	if err != nil || topic != "sensors/a/temp" || string(rest) != "42" {
		t.Fatalf("unexpected publish: topic=%q payload=%q err=%v", topic, rest, err)
	}
}

func TestRetainedMessage(t *testing.T) {
	b := startBroker(t, Config{})
	pub := dialTestClient(t, b.Addr(), "pub-retain")
	body := appendString(nil, "state/a")
	body = append(body, []byte("on")...)
	pub.writePacket(0x31, body) // PUBLISH with retain flag

	sub := dialTestClient(t, b.Addr(), "sub-retain")
	req := []byte{0, 2}
	req = appendString(req, "state/#")
	req = append(req, 0)
	sub.writePacket(0x82, req)
	if _, _, err := sub.readPacket(); err != nil { // SUBACK
		t.Fatal(err)
	}
	h, p, err := sub.readPacket()
	if err != nil || h>>4 != 3 {
		t.Fatalf("retained publish: h=%x err=%v", h, err)
	}
	topic, rest, _ := readString(p)
	if topic != "state/a" || !bytes.Equal(rest, []byte("on")) {
		t.Fatalf("unexpected retained: %q %q", topic, rest)
	}
}

func TestAuthDenied(t *testing.T) {
	b := startBroker(t, Config{Auth: DenyAll{}})
	conn, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	body := connectPayload("denied")
	buf := appendRemainingLength([]byte{0x10}, len(body))
	buf = append(buf, body...)
	if _, err := conn.Write(buf); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	header, err := r.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	n, _ := readRemainingLength(r)
	payload := make([]byte, n)
	_, _ = r.Read(payload)
	if header>>4 != 2 || len(payload) != 2 || payload[1] != 5 {
		t.Fatalf("expected connack 5, got %x %v", header, payload)
	}
	if binary.BigEndian.Uint16(payload) != 0x0005 {
		t.Fatalf("unexpected payload: %v", payload)
	}
}

func dialRawConnack(t *testing.T, addr, clientID string) byte {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	body := connectPayload(clientID)
	buf := appendRemainingLength([]byte{0x10}, len(body))
	buf = append(buf, body...)
	if _, err := conn.Write(buf); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	if _, err := r.ReadByte(); err != nil {
		t.Fatal(err)
	}
	n, err := readRemainingLength(r)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, n)
	if _, err := r.Read(payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) < 2 {
		t.Fatalf("short connack: %v", payload)
	}
	return payload[1]
}

func TestConnectModeUniqueRejectsSecondClient(t *testing.T) {
	b := startBroker(t, Config{ConnectMode: ConnectModeUnique})
	_ = dialTestClient(t, b.Addr(), "dup")
	if code := dialRawConnack(t, b.Addr(), "dup"); code != 2 {
		t.Fatalf("expected CONNACK 2 for duplicate client id, got %d", code)
	}
}

func TestConnectModeKickAcceptsSecondClient(t *testing.T) {
	// NotKickSeconds -1 disables the anti-flap window, so the second CONNECT
	// takes the session over immediately — the behavior this test pins. The
	// window itself is covered by TestTakeoverAntiFlapWindow.
	b := startBroker(t, Config{ConnectMode: ConnectModeKick, NotKickSeconds: -1})
	old := dialTestClient(t, b.Addr(), "dup")
	if code := dialRawConnack(t, b.Addr(), "dup"); code != 0 {
		t.Fatalf("expected CONNACK 0 in KICK mode, got %d", code)
	}
	_ = old.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err := old.r.ReadByte(); err == nil {
		t.Fatal("expected old connection to be closed in KICK mode")
	}
}
