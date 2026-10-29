package gmqx

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"
)

func connectV5Payload(clientID string, props []byte) []byte {
	var b []byte
	b = appendString(b, "MQTT")
	b = append(b, 5, 0x02, 0, 30) // level, clean start, keepalive
	// property length + properties
	n := len(props)
	for {
		enc := byte(n % 128)
		n /= 128
		if n > 0 {
			enc |= 128
		}
		b = append(b, enc)
		if n == 0 {
			break
		}
	}
	b = append(b, props...)
	b = appendString(b, clientID)
	return b
}

func dialV5(t *testing.T, addr, clientID string, props []byte) (net.Conn, *bufio.Reader, byte) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	body := connectV5Payload(clientID, props)
	frame := appendRemainingLength([]byte{0x10}, len(body))
	frame = append(frame, body...)
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	header, err := r.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	n, err := readRemainingLength(r)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		t.Fatal(err)
	}
	if header>>4 != 2 || len(payload) < 2 {
		t.Fatalf("unexpected connack: %x %v", header, payload)
	}
	return conn, r, payload[1]
}

func TestMQTT5ConnectWithProperties(t *testing.T) {
	b := startBroker(t, Config{})
	// Session Expiry Interval (0x11) = 60 seconds.
	conn, _, reason := dialV5(t, b.Addr(), "v5-client", []byte{0x11, 0, 0, 0, 60})
	defer conn.Close()
	if reason != 0 {
		t.Fatalf("v5 CONNACK reason = %d", reason)
	}
}

func TestMQTT5SubscribePublish(t *testing.T) {
	b := startBroker(t, Config{})
	subConn, subReader, reason := dialV5(t, b.Addr(), "v5-sub", nil)
	defer subConn.Close()
	if reason != 0 {
		t.Fatalf("sub connack reason = %d", reason)
	}
	// SUBSCRIBE: pid=1, properties=0, filter, options(qos0)
	body := []byte{0, 1, 0}
	body = appendString(body, "v5/topic")
	body = append(body, 0)
	frame := appendRemainingLength([]byte{0x82}, len(body))
	frame = append(frame, body...)
	if _, err := subConn.Write(frame); err != nil {
		t.Fatal(err)
	}
	header, err := subReader.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	n, _ := readRemainingLength(subReader)
	payload := make([]byte, n)
	_, _ = io.ReadFull(subReader, payload)
	if header>>4 != 9 || len(payload) < 4 || payload[2] != 0 || payload[3] != 0 {
		t.Fatalf("unexpected v5 suback: %x %v", header, payload)
	}

	pub := dialTestClient(t, b.Addr(), "v3-pub")
	pubBody := appendString(nil, "v5/topic")
	pubBody = append(pubBody, []byte("hello-v5")...)
	pub.writePacket(0x30, pubBody)

	// v5 PUBLISH: header, topic, property length 0, payload
	ph, err := subReader.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	pn, _ := readRemainingLength(subReader)
	pp := make([]byte, pn)
	_, _ = io.ReadFull(subReader, pp)
	if ph>>4 != 3 {
		t.Fatalf("expected PUBLISH, got type %d", ph>>4)
	}
	topic, rest, err := readString(pp)
	if err != nil || topic != "v5/topic" {
		t.Fatalf("topic=%q err=%v", topic, err)
	}
	if len(rest) < 1 || rest[0] != 0 { // property length
		t.Fatalf("expected empty v5 properties, got %v", rest)
	}
	if string(rest[1:]) != "hello-v5" {
		t.Fatalf("payload=%q", rest[1:])
	}
}

func TestMQTT5AuthDenied(t *testing.T) {
	b := startBroker(t, Config{Auth: DenyAll{}})
	conn, _, reason := dialV5(t, b.Addr(), "v5-denied", nil)
	defer conn.Close()
	if reason != 0x87 {
		t.Fatalf("expected 0x87 Not authorized, got %d", reason)
	}
}
