package gmqx

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
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

func publishV5(conn net.Conn, topic string, props, payload []byte) {
	body := appendString(nil, topic)
	body = append(body, prefixPropertyLength(props)...)
	body = append(body, payload...)
	frame := appendRemainingLength([]byte{0x30}, len(body))
	frame = append(frame, body...)
	_, _ = conn.Write(frame)
}

func readV5Publish(t *testing.T, r *bufio.Reader) (topic string, props, payload []byte) {
	t.Helper()
	header, err := r.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	if header>>4 != 3 {
		t.Fatalf("expected PUBLISH, got %d", header>>4)
	}
	n, err := readRemainingLength(r)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		t.Fatal(err)
	}
	topic, rest, err := readString(body)
	if err != nil {
		t.Fatal(err)
	}
	props, rest, err = readPropertyBlock(rest)
	if err != nil {
		t.Fatal(err)
	}
	return topic, props, rest
}

func TestMQTT5TopicAliasAndUserProperty(t *testing.T) {
	b := startBroker(t, Config{})
	subConn, subReader, reason := dialV5(t, b.Addr(), "v5-sub2", nil)
	defer subConn.Close()
	if reason != 0 {
		t.Fatalf("connack reason=%d", reason)
	}
	body := []byte{0, 1, 0}
	body = appendString(body, "alias/topic")
	body = append(body, 0)
	frame := appendRemainingLength([]byte{0x82}, len(body))
	frame = append(frame, body...)
	_, _ = subConn.Write(frame)
	if h, _, err := readPacketFrom(subReader); err != nil || h>>4 != 9 {
		t.Fatalf("suback: h=%x err=%v", h, err)
	}

	pubConn, _, _ := dialV5(t, b.Addr(), "v5-pub2", nil)
	defer pubConn.Close()
	userProp := []byte{0x26, 0, 3, 'k', 'e', 'y', 0, 1, 'v'}
	props := append([]byte{0x23, 0, 1}, userProp...) // topic alias 1 + user property
	publishV5(pubConn, "alias/topic", props, []byte("one"))

	topic, gotProps, payload := readV5Publish(t, subReader)
	if topic != "alias/topic" || string(payload) != "one" {
		t.Fatalf("unexpected first publish: topic=%q payload=%q", topic, payload)
	}
	forwarded, err := decodePropertyBlock(gotProps)
	if err != nil {
		t.Fatalf("forwarded properties %x: %v", gotProps, err)
	}
	if len(forwarded.UserProperties) != 1 || forwarded.UserProperties[0].Key != "key" || forwarded.UserProperties[0].Value != "v" {
		t.Fatalf("user property not forwarded: %x", gotProps)
	}
	if forwarded.TopicAlias != 0 {
		t.Fatalf("topic alias should not be forwarded: %x", gotProps)
	}

	// Empty topic + alias resolution.
	aliasOnly := []byte{0x23, 0, 1}
	publishV5(pubConn, "", aliasOnly, []byte("two"))
	topic, _, payload = readV5Publish(t, subReader)
	if topic != "alias/topic" || string(payload) != "two" {
		t.Fatalf("alias resolve failed: topic=%q payload=%q", topic, payload)
	}
}

func readPacketFrom(r *bufio.Reader) (byte, []byte, error) {
	header, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, err := readRemainingLength(r)
	if err != nil {
		return 0, nil, err
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return header, body, nil
}

func TestMQTT5ConnackProperties(t *testing.T) {
	b := startBroker(t, Config{})
	conn, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	body := connectV5Payload("v5-props", nil)
	frame := appendRemainingLength([]byte{0x10}, len(body))
	frame = append(frame, body...)
	_, _ = conn.Write(frame)

	decoder := mqtt.Decoder{Version: mqtt.Version5}
	msg, err := decoder.Decode(bufio.NewReader(conn))
	if err != nil {
		t.Fatalf("decode connack: %v", err)
	}
	ack, ok := msg.(*mqtt.Connack)
	if !ok {
		t.Fatalf("expected CONNACK, got %T", msg)
	}
	if ack.ReasonCode != 0 || ack.SessionPresent {
		t.Fatalf("connack reason=%d session=%v", ack.ReasonCode, ack.SessionPresent)
	}
	p := ack.Properties
	if p.RetainAvailable == nil || !*p.RetainAvailable {
		t.Fatalf("CONNACK does not declare retain support: %+v", p)
	}
	if p.WildcardSubscriptionAvailable == nil || !*p.WildcardSubscriptionAvailable {
		t.Fatalf("CONNACK does not declare wildcard subscriptions: %+v", p)
	}
	if p.SubscriptionIdentifierAvailable == nil || !*p.SubscriptionIdentifierAvailable {
		t.Fatalf("CONNACK must declare subscription identifiers supported: %+v", p)
	}
	if p.SharedSubscriptionAvailable == nil || !*p.SharedSubscriptionAvailable {
		t.Fatalf("CONNACK must declare shared subscriptions supported: %+v", p)
	}
	if p.TopicAliasMaximum != topicAliasMaximum {
		t.Fatalf("topic alias maximum = %d, want %d", p.TopicAliasMaximum, topicAliasMaximum)
	}
	if want := uint32(b.cfg.MaxPacketSize); p.MaximumPacketSize != want {
		t.Fatalf("maximum packet size = %d, want %d", p.MaximumPacketSize, want)
	}
	// Absent Maximum QoS means QoS 2, and the specification allows no other
	// value than 0 or 1 for it.
	if p.MaximumQoS != nil {
		t.Fatalf("CONNACK must not cap the maximum QoS: %d", *p.MaximumQoS)
	}
	if p.AssignedClientID != "" {
		t.Fatalf("unexpected assigned client id %q", p.AssignedClientID)
	}
}
