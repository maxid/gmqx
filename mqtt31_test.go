package gmqx

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"
)

func connect31Payload(clientID string) []byte {
	var b []byte
	b = appendString(b, "MQIsdp")
	b = append(b, 3, 0x02, 0, 30) // level 3, clean session, keepalive
	b = appendString(b, clientID)
	return b
}

func TestMQTT31ConnectAndPublish(t *testing.T) {
	b := startBroker(t, Config{})
	// subscriber (3.1)
	subConn, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer subConn.Close()
	_ = subConn.SetDeadline(time.Now().Add(3 * time.Second))
	body := connect31Payload("v31-sub")
	frame := appendRemainingLength([]byte{0x10}, len(body))
	frame = append(frame, body...)
	if _, err := subConn.Write(frame); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(subConn)
	if h, err := r.ReadByte(); err != nil || h>>4 != 2 {
		t.Fatalf("connack header=%x err=%v", h, err)
	}
	n, _ := readRemainingLength(r)
	ack := make([]byte, n)
	if _, err := io.ReadFull(r, ack); err != nil {
		t.Fatal(err)
	}
	if len(ack) < 2 || ack[1] != 0 {
		t.Fatalf("connack payload=%v", ack)
	}
	// subscribe cluster31/topic
	subBody := []byte{0, 1}
	subBody = appendString(subBody, "cluster31/topic")
	subBody = append(subBody, 0)
	sf := appendRemainingLength([]byte{0x82}, len(subBody))
	sf = append(sf, subBody...)
	if _, err := subConn.Write(sf); err != nil {
		t.Fatal(err)
	}
	if h, _, err := readPacketFrom(r); err != nil || h>>4 != 9 {
		t.Fatalf("suback header=%x err=%v", h, err)
	}
	// publisher (3.1)
	pubConn, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer pubConn.Close()
	_ = pubConn.SetDeadline(time.Now().Add(3 * time.Second))
	pbody := connect31Payload("v31-pub")
	pf := appendRemainingLength([]byte{0x10}, len(pbody))
	pf = append(pf, pbody...)
	if _, err := pubConn.Write(pf); err != nil {
		t.Fatal(err)
	}
	pr := bufio.NewReader(pubConn)
	if _, _, err := readPacketFrom(pr); err != nil {
		t.Fatal(err)
	}
	pub := appendString(nil, "cluster31/topic")
	pub = append(pub, []byte("v31")...)
	msg := appendRemainingLength([]byte{0x30}, len(pub))
	msg = append(msg, pub...)
	if _, err := pubConn.Write(msg); err != nil {
		t.Fatal(err)
	}
	h, payload, err := readPacketFrom(r)
	if err != nil || h>>4 != 3 {
		t.Fatalf("publish header=%x err=%v", h, err)
	}
	topic, rest, _ := readString(payload)
	if topic != "cluster31/topic" || string(rest) != "v31" {
		t.Fatalf("topic=%q rest=%q", topic, rest)
	}
}
