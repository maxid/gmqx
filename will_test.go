package gmqx

import (
	"bufio"
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// lifecycleSink records how each connection ended, so a test can tell a clean
// DISCONNECT from a dropped connection.
type lifecycleSink struct {
	mu    sync.Mutex
	clean []string
	lost  []string
}

func (*lifecycleSink) OnConnect(context.Context, ConnectInfo) {}

func (s *lifecycleSink) OnDisconnect(_ context.Context, clientID string, _ error) {
	s.mu.Lock()
	s.clean = append(s.clean, clientID)
	s.mu.Unlock()
}

func (s *lifecycleSink) OnConnectionLost(_ context.Context, clientID string, _ error) {
	s.mu.Lock()
	s.lost = append(s.lost, clientID)
	s.mu.Unlock()
}

func (*lifecycleSink) OnPublish(context.Context, PublishEvent) PublishResult {
	return PublishResult{Disposition: PublishAccepted}
}

func (*lifecycleSink) OnPing(context.Context, string) {}

func (s *lifecycleSink) counts() (clean, lost int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clean), len(s.lost)
}

// waitEnded blocks until want connections ended, either way.
func waitEnded(t *testing.T, s *lifecycleSink, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if clean, lost := s.counts(); clean+lost >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	clean, lost := s.counts()
	t.Fatalf("ended connections = %d clean + %d lost, want %d", clean, lost, want)
}

// connectWithWillPayload builds an MQTT 3.1.1 CONNECT that registers a will.
func connectWithWillPayload(clientID, willTopic, willPayload string) []byte {
	var b []byte
	b = appendString(b, "MQTT")
	b = append(b, 4, 0x02|0x04, 0, 30) // level 4, clean session, will flag, keepalive 30
	b = appendString(b, clientID)
	b = appendString(b, willTopic)
	b = appendString(b, willPayload)
	return b
}

// connectV5WithWillPayload is the same for MQTT 5, where CONNECT and the will
// each carry a (here empty) property block.
func connectV5WithWillPayload(clientID, willTopic, willPayload string) []byte {
	var b []byte
	b = appendString(b, "MQTT")
	b = append(b, 5, 0x02|0x04, 0, 30) // level 5, clean start, will flag, keepalive 30
	b = append(b, 0)                   // CONNECT properties
	b = appendString(b, clientID)
	b = append(b, 0) // will properties
	b = appendString(b, willTopic)
	b = appendString(b, willPayload)
	return b
}

func dialWithConnect(t *testing.T, addr string, body []byte) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	c := &testClient{conn: conn, r: bufio.NewReader(conn)}
	c.writePacket(0x10, body)
	header, payload, err := c.readPacket()
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if header != 0x20 || len(payload) < 2 || payload[1] != 0 {
		t.Fatalf("unexpected connack: %x %v", header, payload)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return c
}

// TestWillOnCleanDisconnectAndLoss pins both halves of the will contract: a
// DISCONNECT ends the session cleanly and drops the will, while a dropped
// connection is a loss that publishes it. The clean leg also pins that an
// empty-body MQTT 5 DISCONNECT is a complete frame the decoder consumes on its
// own.
func TestWillOnCleanDisconnectAndLoss(t *testing.T) {
	sink := &lifecycleSink{}
	b := startBroker(t, Config{Sink: sink})

	sub := dialTestClient(t, b.Addr(), "will-sub")
	subBody := []byte{0, 1}
	subBody = appendString(subBody, "will/topic")
	subBody = append(subBody, 0)
	sub.writePacket(0x82, subBody)
	if h, p, err := sub.readPacket(); err != nil || h != 0x90 || len(p) < 3 || p[2] != 0 {
		t.Fatalf("suback: %x %v %v", h, p, err)
	}

	// Both protocol levels end cleanly, and neither the 3.1.1 nor the (bodyless)
	// MQTT 5 DISCONNECT may publish the will.
	for i, connect := range [][]byte{
		connectWithWillPayload("will-clean", "will/topic", "clean"),
		connectV5WithWillPayload("will-clean-v5", "will/topic", "clean"),
	} {
		c := dialWithConnect(t, b.Addr(), connect)
		c.writePacket(0xE0, nil) // DISCONNECT
		waitEnded(t, sink, i+1)
		if cleanCount, lostCount := sink.counts(); cleanCount != i+1 || lostCount != 0 {
			t.Fatalf("clean disconnect %d recorded as %d clean / %d lost", i, cleanCount, lostCount)
		}
	}
	_ = sub.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if h, _, err := sub.readPacket(); err == nil {
		t.Fatalf("will published on a clean disconnect: %#x", h)
	}

	drop := dialWithConnect(t, b.Addr(), connectWithWillPayload("will-drop", "will/topic", "lost"))
	_ = drop.conn.Close()

	_ = sub.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	h, p, err := sub.readPacket()
	if err != nil || h>>4 != 3 {
		t.Fatalf("will publish: %x %v %v", h, p, err)
	}
	topic, rest, err := readString(p)
	if err != nil || topic != "will/topic" || string(rest) != "lost" {
		t.Fatalf("will = %q %q (%v)", topic, rest, err)
	}
	waitEnded(t, sink, 3)
	if cleanCount, lostCount := sink.counts(); cleanCount != 2 || lostCount != 1 {
		t.Fatalf("dropped connection recorded as %d clean / %d lost", cleanCount, lostCount)
	}
}

// TestMQTT5DisconnectDecodedByCodec is the regression pin for the frames the
// decoder must consume on its own: the minimal E0 00 DISCONNECT a client sends
// on a clean shutdown and one that carries a reason code. Both end the session
// cleanly: the will is dropped and the sink sees OnDisconnect, never
// OnConnectionLost.
func TestMQTT5DisconnectDecodedByCodec(t *testing.T) {
	tests := []struct {
		name       string
		clientID   string
		disconnect []byte
	}{
		{"empty body", "disc-empty", []byte{0xE0, 0x00}},
		{"reason code", "disc-reason", []byte{0xE0, 0x01, 0x00}}, // Normal disconnection
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := &lifecycleSink{}
			b := startBroker(t, Config{Sink: sink})

			sub := dialTestClient(t, b.Addr(), "disc-sub-"+tt.clientID)
			subBody := []byte{0, 1}
			subBody = appendString(subBody, "will/topic")
			subBody = append(subBody, 0)
			sub.writePacket(0x82, subBody)
			if h, p, err := sub.readPacket(); err != nil || h != 0x90 || len(p) < 3 || p[2] != 0 {
				t.Fatalf("suback: %x %v %v", h, p, err)
			}

			c := dialWithConnect(t, b.Addr(), connectV5WithWillPayload(tt.clientID, "will/topic", "published"))
			if _, err := c.conn.Write(tt.disconnect); err != nil {
				t.Fatal(err)
			}
			waitEnded(t, sink, 1)
			if clean, lost := sink.counts(); clean != 1 || lost != 0 {
				t.Fatalf("disconnect recorded as %d clean / %d lost", clean, lost)
			}

			_ = sub.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			if h, _, err := sub.readPacket(); err == nil {
				t.Fatalf("will published on a clean disconnect: %#x", h)
			}
		})
	}
}
