package conn

// The connection-level exhaustion case: what one connection does when it has no
// packet id left to assign. The broker-level queue behavior around it stays in
// gmqx/internal/engine.

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// TestWritePublishDegradesWhenPacketIDsExhausted covers the other exhaustion case
// at the unit that decides it: when every packet id of the connection is in use,
// a QoS 1 delivery goes out at QoS 0 instead of being dropped or sent with a
// packet id a retransmission would collide with.
func TestWritePublishDegradesWhenPacketIDsExhausted(t *testing.T) {
	metrics := newRecordingMetrics()
	h := &testHost{limits: Limits{QoSRetryInterval: time.Second, QoSMaxRetries: 5}, metrics: metrics}
	server, peer := net.Pipe()
	defer func() { _ = server.Close(); _ = peer.Close() }()

	c := New(h, server)
	c.version = mqtt.Version311
	for id := 1; id <= 65535; id++ {
		c.inflight[uint16(id)] = &inflightMessage{packetID: uint16(id), qos: 1, state: awaitPuback}
	}
	go func() { _ = c.writePublish("ids/x", []byte("payload"), 1, false, mqtt.Properties{}) }()

	_ = peer.SetReadDeadline(time.Now().Add(3 * time.Second))
	decoder := mqtt.Decoder{Version: mqtt.Version311}
	msg, err := decoder.Decode(bufio.NewReader(peer))
	if err != nil {
		t.Fatalf("decode degraded PUBLISH: %v", err)
	}
	pub, ok := msg.(*mqtt.Publish)
	if !ok {
		t.Fatalf("read %T, want a PUBLISH", msg)
	}
	if pub.QoS != 0 || pub.PacketID != 0 || string(pub.Payload) != "payload" {
		t.Fatalf("degraded delivery = qos%d pid%d payload %q, want QoS 0 without a packet id", pub.QoS, pub.PacketID, pub.Payload)
	}
	if n := metrics.overflowsFor("packet_ids"); n != 1 {
		t.Fatalf("packet_ids overflows = %d, want 1", n)
	}
}
