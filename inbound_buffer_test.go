package gmqx

// The inbound read path decodes a PUBLISH into one buffer per connection and
// reuses it for the next packet (client.readBuf, client.readPublish). These tests
// pin the ownership rule that makes the reuse safe: anything that keeps a payload
// past the publish it belongs to gets its own bytes (see client.handlePublish).
//
// Both tests would fail if the broker handed a view of the read buffer to the
// sink or to a delivery: the next PUBLISH of the same connection overwrites those
// bytes.

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
)

// TestInboundPayloadOutlivesReadBufferForSink pins that a PublishEvent payload
// stays valid after the publish that produced it.
//
// The sink runs on the publisher's read goroutine, between two reads of the same
// buffer, so the check is deterministic rather than a race: the second PUBLISH is
// read after the first OnPublish returned, so a payload that still aliased the
// buffer would already carry the second message's bytes. Every payload has the
// same length, so an aliasing payload would be overwritten in place instead of
// merely truncated.
func TestInboundPayloadOutlivesReadBufferForSink(t *testing.T) {
	const count = 8
	kept := make([][]byte, 0, count)
	b := startBroker(t, Config{Sink: sinkFunc(func(_ context.Context, ev PublishEvent) PublishResult {
		// Deliberately not copied: the broker promised the sink its own bytes.
		kept = append(kept, ev.Payload)
		return PublishResult{Disposition: PublishAccepted}
	})})
	pub := dialTestClient(t, b.Addr(), "pub-1")

	want := make([]string, count)
	for i := 0; i < count; i++ {
		want[i] = fmt.Sprintf("payload-%02d", i)
		header, body := qos1Publish("plant/a/temp", uint16(i+1), want[i])
		pub.writePacket(header, body)
		h, p, err := pub.readPacket()
		if err != nil || h != 0x40 || len(p) != 2 {
			t.Fatalf("publish %d: puback h=%#x p=%v err=%v", i, h, p, err)
		}
	}

	if len(kept) != count {
		t.Fatalf("sink saw %d publishes, want %d", len(kept), count)
	}
	for i, payload := range kept {
		if got := string(payload); got != want[i] {
			t.Fatalf("payload %d = %q, want %q: it did not survive the read buffer", i, got, want[i])
		}
	}
}

// TestInboundPayloadDeliveredIntact pins the same rule on the delivery path: what
// a subscriber's writer goroutine encodes comes from the publication, not from the
// publisher's read buffer, even when the publisher sends its frames in one batch
// (so the broker reads the next packet while the delivery is still queued) and one
// of them is large enough to grow that buffer.
func TestInboundPayloadDeliveredIntact(t *testing.T) {
	const count = 16
	b := startBroker(t, Config{})
	sub := dialTestClient(t, b.Addr(), "sub-1")
	subscribeTestClient(t, sub, "reuse/#")
	pub := dialTestClient(t, b.Addr(), "pub-1")

	// Every frame goes out in one Write, so the publisher's read loop is already
	// reading frame i+1 while the delivery of frame i waits in the subscriber's
	// writer queue.
	wantTopic := make([]string, count)
	wantPayload := make([]string, count)
	var batch []byte
	for i := 0; i < count; i++ {
		wantTopic[i] = fmt.Sprintf("reuse/%02d", i)
		if i == count/2 {
			// Larger than any earlier packet: the connection's read buffer grows
			// here, which is the one moment an aliasing payload would be lost.
			wantPayload[i] = strings.Repeat("y", 4096)
		} else {
			wantPayload[i] = fmt.Sprintf("%02d-%s", i, strings.Repeat("x", 32))
		}
		body := appendString(nil, wantTopic[i])
		body = append(body, wantPayload[i]...)
		batch = append(batch, 0x30)
		batch = appendRemainingLength(batch, len(body))
		batch = append(batch, body...)
	}
	if _, err := pub.conn.Write(batch); err != nil {
		t.Fatalf("write publish batch: %v", err)
	}

	sub.armDeadline(testClientIOTimeout)
	for i := 0; i < count; i++ {
		topic, payload := readPublishFull(t, sub)
		if topic != wantTopic[i] {
			t.Fatalf("delivery %d: topic = %q, want %q", i, topic, wantTopic[i])
		}
		if string(payload) != wantPayload[i] {
			t.Fatalf("delivery %d: payload of %d bytes differs from the %d bytes that were published",
				i, len(payload), len(wantPayload[i]))
		}
	}
}

// readPublishFull reads one PUBLISH and returns its topic and payload, requiring
// every body byte. The shared testClient.readPacket takes what one Read of the
// bufio buffer returned, which is enough for the small packets the other wire
// tests read but not for a payload larger than that buffer.
func readPublishFull(t *testing.T, c *testClient) (string, []byte) {
	t.Helper()
	header, err := c.r.ReadByte()
	if err != nil {
		t.Fatalf("read publish header: %v", err)
	}
	if header>>4 != 3 {
		t.Fatalf("expected PUBLISH, got type %d", header>>4)
	}
	n, err := readRemainingLength(c.r)
	if err != nil {
		t.Fatalf("read publish remaining length: %v", err)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(c.r, body); err != nil {
		t.Fatalf("read publish body: %v", err)
	}
	topic, rest, err := readString(body)
	if err != nil {
		t.Fatalf("read publish topic: %v", err)
	}
	return topic, rest
}
