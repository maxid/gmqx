package conn

import (
	"testing"
	"time"

	"github.com/maxid/gmqx/session"
)

// TestResumeInflightResendsOnlyRestored pins the selection resumeInflight makes.
// The CONNACK is on the wire before it runs, so a targeted or local delivery can
// already be registered in the connection's inflight table by then; resending it
// would duplicate a message the peer already received (DUP set), which is the
// Windows CI failure this split was made for. Only the exchanges restoreQoS2State
// carried over from the previous connection may be resent.
func TestResumeInflightResendsOnlyRestored(t *testing.T) {
	h := &testHost{limits: Limits{QoSRetryInterval: time.Minute, QoSMaxRetries: 5}}
	c := &Client{
		host:     h,
		inflight: make(map[uint16]*inflightMessage),
	}

	// A persistent session whose connection ended with one unconfirmed QoS 1
	// exchange: restoreQoS2State carries it over.
	c.restoreQoS2Locked(&session.State{Inflight: []session.Inflight{{
		PacketID: 1, QoS: 1, Topic: "resume/x", Payload: []byte("restored"), Stage: session.AwaitPuback,
	}}})

	// A delivery that reached this fresh connection after the CONNACK and before
	// the handshake finished: it is in the same table, but it was written once and
	// must not be resent.
	c.mu.Lock()
	c.addInflightLocked(2, &inflightMessage{
		packetID: 2, qos: 1, topic: "resume/x", payload: []byte("fresh"), state: awaitPuback,
	})
	batch := c.restoredBatchLocked()
	c.mu.Unlock()

	if len(batch) != 1 || batch[0].packetID != 1 || string(batch[0].payload) != "restored" {
		t.Fatalf("resume batch = %+v, want only the restored exchange (packet id 1)", batch)
	}

	// The restored set is consumed by the first resume.
	c.mu.Lock()
	again := c.restoredBatchLocked()
	_, freshKept := c.inflight[2]
	c.mu.Unlock()
	if len(again) != 0 {
		t.Fatalf("second resume batch = %+v, want none", again)
	}
	if !freshKept {
		t.Fatal("the delivery registered before resumeInflight was dropped from the table")
	}
}
