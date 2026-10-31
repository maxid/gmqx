package gmqx

// QoS 2 state across a reconnect (MQTT §4.4).
//
// Both directions of a QoS 1/2 exchange are session state, not connection state:
//
//   - inbound: the packet ids a client published at QoS 2 whose PUBREL has not
//     arrived yet are stored with the session, so a publisher that reconnects and
//     retransmits one gets the same acknowledgement and no second delivery;
//   - outbound: the packets the session still owes its client are stored too, and
//     a resumed connection re-sends them under their original packet ids — a
//     PUBLISH with DUP set while it is unconfirmed, a PUBREL once PUBREC arrived.
//
// A session that ends (Clean Session / Clean Start, or an expiry) takes the state
// with it, which is what makes the same packet id a new message afterwards.

import (
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

func TestInboundQoS2StateSurvivesReconnect(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "in-q2-resume-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "in/resume", QoS: 2}).ReasonCodes; len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	props := mqtt.Properties{SessionExpiryInterval: 60}
	pub, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "in-q2-resume-pub",
		clean:    false,
		props:    props,
	})
	if ack.SessionPresent {
		t.Fatal("a new session reports Session Present")
	}
	pub.publish(confPublish{topic: "in/resume", payload: "once", qos: 2, pid: 7})
	expectPubrec(t, pub, 7, 0)
	if got := expectPublish(t, sub); string(got.Payload) != "once" {
		t.Fatalf("first delivery = %q", got.Payload)
	}

	// The connection drops without the PUBREL: the exchange is unfinished.
	_ = pub.conn.Close()
	waitDisconnected(t, b, "in-q2-resume-pub")
	stored, ok := b.store.Load("in-q2-resume-pub")
	if !ok || len(stored.InboundQoS2) != 1 || stored.InboundQoS2[0] != 7 {
		t.Fatalf("stored inbound QoS 2 state = %+v (ok=%v), want [7]", stored.InboundQoS2, ok)
	}

	// The reconnect resumes the session and the retransmission is acknowledged
	// again but delivered once.
	again, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "in-q2-resume-pub",
		clean:    false,
		props:    props,
	})
	if !ack.SessionPresent {
		t.Fatal("the publisher's session did not resume")
	}
	again.publish(confPublish{topic: "in/resume", payload: "once", qos: 2, pid: 7, dup: true})
	expectPubrec(t, again, 7, 0)
	expectNoMessage(t, sub, 150*time.Millisecond)

	// PUBREL completes the exchange and clears the stored packet id.
	again.write(&mqtt.Pubrel{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}, PacketID: 7})
	expectPubcomp(t, again, 7)
	again.disconnect()
	waitDisconnected(t, b, "in-q2-resume-pub")
	stored, ok = b.store.Load("in-q2-resume-pub")
	if !ok {
		t.Fatal("the session was not stored after the exchange completed")
	}
	if len(stored.InboundQoS2) != 0 {
		t.Fatalf("stored inbound QoS 2 state after PUBCOMP = %v, want none", stored.InboundQoS2)
	}

	// The packet id is free: the next session sees the same id as a new message.
	next, _ := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "in-q2-resume-pub",
		clean:    false,
		props:    props,
	})
	next.publish(confPublish{topic: "in/resume", payload: "once", qos: 2, pid: 7})
	expectPubrec(t, next, 7, 0)
	if got := expectPublish(t, sub); string(got.Payload) != "once" {
		t.Fatalf("delivery of the reused packet id = %q", got.Payload)
	}
}

func TestOutboundQoS1ResumesAfterReconnect(t *testing.T) {
	b := startBroker(t, Config{})
	sub, ack := mustConnect(t, b.Addr(), confConnect{clientID: "out-q1-resume", clean: false})
	if ack.SessionPresent {
		t.Fatal("a new session reports Session Present")
	}
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "out/resume1", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "out-q1-resume-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "out/resume1", payload: "first", qos: 1, pid: 1})
	pub.mustPublish(t, confPublish{topic: "out/resume1", payload: "second", qos: 1, pid: 2})

	seen := make(map[uint16]string, 2)
	for _, want := range []string{"first", "second"} {
		got := expectPublish(t, sub)
		if string(got.Payload) != want {
			t.Fatalf("delivery = %q, want %q", got.Payload, want)
		}
		seen[got.PacketID] = string(got.Payload)
	}
	// The subscriber drops without acknowledging either delivery.
	_ = sub.conn.Close()
	waitDisconnected(t, b, "out-q1-resume")
	stored, ok := b.store.Load("out-q1-resume")
	if !ok || len(stored.Inflight) != 2 {
		t.Fatalf("stored outbound exchanges = %+v (ok=%v), want 2", stored.Inflight, ok)
	}
	for _, m := range stored.Inflight {
		if m.Stage != InflightAwaitPuback {
			t.Fatalf("stored stage of packet %d = %d, want %d", m.PacketID, m.Stage, InflightAwaitPuback)
		}
	}

	again, ack := mustConnect(t, b.Addr(), confConnect{clientID: "out-q1-resume", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the subscriber's session did not resume")
	}
	for i := 0; i < 2; i++ {
		got := expectPublish(t, again)
		if !got.Dup {
			t.Fatalf("resumed delivery %q does not carry DUP", got.Payload)
		}
		if want, ok := seen[got.PacketID]; !ok || want != string(got.Payload) {
			t.Fatalf("resumed delivery %q with packet id %d does not match the first one (%v)", got.Payload, got.PacketID, seen)
		}
		again.ack(got.PacketID)
	}
	waitInflightEmpty(t, b, "out-q1-resume")
	expectNoMessage(t, again, 150*time.Millisecond)
}

func TestOutboundQoS2PubrelResumesAfterReconnect(t *testing.T) {
	b := startBroker(t, Config{})
	sub, ack := mustConnect(t, b.Addr(), confConnect{clientID: "out-q2-resume", clean: false})
	if ack.SessionPresent {
		t.Fatal("a new session reports Session Present")
	}
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "out/resume2", QoS: 2}).ReasonCodes; len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "out-q2-resume-pub", clean: true})
	pub.publish(confPublish{topic: "out/resume2", payload: "exactly-once", qos: 2, pid: 1})
	expectPubrec(t, pub, 1, 0)

	first := expectPublish(t, sub)
	if first.QoS != 2 || first.Dup {
		t.Fatalf("first delivery = QoS %d DUP %v, want QoS 2 without DUP", first.QoS, first.Dup)
	}
	// PUBREC confirms the PUBLISH: the broker answers with PUBREL and the exchange
	// now waits for PUBCOMP.
	sub.write(&mqtt.Pubrec{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: first.PacketID})
	rel := readConf[*mqtt.Pubrel](t, sub)
	if rel.PacketID != first.PacketID {
		t.Fatalf("PUBREL packet id %d, want %d", rel.PacketID, first.PacketID)
	}
	_ = sub.conn.Close()
	waitDisconnected(t, b, "out-q2-resume")
	stored, ok := b.store.Load("out-q2-resume")
	if !ok || len(stored.Inflight) != 1 || stored.Inflight[0].Stage != InflightAwaitPubcomp {
		t.Fatalf("stored outbound exchanges = %+v (ok=%v), want one awaiting PUBCOMP", stored.Inflight, ok)
	}

	// The resumed session continues the exchange with the PUBREL it owes, under
	// the same packet id.
	again, ack := mustConnect(t, b.Addr(), confConnect{clientID: "out-q2-resume", clean: false})
	if !ack.SessionPresent {
		t.Fatal("the subscriber's session did not resume")
	}
	rel = readConf[*mqtt.Pubrel](t, again)
	if rel.PacketID != first.PacketID {
		t.Fatalf("resumed PUBREL packet id %d, want %d", rel.PacketID, first.PacketID)
	}
	again.write(&mqtt.Pubcomp{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: rel.PacketID})
	waitInflightEmpty(t, b, "out-q2-resume")
	expectNoMessage(t, again, 150*time.Millisecond)
}

// TestSessionStateCopiesQoS2State pins the store contract for the new fields: a
// SessionStore never hands out a slice the caller can mutate behind its back, and
// the QoS 2 state is copied with the rest of the state.
func TestSessionStateCopiesQoS2State(t *testing.T) {
	store := NewMemorySessionStore()
	in := SessionState{
		ClientID:    "copy-qos2",
		InboundQoS2: []uint16{7},
		Inflight: []SessionInflight{
			{PacketID: 9, Topic: "copy/x", Payload: []byte("p"), QoS: 2, Stage: InflightAwaitPubrec},
		},
	}
	store.Save("copy-qos2", in)
	in.InboundQoS2[0] = 99
	in.Inflight[0].PacketID = 100

	got, ok := store.Load("copy-qos2")
	if !ok {
		t.Fatal("Load reported no session after Save")
	}
	if len(got.InboundQoS2) != 1 || got.InboundQoS2[0] != 7 {
		t.Fatalf("stored inbound state = %v, want [7]", got.InboundQoS2)
	}
	if len(got.Inflight) != 1 || got.Inflight[0].PacketID != 9 || got.Inflight[0].Stage != InflightAwaitPubrec {
		t.Fatalf("stored inflight state = %+v, want packet 9 awaiting PUBREC", got.Inflight)
	}
}
