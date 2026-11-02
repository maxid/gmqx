package session

// Session state tests: the projection between the in-memory form a broker routes
// and delivers with (State) and the form a SessionStore persists (SessionState).
// The stored form is what a resumed session is rebuilt from, so the cases below pin
// exactly what survives it, what order the stored form has, and what a broker
// refuses to trust from a store it did not write in this process.

import (
	"reflect"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// stateFixture is a session with every half of the state populated: two filters
// (one carrying subscription identifiers), a queue whose messages carry MQTT 5
// properties, un-released inbound QoS 2 packet ids and unconfirmed outbound
// exchanges in both QoS 2 stages.
func stateFixture() *State {
	return &State{
		ClientID:  "fleet/truck-1",
		ExpiresAt: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC),
		Subs: map[string]Subscription{
			"fleet/+/telemetry": {
				QoS:               1,
				RetainAsPublished: true,
				SubscriptionIDs:   []uint32{7, 9},
			},
			"fleet/truck-1/cmd": {
				QoS:            2,
				NoLocal:        true,
				RetainHandling: 2,
			},
		},
		Queue: []Pending{
			{Topic: "fleet/truck-1/cmd", Payload: []byte("start"), QoS: 1, Props: mqtt.Properties{MessageExpiryInterval: 60}},
			{Topic: "fleet/truck-1/cmd", Payload: []byte("stop"), QoS: 2, Retain: true},
		},
		InboundQoS2: []uint16{9, 3},
		Inflight: []Inflight{
			{
				PacketID: 11,
				Topic:    "fleet/truck-1/cmd",
				Payload:  []byte("left"),
				QoS:      2,
				Retain:   true,
				Props:    mqtt.Properties{ContentType: "text/plain"},
				Stage:    InflightAwaitPubrec,
			},
			{
				PacketID: 4,
				Topic:    "fleet/+/telemetry",
				Payload:  []byte("dropped"),
				QoS:      1,
				Stage:    InflightAwaitPuback,
			},
			{
				PacketID: 12,
				Topic:    "fleet/truck-1/cmd",
				Payload:  []byte("right"),
				QoS:      2,
				Stage:    InflightAwaitPubcomp,
			},
		},
	}
}

// TestStateStoredRoundTrip pins what survives a store: every subscription option,
// the queue with its properties, the inbound QoS 2 packet ids and the outbound
// exchanges with their stage and packet id.
func TestStateStoredRoundTrip(t *testing.T) {
	live := stateFixture()
	stored := live.Stored()

	if stored.ClientID != live.ClientID || !stored.ExpiresAt.Equal(live.ExpiresAt) {
		t.Fatalf("stored identity = %q/%v, want %q/%v", stored.ClientID, stored.ExpiresAt, live.ClientID, live.ExpiresAt)
	}
	if got := stored.Subscriptions["fleet/+/telemetry"]; got.QoS != 1 || !got.RetainAsPublished || !reflect.DeepEqual(got.SubscriptionIdentifiers, []uint32{7, 9}) {
		t.Fatalf("stored subscription = %+v, want the live options", got)
	}
	if got := stored.Subscriptions["fleet/truck-1/cmd"]; !got.NoLocal || got.RetainHandling != 2 {
		t.Fatalf("stored subscription = %+v, want the live options", got)
	}
	if len(stored.Queue) != 2 || stored.Queue[0].Props == nil {
		t.Fatalf("stored queue = %+v, want both messages and the encoded properties", stored.Queue)
	}
	if len(stored.Inflight) != 3 {
		t.Fatalf("stored inflight = %d exchanges, want 3", len(stored.Inflight))
	}

	back, dropped := StateFromStored(stored)
	if dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}
	if !reflect.DeepEqual(back.Subs, live.Subs) {
		t.Fatalf("subscriptions after the round trip = %+v, want %+v", back.Subs, live.Subs)
	}
	if !reflect.DeepEqual(back.Queue, live.Queue) {
		t.Fatalf("queue after the round trip = %+v, want %+v", back.Queue, live.Queue)
	}
	if !reflect.DeepEqual(back.InboundQoS2, []uint16{3, 9}) {
		t.Fatalf("inbound QoS 2 after the round trip = %v, want [3 9]", back.InboundQoS2)
	}
	byID := make(map[uint16]Inflight, len(back.Inflight))
	for _, m := range back.Inflight {
		byID[m.PacketID] = m
	}
	for _, want := range live.Inflight {
		got, ok := byID[want.PacketID]
		if !ok {
			t.Fatalf("exchange %d lost in the round trip", want.PacketID)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("exchange %d after the round trip = %+v, want %+v", want.PacketID, got, want)
		}
	}
}

// TestStateStoredOrdersPacketIDs pins that the stored form of a session is
// reproducible: both packet-id sets are written in ascending order, whatever order
// the live form happened to hold them in.
func TestStateStoredOrdersPacketIDs(t *testing.T) {
	stored := (&State{
		ClientID:    "c",
		InboundQoS2: []uint16{9, 3, 1},
		Inflight: []Inflight{
			{PacketID: 7, QoS: 1},
			{PacketID: 2, QoS: 1},
			{PacketID: 5, QoS: 2, Stage: InflightAwaitPubcomp},
		},
	}).Stored()

	if want := []uint16{1, 3, 9}; !reflect.DeepEqual(stored.InboundQoS2, want) {
		t.Fatalf("stored inbound QoS 2 = %v, want %v", stored.InboundQoS2, want)
	}
	ids := make([]uint16, 0, len(stored.Inflight))
	for _, m := range stored.Inflight {
		ids = append(ids, m.PacketID)
	}
	if want := []uint16{2, 5, 7}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("stored inflight order = %v, want %v", ids, want)
	}
}

// TestStateStoredKeepsMessageWithUnencodableProperties pins that a property block
// the codec refuses costs the properties, not the message: a queued message whose
// properties cannot be encoded for a PUBLISH is still stored and still delivered.
func TestStateStoredKeepsMessageWithUnencodableProperties(t *testing.T) {
	live := &State{
		ClientID: "c",
		Queue: []Pending{
			// The session expiry interval is a CONNECT property, so a PUBLISH
			// property block carrying it cannot be encoded.
			{Topic: "t", Payload: []byte("p"), QoS: 1, Props: mqtt.Properties{SessionExpiryInterval: 30}},
		},
		Inflight: []Inflight{
			{PacketID: 1, Topic: "t", Payload: []byte("p"), QoS: 1, Props: mqtt.Properties{SessionExpiryInterval: 30}},
		},
	}
	stored := live.Stored()
	if len(stored.Queue) != 1 || stored.Queue[0].Props != nil {
		t.Fatalf("stored queue = %+v, want the message without properties", stored.Queue)
	}
	if len(stored.Inflight) != 1 || stored.Inflight[0].Props != nil {
		t.Fatalf("stored inflight = %+v, want the exchange without properties", stored.Inflight)
	}
	back, dropped := StateFromStored(stored)
	if dropped != 0 || len(back.Queue) != 1 || back.Queue[0].Topic != "t" {
		t.Fatalf("round trip = %+v, dropped %d, want the message kept", back.Queue, dropped)
	}
}

// TestStateFromStoredDropsUndecodableQueuedMessages pins that one unreadable
// property block costs that message and is reported, while the rest of the queue
// keeps its order.
func TestStateFromStoredDropsUndecodableQueuedMessages(t *testing.T) {
	back, dropped := StateFromStored(SessionState{
		ClientID: "c",
		Queue: []QueuedMessage{
			{Topic: "t", Payload: []byte("first"), QoS: 1},
			{Topic: "t", Payload: []byte("corrupt"), QoS: 1, Props: []byte{0xff}},
			{Topic: "t", Payload: []byte("last"), QoS: 1},
		},
	})
	if dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
	if len(back.Queue) != 2 || string(back.Queue[0].Payload) != "first" || string(back.Queue[1].Payload) != "last" {
		t.Fatalf("queue = %+v, want first and last in order", back.Queue)
	}
}

// TestStateFromStoredDropsImpossibleInflight pins that an exchange that cannot have
// been a QoS 1/2 PUBLISH is dropped instead of being retransmitted with a shape the
// protocol does not allow.
func TestStateFromStoredDropsImpossibleInflight(t *testing.T) {
	back, _ := StateFromStored(SessionState{
		ClientID: "c",
		Inflight: []SessionInflight{
			{PacketID: 0, QoS: 1},
			{PacketID: 1, QoS: 0},
			{PacketID: 2, QoS: 3},
			{PacketID: 3, QoS: 2},
		},
	})
	if len(back.Inflight) != 1 || back.Inflight[0].PacketID != 3 {
		t.Fatalf("inflight = %+v, want only packet id 3", back.Inflight)
	}
}

// TestStateFromStoredKeepsExchangeWithUndecodableProperties pins that a property
// block that no longer decodes costs the properties, not the exchange: the packet
// id, topic and payload are what a resume needs.
func TestStateFromStoredKeepsExchangeWithUndecodableProperties(t *testing.T) {
	back, _ := StateFromStored(SessionState{
		ClientID: "c",
		Inflight: []SessionInflight{
			{PacketID: 5, Topic: "t", Payload: []byte("p"), QoS: 1, Props: []byte{0xff}},
		},
	})
	if len(back.Inflight) != 1 {
		t.Fatalf("inflight = %+v, want the exchange kept", back.Inflight)
	}
	got := back.Inflight[0]
	if got.PacketID != 5 || got.Topic != "t" || string(got.Payload) != "p" || len(got.Props.UserProperties) != 0 {
		t.Fatalf("exchange = %+v, want packet id, topic and payload without properties", got)
	}
}

// TestStateFromStoredNormalisesUnknownStage pins the one value a store written by a
// future build can hand back: an unknown stage re-sends the packet as a QoS 1
// PUBLISH waiting for its PUBACK instead of dropping it.
func TestStateFromStoredNormalisesUnknownStage(t *testing.T) {
	back, _ := StateFromStored(SessionState{
		ClientID: "c",
		Inflight: []SessionInflight{
			{PacketID: 1, Topic: "t", QoS: 2, Stage: InflightStage(42)},
		},
	})
	if len(back.Inflight) != 1 || back.Inflight[0].Stage != InflightAwaitPuback {
		t.Fatalf("stage = %v, want InflightAwaitPuback", back.Inflight[0].Stage)
	}
}

// TestStateStoredCopiesSubscriptionIDs pins that the stored form does not share the
// live subscription's identifier slice: a store is written from a snapshot, so a
// later SUBSCRIBE must not change what was already persisted.
func TestStateStoredCopiesSubscriptionIDs(t *testing.T) {
	live := &State{
		ClientID: "c",
		Subs: map[string]Subscription{
			"t": {QoS: 1, SubscriptionIDs: []uint32{1}},
		},
	}
	stored := live.Stored()
	live.Subs["t"].SubscriptionIDs[0] = 2
	if got := stored.Subscriptions["t"].SubscriptionIdentifiers; !reflect.DeepEqual(got, []uint32{1}) {
		t.Fatalf("stored identifiers = %v, want [1] after the live form changed", got)
	}
}

// TestCopySubscriptions pins the detachment the routing index and a take-over rely
// on: the copy is a new map with the same entries, and an empty set copies to nil.
func TestCopySubscriptions(t *testing.T) {
	if got := CopySubscriptions(nil); got != nil {
		t.Fatalf("CopySubscriptions(nil) = %v, want nil", got)
	}
	live := map[string]Subscription{"t": {QoS: 1, SubscriptionIDs: []uint32{3}}}
	copied := CopySubscriptions(live)
	live["t2"] = Subscription{QoS: 2}
	live["t"] = Subscription{QoS: 0}
	if len(copied) != 1 {
		t.Fatalf("copy = %+v, want only the filter it was made from", copied)
	}
	if got := copied["t"]; got.QoS != 1 || !reflect.DeepEqual(got.SubscriptionIDs, []uint32{3}) {
		t.Fatalf("copy entry = %+v, want the original options", got)
	}
}
