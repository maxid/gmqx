package session

// A session's live state
//
// State is the in-memory form of the state a client asked the server to keep: the
// subscriptions to restore, the messages published to those subscriptions while
// there was no connection, the QoS 2 state of both directions (the packet ids the
// client published without releasing them and the outbound exchanges it has not
// confirmed) and the deadline after which the state ends. The stored form is
// SessionState; Stored and StateFromStored are the projection between the two, and
// they are the only place where the typed MQTT 5 property blocks of the live form
// become the wire byte blocks a store persists.
//
// The broker keeps a State only for a session with no live connection. While a
// connection owns the session its subscriptions live on the connection and the
// connection has taken the queued messages over (they drain right after the
// CONNACK), so the store and the broker's live table never hold the same key at
// the same time. That invariant is what makes a publication reach an offline
// session exactly once: routing reads the offline half, the offline write path
// re-checks it, and a session that reconnected in between receives the message on
// its live connection instead of a stale queue.
//
// The reference broker of this family also queues for clean sessions; gmqx does
// not, because a clean session is deleted with its connection and recreated empty
// by the next CONNECT — a queued message would be delivered to a session that
// declared it wanted none.

import (
	"sort"
	"time"

	"github.com/maxid/gnety/codec/mqtt"

	mqtt5 "github.com/maxid/gmqx/internal/mqtt5"
)

// Subscription stores the MQTT 5 subscription options of one filter of a session.
type Subscription struct {
	// QoS is the granted maximum QoS of this filter.
	QoS byte
	// NoLocal marks a subscription that does not deliver a publication produced by
	// the same client id.
	NoLocal bool
	// RetainAsPublished keeps the retain flag of a retained publication as the
	// publisher set it, instead of clearing it for a live delivery.
	RetainAsPublished bool
	// RetainHandling decides whether a retained publication is replayed on
	// SUBSCRIBE: 0 always, 1 only when the subscription is new, 2 never.
	RetainHandling byte
	// SubscriptionIDs are the Subscription Identifiers the SUBSCRIBE that created
	// or modified this subscription carried (at most one per packet, §3.8.2.1.2,
	// but the field is a slice because it is part of the state a SessionStore
	// round-trips). They are attached to every PUBLISH delivered because of this
	// subscription.
	SubscriptionIDs []uint32
}

// Pending is one message a session owes its client, with its MQTT 5 properties in
// typed form (the stored form, QueuedMessage, keeps the property block instead).
type Pending struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	Props   mqtt.Properties
}

// Inflight is the persisted projection of one outbound QoS 1/2 exchange a session
// owes its client. It is what a State carries across a disconnect; the connection
// that resumes the session turns each one back into a live exchange with its
// retransmission schedule.
type Inflight struct {
	// PacketID is the packet identifier of the exchange, 1..65535.
	PacketID uint16
	// Topic and Payload are the publication being delivered.
	Topic   string
	Payload []byte
	// QoS is 1 or 2, Retain the retain flag of the PUBLISH.
	QoS    byte
	Retain bool
	// Props is the MQTT 5 PUBLISH property block in typed form.
	Props mqtt.Properties
	// Stage is where the exchange stood when the connection ended.
	Stage InflightStage
}

// State is the in-memory form of a stored session.
type State struct {
	// ClientID is the MQTT client identifier of the session.
	ClientID string
	// Subs are the session's topic filters, keyed by filter.
	Subs map[string]Subscription
	// Queue holds the messages published to the session while it was offline, in
	// publication order, plus the deliveries a connection still owed its inflight
	// window when it ended.
	Queue []Pending
	// ExpiresAt is when the session expires; the zero time means it does not.
	ExpiresAt time.Time
	// InboundQoS2 are the packet ids a client published at QoS 2 whose PUBREL has
	// not arrived yet; they are handed back to the connection that resumes the
	// session, so a retransmission is not delivered twice.
	InboundQoS2 []uint16
	// Inflight are the outbound exchanges the session never confirmed; the
	// resumed connection re-sends them under their original packet ids.
	Inflight []Inflight
}

// Stored converts the in-memory form into the form a SessionStore persists. The
// QoS 2 state of both directions travels with it: the inbound packet ids the
// client has not released and the outbound exchanges the client has not confirmed
// are what makes a reconnect continue the exchanges instead of restarting or losing
// them. Both are written in packet-id order, so the stored form of a session is
// reproducible.
func (s *State) Stored() SessionState {
	state := SessionState{ClientID: s.ClientID, ExpiresAt: s.ExpiresAt}
	if len(s.Subs) > 0 {
		state.Subscriptions = make(map[string]SessionSubscription, len(s.Subs))
		for filter, sub := range s.Subs {
			state.Subscriptions[filter] = SessionSubscription{
				QoS:               sub.QoS,
				NoLocal:           sub.NoLocal,
				RetainAsPublished: sub.RetainAsPublished,
				RetainHandling:    sub.RetainHandling,
				// The subscription identifier is part of the session state, so a
				// resumed session keeps tagging its deliveries.
				SubscriptionIdentifiers: append([]uint32(nil), sub.SubscriptionIDs...),
			}
		}
	}
	if len(s.Queue) > 0 {
		state.Queue = make([]QueuedMessage, 0, len(s.Queue))
		for _, msg := range s.Queue {
			props, err := mqtt5.EncodePropertyBlock(msg.Props)
			if err != nil {
				// A message whose properties cannot be encoded still travels; it
				// simply carries none, exactly like a will with invalid properties.
				props = nil
			}
			state.Queue = append(state.Queue, QueuedMessage{
				Topic:   msg.Topic,
				Payload: msg.Payload,
				QoS:     msg.QoS,
				Retain:  msg.Retain,
				Props:   props,
			})
		}
	}
	if len(s.InboundQoS2) > 0 {
		state.InboundQoS2 = append([]uint16(nil), s.InboundQoS2...)
		sort.Slice(state.InboundQoS2, func(i, j int) bool { return state.InboundQoS2[i] < state.InboundQoS2[j] })
	}
	if len(s.Inflight) > 0 {
		state.Inflight = make([]SessionInflight, 0, len(s.Inflight))
		for _, m := range s.Inflight {
			props, err := mqtt5.EncodePropertyBlock(m.Props)
			if err != nil {
				props = nil
			}
			state.Inflight = append(state.Inflight, SessionInflight{
				PacketID: m.PacketID,
				Topic:    m.Topic,
				Payload:  m.Payload,
				QoS:      m.QoS,
				Retain:   m.Retain,
				Props:    props,
				Stage:    m.Stage,
			})
		}
		sort.Slice(state.Inflight, func(i, j int) bool { return state.Inflight[i].PacketID < state.Inflight[j].PacketID })
	}
	return state
}

// StateFromStored converts the stored form into the in-memory form and reports how
// many queued messages had to be dropped because their property block no longer
// decodes. The QoS 2 state of both directions is restored with it; the
// retransmission schedule of the outbound exchanges is set by the connection that
// takes the session over.
//
// A store is an external input, so what cannot have been written by a broker is
// dropped instead of trusted: an outbound exchange that is not a QoS 1/2 PUBLISH
// with a packet id, and a queued message whose property block no longer decodes,
// which costs the message and not the session.
func StateFromStored(state SessionState) (*State, int) {
	s := &State{ClientID: state.ClientID, ExpiresAt: state.ExpiresAt, Subs: make(map[string]Subscription, len(state.Subscriptions))}
	for filter, sub := range state.Subscriptions {
		s.Subs[filter] = Subscription{
			QoS:               sub.QoS,
			NoLocal:           sub.NoLocal,
			RetainAsPublished: sub.RetainAsPublished,
			RetainHandling:    sub.RetainHandling,
			SubscriptionIDs:   sub.SubscriptionIdentifiers,
		}
	}
	if len(state.InboundQoS2) > 0 {
		s.InboundQoS2 = append([]uint16(nil), state.InboundQoS2...)
	}
	if len(state.Inflight) > 0 {
		s.Inflight = make([]Inflight, 0, len(state.Inflight))
		for _, stored := range state.Inflight {
			if stored.PacketID == 0 || stored.QoS == 0 || stored.QoS > 2 {
				// An exchange that cannot have been a QoS 1/2 PUBLISH is dropped
				// instead of being retransmitted with a shape the protocol does not
				// allow.
				continue
			}
			props, err := mqtt5.DecodePropertyBlock(stored.Props)
			if err != nil {
				// A property block that no longer decodes costs the properties, not
				// the message: the exchange still has its topic, payload and packet id.
				props = mqtt.Properties{}
			}
			s.Inflight = append(s.Inflight, Inflight{
				PacketID: stored.PacketID,
				Topic:    stored.Topic,
				Payload:  stored.Payload,
				QoS:      stored.QoS,
				Retain:   stored.Retain,
				Props:    props,
				Stage:    knownStage(stored.Stage),
			})
		}
	}
	dropped := 0
	if len(state.Queue) > 0 {
		s.Queue = make([]Pending, 0, len(state.Queue))
		for _, msg := range state.Queue {
			props, err := mqtt5.DecodePropertyBlock(msg.Props)
			if err != nil {
				dropped++
				continue
			}
			s.Queue = append(s.Queue, Pending{
				Topic:   msg.Topic,
				Payload: msg.Payload,
				QoS:     msg.QoS,
				Retain:  msg.Retain,
				Props:   props,
			})
		}
	}
	return s, dropped
}

// knownStage normalises the stage a store hands back. An unknown value (a store
// written by a build that knew another stage) is treated as a QoS 1 PUBLISH waiting
// for its PUBACK, which re-sends the packet instead of dropping it.
func knownStage(stage InflightStage) InflightStage {
	switch stage {
	case InflightAwaitPubrec, InflightAwaitPubcomp:
		return stage
	default:
		return InflightAwaitPuback
	}
}

// CopySubscriptions returns a detached copy of a session's subscription set, so a
// stored session, a live connection and the routing index never share one map.
func CopySubscriptions(subs map[string]Subscription) map[string]Subscription {
	if len(subs) == 0 {
		return nil
	}
	out := make(map[string]Subscription, len(subs))
	for filter, sub := range subs {
		out[filter] = sub
	}
	return out
}
