package session

// A session's live state
//
// State is the in-memory form of the state a client asked the server to keep: the
// subscriptions to restore, the messages published to those subscriptions while
// there was no connection, the QoS 2 state of both directions (the packet ids the
// client published without releasing them and the outbound exchanges it has not
// confirmed) and the deadline after which the state ends.
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
// Persistence is not this package's business. The wire form a shared store keeps
// (and the projection between this form and it) lives in gmqx/internal/persist,
// because the same store also keeps retained messages: one persistence seam for
// both, keyed and encoded there.

import (
	"time"

	"github.com/maxid/gnety/codec/mqtt"
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
	// but the field is a slice because it is part of the state a shared store
	// round-trips). They are attached to every PUBLISH delivered because of this
	// subscription.
	SubscriptionIDs []uint32
}

// Pending is one message a session owes its client, with its MQTT 5 properties in
// typed form (the persisted form keeps the property block instead).
type Pending struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	Props   mqtt.Properties
}

// Stage is where the outbound exchange of one session stands: a QoS 1 PUBLISH
// waiting for its PUBACK, a QoS 2 PUBLISH waiting for its PUBREC, or a PUBREL
// waiting for its PUBCOMP. It is state of the exchange, not of the connection, so
// a resumed session re-sends the exchange in the stage it was left in.
type Stage byte

const (
	// AwaitPuback is a QoS 1 PUBLISH whose PUBACK never arrived.
	AwaitPuback Stage = iota
	// AwaitPubrec is a QoS 2 PUBLISH whose PUBREC never arrived.
	AwaitPubrec
	// AwaitPubcomp is a QoS 2 PUBLISH that was confirmed with PUBREC whose PUBREL
	// has not been answered with PUBCOMP yet.
	AwaitPubcomp
)

// Inflight is one outbound QoS 1/2 exchange a session owes its client.
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
	Stage Stage
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
