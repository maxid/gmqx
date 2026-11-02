package routing

// The routing seam's vocabulary: what the index matches sessions against (the
// filters they hold, in gmqx/session) and what it hands back for a publication (a
// Target, which the broker either writes to a live connection or queues for a
// stored session).

import (
	"github.com/maxid/gnety/codec/mqtt"

	"github.com/maxid/gmqx/hook"
)

// ConnectionID identifies one physical connection, as the broker and its hooks
// name it. It is the hook package's type so a Target carries the same value the
// publication event does.
type ConnectionID = hook.ConnectionID

// Endpoint is one live connection as the routing index sees it: the identity a
// routed session is matched against, and the handle a target delivers through.
//
// The three methods are what the index needs and nothing else. The index compares
// two Endpoints to know whether a connection still owns a session: a subscription
// or unsubscribe from a connection a take-over displaced must not steal the
// routing of the session its replacement now owns. An implementation must
// therefore be a comparable value, which a pointer is; the broker's own
// connection type is one.
type Endpoint interface {
	// ClientID is the MQTT client identifier the connection is registered under.
	ClientID() string
	// ConnectionID names the physical connection, which is what a no-local
	// subscription is compared against.
	ConnectionID() ConnectionID
	// DeliverPublish writes (or queues) one PUBLISH to the connection. props is
	// the publication's property block in typed form, already carrying the
	// subscription identifiers of the subscriptions the target was reached through.
	DeliverPublish(topic string, payload []byte, qos byte, retain bool, props mqtt.Properties) error
}

// Target is one local session a publication matches. Its fields are read-only
// reporting; pass the value back to the broker's SendLocal to write the PUBLISH.
// A Target is only valid for the index (and so the broker) that produced it.
type Target struct {
	ClientID     string
	ConnectionID ConnectionID
	// QoS is min(publication QoS, highest matching subscription QoS).
	QoS byte
	// Retain is the retain flag to send: it is set only for a retained
	// publication delivered to a subscription with retain-as-published.
	Retain bool

	endpoint   Endpoint
	sessionKey string
	// subscriptionIDs are the Subscription Identifiers of the subscriptions the
	// target was reached through, deduplicated. SendLocal attaches them to the
	// PUBLISH it writes (or queues), which is what makes the identifier a property
	// of the subscription rather than of the publication.
	subscriptionIDs []uint32
}

// Offline reports whether the target is a persistent session with no live
// connection. SendLocal queues the publication for such a target instead of
// writing it, and the queue drains right after the session's next CONNACK;
// ConnectionID is empty for it, because there is no connection to name.
func (t Target) Offline() bool { return t.endpoint == nil && t.sessionKey != "" }

// Endpoint returns the live connection the target delivers through, nil for an
// offline target.
func (t Target) Endpoint() Endpoint { return t.endpoint }

// SessionKey returns the store key of an offline target and "" for a live one.
// It is what SendLocal queues through when the session has no connection.
func (t Target) SessionKey() string { return t.sessionKey }

// SubscriptionIDs are the Subscription Identifiers the target was reached
// through, deduplicated and capped. They are attached to the PUBLISH of this
// target only.
func (t Target) SubscriptionIDs() []uint32 { return t.subscriptionIDs }

// OfflineTarget builds the target of a stored session with no live connection. It
// is how a caller that already knows the client id and its store key — the
// broker's own overflow path — names a session without going through a match.
func OfflineTarget(clientID, sessionKey string, qos byte, retain bool) Target {
	return Target{ClientID: clientID, QoS: qos, Retain: retain, sessionKey: sessionKey}
}
