package gmqx

import "github.com/maxid/gnety/codec/mqtt"

// Subscription identifiers (MQTT 5 §3.8.2.1.2, §3.8.3.1, §3.3.2.3.8)
//
// A SUBSCRIBE may carry a Subscription Identifier (property 0x0B). It is stored
// with the subscription that packet creates or modifies, and it is copied into
// every PUBLISH the broker sends because of that subscription. The state belongs
// to the subscription, so it is part of what a persistent session keeps: it
// travels through the SessionStore (SessionSubscription.SubscriptionIdentifiers)
// and is restored on a resume, exactly like the filter's QoS, no-local,
// retain-as-published and retain-handling.
//
// The identifier is hop-local and per subscription, never part of a publication:
// a PUBLISH that arrives from a client is stripped of it (client.handlePublish),
// it does not travel through Publication.Props to another node, and a node that
// delivers a publication attaches the identifiers of its own subscriptions when
// it builds the PUBLISH (Broker.SendLocal). One publication that matches several
// subscriptions of the same session therefore carries all of their identifiers,
// deduplicated (§3.3.2.3.8: "If the Server sends a single copy of the message it
// includes the Subscription Identifier for each of the subscriptions").
//
// Two bounds keep the property block bounded:
//
//   - maxSubscriptionIdentifiersPerSubscription caps what one subscription
//     stores. The codec delivers at most one identifier per SUBSCRIBE (the
//     specification forbids repeating the property in SUBSCRIBE, §3.8.2.1.2), so
//     the cap only matters for state restored from a SessionStore that another
//     implementation wrote, and for a subscription modified by several packets.
//   - maxSubscriptionIdentifiersPerDelivery caps one outgoing PUBLISH. Identifiers
//     beyond it are not attached and the drop is counted
//     (Metrics.RecordDroppedMessage("subscription_identifier_limit")), so a
//     session with many matching subscriptions cannot grow a property block
//     without bound.
const (
	// maxSubscriptionIdentifier is the highest value MQTT 5 §3.8.2.1.2 allows:
	// 268,435,455 (the largest Variable Byte Integer). 0 is a Protocol Error.
	maxSubscriptionIdentifier uint32 = 268435455
	// maxSubscriptionIdentifiersPerSubscription bounds the identifiers one
	// subscription stores.
	maxSubscriptionIdentifiersPerSubscription = 8
	// maxSubscriptionIdentifiersPerDelivery bounds the identifiers one outgoing
	// PUBLISH carries.
	maxSubscriptionIdentifiersPerDelivery = 8
)

// normalizeSubscriptionIDs validates and bounds the identifiers of one SUBSCRIBE.
// It reports ok=false when a value is out of range, which is a Protocol Error
// (§3.8.2.1.2) and ends the connection, and how many in-range identifiers were
// dropped because the subscription already holds the maximum.
func normalizeSubscriptionIDs(ids []uint32) (kept []uint32, dropped int, ok bool) {
	if len(ids) == 0 {
		return nil, 0, true
	}
	kept = make([]uint32, 0, min(len(ids), maxSubscriptionIdentifiersPerSubscription))
	for _, id := range ids {
		if id == 0 || id > maxSubscriptionIdentifier {
			return nil, 0, false
		}
		if len(kept) >= maxSubscriptionIdentifiersPerSubscription {
			dropped++
			continue
		}
		kept = append(kept, id)
	}
	return dedupeSubscriptionIDs(kept), dropped, true
}

// mergeSubscriptionIDs returns the union of have and add, deduplicated, without
// mutating either. It is how a publication that matches several subscriptions of
// one session collects their identifiers.
func mergeSubscriptionIDs(have, add []uint32) []uint32 {
	if len(add) == 0 {
		return have
	}
	if len(have) == 0 {
		return add
	}
	out := make([]uint32, 0, len(have)+len(add))
	out = append(out, have...)
	out = append(out, add...)
	return dedupeSubscriptionIDs(out)
}

// dedupeSubscriptionIDs removes repeated identifiers, keeping the first
// occurrence, and returns the input unchanged when it holds no duplicate (the
// common case: one identifier per subscription). It never writes into ids, which
// may be the slice an indexed subscription stores.
func dedupeSubscriptionIDs(ids []uint32) []uint32 {
	if len(ids) < 2 {
		return ids
	}
	seen := make(map[uint32]struct{}, len(ids))
	out := make([]uint32, 0, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	if len(out) == len(ids) {
		return ids
	}
	return out
}

// capSubscriptionIDs bounds the identifiers of one delivery, and reports whether
// it had to drop any.
func capSubscriptionIDs(ids []uint32) ([]uint32, bool) {
	if len(ids) <= maxSubscriptionIdentifiersPerDelivery {
		return ids, false
	}
	return ids[:maxSubscriptionIdentifiersPerDelivery], true
}

// withSubscriptionIDs returns props with the identifiers of the subscriptions the
// delivery was made through. It reports whether the delivery cap dropped any, so
// the caller can count it. props is a value type, so the caller's copy is never
// touched.
func withSubscriptionIDs(props mqtt.Properties, ids []uint32) (mqtt.Properties, bool) {
	if len(ids) == 0 {
		return props, false
	}
	capped, dropped := capSubscriptionIDs(ids)
	props.SubscriptionIdentifiers = capped
	return props, dropped
}
