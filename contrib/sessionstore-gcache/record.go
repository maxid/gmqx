package gcachestore

// The stored record
//
// A SessionState is encoded as an explicit, versioned JSON object instead of a
// marshalled struct, so renaming a Go field cannot change the stored layout and a
// record written by another build is recognised by its version. The field names
// and the version are the ones gmqx's own RedisSessionStore writes, so a value
// stored by one of the two can be read by the other when a deployment moves
// between them (only the key prefix differs).
//
// The structural fields are always emitted, even when empty: `"subs":null` and
// `"subs":{}` are different states, and a store must round-trip a SessionState
// exactly, not approximately. Payload and property bytes travel as base64 (the
// encoding/json representation of []byte), which round-trips them byte for byte,
// NUL bytes included.

import (
	"encoding/json"
	"fmt"
	"time"

	gmqx "github.com/maxid/gmqx"
)

// recordVersion is the version of the stored value. A record of another version
// is treated like an undecodable one: dropped and counted, never guessed at.
const recordVersion = 1

// record is the stored form of one gmqx.SessionState.
type record struct {
	Version int `json:"v"`
	// ClientID is the session's client id; the key carries it too, but a state
	// handed over must name its own session.
	ClientID string `json:"client"`
	// ExpiresAt is the session deadline in Unix nanoseconds; 0 means "never",
	// which is MQTT 3.1.1 and a broker without a Session Expiry Interval.
	ExpiresAt     int64                         `json:"expires"`
	Subscriptions map[string]recordSubscription `json:"subs"`
	Queue         []recordMessage               `json:"queue"`
	InboundQoS2   []uint16                      `json:"in_qos2"`
	Inflight      []recordInflight              `json:"inflight"`
}

// recordSubscription is one stored subscription with its MQTT 5 options; the
// identifiers belong to the subscription, not to the connection.
type recordSubscription struct {
	QoS                     byte     `json:"qos"`
	NoLocal                 bool     `json:"nl"`
	RetainAsPublished       bool     `json:"rap"`
	RetainHandling          byte     `json:"rh"`
	SubscriptionIdentifiers []uint32 `json:"si"`
}

// recordMessage is one queued message: topic, payload, QoS, retain flag and the
// MQTT 5 PUBLISH property block without its length prefix. It carries no packet
// id: the broker assigns one when the queue drains.
type recordMessage struct {
	Topic   string `json:"t"`
	Payload []byte `json:"p"`
	QoS     byte   `json:"q"`
	Retain  bool   `json:"r"`
	Props   []byte `json:"pp"`
}

// recordInflight is one unconfirmed outbound QoS 1/2 exchange, with the packet id
// it must be resent under and the stage it reached.
type recordInflight struct {
	PacketID uint16 `json:"id"`
	Topic    string `json:"t"`
	Payload  []byte `json:"p"`
	QoS      byte   `json:"q"`
	Retain   bool   `json:"r"`
	Props    []byte `json:"pp"`
	Stage    byte   `json:"s"`
}

// encodeState turns a SessionState into the stored value.
//
// The collections are allocated only when the state's are non-nil, so nil and
// empty survive the trip. Byte slices are copied: the caller may reuse its buffer
// as soon as Save returns, while encoding/json runs before that here only because
// the copy is taken explicitly.
func encodeState(state gmqx.SessionState) ([]byte, error) {
	rec := record{
		Version:   recordVersion,
		ClientID:  state.ClientID,
		ExpiresAt: 0,
	}
	if !state.ExpiresAt.IsZero() {
		rec.ExpiresAt = state.ExpiresAt.UnixNano()
	}
	if state.Subscriptions != nil {
		rec.Subscriptions = make(map[string]recordSubscription, len(state.Subscriptions))
		for filter, sub := range state.Subscriptions {
			rec.Subscriptions[filter] = recordSubscription{
				QoS:                     sub.QoS,
				NoLocal:                 sub.NoLocal,
				RetainAsPublished:       sub.RetainAsPublished,
				RetainHandling:          sub.RetainHandling,
				SubscriptionIdentifiers: cloneUint32s(sub.SubscriptionIdentifiers),
			}
		}
	}
	if state.Queue != nil {
		rec.Queue = make([]recordMessage, 0, len(state.Queue))
		for _, message := range state.Queue {
			rec.Queue = append(rec.Queue, recordMessage{
				Topic:   message.Topic,
				Payload: cloneBytes(message.Payload),
				QoS:     message.QoS,
				Retain:  message.Retain,
				Props:   cloneBytes(message.Props),
			})
		}
	}
	if state.InboundQoS2 != nil {
		rec.InboundQoS2 = cloneUint16s(state.InboundQoS2)
	}
	if state.Inflight != nil {
		rec.Inflight = make([]recordInflight, 0, len(state.Inflight))
		for _, exchange := range state.Inflight {
			rec.Inflight = append(rec.Inflight, recordInflight{
				PacketID: exchange.PacketID,
				Topic:    exchange.Topic,
				Payload:  cloneBytes(exchange.Payload),
				QoS:      exchange.QoS,
				Retain:   exchange.Retain,
				Props:    cloneBytes(exchange.Props),
				Stage:    byte(exchange.Stage),
			})
		}
	}
	return json.Marshal(rec)
}

// decodeState turns a stored value back into a SessionState. A value that does
// not decode, or carries another record version, is an error; the caller drops
// the value and counts it instead of failing the reconnect.
func decodeState(raw []byte) (gmqx.SessionState, error) {
	var rec record
	if err := json.Unmarshal(raw, &rec); err != nil {
		return gmqx.SessionState{}, fmt.Errorf("decode stored session: %w", err)
	}
	if rec.Version != recordVersion {
		return gmqx.SessionState{}, fmt.Errorf("stored session version %d, want %d", rec.Version, recordVersion)
	}

	state := gmqx.SessionState{ClientID: rec.ClientID}
	if rec.ExpiresAt != 0 {
		state.ExpiresAt = time.Unix(0, rec.ExpiresAt)
	}
	if rec.Subscriptions != nil {
		state.Subscriptions = make(map[string]gmqx.SessionSubscription, len(rec.Subscriptions))
		for filter, sub := range rec.Subscriptions {
			state.Subscriptions[filter] = gmqx.SessionSubscription{
				QoS:                     sub.QoS,
				NoLocal:                 sub.NoLocal,
				RetainAsPublished:       sub.RetainAsPublished,
				RetainHandling:          sub.RetainHandling,
				SubscriptionIdentifiers: cloneUint32s(sub.SubscriptionIdentifiers),
			}
		}
	}
	if rec.Queue != nil {
		state.Queue = make([]gmqx.QueuedMessage, 0, len(rec.Queue))
		for _, message := range rec.Queue {
			state.Queue = append(state.Queue, gmqx.QueuedMessage{
				Topic:   message.Topic,
				Payload: message.Payload,
				QoS:     message.QoS,
				Retain:  message.Retain,
				Props:   message.Props,
			})
		}
	}
	if rec.InboundQoS2 != nil {
		state.InboundQoS2 = cloneUint16s(rec.InboundQoS2)
	}
	if rec.Inflight != nil {
		state.Inflight = make([]gmqx.SessionInflight, 0, len(rec.Inflight))
		for _, exchange := range rec.Inflight {
			state.Inflight = append(state.Inflight, gmqx.SessionInflight{
				PacketID: exchange.PacketID,
				Topic:    exchange.Topic,
				Payload:  exchange.Payload,
				QoS:      exchange.QoS,
				Retain:   exchange.Retain,
				Props:    exchange.Props,
				Stage:    gmqx.InflightStage(exchange.Stage),
			})
		}
	}
	return state, nil
}

// cloneBytes copies b, keeping nil and empty distinct: a store must return the
// state it was given, and a caller may rely on the difference.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// cloneUint32s is cloneBytes for subscription identifiers.
func cloneUint32s(in []uint32) []uint32 {
	if in == nil {
		return nil
	}
	out := make([]uint32, len(in))
	copy(out, in)
	return out
}

// cloneUint16s is cloneBytes for inbound QoS 2 packet ids.
func cloneUint16s(in []uint16) []uint16 {
	if in == nil {
		return nil
	}
	out := make([]uint16, len(in))
	copy(out, in)
	return out
}
