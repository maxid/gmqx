package persist

// The wire forms of the two records
//
// A record is JSON with an explicit version, because the store outlives the
// process that wrote it: a build that does not know the version drops the value
// instead of guessing, and a newer build can read what an older one wrote. The
// property blocks travel as bytes — the wire form a PUBLISH carries — not as the
// typed MQTT 5 structure, so a record written by one build reads in the next even
// if the codec's property type grows a field.

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/maxid/gnety/codec/mqtt"

	mqtt5 "github.com/maxid/gmqx/internal/mqtt5"
	"github.com/maxid/gmqx/retained"
	"github.com/maxid/gmqx/session"
)

// Errors returned by New. They name a configuration that cannot work, so a
// deployment fails at startup instead of on the first reconnect.
var (
	// ErrStoreRequired means Options.Remote is nil.
	ErrStoreRequired = errors.New("persist: a gcache.RemoteStore is required")
	// ErrInvalidPrefix means Options.Prefix contains a SCAN glob character. A store
	// that can enumerate uses the prefix as a MATCH pattern, so such a prefix would
	// scan keys the store does not own.
	ErrInvalidPrefix = errors.New("persist: prefix must not contain SCAN glob characters")
)

// recordVersion is the version of both wire forms. Bump it when a field changes
// meaning; a record of another version is dropped and counted, never guessed.
const recordVersion = 1

// sessionRecord is the stored form of one persistent session.
type sessionRecord struct {
	Version       int                           `json:"v"`
	ClientID      string                        `json:"client_id"`
	Subscriptions map[string]subscriptionRecord `json:"subscriptions,omitempty"`
	Queue         []messageRecord               `json:"queue,omitempty"`
	ExpiresAt     time.Time                     `json:"expires_at,omitempty"`
	InboundQoS2   []uint16                      `json:"inbound_qos2,omitempty"`
	Inflight      []inflightRecord              `json:"inflight,omitempty"`
}

// subscriptionRecord is one stored subscription.
type subscriptionRecord struct {
	QoS               byte     `json:"qos"`
	NoLocal           bool     `json:"no_local,omitempty"`
	RetainAsPublished bool     `json:"retain_as_published,omitempty"`
	RetainHandling    byte     `json:"retain_handling,omitempty"`
	SubscriptionIDs   []uint32 `json:"subscription_ids,omitempty"`
}

// messageRecord is one queued message.
type messageRecord struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload,omitempty"`
	QoS     byte   `json:"qos"`
	Retain  bool   `json:"retain,omitempty"`
	Props   []byte `json:"props,omitempty"`
}

// inflightRecord is one unconfirmed outbound exchange.
type inflightRecord struct {
	PacketID uint16 `json:"packet_id"`
	Topic    string `json:"topic"`
	Payload  []byte `json:"payload,omitempty"`
	QoS      byte   `json:"qos"`
	Retain   bool   `json:"retain,omitempty"`
	Props    []byte `json:"props,omitempty"`
	Stage    byte   `json:"stage"`
}

// retainedRecord is the stored form of one retained message.
type retainedRecord struct {
	Version   int       `json:"v"`
	Namespace string    `json:"namespace,omitempty"`
	Topic     string    `json:"topic"`
	Payload   []byte    `json:"payload,omitempty"`
	QoS       byte      `json:"qos"`
	Props     []byte    `json:"props,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// encodeSession converts the live form into the wire form. The QoS 2 packet ids
// and the exchanges are written in packet-id order, so a session's stored form is
// reproducible.
func encodeSession(st *session.State) ([]byte, error) {
	rec := sessionRecord{
		Version:   recordVersion,
		ClientID:  st.ClientID,
		ExpiresAt: st.ExpiresAt,
	}
	if len(st.Subs) > 0 {
		rec.Subscriptions = make(map[string]subscriptionRecord, len(st.Subs))
		for filter, sub := range st.Subs {
			rec.Subscriptions[filter] = subscriptionRecord{
				QoS:               sub.QoS,
				NoLocal:           sub.NoLocal,
				RetainAsPublished: sub.RetainAsPublished,
				RetainHandling:    sub.RetainHandling,
				SubscriptionIDs:   append([]uint32(nil), sub.SubscriptionIDs...),
			}
		}
	}
	if len(st.Queue) > 0 {
		rec.Queue = make([]messageRecord, 0, len(st.Queue))
		for _, msg := range st.Queue {
			props, err := mqtt5.EncodePropertyBlock(msg.Props)
			if err != nil {
				// A message whose properties cannot be encoded still travels; it
				// simply carries none, exactly like a will with invalid properties.
				props = nil
			}
			rec.Queue = append(rec.Queue, messageRecord{
				Topic:   msg.Topic,
				Payload: msg.Payload,
				QoS:     msg.QoS,
				Retain:  msg.Retain,
				Props:   props,
			})
		}
	}
	if len(st.InboundQoS2) > 0 {
		rec.InboundQoS2 = append([]uint16(nil), st.InboundQoS2...)
		sort.Slice(rec.InboundQoS2, func(i, j int) bool { return rec.InboundQoS2[i] < rec.InboundQoS2[j] })
	}
	if len(st.Inflight) > 0 {
		rec.Inflight = make([]inflightRecord, 0, len(st.Inflight))
		for _, m := range st.Inflight {
			props, err := mqtt5.EncodePropertyBlock(m.Props)
			if err != nil {
				props = nil
			}
			rec.Inflight = append(rec.Inflight, inflightRecord{
				PacketID: m.PacketID,
				Topic:    m.Topic,
				Payload:  m.Payload,
				QoS:      m.QoS,
				Retain:   m.Retain,
				Props:    props,
				Stage:    byte(m.Stage),
			})
		}
		sort.Slice(rec.Inflight, func(i, j int) bool { return rec.Inflight[i].PacketID < rec.Inflight[j].PacketID })
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("encode session: %w", err)
	}
	return raw, nil
}

// decodeSession converts a stored form back. A record of another version, or one
// that cannot be parsed, is refused. What cannot have been written by a broker is
// dropped instead of trusted: an exchange that is not a QoS 1/2 PUBLISH with a
// packet id, and a queued message whose property block no longer decodes (which
// costs the message, not the session).
func (s *Store) decodeSession(key string, raw []byte) (*session.State, bool) {
	var rec sessionRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		s.corrupt("session", key, err)
		return nil, false
	}
	if rec.Version != recordVersion {
		s.corrupt("session", key, fmt.Errorf("record version %d", rec.Version))
		return nil, false
	}
	st := &session.State{
		ClientID:  rec.ClientID,
		ExpiresAt: rec.ExpiresAt,
		Subs:      make(map[string]session.Subscription, len(rec.Subscriptions)),
	}
	for filter, sub := range rec.Subscriptions {
		st.Subs[filter] = session.Subscription{
			QoS:               sub.QoS,
			NoLocal:           sub.NoLocal,
			RetainAsPublished: sub.RetainAsPublished,
			RetainHandling:    sub.RetainHandling,
			SubscriptionIDs:   sub.SubscriptionIDs,
		}
	}
	if len(rec.InboundQoS2) > 0 {
		st.InboundQoS2 = append([]uint16(nil), rec.InboundQoS2...)
	}
	if len(rec.Inflight) > 0 {
		st.Inflight = make([]session.Inflight, 0, len(rec.Inflight))
		for _, stored := range rec.Inflight {
			if stored.PacketID == 0 || stored.QoS == 0 || stored.QoS > 2 {
				// An exchange that cannot have been a QoS 1/2 PUBLISH is dropped
				// instead of retransmitted with a shape the protocol forbids.
				continue
			}
			props, err := mqtt5.DecodePropertyBlock(stored.Props)
			if err != nil {
				// A property block that no longer decodes costs the properties, not
				// the message: the exchange keeps its topic, payload and packet id.
				props = mqtt.Properties{}
			}
			st.Inflight = append(st.Inflight, session.Inflight{
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
	if len(rec.Queue) > 0 {
		st.Queue = make([]session.Pending, 0, len(rec.Queue))
		for _, msg := range rec.Queue {
			props, err := mqtt5.DecodePropertyBlock(msg.Props)
			if err != nil {
				dropped++
				continue
			}
			st.Queue = append(st.Queue, session.Pending{
				Topic:   msg.Topic,
				Payload: msg.Payload,
				QoS:     msg.QoS,
				Retain:  msg.Retain,
				Props:   props,
			})
		}
	}
	if dropped > 0 {
		s.log.Warn("gmqx: dropped queued messages with an undecodable property block",
			"key", key, "count", dropped)
		s.count("offline_queue_corrupt")
	}
	return st, true
}

// knownStage normalises the stage a store hands back. An unknown value (a record
// written by a build that knew another stage) is treated as a QoS 1 PUBLISH
// waiting for its PUBACK, which re-sends the packet instead of dropping it.
func knownStage(stage byte) session.Stage {
	switch session.Stage(stage) {
	case session.AwaitPubrec, session.AwaitPubcomp:
		return session.Stage(stage)
	default:
		return session.AwaitPuback
	}
}

// encodeRetained converts one retained message into its wire form.
func encodeRetained(m retained.Message) ([]byte, error) {
	props, err := mqtt5.EncodePropertyBlock(m.Props)
	if err != nil {
		props = nil
	}
	raw, err := json.Marshal(retainedRecord{
		Version:   recordVersion,
		Namespace: m.Namespace,
		Topic:     m.Topic,
		Payload:   m.Payload,
		QoS:       m.QoS,
		Props:     props,
		ExpiresAt: m.ExpiresAt,
	})
	if err != nil {
		return nil, fmt.Errorf("encode retained: %w", err)
	}
	return raw, nil
}

// decodeRetained converts a stored retained message back, dropping a record it
// cannot trust.
func (s *Store) decodeRetained(key string, raw []byte) (retained.Message, bool) {
	var rec retainedRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		s.corrupt("retained", key, err)
		return retained.Message{}, false
	}
	if rec.Version != recordVersion {
		s.corrupt("retained", key, fmt.Errorf("record version %d", rec.Version))
		return retained.Message{}, false
	}
	props, err := mqtt5.DecodePropertyBlock(rec.Props)
	if err != nil {
		// The property block is a cost, not the message: keep the payload.
		props = mqtt.Properties{}
	}
	return retained.Message{
		Namespace: rec.Namespace,
		Topic:     rec.Topic,
		Payload:   rec.Payload,
		QoS:       rec.QoS,
		Props:     props,
		ExpiresAt: rec.ExpiresAt,
	}, true
}

// corrupt records a stored value this build cannot use.
func (s *Store) corrupt(kind, key string, err error) {
	s.log.Warn("gmqx: dropping an unreadable stored value", "kind", kind, "key", key, "err", err)
	if kind == "retained" {
		s.count("retained_store_corrupt")
		return
	}
	s.count("session_store_corrupt")
}
