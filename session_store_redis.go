package gmqx

// Redis-backed SessionStore
//
// The in-memory store keeps a session on the node that happens to hold it, so a
// client that reconnects to another node of a cluster loses its subscriptions and
// the messages queued for it while it was away. This store moves that state into
// the Redis a cluster backend already shares, so the node the client reconnects to
// takes the session over instead of starting from nothing.
//
// It is opt-in: Config.SessionStore keeps MemorySessionStore unless a deployment
// builds this store and passes it, and NewRedisClusterWithSessionStore builds the
// Redis backend and the store from one configuration.
//
// Key layout under the configured prefix (the same prefix the Redis cluster
// backend uses, "gmqx:cluster" by default):
//
//	<prefix>:session:<key>   JSON redisSessionRecord of one stored session, where
//	                         <key> is the key the broker passes to the store —
//	                         namespaceKey(Config.Namespace, ClientID) — so a
//	                         session key of one namespace cannot collide with one
//	                         of another, and the segment "session" cannot collide
//	                         with a cluster key ("node", "owner", "member", "subs",
//	                         "client") under the same prefix.
//
// The TTL of a key is the session's deadline, not a fixed cache lifetime: a
// SessionState with an ExpiresAt is written with the remaining time as its TTL, so
// Redis removes an abandoned session at exactly the moment the broker would stop
// resuming it, and a state without a deadline (MQTT 3.1.1, which has no Session
// Expiry Interval) is written without a TTL and lives until the broker deletes it.
// A state whose deadline has already passed is not written at all: the session is
// over, so the key is deleted instead.
//
// A value that cannot be decoded is dropped and counted
// (Metrics.RecordDroppedMessage("session_store_corrupt")) instead of failing a
// CONNECT: the session is lost, but the broker keeps serving. A Redis failure is
// counted as "session_store_error" and degrades the same way — Load reports "no
// session" — which is the at-most-once behavior the cluster itself has.
//
// Save replaces the whole value, and the broker's session lock serialises the
// writers of one node only: two nodes that queue a publication for the same stored
// session at the same instant can lose one of them (last writer wins). The queue of
// an offline session is therefore effectively single-writer — the node that stored
// it — which is the assumption a node-local queue makes as well.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisSessionKeySegment is the key segment session state lives under, next to the
// cluster backend's own segments.
const redisSessionKeySegment = "session"

// redisSessionScanCount bounds one SCAN round trip of Keys; it is a page size, not
// a limit on how many sessions a store may hold.
const redisSessionScanCount = 500

// redisSessionTakeScript removes a session key and returns its value in one round
// trip, so two nodes cannot both be handed the same session. It is a script rather
// than GETDEL so a Redis older than 6.2 can still serve the store.
const redisSessionTakeScript = `
local v = redis.call('GET', KEYS[1])
if v then
  redis.call('DEL', KEYS[1])
end
return v
`

// RedisSessionStoreConfig configures the Redis-backed SessionStore.
//
// Zero values select the documented default; a negative duration or DB is refused
// by NewRedisSessionStore.
type RedisSessionStoreConfig struct {
	// Addr is the Redis server address ("host:port"). Required.
	Addr string
	// Password is the Redis password; empty means no AUTH.
	Password string
	// DB is the Redis database index (default 0). It must be the one the cluster
	// backend uses, or a node would read another database's sessions.
	DB int
	// ChannelPrefix is the key namespace, and must be the cluster backend's
	// ChannelPrefix (default "gmqx:cluster"): the store writes
	// "<prefix>:session:<key>", next to that backend's own keys. It must not
	// contain SCAN glob characters (`*`, `?`, `[`, `]`, `\`), because Keys uses it
	// in a SCAN pattern.
	ChannelPrefix string

	// DialTimeout bounds one connection attempt (default 2s) and IOTimeout one
	// command (default 2s). The broker calls the store with its own session lock
	// held, so an unreachable Redis must cost a bounded wait, never an unbounded
	// one.
	DialTimeout time.Duration
	IOTimeout   time.Duration

	// Metrics, when set, receives RecordDroppedMessage for what the store drops
	// ("session_store_corrupt" for a value that does not decode,
	// "session_store_error" for a failed command).
	Metrics MetricsManager
	// Logger receives decode and command failures (default slog.Default()).
	Logger *slog.Logger
}

// RedisSessionStore is a SessionStore shared by every node of a Redis cluster: the
// stored session of a client id lives in Redis, so the node the client reconnects
// to takes it over.
//
// It is safe for concurrent use by one broker and by several brokers of one
// process; the shared state itself lives in Redis, which is what makes it the
// rendezvous between processes. Every operation is a bounded Redis round trip.
type RedisSessionStore struct {
	cfg    RedisSessionStoreConfig
	log    *slog.Logger
	rdb    *redis.Client
	prefix string
	drops  atomic.Uint64
}

// NewRedisSessionStore builds the Redis-backed session store and validates its
// configuration. The error is a configuration error a deployment should fail on at
// startup, not a Redis error: Redis is contacted on the first Load, Save or Take,
// so a broker can start while Redis is down (and count "session_store_error" until
// it is back).
func NewRedisSessionStore(cfg RedisSessionStoreConfig) (*RedisSessionStore, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, fmt.Errorf("gmqx: redis session store needs an Addr")
	}
	if cfg.DB < 0 {
		return nil, fmt.Errorf("gmqx: redis session store DB must not be negative, got %d", cfg.DB)
	}
	if strings.ContainsAny(cfg.ChannelPrefix, `*?[]\`) {
		return nil, fmt.Errorf("gmqx: redis session store ChannelPrefix must not contain SCAN glob characters, got %q", cfg.ChannelPrefix)
	}
	if cfg.DialTimeout < 0 || cfg.IOTimeout < 0 {
		return nil, fmt.Errorf("gmqx: redis session store timeouts must not be negative")
	}
	prefix := cfg.ChannelPrefix
	if prefix == "" {
		prefix = defaultRedisChannelPrefix
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	opts := &redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.IOTimeout,
		WriteTimeout: cfg.IOTimeout,
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = redisDialTimeout
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = redisIOTimeout
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = redisIOTimeout
	}
	return &RedisSessionStore{
		cfg:    cfg,
		log:    logger,
		rdb:    redis.NewClient(opts),
		prefix: prefix,
	}, nil
}

// NewRedisClusterWithSessionStore builds the Redis cluster backend and the Redis
// session store of one cluster from one RedisClusterConfig: both use the same
// address, database and key prefix, so the sessions a node stores are the ones its
// peers take over. The store is the value to put in Config.SessionStore.
//
// The store inherits the backend's Metrics and Logger, so "session_store_corrupt"
// and "session_store_error" land in the same place as the cluster's own drops.
// Close the store when the broker stops; the backend has its own Stop.
func NewRedisClusterWithSessionStore(cfg RedisClusterConfig) (Cluster, *RedisSessionStore, error) {
	store, err := NewRedisSessionStore(RedisSessionStoreConfig{
		Addr:          cfg.Addr,
		Password:      cfg.Password,
		DB:            cfg.DB,
		ChannelPrefix: cfg.ChannelPrefix,
		Metrics:       cfg.Metrics,
		Logger:        cfg.Logger,
	})
	if err != nil {
		return nil, nil, err
	}
	return NewRedisCluster(cfg), store, nil
}

// SharedAcrossNodes implements SharedSessionStore: the store is shared state, so a
// node that a peer kicked must leave the session to the peer.
func (s *RedisSessionStore) SharedAcrossNodes() bool { return true }

// Load implements SessionStore.
func (s *RedisSessionStore) Load(key string) (SessionState, bool) {
	ctx, cancel := s.cmdContext()
	defer cancel()
	raw, err := s.rdb.Get(ctx, s.sessionKey(key)).Bytes()
	if err != nil {
		if err != redis.Nil {
			s.fail("load", key, err)
		}
		return SessionState{}, false
	}
	return s.decode(key, raw)
}

// Save implements SessionStore. The key's TTL is the session's own deadline: a
// state with an ExpiresAt expires with it, a state without one is written without a
// TTL, and a state whose deadline already passed is deleted instead of stored.
func (s *RedisSessionStore) Save(key string, state SessionState) {
	record := redisSessionRecord{
		Version:   redisSessionRecordVersion,
		ClientID:  state.ClientID,
		ExpiresAt: 0,
	}
	if len(state.Subscriptions) > 0 {
		record.Subscriptions = make(map[string]redisSessionSubscription, len(state.Subscriptions))
		for filter, sub := range state.Subscriptions {
			record.Subscriptions[filter] = redisSessionSubscription{
				QoS:                     sub.QoS,
				NoLocal:                 sub.NoLocal,
				RetainAsPublished:       sub.RetainAsPublished,
				RetainHandling:          sub.RetainHandling,
				SubscriptionIdentifiers: sub.SubscriptionIdentifiers,
			}
		}
	}
	if len(state.Queue) > 0 {
		record.Queue = make([]redisSessionMessage, 0, len(state.Queue))
		for _, msg := range state.Queue {
			record.Queue = append(record.Queue, redisSessionMessage{
				Topic:   msg.Topic,
				Payload: msg.Payload,
				QoS:     msg.QoS,
				Retain:  msg.Retain,
				Props:   msg.Props,
			})
		}
	}
	if len(state.InboundQoS2) > 0 {
		record.InboundQoS2 = append([]uint16(nil), state.InboundQoS2...)
	}
	if len(state.Inflight) > 0 {
		record.Inflight = make([]redisSessionInflight, 0, len(state.Inflight))
		for _, m := range state.Inflight {
			record.Inflight = append(record.Inflight, redisSessionInflight{
				PacketID: m.PacketID,
				Topic:    m.Topic,
				Payload:  m.Payload,
				QoS:      m.QoS,
				Retain:   m.Retain,
				Props:    m.Props,
				Stage:    byte(m.Stage),
			})
		}
	}
	var ttl time.Duration
	if !state.ExpiresAt.IsZero() {
		ttl = time.Until(state.ExpiresAt)
		if ttl <= 0 {
			// The session is over: nothing may be resumed from it, so the key is
			// removed instead of being written with an already elapsed deadline.
			s.Delete(key)
			return
		}
		record.ExpiresAt = state.ExpiresAt.UnixNano()
	}
	value, err := json.Marshal(record)
	if err != nil {
		s.fail("encode", key, err)
		return
	}
	ctx, cancel := s.cmdContext()
	defer cancel()
	if err := s.rdb.Set(ctx, s.sessionKey(key), value, ttl).Err(); err != nil {
		s.fail("save", key, err)
	}
}

// Delete implements SessionStore.
func (s *RedisSessionStore) Delete(key string) {
	ctx, cancel := s.cmdContext()
	defer cancel()
	if err := s.rdb.Del(ctx, s.sessionKey(key)).Err(); err != nil {
		s.fail("delete", key, err)
	}
}

// Take implements the broker's sessionTaker seam: it removes the state stored under
// key and returns it in one round trip, so two nodes cannot both be handed the same
// session.
func (s *RedisSessionStore) Take(key string) (SessionState, bool) {
	ctx, cancel := s.cmdContext()
	defer cancel()
	raw, err := s.rdb.Eval(ctx, redisSessionTakeScript, []string{s.sessionKey(key)}).Text()
	if err != nil {
		if err != redis.Nil {
			s.fail("take", key, err)
		}
		return SessionState{}, false
	}
	return s.decode(key, []byte(raw))
}

// Keys implements SessionStore: every session key of this store, without the
// configured prefix and without any other key of the cluster. It scans, so its cost
// is proportional to the sessions the store holds; the broker only calls it from
// New and from the expiry sweeper.
func (s *RedisSessionStore) Keys() []string {
	pattern := s.sessionKeyPrefix() + "*"
	cut := len(s.sessionKeyPrefix())
	var out []string
	ctx, cancel := s.cmdContext()
	defer cancel()
	var cursor uint64
	for {
		keys, next, err := s.rdb.Scan(ctx, cursor, pattern, redisSessionScanCount).Result()
		if err != nil {
			s.fail("keys", pattern, err)
			return out
		}
		for _, key := range keys {
			out = append(out, key[cut:])
		}
		cursor = next
		if cursor == 0 {
			return out
		}
	}
}

// Close releases the store's Redis connection pool.
func (s *RedisSessionStore) Close() error { return s.rdb.Close() }

// Drops reports how many values this store has dropped: undecodable records and
// failed commands. It mirrors the metrics the configuration records.
func (s *RedisSessionStore) Drops() uint64 { return s.drops.Load() }

// sessionKey maps a broker session key onto its Redis key. The key the broker
// passes is already qualified with the broker's namespace (namespaceKey), so it is
// used verbatim: two namespaces therefore address two different Redis keys, and the
// store needs no namespace of its own.
func (s *RedisSessionStore) sessionKey(key string) string { return s.sessionKeyPrefix() + key }

// sessionKeyPrefix is the prefix every session key of this store starts with.
func (s *RedisSessionStore) sessionKeyPrefix() string {
	return s.prefix + ":" + redisSessionKeySegment + ":"
}

// cmdContext bounds one command (or one Keys scan), so a Redis that stops
// answering cannot hold the broker's session lock forever. The caller defers the
// cancel.
func (s *RedisSessionStore) cmdContext() (context.Context, context.CancelFunc) {
	timeout := s.cfg.IOTimeout
	if timeout <= 0 {
		timeout = redisIOTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}

// decode turns a stored value into a SessionState. A value that does not decode is
// removed and counted: a broker must keep serving the other sessions instead of
// failing the CONNECT of the one whose state is unusable.
func (s *RedisSessionStore) decode(key string, raw []byte) (SessionState, bool) {
	var record redisSessionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		s.corrupt(key, err)
		return SessionState{}, false
	}
	if record.Version != redisSessionRecordVersion {
		s.corrupt(key, fmt.Errorf("record version %d is not %d", record.Version, redisSessionRecordVersion))
		return SessionState{}, false
	}
	state := SessionState{ClientID: record.ClientID}
	if record.ExpiresAt != 0 {
		state.ExpiresAt = time.Unix(0, record.ExpiresAt)
	}
	if len(record.Subscriptions) > 0 {
		state.Subscriptions = make(map[string]SessionSubscription, len(record.Subscriptions))
		for filter, sub := range record.Subscriptions {
			state.Subscriptions[filter] = SessionSubscription{
				QoS:                     sub.QoS,
				NoLocal:                 sub.NoLocal,
				RetainAsPublished:       sub.RetainAsPublished,
				RetainHandling:          sub.RetainHandling,
				SubscriptionIdentifiers: sub.SubscriptionIdentifiers,
			}
		}
	}
	if len(record.Queue) > 0 {
		state.Queue = make([]QueuedMessage, 0, len(record.Queue))
		for _, msg := range record.Queue {
			state.Queue = append(state.Queue, QueuedMessage{
				Topic:   msg.Topic,
				Payload: msg.Payload,
				QoS:     msg.QoS,
				Retain:  msg.Retain,
				Props:   msg.Props,
			})
		}
	}
	if len(record.InboundQoS2) > 0 {
		state.InboundQoS2 = append([]uint16(nil), record.InboundQoS2...)
	}
	if len(record.Inflight) > 0 {
		state.Inflight = make([]SessionInflight, 0, len(record.Inflight))
		for _, m := range record.Inflight {
			state.Inflight = append(state.Inflight, SessionInflight{
				PacketID: m.PacketID,
				Topic:    m.Topic,
				Payload:  m.Payload,
				QoS:      m.QoS,
				Retain:   m.Retain,
				Props:    m.Props,
				Stage:    InflightStage(m.Stage),
			})
		}
	}
	return state, true
}

// corrupt drops one undecodable value and counts it.
func (s *RedisSessionStore) corrupt(key string, err error) {
	s.drops.Add(1)
	s.log.Warn("gmqx dropped an undecodable stored session", "key", key, "err", err)
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.RecordDroppedMessage("session_store_corrupt")
	}
	// The value is unusable, so the key goes: a later Save starts from a clean
	// slot instead of tripping over the same bytes.
	s.Delete(key)
}

// fail counts one failed command.
func (s *RedisSessionStore) fail(op, key string, err error) {
	s.drops.Add(1)
	s.log.Warn("gmqx redis session store command failed", "op", op, "key", key, "err", err)
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.RecordDroppedMessage("session_store_error")
	}
}

// redisSessionRecordVersion is the version of the stored value, so a field change
// can be told apart from a value written by another build. A record of another
// version is treated like an undecodable one: dropped and counted.
const redisSessionRecordVersion = 1

// redisSessionRecord is the stored form of one SessionState. It is an explicit
// record instead of a marshalled SessionState so the stored layout cannot change by
// renaming a Go field, and so the version can gate a change.
type redisSessionRecord struct {
	Version int `json:"v"`
	// ClientID is the session's client id; the key carries it too, but a state that
	// is handed over must name its own session.
	ClientID string `json:"client"`
	// ExpiresAt is the session deadline in Unix nanoseconds; 0 means "never", which
	// is MQTT 3.1.1 and a broker without a Session Expiry Interval.
	ExpiresAt     int64                               `json:"expires,omitempty"`
	Subscriptions map[string]redisSessionSubscription `json:"subs,omitempty"`
	Queue         []redisSessionMessage               `json:"queue,omitempty"`
	// InboundQoS2 are the packet ids the client published at QoS 2 whose PUBREL
	// has not arrived yet, and Inflight the outbound exchanges the session never
	// confirmed. Both are additive on the wire: a record written by a build that
	// predates them decodes as "no QoS 2 state", which is exactly how that build
	// behaved.
	InboundQoS2 []uint16               `json:"in_qos2,omitempty"`
	Inflight    []redisSessionInflight `json:"inflight,omitempty"`
}

// redisSessionSubscription is one stored subscription with its MQTT 5 options.
// SubscriptionIdentifiers is additive on the wire: a record written by a build
// that predates the field decodes as "no identifier", exactly as it behaved.
type redisSessionSubscription struct {
	QoS                     byte     `json:"qos,omitempty"`
	NoLocal                 bool     `json:"nl,omitempty"`
	RetainAsPublished       bool     `json:"rap,omitempty"`
	RetainHandling          byte     `json:"rh,omitempty"`
	SubscriptionIdentifiers []uint32 `json:"si,omitempty"`
}

// redisSessionMessage is one queued message: topic, payload, QoS, retain flag and
// the MQTT 5 PUBLISH property block without its length prefix, which is the shape
// QueuedMessage uses. encoding/json writes the two byte slices as base64, which
// round-trips them exactly.
type redisSessionMessage struct {
	Topic   string `json:"t"`
	Payload []byte `json:"p,omitempty"`
	QoS     byte   `json:"q,omitempty"`
	Retain  bool   `json:"r,omitempty"`
	Props   []byte `json:"pp,omitempty"`
}

// redisSessionInflight is one unconfirmed outbound QoS 1/2 exchange. It keeps the
// packet id of the exchange (a resumed session must resend it under the same id)
// and the stage it reached, which is the stable byte of InflightStage: await
// PUBACK, await PUBREC or await PUBCOMP.
type redisSessionInflight struct {
	PacketID uint16 `json:"id"`
	Topic    string `json:"t"`
	Payload  []byte `json:"p,omitempty"`
	QoS      byte   `json:"q,omitempty"`
	Retain   bool   `json:"r,omitempty"`
	Props    []byte `json:"pp,omitempty"`
	Stage    byte   `json:"s,omitempty"`
}
