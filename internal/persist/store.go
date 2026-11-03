package persist

// The broker's persistence seam
//
// The broker keeps two kinds of state a client asked it to hold: persistent
// sessions (subscriptions, the offline queue, the QoS 2 state of both directions)
// and retained messages. Both are written through to one gcache store, under one
// key scheme and one encoding version, and both come back the same way after a
// restart — that is the whole point of this package: one persistence seam, two
// records. A deployment picks a backend by handing the broker a
// gcache.RemoteStore (gcache/memory for a single process, gcache/redis or
// gcache/olric for a shared one); it never picks a "session store" or a "retained
// store".
//
// Key layout, under the configured prefix ("gmqx:" by default):
//
//	<prefix>session:<key>    JSON sessionRecord, where <key> is the key the broker
//	                         builds for the session (namespaceKey(Config.Namespace,
//	                         ClientID)), so sessions of two namespaces sharing one
//	                         store cannot collide.
//	<prefix>retained:<key>   JSON retainedRecord, keyed the same way.
//
// The TTL of a key is the state's own deadline, not a fixed cache lifetime: a
// session or retained message with an ExpiresAt is written with the remaining time
// as its TTL, so the store drops it at exactly the moment the broker would stop
// using it; a state without a deadline (MQTT 3.1.1, which has no Session Expiry
// Interval, and a retained message published without a Message Expiry Interval) is
// written without a TTL. A state whose deadline has already passed is deleted
// instead of written.
//
// The store is an external input. A value that does not decode, or whose version
// this build does not know, is dropped and counted ("session_store_corrupt" /
// "retained_store_corrupt") instead of failing a CONNECT or a SUBSCRIBE; a failed
// command is counted ("session_store_error" / "retained_store_error") and reported
// as a miss, which is the at-most-once behavior the cluster itself has. The broker
// keeps serving either way.
//
// Enumeration is optional. gcache.Scanner is what lets the broker load what a
// previous process stored (at start-up) and, for retained messages, is not needed
// at all: the broker keeps a hot table and scans that. A store that cannot
// enumerate is a store that no other process writes to, so skipping the load is
// correct rather than degraded.

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/maxid/gcache"

	"github.com/maxid/gmqx/hook"
	"github.com/maxid/gmqx/retained"
	"github.com/maxid/gmqx/session"
)

// Defaults.
const (
	// DefaultPrefix is the key prefix of a store that does not configure one.
	DefaultPrefix = "gmqx:"
	// DefaultTimeout bounds one store operation: the broker calls the store from
	// its own session lock, so an unreachable backend must cost a bounded wait,
	// never an unbounded one.
	DefaultTimeout = 2 * time.Second
	// lockTTL bounds the critical section of an atomic take. It must stay well
	// above the store's p99 for a get followed by a delete.
	lockTTL = 5 * time.Second
)

// Options configure a Store.
type Options struct {
	// Remote is the backend. Required.
	Remote gcache.RemoteStore
	// Locker, when set, makes Take atomic across processes and marks the store as
	// shared: the broker then treats a displaced connection's session as owned by
	// another node (it neither writes its copy back nor deletes the key). A nil
	// Locker means a single writer, which is what an in-process store is.
	Locker gcache.Locker
	// Prefix is the key namespace, "gmqx:" by default. It must not contain SCAN
	// glob characters when the store can enumerate keys.
	Prefix string
	// Metrics receives the drop and failure counters. It is a function because the
	// broker's metrics is a snapshot a plugin may swap at run time.
	Metrics func() hook.MetricsManager
	// Logger receives decode and command failures; slog.Default() by default.
	Logger *slog.Logger
	// Timeout bounds one store operation (DefaultTimeout by default).
	Timeout time.Duration
}

// Store is the broker's persistence seam over one gcache backend.
type Store struct {
	remote  gcache.RemoteStore
	locker  gcache.Locker
	scanner gcache.Scanner
	prefix  string
	metrics func() hook.MetricsManager
	log     *slog.Logger
	timeout time.Duration
}

// New builds the seam. It refuses a configuration that cannot work: no backend, a
// prefix with SCAN glob characters on a store that enumerates, or a nil Locker
// that a caller tried to pass explicitly (nil means "not shared", so there is
// nothing to refuse — the check exists for the zero value of Options).
func New(opts Options) (*Store, error) {
	if opts.Remote == nil {
		return nil, ErrStoreRequired
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = DefaultPrefix
	}
	if !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	s := &Store{
		remote:  opts.Remote,
		locker:  opts.Locker,
		prefix:  prefix,
		metrics: opts.Metrics,
		log:     logger,
		timeout: timeout,
	}
	if scanner, ok := opts.Remote.(gcache.Scanner); ok {
		if strings.ContainsAny(prefix, `*?[]\`) {
			return nil, ErrInvalidPrefix
		}
		s.scanner = scanner
	}
	return s, nil
}

// Shared reports whether the store is written by more than one broker.
func (s *Store) Shared() bool { return s.locker != nil }

// Enumerable reports whether the broker can load what a previous process stored.
func (s *Store) Enumerable() bool { return s.scanner != nil }

// SaveSession writes st under key, with its deadline as the TTL. It reports
// whether the state reached the store; an already expired state is deleted
// instead of written.
func (s *Store) SaveSession(key string, st *session.State) bool {
	if s.expired(st.ExpiresAt) {
		s.DeleteSession(key)
		return false
	}
	raw, err := encodeSession(st)
	if err != nil {
		s.count("session_store_corrupt")
		s.log.Warn("gmqx: session state cannot be encoded", "key", key, "err", err)
		return false
	}
	if err := s.set(s.sessionKey(key), raw, st.ExpiresAt); err != nil {
		s.fail("session_store_error", "save session", key, err)
		return false
	}
	return true
}

// LoadSession returns the session stored under key.
func (s *Store) LoadSession(key string) (*session.State, bool) {
	raw, ok := s.get("session_store_error", s.sessionKey(key))
	if !ok {
		return nil, false
	}
	st, ok := s.decodeSession(key, raw)
	if !ok {
		return nil, false
	}
	if s.expired(st.ExpiresAt) {
		s.DeleteSession(key)
		return nil, false
	}
	return st, true
}

// TakeSession removes the session stored under key and returns it. With a Locker
// the removal is atomic across processes; without one it is the single writer's
// Load followed by Delete.
func (s *Store) TakeSession(key string) (*session.State, bool) {
	if s.locker != nil {
		unlock, ok := s.lock(key)
		if !ok {
			// Another process owns the take right now: the session is not ours to
			// hand out, and the caller treats it as a clean connect.
			return nil, false
		}
		defer unlock()
	}
	raw, ok := s.get("session_store_error", s.sessionKey(key))
	if !ok {
		return nil, false
	}
	s.DeleteSession(key)
	st, ok := s.decodeSession(key, raw)
	if !ok {
		return nil, false
	}
	return st, true
}

// RemoveSession deletes the session stored under key and returns its state,
// including when its deadline has already passed. It exists for the broker's
// accounting: DeleteSession cannot report the queued messages it dropped from the
// global counter because LoadSession refuses an expired value by design.
func (s *Store) RemoveSession(key string) (*session.State, bool) {
	raw, ok := s.get("session_store_error", s.sessionKey(key))
	if !ok {
		return nil, false
	}
	s.DeleteSession(key)
	return s.decodeSession(key, raw)
}

// DeleteSession removes the session stored under key.
func (s *Store) DeleteSession(key string) {
	if err := s.delete(s.sessionKey(key)); err != nil {
		s.fail("session_store_error", "delete session", key, err)
	}
}

// SessionKeys returns the keys of the stored sessions. It returns nil when the
// store cannot enumerate: a store no other process writes to has nothing the
// broker did not already index.
func (s *Store) SessionKeys() []string { return s.scan(s.prefix + "session:") }

// SaveRetained writes one retained message under key, with its deadline as the
// TTL. An already expired message is deleted instead of written.
func (s *Store) SaveRetained(key string, m retained.Message) bool {
	if s.expired(m.ExpiresAt) {
		s.DeleteRetained(key)
		return false
	}
	raw, err := encodeRetained(m)
	if err != nil {
		s.count("retained_store_corrupt")
		s.log.Warn("gmqx: retained message cannot be encoded", "key", key, "err", err)
		return false
	}
	if err := s.set(s.retainedKey(key), raw, m.ExpiresAt); err != nil {
		s.fail("retained_store_error", "save retained", key, err)
		return false
	}
	return true
}

// DeleteRetained removes the retained message stored under key.
func (s *Store) DeleteRetained(key string) {
	if err := s.delete(s.retainedKey(key)); err != nil {
		s.fail("retained_store_error", "delete retained", key, err)
	}
}

// Retained returns every retained message the store holds, oldest expiry first
// irrelevant, skipping the ones that no longer decode. It returns nil when the
// store cannot enumerate.
func (s *Store) Retained() []retained.Message {
	if s.scanner == nil {
		return nil
	}
	keys := s.scan(s.prefix + "retained:")
	if len(keys) == 0 {
		return nil
	}
	out := make([]retained.Message, 0, len(keys))
	for _, key := range keys {
		raw, ok := s.get("retained_store_error", key)
		if !ok {
			continue
		}
		m, ok := s.decodeRetained(key, raw)
		if !ok {
			continue
		}
		out = append(out, m)
	}
	return out
}

// sessionKey and retainedKey are the wire key of one record.
func (s *Store) sessionKey(key string) string  { return s.prefix + "session:" + key }
func (s *Store) retainedKey(key string) string { return s.prefix + "retained:" + key }

// expired reports whether the deadline has already passed.
func (s *Store) expired(at time.Time) bool {
	return !at.IsZero() && !time.Now().Before(at)
}

// set writes value under key with the TTL its deadline implies.
func (s *Store) set(key string, value []byte, expiresAt time.Time) error {
	ttl := time.Duration(0)
	if !expiresAt.IsZero() {
		ttl = time.Until(expiresAt)
		if ttl <= 0 {
			// The deadline passed between the check and the write: remove the key
			// instead of storing something already stale.
			_ = s.delete(key)
			return nil
		}
	}
	ctx, cancel := s.op()
	defer cancel()
	return s.remote.Set(ctx, key, value, ttl)
}

// get reads one value, reporting a miss on error.
func (s *Store) get(reason, key string) ([]byte, bool) {
	ctx, cancel := s.op()
	defer cancel()
	entry, ok, err := s.remote.Get(ctx, key)
	if err != nil {
		s.fail(reason, "get", key, err)
		return nil, false
	}
	if !ok {
		return nil, false
	}
	return entry.Value, true
}

// delete removes one key.
func (s *Store) delete(key string) error {
	ctx, cancel := s.op()
	defer cancel()
	return s.remote.Delete(ctx, key)
}

// scan lists the keys under prefix. It returns nil without a Scanner.
func (s *Store) scan(prefix string) []string {
	if s.scanner == nil {
		return nil
	}
	ctx, cancel := s.op()
	defer cancel()
	var keys []string
	err := s.scanner.Scan(ctx, prefix, func(key string) error {
		keys = append(keys, key)
		return nil
	})
	if err != nil {
		s.fail("session_store_error", "scan", prefix, err)
		return keys
	}
	return keys
}

// lock takes the per-session lock of an atomic take. It reports false when another
// process holds it.
func (s *Store) lock(key string) (func(), bool) {
	ctx, cancel := s.op()
	defer cancel()
	lock, ok, err := s.locker.TryAcquire(ctx, s.prefix+"lock:"+key, lockTTL)
	if err != nil {
		s.fail("session_store_error", "lock", key, err)
		return nil, false
	}
	if !ok {
		return nil, false
	}
	return func() {
		unlockCtx, unlockCancel := s.op()
		defer unlockCancel()
		if err := lock.Unlock(unlockCtx); err != nil {
			s.fail("session_store_error", "unlock", key, err)
		}
	}, true
}

// op returns the context of one store operation.
func (s *Store) op() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.timeout)
}

// count records a dropped value.
func (s *Store) count(reason string) {
	if s.metrics != nil {
		if m := s.metrics(); m != nil {
			m.RecordDroppedMessage(reason)
		}
	}
}

// fail records a backend failure. The caller degrades to a miss.
func (s *Store) fail(reason, op, key string, err error) {
	s.log.Warn("gmqx: persistence backend failed", "op", op, "key", key, "err", err)
	s.count(reason)
}
