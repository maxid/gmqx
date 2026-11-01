package gcachestore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/maxid/gcache"
	gmqx "github.com/maxid/gmqx"
)

// Errors returned by New. Each names a configuration that cannot work, so a
// deployment fails at startup instead of on the first reconnect.
var (
	// ErrStoreRequired means Config.Store is nil.
	ErrStoreRequired = errors.New("gcachestore: a gcache.RemoteStore is required")
	// ErrLockerRequired means Config.Locker is nil. The store is shared, so it
	// must provide the atomic Take; without a locker it cannot, and a
	// Load-then-Delete fallback could hand one session to two nodes.
	ErrLockerRequired = errors.New("gcachestore: a gcache.Locker is required for the atomic take")
	// ErrEnumerationUnsupported means the configured store does not implement
	// gcache.Scanner. gmqx calls Keys when a broker starts (to index the sessions
	// a deployment restored into the store) and from the expiry sweeper; a store
	// that cannot enumerate satisfies neither, and the failure would surface on a
	// reconnect or a sweep instead of here.
	ErrEnumerationUnsupported = errors.New("gcachestore: the store cannot enumerate keys: it does not implement gcache.Scanner, which gmqx needs for offline routing and expiry")
	// ErrInvalidPrefix means Config.Prefix contains a SCAN glob character. Keys
	// uses the prefix as a MATCH pattern, so such a prefix would scan keys the
	// store does not own.
	ErrInvalidPrefix = errors.New("gcachestore: prefix must not contain SCAN glob characters")
)

// Defaults applied by New.
const (
	// DefaultPrefix is the key prefix of a store that does not configure one:
	// session keys are written as "<prefix><broker session key>".
	DefaultPrefix = "gmqx:session:"
	// DefaultLockTTL bounds the critical section of one take. It must stay well
	// above the store's p99 for a Get followed by a Delete.
	DefaultLockTTL = 5 * time.Second
	// DefaultTimeout bounds one store operation, and the whole Keys walk.
	DefaultTimeout = 2 * time.Second
	// DefaultLockWaitTimeout bounds how long a take waits for the session lock
	// before treating the session as taken by another node.
	DefaultLockWaitTimeout = 500 * time.Millisecond
)

// lockKeyPrefix is prepended to the data prefix when a lock key is built. Lock
// keys therefore never start with the data prefix, so Keys (which scans the data
// prefix) cannot return one and a lock that outlives its take expires on its own.
const lockKeyPrefix = "take-lock:"

// Config configures a Store.
type Config struct {
	// Store is the shared tier. Required, and it must implement gcache.Scanner:
	// Keys needs enumeration, and New refuses a store that cannot provide it
	// (ErrEnumerationUnsupported).
	Store gcache.RemoteStore
	// Locker guards the atomic take. Required. Every broker that shares the same
	// Store and Prefix must use a Locker over the same lock backend.
	Locker gcache.Locker
	// Prefix is this store's key namespace: a session key is written as
	// Prefix + <broker session key>. Defaults to DefaultPrefix. It must not
	// contain SCAN glob characters (`*`, `?`, `[`, `]`, `\`), because Keys uses it
	// in a MATCH pattern. Two brokers of one deployment must agree on it; two
	// deployments that share a Redis must differ.
	Prefix string
	// LockTTL bounds the lock a take holds, and with it the take's critical
	// section (the read and the delete). Defaults to DefaultLockTTL. It must be
	// larger than the store's p99 for Get+Delete: a lock that expires mid-take
	// lets a second node into the critical section.
	LockTTL time.Duration
	// LockWait bounds how long a take retries the lock. Defaults to a 500 ms
	// timeout with gcache's default backoff. When it is exhausted the session is
	// treated as taken elsewhere and the CONNECT starts a fresh session.
	LockWait gcache.WaitPolicy
	// Timeout bounds one store operation (Get, Set, Delete) and the whole Keys
	// walk. Defaults to DefaultTimeout. The broker calls the store with its own
	// session lock held, so every call must be bounded.
	Timeout time.Duration
	// Metrics, when set, receives RecordDroppedMessage with the reasons this
	// store produces: "session_store_corrupt" (a stored value did not decode and
	// was deleted), "session_store_error" (a store or lock command failed),
	// "session_expired" (a value past its session deadline was deleted) and
	// "session_take_contended" (a take found the session lock held elsewhere).
	Metrics gmqx.MetricsManager
	// Logger receives command, decode and lock failures. Defaults to
	// slog.Default().
	Logger *slog.Logger
}

// Store implements gmqx.SessionStore over a gcache.RemoteStore, with
// enumeration through gcache.Scanner and the atomic take through gcache.Locker.
//
// It reports itself as a gmqx.SharedSessionStore: every broker that points at the
// same RemoteStore and Prefix reads and writes the same sessions, so a connection
// a peer displaced must not save its copy back.
type Store struct {
	cfg     Config
	log     *slog.Logger
	scanner gcache.Scanner
	drops   atomic.Uint64
}

// The broker's optional interfaces are part of this package's contract, so the
// assertions live here: an accidental signature change fails to compile.
var (
	_ gmqx.SessionStore       = (*Store)(nil)
	_ gmqx.SharedSessionStore = (*Store)(nil)
	_ interface {
		Take(key string) (gmqx.SessionState, bool)
	} = (*Store)(nil)
)

// New validates the configuration and returns the store. Everything that can be
// wrong is rejected here rather than on the first reconnect: a store without
// enumeration, a store without a locker, or a prefix that cannot be scanned.
func New(cfg Config) (*Store, error) {
	if cfg.Store == nil {
		return nil, ErrStoreRequired
	}
	scanner, ok := cfg.Store.(gcache.Scanner)
	if !ok {
		return nil, fmt.Errorf("%w: %T", ErrEnumerationUnsupported, cfg.Store)
	}
	if cfg.Locker == nil {
		return nil, ErrLockerRequired
	}
	if cfg.Prefix == "" {
		cfg.Prefix = DefaultPrefix
	}
	if strings.ContainsAny(cfg.Prefix, `*?[]\`) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidPrefix, cfg.Prefix)
	}
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = DefaultLockTTL
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.LockWait.Timeout <= 0 {
		cfg.LockWait.Timeout = DefaultLockWaitTimeout
	}
	cfg.LockWait.Normalize()

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Store{cfg: cfg, log: logger, scanner: scanner}, nil
}

// SharedAcrossNodes implements gmqx.SharedSessionStore: the store is shared
// state, so a node a peer kicked must leave the session to the peer.
func (s *Store) SharedAcrossNodes() bool { return true }

// Load implements gmqx.SessionStore. A missing key is (SessionState{}, false); a
// value that does not decode, or a state whose deadline has passed, is deleted
// and reported instead of being returned or failing the reconnect.
func (s *Store) Load(key string) (gmqx.SessionState, bool) {
	ctx, cancel := s.opContext()
	defer cancel()
	return s.load(ctx, key)
}

// Save implements gmqx.SessionStore. The key's TTL is the session's own deadline:
// a state with an ExpiresAt is written for the remaining time, a state without
// one is written without a TTL, and a state whose deadline already passed is
// deleted rather than written.
func (s *Store) Save(key string, state gmqx.SessionState) {
	ctx, cancel := s.opContext()
	defer cancel()

	if !state.ExpiresAt.IsZero() && !time.Now().Before(state.ExpiresAt) {
		// The session is over: nothing may be resumed from it, so the key is
		// removed instead of being written with an elapsed deadline.
		if err := s.cfg.Store.Delete(ctx, s.dataKey(key)); err != nil {
			s.fail("save-expired", key, err)
		}
		return
	}

	value, err := encodeState(state)
	if err != nil {
		s.fail("encode", key, err)
		return
	}

	var ttl time.Duration
	if !state.ExpiresAt.IsZero() {
		ttl = time.Until(state.ExpiresAt)
		if ttl <= 0 {
			ttl = time.Nanosecond
		}
	}
	if err := s.cfg.Store.Set(ctx, s.dataKey(key), value, ttl); err != nil {
		s.fail("save", key, err)
	}
}

// Delete implements gmqx.SessionStore.
func (s *Store) Delete(key string) {
	ctx, cancel := s.opContext()
	defer cancel()
	if err := s.cfg.Store.Delete(ctx, s.dataKey(key)); err != nil {
		s.fail("delete", key, err)
	}
}

// Keys implements gmqx.SessionStore with the optional gcache.Scanner port. It
// returns the keys this store holds, in the form the broker passed to Save (this
// store's prefix removed), in the order the scan produced them. The broker skips
// the keys of another namespace itself; the scan is already limited to this
// store's prefix.
//
// A scan that fails part-way returns the keys it had produced and counts
// "session_store_error": a broker that starts with a partial index drops the
// publications it cannot route yet (at-most-once), which is the same degradation
// a failed command has everywhere else in the seam.
func (s *Store) Keys() []string {
	ctx, cancel := s.opContext()
	defer cancel()

	var keys []string
	err := s.scanner.Scan(ctx, s.cfg.Prefix, func(stored string) error {
		if !strings.HasPrefix(stored, s.cfg.Prefix) {
			// A Scanner implementation must honour the prefix; a loose one is
			// filtered here rather than handing the broker a foreign key.
			return nil
		}
		keys = append(keys, stored[len(s.cfg.Prefix):])
		return nil
	})
	if err != nil {
		s.fail("keys", s.cfg.Prefix, err)
	}
	return keys
}

// Take implements the broker's sessionTaker seam: it removes the stored session
// and returns it to exactly one caller.
//
// The protocol is three round trips plus the lock release:
//
//  1. acquire the lock for the session key (Locker.Acquire, bounded by LockWait);
//  2. read the value (RemoteStore.Get) while holding the lock;
//  3. delete it (RemoteStore.Delete) while still holding the lock.
//
// Then the lock is released with Lock.Unlock, which is a compare-and-delete of
// the lock token: a lock that expired and was taken over by another node is never
// released by this one. Holding the lock across (2) and (3) is what makes the
// read and the delete one critical section: a second node cannot read the key
// between them and then delete it again, so two nodes cannot both resume the
// session.
//
// The lock has a TTL (LockTTL) and is not renewed: if the store pauses longer
// than the TTL between the acquire and the delete, a second node may enter the
// same critical section and both may hand the state out. That is the documented
// price of a lease, and why LockTTL must be sized above the store's p99. The
// compare-and-delete release narrows the window (the expired lock is not
// released by the wrong owner) but cannot remove it.
//
// A lock that cannot be acquired means another node (or another CONNECT of the
// same client id) is taking the session: Take returns "no session" instead of
// blocking the CONNECT, and the caller starts a fresh session. It is counted as
// "session_take_contended". A store or lock command that fails is counted as
// "session_store_error" and also degrades to "no session" (at-most-once).
func (s *Store) Take(key string) (gmqx.SessionState, bool) {
	acquireCtx, cancel := context.WithTimeout(context.Background(), s.cfg.LockWait.Timeout+s.cfg.Timeout)
	lock, err := s.cfg.Locker.Acquire(acquireCtx, s.lockKey(key), s.cfg.LockTTL, s.cfg.LockWait)
	cancel()
	if err != nil {
		if errors.Is(err, gcache.ErrLockNotAcquired) {
			s.log.Debug("gmqx session take did not acquire the lock", "key", key)
			s.dropped("session_take_contended")
			return gmqx.SessionState{}, false
		}
		s.fail("take-lock", key, err)
		return gmqx.SessionState{}, false
	}

	ctx, cancelLoad := s.opContext()
	state, ok := s.load(ctx, key)
	if !ok {
		cancelLoad()
		s.release(lock, key)
		return gmqx.SessionState{}, false
	}
	if err := s.cfg.Store.Delete(ctx, s.dataKey(key)); err != nil {
		cancelLoad()
		s.fail("take-delete", key, err)
		s.release(lock, key)
		return gmqx.SessionState{}, false
	}
	cancelLoad()
	s.release(lock, key)
	return state, true
}

// Drops reports how many values this store dropped or failed to read or write:
// undecodable and expired records and failed commands. It mirrors what the
// configured Metrics receives, for a deployment that does not install one.
func (s *Store) Drops() uint64 { return s.drops.Load() }

// load reads and decodes one state with a context the caller owns.
func (s *Store) load(ctx context.Context, key string) (gmqx.SessionState, bool) {
	entry, ok, err := s.cfg.Store.Get(ctx, s.dataKey(key))
	if err != nil {
		s.fail("load", key, err)
		return gmqx.SessionState{}, false
	}
	if !ok {
		return gmqx.SessionState{}, false
	}
	state, err := decodeState(entry.Value)
	if err != nil {
		s.corrupt(ctx, key, err)
		return gmqx.SessionState{}, false
	}
	if !state.ExpiresAt.IsZero() && !time.Now().Before(state.ExpiresAt) {
		// A store that honors the TTL has already removed the key; one that does
		// not (any RemoteStore implementation) must still not hand out a session
		// past its deadline.
		if err := s.cfg.Store.Delete(ctx, s.dataKey(key)); err != nil {
			s.fail("expire", key, err)
		}
		s.log.Debug("gmqx session take found an expired session", "key", key)
		s.dropped("session_expired")
		return gmqx.SessionState{}, false
	}
	return state, true
}

// corrupt drops one undecodable value and counts it. The key goes with it, so a
// later Save starts from a clean slot instead of tripping over the same bytes.
func (s *Store) corrupt(ctx context.Context, key string, err error) {
	s.drops.Add(1)
	s.log.Warn("gmqx dropped an undecodable stored session", "key", key, "err", err)
	s.dropped("session_store_corrupt")
	if err := s.cfg.Store.Delete(ctx, s.dataKey(key)); err != nil {
		s.fail("corrupt-delete", key, err)
	}
}

// fail counts one failed command.
func (s *Store) fail(op, key string, err error) {
	s.drops.Add(1)
	s.log.Warn("gmqx gcache session store command failed", "op", op, "key", key, "err", err)
	s.dropped("session_store_error")
}

// release frees the take lock with the locker's compare-and-delete. A failure is
// reported but does not change the outcome of the take: the state has already
// been read and deleted exactly once, and a lock that outlived its TTL expires on
// its own.
func (s *Store) release(lock gcache.Lock, key string) {
	ctx, cancel := s.opContext()
	defer cancel()
	if err := lock.Unlock(ctx); err != nil {
		s.fail("take-unlock", key, err)
	}
}

// dropped records one reason through the metrics hook, when one is configured.
func (s *Store) dropped(reason string) {
	if s.cfg.Metrics != nil {
		s.cfg.Metrics.RecordDroppedMessage(reason)
	}
}

// opContext bounds one store operation. The broker calls the store with its own
// session lock held, so an unreachable store must cost a bounded wait.
func (s *Store) opContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.cfg.Timeout)
}

// dataKey is the key a session is stored under.
func (s *Store) dataKey(key string) string { return s.cfg.Prefix + key }

// lockKey is the key a take lock is acquired on. It is built outside the data
// prefix so a scan of the sessions cannot see a lock.
func (s *Store) lockKey(key string) string { return lockKeyPrefix + s.cfg.Prefix + key }
