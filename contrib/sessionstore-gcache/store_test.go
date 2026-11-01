package gcachestore_test

// Store-level cases: the encoding, the TTL rule, the scan, the atomic take and
// the configuration refusals. The broker-level cases (cross-node resume, a clean
// connect, two nodes taking one session, expiry through a real Session Expiry
// Interval) are in broker_test.go.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/maxid/gcache"
	"github.com/maxid/gcache/redis"
	gmqx "github.com/maxid/gmqx"
	"github.com/maxid/gmqx/contrib/sessionstore-gcache"
)

const testPrefix = "test:session:"

// lockKey is the key a take of a session is locked on: the adapter's documented
// layout is "take-lock:" + data prefix + session key, outside the data prefix.
func lockKey(sessionKey string) string { return "take-lock:" + testPrefix + sessionKey }

// dataKey is the key a session is stored under.
func dataKey(sessionKey string) string { return testPrefix + sessionKey }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// metricRecorder is the MetricsManager the adapter reports to; only the dropped
// reasons are interesting here.
type metricRecorder struct {
	mu    sync.Mutex
	drops map[string]int
}

func newMetricRecorder() *metricRecorder { return &metricRecorder{drops: map[string]int{}} }

func (m *metricRecorder) RecordDroppedMessage(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.drops[reason]++
}

func (m *metricRecorder) count(reason string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.drops[reason]
}

func (m *metricRecorder) IncrementConnections()        {}
func (m *metricRecorder) DecrementConnections()        {}
func (m *metricRecorder) IncrementPublishedMessages()  {}
func (m *metricRecorder) IncrementSubscribeEvents()    {}
func (m *metricRecorder) DecrementSubscribeEvents()    {}
func (m *metricRecorder) IncrementDisconnectEvents()   {}
func (m *metricRecorder) RecordOverflow(string)        {}
func (m *metricRecorder) RecordQueueDepth(string, int) {}
func (m *metricRecorder) RecordConnections(int)        {}

// newTestStore builds the adapter over one miniredis and returns the client, so a
// case can hold the session lock itself or inspect raw keys.
func newTestStore(t *testing.T, mr *miniredis.Miniredis, metrics gmqx.MetricsManager) (*gcachestore.Store, goredis.UniversalClient) {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	remote, err := redis.NewStore(client)
	if err != nil {
		t.Fatalf("redis store: %v", err)
	}
	locker, err := redis.NewLocker(client, "test:fencing")
	if err != nil {
		t.Fatalf("redis locker: %v", err)
	}
	store, err := gcachestore.New(gcachestore.Config{
		Store:   remote,
		Locker:  locker,
		Prefix:  testPrefix,
		Metrics: metrics,
		Logger:  discardLogger(),
	})
	if err != nil {
		t.Fatalf("new session store: %v", err)
	}
	return store, client
}

// richState is a SessionState with every field populated: subscriptions with
// their MQTT 5 options and identifiers, a queue with payload and property bytes
// (including NUL and 0xff), inbound QoS 2 packet ids, unconfirmed outbound
// exchanges with their stages, and a deadline.
//
// The deadline has no monotonic reading on purpose: a stored format keeps the
// wall clock, and time.Now() carries a monotonic reading that cannot survive it.
func richState(clientID string) gmqx.SessionState {
	props := []byte{0x01, 0x02, 0x00, 0xff}
	return gmqx.SessionState{
		ClientID: clientID,
		Subscriptions: map[string]gmqx.SessionSubscription{
			"sensors/+/temp": {QoS: 1, RetainAsPublished: true, SubscriptionIdentifiers: []uint32{7, 9}},
			"plant/#":        {QoS: 2, NoLocal: true, RetainHandling: 2},
		},
		Queue: []gmqx.QueuedMessage{
			{Topic: "sensors/a/temp", Payload: []byte{0, 1, 2}, QoS: 1, Retain: true, Props: props},
			{Topic: "plant/1", Payload: []byte("dry"), QoS: 0, Props: []byte{}},
		},
		InboundQoS2: []uint16{7, 9},
		Inflight: []gmqx.SessionInflight{
			{PacketID: 12, Topic: "plant/1", Payload: []byte("dry"), QoS: 1, Retain: true, Props: props, Stage: gmqx.InflightAwaitPuback},
			{PacketID: 13, Topic: "plant/2", Payload: []byte("wet"), QoS: 2, Stage: gmqx.InflightAwaitPubcomp},
		},
		ExpiresAt: time.Unix(time.Now().Unix()+3600, 42),
	}
}

// TestRoundTripExact pins the stored encoding: a SessionState comes back exactly
// as it was saved, through Load and through Take (which also removes the key).
func TestRoundTripExact(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, nil)

	want := richState("round-trip")
	store.Save("round-trip", want)

	if !mr.Exists(dataKey("round-trip")) {
		t.Fatalf("the session is not stored under %q", dataKey("round-trip"))
	}
	raw, err := mr.Get(dataKey("round-trip"))
	if err != nil {
		t.Fatalf("read stored value: %v", err)
	}
	var header struct {
		Version int `json:"v"`
	}
	if err := json.Unmarshal([]byte(raw), &header); err != nil || header.Version != 1 {
		t.Fatalf("stored value is not a version-1 record: %s (err %v)", raw, err)
	}

	got, ok := store.Load("round-trip")
	if !ok {
		t.Fatal("Load reported no session after Save")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loaded session differs:\n got = %#v\nwant = %#v", got, want)
	}

	taken, ok := store.Take("round-trip")
	if !ok || !reflect.DeepEqual(taken, want) {
		t.Fatalf("Take = %#v (ok=%v), want the saved state", taken, ok)
	}
	if mr.Exists(dataKey("round-trip")) {
		t.Fatal("Take left the key behind")
	}
	if _, ok := store.Take("round-trip"); ok {
		t.Fatal("a second Take returned a session that was already taken")
	}
}

// A store must return the state it was given, and nil and empty collections are
// different states: the record keeps them apart instead of normalising both to
// "nothing there".
func TestRoundTripNilAndEmptyCollections(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, nil)

	cases := map[string]gmqx.SessionState{
		"nil": {ClientID: "nil"},
		"empty": {
			ClientID:      "empty",
			Subscriptions: map[string]gmqx.SessionSubscription{},
			Queue:         []gmqx.QueuedMessage{},
			InboundQoS2:   []uint16{},
			Inflight:      []gmqx.SessionInflight{},
		},
		"nil payloads": {ClientID: "nil payloads", Queue: []gmqx.QueuedMessage{{Topic: "t"}}},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			store.Save(name, want)
			got, ok := store.Load(name)
			if !ok {
				t.Fatal("Load reported no session after Save")
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("loaded session differs:\n got = %#v\nwant = %#v", got, want)
			}
		})
	}
}

// compatState is richState with the empty-but-not-nil payload removed: gmqx's
// own RedisSessionStore writes its record with `omitempty`, so it cannot carry a
// nil-versus-empty distinction. This adapter can (its structural fields are always
// emitted), and a value from either store must still be readable by the other.
func compatState(clientID string) gmqx.SessionState {
	state := richState(clientID)
	state.Queue[1].Props = nil
	return state
}

// TestRecordLayoutIsSharedWithTheRedisStore pins the compatibility the stored
// layout claims: the value this adapter writes is readable by gmqx's own
// RedisSessionStore and the other way round, so a deployment can move between the
// two (only the key prefix has to be aligned).
func TestRecordLayoutIsSharedWithTheRedisStore(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, nil) // Prefix "test:session:"

	// gmqx's store writes under "<ChannelPrefix>:session:", which is the prefix
	// this test's adapter uses when ChannelPrefix is "test".
	theirStore, err := gmqx.NewRedisSessionStore(gmqx.RedisSessionStoreConfig{
		Addr:          mr.Addr(),
		ChannelPrefix: "test",
		Logger:        discardLogger(),
	})
	if err != nil {
		t.Fatalf("gmqx redis session store: %v", err)
	}
	t.Cleanup(func() { _ = theirStore.Close() })

	writtenByThem := compatState("compat-theirs")
	theirStore.Save("compat-theirs", writtenByThem)
	got, ok := store.Load("compat-theirs")
	if !ok {
		t.Fatal("the adapter could not read a session written by gmqx's RedisSessionStore")
	}
	if !reflect.DeepEqual(got, writtenByThem) {
		t.Fatalf("a session written by gmqx's store decoded differently:\n got = %#v\nwant = %#v", got, writtenByThem)
	}

	writtenByUs := compatState("compat-ours")
	store.Save("compat-ours", writtenByUs)
	back, ok := theirStore.Load("compat-ours")
	if !ok {
		t.Fatal("gmqx's RedisSessionStore could not read a session written by the adapter")
	}
	if !reflect.DeepEqual(back, writtenByUs) {
		t.Fatalf("a session written by the adapter decoded differently in gmqx's store:\n got = %#v\nwant = %#v", back, writtenByUs)
	}
}

// The key's TTL is the session's deadline: it is written for the remaining time,
// a session without a deadline is written without a TTL, and a state whose
// deadline has already passed is deleted rather than written.
func TestTTLFromSessionExpiry(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, nil)

	store.Save("expiring", gmqx.SessionState{ClientID: "expiring", ExpiresAt: time.Now().Add(500 * time.Millisecond)})
	ttl := mr.TTL(dataKey("expiring"))
	if ttl <= 0 || ttl > 500*time.Millisecond {
		t.Fatalf("TTL of an expiring session = %v, want (0, 500ms]", ttl)
	}

	store.Save("forever", gmqx.SessionState{ClientID: "forever"})
	if ttl := mr.TTL(dataKey("forever")); ttl != 0 {
		t.Fatalf("TTL of a session without a deadline = %v, want none", ttl)
	}

	// A state past its deadline is not written at all...
	store.Save("already-over", gmqx.SessionState{ClientID: "already-over", ExpiresAt: time.Now().Add(-time.Second)})
	if mr.Exists(dataKey("already-over")) {
		t.Fatal("a session whose deadline passed was stored")
	}
	// ...and saving one over an existing key removes the old value.
	store.Save("over", gmqx.SessionState{ClientID: "over", ExpiresAt: time.Now().Add(time.Hour)})
	if !mr.Exists(dataKey("over")) {
		t.Fatal("a live session was not stored")
	}
	store.Save("over", gmqx.SessionState{ClientID: "over", ExpiresAt: time.Now().Add(-time.Second)})
	if mr.Exists(dataKey("over")) {
		t.Fatal("an elapsed save left the previous value behind")
	}

	// The key disappears at the deadline, without the broker's sweeper.
	mr.FastForward(time.Second)
	if mr.Exists(dataKey("expiring")) {
		t.Fatal("the key outlived the session's deadline")
	}
	if !mr.Exists(dataKey("forever")) {
		t.Fatal("a session without a deadline was expired")
	}
}

// A stored value whose own deadline has passed is dropped even when the backend
// did not enforce the TTL, and it is counted: the deadline is a contract, not a
// hint.
func TestLoadDropsExpiredState(t *testing.T) {
	mr := miniredis.RunT(t)
	metrics := newMetricRecorder()
	store, _ := newTestStore(t, mr, metrics)

	raw, err := json.Marshal(map[string]any{
		"v":        1,
		"client":   "stale",
		"expires":  time.Now().Add(-time.Minute).UnixNano(),
		"subs":     nil,
		"queue":    nil,
		"in_qos2":  nil,
		"inflight": nil,
	})
	if err != nil {
		t.Fatalf("build record: %v", err)
	}
	if err := mr.Set(dataKey("stale"), string(raw)); err != nil {
		t.Fatalf("seed stale session: %v", err)
	}

	if _, ok := store.Load("stale"); ok {
		t.Fatal("Load returned a session past its deadline")
	}
	if mr.Exists(dataKey("stale")) {
		t.Fatal("an expired session was left in the store")
	}
	if got := metrics.count("session_expired"); got != 1 {
		t.Fatalf("session_expired = %d, want 1", got)
	}
}

// Keys returns exactly this store's sessions: the prefix is removed, and keys of
// another store or another Redis namespace — including a lock key — are not
// sessions.
func TestKeysReturnsOnlyThisStoreNamespace(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, nil)

	store.Save("alpha", gmqx.SessionState{ClientID: "alpha"})
	store.Save("beta", gmqx.SessionState{ClientID: "beta"})
	// Another store's prefix, a look-alike that does not start with the prefix,
	// and a leftover take lock of this store.
	if err := mr.Set("other:session:gamma", "x"); err != nil {
		t.Fatalf("seed foreign key: %v", err)
	}
	if err := mr.Set("test:session-other:delta", "x"); err != nil {
		t.Fatalf("seed look-alike key: %v", err)
	}
	if err := mr.Set(lockKey("alpha"), "x"); err != nil {
		t.Fatalf("seed lock key: %v", err)
	}

	got := store.Keys()
	sort.Strings(got)
	want := []string{"alpha", "beta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Keys = %v, want %v", got, want)
	}
}

// A value that cannot be decoded is dropped, counted and removed — never a panic
// and never a half-decoded state.
func TestCorruptValueDroppedAndCounted(t *testing.T) {
	mr := miniredis.RunT(t)
	metrics := newMetricRecorder()
	store, _ := newTestStore(t, mr, metrics)

	if err := mr.Set(dataKey("garbage"), "this is not a session record"); err != nil {
		t.Fatalf("seed garbage: %v", err)
	}
	if _, ok := store.Load("garbage"); ok {
		t.Fatal("Load returned a state from an undecodable value")
	}
	if mr.Exists(dataKey("garbage")) {
		t.Fatal("the undecodable value was not deleted")
	}
	if got := metrics.count("session_store_corrupt"); got != 1 {
		t.Fatalf("session_store_corrupt = %d, want 1", got)
	}

	// A record of another version is not guessed at either.
	if err := mr.Set(dataKey("version"), `{"v":99,"client":"version"}`); err != nil {
		t.Fatalf("seed wrong version: %v", err)
	}
	if _, ok := store.Load("version"); ok {
		t.Fatal("Load accepted a record of an unknown version")
	}
	if got := metrics.count("session_store_corrupt"); got != 2 {
		t.Fatalf("session_store_corrupt = %d, want 2", got)
	}

	// The same through Take: a corrupt value must not stop the take from
	// reporting "no session".
	if err := mr.Set(dataKey("take-garbage"), "{"); err != nil {
		t.Fatalf("seed garbage: %v", err)
	}
	if _, ok := store.Take("take-garbage"); ok {
		t.Fatal("Take returned a state from an undecodable value")
	}
	if mr.Exists(dataKey("take-garbage")) {
		t.Fatal("Take left the undecodable value in the store")
	}
	// The lock of the failed take was released.
	if mr.Exists(lockKey("take-garbage")) {
		t.Fatal("a take that found garbage left its lock behind")
	}
}

// One stored session is handed out once, however many callers race for it.
func TestTakeIsAtomic(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, nil)

	want := richState("atomic")
	store.Save("atomic", want)

	const workers = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			state, ok := store.Take("atomic")
			if !ok {
				return
			}
			if !reflect.DeepEqual(state, want) {
				t.Errorf("the winner received a different state than the one stored")
			}
			mu.Lock()
			winners++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if winners != 1 {
		t.Fatalf("%d takes returned the session, want exactly 1", winners)
	}
	if mr.Exists(lockKey("atomic")) {
		t.Fatal("the take lock outlived the take")
	}
}

// The lock is released after a take, so the next take can proceed; a lock that
// were left behind would make every later reconnect of that session wait for the
// locker's TTL and then start a fresh session.
func TestTakeReleasesTheLock(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, nil)

	store.Save("again", gmqx.SessionState{ClientID: "again"})
	if _, ok := store.Take("again"); !ok {
		t.Fatal("first take did not return the session")
	}
	store.Save("again", gmqx.SessionState{ClientID: "again"})
	started := time.Now()
	if _, ok := store.Take("again"); !ok {
		t.Fatal("second take did not return the session: the lock was not released")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("the second take waited %s: it took the contended path", elapsed)
	}
}

// A lock someone else holds makes Take report "no session" instead of blocking:
// the session belongs to the other node, and this CONNECT gets a fresh one. The
// stored state is not touched, and the outcome is counted.
func TestTakeContended(t *testing.T) {
	mr := miniredis.RunT(t)
	metrics := newMetricRecorder()
	store, client := newTestStore(t, mr, metrics)
	locker, err := redis.NewLocker(client, "test:fencing")
	if err != nil {
		t.Fatalf("locker: %v", err)
	}

	store.Save("busy", gmqx.SessionState{ClientID: "busy"})

	held, ok, err := locker.TryAcquire(context.Background(), lockKey("busy"), time.Minute)
	if err != nil || !ok {
		t.Fatalf("hold the session lock = %v, %v", ok, err)
	}
	started := time.Now()
	if _, ok := store.Take("busy"); ok {
		t.Fatal("Take returned a session whose lock another owner held")
	}
	// The take waited its bounded turn instead of hanging on the lock.
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("a contended take waited %s", elapsed)
	}
	if !mr.Exists(dataKey("busy")) {
		t.Fatal("a contended take deleted the session it never owned")
	}
	if got := metrics.count("session_take_contended"); got != 1 {
		t.Fatalf("session_take_contended = %d, want 1", got)
	}

	if err := held.Unlock(context.Background()); err != nil {
		t.Fatalf("release the session lock: %v", err)
	}
	if _, ok := store.Take("busy"); !ok {
		t.Fatal("Take did not return the session after the lock was released")
	}
}

// remoteOnly hides the Scanner capability of the wrapped store: it is a complete
// RemoteStore, and exactly what New must refuse.
type remoteOnly struct{ gcache.RemoteStore }

func TestNewRefusesWhatCannotWork(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	remote, err := redis.NewStore(client)
	if err != nil {
		t.Fatalf("redis store: %v", err)
	}
	locker, err := redis.NewLocker(client, "test:fencing")
	if err != nil {
		t.Fatalf("redis locker: %v", err)
	}

	if _, err := gcachestore.New(gcachestore.Config{}); !errors.Is(err, gcachestore.ErrStoreRequired) {
		t.Fatalf("New without a store = %v, want ErrStoreRequired", err)
	}

	// A store that cannot enumerate is refused at construction, not on the first
	// reconnect or sweep.
	if _, err := gcachestore.New(gcachestore.Config{Store: remoteOnly{remote}, Locker: locker}); !errors.Is(err, gcachestore.ErrEnumerationUnsupported) {
		t.Fatalf("New with a non-scanning store = %v, want ErrEnumerationUnsupported", err)
	}

	// A shared store without a locker could not take atomically.
	if _, err := gcachestore.New(gcachestore.Config{Store: remote}); !errors.Is(err, gcachestore.ErrLockerRequired) {
		t.Fatalf("New without a locker = %v, want ErrLockerRequired", err)
	}

	// A prefix with a SCAN glob character would scan keys this store does not own.
	if _, err := gcachestore.New(gcachestore.Config{Store: remote, Locker: locker, Prefix: "test:*:"}); !errors.Is(err, gcachestore.ErrInvalidPrefix) {
		t.Fatalf("New with a glob prefix = %v, want ErrInvalidPrefix", err)
	}

	// The valid configuration is accepted, and the store is shared.
	store, err := gcachestore.New(gcachestore.Config{Store: remote, Locker: locker, Prefix: testPrefix})
	if err != nil {
		t.Fatalf("New with a valid configuration: %v", err)
	}
	if !store.SharedAcrossNodes() {
		t.Fatal("the store does not report itself shared; a displaced connection could write its copy back")
	}
	var shared gmqx.SharedSessionStore = store
	if !shared.SharedAcrossNodes() {
		t.Fatal("the store does not satisfy gmqx.SharedSessionStore")
	}
}
