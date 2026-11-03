package persist

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gcache"

	"github.com/maxid/gmqx/hook"
	"github.com/maxid/gmqx/retained"
	"github.com/maxid/gmqx/session"
)

// fakeRemote is a gcache.RemoteStore with TTL, enumeration and failure
// injection, so a case can assert what the seam asks of a backend without one.
type fakeRemote struct {
	mu      sync.Mutex
	items   map[string]fakeItem
	scans   int
	sets    int
	deletes int
	fail    error
	now     func() time.Time
}

type fakeItem struct {
	value     []byte
	expiresAt time.Time
}

func newFakeRemote() *fakeRemote {
	return &fakeRemote{items: map[string]fakeItem{}, now: time.Now}
}

func (f *fakeRemote) Get(_ context.Context, key string) (gcache.Entry, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return gcache.Entry{}, false, f.fail
	}
	it, ok := f.items[key]
	if !ok {
		return gcache.Entry{}, false, nil
	}
	if !it.expiresAt.IsZero() && !f.now().Before(it.expiresAt) {
		delete(f.items, key)
		return gcache.Entry{}, false, nil
	}
	return gcache.Entry{Value: it.value, ExpiresAt: it.expiresAt}, true, nil
}

func (f *fakeRemote) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	f.sets++
	it := fakeItem{value: append([]byte(nil), value...)}
	if ttl > 0 {
		it.expiresAt = f.now().Add(ttl)
	}
	f.items[key] = it
	return nil
}

func (f *fakeRemote) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	f.deletes++
	delete(f.items, key)
	return nil
}

func (f *fakeRemote) Close() error { return nil }

// Scan implements gcache.Scanner.
func (f *fakeRemote) Scan(_ context.Context, prefix string, fn func(key string) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scans++
	if f.fail != nil {
		return f.fail
	}
	for key := range f.items {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			if err := fn(key); err != nil {
				return err
			}
		}
	}
	return nil
}

// fakeLocker is a gcache.Locker for one process.
type fakeLocker struct {
	mu    sync.Mutex
	held  map[string]string
	nonce int
}

func newFakeLocker() *fakeLocker { return &fakeLocker{held: map[string]string{}} }

func (l *fakeLocker) TryAcquire(_ context.Context, key string, _ time.Duration) (gcache.Lock, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.held[key]; ok {
		return nil, false, nil
	}
	l.nonce++
	token := key + "#" + time.Now().Format(time.RFC3339Nano)
	l.held[key] = token
	return &fakeLock{locker: l, key: key, token: token}, true, nil
}

func (l *fakeLocker) Acquire(ctx context.Context, key string, ttl time.Duration, _ gcache.WaitPolicy) (gcache.Lock, error) {
	lock, ok, err := l.TryAcquire(ctx, key, ttl)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("fakelocker: lock is held")
	}
	return lock, nil
}

type fakeLock struct {
	locker *fakeLocker
	key    string
	token  string
}

func (l *fakeLock) Token() string                              { return l.token }
func (l *fakeLock) FencingToken() uint64                       { return 0 }
func (l *fakeLock) ExpiresAt() time.Time                       { return time.Time{} }
func (l *fakeLock) Renew(context.Context, time.Duration) error { return nil }
func (l *fakeLock) Unlock(context.Context) error {
	l.locker.mu.Lock()
	defer l.locker.mu.Unlock()
	if l.locker.held[l.key] == l.token {
		delete(l.locker.held, l.key)
	}
	return nil
}

// metrics is a hook.MetricsManager counting drops by reason.
type metrics struct {
	hook.NoopMetrics
	mu      sync.Mutex
	dropped map[string]int
}

func newMetrics() *metrics { return &metrics{dropped: map[string]int{}} }

func (m *metrics) RecordDroppedMessage(reason string) {
	m.mu.Lock()
	m.dropped[reason]++
	m.mu.Unlock()
}

func (m *metrics) count(reason string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dropped[reason]
}

// testStore builds the seam over fakeRemote, with an optional locker.
func testStore(t *testing.T, remote gcache.RemoteStore, locker gcache.Locker) *Store {
	t.Helper()
	m := newMetrics()
	s, err := New(Options{
		Remote:  remote,
		Locker:  locker,
		Metrics: func() hook.MetricsManager { return m },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// sessionFixture is a session with every half populated.
func sessionFixture() *session.State {
	return &session.State{
		ClientID:  "fleet/truck-1",
		ExpiresAt: time.Now().Add(time.Hour),
		Subs: map[string]session.Subscription{
			"fleet/+/telemetry": {QoS: 1, RetainAsPublished: true, SubscriptionIDs: []uint32{7, 9}},
			"fleet/truck-1/cmd": {QoS: 2, NoLocal: true, RetainHandling: 2},
		},
		Queue: []session.Pending{
			{Topic: "fleet/truck-1/cmd", Payload: []byte("start"), QoS: 1},
			{Topic: "fleet/truck-1/cmd", Payload: []byte("stop"), QoS: 2, Retain: true},
		},
		InboundQoS2: []uint16{9, 3},
		Inflight: []session.Inflight{
			{PacketID: 11, Topic: "fleet/truck-1/cmd", Payload: []byte("left"), QoS: 2, Retain: true, Stage: session.AwaitPubrec},
			{PacketID: 4, Topic: "fleet/+/telemetry", Payload: []byte("dropped"), QoS: 1, Stage: session.AwaitPuback},
			{PacketID: 12, Topic: "fleet/truck-1/cmd", Payload: []byte("right"), QoS: 2, Stage: session.AwaitPubcomp},
		},
	}
}

// TestSessionRoundTrip pins what a session keeps across a restart: the
// subscriptions with their identifiers, the queue, both directions of the QoS 2
// state, and the deadline.
func TestSessionRoundTrip(t *testing.T) {
	remote := newFakeRemote()
	s := testStore(t, remote, nil)
	in := sessionFixture()

	if !s.SaveSession("fleet/truck-1", in) {
		t.Fatal("SaveSession reported failure")
	}
	out, ok := s.LoadSession("fleet/truck-1")
	if !ok {
		t.Fatal("LoadSession found nothing")
	}
	if out.ClientID != in.ClientID || !out.ExpiresAt.Equal(in.ExpiresAt) {
		t.Fatalf("identity = %q/%v, want %q/%v", out.ClientID, out.ExpiresAt, in.ClientID, in.ExpiresAt)
	}
	if len(out.Subs) != 2 || out.Subs["fleet/+/telemetry"].QoS != 1 || !out.Subs["fleet/truck-1/cmd"].NoLocal {
		t.Fatalf("subscriptions = %+v", out.Subs)
	}
	if out.Subs["fleet/+/telemetry"].SubscriptionIDs[0] != 7 {
		t.Fatalf("subscription identifiers = %v", out.Subs["fleet/+/telemetry"].SubscriptionIDs)
	}
	if len(out.Queue) != 2 || string(out.Queue[0].Payload) != "start" || !out.Queue[1].Retain {
		t.Fatalf("queue = %+v", out.Queue)
	}
	if len(out.InboundQoS2) != 2 || len(out.Inflight) != 3 {
		t.Fatalf("qos2 state = %v / %v", out.InboundQoS2, out.Inflight)
	}
	for _, m := range out.Inflight {
		if m.Stage == session.AwaitPubcomp && m.PacketID != 12 {
			t.Fatalf("stage mapping lost: %+v", out.Inflight)
		}
	}
}

// TestSessionRecordIsOrdered pins that a stored record is reproducible: the
// packet-id sets are written sorted.
func TestSessionRecordIsOrdered(t *testing.T) {
	raw, err := encodeSession(sessionFixture())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var rec sessionRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatalf("decode record: %v", err)
	}
	if got := rec.InboundQoS2; len(got) != 2 || got[0] != 3 || got[1] != 9 {
		t.Fatalf("inbound ids = %v, want [3 9]", got)
	}
	if len(rec.Inflight) != 3 || rec.Inflight[0].PacketID != 4 || rec.Inflight[2].PacketID != 12 {
		t.Fatalf("inflight order = %+v", rec.Inflight)
	}
}

// TestSaveUsesDeadlineAsTTL pins the store contract: the record's TTL is the
// remaining lifetime, and a state whose deadline already passed is deleted
// instead of written.
func TestSaveUsesDeadlineAsTTL(t *testing.T) {
	remote := newFakeRemote()
	s := testStore(t, remote, nil)
	in := sessionFixture()
	in.ExpiresAt = time.Now().Add(90 * time.Second)
	if !s.SaveSession("ttl", in) {
		t.Fatal("SaveSession failed")
	}
	remote.mu.Lock()
	it := remote.items[s.sessionKey("ttl")]
	remote.mu.Unlock()
	if remaining := time.Until(it.expiresAt); remaining < 80*time.Second || remaining > 90*time.Second {
		t.Fatalf("TTL = %v, want the session deadline", remaining)
	}

	// No deadline: no TTL.
	in.ExpiresAt = time.Time{}
	if !s.SaveSession("forever", in) {
		t.Fatal("SaveSession failed")
	}
	remote.mu.Lock()
	it = remote.items[s.sessionKey("forever")]
	remote.mu.Unlock()
	if !it.expiresAt.IsZero() {
		t.Fatalf("TTL = %v, want none for a session that never expires", it.expiresAt)
	}

	// An expired state is removed, not stored.
	in.ExpiresAt = time.Now().Add(-time.Minute)
	if s.SaveSession("stale", in) {
		t.Fatal("SaveSession accepted an already expired state")
	}
	if _, ok := remote.items[s.sessionKey("stale")]; ok {
		t.Fatal("an expired state was written")
	}
}

// TestCorruptValuesAreDropped pins that a store is an external input: bytes this
// build cannot read cost the state and are counted, never a panic or a guess.
func TestCorruptValuesAreDropped(t *testing.T) {
	remote := newFakeRemote()
	m := newMetrics()
	s, err := New(Options{Remote: remote, Metrics: func() hook.MetricsManager { return m }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	remote.items[s.sessionKey("bad")] = fakeItem{value: []byte("{not json")}
	if _, ok := s.LoadSession("bad"); ok {
		t.Fatal("a corrupt session was loaded")
	}
	if got := m.count("session_store_corrupt"); got != 1 {
		t.Fatalf("session_store_corrupt = %d, want 1", got)
	}

	raw, err := json.Marshal(sessionRecord{Version: recordVersion + 1, ClientID: "x"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	remote.items[s.sessionKey("future")] = fakeItem{value: raw}
	if _, ok := s.LoadSession("future"); ok {
		t.Fatal("a record of an unknown version was loaded")
	}
	if got := m.count("session_store_corrupt"); got != 2 {
		t.Fatalf("session_store_corrupt = %d, want 2", got)
	}

	remote.items[s.retainedKey("bad")] = fakeItem{value: []byte("[]")}
	if got := len(s.Retained()); got != 0 {
		t.Fatalf("retained = %d, want 0", got)
	}
	if got := m.count("retained_store_corrupt"); got != 1 {
		t.Fatalf("retained_store_corrupt = %d, want 1", got)
	}
}

// TestTakeIsAtomicWithLocker pins the take: one caller wins, the stored value is
// gone, and a concurrent caller sees nothing.
func TestTakeIsAtomicWithLocker(t *testing.T) {
	remote := newFakeRemote()
	s := testStore(t, remote, newFakeLocker())
	if !s.SaveSession("racing", sessionFixture()) {
		t.Fatal("SaveSession failed")
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := s.TakeSession("racing"); ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d takes won, want exactly 1", wins)
	}
	if _, ok := s.LoadSession("racing"); ok {
		t.Fatal("the taken session is still stored")
	}
}

// TestTakeWithoutLocker pins the single-writer fallback.
func TestTakeWithoutLocker(t *testing.T) {
	s := testStore(t, newFakeRemote(), nil)
	if !s.SaveSession("single", sessionFixture()) {
		t.Fatal("SaveSession failed")
	}
	if _, ok := s.TakeSession("single"); !ok {
		t.Fatal("the take reported nothing")
	}
	if _, ok := s.TakeSession("single"); ok {
		t.Fatal("a second take found the same session")
	}
}

// TestRetainedRoundTripAndScan pins the retained record and the enumeration a
// restart uses.
func TestRetainedRoundTripAndScan(t *testing.T) {
	remote := newFakeRemote()
	s := testStore(t, remote, nil)
	first := retained.Message{
		Namespace: "tenant-a", Topic: "plant/1/temp", Payload: []byte("21"),
		QoS: 1, ExpiresAt: time.Now().Add(time.Hour),
	}
	second := retained.Message{Namespace: "tenant-b", Topic: "plant/2/temp", Payload: []byte("22")}
	if !s.SaveRetained("plant/1/temp", first) || !s.SaveRetained("plant/2/temp", second) {
		t.Fatal("SaveRetained failed")
	}
	msgs := s.Retained()
	if len(msgs) != 2 {
		t.Fatalf("Retained() = %d messages, want 2", len(msgs))
	}
	byTopic := map[string]retained.Message{}
	for _, m := range msgs {
		byTopic[m.Topic] = m
	}
	got := byTopic["plant/1/temp"]
	if got.Namespace != "tenant-a" || string(got.Payload) != "21" || got.QoS != 1 || !got.ExpiresAt.After(time.Now()) {
		t.Fatalf("retained message = %+v", got)
	}
	s.DeleteRetained("plant/1/temp")
	if len(s.Retained()) != 1 {
		t.Fatal("DeleteRetained did not remove the message")
	}
}

// TestExpiredRetainedIsNotWritten pins the same deadline rule for retained.
func TestExpiredRetainedIsNotWritten(t *testing.T) {
	remote := newFakeRemote()
	s := testStore(t, remote, nil)
	m := retained.Message{Topic: "gone", Payload: []byte("x"), ExpiresAt: time.Now().Add(-time.Second)}
	if s.SaveRetained("gone", m) {
		t.Fatal("SaveRetained accepted an expired message")
	}
	if _, ok := remote.items[s.retainedKey("gone")]; ok {
		t.Fatal("an expired retained message was written")
	}
}

// TestNewRefusesUnusableConfig pins the startup checks.
func TestNewRefusesUnusableConfig(t *testing.T) {
	if _, err := New(Options{}); !errors.Is(err, ErrStoreRequired) {
		t.Fatalf("New without a store = %v, want ErrStoreRequired", err)
	}
	if _, err := New(Options{Remote: newFakeRemote(), Prefix: "gmqx*"}); !errors.Is(err, ErrInvalidPrefix) {
		t.Fatalf("New with a glob prefix = %v, want ErrInvalidPrefix", err)
	}
	s := testStore(t, newFakeRemote(), nil)
	if !s.Enumerable() || s.Shared() {
		t.Fatalf("default store: enumerable=%v shared=%v, want true/false", s.Enumerable(), s.Shared())
	}
}
