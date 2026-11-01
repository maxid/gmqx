package gcachestore_test

// The same store, against a real Redis in a container managed by testcontainers.
//
// Everything else in this module runs on an embedded Redis, which is enough for
// the logic but cannot prove the two things only a real server shows: that SCAN
// actually pages (the embedded server returns every match in one pass, so a
// cursor bug would pass there) and that a key really expires on its own. Both are
// what a deployment depends on when a session has to outlive a broker process.
//
// The container is started once for the whole file, published on a random host
// port, and terminated by testcontainers when the run ends. When docker is
// unavailable the cases skip with the reason instead of failing.

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	gcacheredis "github.com/maxid/gcache/redis"
	gmqx "github.com/maxid/gmqx"
	"github.com/maxid/gmqx/contrib/sessionstore-gcache"
)

// redisImage is a real server, not a mock: it has SCAN paging and real key
// expiry, which is the point of this file.
const redisImage = "redis:7-alpine"

// realRedis starts one Redis container for the file and returns its address.
func realRedis(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container, err := tcredis.Run(ctx, redisImage,
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").WithStartupTimeout(2*time.Minute)),
	)
	if err != nil {
		t.Skipf("the Redis container cannot be started, so the real-server cases are skipped: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate the Redis container: %v", err)
		}
	})

	addr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis connection string: %v", err)
	}
	// ConnectionString returns a redis:// URL; go-redis wants host:port.
	opts, err := goredis.ParseURL(addr)
	if err != nil {
		t.Fatalf("parse %q: %v", addr, err)
	}
	return opts.Addr
}

// prefixStamp namespaces the keys of this run, so two stores over one server do
// not see each other's sessions.
var prefixStamp = time.Now().UnixNano()

// storeOver builds the adapter over the real Redis at addr.
func storeOver(t *testing.T, addr string, metrics gmqx.MetricsManager) (*gcachestore.Store, goredis.UniversalClient) {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	// A fresh prefix per store keeps two stores over one server independent.
	prefix := fmt.Sprintf("test:%d:", prefixStamp)

	remote, err := gcacheredis.NewStore(client)
	if err != nil {
		t.Fatalf("redis store: %v", err)
	}
	locker, err := gcacheredis.NewLocker(client, prefix+"fencing")
	if err != nil {
		t.Fatalf("redis locker: %v", err)
	}
	store, err := gcachestore.New(gcachestore.Config{
		Store:   remote,
		Locker:  locker,
		Prefix:  prefix,
		Metrics: metrics,
		Logger:  discardLogger(),
	})
	if err != nil {
		t.Fatalf("session store: %v", err)
	}
	return store, client
}

// TestRealRedisRoundTrip is the baseline the embedded server already covers, run
// once against the server a deployment actually runs.
func TestRealRedisRoundTrip(t *testing.T) {
	addr := realRedis(t)
	store, client := storeOver(t, addr, nil)

	want := richState("real-round-trip")
	store.Save("real-round-trip", want)

	// The key exists in the server under the adapter's documented layout.
	keys, err := client.Keys(context.Background(), "*real-round-trip").Result()
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("keys = %v, want one key for the session", keys)
	}

	got, ok := store.Load("real-round-trip")
	if !ok {
		t.Fatal("Load reported no session")
	}
	if fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", want) {
		t.Fatalf("round trip differs:\n got %+v\nwant %+v", got, want)
	}

	if gotKeys := store.Keys(); len(gotKeys) != 1 || gotKeys[0] != "real-round-trip" {
		t.Fatalf("Keys = %v", gotKeys)
	}
	store.Delete("real-round-trip")
	if _, ok := store.Load("real-round-trip"); ok {
		t.Fatal("a deleted session is still loadable")
	}
}

// TestRealRedisScanPages proves enumeration over several SCAN pages. The embedded
// server answers every match in one pass, so only a real server can fail this
// when the cursor loop is wrong.
func TestRealRedisScanPages(t *testing.T) {
	addr := realRedis(t)
	store, client := storeOver(t, addr, nil)

	const sessions = 1500
	for i := 0; i < sessions; i++ {
		id := fmt.Sprintf("page-%04d", i)
		store.Save(id, simpleState(id))
	}

	// The assertion below is only meaningful if the server really pages, so walk
	// the key space with a raw SCAN first. COUNT is a hint, so a page may carry
	// more than the requested count; what matters is that the cursor needs more
	// than one step and that the walk loses and repeats nothing.
	cursor := uint64(0)
	pages := 0
	walked := make(map[string]struct{}, sessions)
	for {
		page, next, err := client.Scan(context.Background(), cursor, fmt.Sprintf("test:%d:*", prefixStamp), 100).Result()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		pages++
		for _, k := range page {
			if _, dup := walked[k]; dup {
				t.Fatalf("the server returned %q twice while paging", k)
			}
			walked[k] = struct{}{}
		}
		cursor = next
		if cursor == 0 {
			break
		}
		if pages > 1000 {
			t.Fatal("the scan never terminated")
		}
	}
	if pages < 2 {
		t.Fatalf("the server returned the key space in %d page(s), so paging is not exercised", pages)
	}
	if len(walked) < sessions {
		t.Fatalf("the raw walk found %d keys, want at least %d", len(walked), sessions)
	}

	keys := store.Keys()
	if len(keys) != sessions {
		t.Fatalf("Keys returned %d sessions, want %d", len(keys), sessions)
	}
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if _, dup := seen[k]; dup {
			t.Fatalf("Keys returned %q twice", k)
		}
		seen[k] = struct{}{}
	}
	for i := 0; i < sessions; i++ {
		if _, ok := seen[fmt.Sprintf("page-%04d", i)]; !ok {
			t.Fatalf("Keys lost page-%04d", i)
		}
	}
}

// TestRealRedisTTLExpiresTheSession relies on the server's own expiry: no broker
// sweeper runs in this file.
func TestRealRedisTTLExpiresTheSession(t *testing.T) {
	addr := realRedis(t)
	store, client := storeOver(t, addr, nil)

	state := simpleState("ttl-real")
	state.ExpiresAt = time.Now().Add(1500 * time.Millisecond)
	store.Save("ttl-real", state)

	keys, err := client.Keys(context.Background(), "*ttl-real").Result()
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys = %v, %v", keys, err)
	}
	ttl := client.PTTL(context.Background(), keys[0]).Val()
	if ttl <= 0 {
		t.Fatalf("the key has no TTL: %v", ttl)
	}
	if ttl > 5*time.Second {
		t.Fatalf("the key TTL %v does not follow the session expiry", ttl)
	}
	if _, ok := store.Load("ttl-real"); !ok {
		t.Fatal("the session is not loadable inside its lifetime")
	}

	time.Sleep(2 * time.Second)
	if _, ok := store.Load("ttl-real"); ok {
		t.Fatal("Load returned a session the server had already expired")
	}
}

// TestRealRedisAtomicTake races two takes through one real server: exactly one may
// win, and it removes the key.
func TestRealRedisAtomicTake(t *testing.T) {
	addr := realRedis(t)
	store, _ := storeOver(t, addr, nil)
	store.Save("race-real", simpleState("race-real"))

	results := make(chan bool, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			_, ok := store.Take("race-real")
			results <- ok
		}()
	}
	close(start)

	wins := 0
	for i := 0; i < 2; i++ {
		if <-results {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("%d of 2 concurrent takes won the session, want exactly 1", wins)
	}
	if _, ok := store.Load("race-real"); ok {
		t.Fatal("the taken session is still loadable")
	}
}

// TestRealRedisBrokerResumesAcrossNodes is the end-to-end case that matters: a
// persistent client subscribes through one broker, disconnects, messages are
// published while it is away, and it reconnects through a second broker that
// shares the same real Redis — receiving every queued message in order.
func TestRealRedisBrokerResumesAcrossNodes(t *testing.T) {
	addr := realRedis(t)

	// One store, shared by both brokers: that is the deployment shape.
	shared, _ := storeOver(t, addr, newMetricRecorder())
	a := startSessionBroker(t, shared)
	b := startSessionBroker(t, shared)

	roamer := dialSession(t, a.Addr(), "roamer-real", false, 3600)
	if roamer.client.SessionPresent() {
		t.Fatal("the first CONNECT reported a session it could not have")
	}
	subscribe(t, roamer, "roam/#", 1)
	if err := roamer.client.Close(); err != nil {
		t.Fatalf("close roamer: %v", err)
	}
	waitFor(t, 5*time.Second, "broker a to store the session", func() bool {
		_, ok := shared.Load("roamer-real")
		return ok
	})

	pub := dialSession(t, a.Addr(), "roam-pub-real", true, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, payload := range []string{"one", "two", "three"} {
		if err := pub.client.Publish(ctx, "roam/"+payload, []byte(payload), 1, false); err != nil {
			t.Fatalf("publish %q: %v", payload, err)
		}
	}
	waitFor(t, 5*time.Second, "the messages to be queued in the shared store", func() bool {
		state, ok := shared.Load("roamer-real")
		return ok && len(state.Queue) == 3
	})

	back := dialSession(t, b.Addr(), "roamer-real", false, 3600)
	if !<-back.present || !back.client.SessionPresent() {
		t.Fatal("broker b did not resume the session broker a stored in real Redis")
	}
	for _, want := range []string{"one", "two", "three"} {
		message := expectPublish(t, back)
		if string(message.Payload) != want {
			t.Fatalf("queued delivery = %q, want %q", message.Payload, want)
		}
		if message.QoS != 1 {
			t.Fatalf("queued delivery %q has QoS %d, want 1", want, message.QoS)
		}
	}
}

// TestRealRedisDefaultsAreSafe guards the case that made this file necessary: a
// store built with defaults must work against a real server without tuning.
func TestRealRedisDefaultsAreSafe(t *testing.T) {
	addr := realRedis(t)
	store, client := storeOver(t, addr, nil)

	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("batch-%03d", i)
		store.Save(id, simpleState(id))
	}
	if got := len(store.Keys()); got != 100 {
		t.Fatalf("Keys = %d sessions, want 100", got)
	}
	// The lock keys live outside the data prefix, so enumeration never returns one.
	all, err := client.Keys(context.Background(), "*").Result()
	if err != nil {
		t.Fatalf("keys: %v", err)
	}
	for _, k := range all {
		for _, session := range store.Keys() {
			if k == session {
				t.Fatalf("a raw key %q equals a session id, so the prefix is not applied", k)
			}
		}
	}
}

// simpleState is the smallest session state a test needs: one subscription and no
// queue. richState covers the full shape elsewhere.
func simpleState(clientID string) gmqx.SessionState {
	return gmqx.SessionState{
		ClientID: clientID,
		Subscriptions: map[string]gmqx.SessionSubscription{
			"test/#": {QoS: 1},
		},
	}
}
