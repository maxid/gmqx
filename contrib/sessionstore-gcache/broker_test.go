package gcachestore_test

// Broker-level cases: the adapter is exercised through a real gmqx broker, not
// only through its own methods. A persistent client subscribes on broker A,
// disconnects, messages are published while it is away, and it reconnects on
// broker B over the same store and receives everything in order. The other cases
// pin the clean-connect discard, the atomic take between two brokers, the session
// expiry as the key's TTL, a corrupt value and the startup scan (Keys) that makes
// an offline session a routing target before it reconnects.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	gmqx "github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
)

// mqtt5 is the MQTT protocol level of the client package's mqtt.Version5. It is
// spelled as a number here so the test module does not import the wire codec.
const mqtt5 = 5

// startSessionBroker starts a broker with its own ephemeral listener whose
// sessions live in store.
func startSessionBroker(t *testing.T, store gmqx.SessionStore) *gmqx.Broker {
	t.Helper()
	broker, err := gmqx.New(gmqx.Config{
		Addr:         "127.0.0.1:0",
		SessionStore: store,
		Logger:       discardLogger(),
	})
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := broker.Start(ctx); err != nil {
		cancel()
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = broker.Stop(stopCtx)
	})
	return broker
}

// sessionClient is one connected test client: the MQTT client, the deliveries it
// received (payload copied, because the client only owns it during the callback)
// and the Session Present values its CONNACKs reported.
type sessionClient struct {
	client  *client.Client
	msgs    chan client.Message
	present chan bool
}

// dialSession connects to addr. A persistent client passes cleanStart=false and a
// SessionExpiry; the broker then keeps its session after the connection ends.
func dialSession(t *testing.T, addr, clientID string, cleanStart bool, expiry uint32) *sessionClient {
	t.Helper()
	msgs := make(chan client.Message, 64)
	present := make(chan bool, 4)
	conn, err := client.Dial(context.Background(), client.Config{
		Addr:          addr,
		ClientID:      clientID,
		Version:       mqtt5,
		CleanStart:    cleanStart,
		SessionExpiry: expiry,
		KeepAlive:     30 * time.Second,
		OnMessage: func(message *client.Message) {
			copied := *message
			copied.Payload = append([]byte(nil), message.Payload...)
			msgs <- copied
		},
		OnConnect: func(sessionPresent bool) { present <- sessionPresent },
	})
	if err != nil {
		t.Fatalf("dial %s (client %q): %v", addr, clientID, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &sessionClient{client: conn, msgs: msgs, present: present}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func expectPublish(t *testing.T, conn *sessionClient) client.Message {
	t.Helper()
	select {
	case message := <-conn.msgs:
		return message
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery arrived")
		return client.Message{}
	}
}

// subscribe makes conn a subscriber of filter and checks the granted QoS.
func subscribe(t *testing.T, conn *sessionClient, filter string, qos byte) {
	t.Helper()
	codes, err := conn.client.Subscribe(context.Background(), client.Subscription{Filter: filter, QoS: qos})
	if err != nil {
		t.Fatalf("subscribe %q: %v", filter, err)
	}
	if len(codes) != 1 || codes[0] != qos {
		t.Fatalf("SUBACK codes %v, want [%d]", codes, qos)
	}
}

// TestSessionResumesOnAnotherBroker is the cross-node case: the session and the
// messages queued while the client was away come from the shared store, so the
// node the client reaches next resumes it instead of starting over.
func TestSessionResumesOnAnotherBroker(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, newMetricRecorder())
	a := startSessionBroker(t, store)
	b := startSessionBroker(t, store)

	// The session is created on a and stored there when the client leaves.
	roamer := dialSession(t, a.Addr(), "roamer", false, 3600)
	if roamer.client.SessionPresent() {
		t.Fatal("the first CONNECT reported a session it could not have")
	}
	subscribe(t, roamer, "roam/#", 1)
	if err := roamer.client.Close(); err != nil {
		t.Fatalf("close roamer: %v", err)
	}
	waitFor(t, 5*time.Second, "broker a to store the session", func() bool {
		_, ok := store.Load("roamer")
		return ok
	})
	if !mr.Exists(dataKey("roamer")) {
		t.Fatal("the adapter did not write the session to the shared store")
	}

	// Messages published on a while the session is away are queued in the store.
	pub := dialSession(t, a.Addr(), "roam-pub", true, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, payload := range []string{"one", "two", "three"} {
		if err := pub.client.Publish(ctx, "roam/"+payload, []byte(payload), 1, false); err != nil {
			t.Fatalf("publish %q: %v", payload, err)
		}
	}
	waitFor(t, 5*time.Second, "the messages to be queued in the store", func() bool {
		state, ok := store.Load("roamer")
		return ok && len(state.Queue) == 3
	})

	// The reconnect on b takes the session over from the shared store.
	back := dialSession(t, b.Addr(), "roamer", false, 3600)
	sessionPresent := <-back.present
	if !sessionPresent || !back.client.SessionPresent() {
		t.Fatal("broker b did not resume the session broker a stored")
	}
	for _, want := range []string{"one", "two", "three"} {
		message := expectPublish(t, back)
		if message.Topic != "roam/"+want || string(message.Payload) != want {
			t.Fatalf("queued delivery = %q %q, want %q %q", message.Topic, message.Payload, "roam/"+want, want)
		}
		if message.QoS != 1 {
			t.Fatalf("queued delivery %q has QoS %d, want 1", want, message.QoS)
		}
	}
	// The restored subscriptions are live on b: a publication on the node that
	// owns the session now reaches it without a new SUBSCRIBE. (It is published
	// on b on purpose: there is no cluster backend in this test, so a publication
	// on a would still be queued by a's copy of the routing index, not delivered
	// to b's connection.)
	livePub := dialSession(t, b.Addr(), "roam-pub-b", true, 0)
	if err := livePub.client.Publish(ctx, "roam/live", []byte("live"), 1, false); err != nil {
		t.Fatalf("publish live: %v", err)
	}
	live := expectPublish(t, back)
	if live.Topic != "roam/live" || string(live.Payload) != "live" {
		t.Fatalf("delivery over a restored subscription = %q %q", live.Topic, live.Payload)
	}

	// The session moved: while b owns the connection nothing is stored under it.
	if _, ok := store.Load("roamer"); ok {
		t.Fatal("the resumed session is still stored while a connection owns it")
	}
}

// TestCleanConnectDiscardsTheSharedSession pins that a clean CONNECT ends the
// session wherever it was stored, exactly as MQTT requires: whichever node
// receives the CONNECT deletes the state another node wrote.
func TestCleanConnectDiscardsTheSharedSession(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, newMetricRecorder())
	a := startSessionBroker(t, store)
	b := startSessionBroker(t, store)

	roamer := dialSession(t, a.Addr(), "discarded", false, 3600)
	subscribe(t, roamer, "state/#", 1)
	if err := roamer.client.Close(); err != nil {
		t.Fatalf("close roamer: %v", err)
	}
	waitFor(t, 5*time.Second, "the session to be stored", func() bool {
		_, ok := store.Load("discarded")
		return ok
	})

	clean := dialSession(t, b.Addr(), "discarded", true, 0)
	if clean.client.SessionPresent() {
		t.Fatal("a clean CONNECT reported a session it just discarded")
	}
	if _, ok := store.Load("discarded"); ok {
		t.Fatal("the clean CONNECT left the stored session behind")
	}
	if mr.Exists(dataKey("discarded")) {
		t.Fatal("the clean CONNECT left the session key in the store")
	}
}

// TestTwoBrokersCannotTakeTheSameSession pins the atomic take through the real
// broker: two nodes reconnecting the same client id at the same time produce
// exactly one session owner, never two.
func TestTwoBrokersCannotTakeTheSameSession(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, newMetricRecorder())
	a := startSessionBroker(t, store)
	b := startSessionBroker(t, store)

	store.Save("racing", gmqx.SessionState{
		ClientID:      "racing",
		Subscriptions: map[string]gmqx.SessionSubscription{"racing/#": {QoS: 1}},
	})

	type result struct {
		conn    *client.Client
		present bool
		err     error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, addr := range []string{a.Addr(), b.Addr()} {
		addr := addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := client.Dial(context.Background(), client.Config{
				Addr:          addr,
				ClientID:      "racing",
				Version:       mqtt5,
				CleanStart:    false,
				SessionExpiry: 3600,
				KeepAlive:     30 * time.Second,
			})
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{conn: conn, present: conn.SessionPresent()}
		}()
	}
	wg.Wait()
	close(results)

	resumed := 0
	for got := range results {
		if got.err != nil {
			t.Fatalf("concurrent persistent CONNECT: %v", got.err)
		}
		conn := got.conn
		t.Cleanup(func() { _ = conn.Close() })
		if got.present {
			resumed++
		}
	}
	if resumed != 1 {
		t.Fatalf("%d nodes resumed the same session, want exactly 1", resumed)
	}
	if _, ok := store.Load("racing"); ok {
		t.Fatal("the taken session is still stored")
	}
}

// TestSessionsExpireWithSessionExpiry pins the TTL rule end to end: the deadline
// the broker stamps from the MQTT 5 Session Expiry Interval becomes the key's TTL,
// so Redis removes an abandoned session without the broker's sweeper.
func TestSessionsExpireWithSessionExpiry(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, newMetricRecorder())
	b := startSessionBroker(t, store)

	conn := dialSession(t, b.Addr(), "short-lived", false, 1) // 1 second
	subscribe(t, conn, "expire/#", 1)
	if err := conn.client.Close(); err != nil {
		t.Fatalf("close conn: %v", err)
	}
	waitFor(t, 5*time.Second, "the session to be stored", func() bool {
		_, ok := store.Load("short-lived")
		return ok
	})

	ttl := mr.TTL(dataKey("short-lived"))
	if ttl <= 0 || ttl > time.Second {
		t.Fatalf("key TTL = %v, want the session's remaining (0, 1s]", ttl)
	}

	mr.FastForward(2 * time.Second)
	if mr.Exists(dataKey("short-lived")) {
		t.Fatal("the key outlived the session's Session Expiry Interval")
	}
	if _, ok := store.Load("short-lived"); ok {
		t.Fatal("the store still hands out the expired session")
	}
}

// TestCorruptStoredSessionDropped pins the corrupt path through the broker: an
// undecodable value neither fails the CONNECT nor panics; it is deleted, counted
// and the client starts a fresh session.
func TestCorruptStoredSessionDropped(t *testing.T) {
	mr := miniredis.RunT(t)
	metrics := newMetricRecorder()
	store, _ := newTestStore(t, mr, metrics)
	b := startSessionBroker(t, store)

	store.Save("corrupted", gmqx.SessionState{
		ClientID:      "corrupted",
		Subscriptions: map[string]gmqx.SessionSubscription{"c/#": {QoS: 1}},
	})
	if err := mr.Set(dataKey("corrupted"), "not a session record"); err != nil {
		t.Fatalf("corrupt the stored value: %v", err)
	}

	conn := dialSession(t, b.Addr(), "corrupted", false, 3600)
	if conn.client.SessionPresent() {
		t.Fatal("a corrupt value was reported as a resumed session")
	}
	if mr.Exists(dataKey("corrupted")) {
		t.Fatal("the corrupt value was left in the store")
	}
	if got := metrics.count("session_store_corrupt"); got < 1 {
		t.Fatalf("session_store_corrupt = %d, want at least 1", got)
	}
}

// TestStartupScanQueuesForAStoredSession pins that Keys has a real consumer: a
// broker indexes the sessions a deployment restored into the store, so a
// publication that arrives before the client reconnects is queued instead of
// dropped as "no such session".
func TestStartupScanQueuesForAStoredSession(t *testing.T) {
	mr := miniredis.RunT(t)
	store, _ := newTestStore(t, mr, newMetricRecorder())

	store.Save("restored", gmqx.SessionState{
		ClientID:      "restored",
		Subscriptions: map[string]gmqx.SessionSubscription{"restore/#": {QoS: 1}},
	})
	b := startSessionBroker(t, store) // New indexes the stored sessions with Keys

	pub := dialSession(t, b.Addr(), "restore-pub", true, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pub.client.Publish(ctx, "restore/one", []byte("one"), 1, false); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitFor(t, 5*time.Second, "the publication to be queued in the store", func() bool {
		state, ok := store.Load("restored")
		return ok && len(state.Queue) == 1
	})

	conn := dialSession(t, b.Addr(), "restored", false, 3600)
	if !conn.client.SessionPresent() {
		t.Fatal("the session restored into the store was not resumed")
	}
	message := expectPublish(t, conn)
	if message.Topic != "restore/one" || string(message.Payload) != "one" {
		t.Fatalf("queued delivery = %q %q", message.Topic, message.Payload)
	}
}
