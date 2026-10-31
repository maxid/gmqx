package gmqx

// Routing-index tests: the index must produce exactly the targets the old
// per-publish scan produced (topic matching, the per-subscription QoS cap, retain
// as published, no-local, the '$' topic exclusion, offline targets), it must drop
// entries on every transition that ends a subscription, it must never consult the
// SessionStore while a publish is routed, and its cost must not grow with the
// number of subscriptions.

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// routingSub is one subscription a case installs before it publishes.
type routingSub struct {
	clientID string
	filter   string
	qos      byte
	noLocal  bool
	rap      bool
	// persistent makes the session a stored (offline) one: it connects with clean
	// start cleared, subscribes, and disconnects before the publication.
	persistent bool
}

// routingWant is one expected target.
type routingWant struct {
	clientID string
	qos      byte
	retain   bool
	offline  bool
}

type routingCase struct {
	name string
	subs []routingSub
	// topic, qos and retain are the publication; from is the client id of the
	// publishing connection (it needs one for a no-local check).
	topic  string
	qos    byte
	retain bool
	from   string
	want   []routingWant
}

// TestRoutingIndexMatchesSubscriptions is the correctness table: every MQTT filter
// shape the broker supports, the options that change a target, and the QoS cap.
func TestRoutingIndexMatchesSubscriptions(t *testing.T) {
	cases := []routingCase{
		{
			name:  "literal filter matches the same topic",
			subs:  []routingSub{{clientID: "a", filter: "plant/1/temp", qos: 1}},
			topic: "plant/1/temp", qos: 1,
			want: []routingWant{{clientID: "a", qos: 1}},
		},
		{
			name:  "literal filter does not match another topic",
			subs:  []routingSub{{clientID: "a", filter: "plant/1/temp", qos: 1}},
			topic: "plant/1/hum", qos: 1,
			want: nil,
		},
		{
			name:  "single-level wildcard matches one level",
			subs:  []routingSub{{clientID: "a", filter: "plant/+/temp", qos: 2}},
			topic: "plant/7/temp", qos: 1,
			want: []routingWant{{clientID: "a", qos: 1}},
		},
		{
			name:  "single-level wildcard does not span levels",
			subs:  []routingSub{{clientID: "a", filter: "plant/+", qos: 2}},
			topic: "plant/7/temp", qos: 1,
			want: nil,
		},
		{
			name:  "multi-level wildcard matches the subtree",
			subs:  []routingSub{{clientID: "a", filter: "plant/#", qos: 1}},
			topic: "plant/7/temp/deep", qos: 0,
			want: []routingWant{{clientID: "a", qos: 0}},
		},
		{
			name: "dollar topic is excluded from wildcard filters",
			subs: []routingSub{
				{clientID: "hash", filter: "#", qos: 1},
				{clientID: "plus", filter: "+/uptime", qos: 1},
			},
			topic: "$SYS/uptime", qos: 1,
			want: nil,
		},
		{
			name:  "dollar topic matches a filter that names the first level",
			subs:  []routingSub{{clientID: "sys", filter: "$SYS/#", qos: 1}},
			topic: "$SYS/uptime", qos: 1,
			want: []routingWant{{clientID: "sys", qos: 1}},
		},
		{
			name: "overlapping filters of one session yield one target with the highest QoS",
			subs: []routingSub{
				{clientID: "a", filter: "plant/+", qos: 0},
				{clientID: "a", filter: "plant/#", qos: 2},
			},
			topic: "plant/7", qos: 2,
			want: []routingWant{{clientID: "a", qos: 2}},
		},
		{
			name: "per-subscription QoS caps the publication QoS of each target",
			subs: []routingSub{
				{clientID: "q0", filter: "cap/x", qos: 0},
				{clientID: "q1", filter: "cap/x", qos: 1},
				{clientID: "q2", filter: "cap/+", qos: 2},
			},
			topic: "cap/x", qos: 1,
			want: []routingWant{
				{clientID: "q0", qos: 0},
				{clientID: "q1", qos: 1},
				{clientID: "q2", qos: 1},
			},
		},
		{
			name: "retain as published decides the retain flag per target",
			subs: []routingSub{
				{clientID: "rap", filter: "state/x", qos: 0, rap: true},
				{clientID: "plain", filter: "state/x", qos: 0},
			},
			topic: "state/x", qos: 0, retain: true,
			want: []routingWant{
				{clientID: "rap", qos: 0, retain: true},
				{clientID: "plain", qos: 0, retain: false},
			},
		},
		{
			name:  "retain as published does not invent a retain flag on a live publish",
			subs:  []routingSub{{clientID: "rap", filter: "state/x", qos: 0, rap: true}},
			topic: "state/x", qos: 0,
			want: []routingWant{{clientID: "rap", qos: 0, retain: false}},
		},
		{
			name:  "no-local excludes the publishing connection",
			subs:  []routingSub{{clientID: "self", filter: "loop/x", qos: 1, noLocal: true}},
			topic: "loop/x", qos: 1, from: "self",
			want: nil,
		},
		{
			name:  "no-local delivers to another connection",
			subs:  []routingSub{{clientID: "self", filter: "loop/x", qos: 1, noLocal: true}},
			topic: "loop/x", qos: 1, from: "other",
			want: []routingWant{{clientID: "self", qos: 1}},
		},
		{
			name: "no-local does not hide a session that matches through another filter",
			subs: []routingSub{
				{clientID: "self", filter: "loop/#", qos: 1, noLocal: true},
				{clientID: "self", filter: "loop/x", qos: 2},
			},
			topic: "loop/x", qos: 2, from: "self",
			want: []routingWant{{clientID: "self", qos: 2}},
		},
		{
			name:  "no-local does not exclude a stored session",
			subs:  []routingSub{{clientID: "off", filter: "loop/x", qos: 1, noLocal: true, persistent: true}},
			topic: "loop/x", qos: 1, from: "other",
			want: []routingWant{{clientID: "off", qos: 1, offline: true}},
		},
		{
			name:  "a publication without an origin connection excludes nothing",
			subs:  []routingSub{{clientID: "self", filter: "loop/x", qos: 1, noLocal: true}},
			topic: "loop/x", qos: 1,
			want: []routingWant{{clientID: "self", qos: 1}},
		},
		{
			name:  "a stored session is an offline target",
			subs:  []routingSub{{clientID: "off", filter: "queue/#", qos: 1, persistent: true}},
			topic: "queue/x", qos: 2,
			want: []routingWant{{clientID: "off", qos: 1, offline: true}},
		},
		{
			name: "an offline target keeps the highest QoS of its stored filters",
			subs: []routingSub{
				{clientID: "off", filter: "queue/+", qos: 0, persistent: true},
				{clientID: "off", filter: "queue/#", qos: 2, persistent: true},
			},
			topic: "queue/x", qos: 2,
			want: []routingWant{{clientID: "off", qos: 2, offline: true}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := startBroker(t, Config{})
			installRoutingSubs(t, b, tc.subs)

			var from ConnectionID
			if tc.from != "" {
				if b.clientByID(tc.from) == nil {
					// The publication names a connection that is not a subscriber of
					// this case: connect it so the no-local comparison has one.
					mustConnect(t, b.Addr(), confConnect{clientID: tc.from, clean: true})
				}
				from = liveConnID(t, b, tc.from)
			}
			pub := Publication{
				Namespace:        b.Namespace(),
				Topic:            tc.topic,
				QoS:              tc.qos,
				Retain:           tc.retain,
				FromClientID:     tc.from,
				FromConnectionID: string(from),
			}
			checkTargets(t, b.LocalTargets(pub), tc.want)
		})
	}
}

// installRoutingSubs connects the subscribers of one case and applies their
// subscriptions. A persistent subscriber disconnects again, so the case publishes
// against a stored session.
func installRoutingSubs(t *testing.T, b *Broker, subs []routingSub) {
	t.Helper()
	type conn struct {
		client *confClient
		nextID uint16
	}
	conns := make(map[string]*conn)
	var persistent []string
	for _, s := range subs {
		c := conns[s.clientID]
		if c == nil {
			cc, _ := mustConnect(t, b.Addr(), confConnect{
				version:  mqtt.Version5,
				clientID: s.clientID,
				clean:    !s.persistent,
				// MQTT 5 ends a session with its connection unless a Session Expiry
				// Interval is set, so a persistent case needs one.
				props: expiryProps(60),
			})
			c = &conn{client: cc}
			conns[s.clientID] = c
			if s.persistent {
				persistent = append(persistent, s.clientID)
			}
		}
		c.nextID++
		ack := subscribe(t, c.client, c.nextID, mqtt.Properties{}, mqtt.Subscription{
			Filter:            s.filter,
			QoS:               s.qos,
			NoLocal:           s.noLocal,
			RetainAsPublished: s.rap,
		})
		if len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] != s.qos {
			t.Fatalf("SUBACK for %q = %v, want [%d]", s.filter, ack.ReasonCodes, s.qos)
		}
	}
	for _, id := range persistent {
		conns[id].client.disconnect()
		waitDisconnected(t, b, id)
		waitFor(t, "the session of "+id+" to be stored", func() bool {
			_, ok := b.store.Load(id)
			return ok
		})
	}
}

// clientByID returns the live connection the broker registered for clientID.
func (b *Broker) clientByID(clientID string) *client {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.clients[clientID]
}

// liveConnID returns the connection id the broker registered for clientID.
func liveConnID(t *testing.T, b *Broker, clientID string) ConnectionID {
	t.Helper()
	b.mu.RLock()
	c := b.clients[clientID]
	b.mu.RUnlock()
	if c == nil {
		t.Fatalf("client %q is not connected", clientID)
	}
	return c.connID
}

// checkTargets compares the target set with the expectation, independent of the
// order the index returned it in.
func checkTargets(t *testing.T, got []LocalTarget, want []routingWant) {
	t.Helper()
	byID := make(map[string]LocalTarget, len(got))
	for _, target := range got {
		if _, dup := byID[target.ClientID]; dup {
			t.Fatalf("client %q appears twice in the target set", target.ClientID)
		}
		byID[target.ClientID] = target
	}
	if len(byID) != len(want) {
		t.Fatalf("targets = %v, want %v", targetIDs(got), want)
	}
	for _, w := range want {
		target, ok := byID[w.clientID]
		if !ok {
			t.Fatalf("client %q missing from targets %v", w.clientID, targetIDs(got))
		}
		if target.QoS != w.qos {
			t.Fatalf("client %q target QoS = %d, want %d", w.clientID, target.QoS, w.qos)
		}
		if target.Retain != w.retain {
			t.Fatalf("client %q target retain = %v, want %v", w.clientID, target.Retain, w.retain)
		}
		if target.Offline() != w.offline {
			t.Fatalf("client %q target offline = %v, want %v", w.clientID, target.Offline(), w.offline)
		}
		if w.offline && target.ConnectionID != "" {
			t.Fatalf("offline target %q names connection %q", w.clientID, target.ConnectionID)
		}
	}
}

func targetIDs(targets []LocalTarget) []string {
	out := make([]string, 0, len(targets))
	for _, target := range targets {
		out = append(out, target.ClientID)
	}
	sort.Strings(out)
	return out
}

// indexEntries reports the (filter, session) entries the index holds and how many
// sessions it knows about.
func indexEntries(b *Broker) (entries, sessions int) {
	b.index.mu.RLock()
	defer b.index.mu.RUnlock()
	for _, bucket := range b.index.exact {
		entries += len(bucket)
	}
	for _, bucket := range b.index.wildcard {
		entries += len(bucket)
	}
	return entries, len(b.index.sessions)
}

// TestRoutingIndexRemovals pins every transition that must take a subscription
// out of the routing table: unsubscribe, a clean connection ending, a persistent
// connection going offline (which stays routable, but as an offline target), the
// expiry of a stored session and a clean CONNECT that discards one.
func TestRoutingIndexRemovals(t *testing.T) {
	b := startBroker(t, Config{unit: testSecond, SessionSweepInterval: 5 * time.Millisecond})
	pub := func(topic string) Publication {
		return Publication{Namespace: b.Namespace(), Topic: topic, QoS: 1}
	}
	subscribeConn := func(t *testing.T, clientID, filter string, clean bool) *confClient {
		t.Helper()
		c, _ := mustConnect(t, b.Addr(), confConnect{
			version:  mqtt.Version5,
			clientID: clientID,
			clean:    clean,
			props:    mqtt.Properties{SessionExpiryInterval: 1},
		})
		subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: filter, QoS: 1})
		return c
	}

	t.Run("unsubscribe", func(t *testing.T) {
		c := subscribeConn(t, "unsub", "u/#", true)
		if filters := b.subscriptionFilters(); len(filters) != 1 || filters[0] != "u/#" {
			t.Fatalf("routing summary = %v, want [u/#]", filters)
		}
		c.unsubscribe(2, "u/#")
		if n := len(b.LocalTargets(pub("u/x"))); n != 0 {
			t.Fatalf("targets after UNSUBSCRIBE = %d, want 0", n)
		}
		if filters := b.subscriptionFilters(); len(filters) != 0 {
			t.Fatalf("routing summary after UNSUBSCRIBE = %v, want none", filters)
		}
		if entries, _ := indexEntries(b); entries != 0 {
			t.Fatalf("index holds %d filter entries after UNSUBSCRIBE, want 0", entries)
		}
	})

	t.Run("clean connection ends with the session", func(t *testing.T) {
		c := subscribeConn(t, "clean-end", "c/#", true)
		c.disconnect()
		waitDisconnected(t, b, "clean-end")
		if n := len(b.LocalTargets(pub("c/x"))); n != 0 {
			t.Fatalf("targets after a clean session ended = %d, want 0", n)
		}
		if entries, sessions := indexEntries(b); entries != 0 || sessions != 0 {
			t.Fatalf("index after a clean session ended = %d entries / %d sessions, want 0/0", entries, sessions)
		}
	})

	t.Run("persistent connection goes offline", func(t *testing.T) {
		c := subscribeConn(t, "persist", "p/#", false)
		c.disconnect()
		waitDisconnected(t, b, "persist")
		waitFor(t, "the session of persist to be stored", func() bool {
			_, ok := b.store.Load("persist")
			return ok
		})
		targets := b.LocalTargets(pub("p/x"))
		if len(targets) != 1 || !targets[0].Offline() || targets[0].ClientID != "persist" {
			t.Fatalf("targets after a persistent disconnect = %+v, want one offline target", targets)
		}
		if filters := b.subscriptionFilters(); len(filters) != 0 {
			t.Fatalf("an offline session is advertised to the cluster: %v", filters)
		}
		if entries, sessions := indexEntries(b); entries != 1 || sessions != 1 {
			t.Fatalf("index = %d entries / %d sessions, want 1/1", entries, sessions)
		}
	})

	t.Run("session expiry", func(t *testing.T) {
		c := subscribeConn(t, "expiring-idx", "e/#", false)
		c.disconnect()
		waitDisconnected(t, b, "expiring-idx")
		waitFor(t, "the session of expiring-idx to be stored", func() bool {
			_, ok := b.store.Load("expiring-idx")
			return ok
		})
		if n := len(b.LocalTargets(pub("e/x"))); n != 1 {
			t.Fatalf("targets while the session is inside its window = %d, want 1", n)
		}
		waitFor(t, "the stored session to expire", func() bool {
			_, ok := b.store.Load("expiring-idx")
			return !ok
		})
		waitFor(t, "the expired session to leave the route", func() bool {
			return len(b.LocalTargets(pub("e/x"))) == 0
		})
	})

	t.Run("clean connect discards a stored session", func(t *testing.T) {
		c := subscribeConn(t, "discard", "d/#", false)
		c.disconnect()
		waitDisconnected(t, b, "discard")
		waitFor(t, "the session of discard to be stored", func() bool {
			_, ok := b.store.Load("discard")
			return ok
		})
		// A clean CONNECT ends the stored session; the new connection has no
		// subscriptions, so the filter must not route any more.
		fresh, ack := mustConnect(t, b.Addr(), confConnect{clientID: "discard", clean: true})
		if ack.SessionPresent {
			t.Fatal("a clean CONNECT resumed the stored session")
		}
		if _, ok := b.store.Load("discard"); ok {
			t.Fatal("the stored session survived a clean CONNECT")
		}
		if targets := b.LocalTargets(pub("d/x")); len(targets) != 0 {
			t.Fatalf("targets after a clean CONNECT = %+v, want none", targets)
		}
		if entries, _ := indexEntries(b); entries != 0 {
			t.Fatalf("index holds %d filter entries after a clean CONNECT, want 0", entries)
		}
		expectNoMessage(t, fresh, 100*time.Millisecond)
	})
}

// TestRoutingIndexNamespaceIsolation pins that two brokers of different namespaces
// sharing one SessionStore never route to each other's sessions: the index of a
// broker only ever holds its own scope, and a publication stamped with the other
// namespace has no local target.
func TestRoutingIndexNamespaceIsolation(t *testing.T) {
	store := NewMemorySessionStore()
	a := startBroker(t, Config{Namespace: "tenant-a", SessionStore: store})
	b := startBroker(t, Config{Namespace: "tenant-b", SessionStore: store})

	subA, _ := mustConnect(t, a.Addr(), confConnect{clientID: "same-id", clean: true})
	subscribe(t, subA, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "iso/#", QoS: 1})
	subB, _ := mustConnect(t, b.Addr(), confConnect{clientID: "same-id", clean: true})
	subscribe(t, subB, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "iso/#", QoS: 1})

	inA := Publication{Namespace: "tenant-a", Topic: "iso/x", QoS: 1}
	inB := Publication{Namespace: "tenant-b", Topic: "iso/x", QoS: 1}
	if targets := a.LocalTargets(inA); len(targets) != 1 {
		t.Fatalf("tenant-a targets = %v, want one", targetIDs(targets))
	}
	if targets := a.LocalTargets(inB); len(targets) != 0 {
		t.Fatalf("tenant-a routed a tenant-b publication to %v", targetIDs(targets))
	}
	if targets := b.LocalTargets(inB); len(targets) != 1 {
		t.Fatalf("tenant-b targets = %v, want one", targetIDs(targets))
	}
	if targets := b.LocalTargets(inA); len(targets) != 0 {
		t.Fatalf("tenant-b routed a tenant-a publication to %v", targetIDs(targets))
	}

	// A stored session of one namespace is invisible to the other broker.
	offline, _ := mustConnect(t, b.Addr(), confConnect{clientID: "offline-b", clean: false})
	subscribe(t, offline, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "iso/#", QoS: 1})
	offline.disconnect()
	waitDisconnected(t, b, "offline-b")
	waitFor(t, "the tenant-b session to be stored", func() bool {
		_, ok := store.Load("tenant-b\x00offline-b")
		return ok
	})
	if targets := a.LocalTargets(Publication{Namespace: "tenant-a", Topic: "iso/x"}); len(targets) != 1 {
		t.Fatalf("tenant-a targets after a tenant-b session was stored = %v, want only its own", targetIDs(targets))
	}
	if targets := b.LocalTargets(Publication{Namespace: "tenant-b", Topic: "iso/x"}); len(targets) != 2 {
		t.Fatalf("tenant-b targets = %v, want the live and the offline session", targetIDs(targets))
	}
}

// keyCountingSessionStore counts the enumeration and load calls the broker makes,
// which is how a test proves the publish path does not read the store.
type keyCountingSessionStore struct {
	*MemorySessionStore
	mu    sync.Mutex
	keys  int
	loads int
}

func (s *keyCountingSessionStore) Keys() []string {
	s.mu.Lock()
	s.keys++
	s.mu.Unlock()
	return s.MemorySessionStore.Keys()
}

func (s *keyCountingSessionStore) Load(key string) (SessionState, bool) {
	s.mu.Lock()
	s.loads++
	s.mu.Unlock()
	return s.MemorySessionStore.Load(key)
}

func (s *keyCountingSessionStore) counts() (keys, loads int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keys, s.loads
}

func (s *keyCountingSessionStore) reset() {
	s.mu.Lock()
	s.keys, s.loads = 0, 0
	s.mu.Unlock()
}

// TestPublishDoesNotConsultSessionStore pins the hot-path promise: subscribing and
// publishing to a live subscriber never enumerates or loads the store. An operator
// implementation may be a network call, so it must not be on the publish path at
// all.
func TestPublishDoesNotConsultSessionStore(t *testing.T) {
	store := &keyCountingSessionStore{MemorySessionStore: NewMemorySessionStore()}
	b := startBroker(t, Config{SessionStore: store, Metrics: newDeliveryMetrics()})
	// New indexed the (empty) store; the store also serves the CONNECT paths.
	store.reset()

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "store-sub", clean: true})
	// The clean CONNECT of the subscriber looked the (missing) stored session up;
	// what the test pins is that the SUBSCRIBE and the publish behind it do not.
	store.reset()
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "hot/#", QoS: 1})
	if keys, loads := store.counts(); keys != 0 || loads != 0 {
		t.Fatalf("SUBSCRIBE consulted the store: Keys()=%d Load()=%d", keys, loads)
	}

	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "store-pub", clean: true})
	store.reset()
	for i := 0; i < 3; i++ {
		pub.mustPublish(t, confPublish{topic: "hot/x", payload: "live", qos: 1, pid: uint16(i + 1)})
	}
	for i := 0; i < 3; i++ {
		expectPublish(t, sub)
	}
	if keys, loads := store.counts(); keys != 0 || loads != 0 {
		t.Fatalf("publishing to a live subscriber consulted the store: Keys()=%d Load()=%d", keys, loads)
	}
}

// benchTargets keeps the benchmark result observable, so the loop is not optimized
// away.
var benchTargets []LocalTarget

// benchBroker builds a broker whose routing index holds size subscriptions, split
// over the given number of wildcard filters. The sessions are index entries only:
// no socket is created, because the benchmark measures the routing table, which is
// exactly what a publish looks up.
func benchBroker(size, wildcards int) (*Broker, Publication) {
	b, err := New(Config{})
	if err != nil {
		panic(err)
	}
	for i := 0; i < size; i++ {
		id := fmt.Sprintf("bench-%06d", i)
		c := &client{id: id, connID: ConnectionID("conn-" + id)}
		b.index.setLive(namespaceKey(b.namespace, id), c, map[string]subscription{
			fmt.Sprintf("plant/%06d/temp", i): {qos: 1},
		})
	}
	for j := 0; j < wildcards; j++ {
		id := fmt.Sprintf("wild-%04d", j)
		c := &client{id: id, connID: ConnectionID("conn-" + id)}
		b.index.setLive(namespaceKey(b.namespace, id), c, map[string]subscription{
			fmt.Sprintf("other%04d/+/temp", j): {qos: 1},
		})
	}
	return b, Publication{Namespace: b.namespace, Topic: fmt.Sprintf("plant/%06d/temp", size/2), QoS: 1}
}

// BenchmarkLocalTargets shows that the publish path does not scale with the number
// of subscriptions. The literal cases are the shape a deployment with one filter
// per session has (10k sessions: one map lookup); the wildcard cases add 64
// distinct wildcard filters, which a publication still has to match one by one,
// and show that this cost is the number of distinct wildcard filters rather than
// the number of subscriptions.
func BenchmarkLocalTargets(b *testing.B) {
	for _, size := range []int{10, 1000, 10000} {
		size := size
		for _, wildcards := range []int{0, 64} {
			name := fmt.Sprintf("subs=%d/wildcards=%d", size, wildcards)
			b.Run(name, func(b *testing.B) {
				broker, pub := benchBroker(size, wildcards)
				want := 1
				if got := len(broker.LocalTargets(pub)); got != want {
					b.Fatalf("targets = %d, want %d", got, want)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					benchTargets = broker.LocalTargets(pub)
				}
			})
		}
	}
}

// TestMatchTopicLevels pins the level rules the routing index relies on, including
// the cases a string-split implementation gets right by accident: "#" matches the
// level the topic stopped at ("a/#" matches "a"), "+" never spans a level, and the
// first level of a '$' topic is only matched by a literal first level. Both the
// literal and the wildcard lookup of the index go through this function.
func TestMatchTopicLevels(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"a", "a", true},
		{"a", "b", false},
		{"a/b", "a/b", true},
		{"a/b", "a/b/c", false},
		{"a/b", "a", false},
		{"a/#", "a", true},
		{"a/#", "a/", true},
		{"a/#", "a/b/c", true},
		{"a/#", "b", false},
		{"a/+/#", "a/b", true},
		{"a/+", "a", false},
		{"a/+", "a/b", true},
		{"a/+", "a/b/c", false},
		{"+", "a", true},
		{"+", "a/b", false},
		{"+", "/", false},
		{"+/+", "/", true},
		{"#", "a/b", true},
		{"#", "", true},
		{"#", "$SYS/uptime", false},
		{"+/uptime", "$SYS/uptime", false},
		{"$SYS/#", "$SYS/uptime", true},
		{"$SYS/+", "$SYS", false},
		{"", "", true},
		{"", "a", false},
		{"a", "", false},
		{"a//b", "a//b", true},
		{"a/+/b", "a//b", true},
	}
	for _, tc := range cases {
		if got := matchTopic(tc.filter, tc.topic); got != tc.want {
			t.Errorf("matchTopic(%q, %q) = %v, want %v", tc.filter, tc.topic, got, tc.want)
		}
	}
}

// TestRoutingIndexExpiredSessionIsNotATarget pins the deadline rule: a stored
// session whose Session Expiry Interval elapsed is not a target even before the
// sweeper deleted it, exactly as the scan it replaced decided. The sweeper is
// disabled here, so only the routing table can make that decision.
func TestRoutingIndexExpiredSessionIsNotATarget(t *testing.T) {
	b := startBroker(t, Config{unit: testSecond, SessionSweepInterval: -1})
	c, _ := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "expiring-route",
		clean:    false,
		props:    expiryProps(1),
	})
	subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "e/#", QoS: 1})
	c.disconnect()
	waitDisconnected(t, b, "expiring-route")
	waitFor(t, "the session to be stored", func() bool {
		_, ok := b.store.Load("expiring-route")
		return ok
	})

	pub := Publication{Namespace: b.Namespace(), Topic: "e/x", QoS: 1}
	if targets := b.LocalTargets(pub); len(targets) != 1 || !targets[0].Offline() {
		t.Fatalf("targets inside the expiry window = %+v, want one offline target", targets)
	}
	time.Sleep(3 * testSecond)
	if _, ok := b.store.Load("expiring-route"); !ok {
		t.Fatal("the session was deleted although the sweeper is disabled")
	}
	if targets := b.LocalTargets(pub); len(targets) != 0 {
		t.Fatalf("targets after the deadline = %+v, want none", targets)
	}
}
