package gmqx

// Shared subscription tests (MQTT 5 §4.8.2): a "$share/{ShareName}/{filter}"
// subscription is a group, a matching publication goes to exactly one live member
// of each group, and a normal subscriber on the same filter still gets its own
// copy. The cluster half of the feature lives in shared_cluster_test.go.

import (
	"fmt"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// subscribeShared subscribes filter on c and requires the broker to grant qos.
func subscribeShared(t *testing.T, c *confClient, pid uint16, filter string, qos byte, props mqtt.Properties) {
	t.Helper()
	codes := subscribe(t, c, pid, props, mqtt.Subscription{Filter: filter, QoS: qos}).ReasonCodes
	if len(codes) != 1 || codes[0] != qos {
		t.Fatalf("SUBACK codes %v for %q, want [%d]", codes, filter, qos)
	}
}

// sharedGroupCount is the number of shared groups the routing index holds. It is
// read under the index lock, because the broker's own goroutines update it.
func sharedGroupCount(b *Broker) int {
	b.index.mu.RLock()
	defer b.index.mu.RUnlock()
	return len(b.index.shared)
}

// TestSharedSubscriptionRoundRobin is the core of the feature: two members of one
// group receive alternating publications, and nothing is duplicated.
func TestSharedSubscriptionRoundRobin(t *testing.T) {
	b := startBroker(t, Config{})
	first, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "rr-1", clean: true})
	second, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "rr-2", clean: true})
	subscribeShared(t, first, 1, "$share/workers/jobs/+", 1, mqtt.Properties{})
	subscribeShared(t, second, 1, "$share/workers/jobs/+", 1, mqtt.Properties{})

	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "rr-pub", clean: true})
	for i, payload := range []string{"one", "two", "three", "four"} {
		pub.publish(confPublish{topic: "jobs/x", payload: payload, qos: 0})
		// The member is chosen round robin over the sorted session keys, so the
		// expected member of each publication is deterministic.
		want := first
		if i%2 == 1 {
			want = second
		}
		if got := expectPublish(t, want); string(got.Payload) != payload {
			t.Fatalf("publication %d delivered %q to the wrong member, want %q", i, got.Payload, payload)
		}
	}
	// Exactly one copy per publication: the other member has nothing pending.
	expectNoMessage(t, first, 100*time.Millisecond)
	expectNoMessage(t, second, 100*time.Millisecond)

	// The group survives the churn of the two members and holds both.
	if groups := b.index.liveSharedGroups(); len(groups) != 1 || groups[0] != "$share/workers/jobs/+" {
		t.Fatalf("live shared groups = %v, want the one group", groups)
	}
}

// TestSharedSubscriptionGroupsAreIndependent pins the two-group rule of §4.8.2: a
// publication matching two groups is delivered once per group, and a normal
// subscriber of the same filter gets its own copy on top.
func TestSharedSubscriptionGroupsAreIndependent(t *testing.T) {
	b := startBroker(t, Config{})
	a1, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "g1-a", clean: true})
	a2, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "g1-b", clean: true})
	b1, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "g2-a", clean: true})
	plain, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "g-plain", clean: true})
	subscribeShared(t, a1, 1, "$share/one/news/+", 1, mqtt.Properties{})
	subscribeShared(t, a2, 1, "$share/one/news/+", 1, mqtt.Properties{})
	subscribeShared(t, b1, 1, "$share/two/news/+", 1, mqtt.Properties{})
	if codes := subscribe(t, plain, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "news/+", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v for the normal subscriber", codes)
	}

	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "g-pub", clean: true})
	pub.publish(confPublish{topic: "news/1", payload: "hello", qos: 0})

	// Group one: one of its two members. Group two: its only member. Plus the
	// normal subscriber.
	if got := expectPublish(t, a1); string(got.Payload) != "hello" {
		t.Fatalf("group one member received %q", got.Payload)
	}
	if got := expectPublish(t, b1); string(got.Payload) != "hello" {
		t.Fatalf("group two member received %q", got.Payload)
	}
	if got := expectPublish(t, plain); string(got.Payload) != "hello" {
		t.Fatalf("normal subscriber received %q", got.Payload)
	}
	// The second member of group one must not have received the same publication.
	expectNoMessage(t, a2, 100*time.Millisecond)
}

// TestSharedSubscriptionUnsubscribe pins that a member that leaves stops receiving
// immediately, that the rest keep rotating, and that the group's state (its
// round-robin cursor) does not outlive its last member.
func TestSharedSubscriptionUnsubscribe(t *testing.T) {
	b := startBroker(t, Config{})
	leaving, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "u-1", clean: true})
	second, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "u-2", clean: true})
	third, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "u-3", clean: true})
	subscribeShared(t, leaving, 1, "$share/g/u/x", 1, mqtt.Properties{})
	subscribeShared(t, second, 1, "$share/g/u/x", 1, mqtt.Properties{})
	subscribeShared(t, third, 1, "$share/g/u/x", 1, mqtt.Properties{})

	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "u-pub", clean: true})
	pub.publish(confPublish{topic: "u/x", payload: "before", qos: 0})
	if got := expectPublish(t, leaving); string(got.Payload) != "before" {
		t.Fatalf("first member received %q", got.Payload)
	}

	leaving.unsubscribe(2, "$share/g/u/x")
	if groups := b.index.liveSharedGroups(); len(groups) != 1 {
		t.Fatalf("live shared groups after one unsubscribe = %v, want the group to survive", groups)
	}

	// Four publications now go to the two remaining members, two each: the cursor
	// keeps rotating where it was, so the exact order depends on how many
	// deliveries happened before the unsubscribe. What is pinned is that the group
	// still rotates (neither remaining member starves) and that the member that
	// left receives nothing.
	for _, payload := range []string{"p1", "p2", "p3", "p4"} {
		pub.publish(confPublish{topic: "u/x", payload: payload, qos: 0})
	}
	gotSecond := payloads(collectPublishes(t, second, 300*time.Millisecond))
	gotThird := payloads(collectPublishes(t, third, 300*time.Millisecond))
	if len(gotSecond) != 2 || len(gotThird) != 2 {
		t.Fatalf("remaining members received %v and %v, want two publications each", gotSecond, gotThird)
	}
	if seen := append(append([]string{}, gotSecond...), gotThird...); !sameSet(seen, []string{"p1", "p2", "p3", "p4"}) {
		t.Fatalf("publications delivered to the remaining members = %v, want each of p1..p4 once", seen)
	}
	expectNoMessage(t, leaving, 100*time.Millisecond)

	// The last members leave: the group and its cursor go with them, so a client
	// that invents group names cannot grow the index without bound.
	second.unsubscribe(2, "$share/g/u/x")
	third.unsubscribe(2, "$share/g/u/x")
	waitFor(t, "the shared group to disappear", func() bool { return sharedGroupCount(b) == 0 })
	if groups := b.index.liveSharedGroups(); len(groups) != 0 {
		t.Fatalf("live shared groups after every member left = %v, want none", groups)
	}
	// A publication to the group's filter now reaches nobody.
	pub.publish(confPublish{topic: "u/x", payload: "after", qos: 0})
	expectNoMessage(t, second, 100*time.Millisecond)
}

// TestSharedSubscriptionMalformed pins the parse rules of §4.8.2: a filter that
// starts with "$share/" but is not "<name>/<filter>" is refused per filter with
// 0x8F, while a filter that merely starts with "$share" stays what it always was,
// an ordinary literal filter (and "$share" alone is a literal too).
func TestSharedSubscriptionMalformed(t *testing.T) {
	b := startBroker(t, Config{})
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "malformed", clean: true})

	malformed := []string{
		"$share/",        // no ShareName and no filter
		"$share//x",      // empty ShareName
		"$share/g",       // ShareName without a "/filter"
		"$share/g/",      // empty filter
		"$share/g+/x",    // "+" in the ShareName
		"$share/g#/x",    // "#" in the ShareName
		"$share/g/a/#/b", // "#" not at the end of the filter
		"$share/g/a+/b",  // "+" inside a level of the filter
	}
	for i, filter := range malformed {
		codes := subscribe(t, c, uint16(i+1), mqtt.Properties{}, mqtt.Subscription{Filter: filter, QoS: 1}).ReasonCodes
		if len(codes) != 1 || codes[0] != 0x8f {
			t.Fatalf("SUBACK codes %v for malformed %q, want [0x8F]", codes, filter)
		}
	}
	// The refusals are per filter: the connection is still usable.
	c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	readConf[*mqtt.Pingresp](t, c)

	// "$share" alone does not have the shared shape: it is a literal filter, and a
	// publication to that literal topic reaches it.
	literal, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "literal", clean: true})
	if codes := subscribe(t, literal, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "$share", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v for \"$share\", want [0]", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "malformed-pub", clean: true})
	pub.publish(confPublish{topic: "$share", payload: "literal", qos: 0})
	if got := expectPublish(t, literal); string(got.Payload) != "literal" {
		t.Fatalf("literal \"$share\" subscriber received %q", got.Payload)
	}
}

// TestSharedSubscriptionNoLocalRefused pins MQTT-3.8.3-4: a shared subscription
// must not set No Local. The refusal is per filter (0x8F) and the connection
// survives, like every other per-filter refusal of §3.9.
func TestSharedSubscriptionNoLocalRefused(t *testing.T) {
	b := startBroker(t, Config{})
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "nl-shared", clean: true})
	codes := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "$share/g/nl/x", QoS: 1, NoLocal: true}).ReasonCodes
	if len(codes) != 1 || codes[0] != 0x8f {
		t.Fatalf("SUBACK codes %v for a No Local shared subscription, want [0x8F]", codes)
	}
	// The same filter without No Local is accepted on the same connection.
	subscribeShared(t, c, 2, "$share/g/nl/x", 1, mqtt.Properties{})
}

// TestSharedSubscriptionNoRetainedReplay pins §4.8.2: no retained message is sent
// to a session when it subscribes to a shared subscription, while a later matching
// publication is delivered normally.
func TestSharedSubscriptionNoRetainedReplay(t *testing.T) {
	b := startBroker(t, Config{})
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "ret-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "shared-ret/x", payload: "stored", qos: 1, pid: 1, retain: true})

	member, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "ret-member", clean: true})
	subscribeShared(t, member, 1, "$share/g/shared-ret/x", 1, mqtt.Properties{})
	expectNoMessage(t, member, 150*time.Millisecond)

	// A normal subscriber of the same topic still gets the replay, which proves
	// the retained message was there and only the shared subscription skipped it.
	normal, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "ret-normal", clean: true})
	if codes := subscribe(t, normal, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "shared-ret/x", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v for the normal subscriber", codes)
	}
	if got := expectPublish(t, normal); string(got.Payload) != "stored" || !got.Retain {
		t.Fatalf("retained replay = %q retain=%v", got.Payload, got.Retain)
	}

	// Later publications reach the group.
	pub.mustPublish(t, confPublish{topic: "shared-ret/x", payload: "live", qos: 1, pid: 2})
	if got := expectPublish(t, member); string(got.Payload) != "live" {
		t.Fatalf("live delivery to the group = %q", got.Payload)
	}
}

// TestSharedSubscriptionOfflineMemberReceivesNothing pins the documented rule:
// shared state is not queued. A persistent session that subscribed to a group and
// went offline is not a member while it is away, so another live member takes
// every publication and nothing is waiting for it on its return.
func TestSharedSubscriptionOfflineMemberReceivesNothing(t *testing.T) {
	b := startBroker(t, Config{})
	away, ack := mustConnect(t, b.Addr(), confConnect{
		version: mqtt.Version5, clientID: "away", clean: false, props: expiryProps(60),
	})
	if ack.SessionPresent {
		t.Fatal("a fresh session reports Session Present")
	}
	subscribeShared(t, away, 1, "$share/g/offline/x", 1, mqtt.Properties{})
	away.disconnect()
	waitDisconnected(t, b, "away")
	waitFor(t, "the session to be stored", func() bool {
		_, ok := b.store.Load("away")
		return ok
	})
	stored, _ := b.store.Load("away")
	if _, kept := stored.Subscriptions["$share/g/offline/x"]; !kept {
		t.Fatal("the stored session lost its shared subscription")
	}
	// The offline session is not a member of the group.
	if groups := b.index.liveSharedGroups(); len(groups) != 0 {
		t.Fatalf("live shared groups with only an offline member = %v, want none", groups)
	}

	live, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "live-member", clean: true})
	subscribeShared(t, live, 1, "$share/g/offline/x", 1, mqtt.Properties{})
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "offline-pub", clean: true})
	for i, payload := range []string{"one", "two", "three"} {
		pub.publish(confPublish{topic: "offline/x", payload: payload, qos: 0})
		if got := expectPublish(t, live); string(got.Payload) != payload {
			t.Fatalf("live member received %q for publication %d", got.Payload, i)
		}
	}
	if queued := offlineDepth(b); queued != 0 {
		t.Fatalf("messages queued for the offline shared member = %d, want 0", queued)
	}

	// The resume restores the subscription (it is session state) but drains no
	// shared message, because none was queued.
	back, ack := mustConnect(t, b.Addr(), confConnect{
		version: mqtt.Version5, clientID: "away", clean: false, props: expiryProps(60),
	})
	if !ack.SessionPresent {
		t.Fatal("the resumed session does not report Session Present")
	}
	expectNoMessage(t, back, 150*time.Millisecond)
	// Both sessions are members again: the next publication goes to exactly one of
	// them, whichever the cursor names.
	pub.publish(confPublish{topic: "offline/x", payload: "after-resume", qos: 0})
	delivered := append(payloads(collectPublishes(t, back, 300*time.Millisecond)),
		payloads(collectPublishes(t, live, 300*time.Millisecond))...)
	if !sameSet(delivered, []string{"after-resume"}) {
		t.Fatalf("publications delivered after the resume = %v, want exactly one copy", delivered)
	}
}

// TestSharedSubscriptionKeepsStoreOffThePublishPath is the explicit routing-index
// assertion: a publication that matches a shared group is answered by the index and
// the peer view, never by a SessionStore call.
func TestSharedSubscriptionKeepsStoreOffThePublishPath(t *testing.T) {
	store := &keyCountingSessionStore{MemorySessionStore: NewMemorySessionStore()}
	b := startBroker(t, Config{SessionStore: store, Metrics: newDeliveryMetrics()})
	store.reset()

	member, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "store-member", clean: true})
	// The CONNECT itself looks the (missing) stored session up; what this test pins
	// is that the SUBSCRIBE and the publish behind it do not.
	store.reset()
	subscribeShared(t, member, 1, "$share/g/store/x", 1, mqtt.Properties{})
	if keys, loads := store.counts(); keys != 0 || loads != 0 {
		t.Fatalf("a shared SUBSCRIBE consulted the store: Keys()=%d Load()=%d", keys, loads)
	}

	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "store-pub", clean: true})
	store.reset()
	for i := 0; i < 3; i++ {
		pub.mustPublish(t, confPublish{topic: "store/x", payload: fmt.Sprintf("m%d", i), qos: 1, pid: uint16(i + 1)})
		if got := expectPublish(t, member); string(got.Payload) != fmt.Sprintf("m%d", i) {
			t.Fatalf("shared delivery %d = %q", i, got.Payload)
		}
	}
	if keys, loads := store.counts(); keys != 0 || loads != 0 {
		t.Fatalf("publishing to a shared group consulted the store: Keys()=%d Load()=%d", keys, loads)
	}
}

// benchSharedSelection keeps the benchmark result observable, so the loop is not
// optimized away.
var (
	benchSharedTarget LocalTarget
	benchSharedOK     bool
)

// BenchmarkSharedTargetSelection shows that picking the member of a shared group is
// a map lookup plus an index into a sorted slice: it does not consult the
// SessionStore, and it does not scan the group's members.
func BenchmarkSharedTargetSelection(b *testing.B) {
	for _, members := range []int{1, 10, 1000} {
		members := members
		b.Run(fmt.Sprintf("members=%d", members), func(b *testing.B) {
			broker, err := New(Config{})
			if err != nil {
				b.Fatal(err)
			}
			const group = "$share/bench/jobs/+"
			for i := 0; i < members; i++ {
				id := fmt.Sprintf("bench-%06d", i)
				c := &client{id: id, connID: ConnectionID("conn-" + id)}
				broker.index.setLive(namespaceKey(broker.namespace, id), c, map[string]subscription{
					group: {qos: 1},
				})
			}
			if len(broker.index.liveSharedGroups()) != 1 {
				b.Fatalf("the benchmark group is not indexed")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchSharedTarget, benchSharedOK = broker.index.sharedTarget(group, 1, false)
			}
		})
	}
}

// payloads returns the payloads of a collected batch, in arrival order.
func payloads(msgs []*mqtt.Publish) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, string(m.Payload))
	}
	return out
}

// sameSet reports whether got holds exactly the values of want, once each.
func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]int, len(got))
	for _, v := range got {
		seen[v]++
	}
	for _, v := range want {
		if seen[v] != 1 {
			return false
		}
	}
	return true
}
