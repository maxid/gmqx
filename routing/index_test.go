package routing

// Routing-index tests. The cases here pin the table itself — buckets, live
// counters, the displacement rule, shared-group membership and the round robin —
// directly against an Index with fake endpoints; the behavior a broker exposes
// (LocalTargets, SendLocal, the store round trip) stays pinned by the broker-level
// suite in gmqx/internal/engine.

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"

	"github.com/maxid/gmqx/session"
)

// fakeEndpoint is one live connection as the index sees it. It records what a
// target delivered through it, so a test can tell which connection a target names.
type fakeEndpoint struct {
	id       string
	connID   ConnectionID
	received []string
}

func (e *fakeEndpoint) ClientID() string           { return e.id }
func (e *fakeEndpoint) ConnectionID() ConnectionID { return e.connID }
func (e *fakeEndpoint) DeliverPublish(topic string, _ []byte, _ byte, _ bool, _ mqtt.Properties) error {
	e.received = append(e.received, topic)
	return nil
}

// liveSession installs one live session with one filter and returns its endpoint.
func liveSession(idx *Index, id, filter string, sub session.Subscription) *fakeEndpoint {
	e := &fakeEndpoint{id: id, connID: ConnectionID("conn/" + id)}
	idx.SetLive(id, e, map[string]session.Subscription{filter: sub})
	return e
}

// targetIDs lists and sorts the client ids of a target set.
func targetIDs(targets []Target) []string {
	out := make([]string, 0, len(targets))
	for _, t := range targets {
		out = append(out, t.ClientID)
	}
	sort.Strings(out)
	return out
}

// TestIndexMatchesFilters pins the matching rules the index owns: literal and
// wildcard filters, the per-session QoS cap, retain-as-published, no-local, the
// '$' exclusion and the merge of overlapping filters of one session into one
// target.
func TestIndexMatchesFilters(t *testing.T) {
	const topic = "plant/7/temp"
	now := ConnectionID("conn/pub")

	t.Run("literal, wildcard and overlap merge into one target per session", func(t *testing.T) {
		idx := NewIndex()
		liveSession(idx, "literal", topic, session.Subscription{QoS: 1})
		liveSession(idx, "wild", "plant/+/temp", session.Subscription{QoS: 2})
		liveSession(idx, "deep", "plant/#", session.Subscription{QoS: 0})
		liveSession(idx, "other", "plant/+/hum", session.Subscription{QoS: 2})

		got := idx.Targets(topic, now, 1, false)
		if ids := targetIDs(got); !reflect.DeepEqual(ids, []string{"deep", "literal", "wild"}) {
			t.Fatalf("targets = %v, want deep, literal, wild", ids)
		}
		for _, target := range got {
			wantQoS := map[string]byte{"literal": 1, "wild": 1, "deep": 0}[target.ClientID]
			if target.QoS != wantQoS {
				t.Fatalf("%s target QoS = %d, want %d", target.ClientID, target.QoS, wantQoS)
			}
		}
	})

	t.Run("overlapping filters keep the highest QoS and merge identifiers", func(t *testing.T) {
		idx := NewIndex()
		idx.SetLive("a", &fakeEndpoint{id: "a", connID: "conn/a"}, map[string]session.Subscription{
			"plant/+/temp": {QoS: 0, SubscriptionIDs: []uint32{5}},
			"plant/#":      {QoS: 2, SubscriptionIDs: []uint32{9}, RetainAsPublished: true},
		})
		got := idx.Targets(topic, "", 2, true)
		if len(got) != 1 {
			t.Fatalf("targets = %+v, want one merged target", got)
		}
		if got[0].QoS != 2 || !got[0].Retain {
			t.Fatalf("target = %+v, want QoS 2 with retain-as-published", got[0])
		}
		// The order the identifiers were collected in depends on map iteration; the
		// property block keeps them all, and MQTT does not order them.
		ids := append([]uint32(nil), got[0].SubscriptionIDs()...)
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if !reflect.DeepEqual(ids, []uint32{5, 9}) {
			t.Fatalf("subscription identifiers = %v, want {5, 9}", got[0].SubscriptionIDs())
		}
	})

	t.Run("no-local excludes the publishing connection only", func(t *testing.T) {
		idx := NewIndex()
		publisher := liveSession(idx, "pub", topic, session.Subscription{QoS: 1, NoLocal: true})
		liveSession(idx, "sub", topic, session.Subscription{QoS: 1})
		liveSession(idx, "shared-pub", topic, session.Subscription{QoS: 1})

		if ids := targetIDs(idx.Targets(topic, publisher.ConnectionID(), 1, false)); !reflect.DeepEqual(ids, []string{"shared-pub", "sub"}) {
			t.Fatalf("targets = %v, want the two other sessions", ids)
		}
		// A publication without an origin connection (a will, a remote event) is
		// excluded by nothing.
		if ids := targetIDs(idx.Targets(topic, "", 1, false)); len(ids) != 3 {
			t.Fatalf("targets without an origin = %v, want all three", ids)
		}
	})

	t.Run("dollar topics are not matched by a wildcard first level", func(t *testing.T) {
		idx := NewIndex()
		liveSession(idx, "hash", "#", session.Subscription{QoS: 1})
		liveSession(idx, "plus", "+/uptime", session.Subscription{QoS: 1})
		liveSession(idx, "sys", "$SYS/#", session.Subscription{QoS: 1})

		if ids := targetIDs(idx.Targets("$SYS/uptime", "", 1, false)); !reflect.DeepEqual(ids, []string{"sys"}) {
			t.Fatalf("targets = %v, want only $SYS/#", ids)
		}
	})
}

// TestIndexTransitions pins every transition the index has: a live install
// replaces a stored entry, subscribe/unsubscribe mutate one session, and removing a
// session drops its buckets, its live counts and its membership, leaving nothing
// behind.
func TestIndexTransitions(t *testing.T) {
	idx := NewIndex()
	idx.SetStored("a", "a", map[string]session.SessionSubscription{
		"stored/x": {QoS: 1},
	}, time.Time{})
	if got := len(idx.Targets("stored/x", "", 1, false)); got != 1 {
		t.Fatalf("stored session targets = %d, want 1", got)
	}

	e := &fakeEndpoint{id: "a", connID: "conn/a"}
	idx.SetLive("a", e, map[string]session.Subscription{"live/x": {QoS: 1}})
	if ids := targetIDs(idx.Targets("stored/x", "", 1, false)); len(ids) != 0 {
		t.Fatalf("stored filter still routes after the session went live: %v", ids)
	}
	if ids := targetIDs(idx.Targets("live/x", "", 1, false)); len(ids) != 1 {
		t.Fatalf("live filter does not route: %v", ids)
	}

	if !idx.Add("a", e, "live/y", session.Subscription{QoS: 2}) {
		t.Fatal("Add of a filter of the owning connection was rejected")
	}
	if got := idx.EntryCount(); got != 2 {
		t.Fatalf("entry count = %d, want 2", got)
	}
	if !idx.RemoveFilter("a", e, "live/y") {
		t.Fatal("RemoveFilter of a filter of the owning connection was rejected")
	}
	if got := idx.EntryCount(); got != 1 {
		t.Fatalf("entry count after the unsubscribe = %d, want 1", got)
	}

	idx.Remove("a")
	if idx.Has("a") || idx.Len() != 0 || idx.EntryCount() != 0 {
		t.Fatalf("after Remove: has=%v len=%d entries=%d, want 0/0/0", idx.Has("a"), idx.Len(), idx.EntryCount())
	}
	if got := len(idx.Targets("live/x", "", 1, false)); got != 0 {
		t.Fatalf("targets after Remove = %d, want 0", got)
	}
	if filters := idx.LiveFilters(); len(filters) != 0 {
		t.Fatalf("advertised filters after Remove = %v, want none", filters)
	}
}

// TestIndexDisplacedEndpointIsIgnored pins the take-over rule: a connection the
// index no longer attributes to a session cannot change that session's routing,
// whichever side of the take-over it is on.
func TestIndexDisplacedEndpointIsIgnored(t *testing.T) {
	idx := NewIndex()
	old := &fakeEndpoint{id: "c", connID: "conn/old"}
	next := &fakeEndpoint{id: "c", connID: "conn/new"}
	idx.SetLive("c", old, map[string]session.Subscription{"t/x": {QoS: 1}})

	if idx.Add("c", next, "t/stolen", session.Subscription{QoS: 2}) {
		t.Fatal("an unregistered connection installed a subscription")
	}
	if idx.RemoveFilter("c", next, "t/x") {
		t.Fatal("an unregistered connection removed a subscription")
	}

	idx.SetLive("c", next, map[string]session.Subscription{"t/x": {QoS: 1}})
	if idx.Add("c", old, "t/hijack", session.Subscription{QoS: 2}) {
		t.Fatal("the displaced connection changed the routing of its replacement")
	}
	if idx.RemoveFilter("c", old, "t/x") {
		t.Fatal("the displaced connection removed the routing of its replacement")
	}
	if got := len(idx.Targets("t/x", "", 1, false)); got != 1 {
		t.Fatalf("targets after the ignored transitions = %d, want 1", got)
	}
}

// TestIndexTarget pins the directed-publish answer: the live form names the
// connection, the stored form names the store key, an expired session is still
// answered (the queue path re-checks the deadline) and an unknown key is not.
func TestIndexTarget(t *testing.T) {
	idx := NewIndex()
	e := &fakeEndpoint{id: "live", connID: "conn/live"}
	idx.SetLive("live", e, map[string]session.Subscription{"t": {QoS: 1}})
	idx.SetStored("stored", "stored", map[string]session.SessionSubscription{"t": {QoS: 1}}, time.Now().Add(-time.Hour))

	live, ok := idx.Target("live", 2, true)
	if !ok || live.Offline() || live.Endpoint() != e || live.ConnectionID != "conn/live" || live.QoS != 2 || !live.Retain {
		t.Fatalf("live target = %+v (found %v), want the connection with the publication's QoS and retain", live, ok)
	}
	stored, ok := idx.Target("stored", 1, false)
	if !ok || !stored.Offline() || stored.Endpoint() != nil || stored.SessionKey() != "stored" || stored.ConnectionID != "" {
		t.Fatalf("stored target = %+v (found %v), want the store key and no connection", stored, ok)
	}
	if _, ok := idx.Target("unknown", 1, false); ok {
		t.Fatal("an unknown key produced a target")
	}
}

// TestIndexStoredExpiry pins the deadline rule: a stored session whose Session
// Expiry Interval elapsed is not a matching target before anything deletes it, but
// its directed target still exists — the queue path is what counts that drop.
func TestIndexStoredExpiry(t *testing.T) {
	idx := NewIndex()
	idx.SetStored("gone", "gone", map[string]session.SessionSubscription{"e/x": {QoS: 1}}, time.Now().Add(-time.Second))
	if got := len(idx.Targets("e/x", "", 1, false)); got != 0 {
		t.Fatalf("expired session targets = %d, want 0", got)
	}
	if _, ok := idx.Target("gone", 1, false); !ok {
		t.Fatal("the directed target of an indexed expired session is missing")
	}
}

// TestIndexSharedGroups pins that a shared subscription is a group of live members
// only: a stored session keeps its subscription in its state (so a resume restores
// it) but is not a member, and a group disappears with its last member.
func TestIndexSharedGroups(t *testing.T) {
	const group = "$share/workers/jobs/+"
	idx := NewIndex()
	idx.SetStored("offline", "offline", map[string]session.SessionSubscription{group: {QoS: 1}}, time.Time{})
	if idx.SharedGroupCount() != 0 || len(idx.LiveSharedGroups()) != 0 {
		t.Fatal("a stored session joined a shared group")
	}
	idx.SetLive("a", &fakeEndpoint{id: "a", connID: "conn/a"}, map[string]session.Subscription{group: {QoS: 1}})
	idx.SetLive("b", &fakeEndpoint{id: "b", connID: "conn/b"}, map[string]session.Subscription{group: {QoS: 1}})

	if groups := idx.SharedGroups("jobs/clean"); !reflect.DeepEqual(groups, []string{group}) {
		t.Fatalf("shared groups matching jobs/clean = %v, want %s", groups, group)
	}
	if groups := idx.SharedGroups("other/clean"); len(groups) != 0 {
		t.Fatalf("shared groups matching another topic = %v, want none", groups)
	}

	// Round robin over the sorted member list: every member is picked once per
	// round, and the cursor wraps.
	var seen []string
	for i := 0; i < 4; i++ {
		target, ok := idx.SharedTarget(group, 1, false)
		if !ok {
			t.Fatal("the group has no target although it has two members")
		}
		seen = append(seen, target.ClientID)
	}
	if !reflect.DeepEqual(seen, []string{"a", "b", "a", "b"}) {
		t.Fatalf("round robin = %v, want a b a b", seen)
	}
	if _, ok := idx.SharedTarget("$share/workers/none", 1, false); ok {
		t.Fatal("an unknown group produced a target")
	}

	idx.SetStored("a", "a", map[string]session.SessionSubscription{group: {QoS: 1}}, time.Time{})
	if got := idx.SharedGroupCount(); got != 1 {
		t.Fatalf("shared groups after one member went offline = %d, want 1", got)
	}
	idx.Remove("b")
	if got := idx.SharedGroupCount(); got != 0 {
		t.Fatalf("shared groups after the last member left = %d, want 0", got)
	}
}

// TestIndexLiveFilters pins the advertised summary: sorted, de-duplicated and live
// only, because a stored session is drained by the node that holds it.
func TestIndexLiveFilters(t *testing.T) {
	idx := NewIndex()
	liveSession(idx, "a", "z/x", session.Subscription{QoS: 1})
	liveSession(idx, "b", "z/x", session.Subscription{QoS: 1})
	liveSession(idx, "c", "a/+", session.Subscription{QoS: 1})
	liveSession(idx, "d", "$share/g/z/#", session.Subscription{QoS: 1})
	idx.SetStored("offline", "offline", map[string]session.SessionSubscription{"stored/x": {QoS: 1}}, time.Time{})

	if got := idx.LiveFilters(); !reflect.DeepEqual(got, []string{"a/+", "z/x"}) {
		t.Fatalf("live filters = %v, want [a/+ z/x] (shared and stored excluded)", got)
	}
	if got := idx.LiveSharedGroups(); !reflect.DeepEqual(got, []string{"$share/g/z/#"}) {
		t.Fatalf("live shared groups = %v, want the one group", got)
	}
}

// TestIndexDeliversThroughEndpoint pins that a target handed out by the index is
// delivered through the endpoint it names, not through anything the index keeps.
func TestIndexDeliversThroughEndpoint(t *testing.T) {
	idx := NewIndex()
	e := liveSession(idx, "a", "t/x", session.Subscription{QoS: 1})
	targets := idx.Targets("t/x", "", 1, false)
	if len(targets) != 1 {
		t.Fatalf("targets = %+v, want one", targets)
	}
	if err := targets[0].Endpoint().DeliverPublish("t/x", []byte("p"), 1, false, mqtt.Properties{}); err != nil {
		t.Fatalf("delivery through the endpoint: %v", err)
	}
	if !reflect.DeepEqual(e.received, []string{"t/x"}) {
		t.Fatalf("endpoint received %v, want [t/x]", e.received)
	}
}
