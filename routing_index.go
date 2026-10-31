package gmqx

// Routing index
//
// The index is the broker's answer to "which local sessions does this
// publication match", and it replaces the scan the broker used to run on every
// publish (every live connection, every stored session, and a SessionStore.Keys
// + Load round trip for each of them, all under the broker's global lock).
//
// Shape:
//
//   - filters without a wildcard level live in `exact`, keyed by the filter, so a
//     literal topic is one map lookup;
//   - filters with a wildcard level live in `wildcard`, keyed by the filter; a
//     publication still has to run matchTopic against each of them, so the cost is
//     the number of distinct wildcard filters, not the number of subscriptions;
//   - each filter bucket maps a session key to the entry that publishes through
//     it, so a session subscribed to several overlapping filters is merged into
//     one target with the highest matching QoS and the OR of retain-as-published;
//   - `sessions` holds every indexed session, including a stored session with no
//     filters at all, so a directed publish can tell "this client id is a known
//     local session" from "this client id is not local";
//   - `exact` and `wildcard` hold normal subscriptions only. A shared
//     subscription ("$share/{name}/{filter}") lives in `shared`, keyed by its
//     full filter, because it must not behave like a normal one: a publication
//     matching it goes to exactly one member of the group, not to every member.
//     Keeping the two apart is also what makes a group's round-robin state
//     disappear with its last member (see sharedGroup);
//   - `live` counts, per filter, the live sessions subscribed to it, which is what
//     the cluster summary (subscriptionFilters) advertises without touching a
//     single connection. Shared groups are advertised separately
//     (liveSharedGroups): a peer needs to know which node holds a member of one,
//     not which filter a normal subscriber uses.
//
// An indexedSession is immutable once installed: a transition (connect,
// disconnect, session save, session take, expiry) installs a new object, so a
// caller that reads the fields of a returned *indexedSession never races with
// the session changing hands. Only the `subs` map of the session currently
// installed is mutated, and only under the index lock.
//
// Consistency guarantee. Every subscription change updates the index before the
// packet that reports it leaves the broker: the entry of a SUBSCRIBE is routable
// before its SUBACK is written, the removal of an UNSUBSCRIBE before its
// UNSUBACK, and the subscriptions a resumed session restores before its CONNACK.
// A session that goes away is converted (a persistent session becomes a queued,
// offline target) or removed under the same Broker.mu section that moves it
// between the client table and the store, so a publication observes either the
// live entry or the stored entry of a session, never both: a message cannot be
// delivered twice because a session reconnected while it was being routed.
//
// Lock order: Broker.mu before subscriptionIndex.mu. Nothing takes Broker.mu
// while holding the index, so routing never nests the two: LocalTargets takes
// only the index, and SendLocal takes the broker lock afterwards.

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// subscriptionIndex is the broker's routing table. See the file comment for the
// structure and the consistency guarantee.
type subscriptionIndex struct {
	mu sync.RWMutex
	// exact holds the filters without a wildcard level, wildcard the ones with a
	// wildcard level. Both map a filter to the sessions subscribed through it.
	exact    map[string]map[string]*routedSubscription
	wildcard map[string]map[string]*routedSubscription
	// sessions holds every indexed session by key, including sessions with no
	// subscription at all.
	sessions map[string]*indexedSession
	// live counts the live sessions subscribed to each filter.
	live map[string]int
	// shared holds the live shared subscription groups, keyed by their full
	// "$share/{name}/{filter}" filter. A group is present exactly while at least
	// one live session is a member of it, so its round-robin cursor cannot
	// outlive the group.
	shared map[string]*sharedGroup
}

// sharedGroup is one live shared subscription: the group key
// ("$share/{name}/{filter}"), its live members and the round-robin cursor that
// picks the member one publication goes to.
//
// Only live sessions are members. A persistent session that subscribed to a
// shared filter while connected keeps the subscription in its stored state (so a
// resume restores it), but it is not a member while it is offline: shared state is
// not queued for it, and another member of the group takes the publication
// instead (§4.8.2: a publication is sent to exactly one of the sessions).
type sharedGroup struct {
	// filter is the parsed group key: what a publication is matched against.
	filter sharedFilter
	// members maps a session key to its entry.
	members map[string]*routedSubscription
	// order is the sorted member keys, so the round-robin choice is stable and
	// does not depend on map iteration order.
	order []string
	// next is the round-robin cursor. It wraps with modulo the current member
	// count, so a member that leaves cannot make it point past the end.
	next uint64
}

// indexedSession is one session as the index knows it: its filters, and whether a
// live connection owns it (client != nil) or it is a stored session with no
// connection (client == nil). The fields are written once, when the object is
// installed.
type indexedSession struct {
	key      string
	clientID string
	client   *client
	connID   ConnectionID
	// expiresAt is the deadline of a stored session (the zero time for a live one
	// and for a session that never expires). A session whose deadline passed is not
	// a target even before the sweeper deleted it, exactly as the scan it replaced
	// decided.
	expiresAt time.Time
	// subs is the session's subscription set. It is only read and written while
	// the index lock is held.
	subs map[string]subscription
}

// routedSubscription is one (filter, session) entry: the subscription that makes
// the session a target of a publication matching filter.
type routedSubscription struct {
	session *indexedSession
	sub     subscription
}

func newSubscriptionIndex() *subscriptionIndex {
	return &subscriptionIndex{
		exact:    make(map[string]map[string]*routedSubscription),
		wildcard: make(map[string]map[string]*routedSubscription),
		sessions: make(map[string]*indexedSession),
		live:     make(map[string]int),
		shared:   make(map[string]*sharedGroup),
	}
}

// hasWildcard reports whether a topic filter contains a wildcard level. A filter
// like "sensors/+/temp" or "plant/#" has one; a literal topic used as a filter
// does not.
func hasWildcard(filter string) bool { return strings.ContainsAny(filter, "+#") }

// bucket returns the map one normal filter stores its entries in. It is not used
// for shared filters: a group's members live in sharedGroup.members.
func (idx *subscriptionIndex) bucket(filter string) map[string]*routedSubscription {
	if hasWildcard(filter) {
		return idx.wildcard[filter]
	}
	return idx.exact[filter]
}

// setLive installs the subscriptions of one live connection, replacing whatever
// the session had before. It is called when a connection is registered (which is
// also where a resumed session takes its stored subscriptions over) and it is the
// only transition that can turn a stored session into a live one.
func (idx *subscriptionIndex) setLive(key string, c *client, subs map[string]subscription) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.replaceLocked(&indexedSession{
		key:      key,
		clientID: c.id,
		client:   c,
		connID:   c.connID,
		subs:     copySubscriptions(subs),
	})
}

// setStored installs the subscriptions of a stored (offline) session, replacing
// whatever the session had before. The session is indexed even when it has no
// filters, so a directed publish can find it and queue the publication.
func (idx *subscriptionIndex) setStored(key, clientID string, subs map[string]subscription, expiresAt time.Time) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.replaceLocked(&indexedSession{
		key:       key,
		clientID:  clientID,
		expiresAt: expiresAt,
		subs:      copySubscriptions(subs),
	})
}

// remove drops every entry of a session. It is the transition of a session that
// ended: the store entry is deleted (clean connect, expiry) or taken over, so the
// session is not a local target any more.
func (idx *subscriptionIndex) remove(key string) {
	idx.mu.Lock()
	idx.removeLocked(key)
	idx.mu.Unlock()
}

// add installs one subscription of the session's live connection. A connection the
// index no longer attributes to the session — one that a take-over displaced — is
// ignored: the displaced connection's SUBSCRIBE must not steal the routing of the
// session its replacement now owns.
func (idx *subscriptionIndex) add(key string, c *client, filter string, sub subscription) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	s := idx.sessions[key]
	if s == nil || s.client != c {
		return
	}
	if s.subs == nil {
		s.subs = make(map[string]subscription, 1)
	}
	s.subs[filter] = sub
	idx.putLocked(filter, key, &routedSubscription{session: s, sub: sub})
}

// removeFilter removes one subscription of the session's live connection. Like
// add, it only acts when the connection still owns the session.
func (idx *subscriptionIndex) removeFilter(key string, c *client, filter string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	s := idx.sessions[key]
	if s == nil || s.client != c {
		return
	}
	delete(s.subs, filter)
	idx.deleteLocked(filter, key)
}

// session returns the indexed session for key. The returned object is immutable
// for as long as the caller holds it: a later transition installs a new one.
func (idx *subscriptionIndex) session(key string) (*indexedSession, bool) {
	idx.mu.RLock()
	s, ok := idx.sessions[key]
	idx.mu.RUnlock()
	return s, ok
}

// targets returns one LocalTarget per local session matching topic, with the
// publication's QoS capped by the highest matching subscription QoS and the retain
// flag set only for a retained publication delivered to a subscription with
// retain-as-published. A no-local subscription is skipped when the publication was
// produced by the same connection; a session that also matches through another
// filter is still a target.
//
// Shared subscriptions are not part of this answer: they are groups, and one
// publication goes to exactly one member of each, which sharedTarget decides.
func (idx *subscriptionIndex) targets(topic string, from ConnectionID, pubQoS byte, pubRetain bool) []LocalTarget {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	now := time.Now()
	exact := idx.exact[topic]
	if len(idx.wildcard) == 0 {
		// Fast path: a literal filter is the only way to match a literal topic, so
		// every session in the bucket appears exactly once.
		if len(exact) == 0 {
			return nil
		}
		targets := make([]LocalTarget, 0, len(exact))
		for _, rs := range exact {
			if from != "" && rs.sub.noLocal && rs.session.connID == from {
				continue
			}
			if target, ok := rs.session.localTarget(now, pubQoS, pubRetain, rs.sub.qos, rs.sub.retainAsPublished, rs.sub.subscriptionIDs); ok {
				targets = append(targets, target)
			}
		}
		return targets
	}

	// General path: a session can be reached through the exact filter and through
	// any number of wildcard filters, so the matches are merged per session.
	type match struct {
		session *indexedSession
		qos     byte
		rap     bool
		ids     []uint32
	}
	matched := make(map[string]*match, len(exact)+4)
	merge := func(entries map[string]*routedSubscription) {
		for _, rs := range entries {
			// A stored session has no connection, so no-local cannot name one and
			// never excludes an offline target; a publication without an origin
			// connection (a remote event, a will) excludes nothing either.
			if from != "" && rs.sub.noLocal && rs.session.connID == from {
				continue
			}
			m, ok := matched[rs.session.key]
			if !ok {
				matched[rs.session.key] = &match{session: rs.session, qos: rs.sub.qos, rap: rs.sub.retainAsPublished, ids: rs.sub.subscriptionIDs}
				continue
			}
			if rs.sub.qos > m.qos {
				m.qos = rs.sub.qos
			}
			if rs.sub.retainAsPublished {
				m.rap = true
			}
			m.ids = mergeSubscriptionIDs(m.ids, rs.sub.subscriptionIDs)
		}
	}
	merge(exact)
	for filter, entries := range idx.wildcard {
		if matchTopic(filter, topic) {
			merge(entries)
		}
	}
	if len(matched) == 0 {
		return nil
	}
	targets := make([]LocalTarget, 0, len(matched))
	for _, m := range matched {
		if target, ok := m.session.localTarget(now, pubQoS, pubRetain, m.qos, m.rap, m.ids); ok {
			targets = append(targets, target)
		}
	}
	return targets
}

// sharedGroups returns the keys of the live shared subscription groups whose
// filter matches topic, sorted. A group is live exactly while a live session is a
// member of it, so the answer is also the set of groups this node can deliver.
func (idx *subscriptionIndex) sharedGroups(topic string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if len(idx.shared) == 0 {
		return nil
	}
	out := make([]string, 0, 2)
	for key, group := range idx.shared {
		if group.filter.matches(topic) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// sharedTarget picks the member one publication of a shared group goes to, round
// robin, and reports whether the group has a live member at all. The QoS cap and
// the retain flag are the member's own subscription options, so two members of the
// same group may receive the same publication at different QoS (§4.8.2).
func (idx *subscriptionIndex) sharedTarget(group string, pubQoS byte, pubRetain bool) (LocalTarget, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	g := idx.shared[group]
	if g == nil || len(g.order) == 0 {
		return LocalTarget{}, false
	}
	// The cursor is advanced by the attempt, not by the delivery: a member that
	// disappears between two publications must not make the next one land on the
	// same session twice, and every member is live here, so the loop normally
	// returns on its first iteration.
	now := time.Now()
	for attempt := 0; attempt < len(g.order); attempt++ {
		rs := g.members[g.order[int(g.next%uint64(len(g.order)))]]
		g.next++
		if rs == nil {
			continue
		}
		if target, ok := rs.session.localTarget(now, pubQoS, pubRetain, rs.sub.qos, rs.sub.retainAsPublished, rs.sub.subscriptionIDs); ok {
			return target, true
		}
	}
	return LocalTarget{}, false
}

// localTarget builds the LocalTarget of one matched session, and reports whether
// the session is a target at all: a stored session whose deadline passed is not,
// even when the sweeper has not deleted it yet. It is called with the index lock
// held; the returned value only shares the immutable parts of the indexed session
// with the index.
func (s *indexedSession) localTarget(now time.Time, pubQoS byte, pubRetain bool, maxQoS byte, rap bool, ids []uint32) (LocalTarget, bool) {
	target := LocalTarget{
		ClientID:        s.clientID,
		ConnectionID:    s.connID,
		QoS:             minQoS(pubQoS, maxQoS),
		Retain:          rap && pubRetain,
		client:          s.client,
		subscriptionIDs: ids,
	}
	if s.client == nil {
		if !s.expiresAt.IsZero() && !now.Before(s.expiresAt) {
			return LocalTarget{}, false
		}
		// A stored session has no connection to name; the key is what SendLocal
		// queues through.
		target.ConnectionID = ""
		target.sessionKey = s.key
	}
	return target, true
}

// liveFilters returns the filters subscribed by at least one live session, sorted
// and de-duplicated: that is the routing summary the cluster backends advertise.
// Stored sessions are not part of it, because a stored session is drained by the
// node that holds it, not by a peer. Shared subscriptions are advertised apart
// (liveSharedGroups), because a group is routed to one node, not to every node a
// filter matches.
func (idx *subscriptionIndex) liveFilters() []string {
	idx.mu.RLock()
	out := make([]string, 0, len(idx.live))
	for filter, n := range idx.live {
		if n > 0 {
			out = append(out, filter)
		}
	}
	idx.mu.RUnlock()
	sort.Strings(out)
	return out
}

// liveSharedGroups returns the shared subscription group keys (their full
// "$share/{name}/{filter}" spelling) that have at least one live local member,
// sorted. It is the shared half of the cluster summary: a peer needs to know
// which nodes hold a member of a group to elect one of them per publication,
// while the filter itself is matched by each member's own options.
func (idx *subscriptionIndex) liveSharedGroups() []string {
	idx.mu.RLock()
	out := make([]string, 0, len(idx.shared))
	for group, state := range idx.shared {
		if len(state.members) > 0 {
			out = append(out, group)
		}
	}
	idx.mu.RUnlock()
	sort.Strings(out)
	return out
}

// replaceLocked installs next as the session of its key, updating every filter
// bucket it touches. The old entries are dropped first, not diffed: a shared
// filter must leave its group when the session stops being live even though the
// subscription itself is kept in the new state (a stored session keeps its shared
// subscriptions for the resume, but is not a member of the group while offline).
func (idx *subscriptionIndex) replaceLocked(next *indexedSession) {
	if old := idx.sessions[next.key]; old != nil {
		for filter := range old.subs {
			idx.deleteLocked(filter, next.key)
		}
	}
	idx.sessions[next.key] = next
	for filter, sub := range next.subs {
		idx.putLocked(filter, next.key, &routedSubscription{session: next, sub: sub})
	}
}

// removeLocked drops every entry of a session.
func (idx *subscriptionIndex) removeLocked(key string) {
	old := idx.sessions[key]
	if old == nil {
		return
	}
	for filter := range old.subs {
		idx.deleteLocked(filter, key)
	}
	delete(idx.sessions, key)
}

// putLocked installs one entry, keeping the live-filter counts right. A shared
// filter goes to its group, and only a live session joins a group: a stored
// session's shared subscription is kept in its state (so a resume restores it)
// but is not a member, because a publication is delivered to exactly one live
// member and shared state is not queued.
func (idx *subscriptionIndex) putLocked(filter, key string, entry *routedSubscription) {
	if isSharedSubscription(filter) {
		if entry.session.client == nil {
			return
		}
		idx.putSharedLocked(filter, key, entry)
		return
	}
	bucket := idx.bucket(filter)
	if bucket == nil {
		bucket = make(map[string]*routedSubscription, 1)
		if hasWildcard(filter) {
			idx.wildcard[filter] = bucket
		} else {
			idx.exact[filter] = bucket
		}
	}
	if prev, ok := bucket[key]; ok && prev.session.client != nil {
		idx.live[filter]--
	}
	bucket[key] = entry
	if entry.session.client != nil {
		idx.live[filter]++
	}
	if idx.live[filter] <= 0 {
		delete(idx.live, filter)
	}
}

// deleteLocked drops one entry, keeping the live-filter counts right.
func (idx *subscriptionIndex) deleteLocked(filter, key string) {
	if isSharedSubscription(filter) {
		idx.deleteSharedLocked(filter, key)
		return
	}
	bucket := idx.bucket(filter)
	if bucket == nil {
		return
	}
	if prev, ok := bucket[key]; ok {
		if prev.session.client != nil {
			idx.live[filter]--
			if idx.live[filter] <= 0 {
				delete(idx.live, filter)
			}
		}
		delete(bucket, key)
	}
	if len(bucket) == 0 {
		if hasWildcard(filter) {
			delete(idx.wildcard, filter)
		} else {
			delete(idx.exact, filter)
		}
	}
}

// indexedSubscriptions converts a stored subscription set into the index's form.
func indexedSubscriptions(subs map[string]SessionSubscription) map[string]subscription {
	if len(subs) == 0 {
		return nil
	}
	out := make(map[string]subscription, len(subs))
	for filter, sub := range subs {
		out[filter] = subscription{
			qos:               sub.QoS,
			noLocal:           sub.NoLocal,
			retainAsPublished: sub.RetainAsPublished,
			retainHandling:    sub.RetainHandling,
			subscriptionIDs:   sub.SubscriptionIdentifiers,
		}
	}
	return out
}

// putSharedLocked adds or replaces one member of a shared group, keeping the
// group's sorted member list and its round-robin cursor consistent.
func (idx *subscriptionIndex) putSharedLocked(filter, key string, entry *routedSubscription) {
	group := idx.shared[filter]
	if group == nil {
		parsed, _, ok := parseSharedFilter(filter)
		if !ok {
			return
		}
		group = &sharedGroup{filter: parsed, members: make(map[string]*routedSubscription, 2)}
		idx.shared[filter] = group
	}
	if _, exists := group.members[key]; !exists {
		at := sort.SearchStrings(group.order, key)
		group.order = append(group.order, "")
		copy(group.order[at+1:], group.order[at:])
		group.order[at] = key
	}
	group.members[key] = entry
}

// deleteSharedLocked removes one member of a shared group. The group itself goes
// away with its last member: its round-robin cursor is state of the group, and
// keeping it would leak one entry per group name a client ever used.
func (idx *subscriptionIndex) deleteSharedLocked(filter, key string) {
	group := idx.shared[filter]
	if group == nil {
		return
	}
	if _, ok := group.members[key]; !ok {
		return
	}
	delete(group.members, key)
	if at := sort.SearchStrings(group.order, key); at < len(group.order) && group.order[at] == key {
		group.order = append(group.order[:at], group.order[at+1:]...)
	}
	if len(group.members) == 0 {
		delete(idx.shared, filter)
	}
}
