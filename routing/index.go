package routing

// Routing index
//
// The index is the broker's answer to "which local sessions does this publication
// match", and it replaces the scan the broker used to run on every publish (every
// live connection, every stored session, and a store Keys + Load round trip
// for each of them, all under the broker's global lock).
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
//     the cluster summary advertises without touching a single connection. Shared
//     groups are advertised separately (LiveSharedGroups): a peer needs to know
//     which node holds a member of one, not which filter a normal subscriber uses.
//
// The index holds an Endpoint, never the broker's connection type: a live session
// is reachable through the three methods of gmqx/routing.Endpoint (its client id,
// its connection id and a delivery handle) and nothing else. That is what lets the
// table live here while the connection state machine stays in the engine, and it
// is also what the displacement rule needs: two Endpoints compare by identity, so
// a displaced connection's SUBSCRIBE can be recognised and ignored.
//
// An indexedSession is immutable once installed: a transition (connect,
// disconnect, session save, session take, expiry) installs a new object, so a
// caller that reads the fields of a returned session never races with the session
// changing hands. Only the `subs` map of the session currently installed is
// mutated, and only under the index lock.
//
// Consistency guarantee. Every subscription change updates the index before the
// packet that reports it leaves the broker: the entry of a SUBSCRIBE is routable
// before its SUBACK is written, the removal of an UNSUBSCRIBE before its
// UNSUBACK, and the subscriptions a resumed session restores before its CONNACK.
// A session that goes away is converted (a persistent session becomes a queued,
// offline target) or removed under the same broker lock section that moves it
// between the connection table and the store, so a publication observes either the
// live entry or the stored entry of a session, never both: a message cannot be
// delivered twice because a session reconnected while it was being routed.
//
// Lock order: the broker's session lock before Index.mu. Nothing takes that lock
// while holding the index, so routing never nests the two: Targets takes only the
// index, and the broker's SendLocal takes its own lock afterwards.

import (
	"sort"
	"strings"
	"sync"
	"time"

	mqtt5 "github.com/maxid/gmqx/internal/mqtt5"
	topicmatch "github.com/maxid/gmqx/internal/topic"
	sessionstate "github.com/maxid/gmqx/session"
)

// Index is the broker's routing table. See the file comment for the
// structure and the consistency guarantee.
type Index struct {
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
	filter topicmatch.SharedFilter
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
// live connection owns it (endpoint != nil) or it is a stored session with no
// connection (endpoint == nil). The fields are written once, when the object is
// installed.
type indexedSession struct {
	key      string
	clientID string
	endpoint Endpoint
	connID   ConnectionID
	// expiresAt is the deadline of a stored session (the zero time for a live one
	// and for a session that never expires). A session whose deadline passed is not
	// a target even before the sweeper deleted it, exactly as the scan it replaced
	// decided.
	expiresAt time.Time
	// subs is the session's subscription set. It is only read and written while
	// the index lock is held.
	subs map[string]sessionstate.Subscription
	// queued is how many messages the stored session's offline queue holds. The
	// broker keeps the global offline-queue counter with it: the store may evict an
	// expired value before the broker deletes it, so the index is the only copy of
	// that number that is still reliable at sweep time.
	queued int
}

// routedSubscription is one (filter, session) entry: the subscription that makes
// the session a target of a publication matching filter.
type routedSubscription struct {
	session *indexedSession
	sub     sessionstate.Subscription
}

// NewIndex returns an empty routing table. Its zero value is not usable: the
// buckets and the session map are created here.
func NewIndex() *Index {
	return &Index{
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
func (idx *Index) bucket(filter string) map[string]*routedSubscription {
	if hasWildcard(filter) {
		return idx.wildcard[filter]
	}
	return idx.exact[filter]
}

// SetLive installs the subscriptions of one live connection, replacing whatever
// the session had before. It is called when a connection is registered (which is
// also where a resumed session takes its stored subscriptions over) and it is the
// only transition that can turn a stored session into a live one.
func (idx *Index) SetLive(key string, e Endpoint, subs map[string]sessionstate.Subscription) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.replaceLocked(&indexedSession{
		key:      key,
		clientID: e.ClientID(),
		endpoint: e,
		connID:   e.ConnectionID(),
		subs:     sessionstate.CopySubscriptions(subs),
	})
}

// SetStored installs the subscriptions of a stored (offline) session, replacing
// whatever the session had before. The session is indexed even when it has no
// filters, so a directed publish can find it and queue the publication.
//
// It returns how many queued messages the entry it replaced held, so the caller
// keeps its global offline-queue counter right when it stores a session twice.
func (idx *Index) SetStored(key, clientID string, subs map[string]sessionstate.Subscription, expiresAt time.Time, queued int) int {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	prev := 0
	if old := idx.sessions[key]; old != nil && old.endpoint == nil {
		prev = old.queued
	}
	idx.replaceLocked(&indexedSession{
		key:       key,
		clientID:  clientID,
		expiresAt: expiresAt,
		subs:      sessionstate.CopySubscriptions(subs),
		queued:    queued,
	})
	return prev
}

// SetQueued records how many messages the stored session under key now holds. It
// is what the offline write path updates after it appends to the queue, so the
// index's copy of the count (and the broker's global counter) stay right even when
// the store later evicts the value.
func (idx *Index) SetQueued(key string, queued int) {
	idx.mu.Lock()
	if s := idx.sessions[key]; s != nil && s.endpoint == nil {
		s.queued = queued
	}
	idx.mu.Unlock()
}

// Remove drops every entry of a session and returns how many queued messages its
// stored form held (zero for a live or unknown session). It is the transition of a
// session that ended: the store entry is deleted (clean connect, expiry) or taken
// over, so the session is not a local target any more.
func (idx *Index) Remove(key string) int {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	queued := 0
	if s := idx.sessions[key]; s != nil && s.endpoint == nil {
		queued = s.queued
	}
	idx.removeLocked(key)
	return queued
}

// Add installs one subscription of the session's live connection. A connection the
// index no longer attributes to the session — one that a take-over displaced — is
// ignored: the displaced connection's SUBSCRIBE must not steal the routing of the
// session its replacement now owns.
//
// It reports whether the subscription was installed.
func (idx *Index) Add(key string, e Endpoint, filter string, sub sessionstate.Subscription) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	s := idx.sessions[key]
	if s == nil || s.endpoint != e {
		return false
	}
	if s.subs == nil {
		s.subs = make(map[string]sessionstate.Subscription, 1)
	}
	s.subs[filter] = sub
	idx.putLocked(filter, key, &routedSubscription{session: s, sub: sub})
	return true
}

// RemoveFilter removes one subscription of the session's live connection. Like
// Add, it only acts when the connection still owns the session.
//
// It reports whether the subscription was removed.
func (idx *Index) RemoveFilter(key string, e Endpoint, filter string) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	s := idx.sessions[key]
	if s == nil || s.endpoint != e {
		return false
	}
	delete(s.subs, filter)
	idx.deleteLocked(filter, key)
	return true
}

// Has reports whether key is an indexed session at all, live or stored. It is the
// "is this client id local" question a directed publish asks before it hands the
// publication to the cluster.
func (idx *Index) Has(key string) bool {
	idx.mu.RLock()
	_, ok := idx.sessions[key]
	idx.mu.RUnlock()
	return ok
}

// Target returns the delivery target of one indexed session under key, live or
// stored. It is the directed-publish side of the index: the caller already knows
// the client id is local (Has), so the session is returned even when its expiry
// deadline has passed — the offline queue path re-checks the deadline and counts
// the drop, which is a different answer from "not local".
func (idx *Index) Target(key string, pubQoS byte, pubRetain bool) (Target, bool) {
	idx.mu.RLock()
	s, ok := idx.sessions[key]
	idx.mu.RUnlock()
	if !ok {
		return Target{}, false
	}
	target := Target{
		ClientID:     s.clientID,
		ConnectionID: s.connID,
		QoS:          pubQoS,
		Retain:       pubRetain,
		endpoint:     s.endpoint,
	}
	if s.endpoint == nil {
		// An offline session has no connection to name: SendLocal queues through
		// the session key, and its re-check makes a session that reconnected in the
		// meantime receive the publication live.
		target.ConnectionID = ""
		target.sessionKey = s.key
	}
	return target, true
}

// Len is the number of indexed sessions, live and stored.
func (idx *Index) Len() int {
	idx.mu.RLock()
	n := len(idx.sessions)
	idx.mu.RUnlock()
	return n
}

// EntryCount is the number of (filter, session) entries in the normal-subscription
// buckets. A session subscribed to two filters counts twice; shared groups are
// counted separately (SharedGroupCount), because they are not entries of a normal
// bucket.
func (idx *Index) EntryCount() int {
	idx.mu.RLock()
	n := 0
	for _, bucket := range idx.exact {
		n += len(bucket)
	}
	for _, bucket := range idx.wildcard {
		n += len(bucket)
	}
	idx.mu.RUnlock()
	return n
}

// SharedGroupCount is the number of live shared subscription groups.
func (idx *Index) SharedGroupCount() int {
	idx.mu.RLock()
	n := len(idx.shared)
	idx.mu.RUnlock()
	return n
}

// Targets returns one Target per local session matching topic. It is the slice
// form of the answer, for callers that need to keep or count the targets (the
// broker's LocalTargets seam); the broker's own dispatcher uses EachTarget so a
// publication does not allocate a slice.
func (idx *Index) Targets(topic string, from ConnectionID, pubQoS byte, pubRetain bool) []Target {
	return idx.collectTargets(topic, from, pubQoS, pubRetain, nil)
}

// EachTarget hands every session whose subscriptions match topic to fn, with the
// per-session QoS cap and retain flag already applied and no-local subscriptions
// of the publishing connection excluded.
//
// The targets are collected under the index lock and then handed to fn with the
// lock released: fn delivers a publication, and a delivery can close a
// connection, which unregisters its session and takes the index for writing — a
// callback under the lock would deadlock against exactly that teardown.
//
// The common case of a handful of matching sessions is collected into a stack
// array, so the broker's own dispatcher does not allocate a target slice per
// publication.
func (idx *Index) EachTarget(topic string, from ConnectionID, pubQoS byte, pubRetain bool, fn func(Target)) {
	var local [4]Target
	targets := idx.collectTargets(topic, from, pubQoS, pubRetain, local[:0])
	for i := range targets {
		fn(targets[i])
	}
}

// collectTargets appends the sessions whose subscriptions match topic to dst and
// returns the result. The caller owns dst and may pass a stack array; only a
// match set larger than its capacity allocates.
//
// One Target per local session matching topic: its QoS is capped by the
// highest matching subscription QoS and its retain flag is set only for a
// retained publication delivered to a subscription with retain-as-published. A
// no-local subscription is skipped when the publication was produced by the same
// connection; a session that also matches through another filter is still a
// target. Shared subscriptions are not part of this answer: they are groups, and
// one publication goes to exactly one member of each, which SharedTarget decides.
//
// The fast path — a literal topic and no wildcard filter in the index — is one
// map lookup plus one target per session. When wildcard filters exist they are
// matched one by one; a topic that matches exactly one of them (the normal case
// for a wildcard space that is sharded per subscriber, which is how the
// throughput harness of this repository publishes) is delivered without the
// merge map a multi-filter match needs.
func (idx *Index) collectTargets(topic string, from ConnectionID, pubQoS byte, pubRetain bool, dst []Target) []Target {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	now := time.Now()
	exact := idx.exact[topic]
	if len(idx.wildcard) == 0 {
		// Fast path: a literal filter is the only way to match a literal topic, so
		// every session in the bucket appears exactly once.
		return idx.appendEntries(dst, now, exact, from, pubQoS, pubRetain)
	}

	// Which buckets match at all. A session can be reached through the exact
	// filter and through any number of wildcard filters, so more than one matching
	// bucket has to be merged per session; the common single-bucket case does not.
	var local [2]map[string]*routedSubscription
	matches := local[:0]
	if len(exact) > 0 {
		matches = append(matches, exact)
	}
	for filter, entries := range idx.wildcard {
		if topicmatch.Match(filter, topic) {
			matches = append(matches, entries)
		}
	}
	if len(matches) == 0 {
		return dst
	}
	if len(matches) == 1 {
		return idx.appendEntries(dst, now, matches[0], from, pubQoS, pubRetain)
	}

	type match struct {
		session *indexedSession
		qos     byte
		rap     bool
		ids     []uint32
	}
	matched := make(map[string]*match, len(exact)+4)
	for _, entries := range matches {
		for _, rs := range entries {
			// A stored session has no connection, so no-local cannot name one and
			// never excludes an offline target; a publication without an origin
			// connection (a remote event, a will) excludes nothing either.
			if from != "" && rs.sub.NoLocal && rs.session.connID == from {
				continue
			}
			m, ok := matched[rs.session.key]
			if !ok {
				matched[rs.session.key] = &match{session: rs.session, qos: rs.sub.QoS, rap: rs.sub.RetainAsPublished, ids: rs.sub.SubscriptionIDs}
				continue
			}
			if rs.sub.QoS > m.qos {
				m.qos = rs.sub.QoS
			}
			if rs.sub.RetainAsPublished {
				m.rap = true
			}
			m.ids = mqtt5.MergeSubscriptionIDs(m.ids, rs.sub.SubscriptionIDs)
		}
	}
	for _, m := range matched {
		if target, ok := m.session.localTarget(now, pubQoS, pubRetain, m.qos, m.rap, m.ids); ok {
			dst = append(dst, target)
		}
	}
	return dst
}

// appendEntries appends one bucket of the index, whose sessions all match the
// same topic, to dst. The caller holds idx.mu for reading.
func (idx *Index) appendEntries(dst []Target, now time.Time, entries map[string]*routedSubscription, from ConnectionID, pubQoS byte, pubRetain bool) []Target {
	for _, rs := range entries {
		if from != "" && rs.sub.NoLocal && rs.session.connID == from {
			continue
		}
		if target, ok := rs.session.localTarget(now, pubQoS, pubRetain, rs.sub.QoS, rs.sub.RetainAsPublished, rs.sub.SubscriptionIDs); ok {
			dst = append(dst, target)
		}
	}
	return dst
}

// SharedGroups returns the keys of the live shared subscription groups whose
// filter matches topic, sorted. A group is live exactly while a live session is a
// member of it, so the answer is also the set of groups this node can deliver.
func (idx *Index) SharedGroups(topic string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if len(idx.shared) == 0 {
		return nil
	}
	out := make([]string, 0, 2)
	for key, group := range idx.shared {
		if group.filter.Matches(topic) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// SharedTarget picks the member one publication of a shared group goes to, round
// robin, and reports whether the group has a live member at all. The QoS cap and
// the retain flag are the member's own subscription options, so two members of the
// same group may receive the same publication at different QoS (§4.8.2).
func (idx *Index) SharedTarget(group string, pubQoS byte, pubRetain bool) (Target, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	g := idx.shared[group]
	if g == nil || len(g.order) == 0 {
		return Target{}, false
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
		if target, ok := rs.session.localTarget(now, pubQoS, pubRetain, rs.sub.QoS, rs.sub.RetainAsPublished, rs.sub.SubscriptionIDs); ok {
			return target, true
		}
	}
	return Target{}, false
}

// localTarget builds the Target of one matched session, and reports whether the
// session is a target at all: a stored session whose deadline passed is not, even
// when the sweeper has not deleted it yet. It is called with the index lock held;
// the returned value only shares the immutable parts of the indexed session with
// the index.
func (s *indexedSession) localTarget(now time.Time, pubQoS byte, pubRetain bool, maxQoS byte, rap bool, ids []uint32) (Target, bool) {
	target := Target{
		ClientID:        s.clientID,
		ConnectionID:    s.connID,
		QoS:             minQoS(pubQoS, maxQoS),
		Retain:          rap && pubRetain,
		endpoint:        s.endpoint,
		subscriptionIDs: ids,
	}
	if s.endpoint == nil {
		if !s.expiresAt.IsZero() && !now.Before(s.expiresAt) {
			return Target{}, false
		}
		// A stored session has no connection to name; the key is what SendLocal
		// queues through.
		target.ConnectionID = ""
		target.sessionKey = s.key
	}
	return target, true
}

// Deadline returns the expiry deadline of the stored session under key, and false
// when the index does not hold it as a stored session. It is what lets the sweeper
// re-check one candidate after taking the broker lock.
func (idx *Index) Deadline(key string) (deadline time.Time, queued int, ok bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	s, ok := idx.sessions[key]
	if !ok || s.endpoint != nil {
		return time.Time{}, 0, false
	}
	return s.expiresAt, s.queued, true
}

// ExpiredSession is one stored session whose deadline passed.
type ExpiredSession struct {
	// Key is the session key the index holds it under.
	Key string
	// ClientID is the MQTT client id of the session, for the presence record the
	// sweeper has to forget.
	ClientID string
}

// Expired returns the stored sessions whose deadline passed by now. It is what the
// expiry sweeper walks: the index holds every stored session together with its
// deadline, so the sweep does not depend on the store being enumerable.
func (idx *Index) Expired(now time.Time) []ExpiredSession {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var out []ExpiredSession
	for key, s := range idx.sessions {
		if s.endpoint != nil || s.expiresAt.IsZero() || now.Before(s.expiresAt) {
			continue
		}
		out = append(out, ExpiredSession{Key: key, ClientID: s.clientID})
	}
	return out
}

// LiveFilters returns the filters subscribed by at least one live session, sorted
// and de-duplicated: that is the routing summary the cluster backends advertise.
// Stored sessions are not part of it, because a stored session is drained by the
// node that holds it, not by a peer. Shared subscriptions are advertised apart
// (LiveSharedGroups), because a group is routed to one node, not to every node a
// filter matches.
func (idx *Index) LiveFilters() []string {
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

// LiveSharedGroups returns the shared subscription group keys (their full
// "$share/{name}/{filter}" spelling) that have at least one live local member,
// sorted. It is the shared half of the cluster summary: a peer needs to know
// which nodes hold a member of a group to elect one of them per publication,
// while the filter itself is matched by each member's own options.
func (idx *Index) LiveSharedGroups() []string {
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
func (idx *Index) replaceLocked(next *indexedSession) {
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
func (idx *Index) removeLocked(key string) {
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
func (idx *Index) putLocked(filter, key string, entry *routedSubscription) {
	if topicmatch.IsSharedSubscription(filter) {
		if entry.session.endpoint == nil {
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
	if prev, ok := bucket[key]; ok && prev.session.endpoint != nil {
		idx.live[filter]--
	}
	bucket[key] = entry
	if entry.session.endpoint != nil {
		idx.live[filter]++
	}
	if idx.live[filter] <= 0 {
		delete(idx.live, filter)
	}
}

// deleteLocked drops one entry, keeping the live-filter counts right.
func (idx *Index) deleteLocked(filter, key string) {
	if topicmatch.IsSharedSubscription(filter) {
		idx.deleteSharedLocked(filter, key)
		return
	}
	bucket := idx.bucket(filter)
	if bucket == nil {
		return
	}
	if prev, ok := bucket[key]; ok {
		if prev.session.endpoint != nil {
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

// putSharedLocked adds or replaces one member of a shared group, keeping the
// group's sorted member list and its round-robin cursor consistent.
func (idx *Index) putSharedLocked(filter, key string, entry *routedSubscription) {
	group := idx.shared[filter]
	if group == nil {
		parsed, _, ok := topicmatch.ParseSharedFilter(filter)
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
func (idx *Index) deleteSharedLocked(filter, key string) {
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

// minQoS is the smaller of two QoS levels.
func minQoS(a, b byte) byte {
	if a < b {
		return a
	}
	return b
}
