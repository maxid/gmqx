package gmqx

// Shared-subscription routing
//
// A shared subscription ("$share/{ShareName}/{filter}", MQTT 5 §4.8.2) is a
// group: a publication that matches it is sent to exactly one of its sessions,
// not to all of them. Outside a cluster the routing index answers that on its own
// (subscriptionIndex.sharedTarget picks the member round robin). Across a cluster
// it would be wrong for every node to do that: each node would deliver its own
// copy, so a group whose members live on two nodes would receive the publication
// twice. Routing a shared publication is therefore two decisions, and both are
// taken by the node that produced the publication:
//
//  1. Which node delivers it. Its candidates are the node ids that currently hold
//     a live member of the group: this node when the routing index has one, plus
//     the peers the cluster backend advertises. The advertised subscription
//     summary carries the group keys (sharedDirectory, implemented by the TCP and
//     Redis backends), so no node has to ask another. The elected node is
//
//     candidates[(fnv64(groupKey) + Seat) % len(candidates)]
//
//     over the sorted candidate list. The group hash is the group's affinity:
//     different groups spread over the nodes without any coordination, and two
//     origins publishing to the same group do not have to agree. Seat is a
//     per-group counter of this origin (sharedRotation), so successive publications
//     of one group visit every candidate instead of pinning the group to one node
//     while it lives. The election needs no shared state and no round trip — each
//     node decides alone for the publications it produces, which is what makes it
//     correct for both backends.
//
//  2. Which member of the elected node. That node picks round robin among its own
//     live members (subscriptionIndex.sharedTarget), so two members of one group
//     on the same node alternate. A peer learns which groups it must deliver from
//     ClusterMessage.Shared: an entry names one node and the groups that node is
//     responsible for, and a peer applies only the entries that name it (see
//     Broker.OnRemotePublish).
//
// Consequences, deliberately chosen and documented:
//
//   - A publication is delivered at most once per group, cluster-wide. A node that
//     is not named delivers nothing for the group even though it may hold members.
//   - The elected node may have lost its last member between the advertisement and
//     the arrival (a disconnect, or a summary one refresh old). It then drops the
//     publication and counts it (Metrics.RecordDroppedMessage("shared_no_member")).
//     It is never handed to another node's member: delivery is at-most-once, and
//     re-routing could duplicate a copy the elected node has already delivered.
//   - A group with no live member anywhere has no candidate at all: the origin
//     drops the publication and counts the same reason.
//   - A backend that does not advertise shared groups (any backend outside this
//     package: sharedDirectory is not part of the Cluster interface) keeps shared
//     subscriptions local to the node that produced the publication. Advertising
//     the groups is what opts a backend into cluster-wide shared subscriptions.
//   - A publication is only delivered to a live member. Shared state is never
//     queued for an offline persistent session, which keeps a group's messages
//     from being drained twice on a resume (the subscription itself survives in
//     the stored session and rejoins the group when the session does).

import (
	"context"
	"hash/fnv"
	"sort"
)

// sharedDirectory is implemented by a cluster backend that advertises the shared
// subscription groups its nodes hold. The broker asks it once per publication that
// could match a shared group and elects one node per group from the answer.
//
// The returned map is group key ("$share/{ShareName}/{filter}") to the peer node
// ids that have at least one live member of it; this node is never in it. A
// backend builds it from the same subscription summary it already advertises to
// route normal publishes, so the cost is a pass over the peers, not a round trip.
type sharedDirectory interface {
	sharedGroups() map[string][]string
}

// dispatchShared routes one publication through the shared subscription groups it
// matches, cluster-wide: it delivers the groups this node was elected for and
// returns the groups a peer must deliver, one entry per node, sorted. It is called
// from Broker.SpreadPublication, so a publication that matches no shared group
// costs one read-locked lookup in the routing index and one pass over the peer view
// (both O(1) when there are no groups).
func (b *Broker) dispatchShared(ctx context.Context, pub Publication) []SharedDelivery {
	if pub.Namespace != b.namespace {
		// Same rule as LocalTargets: a publication of another namespace is not
		// this broker's to route, and a shared group is a local session set.
		return nil
	}
	local := b.index.sharedGroups(pub.Topic)
	var peers map[string][]string
	if b.sharedNodes != nil {
		peers = b.sharedNodes.sharedGroups()
	}
	if len(local) == 0 && len(peers) == 0 {
		return nil
	}

	// The groups to decide: every live local group that matches, and every group a
	// peer advertises whose filter matches the publication. The peer view is the
	// only way this node can learn about a group whose members live elsewhere.
	localSet := make(map[string]struct{}, len(local))
	groups := make([]string, 0, len(local)+len(peers))
	for _, group := range local {
		localSet[group] = struct{}{}
		groups = append(groups, group)
	}
	for group := range peers {
		if _, isLocal := localSet[group]; isLocal {
			continue
		}
		parsed, isShared, ok := parseSharedFilter(group)
		if !isShared || !ok || !parsed.matches(pub.Topic) {
			continue
		}
		groups = append(groups, group)
	}
	sort.Strings(groups)

	var remote []SharedDelivery
	for _, group := range groups {
		candidates := append([]string(nil), peers[group]...)
		if _, isLocal := localSet[group]; isLocal {
			candidates = append(candidates, b.nodeID)
		}
		candidates = uniqueSorted(candidates)
		if len(candidates) == 0 {
			// No node holds a live member: the group exists nowhere by the time the
			// publication is routed. Dropped, and counted, rather than guessed at.
			b.metrics.RecordDroppedMessage("shared_no_member")
			b.log.Debug("gmqx shared publication dropped", "group", group, "topic", pub.Topic, "reason", "no_member")
			continue
		}
		seat := (groupHash(group) + b.nextSharedSeat(group)) % uint64(len(candidates))
		if chosen := candidates[seat]; chosen != b.nodeID {
			remote = append(remote, SharedDelivery{Node: chosen, Groups: []string{group}})
			continue
		}
		b.deliverSharedGroup(ctx, group, pub)
	}
	return mergeSharedDeliveries(remote)
}

// deliverSharedGroup delivers one publication to the member a shared group picks
// on this node, and counts the drop when the group has no live member left. It is
// the only place a shared publication is written, locally or from a peer event.
func (b *Broker) deliverSharedGroup(ctx context.Context, group string, pub Publication) bool {
	target, ok := b.index.sharedTarget(group, pub.QoS, pub.Retain)
	if !ok {
		b.metrics.RecordDroppedMessage("shared_no_member")
		b.log.Debug("gmqx shared publication dropped", "group", group, "topic", pub.Topic, "reason", "no_live_member")
		return false
	}
	_ = b.SendLocal(ctx, target, pub)
	return true
}

// maxSharedRotationEntries bounds the per-group election counters. Past it the
// table is reset: the group hash still spreads the groups, so the only cost is that
// a group's rotation restarts from its affinity, never a correctness problem.
const maxSharedRotationEntries = 1024

// nextSharedSeat advances the rotation of one group and returns the value to elect
// with. The table is per broker and per group, so the counter of a group is not
// disturbed by the publications of another one (a single counter shared by all
// groups would keep the parity of a group constant whenever two groups match the
// same publication).
func (b *Broker) nextSharedSeat(group string) uint64 {
	b.sharedMu.Lock()
	defer b.sharedMu.Unlock()
	if b.sharedRotation == nil {
		b.sharedRotation = make(map[string]uint64, 8)
	}
	if len(b.sharedRotation) >= maxSharedRotationEntries {
		clear(b.sharedRotation)
	}
	seat := b.sharedRotation[group]
	b.sharedRotation[group] = seat + 1
	return seat
}

// groupHash is the affinity of a shared group: FNV-1a over its key. It is stable
// across processes and restarts, so two nodes that elect for the same group start
// from the same candidate.
func groupHash(group string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(group))
	return h.Sum64()
}

// uniqueSorted copies ids, sorts the copy and removes duplicates.
func uniqueSorted(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	out := append([]string(nil), ids...)
	sort.Strings(out)
	kept := out[:1]
	for _, id := range out[1:] {
		if id != kept[len(kept)-1] {
			kept = append(kept, id)
		}
	}
	return kept
}

// mergeSharedDeliveries folds the per-group entries into one per node, sorted by
// node id and with the group list sorted, so the event a peer receives does not
// depend on map iteration.
func mergeSharedDeliveries(in []SharedDelivery) []SharedDelivery {
	if len(in) == 0 {
		return nil
	}
	byNode := make(map[string][]string, len(in))
	for _, d := range in {
		byNode[d.Node] = append(byNode[d.Node], d.Groups...)
	}
	out := make([]SharedDelivery, 0, len(byNode))
	for node, groups := range byNode {
		sort.Strings(groups)
		out = append(out, SharedDelivery{Node: node, Groups: groups})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Node < out[j].Node })
	return out
}

// sharedGroupsFor returns the groups one node must deliver out of a cluster
// event's shared entries.
func (b *Broker) sharedGroupsFor(shared []SharedDelivery) []string {
	var out []string
	for _, d := range shared {
		if d.Node != b.nodeID {
			continue
		}
		out = append(out, d.Groups...)
	}
	return out
}
