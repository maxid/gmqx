package engine

import (
	"context"
	"sort"

	"github.com/maxid/gmqx/cluster"
	topicmatch "github.com/maxid/gmqx/internal/topic"
)

// The cluster contract lives in gmqx/cluster (docs/design/source-layout.md §2, rule
// L3): the root package re-exports every type and constant as an alias, so an import
// of either path names the same type and existing code needs no change. The
// implementation and its configuration types live there too; this file is the facade
// the broker and existing consumers keep importing.

type (
	// ClusterEventType identifies a cross-node cluster event.
	ClusterEventType = cluster.ClusterEventType
	// ClusterMessage is a cross-node cluster event.
	ClusterMessage = cluster.ClusterMessage
	// ClusterHandler receives the remote events every backend delivers.
	ClusterHandler = cluster.ClusterHandler
	// Cluster is a pluggable cluster backend.
	Cluster = cluster.Cluster
	// SharedDelivery is the shared-subscription half of one cluster publish.
	SharedDelivery = cluster.SharedDelivery
	// MemberState is the liveness of one node in a member view.
	MemberState = cluster.MemberState
	// Member is one node in a cluster backend's member view.
	Member = cluster.Member
	// MemberView is implemented by backends that expose their member view.
	MemberView = cluster.MemberView
	// ClusterStats is a snapshot of one backend's counters.
	ClusterStats = cluster.ClusterStats
	// StatsView is implemented by backends that expose their counters.
	StatsView = cluster.StatsView
	// Link is one transport connection of a cluster node.
	Link = cluster.Link
	// LinkView is implemented by backends that expose their transport links.
	LinkView = cluster.LinkView
	// PresenceView is implemented by backends that track client-id presence.
	PresenceView = cluster.PresenceView
	// Routed is one peer as a backend's routing view holds it.
	Routed = cluster.Routed
	// RoutingView is implemented by backends that route through a cached view.
	RoutingView = cluster.RoutingView

	// GossipClusterConfig configures the built-in gossip cluster backend.
	GossipClusterConfig = cluster.GossipClusterConfig
	// TCPClusterConfig configures the built-in TCP mesh cluster backend.
	TCPClusterConfig = cluster.TCPClusterConfig
	// RedisClusterConfig configures the built-in Redis cluster backend.
	RedisClusterConfig = cluster.RedisClusterConfig
	// SubscriptionSummary is the routing metadata one gossip node advertises.
	SubscriptionSummary = cluster.SubscriptionSummary
)

const (
	ClusterPublish = cluster.ClusterPublish
	ClusterKick    = cluster.ClusterKick
	ClusterTarget  = cluster.ClusterTarget
)

const (
	MemberAlive   = cluster.MemberAlive
	MemberSuspect = cluster.MemberSuspect
	MemberDead    = cluster.MemberDead
)

// NewGossipCluster builds the gossip backend from cfg. It is a forwarder: the
// backend lives in gmqx/cluster, and the root name stays so existing code compiles
// unchanged (rule L3).
func NewGossipCluster(cfg GossipClusterConfig) Cluster { return cluster.NewGossipCluster(cfg) }

// NewTCPCluster builds the TCP mesh backend from cfg.
func NewTCPCluster(cfg TCPClusterConfig) Cluster { return cluster.NewTCPCluster(cfg) }

// NewRedisCluster builds the Redis backend from cfg.
func NewRedisCluster(cfg RedisClusterConfig) Cluster { return cluster.NewRedisCluster(cfg) }

// backendHandler is the ClusterHandler the broker hands a backend at Start. It
// forwards the event sinks to the broker and adds the exported method names the
// backends of gmqx/cluster type-assert (cluster.SubscriptionProvider and
// cluster.SharedSubscriptionProvider). It exists as an adapter instead of growing
// those methods on Broker: the remote-event hooks are the broker's public surface,
// while the two summary halves are a collaboration detail only a backend reads.
type backendHandler struct{ b *Broker }

func (h backendHandler) OnRemotePublish(msg ClusterMessage) { h.b.OnRemotePublish(msg) }
func (h backendHandler) OnRemoteKick(msg ClusterMessage)    { h.b.OnRemoteKick(msg) }
func (h backendHandler) OnRemoteTarget(msg ClusterMessage)  { h.b.OnRemoteTarget(msg) }

// SubscriptionFilters implements cluster.SubscriptionProvider.
func (h backendHandler) SubscriptionFilters() []string { return h.b.subscriptionFilters() }

// SharedSubscriptionGroups implements cluster.SharedSubscriptionProvider.
func (h backendHandler) SharedSubscriptionGroups() []string { return h.b.sharedSubscriptionGroups() }

// The collaboration seams below are implemented by a cluster backend and called by
// the broker. The interfaces stay in the root package because they are what the
// broker asks for; their methods are exported because the backend that implements
// them lives in gmqx/cluster. A backend that does not implement one is simply not
// given that job.

// namespaceScoped is implemented by cluster backends that scope their traffic to
// one broker namespace. The broker calls SetNamespace with its Config.Namespace
// before Cluster.Start, so the broker's namespace is authoritative for a backend
// attached to it. A backend that does not implement it is not scoped by the
// broker: it still receives ClusterMessage.Namespace and may filter on it.
type namespaceScoped interface {
	SetNamespace(namespace string)
}

// subscriptionChangeNotifier is implemented by cluster backends that want to
// push a fresh subscription summary as soon as local subscriptions change
// instead of waiting for the next periodic refresh. The broker calls it after a
// successful SUBSCRIBE/UNSUBSCRIBE and after a session with subscriptions goes
// away.
type subscriptionChangeNotifier interface {
	SubscriptionsChanged()
}

// clientPresence is implemented by a cluster backend that tracks which node holds
// a client id, so a directed publish can be sent to that node instead of to every
// node. The broker tells the backend about every admitted CONNECT and every
// connection that leaves without a replacement, outside the broker's lock: an
// implementation must not block, and must tolerate a lost or late notification
// (presence is a TTL'd hint, refreshed by the backend, not a source of truth).
type clientPresence interface {
	ClientConnected(clientID string)
	ClientDisconnected(clientID string)
}

// Shared-subscription routing
//
// A shared subscription ("$share/{ShareName}/{filter}", MQTT 5 §4.8.2) is a
// group: a publication that matches it is sent to exactly one of its sessions,
// not to all of them. Outside a cluster the routing index answers that on its own
// (routing.Index.SharedTarget picks the member round robin). Across a cluster
// it would be wrong for every node to do that: each node would deliver its own
// copy, so a group whose members live on two nodes would receive the publication
// twice. Routing a shared publication is therefore two decisions, and both are
// taken by the node that produced the publication:
//
//  1. Which node delivers it. Its candidates are the node ids that currently hold
//     a live member of the group: this node when the routing index has one, plus
//     the peers the cluster backend advertises. The advertised subscription
//     summary carries the group keys (sharedDirectory, implemented by the TCP and
//     Redis backends), so no node has to ask another. The election itself is
//     routing.Election: candidates[(fnv64(groupKey) + Seat) % len(candidates)]
//     over the sorted candidate list, with the group hash as the group's affinity
//     and a per-group counter as the seat, so successive publications of one group
//     visit every candidate instead of pinning the group to one node while it
//     lives. It needs no shared state and no round trip — each node decides alone
//     for the publications it produces, which is what makes it correct for both
//     backends.
//
//  2. Which member of the elected node. That node picks round robin among its own
//     live members (routing.Index.SharedTarget), so two members of one group
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

// sharedDirectory is implemented by a cluster backend that advertises the shared
// subscription groups its nodes hold. The broker asks it once per publication that
// could match a shared group and elects one node per group from the answer. The
// method is exported because the backends live in gmqx/cluster; the interface stays
// here because it is what the broker asks for.
//
// The returned map is group key ("$share/{ShareName}/{filter}") to the peer node
// ids that have at least one live member of it; this node is never in it. A
// backend builds it from the same subscription summary it already advertises to
// route normal publishes, so the cost is a pass over the peers, not a round trip.
type sharedDirectory interface {
	SharedGroups() map[string][]string
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
	local := b.index.SharedGroups(pub.Topic)
	var peers map[string][]string
	if b.sharedNodes != nil {
		peers = b.sharedNodes.SharedGroups()
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
		parsed, isShared, ok := topicmatch.ParseSharedFilter(group)
		if !isShared || !ok || !parsed.Matches(pub.Topic) {
			continue
		}
		groups = append(groups, group)
	}
	sort.Strings(groups)

	var remote []SharedDelivery
	for _, group := range groups {
		candidates := peers[group]
		if _, isLocal := localSet[group]; isLocal {
			// A nil append would replace the peer slice; copy before adding this node
			// so the backend's view is never written to.
			candidates = append(append([]string(nil), candidates...), b.nodeID)
		}
		chosen, ok := b.sharedElection.Choose(group, candidates)
		if !ok {
			// No node holds a live member: the group exists nowhere by the time the
			// publication is routed. Dropped, and counted, rather than guessed at.
			b.metrics().RecordDroppedMessage("shared_no_member")
			b.log.Debug("gmqx shared publication dropped", "group", group, "topic", pub.Topic, "reason", "no_member")
			continue
		}
		if chosen != b.nodeID {
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
	target, ok := b.index.SharedTarget(group, pub.QoS, pub.Retain)
	if !ok {
		b.metrics().RecordDroppedMessage("shared_no_member")
		b.log.Debug("gmqx shared publication dropped", "group", group, "topic", pub.Topic, "reason", "no_live_member")
		return false
	}
	_ = b.SendLocal(ctx, target, pub)
	return true
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
