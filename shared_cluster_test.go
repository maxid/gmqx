package gmqx

// Cluster-side shared subscription tests: a shared group must be distributed
// across nodes, not per node. The node that produces a publication elects exactly
// one node per group out of the nodes that advertise a live member (see shared.go),
// the elected node picks the member, and a node that is not elected delivers
// nothing for that group. The three cases below pin that, plus the advertisement
// each built-in backend has to contribute for the election to be possible.

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// sharedTestCluster is an in-memory cluster backend that behaves like the built-in
// ones for the two things shared routing needs: it advertises the shared groups of
// every node (sharedDirectory) and it delivers an event to the peers that either
// have a matching normal subscription or are named in the event's shared half.
//
// extraGroups lets a test advertise a group for a node that does not actually hold
// a member, which is how the "elected node has no live member" case is forced.
type sharedTestCluster struct {
	mu    sync.Mutex
	nodes map[string]ClusterHandler
	extra map[string][]string // node id -> group keys advertised but not held
}

func newSharedTestCluster() *sharedTestCluster {
	return &sharedTestCluster{nodes: make(map[string]ClusterHandler), extra: make(map[string][]string)}
}

func (c *sharedTestCluster) Start(_ context.Context, nodeID string, h ClusterHandler) error {
	c.mu.Lock()
	c.nodes[nodeID] = h
	c.mu.Unlock()
	return nil
}

func (c *sharedTestCluster) Stop(context.Context) error { return nil }

func (c *sharedTestCluster) Spread(_ context.Context, msg ClusterMessage) error {
	c.mu.Lock()
	handlers := make(map[string]ClusterHandler, len(c.nodes))
	for id, h := range c.nodes {
		handlers[id] = h
	}
	c.mu.Unlock()
	for id, h := range handlers {
		if id == msg.NodeID {
			continue
		}
		switch msg.Type {
		case ClusterPublish:
			if !namesNode(msg.Shared, id) && !handlerMatches(h, msg.Topic) {
				continue
			}
			h.OnRemotePublish(msg)
		case ClusterKick:
			h.OnRemoteKick(msg)
		}
	}
	return nil
}

// sharedGroups implements sharedDirectory: the groups each node advertises,
// computed from the live handlers (plus whatever a test forces in extra).
func (c *sharedTestCluster) sharedGroups() map[string][]string {
	c.mu.Lock()
	handlers := make(map[string]ClusterHandler, len(c.nodes))
	for id, h := range c.nodes {
		handlers[id] = h
	}
	extra := make(map[string][]string, len(c.extra))
	for id, groups := range c.extra {
		extra[id] = append([]string(nil), groups...)
	}
	c.mu.Unlock()

	out := make(map[string][]string, 4)
	for id, h := range handlers {
		provider, ok := h.(sharedSubscriptionProvider)
		if !ok {
			continue
		}
		for _, group := range provider.sharedSubscriptionGroups() {
			out[group] = append(out[group], id)
		}
	}
	for id, groups := range extra {
		for _, group := range groups {
			out[group] = append(out[group], id)
		}
	}
	for group := range out {
		sort.Strings(out[group])
	}
	return out
}

// handlerMatches reports whether the handler has a normal filter matching topic.
func handlerMatches(h ClusterHandler, topic string) bool {
	provider, ok := h.(subscriptionProvider)
	if !ok {
		return false
	}
	for _, filter := range provider.subscriptionFilters() {
		if matchTopic(filter, topic) {
			return true
		}
	}
	return false
}

// sharedClusterPair starts two brokers on the same in-memory cluster and returns
// them with their metrics.
func sharedClusterPair(t *testing.T) (*Broker, *Broker, *deliveryMetrics, *deliveryMetrics, *sharedTestCluster) {
	t.Helper()
	cluster := newSharedTestCluster()
	metricsA, metricsB := newDeliveryMetrics(), newDeliveryMetrics()
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: cluster, Metrics: metricsA})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cluster, Metrics: metricsB})
	return a, b, metricsA, metricsB, cluster
}

// TestSharedClusterDeliversOneCopy is the cluster core: a group whose members live
// on two nodes receives each publication exactly once across the cluster, and the
// load is spread over both nodes instead of pinning the group to one.
func TestSharedClusterDeliversOneCopy(t *testing.T) {
	a, b, _, _, _ := sharedClusterPair(t)

	// One member of the group on each node.
	memberA, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "a-member", clean: true})
	memberB, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "b-member", clean: true})
	subscribeShared(t, memberA, 1, "$share/workers/cluster/jobs", 0, mqtt.Properties{})
	subscribeShared(t, memberB, 1, "$share/workers/cluster/jobs", 0, mqtt.Properties{})

	// A third group with a single member on node b, and a normal subscriber on
	// node b: each publication must reach exactly one member of each group and the
	// normal subscriber once.
	otherGroup, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "b-other", clean: true})
	subscribeShared(t, otherGroup, 1, "$share/reporters/cluster/jobs", 0, mqtt.Properties{})
	plain, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "b-plain", clean: true})
	if codes := subscribe(t, plain, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "cluster/jobs", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v for the normal subscriber", codes)
	}

	const total = 12
	pub, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "a-pub", clean: true})
	want := make([]string, 0, total)
	for i := 0; i < total; i++ {
		payload := "job-" + string(rune('a'+i))
		want = append(want, payload)
		pub.mustPublish(t, confPublish{topic: "cluster/jobs", payload: payload, qos: 1, pid: uint16(i + 1)})
	}

	gotA := payloads(collectPublishes(t, memberA, 400*time.Millisecond))
	gotB := payloads(collectPublishes(t, memberB, 400*time.Millisecond))
	// Every publication of the group reached exactly one member, across the
	// cluster: the origin elects the node, so no publication is delivered twice.
	if delivered := append(append([]string{}, gotA...), gotB...); !sameSet(delivered, want) {
		t.Fatalf("group deliveries = %v, want each of %d publications exactly once", delivered, total)
	}
	// Both nodes took part: the group follows the nodes, it is not pinned to one.
	if len(gotA) == 0 || len(gotB) == 0 {
		t.Fatalf("deliveries per node = %d/%d, want the group spread over both nodes", len(gotA), len(gotB))
	}

	// The second group got its own copy of every publication, and the normal
	// subscriber got one copy per publication.
	if got := payloads(collectPublishes(t, otherGroup, 400*time.Millisecond)); !sameSet(got, want) {
		t.Fatalf("second group deliveries = %v, want each publication once", got)
	}
	if got := payloads(collectPublishes(t, plain, 400*time.Millisecond)); !sameSet(got, want) {
		t.Fatalf("normal subscriber deliveries = %v, want each publication once", got)
	}
}

// TestSharedClusterNoLiveMemberIsDropped pins the documented end: when the
// election names a node that holds no live member of the group, the publication is
// dropped there and counted, never handed to another node's member. A group that no
// node advertises any more is not a shared publication at all — nobody matched it,
// so no drop is invented for it.
func TestSharedClusterNoLiveMemberIsDropped(t *testing.T) {
	a, _, metricsA, metricsB, cluster := sharedClusterPair(t)

	// The group is advertised for node b, but no node holds a member of it any
	// more: a summary one refresh old is exactly how this happens in production.
	cluster.mu.Lock()
	cluster.extra["b"] = []string{"$share/workers/void/jobs"}
	cluster.mu.Unlock()

	pub, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "void-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "void/jobs", payload: "lost", qos: 1, pid: 1})
	if got := metricsB.count("shared_no_member"); got != 1 {
		t.Fatalf("shared_no_member drops on the elected node = %d, want 1", got)
	}
	if got := metricsA.count("shared_no_member"); got != 0 {
		t.Fatalf("the origin counted %d drops, want 0: it elected a node", got)
	}

	// Once the advertisement is gone too, the publication is an ordinary
	// publication with no subscriber: it is neither delivered nor counted as a
	// shared drop (there is no group to drop it for).
	cluster.mu.Lock()
	delete(cluster.extra, "b")
	cluster.mu.Unlock()
	pub.mustPublish(t, confPublish{topic: "void/jobs", payload: "lost", qos: 1, pid: 2})
	if got := metricsB.count("shared_no_member"); got != 1 {
		t.Fatalf("shared_no_member drops after the group vanished = %d, want the previous 1 only", got)
	}
}

// TestSharedClusterElectedNodeWithoutMemberDrops pins the exact-once-or-drop
// invariant when the advertisement is asymmetrical: node a holds a member, the
// cluster also claims node b does, and node b does not. Every publication is either
// delivered to a's member (when a is elected) or dropped at b (when b is), never
// both, and never delivered to a node that does not hold the group.
func TestSharedClusterElectedNodeWithoutMemberDrops(t *testing.T) {
	a, _, metricsA, metricsB, cluster := sharedClusterPair(t)

	memberA, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "a-only", clean: true})
	subscribeShared(t, memberA, 1, "$share/workers/stale/jobs", 0, mqtt.Properties{})
	cluster.mu.Lock()
	cluster.extra["b"] = []string{"$share/workers/stale/jobs"}
	cluster.mu.Unlock()

	pub, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "stale-pub", clean: true})
	const total = 10
	for i := 0; i < total; i++ {
		pub.mustPublish(t, confPublish{topic: "stale/jobs", payload: "job", qos: 1, pid: uint16(i + 1)})
	}
	delivered := payloads(collectPublishes(t, memberA, 400*time.Millisecond))
	dropped := metricsB.count("shared_no_member")
	if len(delivered)+dropped != total {
		t.Fatalf("delivered %d and dropped %d, want %d together: every publication is delivered once or dropped once",
			len(delivered), dropped, total)
	}
	if dropped == 0 || len(delivered) == 0 {
		t.Fatalf("delivered %d and dropped %d, want both paths exercised", len(delivered), dropped)
	}
	if got := metricsA.count("shared_no_member"); got != 0 {
		t.Fatalf("the origin counted %d drops, want 0: it always had a member", got)
	}
}

// TestTCPClusterAdvertisesSharedGroups pins the TCP backend's half of the
// election: the group keys travel in the subscription summary and reach
// sharedGroups, while the topic filters of the same summary stay normal.
func TestTCPClusterAdvertisesSharedGroups(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	cfg := func(addr string, seed string) TCPClusterConfig {
		return TCPClusterConfig{
			Addr:              addr,
			Seeds:             []string{seed},
			ReconnectInterval: 20 * time.Millisecond,
			SummaryInterval:   20 * time.Millisecond,
			Logger:            discardLogger(),
		}
	}
	providerA := &summaryProvider{filters: []string{"normal/a"}, groups: []string{"$share/g/tcp/x"}}
	providerB := &summaryProvider{filters: []string{"normal/b"}, groups: []string{"$share/g/tcp/x"}}
	ca := startTestCluster(t, "a", cfg(aAddr, bAddr), providerA)
	cb := startTestCluster(t, "b", cfg(bAddr, aAddr), providerB)
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)
	_ = cb
	waitFor(t, "node a to see the shared group of node b", func() bool {
		groups := ca.sharedGroups()
		return len(groups["$share/g/tcp/x"]) == 1 && groups["$share/g/tcp/x"][0] == "b"
	})
	// The shared group travels beside the topic filters, not among them: the filter
	// list stays "topics a normal subscriber of this node would match".
	for _, filter := range tcpMemberFilters(ca, "b") {
		if strings.HasPrefix(filter, "$share/") {
			t.Fatalf("the shared group leaked into the topic filters: %v", tcpMemberFilters(ca, "b"))
		}
	}
}

// TestRedisClusterAdvertisesSharedGroups is the Redis backend's half of the same
// contract: the group keys round-trip through the summary record.
func TestRedisClusterAdvertisesSharedGroups(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	providerA := &summaryProvider{filters: []string{"normal/a"}, groups: []string{"$share/g/redis/x"}}
	providerB := &summaryProvider{filters: []string{"normal/b"}, groups: []string{"$share/g/redis/x"}}
	ca := startRedisCluster(t, mr, "a", providerA, cfg)
	cb := startRedisCluster(t, mr, "b", providerB, cfg)
	_ = cb
	// Members() reports the peers, not the node itself.
	waitRedisMembers(t, ca, "b")
	waitFor(t, "node a to see the shared group of node b", func() bool {
		groups := ca.sharedGroups()
		return len(groups["$share/g/redis/x"]) == 1 && groups["$share/g/redis/x"][0] == "b"
	})
}

// summaryProvider is a cluster handler that advertises a fixed filter and group
// set, so a test can assert what a backend puts on the wire without a broker.
type summaryProvider struct {
	filters []string
	groups  []string
}

func (p *summaryProvider) OnRemotePublish(ClusterMessage)     {}
func (p *summaryProvider) OnRemoteKick(ClusterMessage)        {}
func (p *summaryProvider) subscriptionFilters() []string      { return p.filters }
func (p *summaryProvider) sharedSubscriptionGroups() []string { return p.groups }

// tcpMemberFilters returns the topic filters a peer advertised.
func tcpMemberFilters(c *tcpCluster, nodeID string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m, ok := c.members[nodeID]; ok {
		return append([]string(nil), m.filters...)
	}
	return nil
}

// TestSharedAcrossTCPCluster runs the whole feature over the real TCP backend: two
// nodes, one member of the group on each, and the publications of one node reaching
// exactly one member across the cluster.
func TestSharedAcrossTCPCluster(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	cfg := func(addr, seed string) TCPClusterConfig {
		return TCPClusterConfig{
			Addr:              addr,
			Seeds:             []string{seed},
			ReconnectInterval: 20 * time.Millisecond,
			HeartbeatInterval: 50 * time.Millisecond,
			SummaryInterval:   20 * time.Millisecond,
			Logger:            discardLogger(),
		}
	}
	ca := NewTCPCluster(cfg(aAddr, bAddr))
	cb := NewTCPCluster(cfg(bAddr, aAddr))
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)

	memberA, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "tcp-member-a", clean: true})
	memberB, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "tcp-member-b", clean: true})
	subscribeShared(t, memberA, 1, "$share/workers/tcp/jobs", 0, mqtt.Properties{})
	subscribeShared(t, memberB, 1, "$share/workers/tcp/jobs", 0, mqtt.Properties{})
	// The election needs node a to see node b's group: the summary is pushed when
	// the subscription is accepted, so wait for it instead of sleeping.
	waitFor(t, "node a to see the group of node b", func() bool {
		return len(ca.(*tcpCluster).sharedGroups()["$share/workers/tcp/jobs"]) == 1
	})

	const total = 8
	want := make([]string, 0, total)
	pub, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "tcp-pub", clean: true})
	for i := 0; i < total; i++ {
		payload := "job-" + string(rune('a'+i))
		want = append(want, payload)
		pub.mustPublish(t, confPublish{topic: "tcp/jobs", payload: payload, qos: 1, pid: uint16(i + 1)})
	}
	gotA := payloads(collectPublishes(t, memberA, 500*time.Millisecond))
	gotB := payloads(collectPublishes(t, memberB, 500*time.Millisecond))
	if delivered := append(append([]string{}, gotA...), gotB...); !sameSet(delivered, want) {
		t.Fatalf("group deliveries = %v, want each of %d publications exactly once", delivered, total)
	}
	if len(gotA) == 0 || len(gotB) == 0 {
		t.Fatalf("deliveries per node = %d/%d, want the group spread over both nodes", len(gotA), len(gotB))
	}
}

// TestSharedAcrossRedisCluster is the same end-to-end case over the Redis backend,
// where the routing view is a TTL'd record instead of a peer link.
func TestSharedAcrossRedisCluster(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	ca := NewRedisCluster(cfg)
	cb := NewRedisCluster(cfg)
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb})
	waitRedisMembers(t, ca.(*redisCluster), "b")

	memberA, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "redis-member-a", clean: true})
	memberB, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "redis-member-b", clean: true})
	subscribeShared(t, memberA, 1, "$share/workers/redis/jobs", 0, mqtt.Properties{})
	subscribeShared(t, memberB, 1, "$share/workers/redis/jobs", 0, mqtt.Properties{})
	waitFor(t, "node a to see the group of node b", func() bool {
		return len(ca.(*redisCluster).sharedGroups()["$share/workers/redis/jobs"]) == 1
	})

	const total = 8
	want := make([]string, 0, total)
	pub, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "redis-pub", clean: true})
	for i := 0; i < total; i++ {
		payload := "job-" + string(rune('a'+i))
		want = append(want, payload)
		pub.mustPublish(t, confPublish{topic: "redis/jobs", payload: payload, qos: 1, pid: uint16(i + 1)})
	}
	gotA := payloads(collectPublishes(t, memberA, 500*time.Millisecond))
	gotB := payloads(collectPublishes(t, memberB, 500*time.Millisecond))
	if delivered := append(append([]string{}, gotA...), gotB...); !sameSet(delivered, want) {
		t.Fatalf("group deliveries = %v, want each of %d publications exactly once", delivered, total)
	}
	if len(gotA) == 0 || len(gotB) == 0 {
		t.Fatalf("deliveries per node = %d/%d, want the group spread over both nodes", len(gotA), len(gotB))
	}
}
