package engine

// Helpers that build and observe the built-in backends for the broker × cluster
// tests in this package. The backends live in gmqx/cluster (see
// docs/design/source-layout.md §4.1), so nothing here reaches into one: they use
// only the public Cluster contract and the public views re-exported by cluster.go.
//
// Nothing here reaches into a backend's fields. What a test needs and cannot find
// here has to become a public observation first, which is the point: a consumer has
// no more access than a test in this package.

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// redisWaitCeiling bounds a Redis-backed cluster wait.
const redisWaitCeiling = 10 * time.Second

// discardLogger keeps test output readable: membership and routing decisions are
// logged at Info/Debug and the tests do not assert on them.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// startTestCluster starts a TCP backend and returns it as a Cluster: the concrete
// backend type is the backend package's business, not a test's.
func startTestCluster(t *testing.T, nodeID string, cfg TCPClusterConfig, handler ClusterHandler) Cluster {
	t.Helper()
	if cfg.Logger == nil {
		cfg.Logger = discardLogger()
	}
	c := NewTCPCluster(cfg)
	if err := c.Start(context.Background(), nodeID, handler); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

// gossipTestConfig is the fast variant of the gossip defaults: every timer is in
// the millisecond range so a membership transition is observed in a few polls
// instead of a few seconds.
func gossipTestConfig(addr string, seeds ...string) GossipClusterConfig {
	return GossipClusterConfig{
		Addr:  addr,
		Seeds: seeds,
		// The probe timeout has to absorb a scheduling stall of the test process, not
		// a network round trip: a direct probe that times out in a two-node cluster
		// marks the peer suspect at once, and a suspect member is declared dead (and
		// its link closed) one suspicion window later. 100 ms was small enough that a
		// loaded -race run declared a live peer dead, dropped the link and lost the
		// one-shot event a test was waiting for. The suspicion window is still short
		// enough for the membership test, which drives the clock itself.
		PingInterval:        25 * time.Millisecond,
		PingTimeout:         500 * time.Millisecond,
		SuspicionMultiplier: 4,
		DeadTimeout:         300 * time.Millisecond,
		ReconnectInterval:   10 * time.Millisecond,
		DialTimeout:         500 * time.Millisecond,
		SummaryInterval:     25 * time.Millisecond,
		PresenceTTL:         time.Second,
		Logger:              discardLogger(),
	}
}

// startGossipCluster starts a gossip backend and returns it as a Cluster. A test
// that needs a backend internal asserts the concrete type itself.
func startGossipCluster(t *testing.T, nodeID string, cfg GossipClusterConfig, handler ClusterHandler) Cluster {
	t.Helper()
	c := NewGossipCluster(cfg)
	if err := c.Start(context.Background(), nodeID, handler); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

// runMiniRedis starts miniredis with clock-driven TTLs. Without ClockTTL a
// miniredis key only expires on an explicit FastForward, and a test that asserts
// "the member key expired" would assert against the fake clock instead of the real
// one.
func runMiniRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.ClockTTL(true)
	return mr
}

// redisTestConfig is a RedisClusterConfig tuned for tests: short intervals and
// TTLs so a test does not have to wait a second for a refresh or an expiry.
func redisTestConfig(mr *miniredis.Miniredis) RedisClusterConfig {
	return RedisClusterConfig{
		Addr:              mr.Addr(),
		SummaryInterval:   20 * time.Millisecond,
		SummaryTTL:        400 * time.Millisecond,
		NodeTTL:           400 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Logger:            discardLogger(),
	}
}

// startRedisCluster starts one node against the shared miniredis and stops it at
// the end of the test.
func startRedisCluster(t *testing.T, mr *miniredis.Miniredis, nodeID string, handler ClusterHandler, cfg RedisClusterConfig) Cluster {
	t.Helper()
	c := NewRedisCluster(cfg)
	if err := c.Start(context.Background(), nodeID, handler); err != nil {
		t.Fatalf("start node %q: %v", nodeID, err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

// redisTestClient is a plain client for the same miniredis: it lets a test
// publish a raw frame or inspect a key without going through the backend.
func redisTestClient(t *testing.T, mr *miniredis.Miniredis) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// redisTestKey builds one key of the Redis backend's layout without calling the
// backend. The layout is a pinned contract (TestRedisClusterKeyLayout asserts it
// against the backend's own builders), and this package's tests may not call those
// builders once the backend moves to gmqx/cluster.
func redisTestKey(cfg RedisClusterConfig, kind, name string) string {
	prefix := cfg.ChannelPrefix
	if prefix == "" {
		prefix = "gmqx:cluster"
	}
	return prefix + ":" + kind + ":" + name
}

// redisTestPresenceKey is the presence key of one client id under one namespace.
func redisTestPresenceKey(cfg RedisClusterConfig, namespace, clientID string) string {
	return redisTestKey(cfg, "client", namespaceKey(namespace, clientID))
}

// waitRedisMembers waits until the member view read from Redis contains exactly
// want.
func waitRedisMembers(t *testing.T, c Cluster, want ...string) {
	t.Helper()
	sort.Strings(want)
	deadline := time.Now().Add(redisWaitCeiling)
	var got []string
	for time.Now().Before(deadline) {
		got = clusterMemberIDs(t, c)
		if equalStrings(got, want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("members = %v, want %v", got, want)
}

// routedIDs returns the sorted node ids of a routing view.
func routedIDs(routed []Routed) []string {
	out := make([]string, 0, len(routed))
	for _, r := range routed {
		out = append(out, r.NodeID)
	}
	sort.Strings(out)
	return out
}

// routedSummary returns the subscription summary a routing view holds for a node.
func routedSummary(routed []Routed, nodeID string) ([]string, bool) {
	for _, r := range routed {
		if r.NodeID == nodeID {
			if !r.SummaryKnown {
				return nil, false
			}
			return r.Filters, true
		}
	}
	return nil, false
}

// waitRedisView waits until the node's cached routing view holds exactly want: the
// view is a node's own copy of the shared Redis state, refreshed at most once per
// SummaryInterval. Waiting for membership is not enough, because a spread routes
// through this view.
func waitRedisView(t *testing.T, c Cluster, want ...string) {
	t.Helper()
	view, ok := c.(RoutingView)
	if !ok {
		t.Fatalf("%T does not implement RoutingView", c)
	}
	sort.Strings(want)
	deadline := time.Now().Add(redisWaitCeiling)
	var got []string
	for time.Now().Before(deadline) {
		got = routedIDs(view.Routed())
		if equalStrings(got, want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("routing view = %v, want %v", got, want)
}

// gossipWaitCeiling bounds every membership/routing wait of a gossip test. It is a
// ceiling, not a timing assertion: the wait returns as soon as the condition it
// observes holds, so a loaded machine costs nothing but a genuinely broken build
// still fails instead of hanging.
const gossipWaitCeiling = 10 * time.Second

// testClusterConfig is the fast TCP mesh configuration the broker-level cluster
// tests use: short intervals so a handshake, a summary push or a link collapse is
// observed in a few polls instead of a few seconds.
func testClusterConfig(addr string, seeds []string) TCPClusterConfig {
	return TCPClusterConfig{
		Addr:                addr,
		Seeds:               seeds,
		ReconnectInterval:   20 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		HeartbeatMissBudget: 3,
		SummaryInterval:     50 * time.Millisecond,
		Logger:              discardLogger(),
	}
}

// filterHandler advertises a mutable filter set, like a broker with local
// subscriptions. It implements cluster.SubscriptionProvider, which is what a
// backend type-asserts the handler it was given for; the method is exported because
// that interface lives in gmqx/cluster.
type filterHandler struct {
	recClusterHandler
	mu      sync.Mutex
	filters []string
}

// SubscriptionFilters implements cluster.SubscriptionProvider.
func (h *filterHandler) SubscriptionFilters() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.filters...)
}

func (h *filterHandler) setFilters(filters ...string) {
	h.mu.Lock()
	h.filters = filters
	h.mu.Unlock()
}

// namesNode reports whether a shared-delivery list names nodeID. The backends use
// the same rule to decide which node delivers a shared group; a fake backend in the
// root package needs it too, and the backend's own copy is unexported.
func namesNode(shared []SharedDelivery, nodeID string) bool {
	for _, d := range shared {
		if d.Node == nodeID {
			return true
		}
	}
	return false
}
