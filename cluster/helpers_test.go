package cluster

// Shared helpers of the backend tests in this package.
//
// They are the package-internal version of what the root package's tests see
// through the public views (docs/design/source-layout.md §4.1): these tests live
// beside the implementation and may read its fields, while the helpers here stay
// on the exported contract and views so they keep working if a backend grows a new
// internal shape. The root package keeps its own copy for the broker × cluster
// tests it holds.

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Wait policy for the TCP tests (and the rule a future change must keep): every wait
// is on an observable cluster condition with a generous ceiling (tcpWaitCeiling,
// 10 s). Nothing here sleeps for a fixed time and asserts afterwards.
// tcpWaitCeiling bounds a cluster wait. It exists so a genuinely broken build fails
// instead of hanging; a correct one returns as soon as its condition holds.
const tcpWaitCeiling = 10 * time.Second

type recClusterHandler struct {
	mu      sync.Mutex
	publish []ClusterMessage
	kick    []ClusterMessage
}

func (r *recClusterHandler) OnRemotePublish(msg ClusterMessage) {
	r.mu.Lock()
	r.publish = append(r.publish, msg)
	r.mu.Unlock()
}

func (r *recClusterHandler) OnRemoteKick(msg ClusterMessage) {
	r.mu.Lock()
	r.kick = append(r.kick, msg)
	r.mu.Unlock()
}

func (r *recClusterHandler) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.publish), len(r.kick)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// clusterWaitCeiling bounds every wait below. It is generous because each wait is
// on an observable condition and returns as soon as the condition holds.
const clusterWaitCeiling = 10 * time.Second

// clusterLinkQuiet is how long a node's link set must be unchanged before a test
// treats it as settled.
const clusterLinkQuiet = 250 * time.Millisecond

// clusterStats returns a backend's counters, failing the test when it has no
// StatsView.
func clusterStats(t *testing.T, c Cluster) ClusterStats {
	t.Helper()
	view, ok := c.(StatsView)
	if !ok {
		t.Fatalf("%T does not implement StatsView", c)
	}
	return view.Stats()
}

// clusterLinks returns a backend's physical links, failing the test when it has no
// LinkView.
func clusterLinks(t *testing.T, c Cluster) []Link {
	t.Helper()
	view, ok := c.(LinkView)
	if !ok {
		t.Fatalf("%T does not implement LinkView", c)
	}
	return view.Links()
}

// clusterMembers returns a backend's member view, failing the test when it has no
// MemberView.
func clusterMembers(t *testing.T, c Cluster) []Member {
	t.Helper()
	view, ok := c.(MemberView)
	if !ok {
		t.Fatalf("%T does not implement MemberView", c)
	}
	return view.Members()
}

// clusterMemberIDs returns the sorted peer ids of a node.
func clusterMemberIDs(t *testing.T, c Cluster) []string {
	t.Helper()
	out := []string{}
	for _, m := range clusterMembers(t, c) {
		out = append(out, m.NodeID)
	}
	return out
}

// clusterMemberSummary returns the subscription summary a peer advertised, and
// whether that summary is known at all.
func clusterMemberSummary(t *testing.T, c Cluster, nodeID string) ([]string, bool) {
	t.Helper()
	for _, m := range clusterMembers(t, c) {
		if m.NodeID == nodeID {
			if !m.SummaryKnown {
				return nil, false
			}
			return m.Filters, true
		}
	}
	return nil, false
}

// clusterRouted returns a backend's routing view, failing the test when it has no
// RoutingView.
func clusterRouted(t *testing.T, c Cluster) []Routed {
	t.Helper()
	view, ok := c.(RoutingView)
	if !ok {
		t.Fatalf("%T does not implement RoutingView", c)
	}
	return view.Routed()
}

// clusterRoutedSummary returns the summary a backend's routing view holds for a
// node, which is the summary a spread would route by right now.
func clusterRoutedSummary(t *testing.T, c Cluster, nodeID string) ([]string, bool) {
	t.Helper()
	for _, r := range clusterRouted(t, c) {
		if r.NodeID == nodeID {
			if !r.SummaryKnown {
				return nil, false
			}
			return r.Filters, true
		}
	}
	return nil, false
}

// waitClusterMembers waits until the node sees exactly the given peers (itself
// excluded by Members).
func waitClusterMembers(t *testing.T, c Cluster, want ...string) {
	t.Helper()
	sort.Strings(want)
	deadline := time.Now().Add(clusterWaitCeiling)
	var got []string
	for time.Now().Before(deadline) {
		got = clusterMemberIDs(t, c)
		if equalStrings(got, want) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("member view = %v, want %v", got, want)
}

// waitClusterLink waits until a link backs the peer, which is what a send needs;
// the member view alone can be one gossip frame ahead of the link.
func waitClusterLink(t *testing.T, c Cluster, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(clusterWaitCeiling)
	for time.Now().Before(deadline) {
		for _, l := range clusterLinks(t, c) {
			if l.Peer == nodeID {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no link to %s in %v", nodeID, clusterLinks(t, c))
}

// equalStrings reports whether two sorted string slices are equal.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// equalStringsUnordered compares two string slices ignoring order.
func equalStringsUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	return equalStrings(as, bs)
}

// clusterLinkSignature is the identity of a node's link set: one token per link
// with its connection id, its peer, its addresses and its direction. A replaced
// link produces a new token even when the replacement has the same peer and
// direction, because the connection id is never reused.
func clusterLinkSignature(t *testing.T, c Cluster) string {
	t.Helper()
	parts := make([]string, 0, 4)
	for _, l := range clusterLinks(t, c) {
		dir := "in"
		if l.Outbound {
			dir = "out"
		}
		parts = append(parts, fmt.Sprintf("%d/%s/%s/%s/%s", l.ID, l.Peer, l.Addr, l.DialAddr, dir))
	}
	return strings.Join(parts, ",")
}

// waitClusterSettled waits until the link sets of all given nodes have been
// unchanged for clusterLinkQuiet, so a broadcast made right after it is not sent
// into a link that is being superseded.
//
// Two nodes that dial each other reconcile to one physical connection just after
// startup: the superseded link is closed and the survivor is the other dial, so for
// a few milliseconds a peer has no link at all. A send in that window is dropped
// and a cluster event is never retried, so a test that asserts a delivery waits for
// the reconciliation to finish rather than for a link to exist. The wait is an
// observation of the cluster, not a fixed sleep: it is bounded by
// clusterWaitCeiling and returns as soon as the sets are proven stable.
func waitClusterSettled(t *testing.T, cs ...Cluster) {
	t.Helper()
	deadline := time.Now().Add(clusterWaitCeiling)
	sigs := make([]string, len(cs))
	since := make([]time.Time, len(cs))
	start := time.Now()
	for i, c := range cs {
		sigs[i], since[i] = clusterLinkSignature(t, c), start
	}
	var last []string
	for time.Now().Before(deadline) {
		settled := true
		now := time.Now()
		for i, c := range cs {
			sig := clusterLinkSignature(t, c)
			if sig != sigs[i] {
				sigs[i], since[i] = sig, now
				settled = false
				continue
			}
			if now.Sub(since[i]) < clusterLinkQuiet {
				settled = false
			}
		}
		last = sigs
		if settled {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("link sets did not settle within %v (last: %v)", clusterWaitCeiling, last)
}

// redisWaitCeiling bounds a Redis-backed cluster wait.
const redisWaitCeiling = 10 * time.Second

// discardLogger keeps test output readable: membership and routing decisions are
// logged at Info/Debug and the tests do not assert on them.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// startTestCluster starts a TCP backend and returns it as a Cluster; a test that
// needs the concrete type uses startTCPTestCluster.
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
// backend's own builders. The layout is a pinned contract: TestRedisClusterKeyLayout
// asserts this hand-built copy against the backend's builders, so a change to either
// side fails the test.
func redisTestKey(cfg RedisClusterConfig, kind, name string) string {
	prefix := cfg.ChannelPrefix
	if prefix == "" {
		prefix = "gmqx:cluster"
	}
	return prefix + ":" + kind + ":" + name
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

// waitFor polls a condition with a short ceiling. The backends tear links down on
// their own goroutines, so a test that follows a change waits for the goroutine
// instead of assuming it has already run.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
