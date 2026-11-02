package cluster

// Wait policy for this file (and the rule a future change must keep): every wait is
// on an observable condition with a generous ceiling (redisWaitCeiling, 10 s).
// Nothing sleeps for a fixed time and then asserts.
//
// Two conditions are easy to get wrong here:
//
//   - Members() reads the member keys straight from Redis, but Spread routes through
//     the node's *cached* routing view, which is refreshed at most once per
//     SummaryInterval. Waiting for Members() therefore does not make a peer routable:
//     a kick or a retained publish spread in that window is written to no channel and
//     is lost. A test that asserts such a delivery waits for waitRedisView first.
//   - a Redis node's SUBSCRIBE is confirmed before it registers itself (see
//     redisCluster.Start), so a registered member is a live subscriber; there is no
//     separate subscription wait to add.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// startRedisTestCluster starts a Redis node and keeps the concrete type, which the
// backend tests below drive directly. The backend-agnostic starter it wraps lives
// in helpers_test.go.
func startRedisTestCluster(t *testing.T, mr *miniredis.Miniredis, nodeID string, handler ClusterHandler, cfg RedisClusterConfig) *redisCluster {
	t.Helper()
	return startRedisCluster(t, mr, nodeID, handler, cfg).(*redisCluster)
}

// waitRedisSummary waits until the node's cached routing view holds the summary a
// peer advertised. Routing is only directed once the publisher saw it, so a test
// that asserts on directed delivery waits for this and not for a sleep.
func waitRedisSummary(t *testing.T, c *redisCluster, nodeID string, want ...string) {
	t.Helper()
	deadline := time.Now().Add(redisWaitCeiling)
	var got []string
	for time.Now().Before(deadline) {
		if filters, known := routedSummary(c.Routed(), nodeID); known {
			got = filters
			if equalStringsUnordered(got, want) {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("routing view of %q = %v, want %v", nodeID, got, want)
}

// redisCounter is anything that reports applied publish/kick counts, which both
// recClusterHandler and filterHandler (through the embedded handler) do.
type redisCounter interface {
	counts() (int, int)
}

// waitRedisCounts waits until the handler applied at least the wanted publishes
// and kicks.
func waitRedisCounts(t *testing.T, h redisCounter, wantPublish, wantKick int) {
	t.Helper()
	deadline := time.Now().Add(redisWaitCeiling)
	var pub, kick int
	for time.Now().Before(deadline) {
		pub, kick = h.counts()
		if pub >= wantPublish && kick >= wantKick {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("applied publishes=%d kicks=%d, want %d/%d", pub, kick, wantPublish, wantKick)
}

// stopWithin asserts that Stop returns: a receive loop blocked without noticing
// the cancellation would hang here.
func stopWithin(t *testing.T, c Cluster, d time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Stop(context.Background()) }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("Stop did not return within %v", d)
		return nil
	}
}

// TestRedisClusterDirectedPublish checks the core difference from the TCP mesh
// and from a shared broadcast channel: each node subscribes to its own channel,
// and a publish is written only to the nodes whose advertised summary matches.
func TestRedisClusterDirectedPublish(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)

	hb := &filterHandler{}
	hb.setFilters("t/#")
	hc := &filterHandler{}
	hc.setFilters("other/#")

	a := startRedisTestCluster(t, mr, "a", &recClusterHandler{}, cfg)
	startRedisTestCluster(t, mr, "b", hb, cfg)
	startRedisTestCluster(t, mr, "c", hc, cfg)
	waitRedisMembers(t, a, "b", "c")
	waitRedisView(t, a, "b", "c")

	// Only the per-node channels exist, and exactly one node listens on each.
	subs := mr.PubSubNumSub(a.nodeChannel("a"), a.nodeChannel("b"), a.nodeChannel("c"))
	for _, nodeID := range []string{"a", "b", "c"} {
		if got := subs[a.nodeChannel(nodeID)]; got != 1 {
			t.Fatalf("subscribers on %s = %d, want 1", a.nodeChannel(nodeID), got)
		}
	}

	waitRedisSummary(t, a, "b", "t/#")
	waitRedisSummary(t, a, "c", "other/#")

	spread := func(topic, payload string) {
		t.Helper()
		if err := a.Spread(context.Background(), ClusterMessage{
			Type: ClusterPublish, NodeID: "a", Topic: topic, Payload: []byte(payload),
		}); err != nil {
			t.Fatalf("spread %s: %v", topic, err)
		}
	}
	spread("t/x", "to-b")
	spread("other/x", "to-c")

	waitRedisCounts(t, hb, 1, 0)
	waitRedisCounts(t, hc, 1, 0)
	// Neither node saw the topic the other one subscribed to. The publishes were
	// synchronous, so a stray delivery would already be in these counts.
	time.Sleep(100 * time.Millisecond)
	if pub, _ := hb.counts(); pub != 1 {
		t.Fatalf("node b applied %d publishes, want 1", pub)
	}
	if pub, _ := hc.counts(); pub != 1 {
		t.Fatalf("node c applied %d publishes, want 1", pub)
	}
}

// TestRedisClusterKickAndRetainedBroadcast checks the two exceptions to directed
// routing: a kick names a client id, and retained state must exist on every node
// for future subscribers, so both go to every known node channel.
func TestRedisClusterKickAndRetainedBroadcast(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)

	hb := &filterHandler{}
	hb.setFilters("t/#")
	hc := &filterHandler{}
	hc.setFilters("other/#")

	a := startRedisTestCluster(t, mr, "a", &recClusterHandler{}, cfg)
	startRedisTestCluster(t, mr, "b", hb, cfg)
	startRedisTestCluster(t, mr, "c", hc, cfg)
	waitRedisMembers(t, a, "b", "c")
	// Members() is read from Redis; the kick is routed through a's cached routing
	// view, which is only refreshed once per SummaryInterval. Without this wait the
	// kick can be spread before the view holds b and c, and a kick is published once
	// with no retry.
	waitRedisView(t, a, "b", "c")

	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterKick, NodeID: "a", ClientID: "device-1",
	}); err != nil {
		t.Fatal(err)
	}
	waitRedisCounts(t, hb, 0, 1)
	waitRedisCounts(t, hc, 0, 1)

	// A retained publish for a topic neither node subscribed to still reaches
	// both: the retained state has to be available to their future subscribers.
	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, NodeID: "a", Topic: "state/x", Payload: []byte("on"), Retain: true,
	}); err != nil {
		t.Fatal(err)
	}
	waitRedisCounts(t, hb, 1, 1)
	waitRedisCounts(t, hc, 1, 1)

	hb.mu.Lock()
	retained := hb.publish[0]
	hb.mu.Unlock()
	if retained.Topic != "state/x" || !retained.Retain || string(retained.Payload) != "on" {
		t.Fatalf("retained publish = %+v", retained)
	}
	hc.mu.Lock()
	kick := hc.kick[0]
	hc.mu.Unlock()
	if kick.ClientID != "device-1" {
		t.Fatalf("kick = %+v", kick)
	}
}

// TestRedisClusterUnknownSummaryReceivesPublish checks the availability rule: a
// node whose handler cannot describe its subscriptions advertises no summary,
// and peers then send it every publish instead of sending it nothing.
func TestRedisClusterUnknownSummaryReceivesPublish(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)

	ha := &recClusterHandler{}
	hb := &recClusterHandler{} // no SubscriptionProvider: never advertises
	a := startRedisTestCluster(t, mr, "a", ha, cfg)
	startRedisTestCluster(t, mr, "b", hb, cfg)
	waitRedisMembers(t, a, "b")

	if key := redisTestKey(cfg, "subs", "b"); mr.Exists(key) {
		t.Fatalf("a node without a subscription provider advertised %s", key)
	}
	if _, ok := clusterRoutedSummary(t, a, "b"); ok {
		t.Fatal("view reports a summary for a node that advertised none")
	}
	if err := a.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, NodeID: "a", Topic: "nobody/listens", Payload: []byte("still-routed"),
	}); err != nil {
		t.Fatal(err)
	}
	waitRedisCounts(t, hb, 1, 0)
	if pub, _ := ha.counts(); pub != 0 {
		t.Fatalf("a node received its own publish (%d)", pub)
	}
}

// TestRedisClusterDedupAndLoopGuard checks the loop and duplicate rules on the
// receive path: a redelivered (origin, seq) is applied once, and an event whose
// origin is this node is dropped before the dedup table so own traffic cannot
// pollute it.
func TestRedisClusterDedupAndLoopGuard(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	// A small table makes pollution observable: five own-origin events would
	// evict the two real identities below.
	cfg.DedupSize = 4

	hb := &recClusterHandler{}
	b := startRedisTestCluster(t, mr, "b", hb, cfg)
	rdb := redisTestClient(t, mr)

	frame := func(origin string, seq uint64, payload string) string {
		t.Helper()
		raw, err := json.Marshal(redisFrame{Version: redisClusterWireVersion, NodeID: origin, Event: &ClusterMessage{
			Type: ClusterPublish, NodeID: origin, Origin: origin, Seq: seq,
			Topic: "dup/x", Payload: []byte(payload),
		}})
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	publish := func(payload string) {
		t.Helper()
		if err := rdb.Publish(context.Background(), b.nodeChannel("b"), payload).Err(); err != nil {
			t.Fatalf("publish to node channel: %v", err)
		}
	}

	// The same (origin, seq) twice, as a reconnect or a duplicate link would.
	publish(frame("a", 7, "once"))
	publish(frame("a", 7, "once"))
	waitRedisCounts(t, hb, 1, 0)
	// A different seq is a different event and is applied.
	publish(frame("a", 8, "twice"))
	waitRedisCounts(t, hb, 2, 0)

	// Own-origin events are dropped before dedup: five of them must leave the
	// table untouched and be counted as loop drops, not duplicate drops.
	for seq := uint64(100); seq < 105; seq++ {
		publish(frame("b", seq, "own"))
	}
	// A redelivery of an identity that is still remembered proves the table was
	// not overwritten by the five own-origin events: it must not be applied again.
	publish(frame("a", 7, "once"))
	publish(frame("a", 9, "marker")) // ordering marker: processed after the above
	waitRedisCounts(t, hb, 3, 0)

	if got := b.dedup.Len(); got != 3 {
		t.Fatalf("dedup table holds %d identities, want 3 (a/7, a/8, a/9)", got)
	}
	// One duplicate drop for the first redelivery, five loop drops, one duplicate
	// drop for the redelivery that survived the own-origin traffic.
	if got := clusterStats(t, b).Drops; got != 7 {
		t.Fatalf("dropped %d events, want 7 (1 duplicate + 5 own-origin + 1 duplicate)", got)
	}
	hb.mu.Lock()
	defer hb.mu.Unlock()
	if len(hb.publish) != 3 {
		t.Fatalf("applied %d publishes, want 3", len(hb.publish))
	}
	if got := string(hb.publish[2].Payload); got != "marker" {
		t.Fatalf("third applied event = %q, want the marker (own-origin traffic was applied?)", got)
	}
}

// TestRedisClusterDoesNotForwardReceivedEvents checks the invariant that makes a
// re-broadcasting topology safe: an event that already carries another node's
// origin is dropped (and counted), not republished.
func TestRedisClusterDoesNotForwardReceivedEvents(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)

	ha := &recClusterHandler{}
	a := startRedisTestCluster(t, mr, "a", ha, cfg)
	b := startRedisTestCluster(t, mr, "b", &recClusterHandler{}, cfg)
	waitRedisMembers(t, a, "b")

	// A received event carries the origin of the node that produced it.
	if err := b.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, NodeID: "a", Origin: "a", Seq: 1,
		Topic: "forward/x", Payload: []byte("must-not-leave"),
	}); err != nil {
		t.Fatalf("Spread returned %v, want a silent drop", err)
	}
	if got := clusterStats(t, b).Drops; got != 1 {
		t.Fatalf("drops = %d, want 1 (cluster_forward)", got)
	}
	time.Sleep(100 * time.Millisecond)
	if pub, _ := ha.counts(); pub != 0 {
		t.Fatalf("the origin node received a forwarded event (%d)", pub)
	}
}

// TestRedisClusterNodeTTLAndRestart checks that membership is a Redis TTL: a node
// that stops refreshing leaves the view on its own, and a restart reappears under
// a new incarnation.
func TestRedisClusterNodeTTLAndRestart(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	cfg.NodeTTL = 250 * time.Millisecond
	cfg.SummaryTTL = 250 * time.Millisecond

	a := startRedisTestCluster(t, mr, "a", &recClusterHandler{}, cfg)
	b := startRedisTestCluster(t, mr, "b", &recClusterHandler{}, cfg)
	waitRedisMembers(t, a, "b")

	b.mu.Lock()
	incarnation := b.incarnation
	cancel := b.cancel
	b.mu.Unlock()

	// Simulate a crash: stop refreshing without deregistering (Stop would delete
	// the keys immediately). Only the TTL may remove the node.
	cancel()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(a.Members()) != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := clusterMemberIDs(t, a); len(got) != 0 {
		t.Fatalf("crashed node is still a member after NodeTTL: %v", got)
	}
	if mr.Exists(a.memberKey("b")) {
		t.Fatalf("member key survived NodeTTL: %s", a.memberKey("b"))
	}

	// The same node id restarting is a new incarnation, not the old member.
	startRedisTestCluster(t, mr, "b", &recClusterHandler{}, cfg)
	waitRedisMembers(t, a, "b")
	members := a.Members()
	if members[0].Incarnation == "" || members[0].Incarnation == incarnation {
		t.Fatalf("restarted member incarnation = %q, want a new incarnation (old %q)", members[0].Incarnation, incarnation)
	}
	if members[0].Since.IsZero() {
		t.Fatal("restarted member has no join time")
	}
}

// TestRedisClusterRedisOutage checks the failure contract: a locally produced
// event reports a classified error while Redis is unreachable, the receive loop
// keeps retrying instead of giving up, delivery resumes when Redis comes back,
// and Stop returns throughout.
func TestRedisClusterRedisOutage(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)

	ha, hb := &recClusterHandler{}, &recClusterHandler{}
	a := startRedisTestCluster(t, mr, "a", ha, cfg)
	b := startRedisTestCluster(t, mr, "b", hb, cfg)
	victim := startRedisTestCluster(t, mr, "victim", &recClusterHandler{}, cfg)
	waitRedisMembers(t, a, "b", "victim")

	spread := func(payload string) error {
		return a.Spread(context.Background(), ClusterMessage{
			Type: ClusterPublish, NodeID: "a", Topic: "outage/x", Payload: []byte(payload),
		})
	}
	if err := spread("before"); err != nil {
		t.Fatalf("spread before the outage: %v", err)
	}
	waitRedisCounts(t, hb, 1, 0)

	mr.Close()

	// A locally produced event must not be reported as delivered.
	err := spread("during")
	if err == nil {
		t.Fatal("Spread succeeded while Redis was unreachable")
	}
	if !errors.Is(err, ErrClusterUnavailable) {
		t.Fatalf("Spread error %q does not wrap ErrClusterUnavailable", err)
	}

	// Stop still returns while Redis is down (twice: it is idempotent), which it
	// could not do if the receive loop ignored the cancellation.
	if err := stopWithin(t, victim, 5*time.Second); err != nil {
		t.Fatalf("Stop during outage: %v", err)
	}
	if err := victim.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	// Redis comes back on the same address: the receive loop must reconnect and
	// resubscribe without a restart of the node.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := mr.Restart(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("miniredis did not restart")
		}
		time.Sleep(10 * time.Millisecond)
	}
	ok := false
	for time.Now().Before(deadline) {
		if err := spread("after"); err == nil {
			// A retry may be published before the peer finished resubscribing, so
			// wait for any delivery after the one that succeeded before the outage.
			if pub, _ := hb.counts(); pub >= 2 {
				ok = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ok {
		pub, _ := hb.counts()
		t.Fatalf("delivery did not resume after the outage (applied %d, want 2)", pub)
	}

	// And the recovered nodes still stop cleanly.
	if err := stopWithin(t, a, 5*time.Second); err != nil {
		t.Fatalf("Stop after recovery: %v", err)
	}
	if err := stopWithin(t, b, 5*time.Second); err != nil {
		t.Fatalf("Stop after recovery: %v", err)
	}
}

// TestRedisClusterKeyLayout pins the documented Redis layout: it is shared state
// that operators and other tools read, not an implementation detail.
func TestRedisClusterKeyLayout(t *testing.T) {
	c := NewRedisCluster(RedisClusterConfig{Addr: "127.0.0.1:6379"}).(*redisCluster)
	for _, tc := range []struct {
		got, want string
	}{
		{c.nodeChannel("n1"), "gmqx:cluster:node:n1"},
		{c.summaryKey("n1"), "gmqx:cluster:subs:n1"},
		{c.memberKey("n1"), "gmqx:cluster:member:n1"},
		{c.ownerKey("n1"), "gmqx:cluster:owner:n1"},
		{c.memberPattern(), "gmqx:cluster:member:*"},
	} {
		if tc.got != tc.want {
			t.Fatalf("key = %q, want %q", tc.got, tc.want)
		}
	}
	custom := NewRedisCluster(RedisClusterConfig{Addr: "127.0.0.1:6379", ChannelPrefix: "tenant:7"}).(*redisCluster)
	if got, want := custom.nodeChannel("n1"), "tenant:7:node:n1"; got != want {
		t.Fatalf("prefixed channel = %q, want %q", got, want)
	}

	// The keys really exist with the configured TTLs once a node starts.
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	c1 := NewRedisCluster(cfg).(*redisCluster)
	h := &filterHandler{}
	h.setFilters("a/#")
	if err := c1.Start(context.Background(), "n1", h); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c1.Stop(context.Background()) })
	if !mr.Exists(c1.memberKey("n1")) {
		t.Fatalf("member key %s was not written", c1.memberKey("n1"))
	}
	if !mr.Exists(c1.summaryKey("n1")) {
		t.Fatalf("summary key %s was not written", c1.summaryKey("n1"))
	}
	if !mr.Exists(c1.ownerKey("n1")) {
		t.Fatalf("ownership key %s was not written", c1.ownerKey("n1"))
	}
	if ttl := mr.TTL(c1.memberKey("n1")); ttl <= 0 || ttl > cfg.NodeTTL {
		t.Fatalf("member key TTL = %v, want (0, %v]", ttl, cfg.NodeTTL)
	}
	if ttl := mr.TTL(c1.ownerKey("n1")); ttl <= 0 || ttl > cfg.NodeTTL {
		t.Fatalf("ownership key TTL = %v, want (0, %v]", ttl, cfg.NodeTTL)
	}
	if ttl := mr.TTL(c1.summaryKey("n1")); ttl <= 0 || ttl > cfg.SummaryTTL {
		t.Fatalf("summary key TTL = %v, want (0, %v]", ttl, cfg.SummaryTTL)
	}
	raw, err := mr.Get(c1.summaryKey("n1"))
	if err != nil {
		t.Fatal(err)
	}
	var rec redisSummaryRecord
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		t.Fatalf("summary value %q: %v", raw, err)
	}
	if rec.NodeID != "n1" || !equalStrings(rec.Filters, []string{"a/#"}) {
		t.Fatalf("summary record = %+v", rec)
	}
}

// TestRedisClusterConfigValidation checks that Start refuses a configuration it
// cannot honour, before it touches the network.
func TestRedisClusterConfigValidation(t *testing.T) {
	valid := RedisClusterConfig{Addr: "127.0.0.1:1", Logger: discardLogger()}
	cases := []struct {
		name   string
		nodeID string
		mutate func(*RedisClusterConfig)
		want   string
	}{
		{name: "missing node id", nodeID: "", want: "node id"},
		{name: "empty addr", nodeID: "n", mutate: func(c *RedisClusterConfig) { c.Addr = " " }, want: "Addr"},
		{name: "negative db", nodeID: "n", mutate: func(c *RedisClusterConfig) { c.DB = -1 }, want: "DB"},
		{name: "negative summary interval", nodeID: "n", mutate: func(c *RedisClusterConfig) { c.SummaryInterval = -time.Second }, want: "SummaryInterval"},
		{name: "negative summary ttl", nodeID: "n", mutate: func(c *RedisClusterConfig) { c.SummaryTTL = -time.Second }, want: "SummaryTTL"},
		{name: "negative node ttl", nodeID: "n", mutate: func(c *RedisClusterConfig) { c.NodeTTL = -time.Second }, want: "NodeTTL"},
		{name: "negative heartbeat", nodeID: "n", mutate: func(c *RedisClusterConfig) { c.HeartbeatInterval = -time.Second }, want: "HeartbeatInterval"},
		{name: "negative dedup ttl", nodeID: "n", mutate: func(c *RedisClusterConfig) { c.DedupTTL = -time.Second }, want: "DedupTTL"},
		{name: "negative dedup size", nodeID: "n", mutate: func(c *RedisClusterConfig) { c.DedupSize = -1 }, want: "DedupSize"},
		{name: "negative summary cap", nodeID: "n", mutate: func(c *RedisClusterConfig) { c.MaxSummaryFilters = -1 }, want: "MaxSummaryFilters"},
		{
			name: "node ttl does not outlive the refresh period", nodeID: "n",
			mutate: func(c *RedisClusterConfig) { c.SummaryInterval = time.Second; c.NodeTTL = time.Second },
			want:   "NodeTTL",
		},
		{
			name: "summary ttl does not outlive the refresh period", nodeID: "n",
			mutate: func(c *RedisClusterConfig) { c.SummaryInterval = time.Second; c.SummaryTTL = 500 * time.Millisecond },
			want:   "SummaryTTL",
		},
		{name: "glob prefix", nodeID: "n", mutate: func(c *RedisClusterConfig) { c.ChannelPrefix = "gmqx:*" }, want: "ChannelPrefix"},
		{name: "unreachable redis", nodeID: "n", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			if tc.mutate != nil {
				tc.mutate(&cfg)
			}
			c := NewRedisCluster(cfg)
			err := c.Start(context.Background(), tc.nodeID, &recClusterHandler{})
			if err == nil {
				_ = c.Stop(context.Background())
				t.Fatalf("Start accepted %+v", cfg)
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
			// Stop after a refused Start is a no-op, and nothing may be left over.
			if err := stopWithin(t, c, 5*time.Second); err != nil {
				t.Fatalf("Stop after refused Start: %v", err)
			}
		})
	}

	// Zero values select the documented defaults.
	mr := runMiniRedis(t)
	c := NewRedisCluster(RedisClusterConfig{Addr: mr.Addr(), Logger: discardLogger()})
	if err := c.Start(context.Background(), "n1", &recClusterHandler{}); err != nil {
		t.Fatalf("defaults rejected: %v", err)
	}
	defer func() { _ = c.Stop(context.Background()) }()
	rc := c.(*redisCluster)
	if rc.prefix() != DefaultRedisChannelPrefix {
		t.Fatalf("prefix = %q", rc.prefix())
	}
	if rc.summaryTTL() <= rc.summaryInterval() || rc.nodeTTL() <= rc.summaryInterval() {
		t.Fatalf("default TTLs do not outlive the refresh period: summary=%v node=%v interval=%v",
			rc.summaryTTL(), rc.nodeTTL(), rc.summaryInterval())
	}
	if rc.maxSummaryFilters() != defaultMaxSummaryFilters {
		t.Fatalf("max summary filters = %d", rc.maxSummaryFilters())
	}
}

// TestRedisClusterSummaryCap checks that the advertised summary is capped and
// that the oldest filters are the ones left out, as in the TCP backend.
func TestRedisClusterSummaryCap(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	cfg.MaxSummaryFilters = 3

	h := &filterHandler{}
	h.setFilters("a/1", "a/2", "a/3", "a/4", "a/5")
	c := startRedisTestCluster(t, mr, "n1", h, cfg)
	rdb := redisTestClient(t, mr)

	summary := func() redisSummaryRecord {
		t.Helper()
		raw, err := rdb.Get(context.Background(), c.summaryKey("n1")).Result()
		if err != nil {
			t.Fatalf("read summary: %v", err)
		}
		var rec redisSummaryRecord
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			t.Fatalf("summary value %q: %v", raw, err)
		}
		return rec
	}
	waitSummary := func(want ...string) {
		t.Helper()
		deadline := time.Now().Add(redisWaitCeiling)
		var got []string
		for time.Now().Before(deadline) {
			got = summary().Filters
			if equalStrings(got, want) {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("advertised summary = %v, want %v", got, want)
	}
	waitSummary("a/3", "a/4", "a/5")

	// A subscription change is pushed, not awaited: the same cap applies.
	h.setFilters("a/4", "a/5", "a/6")
	c.SubscriptionsChanged()
	waitSummary("a/4", "a/5", "a/6")
}
