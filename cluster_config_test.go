package gmqx

// Broker-level cluster configuration. The backends themselves live in gmqx/cluster
// (docs/design/source-layout.md §4.1), so this file observes them only through the
// public views in cluster.go; the backend's own validation lives in that package.

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestClusterConfigSelectsGossip pins the default-selection rule and the mapping
// from ClusterConfig onto the gossip backend: Cluster.Enabled with no explicit
// backend creates the gossip backend, its fields reach the backend, and an explicit
// ClusterBackend always wins.
func TestClusterConfigSelectsGossip(t *testing.T) {
	addr := freeAddr(t)
	clusterCfg := ClusterConfig{
		Enabled:             true,
		Addr:                addr,
		Seeds:               []string{"127.0.0.1:1"},
		Node:                "cluster-node",
		Namespace:           "tenant-x",
		PingInterval:        30 * time.Millisecond,
		PingTimeout:         20 * time.Millisecond,
		SuspicionMultiplier: 4,
		Fanout:              2,
	}
	b, err := New(Config{Addr: "127.0.0.1:0", Cluster: clusterCfg})
	if err != nil {
		t.Fatal(err)
	}
	if b.cfg.ClusterBackend == nil {
		t.Fatal("Cluster.Enabled installed no backend")
	}
	// The default is the gossip backend. It is the only built-in backend with
	// both a member view and a presence view (the TCP mesh has no presence, and
	// the Redis backend is never selected by Cluster.Enabled alone), which is
	// what makes it observable from this package after the move.
	if _, ok := b.cfg.ClusterBackend.(MemberView); !ok {
		t.Fatalf("default backend %T has no member view", b.cfg.ClusterBackend)
	}
	if _, ok := b.cfg.ClusterBackend.(PresenceView); !ok {
		t.Fatalf("default backend %T has no presence view", b.cfg.ClusterBackend)
	}
	if b.nodeID != "cluster-node" {
		t.Fatalf("node id = %q, want Cluster.Node", b.nodeID)
	}
	if b.namespace != "tenant-x" {
		t.Fatalf("namespace = %q, want Cluster.Namespace", b.namespace)
	}
	gc := defaultGossipConfig(clusterCfg, b.namespace, nil, nil)
	if gc.Addr != addr || len(gc.Seeds) != 1 || gc.Seeds[0] != "127.0.0.1:1" {
		t.Fatalf("gossip config = %+v", gc)
	}
	if gc.PingInterval != 30*time.Millisecond || gc.PingTimeout != 20*time.Millisecond ||
		gc.SuspicionMultiplier != 4 || gc.Fanout != 2 {
		t.Fatalf("gossip timers = %+v", gc)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if members := b.cfg.ClusterBackend.(MemberView).Members(); len(members) != 0 {
		t.Fatalf("single-node members = %+v, want none", members)
	}
	if err := b.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	// An explicit backend wins over Enabled.
	explicit := newFakeCluster()
	b2, err := New(Config{Addr: "127.0.0.1:0", Cluster: ClusterConfig{Enabled: true, Addr: addr}, ClusterBackend: explicit})
	if err != nil {
		t.Fatal(err)
	}
	if b2.cfg.ClusterBackend != explicit {
		t.Fatalf("explicit backend was replaced by %T", b2.cfg.ClusterBackend)
	}

	// A disabled cluster without a backend installs nothing.
	b3, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if b3.cfg.ClusterBackend != nil {
		t.Fatalf("disabled cluster installed %T", b3.cfg.ClusterBackend)
	}
}

// TestClusterConfigEnabledRequiresAddr pins the one required value of the default
// backend: a cluster with no listen address cannot be advertised to peers, so
// Broker.Start refuses it instead of silently serving unclustered traffic.
func TestClusterConfigEnabledRequiresAddr(t *testing.T) {
	b, err := New(Config{Addr: "127.0.0.1:0", Cluster: ClusterConfig{Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	err = b.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "gossip cluster Addr is required") {
		t.Fatalf("Start = %v, want the missing cluster Addr error", err)
	}
}

// TestClusterEnabledGossipEndToEnd runs the default selection through two brokers:
// setting only Cluster.Enabled/Addr/Seeds forms a cluster and a publish on one node
// reaches a subscriber on the other.
func TestClusterEnabledGossipEndToEnd(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	a, err := New(Config{Addr: "127.0.0.1:0", NodeID: "a", Cluster: ClusterConfig{
		Enabled: true, Addr: aAddr, Seeds: []string{bAddr},
	}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{Addr: "127.0.0.1:0", NodeID: "b", Cluster: ClusterConfig{
		Enabled: true, Addr: bAddr, Seeds: []string{aAddr},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if a.cfg.ClusterBackend == nil || b.cfg.ClusterBackend == nil {
		t.Fatalf("Cluster.Enabled installed %T and %T", a.cfg.ClusterBackend, b.cfg.ClusterBackend)
	}
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	gcsA, gcsB := a.cfg.ClusterBackend, b.cfg.ClusterBackend
	waitClusterMembers(t, gcsA, "b")
	waitClusterMembers(t, gcsB, "a")
	waitClusterLink(t, gcsA, "b")
	waitClusterLink(t, gcsB, "a")
	// The publish below is a one-shot gossip event: wait for the startup link
	// reconciliation to finish, otherwise it can be written into a link that is
	// being superseded and dropped.
	waitClusterSettled(t, gcsA, gcsB)

	sub := dialTestClient(t, b.Addr(), "default-sub")
	body := []byte{0, 1}
	body = appendString(body, "default/x")
	body = append(body, 0)
	sub.writePacket(0x82, body)
	if h, p, err := sub.readPacket(); err != nil || h>>4 != 9 || len(p) < 3 || p[2] != 0 {
		t.Fatalf("suback: h=%x p=%v err=%v", h, p, err)
	}
	// The SUBACK proves b's broker has the subscription, not that a's view of b's
	// summary does: the summary update travels as gossip, and a directed routing
	// decision made before it arrives sees a fresh, empty summary and skips b. Wait
	// for the advertisement the publish will be routed by.
	waitClusterSummary(t, gcsA, "b", "default/x")

	pub := dialTestClient(t, a.Addr(), "default-pub")
	pubBody := appendString(nil, "default/x")
	pubBody = append(pubBody, []byte("hello")...)
	pub.writePacket(0x30, pubBody)

	// A fresh deadline for the read that proves the routing: the handshake deadline
	// of dialTestClient is not a bound on the whole test.
	sub.armDeadline(testClientIOTimeout)
	h, p, err := sub.readPacket()
	if err != nil || h>>4 != 3 {
		t.Fatalf("publish: h=%x err=%v", h, err)
	}
	topic, rest, err := readString(p)
	if err != nil || topic != "default/x" || string(rest) != "hello" {
		t.Fatalf("publish topic=%q payload=%q err=%v", topic, rest, err)
	}
}
