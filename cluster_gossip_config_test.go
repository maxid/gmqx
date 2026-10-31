package gmqx

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestGossipConfigValidation is the table of configuration mistakes Start refuses.
// Zero selects a documented default everywhere, so only missing required values and
// negative values are errors.
func TestGossipConfigValidation(t *testing.T) {
	base := func() GossipClusterConfig {
		return GossipClusterConfig{Addr: "127.0.0.1:7946"}
	}
	cases := []struct {
		name   string
		nodeID string
		mutate func(*GossipClusterConfig)
		want   string
	}{
		{name: "valid", nodeID: "n1"},
		{name: "zero timers select defaults", nodeID: "n1"},
		{name: "missing node id", nodeID: "", want: "node id is required"},
		{
			name: "missing addr", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.Addr = "" },
			want:   "Addr is required",
		},
		{
			name: "ephemeral addr", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.Addr = "127.0.0.1:0" },
			want:   "Addr must be explicit",
		},
		{
			name: "negative ping interval", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.PingInterval = -time.Second },
			want:   "PingInterval must not be negative",
		},
		{
			name: "negative ping timeout", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.PingTimeout = -time.Second },
			want:   "PingTimeout must not be negative",
		},
		{
			name: "negative suspicion multiplier", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.SuspicionMultiplier = -1 },
			want:   "SuspicionMultiplier must not be negative",
		},
		{
			name: "negative dead timeout", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.DeadTimeout = -time.Second },
			want:   "DeadTimeout must not be negative",
		},
		{
			name: "negative fanout", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.Fanout = -1 },
			want:   "Fanout must not be negative",
		},
		{
			name: "negative reconnect interval", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.ReconnectInterval = -time.Second },
			want:   "ReconnectInterval must not be negative",
		},
		{
			name: "negative dial timeout", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.DialTimeout = -time.Second },
			want:   "DialTimeout must not be negative",
		},
		{
			name: "negative dedup size", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.DedupSize = -1 },
			want:   "DedupSize must not be negative",
		},
		{
			name: "negative dedup ttl", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.DedupTTL = -time.Second },
			want:   "DedupTTL must not be negative",
		},
		{
			name: "negative summary interval", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.SummaryInterval = -time.Second },
			want:   "SummaryInterval must not be negative",
		},
		{
			name: "negative summary cap", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.MaxSummaryFilters = -1 },
			want:   "MaxSummaryFilters must not be negative",
		},
		{
			name: "negative presence ttl", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.PresenceTTL = -time.Second },
			want:   "PresenceTTL must not be negative",
		},
		{
			name: "presence ttl below the ping interval", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.PresenceTTL = time.Millisecond },
			want:   "PresenceTTL 1ms must be at least PingInterval",
		},
		{
			name: "negative gossip delta", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.MaxGossipDelta = -1 },
			want:   "MaxGossipDelta must not be negative",
		},
		{
			name: "negative presence delta", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.MaxPresenceDelta = -1 },
			want:   "MaxPresenceDelta must not be negative",
		},
		{
			name: "negative presence entries", nodeID: "n1",
			mutate: func(c *GossipClusterConfig) { c.MaxPresenceEntries = -1 },
			want:   "MaxPresenceEntries must not be negative",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			if tc.mutate != nil {
				tc.mutate(&cfg)
			}
			err := newGossipCluster(cfg).validate(tc.nodeID)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("validate = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate = %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

// TestClusterConfigSelectsGossip pins the default-selection rule and the mapping
// from ClusterConfig onto the gossip backend: Cluster.Enabled with no explicit
// backend creates the gossip backend, its fields reach the backend, and an explicit
// ClusterBackend always wins.
func TestClusterConfigSelectsGossip(t *testing.T) {
	addr := freeAddr(t)
	cfg := Config{
		Addr: "127.0.0.1:0",
		Cluster: ClusterConfig{
			Enabled:             true,
			Addr:                addr,
			Seeds:               []string{"127.0.0.1:1"},
			Node:                "cluster-node",
			Namespace:           "tenant-x",
			PingInterval:        30 * time.Millisecond,
			PingTimeout:         20 * time.Millisecond,
			SuspicionMultiplier: 4,
			Fanout:              2,
		},
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	gc, ok := b.cfg.ClusterBackend.(*gossipCluster)
	if !ok {
		t.Fatalf("default backend = %T, want *gossipCluster", b.cfg.ClusterBackend)
	}
	if b.nodeID != "cluster-node" {
		t.Fatalf("node id = %q, want Cluster.Node", b.nodeID)
	}
	if b.namespace != "tenant-x" {
		t.Fatalf("namespace = %q, want Cluster.Namespace", b.namespace)
	}
	if gc.cfg.Addr != addr || len(gc.cfg.Seeds) != 1 || gc.cfg.Seeds[0] != "127.0.0.1:1" {
		t.Fatalf("gossip config = %+v", gc.cfg)
	}
	if gc.cfg.PingInterval != 30*time.Millisecond || gc.cfg.PingTimeout != 20*time.Millisecond ||
		gc.cfg.SuspicionMultiplier != 4 || gc.cfg.Fanout != 2 {
		t.Fatalf("gossip timers = %+v", gc.cfg)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if members := gc.Members(); len(members) != 0 {
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
	if _, ok := a.cfg.ClusterBackend.(*gossipCluster); !ok {
		t.Fatalf("a backend = %T", a.cfg.ClusterBackend)
	}
	if _, ok := b.cfg.ClusterBackend.(*gossipCluster); !ok {
		t.Fatalf("b backend = %T", b.cfg.ClusterBackend)
	}
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	waitGossipMembers(t, a.cfg.ClusterBackend.(*gossipCluster), "b")
	waitGossipMembers(t, b.cfg.ClusterBackend.(*gossipCluster), "a")
	waitGossipLink(t, a.cfg.ClusterBackend.(*gossipCluster), "b")
	waitGossipLink(t, b.cfg.ClusterBackend.(*gossipCluster), "a")

	sub := dialTestClient(t, b.Addr(), "default-sub")
	body := []byte{0, 1}
	body = appendString(body, "default/x")
	body = append(body, 0)
	sub.writePacket(0x82, body)
	if h, p, err := sub.readPacket(); err != nil || h>>4 != 9 || len(p) < 3 || p[2] != 0 {
		t.Fatalf("suback: h=%x p=%v err=%v", h, p, err)
	}

	pub := dialTestClient(t, a.Addr(), "default-pub")
	pubBody := appendString(nil, "default/x")
	pubBody = append(pubBody, []byte("hello")...)
	pub.writePacket(0x30, pubBody)

	h, p, err := sub.readPacket()
	if err != nil || h>>4 != 3 {
		t.Fatalf("publish: h=%x err=%v", h, err)
	}
	topic, rest, err := readString(p)
	if err != nil || topic != "default/x" || string(rest) != "hello" {
		t.Fatalf("publish topic=%q payload=%q err=%v", topic, rest, err)
	}
}
