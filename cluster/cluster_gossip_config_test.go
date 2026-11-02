package cluster

// Backend-level configuration validation. The broker-level mapping from the root
// package's ClusterConfig onto this configuration stays in the root package's
// cluster_config_test.go, because that is where Config and New live.

import (
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
