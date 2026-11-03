//go:build integration

package engine

// The cluster × persistence matrix, over real backends.
//
// The rule these cases pin (docs/PERSISTENCE.md, "A cluster does not pick a
// store"): the cluster shares routing, events and retained replication, and it
// never carries sessions; whether a session or a retained message survives a node
// restart is decided by Config.Store alone. So each case runs the same scenario
// against every backend and lets the backend decide the answer:
//
//   - a retained publish reaches the other node whatever the store is, because the
//     cluster replicates it — but only a shared store brings it back after a restart;
//   - a session is resumed on another node only when the store is shared.
//
// Everything here needs Docker, so it lives behind -tags=integration: `go test
// ./...` stays Docker-free, the same rule gjob's mysqlstore tests follow.

import (
	"context"
	"testing"
	"time"

	olricdb "github.com/olric-data/olric"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/maxid/gcache"
	gcacheolric "github.com/maxid/gcache/olric"
	gcacheredis "github.com/maxid/gcache/redis"
	"github.com/maxid/gnety/codec/mqtt"
)

// negativeWindow bounds the reads that assert a delivery does *not* arrive. It is
// long enough that a delivery through the loopback cluster would have landed.
const negativeWindow = 300 * time.Millisecond

// backend is one row of the matrix.
type backend struct {
	// name is the Config.Store value the row stands for.
	name string
	// shared reports whether the store is one tier every node sees and that
	// survives a restart — i.e. whether the documented promise ("a session resumes
	// on another node") applies to this row.
	shared bool
	// open returns the store for node i. It is called once per node, so a shared row
	// yields one client per node of the same tier, exactly as a deployment holds.
	// A nil store is the documented default: the broker installs gcache's in-process
	// memory store, node-local.
	open func(tb testing.TB, i int) gcache.RemoteStore
}

// matrix starts the containers once and returns every row. Subtests share them: a
// row's store clients are per test, the tier is per run.
func matrix(tb testing.TB) []backend {
	tb.Helper()
	redisAddr := startContainer(tb, "redis:7-alpine", "6379/tcp", wait.ForListeningPort("6379/tcp"))
	olricAddr := startContainer(tb, "ghcr.io/olric-data/olric:latest", "3320/tcp",
		wait.ForListeningPort("3320/tcp"))

	return []backend{
		{name: "memory", shared: false, open: func(testing.TB, int) gcache.RemoteStore { return nil }},
		redisBackend(redisAddr),
		{name: "olric", shared: true, open: func(tb testing.TB, _ int) gcache.RemoteStore {
			client, err := olricdb.NewClusterClient([]string{olricAddr})
			if err != nil {
				tb.Fatalf("olric client: %v", err)
			}
			tb.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = client.Close(ctx)
			})
			dmap, err := client.NewDMap("gmqx-matrix")
			if err != nil {
				tb.Fatalf("olric dmap: %v", err)
			}
			store, err := gcacheolric.NewStore(gcacheolric.FromDMap(dmap))
			if err != nil {
				tb.Fatalf("olric store: %v", err)
			}
			return store
		}},
	}
}

// redisBackend is the gcache/redis row against the container at addr: one client
// per node, the way a deployment holds it.
func redisBackend(addr string) backend {
	return backend{name: "redis", shared: true, open: func(tb testing.TB, _ int) gcache.RemoteStore {
		client := goredis.NewClient(&goredis.Options{Addr: addr})
		tb.Cleanup(func() { _ = client.Close() })
		store, err := gcacheredis.NewStore(client)
		if err != nil {
			tb.Fatalf("redis store: %v", err)
		}
		return store
	}}
}

// startContainer runs img with one exposed port and returns its "host:port".
func startContainer(tb testing.TB, img, port string, ready wait.Strategy) string {
	tb.Helper()
	tb.Logf("starting %s", img)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        img,
			ExposedPorts: []string{port},
			WaitingFor:   ready,
		},
		Started: true,
	})
	if err != nil {
		tb.Fatalf("start %s: %v", img, err)
	}
	testcontainers.CleanupContainer(tb, ctr)

	endpoint, err := ctr.Endpoint(ctx, "")
	if err != nil {
		tb.Fatalf("%s endpoint: %v", img, err)
	}
	return endpoint
}

// tcpPair starts two brokers with a TCP mesh between them, each with the store its
// node of this backend gets. tune, when given, adjusts both Configs before the
// brokers start.
func tcpPair(t *testing.T, b backend, tune ...func(*Config)) (*Broker, *Broker) {
	t.Helper()

	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(TCPClusterConfig{Addr: aAddr, Seeds: []string{bAddr}, ReconnectInterval: 20 * time.Millisecond})
	cb := NewTCPCluster(TCPClusterConfig{Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond})

	cfgA := Config{NodeID: "a", ClusterBackend: ca, Store: b.open(t, 0), Logger: discardLogger()}
	cfgB := Config{NodeID: "b", ClusterBackend: cb, Store: b.open(t, 1), Logger: discardLogger()}
	for _, apply := range tune {
		apply(&cfgA)
		apply(&cfgB)
	}
	a := startBroker(t, cfgA)
	bn := startBroker(t, cfgB)

	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)
	waitClusterSettled(t, ca, cb)
	return a, bn
}

// TestClusterRetainedMatrix: within one cluster a retained publish reaches every
// node whatever the store is; after a restart only a shared store has it back.
func TestClusterRetainedMatrix(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	for _, b := range matrix(t) {
		t.Run(b.name, func(t *testing.T) {
			a, bn := tcpPair(t, b)

			pub := dialTestClient(t, a.Addr(), "matrix-retain-pub")
			publishRetained(t, pub, "matrix/state", "on")
			// The replication half does not depend on the backend: the cluster
			// broadcasts the retained publish regardless of the store.
			waitRetained(t, bn, "matrix/state", "on")

			// A third broker is a restart as far as the store is concerned: it loads
			// what the tier holds and has no other way to learn about the message.
			restarted := startBroker(t, Config{NodeID: "c", Store: b.open(t, 2), Logger: discardLogger()})
			sub := dialTestClient(t, restarted.Addr(), "matrix-retain-sub")
			subscribeTestClient(t, sub, "matrix/#")

			if !b.shared {
				expectNoPublish(t, sub, negativeWindow, "retained replay from a node-local store")
				return
			}
			h, topic, payload := readPublish(t, sub)
			if h&0x01 == 0 {
				t.Fatal("retained replay without the retain flag")
			}
			if topic != "matrix/state" || string(payload) != "on" {
				t.Fatalf("replayed retained = %q %q", topic, payload)
			}
		})
	}
}

// TestClusterSessionMatrix: a session stored by one node is resumed by another only
// when the store is shared. The row that is not shared is the documented default —
// a clustered deployment that leaves Config.Store unset.
func TestClusterSessionMatrix(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	for _, b := range matrix(t) {
		t.Run(b.name, func(t *testing.T) {
			a, bn := tcpPair(t, b)

			offlineSessionOn(t, a, "roamer", "roam/#", 1)

			pub, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "matrix-roam-pub", clean: true})
			pub.mustPublish(t, confPublish{topic: "roam/one", payload: "one", qos: 1, pid: 1})
			pub.mustPublish(t, confPublish{topic: "roam/two", payload: "two", qos: 1, pid: 2})

			back, ack := mustConnect(t, bn.Addr(), confConnect{
				version: mqtt.Version5, clientID: "roamer", clean: false,
				props: mqtt.Properties{SessionExpiryInterval: 3600},
			})

			if !b.shared {
				if ack.SessionPresent {
					t.Fatal("the node-local default resumed a session another node stored")
				}
				expectNoConfPublish(t, back, negativeWindow)
				return
			}

			if !ack.SessionPresent {
				t.Fatalf("node b did not resume the session node a stored in %s", b.name)
			}
			first := expectPublish(t, back)
			second := expectPublish(t, back)
			if first.Topic != "roam/one" || string(first.Payload) != "one" {
				t.Fatalf("first queued delivery = %q %q", first.Topic, first.Payload)
			}
			if second.Topic != "roam/two" || string(second.Payload) != "two" {
				t.Fatalf("second queued delivery = %q %q", second.Topic, second.Payload)
			}
			back.ack(first.PacketID)
			back.ack(second.PacketID)
		})
	}
}

// TestGossipClusterSharedSessionSmoke is the one gossip case: the default backend
// carries a session to another node as long as the store is shared, which is the
// property a deployment that changes only the backend must not lose.
func TestGossipClusterSharedSessionSmoke(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	redisAddr := startContainer(t, "redis:7-alpine", "6379/tcp", wait.ForListeningPort("6379/tcp"))
	store := func(tb testing.TB) gcache.RemoteStore {
		client := goredis.NewClient(&goredis.Options{Addr: redisAddr})
		tb.Cleanup(func() { _ = client.Close() })
		s, err := gcacheredis.NewStore(client)
		if err != nil {
			tb.Fatalf("redis store: %v", err)
		}
		return s
	}

	aAddr, bAddr := freeAddr(t), freeAddr(t)
	// The broker starts its backend, exactly as a deployment's Config does: only the
	// TCP cases above hand the test a separately started cluster.
	ca := NewGossipCluster(gossipTestConfig(aAddr, bAddr))
	cb := NewGossipCluster(gossipTestConfig(bAddr, aAddr))
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca, Store: store(t), Logger: discardLogger()})
	bn := startBroker(t, Config{NodeID: "b", ClusterBackend: cb, Store: store(t), Logger: discardLogger()})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)

	offlineSessionOn(t, a, "gossip-roamer", "roam/#", 1)
	pub, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "gossip-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "roam/one", payload: "one", qos: 1, pid: 1})

	back, ack := mustConnect(t, bn.Addr(), confConnect{
		version: mqtt.Version5, clientID: "gossip-roamer", clean: false,
		props: mqtt.Properties{SessionExpiryInterval: 3600},
	})
	if !ack.SessionPresent {
		t.Fatal("the gossip backend did not resume a session stored over a shared store")
	}
	first := expectPublish(t, back)
	if first.Topic != "roam/one" || string(first.Payload) != "one" {
		t.Fatalf("queued delivery = %q %q", first.Topic, first.Payload)
	}
	back.ack(first.PacketID)
}

// expectNoConfPublish is expectNoPublish for the conformance client: the read has to
// fail on its deadline rather than deliver a packet.
func expectNoConfPublish(t *testing.T, c *confClient, window time.Duration) {
	t.Helper()
	if msg, err := c.readFor(time.Now().Add(window)); err == nil {
		t.Fatalf("unexpected %T, want nothing", msg)
	}
}
