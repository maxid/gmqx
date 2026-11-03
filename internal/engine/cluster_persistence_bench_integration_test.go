//go:build integration

package engine

// What the persistence backends cost, in a clustered broker.
//
// Two paths touch the store while a cluster is running, and these are the two the
// suite measures:
//
//   - a retained publish, which the publishing node writes through to the store and
//     broadcasts to every peer, and which the peer also writes;
//   - a session that moves between nodes, which is one store write on the node it
//     leaves and one take (read + delete) on the node it arrives at.
//
// A plain publish is not measured against a backend: it never reaches the store, so
// a benchmark of it would compare the cluster backends' routing, not persistence.
// The numbers belong next to the shared-backend matrix in docs/PERFORMANCE.md;
// containers are on the same host and the Olric image runs under emulation on an
// arm64 machine, so read them as relative costs, not absolutes.

import (
	"net"
	"testing"
	"time"
)

// benchClusterPair is tcpPair for the benchmark goroutine: no *testing.T, and the
// brokers are stopped when the benchmark ends.
func benchClusterPair(b *testing.B, row backend) (*Broker, *Broker) {
	b.Helper()

	aAddr, bAddr := freeBenchAddr(b), freeBenchAddr(b)
	ca := NewTCPCluster(TCPClusterConfig{Addr: aAddr, Seeds: []string{bAddr}, ReconnectInterval: 20 * time.Millisecond, Logger: benchLogger})
	cb := NewTCPCluster(TCPClusterConfig{Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond, Logger: benchLogger})

	// Moving a session between nodes is a takeover, and the benchmark does it far
	// faster than the 30 s anti-flap window allows: KICK with the window off is the
	// configuration a deployment of this shape runs.
	cfgA := Config{NodeID: "a", ClusterBackend: ca, Store: row.open(b, 0), ConnectMode: ConnectModeKick, NotKickSeconds: -1}
	cfgB := Config{NodeID: "b", ClusterBackend: cb, Store: row.open(b, 1), ConnectMode: ConnectModeKick, NotKickSeconds: -1}
	a := startBenchBroker(b, cfgA)
	bn := startBenchBroker(b, cfgB)
	b.Cleanup(func() {
		stopBenchBroker(b, a)
		stopBenchBroker(b, bn)
	})
	waitPeersB(b, ca, 1)
	waitPeersB(b, cb, 1)
	return a, bn
}

// BenchmarkRetainedPublishMatrix measures one retained publish, round trip: the
// publisher's write, the store write-through, the cluster broadcast and the
// delivery to a subscriber on the other node. The first case is the single-node
// baseline, the three after it are the cluster rows of the persistence matrix.
func BenchmarkRetainedPublishMatrix(b *testing.B) {
	b.Run("single-node", func(b *testing.B) {
		broker := startBenchBroker(b, Config{})
		defer stopBenchBroker(b, broker)
		benchRetainedPublish(b, broker.Addr(), broker.Addr())
	})

	rows := matrix(b)
	for _, row := range rows {
		b.Run("cluster/"+row.name, func(b *testing.B) {
			a, bn := benchClusterPair(b, row)
			benchRetainedPublish(b, a.Addr(), bn.Addr())
		})
	}
}

// benchRetainedPublish publishes retained on pubAddr and reads the delivery on the
// subscriber at subAddr, so one iteration covers both ends.
func benchRetainedPublish(b *testing.B, pubAddr, subAddr string) {
	b.Helper()
	const topic = "bench/retained"

	sub := dialBench(b, subAddr, "bench-retain-sub")
	defer sub.close()
	if err := sub.subscribe(1, []string{topic}, 0); err != nil {
		b.Fatalf("subscribe: %v", err)
	}
	pub := dialBench(b, pubAddr, "bench-retain-pub")
	defer pub.close()

	payload := []byte("0123456789abcdef")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.sendPublish(topic, payload, 0, 0, true); err != nil {
			b.Fatalf("publish: %v", err)
		}
		header, body, err := sub.readFrame()
		if err != nil {
			b.Fatalf("delivery: %v", err)
		}
		if header>>4 != 3 || len(body) == 0 {
			b.Fatalf("delivery: header %#x body %x", header, body)
		}
	}
}

// BenchmarkSessionResumeMatrix measures one session move: a persistent CONNECT and
// SUBSCRIBE on node a, a disconnect (the store write), then a CONNECT with the same
// client id on node b (the take) and a disconnect (the next write). The session
// ping-pongs, so every iteration exercises one write, one take and the cluster
// bookkeeping around them.
func BenchmarkSessionResumeMatrix(b *testing.B) {
	for _, row := range matrix(b) {
		b.Run("cluster/"+row.name, func(b *testing.B) {
			a, bn := benchClusterPair(b, row)
			const topic = "bench/roam"

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				from, to := a.Addr(), bn.Addr()
				if i%2 == 1 {
					from, to = to, from
				}
				moveSession(b, from, to, topic)
			}
		})
	}
}

// moveSession connects a persistent client to from, subscribes, disconnects, then
// reconnects the same client id on to, and disconnects again.
func moveSession(b *testing.B, from, to, topic string) {
	b.Helper()
	const clientID = "bench-roamer"

	c, err := dialWire(from, clientID, false)
	if err != nil {
		b.Fatalf("connect %s: %v", from, err)
	}
	if err := c.subscribe(1, []string{topic}, 0); err != nil {
		b.Fatalf("subscribe: %v", err)
	}
	c.close()

	back, err := dialWire(to, clientID, false)
	if err != nil {
		b.Fatalf("reconnect %s: %v", to, err)
	}
	back.close()
}

// freeBenchAddr is freeAddr for the benchmark goroutine: reserve a port and hand it
// back, so the backend can bind it.
func freeBenchAddr(b *testing.B) string {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// waitPeersB is waitPeers for the benchmark goroutine, through the same public
// observation views a consumer has.
func waitPeersB(b *testing.B, c Cluster, want int) {
	b.Helper()
	links, ok := c.(LinkView)
	if !ok {
		b.Fatalf("%T does not implement LinkView", c)
	}
	members, ok := c.(MemberView)
	if !ok {
		b.Fatalf("%T does not implement MemberView", c)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(links.Links()) >= want && len(members.Members()) >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	b.Fatalf("cluster never saw %d peer(s)", want)
}
