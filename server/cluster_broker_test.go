package server

// broker × cluster cases whose backends moved to gmqx/cluster
// (docs/design/source-layout.md §4.1): they start a real broker on a real backend,
// so they stay here and observe the backend only through the public views. The
// backend's own behaviour (key layout, TTLs, membership, dedup) is tested in that
// package.

import (
	"testing"
	"time"
)

// TestTCPClusterBrokerDirectedRouting checks the broker path: a publish is
// delivered to the node with a matching subscriber and not to a node without
// one.
func TestTCPClusterBrokerDirectedRouting(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(testClusterConfig(aAddr, []string{bAddr}))
	cb := NewTCPCluster(testClusterConfig(bAddr, []string{aAddr}))
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)
	waitClusterMembers(t, ca, "b")

	sub := dialTestClient(t, b.Addr(), "sub-b")
	subBody := []byte{0, 1}
	subBody = appendString(subBody, "plant/+/temp")
	subBody = append(subBody, 0)
	sub.writePacket(0x82, subBody)
	if h, p, err := sub.readPacket(); err != nil || h>>4 != 9 || len(p) < 3 || p[2] != 0 {
		t.Fatalf("suback: h=%x p=%v err=%v", h, p, err)
	}
	// The broker pushes its summary before it acknowledges the subscription.
	waitClusterSummary(t, ca, "b", "plant/+/temp")

	pub := dialTestClient(t, a.Addr(), "pub-a")
	pubBody := appendString(nil, "plant/1/temp")
	pubBody = append(pubBody, []byte("21")...)
	pub.writePacket(0x30, pubBody)

	h, p, err := sub.readPacket()
	if err != nil {
		t.Fatal(err)
	}
	if h>>4 != 3 {
		t.Fatalf("expected PUBLISH, got type %d", h>>4)
	}
	topic, rest, err := readString(p)
	if err != nil || topic != "plant/1/temp" || string(rest) != "21" {
		t.Fatalf("topic=%q payload=%q err=%v", topic, rest, err)
	}

	// A topic nobody subscribed to is not routed to b at all.
	_ = sub.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	pubBody = appendString(nil, "plant/1/hum")
	pubBody = append(pubBody, []byte("60")...)
	pub.writePacket(0x30, pubBody)
	if _, _, err := sub.readPacket(); err == nil {
		t.Fatal("unexpected publish for an unsubscribed topic")
	}
}

// TestRedisClusterBrokerCrossNodePublish runs the broker path end to end: a
// publish on one node reaches a subscriber on another node through Redis, with
// the subscription summary coming from the broker's own filter set.
func TestRedisClusterBrokerCrossNodePublish(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)

	ca := NewRedisCluster(cfg)
	cb := NewRedisCluster(cfg)
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb})

	sub := dialTestClient(t, b.Addr(), "sub-b")
	body := []byte{0, 1}
	body = appendString(body, "cluster/redis")
	body = append(body, 0)
	sub.writePacket(0x82, body)
	if h, p, err := sub.readPacket(); err != nil || h>>4 != 9 || len(p) < 3 || p[2] != 0 {
		t.Fatalf("suback: h=%x p=%v err=%v", h, p, err)
	}
	// The broker pushes its summary when the subscription is accepted; wait until
	// the publishing node routes by it instead of sleeping. The routing view is
	// the cache a spread reads, which is what the wait must observe.
	waitSummaryContains(t, "node a (redis)", func() ([]string, bool) { return clusterRoutedSummary(t, ca, "b") }, "cluster/redis")

	pub := dialTestClient(t, a.Addr(), "pub-a")
	pubBody := appendString(nil, "cluster/redis")
	pubBody = append(pubBody, []byte("remote")...)
	pub.writePacket(0x30, pubBody)

	h, p, err := sub.readPacket()
	if err != nil {
		t.Fatal(err)
	}
	if h>>4 != 3 {
		t.Fatalf("expected PUBLISH, got type %d", h>>4)
	}
	topic, rest, err := readString(p)
	if err != nil || topic != "cluster/redis" || string(rest) != "remote" {
		t.Fatalf("topic=%q payload=%q err=%v", topic, rest, err)
	}
}
