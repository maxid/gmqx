package gmqx

import (
	"net"
	"testing"
	"time"
)

// Retained replication is shared behaviour of the two backends: a retained
// publish is broadcast to every node so a subscriber that connects to another
// node later still receives it from that node's retained store. These tests
// drive the same scenario through the TCP mesh and the embedded-Redis pair, so a
// backend that stops storing remote retained publishes fails here.

// TestTCPClusterRetainedReplication checks retained replication on the TCP mesh.
func TestTCPClusterRetainedReplication(t *testing.T) {
	checkRetainedReplication(t, newTCPRetainedPair(t))
}

// TestRedisClusterRetainedReplication checks retained replication on the Redis
// backend.
func TestRedisClusterRetainedReplication(t *testing.T) {
	checkRetainedReplication(t, newRedisRetainedPair(t))
}

// clusterPair is two brokers of one backend. waitSummary blocks until the
// publishing node's routing view contains a filter the receiving node
// subscribed to; it is only needed to order the bounded-store assertion behind a
// barrier publish.
type clusterPair struct {
	a, b        *Broker
	waitSummary func(t *testing.T, filter string)
}

// checkRetainedReplication is the retained contract both backends must satisfy:
// a retained publish on a is stored on b and reaches a subscriber that joins b
// afterwards, an empty payload removes it on both, MaxRetainMessages bounds the
// receiving store, and the QoS and properties of the publish are kept.
func checkRetainedReplication(t *testing.T, p clusterPair) {
	t.Helper()
	pub := dialTestClient(t, p.a.Addr(), "retain-pub")

	// 1. A retained publish on node a is stored on node b. The subscriber that
	// joins b afterwards did not exist when the publish happened, so the value
	// can only come from b's own retained store.
	publishRetained(t, pub, "state/x", "on")
	if m := waitRetained(t, p.b, "state/x", "on"); m.qos != 0 {
		t.Fatalf("replicated retained QoS = %d, want 0", m.qos)
	}
	sub := dialTestClient(t, p.b.Addr(), "retain-sub")
	subscribeTestClient(t, sub, "state/#")
	h, topic, payload := readPublish(t, sub)
	if h&0x01 == 0 {
		t.Fatal("retained delivery without the retain flag")
	}
	if topic != "state/x" || string(payload) != "on" {
		t.Fatalf("retained delivery = %q %q", topic, payload)
	}

	// 2. An empty retained publish deletes the value on the node that received
	// it, not only on the publisher.
	publishRetained(t, pub, "state/x", "")
	waitRetainedGone(t, p.a, "state/x")
	waitRetainedGone(t, p.b, "state/x")

	// 3. MaxRetainMessages bounds the receiving store. The barrier publish is
	// ordered after the two retained publishes on the same event stream, so once
	// it arrives both were applied: the first filled the store, the second was
	// refused.
	barrier := dialTestClient(t, p.b.Addr(), "retain-barrier")
	subscribeTestClient(t, barrier, "barrier/#")
	p.waitSummary(t, "barrier/#")
	publishRetained(t, pub, "state/one", "1")
	publishRetained(t, pub, "state/two", "2")
	publishPlain(t, pub, "barrier/go", "done")
	if _, topic, payload := readPublish(t, barrier); topic != "barrier/go" || string(payload) != "done" {
		t.Fatalf("barrier delivery = %q %q", topic, payload)
	}
	if got := retainedCount(p.b); got != 1 {
		t.Fatalf("node %s retained %d topics, want 1 (MaxRetainMessages)", p.b.nodeID, got)
	}
	if _, ok := retainedSnapshot(p.b, "state/one"); !ok {
		t.Fatal("node b dropped the first retained topic instead of the second")
	}
	if _, ok := retainedSnapshot(p.b, "state/two"); ok {
		t.Fatal("node b stored a retained topic past MaxRetainMessages")
	}

	// 4. The QoS and the properties of the publish travel with the retained
	// state: b stores a QoS 1 v5 publish with its user property.
	publishRetained(t, pub, "state/one", "") // make room: b keeps one topic
	waitRetainedGone(t, p.b, "state/one")

	v5, r, reason := dialV5(t, p.a.Addr(), "retain-v5-pub", nil)
	defer v5.Close()
	if reason != 0 {
		t.Fatalf("v5 connack reason = %d", reason)
	}
	userProp := []byte{0x26, 0, 3, 'k', 'e', 'y', 0, 1, 'v'}
	publishV5Retained(t, v5, "state/v5", userProp, []byte("on"))
	if h, _, err := readPacketFrom(r); err != nil || h != 0x40 {
		t.Fatalf("v5 puback: h=%#x err=%v", h, err)
	}
	m := waitRetained(t, p.b, "state/v5", "on")
	if m.qos != 1 {
		t.Fatalf("replicated retained QoS = %d, want 1", m.qos)
	}
	if len(m.props.UserProperties) != 1 || m.props.UserProperties[0].Key != "key" || m.props.UserProperties[0].Value != "v" {
		t.Fatalf("replicated retained properties = %+v", m.props)
	}
}

// newTCPRetainedPair starts two brokers on a TCP mesh and returns them.
func newTCPRetainedPair(t *testing.T) clusterPair {
	t.Helper()
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(TCPClusterConfig{
		Addr: aAddr, Seeds: []string{bAddr}, ReconnectInterval: 20 * time.Millisecond, Logger: discardLogger(),
	})
	cb := NewTCPCluster(TCPClusterConfig{
		Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond, Logger: discardLogger(),
	})
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb, MaxRetainMessages: 1})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)
	tc := ca.(*tcpCluster)
	return clusterPair{
		a: a, b: b,
		waitSummary: func(t *testing.T, filter string) {
			t.Helper()
			waitSummaryContains(t, "node a (tcp)", func() ([]string, bool) { return tc.memberSummary("b") }, filter)
		},
	}
}

// newRedisRetainedPair starts two brokers on an embedded Redis and returns them.
func newRedisRetainedPair(t *testing.T) clusterPair {
	t.Helper()
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	ca := NewRedisCluster(cfg).(*redisCluster)
	cb := NewRedisCluster(cfg).(*redisCluster)
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb, MaxRetainMessages: 1})
	waitRedisView(t, ca, "b")
	return clusterPair{
		a: a, b: b,
		waitSummary: func(t *testing.T, filter string) {
			t.Helper()
			waitSummaryContains(t, "node a (redis)", func() ([]string, bool) { return ca.viewSummary("b") }, filter)
		},
	}
}

// waitSummaryContains waits until a node's routing view knows that a peer
// subscribed to filter.
func waitSummaryContains(t *testing.T, name string, view func() ([]string, bool), filter string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		filters, known := view()
		if known {
			got = filters
			for _, f := range filters {
				if f == filter {
					return
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s summary = %v, want it to contain %q", name, got, filter)
}

// retainedSnapshot returns the retained state of one topic.
func retainedSnapshot(b *Broker, topic string) (retainedMessage, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m, ok := b.retained[topic]
	return m, ok
}

// retainedCount returns how many topics the broker retains.
func retainedCount(b *Broker) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.retained)
}

// waitRetained waits until the broker holds payload for topic.
func waitRetained(t *testing.T, b *Broker, topic, payload string) retainedMessage {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m, ok := retainedSnapshot(b, topic); ok && string(m.payload) == payload {
			return m
		}
		time.Sleep(5 * time.Millisecond)
	}
	m, ok := retainedSnapshot(b, topic)
	t.Fatalf("node %s retained[%q] = (%+v, %t), want payload %q", b.nodeID, topic, m, ok, payload)
	return retainedMessage{}
}

// waitRetainedGone waits until the broker no longer retains topic.
func waitRetainedGone(t *testing.T, b *Broker, topic string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := retainedSnapshot(b, topic); !ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	m, _ := retainedSnapshot(b, topic)
	t.Fatalf("node %s still retains %q = %q", b.nodeID, topic, m.payload)
}

// publishRetained sends a QoS 0 PUBLISH with the retain flag.
func publishRetained(t *testing.T, c *testClient, topic, payload string) {
	t.Helper()
	body := appendString(nil, topic)
	body = append(body, []byte(payload)...)
	c.writePacket(0x31, body)
}

// publishPlain sends a QoS 0 PUBLISH without the retain flag.
func publishPlain(t *testing.T, c *testClient, topic, payload string) {
	t.Helper()
	body := appendString(nil, topic)
	body = append(body, []byte(payload)...)
	c.writePacket(0x30, body)
}

// publishV5Retained sends a QoS 1 MQTT 5 PUBLISH with the retain flag and a
// property block.
func publishV5Retained(t *testing.T, conn net.Conn, topic string, props, payload []byte) {
	t.Helper()
	body := appendString(nil, topic)
	body = append(body, 0, 7) // packet id
	body = append(body, prefixPropertyLength(props)...)
	body = append(body, payload...)
	frame := appendRemainingLength([]byte{0x33}, len(body)) // PUBLISH, QoS 1, retain
	frame = append(frame, body...)
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write retained v5 publish: %v", err)
	}
}

// subscribeTestClient subscribes a test client at QoS 0 and waits for the SUBACK.
func subscribeTestClient(t *testing.T, c *testClient, filter string) {
	t.Helper()
	body := []byte{0, 1}
	body = appendString(body, filter)
	body = append(body, 0)
	c.writePacket(0x82, body)
	if h, p, err := c.readPacket(); err != nil || h>>4 != 9 || len(p) < 3 || p[2] != 0 {
		t.Fatalf("suback for %q: h=%#x p=%v err=%v", filter, h, p, err)
	}
}

// readPublish reads one PUBLISH and returns its header, topic and payload.
func readPublish(t *testing.T, c *testClient) (byte, string, []byte) {
	t.Helper()
	h, p, err := c.readPacket()
	if err != nil {
		t.Fatalf("read publish: %v", err)
	}
	if h>>4 != 3 {
		t.Fatalf("expected PUBLISH, got type %d", h>>4)
	}
	topic, rest, err := readString(p)
	if err != nil {
		t.Fatalf("read publish topic: %v", err)
	}
	return h, topic, rest
}
