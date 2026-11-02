package gmqx

// Wait policy for this file (and the rule a future change must keep): every wait is
// on an observable cluster condition with a generous ceiling (tcpWaitCeiling, 10 s).
// Two things are deliberately not waited on with a sleep:
//
//   - the link set. Two nodes that dial each other collapse to one connection; the
//     link that loses is dropped just after startup, and a publish written into it is
//     lost (the TCP cluster does not retry a one-shot spread).
//     TestTCPClusterBrokerCrossNodePublish therefore waits for the link set to settle
//     before it publishes.
//   - the socket read. dialTestClient fixes one absolute deadline for the handshake;
//     a test that waits on cluster state first re-arms it (armDeadline) so the wait is
//     not charged to the socket timeout.

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

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

// waitPeers waits until want links exist and the handshake admitted the same
// number of members: a link alone is not yet routable.
func waitPeers(t *testing.T, c Cluster, want int) {
	t.Helper()
	deadline := time.Now().Add(tcpWaitCeiling)
	for time.Now().Before(deadline) {
		if links, members := len(clusterLinks(t, c)), len(clusterMembers(t, c)); links >= want && members >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("cluster peers = %d (%d members), want %d", len(clusterLinks(t, c)), len(clusterMembers(t, c)), want)
}

// waitTCPSummary waits until this node's view of a peer advertises the filter. A
// publication is routed to a peer only when that peer's advertised summary matches or
// when the summary is unknown or stale; the advertisement of a fresh subscription
// reaches the publisher asynchronously, so a test that publishes right after a SUBACK
// waits for it instead of assuming the cluster already knows.
func waitTCPSummary(t *testing.T, c Cluster, nodeID, want string) {
	t.Helper()
	deadline := time.Now().Add(tcpWaitCeiling)
	var got []string
	for time.Now().Before(deadline) {
		filters, known := clusterMemberSummary(t, c, nodeID)
		if known {
			got = filters
			for _, f := range filters {
				if f == want {
					return
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("tcp summary of %s = %v, want %q", nodeID, got, want)
}

func newTCPClusterPair(t *testing.T) (Cluster, Cluster, *recClusterHandler, *recClusterHandler) {
	t.Helper()
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(TCPClusterConfig{Addr: aAddr, Seeds: []string{bAddr}, ReconnectInterval: 20 * time.Millisecond})
	cb := NewTCPCluster(TCPClusterConfig{Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond})
	ha, hb := &recClusterHandler{}, &recClusterHandler{}
	if err := ca.Start(context.Background(), "a", ha); err != nil {
		t.Fatal(err)
	}
	if err := cb.Start(context.Background(), "b", hb); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ca.Stop(context.Background())
		_ = cb.Stop(context.Background())
	})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)
	return ca, cb, ha, hb
}

func TestTCPClusterCrossNodePublish(t *testing.T) {
	ca, _, _, hb := newTCPClusterPair(t)
	if err := ca.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, NodeID: "a", Topic: "t/x", Payload: []byte("remote"),
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := hb.counts(); n == 1 {
			hb.mu.Lock()
			got := hb.publish[0]
			hb.mu.Unlock()
			if got.Topic != "t/x" || string(got.Payload) != "remote" || got.NodeID != "a" {
				t.Fatalf("message = %+v", got)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("remote publish not delivered")
}

func TestTCPClusterCrossNodeKick(t *testing.T) {
	ca, _, _, hb := newTCPClusterPair(t)
	// The epoch of a kick travels as a plain JSON field, so a peer that predates it
	// ignores it and a peer that knows it can order the event.
	epoch := time.Now().Add(-time.Second)
	if err := ca.Spread(context.Background(), ClusterMessage{
		Type: ClusterKick, NodeID: "a", ClientID: "device-1", ConnectTime: epoch.UnixNano(),
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, n := hb.counts(); n == 1 {
			hb.mu.Lock()
			got := hb.kick[0]
			hb.mu.Unlock()
			if got.ClientID != "device-1" {
				t.Fatalf("kick = %+v", got)
			}
			if got.ConnectTime != epoch.UnixNano() {
				t.Fatalf("kick epoch = %d, want %d", got.ConnectTime, epoch.UnixNano())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("remote kick not delivered")
}

func TestTCPClusterBrokerCrossNodePublish(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(TCPClusterConfig{Addr: aAddr, Seeds: []string{bAddr}, ReconnectInterval: 20 * time.Millisecond})
	cb := NewTCPCluster(TCPClusterConfig{Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond})
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)
	waitClusterSettled(t, ca, cb)

	sub := dialTestClient(t, b.Addr(), "sub-b")
	body := []byte{0, 1}
	body = appendString(body, "cluster/tcp")
	body = append(body, 0)
	sub.writePacket(0x82, body)
	if h, p, err := sub.readPacket(); err != nil || h>>4 != 9 || len(p) < 3 || p[2] != 0 {
		t.Fatalf("suback: h=%x p=%v err=%v", h, p, err)
	}
	// The SUBACK proves b's broker has the subscription; a's directed routing needs
	// b's advertised summary, which arrives asynchronously. Publishing before it does
	// means a's view is fresh and empty, and b is skipped.
	waitTCPSummary(t, ca, "b", "cluster/tcp")

	pub := dialTestClient(t, a.Addr(), "pub-a")
	pubBody := appendString(nil, "cluster/tcp")
	pubBody = append(pubBody, []byte("remote")...)
	pub.writePacket(0x30, pubBody)

	// A fresh deadline for the read that proves the routing; the handshake deadline
	// is not a bound on the whole test.
	sub.armDeadline(testClientIOTimeout)
	h, p, err := sub.readPacket()
	if err != nil {
		t.Fatal(err)
	}
	if h>>4 != 3 {
		t.Fatalf("expected PUBLISH, got type %d", h>>4)
	}
	topic, rest, err := readString(p)
	if err != nil || topic != "cluster/tcp" || string(rest) != "remote" {
		t.Fatalf("topic=%q payload=%q err=%v", topic, rest, err)
	}
}

func TestTCPClusterReconnect(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(TCPClusterConfig{Addr: aAddr, Seeds: []string{bAddr}, ReconnectInterval: 20 * time.Millisecond})
	cb := NewTCPCluster(TCPClusterConfig{Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond})
	if err := ca.Start(context.Background(), "a", &recClusterHandler{}); err != nil {
		t.Fatal(err)
	}
	if err := cb.Start(context.Background(), "b", &recClusterHandler{}); err != nil {
		t.Fatal(err)
	}
	waitPeers(t, ca, 1)
	if err := cb.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ca.Stop(context.Background()) })

	// Wait for the dead peer to be noticed before restarting b.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(clusterLinks(t, ca)) != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if links := clusterLinks(t, ca); len(links) != 0 {
		t.Fatalf("dead peer was not removed: %v", links)
	}

	// Restart b on the same address; a's dial loop must reconnect.
	hb := &recClusterHandler{}
	cb2 := NewTCPCluster(TCPClusterConfig{Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond})
	start := time.Now()
	for i := 0; i < 20; i++ {
		if err := cb2.Start(context.Background(), "b", hb); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Cleanup(func() { _ = cb2.Stop(context.Background()) })
	waitPeers(t, ca, 1)

	if err := ca.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, NodeID: "a", Topic: "reconnect", Payload: []byte("ok"),
	}); err != nil {
		t.Fatal(err)
	}
	wait := time.Now().Add(3 * time.Second)
	for time.Now().Before(wait) {
		if n, _ := hb.counts(); n == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("publish not delivered after reconnect (waited %v)", time.Since(start))
}

// TestTCPClusterBrokerCrossNodeKick pins the connect-time kick end to end: the
// second node accepts the client id and the first node's connection is closed. The
// anti-flap window is disabled (NotKickSeconds -1) because the two connections here
// are milliseconds apart and the receive side now applies the same window: this
// case is about the kick crossing the mesh, not about its rate.
func TestTCPClusterBrokerCrossNodeKick(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(TCPClusterConfig{Addr: aAddr, Seeds: []string{bAddr}, ReconnectInterval: 20 * time.Millisecond})
	cb := NewTCPCluster(TCPClusterConfig{Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond})
	a := startBroker(t, Config{NodeID: "a", ClusterBackend: ca, ConnectMode: ConnectModeKick, NotKickSeconds: -1})
	b := startBroker(t, Config{NodeID: "b", ClusterBackend: cb, ConnectMode: ConnectModeKick, NotKickSeconds: -1})
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)

	remote := dialTestClient(t, b.Addr(), "dup")
	// The kick b just spread is delivered asynchronously: let it reach a before
	// the same client id connects there. A kick that is still in flight when a's
	// newer connection exists is refused by the epoch ordering (it is older than
	// that connection), so the two connections do not race.
	time.Sleep(50 * time.Millisecond)
	_ = dialTestClient(t, a.Addr(), "dup") // same client id on another node must take over

	_ = remote.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := remote.r.ReadByte(); err == nil {
		t.Fatal("expected remote session to be kicked")
	}
}
