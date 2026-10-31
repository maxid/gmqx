package gmqx

// Kick ordering across nodes: a kick carries the epoch of the connection that
// produced it (ClusterMessage.ConnectTime) and the node that produced it, so a
// receiving node only closes a connection that is older than the one the peer
// actually replaced. A kick that is older than the connection it names, or that
// arrives inside the Config.NotKickSeconds anti-flap window of that connection, is
// refused and counted instead of closing a younger connection; equal epochs are
// broken by node id so both nodes agree; a kick without an epoch comes from a peer
// from before this change and keeps the pre-epoch behavior.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// recordingCluster is a cluster backend that keeps the events the broker spreads
// and lets a test deliver one to the broker as if a peer had sent it. It does not
// stamp Origin or Seq, so a test controls the epoch and the origin node exactly.
type recordingCluster struct {
	mu      sync.Mutex
	handler ClusterHandler
	spread  []ClusterMessage
}

func (c *recordingCluster) Start(_ context.Context, _ string, h ClusterHandler) error {
	c.mu.Lock()
	c.handler = h
	c.mu.Unlock()
	return nil
}

func (c *recordingCluster) Stop(context.Context) error { return nil }

func (c *recordingCluster) Spread(_ context.Context, msg ClusterMessage) error {
	c.mu.Lock()
	c.spread = append(c.spread, msg)
	c.mu.Unlock()
	return nil
}

// deliver hands msg to the broker as a received event.
func (c *recordingCluster) deliver(t *testing.T, msg ClusterMessage) {
	t.Helper()
	c.mu.Lock()
	h := c.handler
	c.mu.Unlock()
	if h == nil {
		t.Fatal("the broker did not start the cluster backend")
	}
	switch msg.Type {
	case ClusterKick:
		h.OnRemoteKick(msg)
	case ClusterPublish:
		h.OnRemotePublish(msg)
	default:
		t.Fatalf("recordingCluster cannot deliver %q", msg.Type)
	}
}

// lastSpread returns the last event the broker handed to the cluster.
func (c *recordingCluster) lastSpread(t *testing.T) ClusterMessage {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.spread) == 0 {
		t.Fatal("the broker spread no cluster event")
	}
	return c.spread[len(c.spread)-1]
}

// connects returns the kicks the broker spread, in order.
func (c *recordingCluster) kicks() []ClusterMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ClusterMessage, 0, len(c.spread))
	for _, msg := range c.spread {
		if msg.Type == ClusterKick {
			out = append(out, msg)
		}
	}
	return out
}

// connectTimeOf reads the registration time of a live local session, which is the
// reference point of the ordering rules.
func connectTimeOf(t *testing.T, b *Broker, clientID string) time.Time {
	t.Helper()
	b.mu.RLock()
	defer b.mu.RUnlock()
	c, ok := b.clients[clientID]
	if !ok {
		t.Fatalf("client %q is not connected", clientID)
	}
	return c.connectedAt
}

// kickFor builds the kick a peer would send for a connection that started at
// startedAt.
func kickFor(clientID, origin string, startedAt time.Time) ClusterMessage {
	return ClusterMessage{
		Type:        ClusterKick,
		NodeID:      origin,
		Origin:      origin,
		ClientID:    clientID,
		ConnectTime: startedAt.UnixNano(),
	}
}

// alive asserts that a connection still works, which is how "the kick was refused
// and the younger connection survived" is observed.
func alive(t *testing.T, c *confClient) {
	t.Helper()
	c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	readConf[*mqtt.Pingresp](t, c)
}

// TestClusterKickSpreadsEpoch pins what a CONNECT puts on the wire: the kick names
// the client id, the node that produced it and the epoch of the connection it came
// from — the fields the receiving node orders its own connection against.
func TestClusterKickSpreadsEpoch(t *testing.T) {
	backend := &recordingCluster{}
	b := startBroker(t, Config{NodeID: "a", ClusterBackend: backend, Logger: discardLogger()})

	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "epoch-wire", clean: true})
	alive(t, c)

	msg := backend.lastSpread(t)
	if msg.Type != ClusterKick || msg.ClientID != "epoch-wire" || msg.NodeID != "a" {
		t.Fatalf("spread kick = %+v, want a kick for epoch-wire from node a", msg)
	}
	if msg.ConnectTime == 0 {
		t.Fatal("the kick carries no epoch")
	}
	if want := connectTimeOf(t, b, "epoch-wire").UnixNano(); msg.ConnectTime != want {
		t.Fatalf("kick epoch = %d, want the connection's %d", msg.ConnectTime, want)
	}
}

// TestClusterKickOlderEpochIsRefused pins the ordering rule: a kick older than the
// connection it names is a leftover of a connection that this node already
// replaced, so it is refused, counted and logged instead of closing the younger
// connection.
func TestClusterKickOlderEpochIsRefused(t *testing.T) {
	metrics := newDeliveryMetrics()
	backend := &recordingCluster{}
	logger, logs := captureLogger()
	// NotKickSeconds -1 isolates the ordering rule from the anti-flap window, which
	// has a case of its own.
	b := startBroker(t, Config{
		NodeID: "b", ClusterBackend: backend, Metrics: metrics,
		Logger: logger, NotKickSeconds: -1,
	})

	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "older"})
	incumbent := connectTimeOf(t, b, "older")

	backend.deliver(t, kickFor("older", "a", incumbent.Add(-time.Millisecond)))

	alive(t, c)
	if got := metrics.count("cluster_kick_stale"); got == 0 {
		t.Fatal("the refused kick was not counted as stale")
	}
	if got := metrics.count("cluster_kick_rate_limit"); got != 0 {
		t.Fatalf("a stale kick was counted as a rate-limited one (%d)", got)
	}
	// A refusal is never silent: it is logged at Debug with the reason and the id.
	if out := logs.String(); !strings.Contains(out, "gmqx remote kick refused") || !strings.Contains(out, "cluster_kick_stale") {
		t.Fatalf("the refused kick was not logged at Debug:\n%s", out)
	}
}

// TestClusterKickNewerEpochDisplaces pins the case the event exists for: a kick
// newer than the connection here means the peer replaced it, so the connection is
// displaced with DISCONNECT 0x8E on MQTT 5 and a pending delayed will is cancelled
// rather than published — the session moved, it did not die.
func TestClusterKickNewerEpochDisplaces(t *testing.T) {
	backend := &recordingCluster{}
	b := startBroker(t, Config{
		NodeID: "a", ClusterBackend: backend, NotKickSeconds: -1,
		Logger: discardLogger(), unit: testSecond,
	})

	sub := willSubscriber(t, b, "epoch-will-sub", "will/#")
	victim, _ := mustConnect(t, b.Addr(), willConnect("epoch-newer", "will/epoch", "gone", 5, 100))
	incumbent := connectTimeOf(t, b, "epoch-newer")

	backend.deliver(t, kickFor("epoch-newer", "b", incumbent.Add(time.Millisecond)))

	expectDisconnect(t, victim, ReasonSessionTakenOver)
	waitDisconnected(t, b, "epoch-newer")
	// The session moved to another node: its will belongs to neither node, and the
	// displaced connection scheduled no delayed will on the way out.
	if n := pendingWillCount(b); n != 0 {
		t.Fatalf("the broker holds %d delayed wills after a displacing kick", n)
	}
	expectNoMessage(t, sub, 8*testSecond)
}

// TestClusterKickPendingWillCancelled pins the other half of the will rule: a kick
// for a client id whose session went offline here cancels the delayed will this
// node was holding, because the peer just accepted the session.
func TestClusterKickPendingWillCancelled(t *testing.T) {
	backend := &recordingCluster{}
	b := startBroker(t, Config{
		NodeID: "a", ClusterBackend: backend, NotKickSeconds: -1,
		Logger: discardLogger(), unit: testSecond,
	})

	sub := willSubscriber(t, b, "epoch-pending-sub", "will/#")
	client, _ := mustConnect(t, b.Addr(), willConnect("epoch-pending", "will/pending", "gone", 5, 100))
	_ = client.conn.Close() // an abrupt close, so the will is due after its delay
	waitPendingWill(t, b, 1)

	backend.deliver(t, kickFor("epoch-pending", "b", time.Now().Add(time.Second)))

	waitPendingWill(t, b, 0)
	expectNoMessage(t, sub, 8*testSecond)
}

// TestClusterKickEqualEpochTieBreaksByNodeID pins the tie rule: at the same epoch
// the connection on the greater node id wins, decided the same way on both nodes,
// so the two ends of one client id agree instead of both closing or both keeping.
func TestClusterKickEqualEpochTieBreaksByNodeID(t *testing.T) {
	// Node "a" holds a connection and receives a kick from the greater node id "b":
	// the kick wins and a's connection is displaced.
	backendA := &recordingCluster{}
	a := startBroker(t, Config{
		NodeID: "a", ClusterBackend: backendA, NotKickSeconds: -1, Logger: discardLogger(),
	})
	loser, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "tie"})
	incumbentA := connectTimeOf(t, a, "tie")
	backendA.deliver(t, kickFor("tie", "b", incumbentA))
	expectDisconnect(t, loser, ReasonSessionTakenOver)
	waitDisconnected(t, a, "tie")

	// Node "b" holds a connection and receives a kick from the smaller node id "a":
	// b's connection wins, so the kick is refused and b keeps it.
	metricsB := newDeliveryMetrics()
	backendB := &recordingCluster{}
	b := startBroker(t, Config{
		NodeID: "b", ClusterBackend: backendB, Metrics: metricsB,
		NotKickSeconds: -1, Logger: discardLogger(),
	})
	winner, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "tie"})
	incumbentB := connectTimeOf(t, b, "tie")
	backendB.deliver(t, kickFor("tie", "a", incumbentB))
	alive(t, winner)
	if got := metricsB.count("cluster_kick_stale"); got == 0 {
		t.Fatal("the losing side of an equal-epoch tie did not count the refusal")
	}
}

// TestClusterKickInsideAntiFlapWindowIsRefused pins the receive-side
// Config.NotKickSeconds: a peer kick that arrives while the connection it would
// close is younger than the window is refused, counted and logged, even when its
// epoch is newer than that connection. After the window the same kick displaces.
func TestClusterKickInsideAntiFlapWindowIsRefused(t *testing.T) {
	// One protocol second is 40 ms, so NotKickSeconds 1 is a 40 ms window.
	const window = 40 * time.Millisecond
	metrics := newDeliveryMetrics()
	backend := &recordingCluster{}
	b := startBroker(t, Config{
		NodeID: "b", ClusterBackend: backend, Metrics: metrics,
		NotKickSeconds: 1, unit: window, Logger: discardLogger(),
	})

	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "flap-peer"})
	incumbent := connectTimeOf(t, b, "flap-peer")
	kick := kickFor("flap-peer", "a", time.Now().Add(time.Second))

	backend.deliver(t, kick)
	alive(t, c)
	if got := metrics.count("cluster_kick_rate_limit"); got == 0 {
		t.Fatal("the kick refused by the anti-flap window was not counted")
	}

	// Outside the window the same kick displaces: the window protects a young
	// connection, it does not disable the event.
	time.Sleep(2 * window)
	kick.ConnectTime = time.Now().Add(time.Second).UnixNano()
	backend.deliver(t, kick)
	expectDisconnect(t, c, ReasonSessionTakenOver)
	if incumbent.Add(2 * window).After(time.Now()) {
		t.Fatal("the incumbent was still inside its window when the second kick arrived")
	}
}

// TestClusterKickWithoutEpochStillDisplaces pins the rolling-upgrade behavior: a
// peer from before kick epochs sends no timestamp, so its kick is applied
// unconditionally — the pre-epoch semantics — and counted as legacy, even inside
// the anti-flap window that protects the connection. A mixed cluster therefore
// degrades to the behavior it had, instead of refusing every kick.
func TestClusterKickWithoutEpochStillDisplaces(t *testing.T) {
	metrics := newDeliveryMetrics()
	backend := &recordingCluster{}
	b := startBroker(t, Config{
		NodeID: "b", ClusterBackend: backend, Metrics: metrics, Logger: discardLogger(),
	})

	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "legacy-kick"})
	// A legacy event: no ConnectTime, still a kick for this client id.
	backend.deliver(t, ClusterMessage{Type: ClusterKick, NodeID: "a", Origin: "a", ClientID: "legacy-kick"})

	expectDisconnect(t, c, ReasonSessionTakenOver)
	if got := metrics.count("cluster_kick_legacy"); got == 0 {
		t.Fatal("the legacy kick was not counted")
	}
	if got := metrics.count("cluster_kick_stale") + metrics.count("cluster_kick_rate_limit"); got != 0 {
		t.Fatalf("a legacy kick was refused (%d refusals counted)", got)
	}
}

// TestClusterKickDelayedDoesNotDisplaceNewerConnection replays the case the epoch
// exists for end to end: the kick of an old connection arrives after the same
// client id reconnected here, so it must not close the newer connection.
func TestClusterKickDelayedDoesNotDisplaceNewerConnection(t *testing.T) {
	metrics := newDeliveryMetrics()
	backend := &recordingCluster{}
	b := startBroker(t, Config{
		NodeID: "b", ClusterBackend: backend, Metrics: metrics,
		ConnectMode: ConnectModeKick, NotKickSeconds: -1, Logger: discardLogger(),
	})

	first, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "delayed"})
	oldKick := kickFor("delayed", "a", connectTimeOf(t, b, "delayed"))
	// The same client id reconnects here, in ConnectModeKick, and becomes the
	// younger connection.
	second, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "delayed"})
	expectDisconnect(t, first, ReasonSessionTakenOver)

	// The peer's kick for the replaced connection arrives now: it is older than the
	// connection that replaced it.
	backend.deliver(t, oldKick)
	alive(t, second)
	if got := metrics.count("cluster_kick_stale"); got == 0 {
		t.Fatal("the delayed kick was not counted as stale")
	}
}
