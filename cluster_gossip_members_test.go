package gmqx

// Wait policy for this file (and the rule a future change must keep): every wait is
// on an observable condition with a generous ceiling (gossipWaitCeiling, 10 s).
// Nothing here sleeps for a fixed time and asserts afterwards.
//
// Two conditions need care because they are not monotone:
//
//   - Membership states suspect and dead are transient (the sweep advances them on
//     its own tick). Polling Members() for an exact state can miss one entirely when
//     the test goroutine is descheduled, so the transition test drives the cluster
//     clock (nowFn) itself: the state stays where the test put it until the test
//     advances the clock and sweeps, and the three transitions are still asserted.
//   - A node's link set is not stable the moment a link first exists: two nodes that
//     dial each other reconcile to one physical connection just after startup, and a
//     frame queued on the link that loses is dropped. A test that needs a delivery
//     waits for waitGossipSettled, not just for waitGossipLink.
//
// A member also only learns that a peer is gone through a link or a probe. A test
// that stops a node before the link came up observes a member that stays "alive"
// forever, because a failed dial is not a suspicion source.

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// gossipWaitCeiling bounds every membership/routing wait of a gossip test. It is a
// ceiling, not a timing assertion: the wait returns as soon as the condition it
// observes holds, so a loaded machine costs nothing but a genuinely broken build
// still fails instead of hanging.
const gossipWaitCeiling = 10 * time.Second

// startGossipTestCluster starts a gossip node and keeps the concrete type, which
// the membership tests below drive directly (a controlled clock, forced suspicion).
// This file moves into gmqx/cluster with the implementation; the backend-agnostic
// helpers the rest of the package uses live in cluster_backend_helpers_test.go.
func startGossipTestCluster(t *testing.T, nodeID string, cfg GossipClusterConfig, handler ClusterHandler) *gossipCluster {
	t.Helper()
	return startGossipCluster(t, nodeID, cfg, handler).(*gossipCluster)
}

// waitGossipState waits until the node's view of the member holds exactly want.
// Use it for a state the test has made stable (a controlled clock), never for a
// transient one: the sweep can move suspect to dead between two polls, which is how a
// poll for "suspect" used to fail after a full deadline.
func waitGossipState(t *testing.T, c *gossipCluster, nodeID string, want MemberState) {
	t.Helper()
	deadline := time.Now().Add(gossipWaitCeiling)
	var got MemberState
	for time.Now().Before(deadline) {
		if state, ok := c.memberState(nodeID); ok {
			got = state
			if state == want {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("state of %s on %s = %q, want %q", nodeID, c.nodeID, got, want)
}

func waitGossipGone(t *testing.T, c *gossipCluster, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(gossipWaitCeiling)
	for time.Now().Before(deadline) {
		if _, ok := c.memberState(nodeID); !ok {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("%s was not removed from the member view of %s", nodeID, c.nodeID)
}

// TestGossipMembershipOneSidedSeed pins the seed rule: only one of the two nodes
// lists the other, and both still end up with each other as a member. A node that
// is dialed learns the dialer from the ping it receives, so the seed list does not
// have to be symmetric.
func TestGossipMembershipOneSidedSeed(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	b := startGossipTestCluster(t, "b", gossipTestConfig(bAddr), &recClusterHandler{})
	a := startGossipTestCluster(t, "a", gossipTestConfig(aAddr, bAddr), &recClusterHandler{})

	waitClusterMembers(t, a, "b")
	waitClusterMembers(t, b, "a")

	view, ok := Cluster(a).(MemberView)
	if !ok {
		t.Fatal("gossip backend does not implement MemberView")
	}
	members := view.Members()
	if len(members) != 1 || members[0].NodeID != "b" {
		t.Fatalf("member view = %+v", members)
	}
	if members[0].Incarnation == "" || members[0].State != MemberAlive || members[0].Since.IsZero() {
		t.Fatalf("member = %+v, want an alive incarnation with a join time", members[0])
	}
	if members[0].Addr != bAddr {
		t.Fatalf("member address = %q, want %q", members[0].Addr, bAddr)
	}
}

// TestGossipMembershipJoinThroughGossip pins the "one reachable seed is enough"
// rule: c knows only a, and learns b from the member records a piggybacks on its
// ack, then dials b itself. a also learns c and gossips it to b.
func TestGossipMembershipJoinThroughGossip(t *testing.T) {
	aAddr, bAddr, cAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	a := startGossipTestCluster(t, "a", gossipTestConfig(aAddr, bAddr), &recClusterHandler{})
	b := startGossipTestCluster(t, "b", gossipTestConfig(bAddr, aAddr), &recClusterHandler{})
	waitClusterMembers(t, a, "b")
	waitClusterMembers(t, b, "a")

	c := startGossipTestCluster(t, "c", gossipTestConfig(cAddr, aAddr), &recClusterHandler{})
	for _, node := range []*gossipCluster{a, b, c} {
		want := []string{}
		for _, id := range []string{"a", "b", "c"} {
			if id != node.nodeID {
				want = append(want, id)
			}
		}
		waitClusterMembers(t, node, want...)
	}

	// The member view is sorted by node id and carries incarnation and state.
	members := a.Members()
	if len(members) != 2 || members[0].NodeID != "b" || members[1].NodeID != "c" {
		t.Fatalf("member view = %+v, want b then c", members)
	}
	for _, m := range members {
		if m.State != MemberAlive || m.Incarnation == "" || m.Since.IsZero() {
			t.Fatalf("member = %+v, want an alive incarnation with a join time", m)
		}
	}
}

// TestGossipMembershipSuspectDeadRemoved pins the failure path: the link of a
// stopped node closes, the member becomes suspect, then dead once its suspicion
// window elapsed, and is removed once DeadTimeout elapsed.
//
// The suspicion windows are driven by the test's clock (nowFn), not raced against the
// sweeper: the link closing is the event that starts the suspicion, and after it the
// test advances the clock by one window and sweeps, so "suspect" and "dead" are
// stable states that cannot be missed between two polls. The assertions are the same
// three transitions; only the waiting changed.
func TestGossipMembershipSuspectDeadRemoved(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	cfgA := gossipTestConfig(aAddr, bAddr)
	ca, ok := NewGossipCluster(cfgA).(*gossipCluster)
	if !ok {
		t.Fatal("NewGossipCluster did not return a gossipCluster")
	}
	ca.nowFn = func() time.Time { return time.Unix(0, clock.Load()) }
	if err := ca.Start(context.Background(), "a", &recClusterHandler{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ca.Stop(context.Background()) })
	a := ca
	b := startGossipTestCluster(t, "b", gossipTestConfig(bAddr, aAddr), &recClusterHandler{})
	waitClusterMembers(t, a, "b")
	waitClusterMembers(t, b, "a")
	// The transition is driven by the link closing, so the link has to exist before b
	// stops. A node that never linked to b only re-dials a stopped address and would
	// keep the member alive, which is a different scenario from the one under test.
	waitClusterLink(t, a, "b")
	waitClusterSettled(t, a)

	if err := b.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The closed link is what makes the member suspect. The clock is frozen, so the
	// state cannot move past suspect until the test advances it below.
	waitGossipState(t, a, "b", MemberSuspect)

	// One suspicion window later the sweep declares the member dead.
	clock.Add(int64(a.suspectTimeout() + time.Millisecond))
	a.tick()
	waitGossipState(t, a, "b", MemberDead)

	// DeadTimeout after that the tombstone is removed, so the node id can be reused.
	clock.Add(int64(a.deadTimeout() + time.Millisecond))
	a.tick()
	waitGossipGone(t, a, "b")
}

// TestGossipMembershipRefutesFalseSuspicion pins the incarnation refutation: a
// member accused of being suspect learns about it from the gossip delta carried by
// the probe, increments its incarnation number and answers alive, and the accusing
// node accepts the newer record. A false suspicion therefore heals instead of
// turning into a death.
func TestGossipMembershipRefutesFalseSuspicion(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	a := startGossipTestCluster(t, "a", gossipTestConfig(aAddr, bAddr), &recClusterHandler{})
	b := startGossipTestCluster(t, "b", gossipTestConfig(bAddr, aAddr), &recClusterHandler{})
	waitClusterMembers(t, a, "b")
	waitClusterMembers(t, b, "a")

	incA, ok := a.memberIncNumber("b")
	if !ok {
		t.Fatal("a does not know b")
	}
	incB := b.selfIncNumber()

	// Force the state a real probe timeout would produce, then probe with a fresh
	// frame so the suspicion travels in its delta. The regular tick races with this
	// loop for the probe slot, so the forcing is repeated until the refutation
	// arrives; each forced probe carries the suspicion, so it converges quickly.
	deadline := time.Now().Add(gossipWaitCeiling)
	for time.Now().Before(deadline) {
		state, ok := a.memberState("b")
		inc, _ := a.memberIncNumber("b")
		if ok && state == MemberAlive && inc > incA {
			if got := b.selfIncNumber(); got <= incB {
				t.Fatalf("b refuted with incarnation number %d, want > %d", got, incB)
			}
			return
		}
		a.mu.Lock()
		for nonce, p := range a.pendingProbes {
			if p.target == "b" {
				delete(a.pendingProbes, nonce)
			}
		}
		if m := a.members["b"]; m != nil {
			m.State = MemberSuspect
			m.suspectAt = a.now()
			m.changedAt = a.now()
		}
		a.mu.Unlock()
		a.startProbe("b")
		time.Sleep(2 * time.Millisecond)
	}
	state, _ := a.memberState("b")
	inc, _ := a.memberIncNumber("b")
	t.Fatalf("b state=%q incarnation_number=%d self=%d, want alive with a refuted incarnation number (was %d)",
		state, inc, b.selfIncNumber(), incA)
}

// TestGossipMembershipDuplicateNodeIDRefused pins that two processes announcing
// the same node id never merge: each refuses the other's link and both member views
// stay empty.
func TestGossipMembershipDuplicateNodeIDRefused(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	a := startGossipTestCluster(t, "dup", gossipTestConfig(aAddr, bAddr), &recClusterHandler{})
	b := startGossipTestCluster(t, "dup", gossipTestConfig(bAddr, aAddr), &recClusterHandler{})

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := clusterMemberIDs(t, a); len(got) != 0 {
			t.Fatalf("duplicate node id admitted on a: %v", got)
		}
		if got := clusterMemberIDs(t, b); len(got) != 0 {
			t.Fatalf("duplicate node id admitted on b: %v", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestGossipSingleNodeNoSeeds pins the alone case: the backend starts, serves
// local traffic (Spread is a successful no-op with no peers), reports an empty
// member list and logs that it is alone.
func TestGossipSingleNodeNoSeeds(t *testing.T) {
	logs := &captureHandler{}
	cfg := gossipTestConfig(freeAddr(t))
	cfg.Logger = slog.New(logs)
	c := startGossipTestCluster(t, "solo", cfg, &recClusterHandler{})

	waitFor(t, "the alone notice", func() bool {
		return logs.count("gmqx gossip cluster is alone: no seeds and no peers") == 1
	})
	if members := c.Members(); len(members) != 0 {
		t.Fatalf("single-node members = %+v, want none", members)
	}
	if err := c.Spread(context.Background(), ClusterMessage{
		Type: ClusterPublish, Topic: "local/x", Payload: []byte("local"),
	}); err != nil {
		t.Fatalf("Spread on a single node: %v", err)
	}
}
