package gmqx

import (
	"context"
	"log/slog"
	"sort"
	"testing"
	"time"
)

// gossipTestConfig is the fast variant of the gossip defaults: every timer is in
// the millisecond range so a membership transition is observed in a few polls
// instead of a few seconds.
func gossipTestConfig(addr string, seeds ...string) GossipClusterConfig {
	return GossipClusterConfig{
		Addr:  addr,
		Seeds: seeds,
		// The probe timeout is deliberately larger than the probe interval: the
		// tests run under -race on a loaded machine, and a member must not be
		// declared dead because one ack was a few milliseconds late.
		PingInterval:        25 * time.Millisecond,
		PingTimeout:         100 * time.Millisecond,
		SuspicionMultiplier: 4,
		DeadTimeout:         300 * time.Millisecond,
		ReconnectInterval:   10 * time.Millisecond,
		DialTimeout:         500 * time.Millisecond,
		SummaryInterval:     25 * time.Millisecond,
		PresenceTTL:         time.Second,
		Logger:              discardLogger(),
	}
}

func startGossipCluster(t *testing.T, nodeID string, cfg GossipClusterConfig, handler ClusterHandler) *gossipCluster {
	t.Helper()
	c, ok := NewGossipCluster(cfg).(*gossipCluster)
	if !ok {
		t.Fatal("NewGossipCluster did not return a gossipCluster")
	}
	if err := c.Start(context.Background(), nodeID, handler); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

// gossipMemberIDs returns the sorted peer ids of a node.
func gossipMemberIDs(c *gossipCluster) []string {
	out := []string{}
	for _, m := range c.Members() {
		out = append(out, m.NodeID)
	}
	return out
}

// waitGossipMembers waits until the node sees exactly the given peers (itself
// excluded by Members).
func waitGossipMembers(t *testing.T, c *gossipCluster, want ...string) {
	t.Helper()
	sort.Strings(want)
	deadline := time.Now().Add(5 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got = gossipMemberIDs(c)
		if equalStrings(got, want) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("members of %s = %v, want %v", c.nodeID, got, want)
}

// waitGossipLink waits until a link backs the peer, which is what a send needs; the
// member view alone can be one gossip frame ahead of the link.
func waitGossipLink(t *testing.T, c *gossipCluster, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		link := c.linksByNode[nodeID]
		c.mu.Unlock()
		if link != nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("%s has no link to %s", c.nodeID, nodeID)
}

func waitGossipState(t *testing.T, c *gossipCluster, nodeID string, want MemberState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
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
	deadline := time.Now().Add(5 * time.Second)
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
	b := startGossipCluster(t, "b", gossipTestConfig(bAddr), &recClusterHandler{})
	a := startGossipCluster(t, "a", gossipTestConfig(aAddr, bAddr), &recClusterHandler{})

	waitGossipMembers(t, a, "b")
	waitGossipMembers(t, b, "a")

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
	a := startGossipCluster(t, "a", gossipTestConfig(aAddr, bAddr), &recClusterHandler{})
	b := startGossipCluster(t, "b", gossipTestConfig(bAddr, aAddr), &recClusterHandler{})
	waitGossipMembers(t, a, "b")
	waitGossipMembers(t, b, "a")

	c := startGossipCluster(t, "c", gossipTestConfig(cAddr, aAddr), &recClusterHandler{})
	for _, node := range []*gossipCluster{a, b, c} {
		want := []string{}
		for _, id := range []string{"a", "b", "c"} {
			if id != node.nodeID {
				want = append(want, id)
			}
		}
		waitGossipMembers(t, node, want...)
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
func TestGossipMembershipSuspectDeadRemoved(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	a := startGossipCluster(t, "a", gossipTestConfig(aAddr, bAddr), &recClusterHandler{})
	b := startGossipCluster(t, "b", gossipTestConfig(bAddr, aAddr), &recClusterHandler{})
	waitGossipMembers(t, a, "b")
	waitGossipMembers(t, b, "a")

	if err := b.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	waitGossipState(t, a, "b", MemberSuspect)
	waitGossipState(t, a, "b", MemberDead)
	waitGossipGone(t, a, "b")
}

// TestGossipMembershipRefutesFalseSuspicion pins the incarnation refutation: a
// member accused of being suspect learns about it from the gossip delta carried by
// the probe, increments its incarnation number and answers alive, and the accusing
// node accepts the newer record. A false suspicion therefore heals instead of
// turning into a death.
func TestGossipMembershipRefutesFalseSuspicion(t *testing.T) {
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	a := startGossipCluster(t, "a", gossipTestConfig(aAddr, bAddr), &recClusterHandler{})
	b := startGossipCluster(t, "b", gossipTestConfig(bAddr, aAddr), &recClusterHandler{})
	waitGossipMembers(t, a, "b")
	waitGossipMembers(t, b, "a")

	incA, ok := a.memberIncNumber("b")
	if !ok {
		t.Fatal("a does not know b")
	}
	incB := b.selfIncNumber()

	// Force the state a real probe timeout would produce, then probe with a fresh
	// frame so the suspicion travels in its delta. The regular tick races with this
	// loop for the probe slot, so the forcing is repeated until the refutation
	// arrives; each forced probe carries the suspicion, so it converges quickly.
	deadline := time.Now().Add(5 * time.Second)
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
	a := startGossipCluster(t, "dup", gossipTestConfig(aAddr, bAddr), &recClusterHandler{})
	b := startGossipCluster(t, "dup", gossipTestConfig(bAddr, aAddr), &recClusterHandler{})

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := gossipMemberIDs(a); len(got) != 0 {
			t.Fatalf("duplicate node id admitted on a: %v", got)
		}
		if got := gossipMemberIDs(b); len(got) != 0 {
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
	c := startGossipCluster(t, "solo", cfg, &recClusterHandler{})

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
