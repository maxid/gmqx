package gmqx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// Node id ownership is the Redis backend's answer to the duplicate-node defect:
// two processes with the same id would subscribe to the same channel, apply every
// event twice and collapse into one Members() entry. Start takes a TTL'd
// ownership key, a second live process is refused, Stop releases the key, and a
// crashed owner blocks a restart only until the TTL expires.

// TestRedisClusterNodeIDOwnershipRejectsSecondProcess checks that a second live
// process with the same node id is refused with the classified error and that
// the refusal leaves the owner's key untouched.
func TestRedisClusterNodeIDOwnershipRejectsSecondProcess(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	// Long enough that the live owner cannot lose its lease to the test's own
	// wall clock; the refresh interval is 20ms.
	cfg.NodeTTL = 5 * time.Second
	cfg.SummaryTTL = 5 * time.Second

	owner := startRedisCluster(t, mr, "n1", &recClusterHandler{}, cfg)
	if !mr.Exists(owner.ownerKey("n1")) {
		t.Fatalf("ownership key %s was not written", owner.ownerKey("n1"))
	}
	rec, err := owner.readOwner(context.Background())
	if err != nil {
		t.Fatalf("read owner record: %v", err)
	}
	if rec.NodeID != "n1" || rec.Incarnation != owner.incarnation {
		t.Fatalf("owner record = %+v, want node n1 incarnation %q", rec, owner.incarnation)
	}

	second, ok := NewRedisCluster(cfg).(*redisCluster)
	if !ok {
		t.Fatal("NewRedisCluster did not return a redisCluster")
	}
	err = second.Start(context.Background(), "n1", &recClusterHandler{})
	if err == nil {
		_ = second.Stop(context.Background())
		t.Fatal("a second process started with the node id of a live process")
	}
	if !errors.Is(err, ErrNodeIDInUse) {
		t.Fatalf("Start error %q does not wrap ErrNodeIDInUse", err)
	}
	for _, want := range []string{"n1", "already in use", owner.incarnation} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Start error %q does not mention %q", err, want)
		}
	}
	// The refused process released nothing: the live owner still holds the id.
	if rec, err := owner.readOwner(context.Background()); err != nil || rec.Incarnation != owner.incarnation {
		t.Fatalf("owner record after refused Start = (%+v, %v), want incarnation %q", rec, err, owner.incarnation)
	}
	// Stop after a refused Start is a no-op and leaves the owner alone.
	if err := stopWithin(t, second, 5*time.Second); err != nil {
		t.Fatalf("Stop after refused Start: %v", err)
	}
	if rec, err := owner.readOwner(context.Background()); err != nil || rec.Incarnation != owner.incarnation {
		t.Fatalf("owner record after refused Stop = (%+v, %v), want incarnation %q", rec, err, owner.incarnation)
	}
}

// TestRedisClusterNodeIDOwnershipReleasedOnStop checks that Stop releases the
// lease, so a restart with the same id does not wait for NodeTTL.
func TestRedisClusterNodeIDOwnershipReleasedOnStop(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	cfg.NodeTTL = 5 * time.Second
	cfg.SummaryTTL = 5 * time.Second

	first := startRedisCluster(t, mr, "n1", &recClusterHandler{}, cfg)
	if err := stopWithin(t, first, 5*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if mr.Exists(first.ownerKey("n1")) {
		t.Fatalf("ownership key %s survived Stop", first.ownerKey("n1"))
	}

	// The same id is free immediately: the restart does not wait for the TTL.
	second := startRedisCluster(t, mr, "n1", &recClusterHandler{}, cfg)
	rec, err := second.readOwner(context.Background())
	if err != nil {
		t.Fatalf("read owner record: %v", err)
	}
	if rec.Incarnation != second.incarnation || rec.Incarnation == first.incarnation {
		t.Fatalf("owner incarnation = %q, want the restarted process %q (old %q)", rec.Incarnation, second.incarnation, first.incarnation)
	}
}

// TestRedisClusterNodeIDOwnershipStaleOwner checks the escape hatch: a process
// that died without releasing its lease blocks a restart only until NodeTTL
// expires. The expiry is driven by miniredis FastForward, not a real sleep.
func TestRedisClusterNodeIDOwnershipStaleOwner(t *testing.T) {
	mr := runMiniRedis(t)
	cfg := redisTestConfig(mr)
	cfg.NodeTTL = 3 * time.Second
	cfg.SummaryTTL = 3 * time.Second

	crashed := startRedisCluster(t, mr, "n1", &recClusterHandler{}, cfg)

	// Simulate a crash: stop the loops without deregistering (Stop would release
	// the lease at once) and without waiting for the TTL. Closing the
	// subscription is what Stop does to unblock the receive loop; no key is
	// touched. Waiting for the loops proves no refresh is in flight before the
	// clock is moved.
	crashed.mu.Lock()
	cancel := crashed.cancel
	sub := crashed.sub
	crashed.mu.Unlock()
	cancel()
	if sub != nil {
		_ = sub.Close()
	}
	crashed.wg.Wait()

	if !mr.Exists(crashed.ownerKey("n1")) {
		t.Fatalf("ownership key %s disappeared without its TTL expiring", crashed.ownerKey("n1"))
	}

	// While the stale lease is alive the id is still refused.
	second, ok := NewRedisCluster(cfg).(*redisCluster)
	if !ok {
		t.Fatal("NewRedisCluster did not return a redisCluster")
	}
	err := second.Start(context.Background(), "n1", &recClusterHandler{})
	if !errors.Is(err, ErrNodeIDInUse) {
		t.Fatalf("Start with a stale owner: err = %v, want ErrNodeIDInUse", err)
	}
	if err := stopWithin(t, second, 5*time.Second); err != nil {
		t.Fatalf("Stop after refused Start: %v", err)
	}

	// TTL expiry is the escape hatch: after NodeTTL the id is free even though the
	// dead process never released it.
	mr.FastForward(cfg.NodeTTL + 100*time.Millisecond)
	if mr.Exists(crashed.ownerKey("n1")) {
		t.Fatalf("ownership key %s outlived NodeTTL", crashed.ownerKey("n1"))
	}
	restarted := startRedisCluster(t, mr, "n1", &recClusterHandler{}, cfg)
	if restarted.incarnation == crashed.incarnation {
		t.Fatalf("restarted process reused incarnation %q", restarted.incarnation)
	}
	if rec, err := restarted.readOwner(context.Background()); err != nil || rec.Incarnation != restarted.incarnation {
		t.Fatalf("owner record after restart = (%+v, %v), want incarnation %q", rec, err, restarted.incarnation)
	}
}
