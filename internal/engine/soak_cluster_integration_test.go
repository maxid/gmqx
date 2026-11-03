//go:build integration

package engine

// The soak variant for a clustered deployment with a shared store.
//
// TestSoak churns connections against one broker with the node-local default. This
// one keeps that churn and adds the thing a shared backend makes possible and a
// deployment will actually do: sessions that move between two nodes while the
// cluster is under load. Every migration is one store write on the node the session
// leaves and one take on the node it arrives at, so a leak in either shows up as
// stored sessions that outlive their clients.
//
// It needs Docker, so it is behind -tags=integration and off by default:
//
//	GMQX_SOAK=1 GMQX_SOAK_DURATION=30s go test -tags=integration -timeout 15m \
//	    -run TestSoakClusterSharedStore -v ./internal/engine

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/wait"
)

// soakMigrators is how many client ids move between the two nodes for the whole
// run. Each one owns a persistent session and keeps moving it.
const soakMigrators = 4

func TestSoakClusterSharedStore(t *testing.T) {
	if os.Getenv(soakEnvEnable) != "1" {
		t.Skipf("soak test disabled: set %s=1 to run it (duration via %s, default %s)",
			soakEnvEnable, soakEnvDuration, soakDefaultDuration)
	}
	if testing.Short() {
		t.Skip("soak test skipped in -short mode")
	}
	duration := soakDefaultDuration
	if raw := os.Getenv(soakEnvDuration); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("%s=%q is not a positive Go duration: %v", soakEnvDuration, raw, err)
		}
		duration = parsed
	}

	redisAddr := startContainer(t, "redis:7-alpine", "6379/tcp", wait.ForListeningPort("6379/tcp"))
	row := redisBackend(redisAddr)

	// A session that moves between nodes is a takeover by definition, and the
	// migrators do it far faster than the 30 s anti-flap window would allow, so the
	// soak runs the mode and the window a deployment of this shape runs: KICK with
	// the window off.
	a, bn := tcpPair(t, row, func(c *Config) {
		c.ConnectMode = ConnectModeKick
		c.NotKickSeconds = -1
	})
	// The baseline is the idle cluster, not an empty process: two brokers, two
	// cluster backends and a Redis client pool own goroutines of their own, and those
	// are the system under test rather than a leak. What the soak asserts is that the
	// run returns to this count once every client is gone.
	baselineGoroutines := settledGoroutines()

	// The connection churn of the plain soak, against node a.
	stats := &soakStats{}
	failures := make(chan error, soakWorkers+soakMigrators)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < soakWorkers; i++ {
		worker := newSoakWorker(i, a.Addr(), stats)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := worker.run(stop); err != nil {
				select {
				case failures <- err:
				default:
				}
			}
		}()
	}

	// The migrations, between the two nodes.
	var migrations atomic.Int64
	for i := 0; i < soakMigrators; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			counter := &migrator{id: id, from: a.Addr(), to: bn.Addr()}
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := counter.cycle(); err != nil {
					select {
					case failures <- err:
					default:
					}
					return
				}
				migrations.Add(1)
			}
		}(i)
	}

	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var failure error
run:
	for {
		select {
		case <-ticker.C:
			t.Logf("soak progress: acked=%d delivered_qos12=%d connections=%d migrations=%d goroutines=%d",
				stats.acked.Load(), stats.deliveredQoS12.Load(), stats.connections.Load(),
				migrations.Load(), runtime.NumGoroutine())
		case err := <-failures:
			failure = err
			break run
		case <-deadline.C:
			break run
		}
	}
	close(stop)
	wg.Wait()
	if failure != nil {
		t.Fatalf("soak worker failed: %v", failure)
	}

	// A disconnect stores the session on the unregister goroutine, so wait for the
	// migrators' last write to land before deleting the keys: a clean CONNECT that
	// deleted first would be undone by that pending write.
	if err := waitUntil(soakSettleTimeout, func() bool {
		return liveClients(a)+liveClients(bn) == 0
	}); err != nil {
		t.Fatalf("connections did not drain after the soak: a=%d b=%d", liveClients(a), liveClients(bn))
	}

	// Clean up the migrators' sessions on both nodes. A clean CONNECT deletes the
	// stored session and drops the offline target from that node's routing index, and
	// a take is not broadcast: the node that stored the session keeps its index entry
	// until it sees the client id again or restarts. So each node gets one clean
	// CONNECT per migrator — which is also what a deployment drains a stale offline
	// target with.
	for i := 0; i < soakMigrators; i++ {
		id := fmt.Sprintf("soak-roam-%d", i)
		for _, addr := range []string{a.Addr(), bn.Addr()} {
			c, err := dialWire(addr, id, true)
			if err != nil {
				t.Fatalf("clean up %s on %s: %v", id, addr, err)
			}
			c.close()
		}
	}

	if err := waitUntil(soakSettleTimeout, func() bool {
		return len(a.persist.SessionKeys()) == 0 && len(bn.persist.SessionKeys()) == 0
	}); err != nil {
		t.Fatalf("stored sessions after the soak: a=%d b=%d, want 0 on both nodes",
			len(a.persist.SessionKeys()), len(bn.persist.SessionKeys()))
	}
	if clients := liveClients(a) + liveClients(bn); clients != 0 {
		t.Errorf("%d connections still registered after the soak, want 0", clients)
	}
	if indexed := a.index.Len() + bn.index.Len(); indexed != 0 {
		t.Errorf("routing index holds %d entries after the soak, want 0", indexed)
	}
	if acked := stats.acked.Load(); acked == 0 {
		t.Error("the soak did no acknowledged work")
	}
	if migrations.Load() == 0 {
		t.Error("no session migrated between the nodes")
	}
	if err := waitUntil(soakSettleTimeout, func() bool {
		return runtime.NumGoroutine() <= baselineGoroutines+soakGoroutineSlack
	}); err != nil {
		t.Fatalf("goroutines after the soak: %d, want at most %d (baseline %d + slack %d)",
			runtime.NumGoroutine(), baselineGoroutines+soakGoroutineSlack, baselineGoroutines, soakGoroutineSlack)
	}

	t.Logf("soak result: %d migrations, %d connections, %d acknowledged messages, %d QoS 1/2 delivered",
		migrations.Load(), stats.connections.Load(), stats.acked.Load(), stats.deliveredQoS12.Load())
}

// migrator moves one persistent session between two nodes: connect and subscribe
// on the first, disconnect (the store write), then reconnect with the same client id
// on the second and disconnect again (the take, then the next write).
type migrator struct {
	id   int
	from string
	to   string
}

func (m *migrator) cycle() error {
	clientID := fmt.Sprintf("soak-roam-%d", m.id)
	topic := fmt.Sprintf("soak/roam/%d", m.id)

	out, err := dialWire(m.from, clientID, false)
	if err != nil {
		return fmt.Errorf("migrator %d connect %s: %w", m.id, m.from, err)
	}
	if err := out.subscribe(1, []string{topic}, 0); err != nil {
		_ = out.conn.Close()
		return fmt.Errorf("migrator %d subscribe: %w", m.id, err)
	}
	out.close()

	in, err := dialWire(m.to, clientID, false)
	if err != nil {
		return fmt.Errorf("migrator %d connect %s: %w", m.id, m.to, err)
	}
	in.close()

	m.from, m.to = m.to, m.from
	return nil
}

// liveClients is the number of connections the broker still has registered.
func liveClients(b *Broker) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}
