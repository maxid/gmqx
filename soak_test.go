package gmqx

// Soak test: continuous connect/publish/subscribe/disconnect churn against a
// real broker, then a set of post-run assertions about the state it left behind.
//
// It is off by default because it takes real time:
//
//	GMQX_SOAK=1 GMQX_SOAK_DURATION=10s go test -run TestSoak -v .
//
// Each worker owns one publisher connection and one subscriber connection. It
// subscribes to its own topic at QoS 2, publishes batches that mix QoS 0, 1 and
// 2, waits until every acknowledged QoS 1/2 publication of the batch was
// delivered, and then reconnects — sometimes with DISCONNECT, every third cycle
// by aborting the sockets, which is the connection loss the broker has to clean
// up after. Publishing a batch only after the previous one was fully drained is
// what makes the delivery assertion exact: no message can be acknowledged and
// then lost to a teardown the test performed itself.
//
// After the run the test asserts, and documents the bounds it uses:
//
//   - the broker's client table, its session store and its routing index are
//     empty, and so are its retained store and pending wills;
//   - every QoS 1/2 publication whose acknowledgement the worker received was
//     delivered (checked per batch while the run is in progress, and again on the
//     run's totals);
//   - the goroutine count returns to the pre-test baseline plus a slack of 8
//     goroutines (see soakGoroutineSlack);
//   - the live heap after two forced GCs stays below the pre-test heap times a
//     factor of 2 plus 64 MiB (see soakHeapFactor and soakHeapFloor).

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	// soakEnvEnable gates the test: any value other than "1" skips it.
	soakEnvEnable = "GMQX_SOAK"
	// soakEnvDuration sets the run length, as a Go duration string.
	soakEnvDuration = "GMQX_SOAK_DURATION"

	// soakDefaultDuration is the default run length: long enough to go through
	// thousands of connections and tens of thousands of messages, short enough
	// for a pre-merge check.
	soakDefaultDuration = 10 * time.Second

	// soakWorkers is the number of independent publisher/subscriber pairs that
	// churn concurrently.
	soakWorkers = 4
	// soakBatch is how many messages one worker publishes before it verifies the
	// deliveries of that batch. It is deliberately far below the broker's
	// 256-packet write-behind queue: a batch must never fill a subscriber queue by
	// itself, or a QoS 1/2 delivery would be replaced by a slow-consumer
	// disconnect and the delivery assertion would be about backpressure instead of
	// about the broker's bookkeeping.
	soakBatch = 16
	// soakBatchesPerConn is how many batches a worker publishes on one
	// connection pair before it reconnects. 64 batches are 1024 messages per
	// connection, which keeps connection churn real (roughly 100 connections per
	// second across the four workers) while staying far from the ephemeral port
	// range over a short run.
	soakBatchesPerConn = 64

	// soakGoroutineSlack is how many goroutines above the pre-test baseline the
	// run may leave behind. Reaching the bound would need a leak of at least one
	// goroutine per worker (4), and a real leak — a broker connection goroutine,
	// a writer, a retransmission timer or a drain goroutine — adds at least one per
	// connection, so 8 is well above runtime noise and well below the smallest
	// leak this test could hide. The broker's own goroutines are joined by Stop
	// (the dedicated leak test asserts baseline+2 for that), so the slack covers
	// the netpoller, GC workers and the testing package's helpers instead.
	soakGoroutineSlack = 8
	// soakHeapFactor and soakHeapFloor bound the live heap after the run:
	// after <= before*soakHeapFactor + soakHeapFloor. The live heap is measured
	// after two forced GCs, so it holds only objects that are still reachable. A
	// factor alone would be too tight on a small baseline (a few MiB of runtime
	// bookkeeping would trip it), and a floor alone would ignore a real growth on
	// a large one; the pair bounds both. 64 MiB is far above what a stopped broker
	// that holds no sessions can legitimately retain on a run of this size.
	soakHeapFactor = 2.0
	soakHeapFloor  = 64 << 20

	// soakSettleTimeout bounds how long the teardown waits for the broker to
	// unregister the last connection and for the goroutine count to settle.
	soakSettleTimeout = 15 * time.Second
	// soakDeliveryTimeout bounds one batch's wait for the deliveries of its
	// acknowledged publications.
	soakDeliveryTimeout = 10 * time.Second
)

// TestSoak is the guarded soak test. Without GMQX_SOAK=1 it skips immediately,
// so the default `go test ./...` stays fast.
func TestSoak(t *testing.T) {
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

	baselineGoroutines := settledGoroutines()
	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	broker := startBenchBroker(t, Config{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = broker.Stop(ctx)
	})

	stats := &soakStats{}
	failures := make(chan error, soakWorkers)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < soakWorkers; i++ {
		worker := newSoakWorker(i, broker.Addr(), stats)
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

	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var failure error
	t.Logf("soak running: duration=%s workers=%d batch=%d batches_per_conn=%d",
		duration, soakWorkers, soakBatch, soakBatchesPerConn)
run:
	for {
		select {
		case <-ticker.C:
			t.Logf("soak progress: acked=%d qos0=%d qos1=%d qos2=%d delivered=%d delivered_qos12=%d generations=%d connections=%d goroutines=%d",
				stats.acked.Load(), stats.qos0.Load(), stats.qos1.Load(), stats.qos2.Load(),
				stats.delivered.Load(), stats.deliveredQoS12.Load(), stats.generations.Load(),
				stats.connections.Load(), runtime.NumGoroutine())
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
	select {
	case err := <-failures:
		t.Fatalf("soak worker failed: %v", err)
	default:
	}

	// Teardown: the workers closed every connection; the broker unregisters a
	// connection on its own serve goroutine, so give it a moment to drain.
	if err := waitUntil(soakSettleTimeout, func() bool {
		broker.mu.RLock()
		defer broker.mu.RUnlock()
		return len(broker.clients) == 0
	}); err != nil {
		t.Fatalf("broker client table did not empty: %v", err)
	}
	stopBenchBroker(t, broker)

	broker.mu.RLock()
	clients := len(broker.clients)
	retained := len(broker.retained)
	offlineQueued := broker.offlineQueued
	broker.mu.RUnlock()
	stored := len(broker.store.Keys())
	broker.index.mu.RLock()
	indexed := len(broker.index.sessions)
	broker.index.mu.RUnlock()
	broker.willMu.Lock()
	wills := len(broker.wills)
	broker.willMu.Unlock()

	t.Logf("soak teardown: clients=%d stored_sessions=%d indexed_sessions=%d retained=%d pending_wills=%d offline_queued=%d",
		clients, stored, indexed, retained, wills, offlineQueued)
	if clients != 0 {
		t.Errorf("broker client table holds %d entries after the soak, want 0", clients)
	}
	if stored != 0 {
		t.Errorf("session store holds %d sessions after the soak, want 0 (every soak session is clean)", stored)
	}
	if indexed != 0 {
		t.Errorf("routing index holds %d sessions after the soak, want 0", indexed)
	}
	if retained != 0 {
		t.Errorf("retained store holds %d messages after the soak, want 0", retained)
	}
	if wills != 0 {
		t.Errorf("%d pending wills after the soak, want 0", wills)
	}

	acked := stats.acked.Load()
	deliveredQoS12 := stats.deliveredQoS12.Load()
	t.Logf("soak delivery ledger: acknowledged=%d delivered_qos12=%d duplicates=%d",
		acked, deliveredQoS12, deliveredQoS12-acked)
	if deliveredQoS12 < acked {
		t.Errorf("%d acknowledged QoS 1/2 messages were never delivered (%d acknowledged, %d delivered)",
			acked-deliveredQoS12, acked, deliveredQoS12)
	}
	if deliveredQoS12 > acked {
		// At-least-once permits a duplicate, e.g. a retransmission after a
		// connection loss; it is reported, not failed.
		t.Logf("note: %d duplicate QoS 1/2 deliveries (at-least-once semantics)", deliveredQoS12-acked)
	}
	if acked == 0 || stats.qos0.Load() == 0 {
		t.Errorf("soak did no work: acked=%d qos0=%d", acked, stats.qos0.Load())
	}

	if err := waitUntil(soakSettleTimeout, func() bool {
		return runtime.NumGoroutine() <= baselineGoroutines+soakGoroutineSlack
	}); err != nil {
		t.Fatalf("goroutines after the soak: %d, want at most %d (baseline %d + slack %d)",
			runtime.NumGoroutine(), baselineGoroutines+soakGoroutineSlack, baselineGoroutines, soakGoroutineSlack)
	}
	goroutines := runtime.NumGoroutine()

	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	bound := uint64(float64(before.HeapAlloc)*soakHeapFactor) + soakHeapFloor
	t.Logf("soak resources: goroutines=%d (baseline %d, bound %d) heap_before=%d heap_after=%d heap_bound=%d total_alloc_delta=%d",
		goroutines, baselineGoroutines, baselineGoroutines+soakGoroutineSlack,
		before.HeapAlloc, after.HeapAlloc, bound, after.TotalAlloc-before.TotalAlloc)
	if after.HeapAlloc > bound {
		t.Errorf("live heap after the soak = %d bytes, want at most %d (before %d * %.1f + %d)",
			after.HeapAlloc, bound, before.HeapAlloc, soakHeapFactor, soakHeapFloor)
	}

	t.Logf("soak result: %d generations, %d connections, %d batches, %d messages (%d QoS 0, %d QoS 1, %d QoS 2), %d acknowledged, %d delivered",
		stats.generations.Load(), stats.connections.Load(), stats.batches.Load(),
		stats.qos0.Load()+stats.qos1.Load()+stats.qos2.Load(),
		stats.qos0.Load(), stats.qos1.Load(), stats.qos2.Load(), acked, deliveredQoS12)
}

// soakStats is the run's shared counter set.
type soakStats struct {
	qos0           atomic.Int64
	qos1           atomic.Int64
	qos2           atomic.Int64
	acked          atomic.Int64
	delivered      atomic.Int64
	deliveredQoS12 atomic.Int64
	batches        atomic.Int64
	generations    atomic.Int64
	connections    atomic.Int64
}

// soakWorker is one churning publisher/subscriber pair.
type soakWorker struct {
	id     int
	addr   string
	topic  string
	prefix string
	stats  *soakStats

	pub   *benchWire
	sub   *benchWire
	drain *benchDrainer

	mu        sync.Mutex
	delivered map[string]struct{}

	seq     int64
	pid     uint16
	batches int
	cycle   int
	payload []byte
}

func newSoakWorker(id int, addr string, stats *soakStats) *soakWorker {
	return &soakWorker{
		id:        id,
		addr:      addr,
		topic:     fmt.Sprintf("soak/%d", id),
		prefix:    strconv.Itoa(id) + "-",
		stats:     stats,
		delivered: make(map[string]struct{}, 2*soakBatch),
	}
}

// run churns until stop is closed. Any error ends the worker and is reported to
// the test.
func (w *soakWorker) run(stop <-chan struct{}) error {
	defer w.close()
	for {
		select {
		case <-stop:
			return nil
		default:
		}
		if w.pub == nil || w.batches >= soakBatchesPerConn {
			if err := w.connect(); err != nil {
				return fmt.Errorf("worker %d: %w", w.id, err)
			}
		}
		if err := w.publishBatch(); err != nil {
			return fmt.Errorf("worker %d: %w", w.id, err)
		}
		w.batches++
	}
}

// connect closes the current pair (aborting it every third cycle) and opens a
// fresh subscriber and publisher. The subscriber is subscribed before the
// publisher connects, so the batch that follows always has a target.
func (w *soakWorker) connect() error {
	w.close()
	w.cycle++
	w.batches = 0

	sub, err := dialWire(w.addr, fmt.Sprintf("%s-sub-%d", w.prefix, w.cycle), true)
	if err != nil {
		return err
	}
	if err := sub.subscribe(1, []string{w.topic}, 2); err != nil {
		sub.close()
		return err
	}
	w.sub = sub
	w.drain = startBenchDrainer(sub, w.record)

	pub, err := dialWire(w.addr, fmt.Sprintf("%s-pub-%d", w.prefix, w.cycle), true)
	if err != nil {
		w.close()
		return err
	}
	w.pub = pub
	w.stats.connections.Add(2)
	if w.cycle > 1 {
		w.stats.generations.Add(1)
	}
	return nil
}

// close tears the current connection pair down. Every third cycle closes the
// sockets without a DISCONNECT, so the broker also sees unexpected connection
// losses and has to run its cleanup path.
func (w *soakWorker) close() {
	abrupt := w.cycle%3 == 0
	if w.sub != nil {
		if abrupt {
			w.sub.abort()
		} else {
			w.sub.close()
		}
	}
	if w.drain != nil {
		w.drain.wait()
	}
	if w.pub != nil {
		if abrupt {
			w.pub.abort()
		} else {
			w.pub.close()
		}
	}
	w.sub, w.drain, w.pub = nil, nil, nil
}

// record sees every delivery on the drain goroutine and remembers the ids of the
// current batch, so the batch can prove that each acknowledged publication
// arrived.
func (w *soakWorker) record(d benchDelivery) {
	id := string(d.Payload)
	w.mu.Lock()
	w.delivered[id] = struct{}{}
	w.mu.Unlock()
	w.stats.delivered.Add(1)
	if d.QoS > 0 {
		w.stats.deliveredQoS12.Add(1)
	}
}

// publishBatch publishes soakBatch messages with a mixed QoS, then waits until
// every acknowledged one was delivered.
func (w *soakWorker) publishBatch() error {
	w.clearDelivered()
	acked := make([]string, 0, soakBatch)
	for i := 0; i < soakBatch; i++ {
		w.seq++
		id := w.prefix + strconv.FormatInt(w.seq, 10)
		w.payload = append(w.payload[:0], id...)
		switch qos := byte(w.seq % 3); qos {
		case 0:
			if err := w.pub.sendPublish(w.topic, w.payload, 0, 0, false); err != nil {
				return fmt.Errorf("publish QoS 0: %w", err)
			}
			w.stats.qos0.Add(1)
		case 1, 2:
			w.pid = nextPID(w.pid)
			if err := w.pub.publishWait(w.topic, w.payload, qos, w.pid, false); err != nil {
				return fmt.Errorf("publish QoS %d: %w", qos, err)
			}
			acked = append(acked, id)
			w.stats.acked.Add(1)
			if qos == 1 {
				w.stats.qos1.Add(1)
			} else {
				w.stats.qos2.Add(1)
			}
		}
	}
	if err := w.waitAcked(acked); err != nil {
		return err
	}
	w.stats.batches.Add(1)
	return nil
}

// waitAcked blocks until every id in acked was delivered, or fails.
func (w *soakWorker) waitAcked(acked []string) error {
	deadline := time.Now().Add(soakDeliveryTimeout)
	for {
		if err := w.drain.errValue(); err != nil {
			return err
		}
		missing := w.missing(acked)
		if len(missing) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d of %d acknowledged messages were not delivered within %s: %v",
				len(missing), len(acked), soakDeliveryTimeout, missing)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// missing returns the ids of acked the drain goroutine has not seen yet.
func (w *soakWorker) missing(acked []string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var missing []string
	for _, id := range acked {
		if _, ok := w.delivered[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

// clearDelivered forgets the previous batch. A QoS 0 publication of the previous
// batch may still arrive afterwards; it is recorded under its own id and is
// never checked, so a late arrival can neither hide nor fake a delivery.
func (w *soakWorker) clearDelivered() {
	w.mu.Lock()
	clear(w.delivered)
	w.mu.Unlock()
}

// waitUntil polls cond until it is true or the timeout expires.
func waitUntil(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not met within %s", timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
