package gmqx

// Concurrent throughput harness.
//
// Every other benchmark in this package measures one publish→deliver round trip
// at a time, so it reports a latency (ns/op) and not the message rate a broker
// sustains when many connections publish at once. This file measures the same
// quantity the reference broker of this family reports: N concurrent publisher
// connections, one fixed measurement window, and a per-second interval sample of
// the messages the broker accepted, acknowledged and delivered. The numbers are
// therefore directly comparable to a "200 connections, 5 s window, msg/s"
// figure, with the differences that do exist spelled out below.
//
// Shape of one case:
//
//   - one broker per case, on 127.0.0.1:0, with the harness configuration
//     described by thrBrokerConfig;
//   - N publisher connections (default 200), each its own TCP connection, each
//     publishing to its own topic `bench/throughput/p<shard>/<publisher>`;
//   - S subscriber connections (default 4); subscriber s subscribes to
//     `bench/throughput/p<s>/#`, so the publishers are sharded across the
//     subscribers and every publication is delivered to exactly one of them (no
//     fanout amplification);
//   - the publishers run flat out for the measurement window (default 5 s) —
//     there is no per-message round trip in the loop. At QoS 1/2 each publisher
//     keeps a bounded pipeline of unconfirmed publications (default 256, the
//     receive maximum the harness configures) and its reader goroutine releases a
//     slot per PUBACK (QoS 1) or per PUBCOMP (QoS 2), so the send rate is not
//     request/response bound;
//   - the load is closed-loop: a publication enters the harness only when an
//     earlier one was delivered or dropped (see thrPacer). The fleet therefore
//     sustains the broker's rate instead of burying its bounded delivery queues
//     and reporting how fast it discards messages, and the queue of the load
//     generator stays in the load generator instead of in the kernel's socket
//     buffers;
//   - a subscriber reads every delivery, records the delivery latency carried in
//     the payload and acknowledges it; acknowledgements are flushed when the
//     socket has no more buffered frames, so one read burst costs one ack write;
//   - the window is sampled once per second: the sample shows published,
//     acknowledged and delivered messages/s of that second, which makes the
//     warmup visible instead of hiding it in an average;
//   - the headline numbers of a case are the counter deltas between the first
//     and the last sample, divided by the measured window length, plus the peak
//     one-second rate and the p50/p99 delivery latency over the deliveries of the
//     window. Nothing a publisher still holds when the window closes — a blocked
//     write, a batch in flight, bytes in a socket buffer the broker has not read
//     yet — is added to the totals: at QoS 0 an overloaded broker lets those
//     buffers grow, and counting them afterwards would report a rate the window
//     never served.
//
// Payload and latency. A payload is the documented size (64 B and 1 KiB by
// default) with a fixed layout: the first 8 bytes are a per-publisher sequence
// number, the next 8 the sender's wall clock in Unix nanoseconds, the rest is
// filler. The subscriber computes the delivery latency from that timestamp, so
// p50/p99 cover the whole publish→deliver path inside one process (the same
// clock), including the time a message waited in a socket buffer, but not any
// clock skew.
//
// Why this is comparable to a 200-connection concurrent throughput figure:
// the load generator is not a request/response loop, the connection count is the
// same order, the window is fixed and sampled per second, and the headline is
// messages/second the broker actually acknowledged (QoS 1/2) or delivered
// (QoS 0). The differences are deliberate and stated: the payload sizes, the
// number of subscribers, the harness's raised flow-control windows, and that the
// clients and the broker share this process's CPUs. Like every benchmark in this
// package, these are indicative numbers for one machine, not a capacity
// guarantee.
//
// The harness reuses the wire client of bench_test.go (benchWire for CONNECT,
// SUBSCRIBE, frame reading and the small control packets) instead of the client
// package: the client package has its own delivery goroutine, acknowledgement
// bookkeeping and reconnect logic, and at 200 connections that machinery would
// sit in front of the broker in the measurement. The only part that does not go
// through benchWire is the publisher's hot loop: it writes pre-framed PUBLISHes
// in batches straight to the socket, so the load generator itself is not the
// syscall bottleneck of the measurement.
//
// Run it with:
//
//	GMQX_THROUGHPUT=1 go test -run TestThroughput -v .
//
// The case matrix, the load and the window are environment-tunable:
//
//	GMQX_THROUGHPUT=1                 gate; unset means the test skips
//	GMQX_THROUGHPUT_CLIENTS=200       concurrent publisher connections
//	GMQX_THROUGHPUT_SUBS=4            subscriber connections (publisher shards)
//	GMQX_THROUGHPUT_DURATION=5s       measurement window
//	GMQX_THROUGHPUT_QOS=0,1,2         cases to run
//	GMQX_THROUGHPUT_PAYLOAD=64,1024   payload sizes in bytes
//	GMQX_THROUGHPUT_UNACKED=256       per-publisher pipeline depth and the broker's
//	                                  inflight window / receive maximum
//	GMQX_THROUGHPUT_INFLIGHT=N        publications in flight across the whole load
//	                                  (default: two delivery windows per shard)
//	GMQX_THROUGHPUT_PROFILE=1         log the goroutine count per case
//
// A profile of one case runs with the same test:
//
//	GMQX_THROUGHPUT=1 GMQX_THROUGHPUT_QOS=1 GMQX_THROUGHPUT_PAYLOAD=64 \
//	    go test -run TestThroughput -cpuprofile /tmp/cpu.out \
//	    -memprofile /tmp/mem.out .

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

const (
	// thrDefaultClients is the concurrent publisher connection count of one case:
	// the reference figure this harness is comparable to is measured with 200
	// connections.
	thrDefaultClients = 200
	// thrDefaultSubscribers is how many subscriber connections share the load.
	// More than one keeps one subscriber's writer goroutine and one socket from
	// being the ceiling of the delivered rate; the publishers are sharded across
	// them, so there is still exactly one delivery per publication.
	thrDefaultSubscribers = 4
	// thrDefaultWindow is the measurement window.
	thrDefaultWindow = 5 * time.Second
	// thrDefaultUnacked is the depth of the per-publisher pipeline of unconfirmed
	// publications at QoS 1/2.
	thrDefaultUnacked = 256
	// thrMaxUnacked caps the pipeline at a value the configured receive maximum
	// can absorb even when both QoS windows are counted together.
	thrMaxUnacked = 1024
	// thrPublishBatch is how many pre-framed PUBLISHes one publisher writes with
	// one write syscall. The frames are still separate MQTT packets; batching only
	// removes the harness's own per-packet write syscall from the measurement.
	thrPublishBatch = 16
	// thrAckBatch caps the bytes of acknowledgement frames buffered by a
	// subscriber before it flushes them, so a burst cannot grow the buffer
	// without bound.
	thrAckBatch = 512
	// thrPayloadHeader is the per-message header a payload starts with: 8 bytes
	// of sequence number and 8 bytes of send timestamp.
	thrPayloadHeader = 16
	// thrTopicPrefix is the topic space of one case. Subscriber s owns
	// thrTopicPrefix + "p<s>/#".
	thrTopicPrefix = "bench/throughput/"
)

// TestThroughput measures sustained concurrent throughput: see the file comment
// for the shape of the load and why the number is comparable to a
// 200-connection figure. It is gated by GMQX_THROUGHPUT=1 because a full matrix
// run takes half a minute and is only meaningful on an otherwise idle machine.
func TestThroughput(t *testing.T) {
	if os.Getenv("GMQX_THROUGHPUT") != "1" {
		t.Skip("set GMQX_THROUGHPUT=1 to run the concurrent throughput harness")
	}
	cfg := thrLoadConfig(t)
	results := make([]thrResult, 0, len(cfg.qosList)*len(cfg.payloads))
	for _, qos := range cfg.qosList {
		for _, payload := range cfg.payloads {
			results = append(results, thrRunCase(t, cfg, qos, payload))
		}
	}
	fmt.Println()
	fmt.Println("Concurrent throughput, published/acked/delivered per second, latency in µs")
	thrPrintTable(results)
}

// thrLoadConfigValue is the parsed harness configuration.
type thrLoadConfigValue struct {
	clients     int
	subscribers int
	window      time.Duration
	qosList     []byte
	payloads    []int
	unacked     int
	inflight    int
	profile     bool
}

// thrLoadConfig reads the harness configuration from the environment and fails
// the test on a value that cannot produce a comparable number.
func thrLoadConfig(t *testing.T) thrLoadConfigValue {
	t.Helper()
	cfg := thrLoadConfigValue{
		clients:     thrDefaultClients,
		subscribers: thrDefaultSubscribers,
		window:      thrDefaultWindow,
		qosList:     []byte{0, 1, 2},
		payloads:    []int{64, 1024},
		unacked:     thrDefaultUnacked,
	}
	var err error
	if v := os.Getenv("GMQX_THROUGHPUT_CLIENTS"); v != "" {
		if cfg.clients, err = strconv.Atoi(v); err != nil || cfg.clients <= 0 {
			t.Fatalf("GMQX_THROUGHPUT_CLIENTS=%q: want a positive integer", v)
		}
	}
	if v := os.Getenv("GMQX_THROUGHPUT_SUBS"); v != "" {
		if cfg.subscribers, err = strconv.Atoi(v); err != nil || cfg.subscribers <= 0 {
			t.Fatalf("GMQX_THROUGHPUT_SUBS=%q: want a positive integer", v)
		}
	}
	if v := os.Getenv("GMQX_THROUGHPUT_DURATION"); v != "" {
		if cfg.window, err = time.ParseDuration(v); err != nil || cfg.window <= 0 {
			t.Fatalf("GMQX_THROUGHPUT_DURATION=%q: want a positive duration", v)
		}
	}
	if v := os.Getenv("GMQX_THROUGHPUT_QOS"); v != "" {
		cfg.qosList = cfg.qosList[:0]
		for _, part := range strings.Split(v, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || n < 0 || n > 2 {
				t.Fatalf("GMQX_THROUGHPUT_QOS=%q: want a comma list of 0, 1 and 2", v)
			}
			cfg.qosList = append(cfg.qosList, byte(n))
		}
	}
	if v := os.Getenv("GMQX_THROUGHPUT_PAYLOAD"); v != "" {
		cfg.payloads = cfg.payloads[:0]
		for _, part := range strings.Split(v, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || n < thrPayloadHeader {
				t.Fatalf("GMQX_THROUGHPUT_PAYLOAD=%q: want payload sizes of at least %d bytes", v, thrPayloadHeader)
			}
			cfg.payloads = append(cfg.payloads, n)
		}
	}
	if v := os.Getenv("GMQX_THROUGHPUT_UNACKED"); v != "" {
		if cfg.unacked, err = strconv.Atoi(v); err != nil || cfg.unacked <= 0 {
			t.Fatalf("GMQX_THROUGHPUT_UNACKED=%q: want a positive integer", v)
		}
	}
	if v := os.Getenv("GMQX_THROUGHPUT_INFLIGHT"); v != "" {
		if cfg.inflight, err = strconv.Atoi(v); err != nil || cfg.inflight <= 0 {
			t.Fatalf("GMQX_THROUGHPUT_INFLIGHT=%q: want a positive integer", v)
		}
	}
	if cfg.subscribers > cfg.clients {
		cfg.subscribers = cfg.clients
	}
	if cfg.unacked > thrMaxUnacked {
		cfg.unacked = thrMaxUnacked
	}
	cfg.profile = os.Getenv("GMQX_THROUGHPUT_PROFILE") == "1"
	return cfg
}

// thrBrokerConfig is the broker configuration of one case. Everything a
// deployment would tune for flow control and shaping is widened or disabled here,
// because those limits would otherwise be the measured ceiling instead of the
// broker's own throughput: the per-connection inflight windows, the queue behind
// them, the write-behind queue and the byte-rate shaper. Drops are still counted
// by thrMetrics and reported, and every other limit (packet size, connection
// count, ordering, QoS 1/2 delivery guarantees) keeps its default.
func thrBrokerConfig(cfg thrLoadConfigValue, metrics MetricsManager) Config {
	return Config{
		Metrics:               metrics,
		MaxInflightQoS1:       cfg.unacked,
		MaxInflightQoS2:       cfg.unacked,
		ReceiveMaximum:        cfg.unacked,
		MaxInflightQueueSize:  8192,
		MaxOutboundQueueSize:  4096,
		MaxReadByteRate:       -1,
		MaxWriteByteRate:      -1,
		MaxTotalReadByteRate:  -1,
		MaxTotalWriteByteRate: -1,
	}
}

// thrMetrics counts what the broker accepted, dropped and overflowed during a
// case. The counters are the only places the harness observes the broker itself;
// the delivered count comes from the subscribers. They are lock-free because a
// saturated case drops hundreds of thousands of messages per second, and a mutex
// on that path would be measuring the harness.
type thrMetrics struct {
	accepted atomic.Int64
	dropped  atomic.Int64

	dropReasons sync.Map // reason -> *atomic.Int64
	overflows   sync.Map // sink name -> *atomic.Int64
}

func newThrMetrics() *thrMetrics { return &thrMetrics{} }

func (m *thrMetrics) IncrementPublishedMessages() { m.accepted.Add(1) }
func (m *thrMetrics) IncrementConnections()       {}
func (m *thrMetrics) DecrementConnections()       {}
func (m *thrMetrics) IncrementSubscribeEvents()   {}
func (m *thrMetrics) DecrementSubscribeEvents()   {}
func (m *thrMetrics) IncrementDisconnectEvents()  {}
func (m *thrMetrics) RecordConnections(int)       {}
func (m *thrMetrics) RecordQueueDepth(string, int) {
}

func (m *thrMetrics) RecordDroppedMessage(reason string) {
	m.dropped.Add(1)
	thrCount(&m.dropReasons, reason)
}

func (m *thrMetrics) RecordOverflow(name string) {
	thrCount(&m.overflows, name)
}

// thrCount increments the counter of key in table.
func thrCount(table *sync.Map, key string) {
	if v, ok := table.Load(key); ok {
		v.(*atomic.Int64).Add(1)
		return
	}
	v, _ := table.LoadOrStore(key, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

// droppedTotal is the number of messages the broker discarded, which is what the
// QoS 0 pacer refunds: a dropped publication is no longer in flight.
func (m *thrMetrics) droppedTotal() int64 { return m.dropped.Load() }

// snapshot returns the drop and overflow counters as "reason=count" strings.
func (m *thrMetrics) snapshot() (dropped, overflow []string) {
	collect := func(table *sync.Map) []string {
		var out []string
		table.Range(func(key, value any) bool {
			out = append(out, fmt.Sprintf("%s=%d", key, value.(*atomic.Int64).Load()))
			return true
		})
		sort.Strings(out)
		return out
	}
	return collect(&m.dropReasons), collect(&m.overflows)
}

// thrResult is the measured line of one case.
type thrResult struct {
	qos         byte
	payload     int
	clients     int
	subscribers int
	window      time.Duration

	// The counters are the deltas over the measurement window, taken from the
	// final per-second sample so nothing that happens after the window can
	// inflate them: published is what the publishers finished writing, accepted is
	// what the broker routed, acked is what it acknowledged and delivered is what
	// the subscribers received before the window closed.
	published int64
	accepted  int64
	acked     int64
	delivered int64

	pubRate   float64
	ackRate   float64
	delivRate float64
	peakPub   float64
	peakAck   float64
	peakDeliv float64
	p50Micros int64
	p99Micros int64
	latencyN  int
	dropped   []string
	overflow  []string
}

// thrRunCase starts a broker and the load for one (QoS, payload) case, samples
// the window once per second and returns the measured result.
func thrRunCase(t *testing.T, cfg thrLoadConfigValue, qos byte, payloadSize int) thrResult {
	t.Helper()
	metrics := newThrMetrics()
	broker := startBenchBroker(t, thrBrokerConfig(cfg, metrics))
	defer stopBenchBroker(t, broker)

	result := thrResult{
		qos:         qos,
		payload:     payloadSize,
		clients:     cfg.clients,
		subscribers: cfg.subscribers,
		window:      cfg.window,
	}

	// Subscribers first, so every publisher's first publication already matches a
	// live subscription: a case must not spend its window building the routing
	// index.
	subs := make([]*thrSubscriber, 0, cfg.subscribers)
	for s := 0; s < cfg.subscribers; s++ {
		wire := dialBench(t, broker.Addr(), fmt.Sprintf("thr-sub-%d-%d-%d", qos, payloadSize, s))
		filter := fmt.Sprintf("%sp%d/#", thrTopicPrefix, s)
		if err := wire.subscribe(1, []string{filter}, qos); err != nil {
			t.Fatalf("subscribe %q: %v", filter, err)
		}
		sub := &thrSubscriber{wire: wire}
		sub.start()
		subs = append(subs, sub)
	}
	defer func() {
		for _, sub := range subs {
			sub.stop()
		}
	}()

	// The load is closed-loop: a publication enters the harness only when an
	// earlier one was delivered (see thrPacer). The pool is two delivery windows
	// of the subscriber shards — enough to keep every subscriber's inflight window
	// full for the whole window, and small enough that the broker's bounded
	// delivery queues are never the thing being measured. Without the closed loop
	// a QoS 0 case fills the kernel's socket buffers (the write eventually fails
	// with "no buffer space available" on this machine) and a QoS 1/2 case buries
	// the broker's bounded inflight queue and measures how fast it drops.
	pacer := newThrPacer(thrPacerSlots(cfg))
	publishers := make([]*thrPublisher, 0, cfg.clients)
	for p := 0; p < cfg.clients; p++ {
		wire := dialBench(t, broker.Addr(), fmt.Sprintf("thr-pub-%d-%d-%d", qos, payloadSize, p))
		topic := fmt.Sprintf("%sp%d/pub%d", thrTopicPrefix, p%cfg.subscribers, p)
		pub := newThrPublisher(wire, topic, qos, payloadSize, cfg.unacked)
		pub.pacer = pacer
		pub.start()
		publishers = append(publishers, pub)
	}
	stopPacer := make(chan struct{})
	pacerDone := make(chan struct{})
	go func() {
		defer close(pacerDone)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		var completed int64
		for {
			select {
			case <-stopPacer:
				return
			case <-ticker.C:
				now := thrSumDelivered(subs) + metrics.droppedTotal()
				if delta := now - completed; delta > 0 {
					completed += pacer.release(delta)
				}
			}
		}
	}()
	defer func() {
		close(stopPacer)
		<-pacerDone
	}()

	// The window starts here: every connection is established, every subscription
	// is live and every publisher goroutine is running. Counters are sampled as
	// deltas from this instant, so whatever a publisher wrote while the remaining
	// connections were being dialled does not count.
	start := time.Now()
	deadline := start.Add(cfg.window)
	base := thrSnapshot{
		published: thrSumSent(publishers),
		acked:     thrSumAcked(publishers),
		accepted:  metrics.accepted.Load(),
		delivered: thrSumDelivered(subs),
		at:        start,
	}
	fmt.Printf("\ncase qos=%d payload=%dB clients=%d subscribers=%d window=%v\n",
		qos, payloadSize, cfg.clients, cfg.subscribers, cfg.window)
	fmt.Printf("  %-8s %12s %12s %12s %12s\n", "t", "published/s", "acked/s", "accepted/s", "delivered/s")
	var peakPub, peakAck, peakDeliv float64
	last := base
	for {
		target := last.at.Add(time.Second)
		if !target.Before(deadline) {
			target = deadline
		}
		thrSleepUntil(target)
		now := time.Now()
		snap := thrSnapshot{
			published: thrSumSent(publishers),
			acked:     thrSumAcked(publishers),
			accepted:  metrics.accepted.Load(),
			delivered: thrSumDelivered(subs),
			at:        now,
		}
		seconds := now.Sub(last.at).Seconds()
		if seconds <= 0 {
			continue
		}
		fmt.Printf("  %-8s %12.0f %12.0f %12.0f %12.0f\n",
			fmt.Sprintf("t+%.1fs", now.Sub(start).Seconds()),
			float64(snap.published-last.published)/seconds,
			float64(snap.acked-last.acked)/seconds,
			float64(snap.accepted-last.accepted)/seconds,
			float64(snap.delivered-last.delivered)/seconds)
		// A partial final interval is printed but not taken as a peak: it would
		// understate the rate of the interval it covers.
		if seconds >= 0.9 {
			peakPub = max(peakPub, float64(snap.published-last.published)/seconds)
			peakAck = max(peakAck, float64(snap.acked-last.acked)/seconds)
			peakDeliv = max(peakDeliv, float64(snap.delivered-last.delivered)/seconds)
		}
		last = snap
		if !now.Before(deadline) {
			break
		}
	}
	window := last.at.Sub(start)

	// The final sample is the measurement. Everything a publisher may still hold
	// when it stops (a blocked write, a batch in flight, socket buffers the broker
	// has not read) belongs to the load after the window and is deliberately not
	// added to the counters: at QoS 0 the socket buffers can hold a large amount of
	// already-framed publications, and adding them to a 5 s window would report a
	// rate the window never saw.
	result.published = last.published - base.published
	result.acked = last.acked - base.acked
	result.accepted = last.accepted - base.accepted
	result.delivered = last.delivered - base.delivered
	result.pubRate = float64(result.published) / window.Seconds()
	result.ackRate = float64(result.acked) / window.Seconds()
	result.delivRate = float64(result.delivered) / window.Seconds()
	result.peakPub, result.peakAck, result.peakDeliv = peakPub, peakAck, peakDeliv
	result.p50Micros, result.p99Micros, result.latencyN = thrLatency(subs)
	result.dropped, result.overflow = metrics.snapshot()

	fmt.Printf("  %-8s %12.0f %12.0f %12.0f %12.0f   (p50=%dµs p99=%dµs n=%d)\n",
		"total", result.pubRate, result.ackRate, float64(result.accepted)/window.Seconds(), result.delivRate,
		result.p50Micros, result.p99Micros, result.latencyN)
	if len(result.dropped) > 0 {
		fmt.Printf("  dropped: %s\n", strings.Join(result.dropped, " "))
	}
	if len(result.overflow) > 0 {
		fmt.Printf("  overflow: %s\n", strings.Join(result.overflow, " "))
	}
	if cfg.profile {
		fmt.Printf("  goroutines before teardown: %d\n", runtime.NumGoroutine())
	}

	// Teardown is not part of the measurement, and an overloaded case can leave a
	// publisher blocked in a write for as long as the broker keeps reading: abort
	// the sockets instead of waiting for a graceful stop, so the harness does not
	// spend seconds draining a backlog it will not count.
	for _, pub := range publishers {
		pub.abort()
		if n := pub.readErrors.Load(); n > 0 {
			t.Fatalf("qos=%d payload=%d: publisher %q had %d socket errors during the run", qos, payloadSize, pub.topic, n)
		}
	}
	for _, sub := range subs {
		if err := sub.errValue(); err != nil {
			t.Fatalf("qos=%d payload=%d: %v", qos, payloadSize, err)
		}
	}
	return result
}

// thrSnapshot is one instant of the counters of a case.
type thrSnapshot struct {
	published int64
	acked     int64
	accepted  int64
	delivered int64
	at        time.Time
}

// thrSleepUntil sleeps until t. It is the sampling clock of the window; a sleep
// that overshoots is fine, because every rate is computed from the measured
// interval and not from the intended one.
func thrSleepUntil(t time.Time) {
	if wait := time.Until(t); wait > 0 {
		time.Sleep(wait)
	}
}

func thrSumSent(publishers []*thrPublisher) int64 {
	var n int64
	for _, p := range publishers {
		n += p.sent.Load()
	}
	return n
}

func thrSumAcked(publishers []*thrPublisher) int64 {
	var n int64
	for _, p := range publishers {
		n += p.acked.Load()
	}
	return n
}

func thrSumDelivered(subs []*thrSubscriber) int64 {
	var n int64
	for _, s := range subs {
		n += s.delivered.Load()
	}
	return n
}

// thrLatency merges the latency samples of every subscriber and returns p50, p99
// and the sample count, in microseconds.
func thrLatency(subs []*thrSubscriber) (p50, p99 int64, n int) {
	var all []int32
	for _, s := range subs {
		all = append(all, s.latencies()...)
	}
	if len(all) == 0 {
		return 0, 0, 0
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return int64(all[len(all)*50/100]), int64(all[len(all)*99/100]), len(all)
}

// thrPrintTable prints the measured cases as a markdown table, the form the
// numbers take in docs/PERFORMANCE.md.
func thrPrintTable(results []thrResult) {
	fmt.Println()
	fmt.Println("| QoS | payload | clients | subs | published/s | acked/s | accepted/s | delivered/s | peak delivered/s | p50 | p99 | drops |")
	fmt.Println("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
	for _, r := range results {
		drops := "none"
		if len(r.dropped) > 0 {
			drops = strings.Join(r.dropped, " ")
		}
		fmt.Printf("| %d | %d B | %d | %d | %.0f | %.0f | %.0f | %.0f | %.0f | %d µs | %d µs | %s |\n",
			r.qos, r.payload, r.clients, r.subscribers,
			r.pubRate, r.ackRate, float64(r.accepted)/r.window.Seconds(), r.delivRate, r.peakDeliv,
			r.p50Micros, r.p99Micros, drops)
	}
}

// thrPacer is the closed loop of a case: a publisher takes a slot before it
// writes a publication and the slot comes back when a subscriber delivered one.
// It keeps the queue of the load generator where a real client keeps it — in the
// client — instead of in the kernel's socket buffers or in the broker's bounded
// delivery queue, and it makes the measured rate a sustained rate: the fleet
// publishes at the rate the broker finishes publications at, and the per-second
// samples show whether that rate is still warming up.
//
// A publication the broker dropped returns its slot too (the broker is done with
// it), so the pool is stable for the whole window. The drops are reported
// separately: a QoS 1/2 case that overflows the queue behind a subscriber's
// inflight window shows them as the gap between acked and delivered.
type thrPacer struct {
	tokens chan struct{}
}

// thrPacerSlots is the pool of one case: the inflight window of every subscriber
// shard plus a few thousand publications of slack, which is enough to keep the
// delivery path busy (and to let the publishers batch) without ever overflowing
// the queue behind a window. The default queue of a connection in this harness is
// 8192, so 4096 of slack per shard stays comfortably below the point where the
// broker would have to drop a delivery.
func thrPacerSlots(cfg thrLoadConfigValue) int {
	if cfg.inflight > 0 {
		return cfg.inflight
	}
	slots := cfg.subscribers * (cfg.unacked + 4096)
	if slots < cfg.clients {
		slots = cfg.clients
	}
	return slots
}

func newThrPacer(n int) *thrPacer {
	p := &thrPacer{tokens: make(chan struct{}, n)}
	for i := 0; i < n; i++ {
		p.tokens <- struct{}{}
	}
	return p
}

// acquire blocks until one publication may be written or the publisher stops.
func (p *thrPacer) acquire(stop <-chan struct{}) bool {
	select {
	case <-p.tokens:
		return true
	case <-stop:
		return false
	}
}

// tryAcquire takes a slot only if one is free.
func (p *thrPacer) tryAcquire() bool {
	select {
	case <-p.tokens:
		return true
	default:
		return false
	}
}

// release returns n slots and reports how many fit: the channel is the whole
// credit pool, so a release that finds it full has nothing to add (the
// publishers are already not credit-starved) and the caller must not treat the
// surplus as released, or the pool would shrink over time.
func (p *thrPacer) release(n int64) int64 {
	var released int64
	for released < n {
		select {
		case p.tokens <- struct{}{}:
			released++
		default:
			return released
		}
	}
	return released
}

// thrSubscriber reads and acknowledges every delivery of one subscription
// connection. Only its own goroutine touches the wire, the latency samples and
// the delivered counter; the samples are behind a mutex so the test goroutine can
// take them at the end of the window.
type thrSubscriber struct {
	wire      *benchWire
	delivered atomic.Int64

	latMu sync.Mutex
	lat   []int32

	mu  sync.Mutex
	err error
	// acks is the batch of acknowledgement frames waiting for one write. It is
	// owned by the read goroutine.
	acks []byte

	stopCh chan struct{}
	done   chan struct{}
}

func (s *thrSubscriber) start() {
	s.stopCh = make(chan struct{})
	s.done = make(chan struct{})
	s.acks = make([]byte, 0, thrAckBatch)
	go s.run()
}

func (s *thrSubscriber) run() {
	defer close(s.done)
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}
		header, body, err := s.wire.readFrame()
		if err != nil {
			if !s.wire.closed.Load() {
				s.failf("subscriber read: %v", err)
			}
			s.flushAcks()
			return
		}
		switch header >> 4 {
		case mqtt.TypePublish:
			qos := (header >> 1) & 0x03
			_, rest, err := readString(body)
			if err != nil {
				s.failf("subscriber PUBLISH topic: %v", err)
				return
			}
			var pid uint16
			if qos > 0 {
				if len(rest) < 2 {
					s.failf("subscriber PUBLISH body of %d bytes", len(body))
					return
				}
				pid = binary.BigEndian.Uint16(rest)
				rest = rest[2:]
			}
			if len(rest) >= thrPayloadHeader {
				sent := int64(binary.BigEndian.Uint64(rest[8:16]))
				if sent > 0 {
					latency := time.Since(time.Unix(0, sent))
					if latency < 0 {
						latency = 0
					}
					micros := int32(latency / time.Microsecond)
					s.latMu.Lock()
					s.lat = append(s.lat, micros)
					s.latMu.Unlock()
				}
			}
			s.delivered.Add(1)
			switch qos {
			case 1:
				s.appendAck(mqtt.TypePuback<<4, pid)
			case 2:
				s.appendAck(mqtt.TypePubrec<<4, pid)
			}
		case mqtt.TypePubrel:
			if len(body) >= 2 {
				s.appendAck(mqtt.TypePubcomp<<4, binary.BigEndian.Uint16(body))
			}
		}
		if s.wire.r.Buffered() == 0 || len(s.acks) >= thrAckBatch {
			s.flushAcks()
		}
	}
}

// appendAck appends one acknowledgement frame (fixed header, remaining length 2,
// packet id) to the batch.
func (s *thrSubscriber) appendAck(header byte, pid uint16) {
	s.acks = append(s.acks, header, 2, byte(pid>>8), byte(pid))
}

// flushAcks writes the buffered acknowledgements with one write call. A failure
// on a connection that is being torn down is not an error; every other failure is
// reported by the read loop that follows.
func (s *thrSubscriber) flushAcks() {
	if len(s.acks) == 0 {
		return
	}
	s.wire.wmu.Lock()
	_, err := s.wire.conn.Write(s.acks)
	s.wire.wmu.Unlock()
	s.acks = s.acks[:0]
	if err != nil && !s.wire.closed.Load() {
		s.failf("subscriber ack write: %v", err)
	}
}

func (s *thrSubscriber) latencies() []int32 {
	s.latMu.Lock()
	defer s.latMu.Unlock()
	out := make([]int32, len(s.lat))
	copy(out, s.lat)
	return out
}

func (s *thrSubscriber) failf(format string, args ...any) {
	s.mu.Lock()
	if s.err == nil {
		s.err = fmt.Errorf(format, args...)
	}
	s.mu.Unlock()
}

func (s *thrSubscriber) errValue() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// stop closes the subscription connection and waits for the read goroutine. The
// connection is marked closed before the socket is closed, so the read error the
// close causes is not reported as a failure.
func (s *thrSubscriber) stop() {
	if s.stopCh == nil {
		return
	}
	s.wire.closed.Store(true)
	_ = s.wire.conn.Close()
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
	}
}

// thrPublisher is one concurrent publisher connection. Its hot loop writes
// pre-framed PUBLISHes in batches; a reader goroutine consumes the
// acknowledgements and releases pipeline slots.
type thrPublisher struct {
	wire       *benchWire
	topic      string
	qos        byte
	sent       atomic.Int64
	acked      atomic.Int64
	readErrors atomic.Int64

	// tokens is the bounded pipeline: one token is taken per unconfirmed
	// publication and returned when it is acknowledged. It is nil at QoS 0, where
	// the shared pacer bounds the load instead.
	tokens chan struct{}
	// pacer bounds the QoS 0 load; nil at QoS 1/2.
	pacer *thrPacer

	buf      []byte
	tmpl     []byte
	frameLen int
	pidOff   int
	payOff   int
	pid      uint16
	seq      uint64

	stopCh     chan struct{}
	done       chan struct{}
	readerDone chan struct{}
	stopOnce   sync.Once
}

func newThrPublisher(wire *benchWire, topic string, qos byte, payloadSize, unacked int) *thrPublisher {
	p := &thrPublisher{
		wire:       wire,
		topic:      topic,
		qos:        qos,
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	frame := []byte{mqtt.TypePublish<<4 | qos<<1}
	remaining := 2 + len(topic) + payloadSize
	if qos > 0 {
		remaining += 2
	}
	frame = appendRemainingLength(frame, remaining)
	frame = appendString(frame, topic)
	if qos > 0 {
		p.pidOff = len(frame)
		frame = append(frame, 0, 0)
	}
	p.payOff = len(frame)
	frame = append(frame, make([]byte, payloadSize)...)
	for i := p.payOff + thrPayloadHeader; i < len(frame); i++ {
		frame[i] = 'x'
	}
	p.tmpl = frame
	p.frameLen = len(frame)
	p.buf = make([]byte, 0, thrPublishBatch*p.frameLen)
	if qos > 0 {
		p.tokens = make(chan struct{}, unacked)
		for i := 0; i < unacked; i++ {
			p.tokens <- struct{}{}
		}
	}
	return p
}

func (p *thrPublisher) start() {
	go p.run()
	if p.qos > 0 {
		go p.readAcks()
	} else {
		close(p.readerDone)
	}
}

// run is the publisher's hot loop: take up to thrPublishBatch pipeline slots,
// frame that many PUBLISHes into the write buffer and write them with one call.
func (p *thrPublisher) run() {
	defer close(p.done)
	for {
		n := p.acquireBatch()
		if n == 0 {
			return
		}
		sent := time.Now().UnixNano()
		p.buf = p.buf[:0]
		for i := 0; i < n; i++ {
			p.seq++
			p.pid = nextPID(p.pid)
			off := len(p.buf)
			p.buf = append(p.buf, p.tmpl...)
			if p.qos > 0 {
				binary.BigEndian.PutUint16(p.buf[off+p.pidOff:], p.pid)
			}
			payload := p.buf[off+p.payOff:]
			binary.BigEndian.PutUint64(payload[0:8], p.seq)
			binary.BigEndian.PutUint64(payload[8:16], uint64(sent))
		}
		p.wire.wmu.Lock()
		written, err := p.wire.conn.Write(p.buf)
		p.wire.wmu.Unlock()
		if err != nil {
			if !p.wire.closed.Load() {
				p.readErrors.Add(1)
			}
			return
		}
		if written != len(p.buf) {
			if !p.wire.closed.Load() {
				p.readErrors.Add(1)
			}
			return
		}
		p.sent.Add(int64(n))
	}
}

// acquireBatch takes up to thrPublishBatch slots: the first one blocks until
// the closed loop has room, the rest are taken only if they are free. A batch is
// therefore written with one write call as long as the load has slack, and the
// load still adapts to the measured delivery rate instead of piling up in the
// kernel when it does not.
func (p *thrPublisher) acquireBatch() int {
	n := 0
	for n < thrPublishBatch {
		if n == 0 {
			if !p.pacer.acquire(p.stopCh) {
				return 0
			}
		} else if !p.pacer.tryAcquire() {
			break
		}
		if p.qos > 0 {
			if n == 0 {
				select {
				case <-p.tokens:
				case <-p.stopCh:
					p.pacer.release(1)
					return 0
				}
			} else {
				select {
				case <-p.tokens:
				default:
					// The connection's own unconfirmed pipeline is the limit:
					// give the global slot back and write what was acquired.
					p.pacer.release(1)
					return n
				}
			}
		}
		n++
	}
	return n
}

// release returns one pipeline slot. It never blocks: the token channel has
// exactly as many slots as were taken from it, and an acknowledgement cannot
// arrive for a publication that was never sent.
func (p *thrPublisher) release() {
	select {
	case p.tokens <- struct{}{}:
	default:
	}
}

// readAcks consumes the broker's acknowledgements of one publisher connection and
// releases the pipeline slots they free. A QoS 2 exchange is completed here: the
// PUBREL of the PUBREC is written before the PUBCOMP is awaited.
func (p *thrPublisher) readAcks() {
	defer close(p.readerDone)
	for {
		header, body, err := p.wire.readFrame()
		if err != nil {
			if !p.wire.closed.Load() && !p.isStopped() {
				p.readErrors.Add(1)
			}
			return
		}
		switch header >> 4 {
		case mqtt.TypePuback:
			p.acked.Add(1)
			p.release()
		case mqtt.TypePubrec:
			if len(body) < 2 {
				continue
			}
			pid := binary.BigEndian.Uint16(body)
			if len(body) >= 3 && body[2] >= 0x80 {
				// A refused exchange never got a PUBREL, so its slot is released
				// here; the publication was not accepted by the broker.
				p.release()
				continue
			}
			if err := p.wire.writePIDFrame(mqtt.TypePubrel<<4|0x02, pid); err != nil {
				if !p.wire.closed.Load() && !p.isStopped() {
					p.readErrors.Add(1)
				}
				return
			}
		case mqtt.TypePubcomp:
			p.acked.Add(1)
			p.release()
		}
	}
}

func (p *thrPublisher) isStopped() bool {
	select {
	case <-p.stopCh:
		return true
	default:
		return false
	}
}

// abort closes the publisher's connection without a DISCONNECT and waits for its
// goroutines. It is the teardown path: the window is already measured, and a
// publisher that is blocked writing into a saturated socket must not delay the
// next case.
func (p *thrPublisher) abort() {
	p.stopOnce.Do(func() { close(p.stopCh) })
	p.wire.abort()
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
	}
	select {
	case <-p.readerDone:
	case <-time.After(5 * time.Second):
	}
}
