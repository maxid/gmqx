package gmqx

// Benchmark suite: the real broker over real TCP.
//
// Every case starts its own Broker on an ephemeral loopback port and talks to it
// with the small MQTT 3.1.1 wire client below. Nothing is mocked and nothing is
// shared: no case can inherit the sessions, retained messages or open
// connections of a previous one, and every case tears its broker down when the
// benchmark function returns (so the setup is repeated per calibration run
// instead of accumulating brokers across runs).
//
// Measurement shape
//
// Setup — broker start, connections, subscriptions, retained seeding — happens
// before b.ResetTimer, so it is never part of ns/op. The timed region publishes,
// and a dedicated drain goroutine reads every delivery and acknowledges it, so
// the broker is never stalled by a full socket or by an unconfirmed inflight
// window; without that drain the benchmark would mostly measure its own
// backpressure. Every case is windowed: every few publications the loop waits
// until the subscriber has received them, while the timer still runs, so ns/op
// covers the whole publish→deliver path rather than only "the broker read the
// bytes". Only the tail (fewer than one window) is drained after b.StopTimer.
//
// What ns/op means per case: it is the cost of one publication — for a fanout,
// one publication delivered to every subscriber, so the per-delivery cost is
// ns/op divided by the subscriber count. Bytes/op and allocs/op are reported by
// b.ReportAllocs and cover the whole benchmark process, drain goroutine
// included.

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mqttclient "github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/mqtt"
)

const (
	// benchIODeadline is the read/write deadline of one benchmark connection. It
	// only exists so a wedged broker surfaces as an error instead of a benchmark
	// that waits for the go test timeout; it is far above any healthy run.
	benchIODeadline = 30 * time.Minute
	// benchWaitTimeout bounds one wait for a delivery the broker accepted. A
	// benchmark that cannot reach the expected count within it fails instead of
	// hanging.
	benchWaitTimeout = 30 * time.Second
)

// benchLogger drops the broker's warnings: a benchmark measures the broker, it
// does not want stderr formatting in the timed region.
var benchLogger = slog.New(slog.DiscardHandler)

// startBenchBroker starts an in-process broker on 127.0.0.1:0 and returns it.
// The caller owns its lifetime and is expected to stop it when the benchmark
// function returns.
func startBenchBroker(tb testing.TB, cfg Config) *Broker {
	tb.Helper()
	cfg.Addr = "127.0.0.1:0"
	if cfg.Logger == nil {
		cfg.Logger = benchLogger
	}
	b, err := New(cfg)
	if err != nil {
		tb.Fatalf("gmqx.New: %v", err)
	}
	if err := b.Start(context.Background()); err != nil {
		tb.Fatalf("broker.Start: %v", err)
	}
	return b
}

// stopBenchBroker stops a broker started by startBenchBroker.
func stopBenchBroker(tb testing.TB, b *Broker) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := b.Stop(ctx); err != nil {
		tb.Fatalf("broker.Stop: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Wire client

// benchWire is a minimal MQTT 3.1.1 client on one TCP connection: exactly what a
// benchmark needs to publish, subscribe, acknowledge and read, with reusable
// buffers so the harness itself does not show up in allocs/op. It is not a
// general-purpose client; the repository's client package is benchmarked
// separately by BenchmarkClientPublishQoS1.
//
// One goroutine may read while another writes: the write side is serialized by
// wmu and the read side owns rbuf.
type benchWire struct {
	conn   net.Conn
	r      *bufio.Reader
	w      *bufio.Writer
	wmu    sync.Mutex
	closed atomic.Bool

	rbuf []byte
	wbuf []byte
}

// dialWire opens a connection, completes CONNECT/CONNACK and returns the client.
// A non-zero CONNACK code is an error.
func dialWire(addr, clientID string, clean bool) (*benchWire, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	_ = conn.SetDeadline(time.Now().Add(benchIODeadline))
	c := &benchWire{
		conn: conn,
		r:    bufio.NewReaderSize(conn, 64<<10),
		w:    bufio.NewWriterSize(conn, 64<<10),
	}
	body := appendString(nil, "MQTT")
	flags := byte(0)
	if clean {
		flags = 0x02
	}
	// protocol level 4 (MQTT 3.1.1), connect flags, keepalive 0 (disabled: a
	// benchmark connection is never idle long enough for a PINGREQ to matter).
	body = append(body, 4, flags, 0, 0)
	body = appendString(body, clientID)
	if err := c.writeFrame(mqtt.TypeConnect<<4, body); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("CONNECT %s: %w", clientID, err)
	}
	header, ack, err := c.readFrame()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("CONNACK %s: %w", clientID, err)
	}
	if header != mqtt.TypeConnack<<4 || len(ack) < 2 {
		_ = conn.Close()
		return nil, fmt.Errorf("CONNACK %s: header %#x body %x", clientID, header, ack)
	}
	if ack[1] != 0 {
		_ = conn.Close()
		return nil, fmt.Errorf("CONNACK %s: reason %#x", clientID, ack[1])
	}
	return c, nil
}

// dialBench is dialWire for the benchmark goroutine: a failure ends the
// benchmark.
func dialBench(tb testing.TB, addr, clientID string) *benchWire {
	tb.Helper()
	c, err := dialWire(addr, clientID, true)
	if err != nil {
		tb.Fatalf("%v", err)
	}
	return c
}

// close sends DISCONNECT (when the connection is still usable) and closes the
// socket. It is idempotent.
func (c *benchWire) close() {
	if c.closed.Swap(true) {
		return
	}
	_ = c.writeFrame(mqtt.TypeDisconnect<<4, nil)
	_ = c.conn.Close()
}

// abort closes the socket without a DISCONNECT, which is the unexpected
// connection loss the broker must clean up after.
func (c *benchWire) abort() {
	if c.closed.Swap(true) {
		return
	}
	_ = c.conn.Close()
}

// writeFrame writes one packet: fixed header, remaining length, body. The frame
// is assembled in the connection's scratch buffer, so the call does not
// allocate.
func (c *benchWire) writeFrame(header byte, body []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	buf := c.wbuf[:0]
	buf = append(buf, header)
	buf = appendRemainingLength(buf, len(body))
	buf = append(buf, body...)
	c.wbuf = buf
	return c.flushLocked(buf)
}

// writePIDFrame writes a packet whose body is a single packet identifier
// (PUBACK, PUBREC, PUBREL, PUBCOMP) without allocating.
func (c *benchWire) writePIDFrame(header byte, pid uint16) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	buf := c.wbuf[:0]
	buf = append(buf, header, 2, byte(pid>>8), byte(pid))
	c.wbuf = buf
	return c.flushLocked(buf)
}

// flushLocked writes buf and flushes the buffer. The caller holds wmu.
func (c *benchWire) flushLocked(buf []byte) error {
	if _, err := c.w.Write(buf); err != nil {
		return err
	}
	return c.w.Flush()
}

// readFrame reads one packet into the connection's read buffer and returns its
// fixed header and body. The body is valid until the next read on the
// connection.
func (c *benchWire) readFrame() (byte, []byte, error) {
	header, err := c.r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, err := readRemainingLength(c.r)
	if err != nil {
		return 0, nil, err
	}
	if cap(c.rbuf) < n {
		c.rbuf = make([]byte, n)
	}
	body := c.rbuf[:n]
	if _, err := io.ReadFull(c.r, body); err != nil {
		return 0, nil, err
	}
	return header, body, nil
}

// subscribe sends one SUBSCRIBE with the given filters at the given QoS and
// waits for the SUBACK.
func (c *benchWire) subscribe(pid uint16, filters []string, qos byte) error {
	body := make([]byte, 0, 2+len(filters)*24)
	body = append(body, byte(pid>>8), byte(pid))
	for _, filter := range filters {
		body = appendString(body, filter)
		body = append(body, qos)
	}
	if err := c.writeFrame(mqtt.TypeSubscribe<<4|0x02, body); err != nil {
		return fmt.Errorf("SUBSCRIBE: %w", err)
	}
	header, ack, err := c.readFrame()
	if err != nil {
		return fmt.Errorf("SUBACK: %w", err)
	}
	if header != mqtt.TypeSuback<<4 {
		return fmt.Errorf("SUBACK: header %#x", header)
	}
	if len(ack) < 2+len(filters) {
		return fmt.Errorf("SUBACK: body %x for %d filters", ack, len(filters))
	}
	if got := binary.BigEndian.Uint16(ack); got != pid {
		return fmt.Errorf("SUBACK: packet id %d, want %d", got, pid)
	}
	for i := range filters {
		if code := ack[2+i]; code > 2 {
			return fmt.Errorf("SUBACK: filter %q refused with %#x", filters[i], code)
		}
	}
	return nil
}

// sendPublish writes one PUBLISH without waiting for an acknowledgement. The
// frame is assembled directly around the caller's topic and payload, so the
// call does not allocate.
func (c *benchWire) sendPublish(topic string, payload []byte, qos byte, pid uint16, retain bool) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	header := byte(mqtt.TypePublish << 4)
	header |= qos << 1
	if retain {
		header |= 0x01
	}
	remaining := 2 + len(topic) + len(payload)
	if qos > 0 {
		remaining += 2
	}
	buf := c.wbuf[:0]
	buf = append(buf, header)
	buf = appendRemainingLength(buf, remaining)
	buf = appendString(buf, topic)
	if qos > 0 {
		buf = append(buf, byte(pid>>8), byte(pid))
	}
	buf = append(buf, payload...)
	c.wbuf = buf
	return c.flushLocked(buf)
}

// publishWait publishes at QoS 1 or 2 and waits for the matching acknowledgement
// (PUBACK, or PUBREC/PUBREL/PUBCOMP), which is one full broker round trip.
func (c *benchWire) publishWait(topic string, payload []byte, qos byte, pid uint16, retain bool) error {
	if err := c.sendPublish(topic, payload, qos, pid, retain); err != nil {
		return err
	}
	switch qos {
	case 1:
		return c.expectAck(mqtt.TypePuback<<4, pid)
	case 2:
		if err := c.expectAck(mqtt.TypePubrec<<4, pid); err != nil {
			return err
		}
		if err := c.writePIDFrame(mqtt.TypePubrel<<4|0x02, pid); err != nil {
			return err
		}
		return c.expectAck(mqtt.TypePubcomp<<4, pid)
	}
	return nil
}

// expectAck reads one packet and requires it to be the given acknowledgement for
// pid.
func (c *benchWire) expectAck(header byte, pid uint16) error {
	got, body, err := c.readFrame()
	if err != nil {
		return fmt.Errorf("await acknowledgement %#x: %w", header, err)
	}
	if got != header {
		return fmt.Errorf("await acknowledgement %#x: got packet %#x", header, got)
	}
	if len(body) < 2 {
		return fmt.Errorf("await acknowledgement %#x: body %x", header, body)
	}
	if gotPID := binary.BigEndian.Uint16(body); gotPID != pid {
		return fmt.Errorf("await acknowledgement %#x: packet id %d, want %d", header, gotPID, pid)
	}
	return nil
}

// nextPID returns the next non-zero packet identifier.
func nextPID(pid uint16) uint16 {
	pid++
	if pid == 0 {
		pid = 1
	}
	return pid
}

// ---------------------------------------------------------------------------
// Drain goroutine

// benchDelivery is one PUBLISH the subscriber received. Payload aliases the
// drainer's read buffer and is only valid for the duration of onMsg.
type benchDelivery struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

// benchDrainer reads and acknowledges every delivery of one subscription
// connection. It is the reason a benchmark never measures the broker's
// backpressure: the connection's write-behind queue and inflight window are
// always drained.
type benchDrainer struct {
	c      *benchWire
	onMsg  func(benchDelivery)
	count  atomic.Int64
	notify chan struct{}
	done   chan struct{}
	// timer bounds one waitCount call. It is reused across calls so a wait does
	// not allocate a timer, and it is only touched by the benchmark goroutine:
	// waitCount is never called concurrently for one drainer.
	timer *time.Timer

	mu  sync.Mutex
	err error
}

// startBenchDrainer starts draining c. onMsg, when non-nil, sees every delivery
// on the drain goroutine and must return promptly.
func startBenchDrainer(c *benchWire, onMsg func(benchDelivery)) *benchDrainer {
	d := &benchDrainer{c: c, onMsg: onMsg, notify: make(chan struct{}, 1), done: make(chan struct{})}
	go d.run()
	return d
}

func (d *benchDrainer) run() {
	defer close(d.done)
	for {
		header, body, err := d.c.readFrame()
		if err != nil {
			if !d.c.closed.Load() {
				d.failf("drain: %v", err)
			}
			return
		}
		switch header >> 4 {
		case mqtt.TypePublish:
			topic, rest, err := readString(body)
			if err != nil {
				d.failf("drain: PUBLISH topic: %v", err)
				return
			}
			qos := (header >> 1) & 0x03
			var pid uint16
			if qos > 0 {
				if len(rest) < 2 {
					d.failf("drain: PUBLISH body %x", body)
					return
				}
				pid = binary.BigEndian.Uint16(rest)
				rest = rest[2:]
			}
			switch qos {
			case 1:
				if err := d.c.writePIDFrame(mqtt.TypePuback<<4, pid); err != nil {
					d.failf("drain: PUBACK: %v", err)
					return
				}
			case 2:
				if err := d.c.writePIDFrame(mqtt.TypePubrec<<4, pid); err != nil {
					d.failf("drain: PUBREC: %v", err)
					return
				}
			}
			d.count.Add(1)
			select {
			case d.notify <- struct{}{}:
			default:
			}
			if d.onMsg != nil {
				d.onMsg(benchDelivery{Topic: topic, Payload: rest, QoS: qos, Retain: header&0x01 != 0})
			}
		case mqtt.TypePubrel:
			if len(body) < 2 {
				d.failf("drain: PUBREL body %x", body)
				return
			}
			if err := d.c.writePIDFrame(mqtt.TypePubcomp<<4, binary.BigEndian.Uint16(body)); err != nil {
				d.failf("drain: PUBCOMP: %v", err)
				return
			}
		}
	}
}

func (d *benchDrainer) failf(format string, args ...any) {
	d.mu.Lock()
	if d.err == nil {
		d.err = fmt.Errorf(format, args...)
	}
	d.mu.Unlock()
}

// err returns the first drain failure, nil otherwise.
func (d *benchDrainer) errValue() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

// waitCount waits until the drainer received want deliveries. It blocks on the
// drainer's notification channel, not on a poll, and reuses one timer for the
// timeout so a wait in the timed loop allocates nothing.
func (d *benchDrainer) waitCount(tb testing.TB, want int64) {
	tb.Helper()
	if err := d.errValue(); err != nil {
		tb.Fatalf("%v (delivered %d of %d)", err, d.count.Load(), want)
	}
	if d.count.Load() >= want {
		return
	}
	if d.timer == nil {
		d.timer = time.NewTimer(benchWaitTimeout)
	} else {
		d.timer.Reset(benchWaitTimeout)
	}
	defer d.timer.Stop()
	for {
		select {
		case <-d.notify:
		case <-d.timer.C:
			if err := d.errValue(); err != nil {
				tb.Fatalf("%v (delivered %d of %d)", err, d.count.Load(), want)
			}
			if got := d.count.Load(); got < want {
				tb.Fatalf("timed out after %v waiting for delivery: got %d, want %d", benchWaitTimeout, got, want)
			}
			return
		}
		if err := d.errValue(); err != nil {
			tb.Fatalf("%v (delivered %d of %d)", err, d.count.Load(), want)
		}
		if d.count.Load() >= want {
			return
		}
	}
}

// wait blocks until the drain goroutine has ended.
func (d *benchDrainer) wait() {
	select {
	case <-d.done:
	case <-time.After(10 * time.Second):
	}
}

// close closes the subscription connection and waits for the drain goroutine.
func (d *benchDrainer) close() {
	d.c.close()
	d.wait()
}

// waitAll waits until every drainer received want deliveries.
func waitAll(tb testing.TB, drains []*benchDrainer, want int64) {
	tb.Helper()
	for _, d := range drains {
		d.waitCount(tb, want)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks

// BenchmarkPublishQoS0 measures one publisher delivering to one subscriber at
// QoS 0: the publisher writes a PUBLISH and moves on, the subscriber drains the
// deliveries. ns/op is the end-to-end cost of one publication (broker routing
// plus the delivery packet). There is no acknowledgement to wait for, so the
// loop is windowed to keep the subscriber's write-behind queue from overflowing
// (a full queue drops a QoS 0 delivery).
func BenchmarkPublishQoS0(b *testing.B) { benchPublishQoS(b, 0) }

// BenchmarkPublishQoS1 is BenchmarkPublishQoS0 at QoS 1: every publication waits
// for its PUBACK and every delivery is acknowledged by the subscriber, so ns/op
// covers the publisher's round trip and the delivery path.
func BenchmarkPublishQoS1(b *testing.B) { benchPublishQoS(b, 1) }

func benchPublishQoS(b *testing.B, qos byte) {
	const topic = "bench/publish"
	window := int64(64) // QoS 0: a fraction of the broker's 256-slot write queue.
	if qos == 1 {
		window = 16 // QoS 1 deliveries are windowed by the broker's inflight limit.
	}

	broker := startBenchBroker(b, Config{})
	defer stopBenchBroker(b, broker)

	sub := dialBench(b, broker.Addr(), "bench-publish-sub")
	if err := sub.subscribe(1, []string{topic}, qos); err != nil {
		b.Fatalf("subscribe: %v", err)
	}
	drain := startBenchDrainer(sub, nil)
	defer sub.close()

	pub := dialBench(b, broker.Addr(), "bench-publish-pub")
	defer pub.close()

	payload := []byte("0123456789abcdef")
	pid := uint16(0)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		if qos == 0 {
			err = pub.sendPublish(topic, payload, 0, 0, false)
		} else {
			pid = nextPID(pid)
			err = pub.publishWait(topic, payload, qos, pid, false)
		}
		if err != nil {
			b.Fatalf("publish: %v", err)
		}
		if int64(i+1)%window == 0 {
			drain.waitCount(b, int64(i+1))
		}
	}
	b.StopTimer()
	drain.waitCount(b, int64(b.N))
}

// BenchmarkFanout measures one publisher delivering to N subscribers. The
// publication is QoS 0, so ns/op is the broker's per-publication fanout cost;
// the per-delivery cost is ns/op divided by N. The cases run 10 and 100 live
// subscribers, each its own TCP connection and drain goroutine.
func BenchmarkFanout(b *testing.B) {
	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("subscribers=%d", n), func(b *testing.B) {
			benchFanout(b, n)
		})
	}
}

func benchFanout(b *testing.B, subscribers int) {
	const topic = "bench/fanout"
	const window = 8 // keeps every subscriber's write queue far from full

	broker := startBenchBroker(b, Config{})
	defer stopBenchBroker(b, broker)

	subs := make([]*benchWire, subscribers)
	drains := make([]*benchDrainer, subscribers)
	for i := 0; i < subscribers; i++ {
		c := dialBench(b, broker.Addr(), fmt.Sprintf("bench-fanout-sub-%d", i))
		if err := c.subscribe(1, []string{topic}, 0); err != nil {
			b.Fatalf("subscribe %d: %v", i, err)
		}
		subs[i] = c
		drains[i] = startBenchDrainer(c, nil)
	}
	defer func() {
		for _, c := range subs {
			c.close()
		}
	}()

	pub := dialBench(b, broker.Addr(), "bench-fanout-pub")
	defer pub.close()

	payload := []byte("0123456789abcdef")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.sendPublish(topic, payload, 0, 0, false); err != nil {
			b.Fatalf("publish: %v", err)
		}
		if (i+1)%window == 0 {
			waitAll(b, drains, int64(i+1))
		}
	}
	b.StopTimer()
	waitAll(b, drains, int64(b.N))
}

// BenchmarkManySubscriptions measures a publication against a broker holding a
// lot of live subscriptions, when exactly one of them matches. It is the
// routing-index regression check: ns/op must not grow with the subscription
// count, because the index answers an exact match with one map lookup and the
// end-to-end cost is dominated by the two socket hops, not by routing.
//
// The exact cases run 100 and 10000 subscriptions on one connection. The
// wildcard case adds, on top of the 10000, 100 wildcard filters that do not
// match the published topic: a wildcard filter anywhere in the index forces the
// general path, which runs matchTopic against every distinct wildcard filter and
// merges the matches per session, so the difference to the exact case is the
// documented cost of that scan.
func BenchmarkManySubscriptions(b *testing.B) {
	b.Run("exact/subs=100", func(b *testing.B) {
		benchManySubscriptions(b, 100, 0)
	})
	b.Run("exact/subs=10000", func(b *testing.B) {
		benchManySubscriptions(b, 10000, 0)
	})
	b.Run("wildcard=100/subs=10000", func(b *testing.B) {
		benchManySubscriptions(b, 10000, 100)
	})
}

func benchManySubscriptions(b *testing.B, subscriptions, wildcards int) {
	const (
		subscribeBatch = 500
		window         = 64 // a fraction of the broker's 256-slot write queue
	)
	// The published topic is one of the subscriptions, so exactly one session is
	// a target no matter how many filters the broker holds.
	topic := fmt.Sprintf("bench/subs/%05d", subscriptions/2)

	broker := startBenchBroker(b, Config{})
	defer stopBenchBroker(b, broker)

	sub := dialBench(b, broker.Addr(), "bench-many-sub")
	filters := make([]string, 0, subscribeBatch)
	pid := uint16(0)
	flushFilters := func() {
		if len(filters) == 0 {
			return
		}
		pid = nextPID(pid)
		if err := sub.subscribe(pid, filters, 0); err != nil {
			b.Fatalf("subscribe (%d filters): %v", len(filters), err)
		}
		filters = filters[:0]
	}
	for i := 0; i < subscriptions; i++ {
		filters = append(filters, fmt.Sprintf("bench/subs/%05d", i))
		if len(filters) == subscribeBatch {
			flushFilters()
		}
	}
	flushFilters()
	// Wildcard filters that cannot match the published topic: they still cost the
	// per-publication match the general path pays, while the publication keeps
	// exactly one target.
	for i := 0; i < wildcards; i++ {
		filters = append(filters, fmt.Sprintf("bench/other/%03d/+", i))
	}
	flushFilters()

	drain := startBenchDrainer(sub, nil)
	defer sub.close()

	pub := dialBench(b, broker.Addr(), "bench-many-pub")
	defer pub.close()

	payload := []byte("0123456789abcdef")
	b.ReportAllocs()
	b.ReportMetric(float64(subscriptions+wildcards), "subs")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pub.sendPublish(topic, payload, 0, 0, false); err != nil {
			b.Fatalf("publish: %v", err)
		}
		if (i+1)%window == 0 {
			drain.waitCount(b, int64(i+1))
		}
	}
	b.StopTimer()
	drain.waitCount(b, int64(b.N))
}

// BenchmarkRetainedReplay measures a subscriber joining a broker that holds 1000
// retained topics: connect, SUBSCRIBE, receive the whole retained replay,
// disconnect. ns/op is one such join; the broker scans its retained store on
// every subscription, so this is the cost of that scan plus the deliveries.
//
// The retained messages are seeded at QoS 1 and replayed at QoS 1, which keeps
// the replay lossless: a QoS 0 retained replay is dropped when the connection's
// write-behind queue fills, which would measure the queue limit instead of the
// replay.
func BenchmarkRetainedReplay(b *testing.B) {
	const (
		retained = 1000
		prefix   = "bench/retained/"
	)
	broker := startBenchBroker(b, Config{MaxInflightQueueSize: 4096})
	defer stopBenchBroker(b, broker)

	seed := dialBench(b, broker.Addr(), "bench-retained-seed")
	pid := uint16(0)
	payload := []byte("retained-payload")
	for i := 0; i < retained; i++ {
		pid = nextPID(pid)
		topic := fmt.Sprintf("%s%04d", prefix, i)
		if err := seed.publishWait(topic, payload, 1, pid, true); err != nil {
			b.Fatalf("seed retained %s: %v", topic, err)
		}
	}
	seed.close()

	// Client ids are precomputed so the timed region does not format strings.
	clientIDs := make([]string, b.N)
	for i := range clientIDs {
		clientIDs[i] = fmt.Sprintf("bench-retained-%d", i)
	}

	b.ReportAllocs()
	b.ReportMetric(retained, "retained/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			c := dialBench(b, broker.Addr(), clientIDs[i])
			defer c.close()
			if err := c.subscribe(1, []string{prefix + "+"}, 1); err != nil {
				b.Fatalf("subscribe: %v", err)
			}
			drain := startBenchDrainer(c, nil)
			drain.waitCount(b, retained)
			drain.close()
		}()
	}
}

// BenchmarkClientPublishQoS1 measures the client package publishing QoS 1 to the
// broker in a loop. Each operation is one end-to-end round trip: the client
// queues the PUBLISH, the broker routes it to a subscriber and answers PUBACK,
// and the client returns from Publish when the PUBACK arrives. The subscriber is
// the raw wire drainer above, so the measured cost is the client's publish path
// plus the broker, not a second client instance.
func BenchmarkClientPublishQoS1(b *testing.B) {
	const (
		topic  = "bench/client"
		window = 16
	)
	broker := startBenchBroker(b, Config{})
	defer stopBenchBroker(b, broker)

	sub := dialBench(b, broker.Addr(), "bench-client-sub")
	if err := sub.subscribe(1, []string{topic}, 1); err != nil {
		b.Fatalf("subscribe: %v", err)
	}
	drain := startBenchDrainer(sub, nil)
	defer sub.close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cl, err := mqttclient.Dial(ctx, mqttclient.Config{
		Addr:       broker.Addr(),
		ClientID:   "bench-client-pub",
		CleanStart: true,
	})
	if err != nil {
		b.Fatalf("client.Dial: %v", err)
	}
	defer func() {
		if err := cl.Close(); err != nil {
			b.Errorf("client.Close: %v", err)
		}
	}()

	payload := []byte("0123456789abcdef")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cl.Publish(ctx, topic, payload, 1, false); err != nil {
			b.Fatalf("client.Publish: %v", err)
		}
		if (i+1)%window == 0 {
			drain.waitCount(b, int64(i+1))
		}
	}
	b.StopTimer()
	drain.waitCount(b, int64(b.N))
}
