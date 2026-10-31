package gmqx

// Byte-rate shaping
//
// Config.MaxReadByteRate/MaxWriteByteRate bound the bytes one connection may move
// per second and MaxTotalReadByteRate/MaxTotalWriteByteRate bound the same for the
// whole broker. Both are token buckets: one bucket per connection and direction
// (created when the connection is accepted) and one broker-wide bucket per
// direction (created by New). A transfer is charged to both, and the longer of the
// two waits is taken, so a single connection is never faster than its own bucket
// and the sum of all connections is never faster than the global one.
//
// The policy of the excess is delay. A read charges the bucket after the bytes
// arrived and a write charges it before the bytes leave, so the peer is slowed down
// by TCP backpressure: nothing is dropped, nothing is truncated and no packet is
// split — an MQTT stream cannot survive a hole, which is why a shaper that cut
// bytes would be a corruption bug rather than a limit. A disconnect was the other
// candidate and was rejected: a burst is not a protocol violation, and closing the
// connection would throw away a persistent session's inflight windows, packet ids
// and queue for a client that only spoke too fast for a moment. What bounds a peer
// that ignores the backpressure is the deadline it then misses (WriteTimeout for a
// write, the keep-alive read deadline for a read), not the shaper itself.
//
// A write whose wait cannot fit in the write deadline of the packet is reported as
// os.ErrDeadlineExceeded without writing the packet, so WriteTimeout stays the
// bound on how long one packet may take and the connection is closed by the normal
// write-failure path instead of sleeping past every deadline it has.
//
// The numbers, as documented defaults: 8 MiB/s read and 16 MiB/s write per
// connection, 256 MiB/s each across the broker. A bucket bursts one second of its
// rate (at least 16 KiB), so an ordinary MQTT packet — even the 4 MiB default
// MaxPacketSize — passes without a wait, and the limits only engage on sustained
// throughput far above what a device broker serves.
//
// Throttled bytes are counted: every chunk that had to wait is reported through the
// optional ByteRateMetrics extension of MetricsManager, and the wait itself as
// Metrics.RecordOverflow("byte_rate_read"/"byte_rate_write").

import (
	"net"
	"os"
	"sync"
	"time"
)

// Defaults of the byte-rate limits. They are high enough to leave normal traffic
// untouched: a device publishing a few kilobytes per second is three orders of
// magnitude below them, and the burst absorbs any single maximum-size packet.
const (
	defaultMaxReadByteRate       = 8 << 20   // 8 MiB/s per connection
	defaultMaxWriteByteRate      = 16 << 20  // 16 MiB/s per connection
	defaultMaxTotalReadByteRate  = 256 << 20 // 256 MiB/s for the whole broker
	defaultMaxTotalWriteByteRate = 256 << 20
	// minByteRateBurst is the floor of a bucket's burst: one second of traffic can
	// be smaller than the largest packet a broker accepts, and a bucket without
	// room for one packet would delay even the first message of a connection.
	minByteRateBurst = 16 << 10
)

// ByteRateMetrics is the optional extension of MetricsManager a metrics
// implementation can add to observe byte-rate shaping. direction is "read" or
// "write"; n is the number of bytes whose transfer had to wait for a token.
//
// It is an optional interface instead of a method on MetricsManager so that a
// deployment's existing implementation keeps compiling: a broker whose metrics do
// not implement it still counts the waits through
// MetricsManager.RecordOverflow("byte_rate_read"/"byte_rate_write").
type ByteRateMetrics interface {
	RecordThrottledBytes(direction string, n int)
}

// byteLimiter is a token bucket that reports how long a reservation has to wait
// instead of blocking under its own lock, so a hundred connections share one
// bucket without serialising on a sleep.
//
// A reservation is allowed to drive the bucket negative: the deficit is the debt
// the following reservations queue behind, which is what makes concurrent
// reservations fair and keeps the long-term rate at exactly rate bytes/second.
type byteLimiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

// newByteLimiter returns the bucket of limit bytes per second, or nil when the
// limit is disabled (a non-positive value).
func newByteLimiter(limit int) *byteLimiter {
	if limit <= 0 {
		return nil
	}
	burst := float64(limit)
	if burst < minByteRateBurst {
		burst = minByteRateBurst
	}
	return &byteLimiter{
		rate:   float64(limit),
		burst:  burst,
		tokens: burst,
		last:   time.Now(),
	}
}

// reserve claims n bytes and reports how long the caller has to wait before the
// claim is paid for. A nil bucket (a disabled limit) never waits.
func (l *byteLimiter) reserve(n int) time.Duration {
	if l == nil || n <= 0 || l.rate <= 0 {
		return 0
	}
	l.mu.Lock()
	now := time.Now()
	l.tokens += now.Sub(l.last).Seconds() * l.rate
	l.last = now
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	l.tokens -= float64(n)
	var wait time.Duration
	if l.tokens < 0 {
		wait = time.Duration(-l.tokens / l.rate * float64(time.Second))
	}
	l.mu.Unlock()
	return wait
}

// shapeConn wraps conn in the broker's byte-rate buckets. A broker whose limits
// are all disabled gets the connection back unchanged, so the wrapper costs
// nothing unless it was asked for.
func (b *Broker) shapeConn(conn net.Conn) net.Conn {
	read := newByteLimiter(b.cfg.MaxReadByteRate)
	write := newByteLimiter(b.cfg.MaxWriteByteRate)
	if read == nil && write == nil && b.readBytes == nil && b.writeBytes == nil {
		return conn
	}
	return &shapedConn{
		Conn:       conn,
		broker:     b,
		read:       read,
		write:      write,
		readTotal:  b.readBytes,
		writeTotal: b.writeBytes,
	}
}

// shapedConn applies the per-connection and broker-wide byte-rate limits to one
// connection. It is a net.Conn: the read deadline is forwarded to the underlying
// transport (nothing else needs it), and the write deadline is remembered as well,
// so a shaping wait that cannot fit inside it is reported as the deadline instead
// of sleeping past it.
type shapedConn struct {
	net.Conn
	broker *Broker
	// read/write are the per-connection buckets, readTotal/writeTotal the
	// broker-wide ones. Any of them may be nil (that limit is disabled).
	read, write           *byteLimiter
	readTotal, writeTotal *byteLimiter
	// mu guards writeDeadline, which is the deadline of the packet being
	// written; the shaper needs it to refuse a wait that cannot fit.
	mu            sync.Mutex
	writeDeadline time.Time
}

// Read reads from the peer and charges the bytes to the read buckets. The wait
// happens after the read: the bytes are already ours, and the delay is what keeps
// the *next* read from arriving too early. The read deadline of the connection is
// not consulted here — the caller armed it for the read that already happened, and
// a shaping wait cannot un-read the bytes; the deadline of the following read is
// what closes a peer that keeps overrunning its rate.
func (s *shapedConn) Read(p []byte) (int, error) {
	n, err := s.Conn.Read(p)
	if n > 0 {
		if wait := maxDuration(s.read.reserve(n), s.readTotal.reserve(n)); wait > 0 {
			s.countThrottled("read", n)
			time.Sleep(wait)
		}
	}
	return n, err
}

// Write charges the bytes to the write buckets and then writes them. A wait that
// would exceed the write deadline of the packet is the deadline itself: the packet
// is not written at all, and the connection is closed by the normal write-failure
// path (see Config.WriteTimeout).
func (s *shapedConn) Write(p []byte) (int, error) {
	wait := maxDuration(s.write.reserve(len(p)), s.writeTotal.reserve(len(p)))
	if wait > 0 {
		s.countThrottled("write", len(p))
		if err := s.wait(wait, s.currentWriteDeadline()); err != nil {
			return 0, err
		}
	}
	return s.Conn.Write(p)
}

// wait sleeps for d, bounded by the packet's deadline. A wait that does not fit in
// what the deadline allows is reported as os.ErrDeadlineExceeded instead of
// sleeping past it.
func (s *shapedConn) wait(d time.Duration, deadline time.Time) error {
	if !deadline.IsZero() {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return os.ErrDeadlineExceeded
		}
		if remaining < d {
			time.Sleep(remaining)
			return os.ErrDeadlineExceeded
		}
	}
	time.Sleep(d)
	return nil
}

// countThrottled reports bytes whose transfer had to wait.
func (s *shapedConn) countThrottled(direction string, n int) {
	s.broker.metrics.RecordOverflow("byte_rate_" + direction)
	if m, ok := s.broker.metrics.(ByteRateMetrics); ok {
		m.RecordThrottledBytes(direction, n)
	}
}

func (s *shapedConn) currentWriteDeadline() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeDeadline
}

// SetWriteDeadline forwards the deadline to the transport and remembers it for the
// shaper.
func (s *shapedConn) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	s.writeDeadline = t
	s.mu.Unlock()
	return s.Conn.SetWriteDeadline(t)
}

// maxDuration returns the longer of two waits.
func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
