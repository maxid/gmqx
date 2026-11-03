package engine

// The connection entry point and the Start lifecycle.
//
// handleRaw is the one function every accepted connection of every listener passes
// through, and it is where the broker's two connection-level limits live: the accept
// rate limiter and MaxConnections. Neither had a test, so both counters and both
// refusals were unverified. Start had a similar hole: the already-started and
// closed guards, the no-listener guard and the rollback of listeners that were
// already bound when a later one failed were all uncovered, which is what the
// "a failed Start leaves nothing running behind" guarantee rests on.

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// TestStartTwiceFails pins the guard: a start on a started broker is an error, not
// a second set of listeners on the same configuration.
func TestStartTwiceFails(t *testing.T) {
	b := startBroker(t, Config{})
	err := b.Start(context.Background())
	if err == nil {
		t.Fatal("Start on a started broker succeeded")
	}
	if !strings.Contains(err.Error(), "already started") {
		t.Fatalf("Start error = %v, want the already-started error", err)
	}
	// The running listeners are untouched by the refused call.
	if b.Addrs()["tcp"] != b.Addr() || b.Addr() == "" {
		t.Fatalf("Addrs() after a refused Start = %v", b.Addrs())
	}
}

// TestStartAfterStopFails pins the other guard: a stopped broker is closed for
// good; Start does not resurrect it.
func TestStartAfterStopFails(t *testing.T) {
	b, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = b.Start(context.Background())
	if err == nil {
		t.Fatal("Start on a stopped broker succeeded")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Start error = %v, want the closed error", err)
	}
}

// TestStartWithoutListenersFails covers the last of Start's guards. New always
// derives a TCP address, so the only way to reach it is a config with every address
// cleared — the guard is defensive, and this test keeps it from silently
// disappearing.
func TestStartWithoutListenersFails(t *testing.T) {
	b, err := New(Config{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	b.cfg.Addr = ""
	err = b.Start(context.Background())
	if err == nil {
		t.Fatal("Start without any listener succeeded")
	}
	if !strings.Contains(err.Error(), "no listeners configured") {
		t.Fatalf("Start error = %v, want the no-listeners error", err)
	}
}

// TestStartRollsBackWhenALaterListenerFails is the multi-listener half of the
// rollback guarantee: the TCP listener binds first, then the TLS listener fails on a
// certificate that does not exist. Start must stop the listener it already started
// and leave no address behind.
func TestStartRollsBackWhenALaterListenerFails(t *testing.T) {
	b, err := New(Config{
		Addr:       "127.0.0.1:0",
		SecureAddr: "127.0.0.1:0",
		TLS: TLSConfig{
			Enabled: true,
			Cert:    "/nonexistent/gmqx/cert.pem",
			Key:     "/nonexistent/gmqx/key.pem",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = b.Start(context.Background())
	if err == nil {
		_ = b.Stop(context.Background())
		t.Fatal("Start succeeded with a certificate that does not exist")
	}
	if !strings.Contains(err.Error(), "tls listener") {
		t.Fatalf("Start error = %v, want the failing listener named", err)
	}
	// The TCP listener that bound first was stopped: no address is reported, so
	// nothing of the failed Start is still listening.
	if addrs := b.Addrs(); len(addrs) != 0 {
		t.Fatalf("Addrs() after a failed Start = %v, want none", addrs)
	}
}

// TestConnectionRateLimitDropsExcessConnections drives the accept-rate limiter
// through the real listeners: one token is available, so of five immediate
// connections only the first can complete a CONNECT. The assertion is "at least
// one refused, at least one accepted" rather than an exact split, because the
// bucket refills at one connection per second — a slow test run may legitimately
// admit a later dial, and the counter is what the limit is about either way.
func TestConnectionRateLimitDropsExcessConnections(t *testing.T) {
	metrics := &dropRecorder{}
	b := startBroker(t, Config{ConnectionRateLimit: 1, Metrics: metrics})

	accepted, refused := 0, 0
	for i := 0; i < 5; i++ {
		conn, err := net.Dial("tcp", b.Addr())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		client := &testClient{conn: conn, r: bufio.NewReader(conn)}
		client.writePacket(0x10, connectPayload(fmt.Sprintf("rate-%d", i)))
		if header, _, err := client.readPacket(); err == nil && header>>4 == 2 {
			accepted++
		} else {
			refused++
		}
		_ = conn.Close()
	}
	if accepted == 0 {
		t.Fatal("no connection was accepted with a rate limit of 1/s")
	}
	if refused == 0 {
		t.Fatal("every connection was accepted with a rate limit of 1/s")
	}
	if got := metrics.count("connection_rate_limit"); got != refused {
		t.Fatalf("connection_rate_limit drops = %d, refused connections = %d", got, refused)
	}
}

// TestMaxConnectionsRejectsNewConnection is the deterministic half of the same
// entry point: with MaxConnections at one, the second connection is closed before
// it is registered, so its CONNECT is never answered.
func TestMaxConnectionsRejectsNewConnection(t *testing.T) {
	metrics := &dropRecorder{}
	b := startBroker(t, Config{MaxConnections: 1, Metrics: metrics})
	first := dialTestClient(t, b.Addr(), "within-limit")
	t.Cleanup(func() { _ = first.conn.Close() })

	second, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = second.Close() }()
	_ = second.SetDeadline(time.Now().Add(3 * time.Second))
	c := &testClient{conn: second, r: bufio.NewReader(second)}
	c.writePacket(0x10, connectPayload("over-limit"))
	if header, _, err := c.readPacket(); err == nil {
		t.Fatalf("the broker answered header %x with MaxConnections at 1", header)
	}
	if got := metrics.count("max_connections"); got != 1 {
		t.Fatalf("max_connections drops = %d, want 1", got)
	}
}
