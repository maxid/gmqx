package gmqx

// Listener port fallback.
//
// When the configured port of a listener is already in use, Start tries the next
// ports (Config.PortFallback, default 16) instead of failing. The bind is the probe:
// every attempt really binds a port, so there is no check-then-bind race with
// another process, and the first successful bind wins. Addrs() reports the address
// that was really bound, and the transport (TCP, TLS, WS, WSS) is kept — only the
// port changes.

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/maxid/gnety"
)

// occupyPortWithFreeNext binds a loopback port and returns it together with the
// listener that keeps it busy. The next port is verified to be free (and then
// released), so a case can assert the exact port the broker falls back to instead
// of a range.
func occupyPortWithFreeNext(t *testing.T) (int, net.Listener) {
	t.Helper()
	for attempt := 0; attempt < 32; attempt++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("bind an occupied port: %v", err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		if port >= 65535 {
			_ = ln.Close()
			continue
		}
		probe, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port+1))
		if err != nil {
			// The successor is busy; another ephemeral port may have a free one.
			_ = ln.Close()
			continue
		}
		_ = probe.Close()
		return port, ln
	}
	t.Fatal("could not find a loopback port whose successor is free")
	return 0, nil
}

// startOnBusyPort occupies a loopback port, starts a broker whose listener is
// configured on it and requires the listener to land on the very next port. The
// probe that the successor is free cannot reserve it — an unrelated process may
// take it before the broker binds — so a case that sees a further port retries with
// a fresh pair instead of depending on the machine's port churn.
func startOnBusyPort(t *testing.T, cfg func(addr string) Config, addrOf func(*Broker) string) (*Broker, int) {
	t.Helper()
	for attempt := 0; attempt < 8; attempt++ {
		port, busy := occupyPortWithFreeNext(t)
		b, err := New(cfg(fmt.Sprintf("127.0.0.1:%d", port)))
		if err != nil {
			_ = busy.Close()
			t.Fatal(err)
		}
		if err := b.Start(context.Background()); err != nil {
			_ = busy.Close()
			t.Fatalf("Start on a busy port with the fallback enabled: %v", err)
		}
		if got, want := addrOf(b), fmt.Sprintf("127.0.0.1:%d", port+1); got == want {
			_ = busy.Close()
			t.Cleanup(func() { _ = b.Stop(context.Background()) })
			return b, port
		}
		_ = b.Stop(context.Background())
		_ = busy.Close()
	}
	t.Skip("another process kept taking the successor port; cannot assert the exact fallback port")
	return nil, 0
}

func TestPortFallbackBindsNextPort(t *testing.T) {
	b, port := startOnBusyPort(t,
		func(addr string) Config { return Config{Addr: addr} },
		func(b *Broker) string { return b.Addr() })

	want := fmt.Sprintf("127.0.0.1:%d", port+1)
	if got := b.Addr(); got != want {
		t.Fatalf("Addr() = %q, want the fallback %q", got, want)
	}
	if got := b.Addrs()["tcp"]; got != want {
		t.Fatalf("Addrs()[tcp] = %q, want %q", got, want)
	}

	// The reported address is the one that really serves MQTT.
	c, ack := mustConnect(t, b.Addr(), confConnect{clientID: "fallback-client", clean: true})
	if ack.SessionPresent {
		t.Fatal("a new session reports Session Present")
	}
	c.disconnect()
}

func TestPortFallbackDisabledFails(t *testing.T) {
	port, busy := occupyPortWithFreeNext(t)
	defer func() { _ = busy.Close() }()

	b, err := New(Config{Addr: fmt.Sprintf("127.0.0.1:%d", port), PortFallback: -1})
	if err != nil {
		t.Fatal(err)
	}
	err = b.Start(context.Background())
	if err == nil {
		_ = b.Stop(context.Background())
		t.Fatal("Start succeeded on a busy port with the fallback disabled")
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("Start error = %v, want an address-in-use error", err)
	}
	// A failed Start leaves nothing running behind.
	if addrs := b.Addrs(); len(addrs) != 0 {
		t.Fatalf("Addrs() after a failed Start = %v, want none", addrs)
	}
}

func TestPortFallbackKeepsWebSocketTransport(t *testing.T) {
	b, port := startOnBusyPort(t,
		func(addr string) Config {
			return Config{Addr: "127.0.0.1:0", WebsocketAddr: addr, WebsocketPath: "/mqtt"}
		},
		func(b *Broker) string { return b.WebsocketAddr() })

	want := fmt.Sprintf("127.0.0.1:%d", port+1)
	if got := b.WebsocketAddr(); got != want {
		t.Fatalf("WebsocketAddr() = %q, want the fallback %q", got, want)
	}

	// The fallback listener is still a WebSocket listener: an MQTT WebSocket client
	// completes a CONNECT over it.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cli, err := gnety.Dial(ctx, gnety.ClientConfig{
		Network: "ws", Addr: b.WebsocketAddr(), Path: "/mqtt", Subprotocol: "mqtt", Codec: webSocketConnectCodec{},
	})
	if err != nil {
		t.Fatalf("ws dial to the fallback listener: %v", err)
	}
	defer func() { _ = cli.Close() }()
	if err := cli.Send("fallback-ws"); err != nil {
		t.Fatal(err)
	}
	msg, err := cli.Receive(ctx)
	if err != nil {
		t.Fatalf("read connack over the fallback listener: %v", err)
	}
	if code := msg.(byte); code != 0 {
		t.Fatalf("connack code = %d", code)
	}
}

func TestPortFallbackKeepsTLSListener(t *testing.T) {
	certFile, keyFile := selfSignedCert(t)
	b, port := startOnBusyPort(t,
		func(addr string) Config {
			return Config{
				Addr:       "127.0.0.1:0",
				TLS:        TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
				SecureAddr: addr,
			}
		},
		func(b *Broker) string { return b.SecureAddr() })

	want := fmt.Sprintf("127.0.0.1:%d", port+1)
	if got := b.SecureAddr(); got != want {
		t.Fatalf("SecureAddr() = %q, want the fallback %q", got, want)
	}

	conn, err := tls.Dial("tcp", b.SecureAddr(), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("tls dial to the fallback listener: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeConnect(conn, "fallback-tls"); err != nil {
		t.Fatal(err)
	}
	if code, err := readConnack(conn); err != nil || code != 0 {
		t.Fatalf("CONNACK over the fallback TLS listener = %d, %v", code, err)
	}
}
