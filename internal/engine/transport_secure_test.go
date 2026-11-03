package engine

// The secure listeners: WSS and the configuration errors of the TLS/WSS pair.
//
// transport_test.go proves a CONNECT/CONNACK over a TLS listener and over a plain
// WebSocket listener, and that the four listener names reach Addrs(). That leaves
// the WSS listener never dialed, the two errors listenerSpecs can return never
// returned, and the documented default port of a TLS listener never resolved.
// transport_mtls_test.go carries the RequireClientCert half of TLSConfig.

import (
	"bufio"
	"context"
	"crypto/tls"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/http/websocket"
)

// dialTestWebSocket completes a WebSocket handshake over a fresh (TLS for "wss")
// connection and hands back the stream, so the raw MQTT helpers can drive it.
func dialTestWebSocket(t *testing.T, network, addr, path string, tlsCfg *tls.Config) net.Conn {
	t.Helper()
	var (
		raw net.Conn
		err error
	)
	switch network {
	case "ws":
		raw, err = net.Dial("tcp", addr)
	case "wss":
		raw, err = tls.Dial("tcp", addr, tlsCfg)
	default:
		t.Fatalf("unsupported test transport %q", network)
	}
	if err != nil {
		t.Fatalf("%s dial %s: %v", network, addr, err)
	}
	location := &url.URL{Scheme: network, Host: addr, Path: path}
	conn, err := websocket.ClientHandshake(raw, location, "http://"+addr, websocket.Options{Subprotocol: "mqtt"})
	if err != nil {
		_ = raw.Close()
		t.Fatalf("%s handshake over %s: %v", network, addr, err)
	}
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// connectOverConn sends a CONNECT over an established stream and requires a
// successful, non-session-present CONNACK.
func connectOverConn(t *testing.T, conn net.Conn, clientID string) *testClient {
	t.Helper()
	c := &testClient{conn: conn, r: bufio.NewReader(conn)}
	c.writePacket(0x10, connectPayload(clientID))
	header, payload, err := c.readPacket()
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if header>>4 != 2 || len(payload) != 2 || payload[1] != 0 {
		t.Fatalf("unexpected connack: %x %v", header, payload)
	}
	return c
}

func TestWSSListener(t *testing.T) {
	certFile, keyFile := selfSignedCert(t)
	b := startBroker(t, Config{
		Addr:                "127.0.0.1:0",
		TLS:                 TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
		WebsocketSecureAddr: "127.0.0.1:0",
		WebsocketPath:       "/mqtt",
	})
	addr := b.WebsocketSecureAddr()
	if addr == "" {
		t.Fatal("expected WSS listener address")
	}
	conn := dialTestWebSocket(t, "wss", addr, "/mqtt", &tls.Config{RootCAs: certPool(t, certFile)})
	connectOverConn(t, conn, "wss-client")
}

// TestTLSListenerAddressResolution pins what a TLS listener with no explicit
// address binds to. It resolves the spec instead of binding :8883, because a test
// that binds the documented port fails on any machine that already runs an MQTT
// broker there.
func TestTLSListenerAddressResolution(t *testing.T) {
	certFile, keyFile := selfSignedCert(t)
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"default port", Config{}, ":8883"},
		{"SecurePort", Config{SecurePort: 9999}, ":9999"},
		{"SecureAddr wins over SecurePort", Config{SecureAddr: "127.0.0.1:9443", SecurePort: 9999}, "127.0.0.1:9443"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.Addr = "127.0.0.1:0"
			tc.cfg.TLS = TLSConfig{Enabled: true, Cert: certFile, Key: keyFile}
			b, err := New(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			specs, err := b.listenerSpecs()
			if err != nil {
				t.Fatal(err)
			}
			got := ""
			for _, spec := range specs {
				if spec.name == "tls" {
					got = spec.addr
				}
			}
			if got != tc.want {
				t.Fatalf("TLS listener address = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTLSListenerRequiresCertificateAndKey(t *testing.T) {
	b, err := New(Config{Addr: "127.0.0.1:0", TLS: TLSConfig{Enabled: true}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = b.Start(context.Background())
	if err == nil {
		_ = b.Stop(context.Background())
		t.Fatal("Start succeeded with TLS enabled and no certificate")
	}
	if !strings.Contains(err.Error(), "TLS requires both Cert and Key") {
		t.Fatalf("Start error = %v, want the missing-certificate error", err)
	}
	if addrs := b.Addrs(); len(addrs) != 0 {
		t.Fatalf("Addrs() after a failed Start = %v, want none", addrs)
	}
}

func TestWSSListenerRequiresTLS(t *testing.T) {
	b, err := New(Config{Addr: "127.0.0.1:0", WebsocketSecureAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = b.Start(context.Background())
	if err == nil {
		_ = b.Stop(context.Background())
		t.Fatal("Start succeeded with a WSS listener and TLS disabled")
	}
	if !strings.Contains(err.Error(), "WSS requires TLS.Enabled") {
		t.Fatalf("Start error = %v, want the WSS-needs-TLS error", err)
	}
	if addrs := b.Addrs(); len(addrs) != 0 {
		t.Fatalf("Addrs() after a failed Start = %v, want none", addrs)
	}
}
