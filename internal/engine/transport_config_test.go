package engine

// The WebSocket listener's configuration actually reaching the listener.
//
// Every existing WebSocket test used the path "/mqtt", which is also the default,
// and none used the address-or-port form or a wrong subprotocol. A regression that
// ignored Config.WebsocketPath (or WebsocketPort) would therefore have passed the
// whole suite: the tests asserted the transport, not the configuration.

import (
	"crypto/tls"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/http/websocket"
)

// dialWebSocketPath attempts a WebSocket handshake with an explicit path and
// returns the handshake error, so a case can assert that a path is refused.
func dialWebSocketPath(t *testing.T, addr, path, subprotocol string) (net.Conn, error) {
	t.Helper()
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	_ = raw.SetDeadline(time.Now().Add(3 * time.Second))
	location := &url.URL{Scheme: "ws", Host: addr, Path: path}
	conn, err := websocket.ClientHandshake(raw, location, "http://"+addr, websocket.Options{
		Subprotocol:    subprotocol,
		MaxMessageSize: testWebSocketReadLimit,
	})
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, nil
}

// TestWebsocketPathIsApplied pins that Config.WebsocketPath is the path the
// listener serves, and that another path is refused: the default and the
// configured value are the same string in every other test, so this is the only
// case that can tell them apart.
func TestWebsocketPathIsApplied(t *testing.T) {
	b := startBroker(t, Config{
		Addr:          "127.0.0.1:0",
		WebsocketAddr: "127.0.0.1:0",
		WebsocketPath: "/custom-mqtt",
	})
	addr := b.WebsocketAddr()

	conn, err := dialWebSocketPath(t, addr, "/custom-mqtt", "mqtt")
	if err != nil {
		t.Fatalf("handshake on the configured path: %v", err)
	}
	connectOverConn(t, conn, "custom-path")

	if _, err := dialWebSocketPath(t, addr, "/mqtt", "mqtt"); err == nil {
		t.Fatal("the listener accepted the default path while configured for /custom-mqtt")
	}
}

// TestWebsocketPathDefaults pins the other direction: a config that names no path
// serves the documented default.
func TestWebsocketPathDefaults(t *testing.T) {
	b := startBroker(t, Config{
		Addr:          "127.0.0.1:0",
		WebsocketAddr: "127.0.0.1:0",
	})
	conn, err := dialWebSocketPath(t, b.WebsocketAddr(), "/mqtt", "mqtt")
	if err != nil {
		t.Fatalf("handshake on the default path: %v", err)
	}
	connectOverConn(t, conn, "default-path")
}

// TestWebsocketAddressOrPortResolution pins the address-or-port form of both
// WebSocket listeners. The specs are resolved without binding, so no test depends
// on the documented ports being free on the machine.
func TestWebsocketAddressOrPortResolution(t *testing.T) {
	certFile, keyFile := selfSignedCert(t)
	b, err := New(Config{
		Addr:                "127.0.0.1:0",
		TLS:                 TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
		WebsocketPort:       1884,
		WebsocketSecurePort: 8884,
	})
	if err != nil {
		t.Fatal(err)
	}
	specs, err := b.listenerSpecs()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]string, len(specs))
	for _, spec := range specs {
		got[spec.name] = spec.addr
	}
	if want := ":1884"; got["ws"] != want {
		t.Fatalf("ws listener address = %q, want %q", got["ws"], want)
	}
	if want := ":8884"; got["wss"] != want {
		t.Fatalf("wss listener address = %q, want %q", got["wss"], want)
	}
}

// TestWebSocketRejectsWrongSubprotocol completes the subprotocol pair: a client
// that offers a different subprotocol is refused, not served.
func TestWebSocketRejectsWrongSubprotocol(t *testing.T) {
	b := startBroker(t, Config{
		Addr:          "127.0.0.1:0",
		WebsocketAddr: "127.0.0.1:0",
	})
	if _, err := dialWebSocketPath(t, b.WebsocketAddr(), "/mqtt", "graphql-ws"); err == nil {
		t.Fatal("the listener accepted a client with the wrong subprotocol")
	}
}

// TestWebSocketListenerKeepsTLSOut makes sure the two WebSocket listeners stay
// distinct: the plaintext listener does not speak TLS and the secure one does.
func TestWebSocketListenerKeepsTLSOut(t *testing.T) {
	certFile, keyFile := selfSignedCert(t)
	b := startBroker(t, Config{
		Addr:                "127.0.0.1:0",
		SecureAddr:          "127.0.0.1:0",
		TLS:                 TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
		WebsocketAddr:       "127.0.0.1:0",
		WebsocketSecureAddr: "127.0.0.1:0",
		WebsocketPath:       "/mqtt",
	})
	pool := certPool(t, certFile)

	// A TLS client on the plaintext listener fails the handshake.
	if conn, err := tls.Dial("tcp", b.WebsocketAddr(), &tls.Config{RootCAs: pool}); err == nil {
		_ = conn.Close()
		t.Fatal("the plaintext WebSocket listener completed a TLS handshake")
	}
	// A plaintext client on the secure listener does too.
	if conn, err := net.Dial("tcp", b.WebsocketSecureAddr()); err == nil {
		_, err := websocket.ClientHandshake(conn, &url.URL{Scheme: "ws", Host: b.WebsocketSecureAddr(), Path: "/mqtt"},
			"http://"+b.WebsocketSecureAddr(), websocket.Options{Subprotocol: "mqtt"})
		_ = conn.Close()
		if err == nil {
			t.Fatal("the WSS listener completed a plaintext WebSocket handshake")
		}
	}
}
