package engine

// hook.ConnInfo.Transport: a hook can tell which listener accepted a connection.
//
// ConnInfo carried the client id, the connection id, the namespace, the peer
// address and the protocol version, but nothing identified the transport, so an
// audit trail or an ACL could not distinguish a device on the TLS port from one on
// the WebSocket port — the four listeners the broker advertises were invisible to
// every hook. The transport now travels from the listener spec through conn.Client
// and lands in ConnInfo.Transport, with the same names Addrs() reports.

import (
	"crypto/tls"
	"net"
	"testing"
	"time"
)

func TestConnInfoTransport(t *testing.T) {
	certFile, keyFile := selfSignedCert(t)
	rec := &ctxRecorder{}
	b := startBroker(t, Config{
		Addr:                "127.0.0.1:0",
		SecureAddr:          "127.0.0.1:0",
		TLS:                 TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
		WebsocketAddr:       "127.0.0.1:0",
		WebsocketSecureAddr: "127.0.0.1:0",
		WebsocketPath:       "/mqtt",
		Sink:                ctxSink{rec: rec},
	})
	pool := certPool(t, certFile)

	dial := map[string]func(t *testing.T) net.Conn{
		"tcp": func(t *testing.T) net.Conn {
			conn, err := net.Dial("tcp", b.Addr())
			if err != nil {
				t.Fatalf("tcp dial: %v", err)
			}
			return conn
		},
		"tls": func(t *testing.T) net.Conn {
			conn, err := tls.Dial("tcp", b.SecureAddr(), &tls.Config{RootCAs: pool})
			if err != nil {
				t.Fatalf("tls dial: %v", err)
			}
			return conn
		},
		"ws": func(t *testing.T) net.Conn {
			return dialTestWebSocket(t, "ws", b.WebsocketAddr(), "/mqtt", nil)
		},
		"wss": func(t *testing.T) net.Conn {
			return dialTestWebSocket(t, "wss", b.WebsocketSecureAddr(), "/mqtt", &tls.Config{RootCAs: pool})
		},
	}
	for _, transport := range []string{"tcp", "tls", "ws", "wss"} {
		t.Run(transport, func(t *testing.T) {
			conn := dial[transport](t)
			t.Cleanup(func() { _ = conn.Close() })
			clientID := "transport-" + transport
			connectOverConn(t, conn, clientID)
			info, ok := waitConnInfo(t, rec, clientID)
			if !ok {
				t.Fatalf("no hook observed the connect of %q", clientID)
			}
			if info.Transport != transport {
				t.Fatalf("ConnInfo.Transport = %q, want %q", info.Transport, transport)
			}
		})
	}
}

// waitConnInfo returns the ConnInfo the broker attached to the connect of
// clientID. The recorder is shared by the four subtests, so the lookup is by
// client id rather than "the next connect event".
func waitConnInfo(t *testing.T, rec *ctxRecorder, clientID string) (ConnInfo, bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		for _, ev := range rec.events {
			if ev.hook == "connect" && ev.hasInfo && ev.info.ClientID == clientID {
				rec.mu.Unlock()
				return ev.info, true
			}
		}
		rec.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	return ConnInfo{}, false
}
