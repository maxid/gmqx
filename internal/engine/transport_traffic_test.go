package engine

// One MQTT conversation over every transport the broker serves.
//
// transport_test.go and transport_secure_test.go stop at CONNECT/CONNACK, and
// interop/ only ever dials a plaintext broker, so nothing proved that a PUBLISH
// survives a WebSocket frame or that the client's TLS half (mqttclient.Config.TLS)
// ever runs. This test takes the same subscribe/publish exchange through tcp, tls,
// ws and wss with the ordinary gmqx client: over TLS it uses Config.TLS, over the
// WebSocket transports it uses Config.Dialer, which is how a caller hands the
// client a stream it did not open itself.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	mqttclient "github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/http/websocket"
)

// dialMQTTClient dials a gmqx client and closes it when the test ends.
func dialMQTTClient(t *testing.T, cfg mqttclient.Config) *mqttclient.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := mqttclient.Dial(ctx, cfg)
	if err != nil {
		t.Fatalf("client.Dial over %s: %v", cfg.Addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// webSocketDialer returns a client Dialer that performs the WebSocket handshake
// over tcp ("ws") or TLS ("wss") and hands the resulting stream to the client. A
// WebSocket connection is a byte stream, so nothing above the transport changes.
// The TLS configuration is one a caller would write: a root pool and no
// ServerName, which is filled from the dial address as tls.Dial does.
func webSocketDialer(network, addr, path string, tlsCfg *tls.Config) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, _, _ string) (net.Conn, error) {
		dialer := &net.Dialer{Timeout: 3 * time.Second}
		raw, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		if network == "wss" {
			if tlsCfg.ServerName == "" {
				host, _, splitErr := net.SplitHostPort(addr)
				if splitErr != nil {
					_ = raw.Close()
					return nil, splitErr
				}
				tlsCfg = tlsCfg.Clone()
				tlsCfg.ServerName = host
			}
			tlsConn := tls.Client(raw, tlsCfg)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				_ = raw.Close()
				return nil, err
			}
			raw = tlsConn
		}
		location := &url.URL{Scheme: network, Host: addr, Path: path}
		conn, err := websocket.ClientHandshake(raw, location, "http://"+addr, websocket.Options{Subprotocol: "mqtt"})
		if err != nil {
			_ = raw.Close()
			return nil, err
		}
		return conn, nil
	}
}

func TestPublishSubscribeOverEveryTransport(t *testing.T) {
	certFile, keyFile := selfSignedCert(t)
	cert, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(cert) {
		t.Fatal("the test certificate is not usable as a CA")
	}

	b := startBroker(t, Config{
		Addr:                "127.0.0.1:0",
		SecureAddr:          "127.0.0.1:0",
		TLS:                 TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
		WebsocketAddr:       "127.0.0.1:0",
		WebsocketSecureAddr: "127.0.0.1:0",
		WebsocketPath:       "/mqtt",
	})

	transports := []struct {
		name string
		cfg  func(clientID string) mqttclient.Config
	}{
		{"tcp", func(clientID string) mqttclient.Config {
			return mqttclient.Config{Addr: b.Addr(), ClientID: clientID}
		}},
		{"tls", func(clientID string) mqttclient.Config {
			return mqttclient.Config{
				Addr:     b.SecureAddr(),
				ClientID: clientID,
				// Config.TLS is handed to tls.Client as written: the caller names
				// the server, exactly as for a raw tls.Dial.
				TLS: &tls.Config{RootCAs: pool, ServerName: "127.0.0.1"},
			}
		}},
		{"ws", func(clientID string) mqttclient.Config {
			return mqttclient.Config{
				Addr:     b.WebsocketAddr(),
				ClientID: clientID,
				Dialer:   webSocketDialer("ws", b.WebsocketAddr(), "/mqtt", nil),
			}
		}},
		{"wss", func(clientID string) mqttclient.Config {
			return mqttclient.Config{
				Addr:     b.WebsocketSecureAddr(),
				ClientID: clientID,
				Dialer:   webSocketDialer("wss", b.WebsocketSecureAddr(), "/mqtt", &tls.Config{RootCAs: pool}),
			}
		}},
	}

	for _, tc := range transports {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			inbox := make(chan *mqttclient.Message, 4)
			subCfg := tc.cfg("sub-" + tc.name)
			subCfg.OnMessage = func(m *mqttclient.Message) { inbox <- m }
			sub := dialMQTTClient(t, subCfg)

			filter := "transport/" + tc.name
			if _, err := sub.Subscribe(ctx, mqttclient.Subscription{Filter: filter, QoS: 1}); err != nil {
				t.Fatalf("subscribe over %s: %v", tc.name, err)
			}

			pub := dialMQTTClient(t, tc.cfg("pub-"+tc.name))
			payload := []byte("hello over " + tc.name)
			if err := pub.Publish(ctx, filter, payload, 1, false); err != nil {
				t.Fatalf("publish over %s: %v", tc.name, err)
			}

			select {
			case msg := <-inbox:
				if msg.Topic != filter {
					t.Fatalf("delivered topic = %q, want %q", msg.Topic, filter)
				}
				if string(msg.Payload) != string(payload) {
					t.Fatalf("delivered payload = %q, want %q", msg.Payload, payload)
				}
				if msg.QoS != 1 {
					t.Fatalf("delivered QoS = %d, want 1", msg.QoS)
				}
			case <-time.After(testClientIOTimeout):
				t.Fatalf("no delivery over %s", tc.name)
			}
		})
	}
}
