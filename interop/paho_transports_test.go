//go:build !interop_without_paho

package interop

// The paho direction over the broker's secure and WebSocket listeners.
//
// Every other case in this module dials the plaintext TCP listener: the matrix
// proved that a third-party MQTT client interoperates, but only over 1883. The
// broker advertises four listeners, so the same client runs once per transport
// here — TLS, WSS and mutual TLS with a real certificate exchange — and a delivery
// has to arrive in each case.
//
// With the interop_without_paho tag these cases are not built at all; paho_skip_test.go
// covers the dependency-free matrix in that build.

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/maxid/gmqx"
)

// connectPahoBroker connects a paho client to one explicit broker URL with an
// optional TLS configuration, and disconnects it when the test ends.
func connectPahoBroker(t *testing.T, brokerURL string, opts *mqtt.ClientOptions, tlsCfg *tls.Config) mqtt.Client {
	t.Helper()
	opts.AddBroker(brokerURL)
	if tlsCfg != nil {
		opts.SetTLSConfig(tlsCfg)
	}
	c := mqtt.NewClient(opts)
	waitToken(t, "CONNECT to "+brokerURL, c.Connect())
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// pahoTransportCase is one listener of one broker: the URLs a paho client uses to
// reach it, and the TLS configuration it needs (nil for plaintext).
type pahoTransportCase struct {
	name      string
	brokerURL string
	tlsCfg    *tls.Config
}

// TestPahoOverTransports runs the same publish/subscribe exchange over TLS, WSS and
// mutual TLS with a real third-party client, so "our TLS listener works" is not
// only our own client's opinion.
func TestPahoOverTransports(t *testing.T) {
	ca := newInteropCA(t)
	serverCert, serverKey, _ := ca.issue(t, "server", 2, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	_, _, clientCert := ca.issue(t, "client", 3, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	b := startBroker(t, gmqx.Config{
		Addr:                "127.0.0.1:0",
		SecureAddr:          "127.0.0.1:0",
		TLS:                 gmqx.TLSConfig{Enabled: true, Cert: serverCert, Key: serverKey},
		WebsocketAddr:       "127.0.0.1:0",
		WebsocketSecureAddr: "127.0.0.1:0",
		WebsocketPath:       "/mqtt",
	})
	pool := ca.pool()

	cases := []pahoTransportCase{
		{"tls", "ssl://" + b.SecureAddr(), &tls.Config{RootCAs: pool}},
		{"ws", "ws://" + b.WebsocketAddr() + "/mqtt", nil},
		{"wss", "wss://" + b.WebsocketSecureAddr() + "/mqtt", &tls.Config{RootCAs: pool}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topic := "interop/paho/" + tc.name
			payload := []byte("paho over " + tc.name)

			sink := newSink()
			subOpts := pahoOptions("paho-" + tc.name + "-sub").SetDefaultPublishHandler(
				func(_ mqtt.Client, m mqtt.Message) {
					sink.push(delivery{topic: m.Topic(), payload: m.Payload(), qos: m.Qos(), retain: m.Retained(), dup: m.Duplicate()})
				})
			sub := connectPahoBroker(t, tc.brokerURL, subOpts, tc.tlsCfg)
			waitToken(t, "SUBSCRIBE over "+tc.name, sub.Subscribe(topic, 1, nil))

			pub := connectPahoBroker(t, tc.brokerURL, pahoOptions("paho-"+tc.name+"-pub"), tc.tlsCfg)
			waitToken(t, "PUBLISH over "+tc.name, pub.Publish(topic, 1, false, payload))

			got := sink.next(t, "the delivery over "+tc.name)
			if got.topic != topic {
				t.Fatalf("delivered topic = %q, want %q", got.topic, topic)
			}
			if string(got.payload) != string(payload) {
				t.Fatalf("delivered payload = %s, want %q", describe(got.payload), payload)
			}
		})
	}

	// Mutual TLS: the same broker, reconfigured to require the client certificate
	// the CA signed. A client without one must not complete the CONNECT.
	t.Run("mutual tls", func(t *testing.T) {
		mtlsBroker := startBroker(t, gmqx.Config{
			Addr:       "127.0.0.1:0",
			SecureAddr: "127.0.0.1:0",
			TLS: gmqx.TLSConfig{
				Enabled:           true,
				Cert:              serverCert,
				Key:               serverKey,
				CA:                ca.file,
				RequireClientCert: true,
			},
		})
		brokerURL := "ssl://" + mtlsBroker.SecureAddr()
		topic := "interop/paho/mtls"
		payload := []byte("paho over mutual tls")

		sink := newSink()
		subOpts := pahoOptions("paho-mtls-sub").SetDefaultPublishHandler(
			func(_ mqtt.Client, m mqtt.Message) {
				sink.push(delivery{topic: m.Topic(), payload: m.Payload(), qos: m.Qos()})
			})
		sub := connectPahoBroker(t, brokerURL, subOpts, &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{clientCert}})
		waitToken(t, "SUBSCRIBE over mutual TLS", sub.Subscribe(topic, 1, nil))

		pub := connectPahoBroker(t, brokerURL, pahoOptions("paho-mtls-pub"), &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{clientCert}})
		waitToken(t, "PUBLISH over mutual TLS", pub.Publish(topic, 1, false, payload))
		if got := sink.next(t, "the delivery over mutual TLS"); string(got.payload) != string(payload) {
			t.Fatalf("delivered payload = %s, want %q", describe(got.payload), payload)
		}

		anonymousOpts := pahoOptions("paho-mtls-anonymous")
		anonymousOpts.AddBroker(brokerURL)
		anonymousOpts.SetTLSConfig(&tls.Config{RootCAs: pool})
		anonymous := mqtt.NewClient(anonymousOpts)
		tok := anonymous.Connect()
		if tok.WaitTimeout(connectTimeout) && tok.Error() == nil {
			anonymous.Disconnect(100)
			t.Fatal("a client without a certificate completed the CONNECT")
		}
	})
}
