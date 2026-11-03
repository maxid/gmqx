package interop

// Direction B over TLS: the gmqx client against the reference broker's TLS
// listener.
//
// The matrix proved that our client interoperates with eclipse-mosquitto, but only
// over the plaintext listener: a certificate exchange, a hostname check and a
// server that verifies itself were never exercised against a third-party broker.
// Both cases run against the same container, so the plaintext listener publishes
// and the TLS listener delivers.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"testing"

	"github.com/maxid/gmqx/client"
)

// referenceTLSConfig is a client TLS configuration that trusts the reference
// broker's CA and names the host the container is mapped on.
func referenceTLSConfig(t *testing.T, caFile, addr string) *tls.Config {
	t.Helper()
	pemBytes, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("read the reference broker CA: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatalf("%s is not a usable CA file", caFile)
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("reference TLS address %q: %v", addr, err)
	}
	return &tls.Config{RootCAs: pool, ServerName: host}
}

// TestReferenceBrokerOverTLS is the round trip through the reference broker's TLS
// listener: our client subscribes over TLS, mosquitto_pub publishes over the
// plaintext listener of the same broker, and the delivery has to cross.
func TestReferenceBrokerOverTLS(t *testing.T) {
	broker := startMosquitto(t)
	topic := "interop/reference/tls"
	payload := []byte("reference-broker-over-tls")

	sink := newSink()
	sub := dialGMQX(t, client.Config{
		Addr:      broker.tlsAddr,
		ClientID:  "reference-tls-sub",
		OnMessage: sinkHandler(sink),
		TLS:       referenceTLSConfig(t, broker.caFile, broker.tlsAddr),
	})
	subscribeGMQX(t, sub, topic, 1)

	// mosquitto_pub runs inside the container, so it reaches the broker through the
	// container's own plaintext listener; the message then has to reach a subscriber
	// that arrived over TLS.
	broker.publish(t, "127.0.0.1:"+containerPort, topic, payload, 1, false, "")

	got := sink.next(t, "the delivery through the reference broker's TLS listener")
	if got.topic != topic {
		t.Fatalf("delivered topic = %q, want %q", got.topic, topic)
	}
	if string(got.payload) != string(payload) {
		t.Fatalf("delivered payload = %s, want %q", describe(got.payload), payload)
	}
}

// TestReferenceBrokerTLSRejectsUntrustedCA is the negative half: a client that does
// not trust the reference broker's certificate fails the handshake, which is what
// proves the listener is really TLS and not a plaintext listener that happened to
// accept the bytes.
func TestReferenceBrokerTLSRejectsUntrustedCA(t *testing.T) {
	broker := startMosquitto(t)
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	_, err := client.Dial(ctx, client.Config{
		Addr:           broker.tlsAddr,
		ClientID:       "reference-tls-untrusted",
		TLS:            &tls.Config{RootCAs: x509.NewCertPool(), ServerName: "127.0.0.1"},
		ConnectTimeout: connectTimeout,
	})
	if err == nil {
		t.Fatal("a client with an empty trust store connected to the reference broker's TLS listener")
	}
}
