package client_test

// The client's TLS half.
//
// client.Config.TLS is the only way a client reaches a broker on its TLS listener,
// and no test in this package used it: dialPeer wrapped the socket in TLS and
// nothing ever ran that branch, so neither the successful handshake nor the case
// that matters most — a server certificate that cannot be verified — had a
// regression test. The configuration here is what a caller writes: the broker's
// certificate acts as its own CA, the client trusts that file and names the host.

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
)

// serverCert writes a self-signed certificate for 127.0.0.1 and returns the two
// files a listener is pointed at plus a pool that trusts it.
func serverCert(t *testing.T) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	pool = x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("the test certificate is not usable as a CA")
	}
	return certFile, keyFile, pool
}

// tlsBroker starts a broker whose only MQTT listener is TLS and returns it.
func tlsBroker(t *testing.T) (*gmqx.Broker, *x509.CertPool) {
	t.Helper()
	certFile, keyFile, pool := serverCert(t)
	b := startBroker(t, gmqx.Config{
		Addr:       "127.0.0.1:0",
		SecureAddr: "127.0.0.1:0",
		TLS:        gmqx.TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
	})
	return b, pool
}

// TestClientOverTLS is the round trip a TLS deployment runs: the client completes
// the TLS handshake inside Dial, then subscribes and publishes over that socket.
func TestClientOverTLS(t *testing.T) {
	b, pool := tlsBroker(t)
	ctx := testContext(t)
	rec := newRecorder(4)
	sub := dialClient(t, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "tls-sub",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: pool, ServerName: "127.0.0.1"},
		OnMessage:  rec.OnMessage,
	})
	if _, err := sub.Subscribe(ctx, client.Subscription{Filter: "tls/topic", QoS: 1}); err != nil {
		t.Fatalf("Subscribe over TLS: %v", err)
	}
	pub := dialClient(t, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "tls-pub",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: pool, ServerName: "127.0.0.1"},
	})
	if err := pub.Publish(ctx, "tls/topic", []byte("over tls"), 1, false); err != nil {
		t.Fatalf("Publish over TLS: %v", err)
	}
	msg := rec.next(t)
	if msg.Topic != "tls/topic" || string(msg.Payload) != "over tls" {
		t.Fatalf("delivered %q %q, want tls/topic %q", msg.Topic, msg.Payload, "over tls")
	}
}

// TestClientTLSRejectsUntrustedServer pins the failure a misconfigured trust store
// has to produce: a server certificate that does not chain to the pool fails Dial,
// it never becomes a connected client.
func TestClientTLSRejectsUntrustedServer(t *testing.T) {
	b, _ := tlsBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Dial(ctx, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "tls-untrusted",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: x509.NewCertPool(), ServerName: "127.0.0.1"},
	})
	if err == nil {
		t.Fatal("Dial succeeded against a server the pool does not trust")
	}
	if !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("Dial error = %v, want a certificate verification failure", err)
	}
}

// TestClientTLSRequiresServerName records the contract of Config.TLS: the
// configuration is handed to tls.Client as written, so a caller that sets neither
// ServerName nor InsecureSkipVerify gets an error instead of a silently
// unverified connection.
func TestClientTLSRequiresServerName(t *testing.T) {
	b, pool := tlsBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Dial(ctx, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "tls-no-server-name",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: pool},
	})
	if err == nil {
		t.Fatal("Dial succeeded with neither ServerName nor InsecureSkipVerify set")
	}
	if !strings.Contains(err.Error(), "ServerName") {
		t.Fatalf("Dial error = %v, want the missing-ServerName error", err)
	}
}
