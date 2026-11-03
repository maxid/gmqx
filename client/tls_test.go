package client_test

// The client's TLS half.
//
// client.Config.TLS is the only way a client reaches a broker on its TLS listener.
// The configuration here is what a caller writes: a root pool and nothing else (the
// client derives ServerName from Addr), a client certificate for a broker that
// requires one, and a reconnect that has to repeat the whole handshake.

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
)

// testCA is a throwaway certificate authority. A client certificate only proves
// anything when the server was told which CA to trust, so the mTLS case needs one
// certificate that signs both the server leaf and the client leaf.
type testCA struct {
	cert *x509.Certificate
	key  *rsa.PrivateKey
	pem  []byte
	file string
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gmqx client test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	file := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(file, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key, pem: pemBytes, file: file}
}

// pool is a root pool that trusts this CA.
func (ca *testCA) pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}

// issue signs one leaf certificate with the CA and writes it next to its key. ips
// are the addresses the leaf is valid for; eku picks server or client auth.
func (ca *testCA) issue(t *testing.T, name string, serial int64, ips []net.IP, eku []x509.ExtKeyUsage) (certFile, keyFile string, cert tls.Certificate) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  eku,
		DNSNames:     []string{"localhost"},
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, name+".pem")
	keyFile = filepath.Join(dir, name+".key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile, pair
}

// tlsBroker starts a broker whose only MQTT listener is TLS and returns it with a
// pool that verifies its certificate.
func tlsBroker(t *testing.T) (*gmqx.Broker, *x509.CertPool) {
	t.Helper()
	ca := newTestCA(t)
	certFile, keyFile, _ := ca.issue(t, "server", 2, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	b := startBroker(t, gmqx.Config{
		Addr:       "127.0.0.1:0",
		SecureAddr: "127.0.0.1:0",
		TLS:        gmqx.TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
	})
	return b, ca.pool()
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
		TLS:        &tls.Config{RootCAs: pool},
		OnMessage:  rec.OnMessage,
	})
	if _, err := sub.Subscribe(ctx, client.Subscription{Filter: "tls/topic", QoS: 1}); err != nil {
		t.Fatalf("Subscribe over TLS: %v", err)
	}
	pub := dialClient(t, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "tls-pub",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: pool},
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
		TLS:        &tls.Config{RootCAs: x509.NewCertPool()},
	})
	if err == nil {
		t.Fatal("Dial succeeded against a server the pool does not trust")
	}
	if !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("Dial error = %v, want a certificate verification failure", err)
	}
}

// TestClientTLSFillsServerName is the contract of Config.TLS: the caller names a
// root pool, the client takes the server name from Addr the way tls.Dial does.
// Before this, a configuration that supplied only a pool failed to dial.
func TestClientTLSFillsServerName(t *testing.T) {
	b, pool := tlsBroker(t)
	ctx := testContext(t)
	rec := newRecorder(2)
	sub := dialClient(t, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "tls-derived-name",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: pool},
		OnMessage:  rec.OnMessage,
	})
	if _, err := sub.Subscribe(ctx, client.Subscription{Filter: "tls/derived", QoS: 0}); err != nil {
		t.Fatalf("Subscribe over TLS with a derived ServerName: %v", err)
	}
	pub := dialClient(t, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "tls-derived-pub",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: pool},
	})
	if err := pub.Publish(ctx, "tls/derived", []byte("derived"), 0, false); err != nil {
		t.Fatalf("Publish over TLS with a derived ServerName: %v", err)
	}
	if msg := rec.next(t); msg.Topic != "tls/derived" {
		t.Fatalf("delivered %q, want tls/derived", msg.Topic)
	}
}

// TestClientTLSKeepsExplicitServerName is the other half: an explicit ServerName
// is used as given, so a name the certificate does not cover fails the handshake
// instead of being replaced by the address's host.
func TestClientTLSKeepsExplicitServerName(t *testing.T) {
	b, pool := tlsBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Dial(ctx, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "tls-wrong-name",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: pool, ServerName: "wrong.example"},
	})
	if err == nil {
		t.Fatal("Dial succeeded with a ServerName the certificate does not cover")
	}
	if !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("Dial error = %v, want a certificate verification failure", err)
	}
}

// TestClientMutualTLS is the whole mTLS exchange from the client side: a broker
// that requires a client certificate signs off on a certificate the CA issued, and
// refuses a client that has none.
func TestClientMutualTLS(t *testing.T) {
	ca := newTestCA(t)
	serverCert, serverKey, _ := ca.issue(t, "server", 2, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	_, _, clientCert := ca.issue(t, "client", 3, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	b := startBroker(t, gmqx.Config{
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
	ctx := testContext(t)

	rec := newRecorder(2)
	sub := dialClient(t, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "mtls-sub",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: ca.pool(), Certificates: []tls.Certificate{clientCert}},
		OnMessage:  rec.OnMessage,
	})
	if _, err := sub.Subscribe(ctx, client.Subscription{Filter: "mtls/topic", QoS: 1}); err != nil {
		t.Fatalf("Subscribe over mutual TLS: %v", err)
	}
	pub := dialClient(t, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "mtls-pub",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: ca.pool(), Certificates: []tls.Certificate{clientCert}},
	})
	if err := pub.Publish(ctx, "mtls/topic", []byte("authenticated"), 1, false); err != nil {
		t.Fatalf("Publish over mutual TLS: %v", err)
	}
	if msg := rec.next(t); string(msg.Payload) != "authenticated" {
		t.Fatalf("delivered %q, want authenticated", msg.Payload)
	}

	dialCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Dial(dialCtx, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "mtls-anonymous",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: ca.pool()},
	}); err == nil {
		t.Fatal("Dial succeeded against a broker that requires a client certificate")
	}
}

// TestClientReconnectsOverTLS repeats the whole handshake on reconnect. dialPeer is
// the only place Config.TLS is applied, and it runs again for every reconnect, so a
// regression that skipped TLS on a redial would only show up here.
func TestClientReconnectsOverTLS(t *testing.T) {
	ca := newTestCA(t)
	certFile, keyFile, _ := ca.issue(t, "server", 2, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	b := startBroker(t, gmqx.Config{
		Addr:           "127.0.0.1:0",
		SecureAddr:     "127.0.0.1:0",
		ConnectMode:    gmqx.ConnectModeKick,
		NotKickSeconds: -1,
		TLS:            gmqx.TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
	})
	ctx := testContext(t)
	proxy := newTLSProxy(t, b.SecureAddr())
	connects := make(chan bool, 8)
	disconnects := make(chan error, 8)
	rec := newRecorder(8)

	c := dialClient(t, client.Config{
		Addr:       b.SecureAddr(),
		Dialer:     proxy.dial,
		ClientID:   "tls-reconnect",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: ca.pool()},
		Reconnect: client.ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     20 * time.Millisecond,
		},
		OnMessage:    rec.OnMessage,
		OnConnect:    func(bool) { connects <- true },
		OnDisconnect: func(err error) { disconnects <- err },
	})
	if !recvBool(t, connects, "the initial connect report") {
		t.Fatal("the initial connect report did not arrive")
	}
	if _, err := c.Subscribe(ctx, client.Subscription{Filter: "tls/reconnect", QoS: 1}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	proxy.killAll()
	if err := recvError(t, disconnects, "the disconnect report"); err == nil {
		t.Fatal("a lost connection reported a nil error")
	}
	// The reconnect re-runs the TLS handshake through the proxy and restores the
	// subscription.
	if !recvBool(t, connects, "the reconnect report") {
		t.Fatal("the reconnect report did not arrive")
	}
	pub := dialClient(t, client.Config{
		Addr:       b.SecureAddr(),
		ClientID:   "tls-reconnect-pub",
		CleanStart: true,
		TLS:        &tls.Config{RootCAs: ca.pool()},
	})
	if err := pub.Publish(ctx, "tls/reconnect", []byte("again"), 1, false); err != nil {
		t.Fatalf("Publish after the TLS reconnect: %v", err)
	}
	if msg := rec.next(t); string(msg.Payload) != "again" {
		t.Fatalf("delivered %q, want again", msg.Payload)
	}
}

// tlsProxy is a transparent TCP proxy with a kill switch: it copies bytes both
// ways, so a TLS session passes through it untouched, and killAll drops every live
// connection. It exists because the package's testProxy parses MQTT frames (it
// counts pings, holds PUBACKs) and therefore cannot carry a TLS stream.
type tlsProxy struct {
	ln net.Listener

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newTLSProxy(t *testing.T, target string) *tlsProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &tlsProxy{ln: ln, conns: make(map[net.Conn]struct{})}
	t.Cleanup(func() {
		_ = ln.Close()
		p.killAll()
	})
	go func() {
		for {
			downstream, err := ln.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", target)
			if err != nil {
				_ = downstream.Close()
				continue
			}
			p.track(downstream)
			p.track(upstream)
			go func() { _, _ = io.Copy(upstream, downstream); _ = upstream.Close() }()
			go func() { _, _ = io.Copy(downstream, upstream); _ = downstream.Close() }()
		}
	}()
	return p
}

func (p *tlsProxy) track(conn net.Conn) {
	p.mu.Lock()
	p.conns[conn] = struct{}{}
	p.mu.Unlock()
}

func (p *tlsProxy) addr() string { return p.ln.Addr().String() }

// dial is a client.Config.Dialer: it opens a connection through the proxy.
func (p *tlsProxy) dial(ctx context.Context, network, _ string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, p.addr())
}

// killAll drops every live connection, so the client sees a lost socket instead of
// a DISCONNECT.
func (p *tlsProxy) killAll() {
	p.mu.Lock()
	conns := make([]net.Conn, 0, len(p.conns))
	for conn := range p.conns {
		conns = append(conns, conn)
	}
	p.conns = make(map[net.Conn]struct{})
	p.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}
