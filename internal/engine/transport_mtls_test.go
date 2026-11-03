package engine

// Mutual TLS: the acceptance case of TLSConfig.RequireClientCert.
//
// A client certificate only proves anything when the server was told which CA to
// trust, and selfSignedCert's single self-signed certificate cannot express that.
// This file therefore carries a throwaway CA plus the server and client leaves it
// signs, and asserts both halves of the setting: a certificate the CA signed is
// accepted, a missing one and one from an unrelated CA are not.

import (
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
	"testing"
	"time"
)

// issuedCert is one leaf certificate with its key: the files a listener or a
// client is pointed at, and the parsed pair a client presents in memory.
type issuedCert struct {
	certFile string
	keyFile  string
	cert     tls.Certificate
}

// testCA is a throwaway certificate authority for the mutual-TLS cases. A client
// certificate only proves anything when the server was told which CA to trust,
// and selfSignedCert's one self-signed certificate cannot express that.
type testCA struct {
	cert *x509.Certificate
	key  *rsa.PrivateKey
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
		Subject:               pkix.Name{CommonName: "gmqx test CA"},
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
	file := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key, file: file}
}

// issue signs one leaf certificate with the CA and writes it next to its key. ips
// are the addresses the leaf is valid for, so a dial to 127.0.0.1 verifies without
// InsecureSkipVerify; eku picks server or client authentication.
func (ca *testCA) issue(t *testing.T, name string, serial int64, ips []net.IP, eku []x509.ExtKeyUsage) issuedCert {
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
	out := issuedCert{
		certFile: filepath.Join(dir, name+".pem"),
		keyFile:  filepath.Join(dir, name+".key"),
		cert:     pair,
	}
	if err := os.WriteFile(out.certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out.keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return out
}

// certPool trusts the certificates in files, so a dial verifies the server
// instead of skipping verification.
func certPool(t *testing.T, files ...string) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !pool.AppendCertsFromPEM(data) {
			t.Fatalf("certificate %q is not usable as a CA", file)
		}
	}
	return pool
}

// TestMutualTLSListener is the acceptance case of TLSConfig.RequireClientCert: a
// client certificate signed by TLSConfig.CA is accepted, and a client without one
// (or with one from an unrelated CA) never gets its CONNECT answered.
func TestMutualTLSListener(t *testing.T) {
	ca := newTestCA(t)
	server := ca.issue(t, "server", 2, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	client := ca.issue(t, "client", 3, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	strangerCA := newTestCA(t)
	stranger := strangerCA.issue(t, "stranger", 4, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	b := startBroker(t, Config{
		Addr:       "127.0.0.1:0",
		SecureAddr: "127.0.0.1:0",
		TLS: TLSConfig{
			Enabled:           true,
			Cert:              server.certFile,
			Key:               server.keyFile,
			CA:                ca.file,
			RequireClientCert: true,
		},
	})
	addr := b.SecureAddr()
	if addr == "" {
		t.Fatal("expected TLS listener address")
	}

	t.Run("trusted client certificate", func(t *testing.T) {
		conn, err := tls.Dial("tcp", addr, &tls.Config{
			RootCAs:      certPool(t, ca.file),
			Certificates: []tls.Certificate{client.cert},
		})
		if err != nil {
			t.Fatalf("tls dial with a trusted client certificate: %v", err)
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if err := writeConnect(conn, "mtls-client"); err != nil {
			t.Fatal(err)
		}
		if code, err := readConnack(conn); err != nil || code != 0 {
			t.Fatalf("CONNACK over mTLS = %d, %v", code, err)
		}
	})

	// A refused client is a TLS 1.3 alert: the client can finish its own side of
	// the handshake first, so the refusal surfaces either at the dial or at the
	// first exchange. Both are the same guarantee, and both are accepted here.
	refusals := []struct {
		name string
		tls  *tls.Config
	}{
		{"no client certificate", &tls.Config{RootCAs: certPool(t, ca.file)}},
		{"certificate from another CA", &tls.Config{RootCAs: certPool(t, ca.file), Certificates: []tls.Certificate{stranger.cert}}},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := tls.Dial("tcp", addr, tc.tls)
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			_ = writeConnect(conn, "mtls-refused")
			if code, err := readConnack(conn); err == nil {
				t.Fatalf("CONNACK %d was accepted without a certificate the CA signed", code)
			}
		})
	}
}
