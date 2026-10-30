package gmqx

import (
	"bufio"
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
	"testing"
	"time"

	"github.com/maxid/gnety"
)

func writeConnect(w io.Writer, clientID string) error {
	body := connectPayload(clientID)
	buf := appendRemainingLength([]byte{0x10}, len(body))
	buf = append(buf, body...)
	_, err := w.Write(buf)
	return err
}

func readConnack(r io.Reader) (byte, error) {
	br := bufio.NewReader(r)
	if _, err := br.ReadByte(); err != nil {
		return 0, err
	}
	n, err := readRemainingLength(br)
	if err != nil {
		return 0, err
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(br, payload); err != nil {
		return 0, err
	}
	if len(payload) < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	return payload[1], nil
}

type webSocketConnectCodec struct{}

func (webSocketConnectCodec) Encode(w io.Writer, msg any) error {
	return writeConnect(w, msg.(string))
}

func (webSocketConnectCodec) Decode(r *bufio.Reader) (any, error) {
	return readConnack(r)
}

func selfSignedCert(t *testing.T) (certFile, keyFile string) {
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
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func TestTLSListener(t *testing.T) {
	certFile, keyFile := selfSignedCert(t)
	b := startBroker(t, Config{
		Addr:       "127.0.0.1:0",
		TLS:        TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
		SecureAddr: "127.0.0.1:0",
	})
	addr := b.SecureAddr()
	if addr == "" {
		t.Fatal("expected TLS listener address")
	}
	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeConnect(conn, "tls-client"); err != nil {
		t.Fatal(err)
	}
	code, err := readConnack(conn)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if code != 0 {
		t.Fatalf("connack code = %d", code)
	}
}

func TestWebSocketListener(t *testing.T) {
	b := startBroker(t, Config{
		Addr:          "127.0.0.1:0",
		WebsocketAddr: "127.0.0.1:0",
		WebsocketPath: "/mqtt",
	})
	addr := b.WebsocketAddr()
	if addr == "" {
		t.Fatal("expected WS listener address")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cli, err := gnety.Dial(ctx, gnety.ClientConfig{
		Network: "ws", Addr: addr, Path: "/mqtt", Subprotocol: "mqtt", Codec: webSocketConnectCodec{},
	})
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer cli.Close()
	if err := cli.Send("ws-client"); err != nil {
		t.Fatal(err)
	}
	msg, err := cli.Receive(ctx)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if code := msg.(byte); code != 0 {
		t.Fatalf("connack code = %d", code)
	}
}

func TestWebSocketRequiresSubprotocol(t *testing.T) {
	b := startBroker(t, Config{
		Addr:          "127.0.0.1:0",
		WebsocketAddr: "127.0.0.1:0",
	})
	addr := b.WebsocketAddr()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cli, err := gnety.Dial(ctx, gnety.ClientConfig{
		Network: "ws", Addr: addr, Path: "/mqtt", Codec: webSocketConnectCodec{},
	})
	if err == nil {
		_ = cli.Close()
		t.Fatal("expected dial failure without mqtt subprotocol")
	}
}

func TestMultipleListeners(t *testing.T) {
	certFile, keyFile := selfSignedCert(t)
	b := startBroker(t, Config{
		Addr:                "127.0.0.1:0",
		SecureAddr:          "127.0.0.1:0",
		WebsocketAddr:       "127.0.0.1:0",
		WebsocketSecureAddr: "127.0.0.1:0",
		TLS:                 TLSConfig{Enabled: true, Cert: certFile, Key: keyFile},
	})
	addrs := b.Addrs()
	for _, name := range []string{"tcp", "tls", "ws", "wss"} {
		if addrs[name] == "" {
			t.Fatalf("missing %s listener in %v", name, addrs)
		}
	}
}
