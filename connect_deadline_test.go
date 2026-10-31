package gmqx

// CONNECT-phase and authorization deadlines: a socket that says nothing, or an
// authorization hook that never answers, must not be able to hold a connection
// slot, a goroutine and a session forever.

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// TestConnectDeadlineClosesSilentSocket pins the CONNECT read deadline: a client
// that opens the socket and sends nothing is disconnected when
// Config.ConnectTimeout expires instead of being held forever.
func TestConnectDeadlineClosesSilentSocket(t *testing.T) {
	const timeout = 80 * time.Millisecond
	b := startBroker(t, Config{ConnectTimeout: timeout})

	conn, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// The client never writes a CONNECT.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	start := time.Now()
	var buf [1]byte
	n, err := conn.Read(buf[:])
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("read %#x from a connection that never sent CONNECT, want a close", buf[:n])
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("the broker held a silent connection open for %v (CONNECT deadline %v)", elapsed, timeout)
	}
	if elapsed < timeout/2 {
		t.Fatalf("connection closed after %v, want about the %v CONNECT deadline", elapsed, timeout)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("connection closed after %v, want about the %v CONNECT deadline", elapsed, timeout)
	}
	if n := b.connectionCount(); n != 0 {
		t.Fatalf("broker holds %d connections after the CONNECT deadline, want 0", n)
	}
}

// TestConnectDeadlineClearedAfterHandshake pins the other half of the rule: the
// deadline covers the CONNECT phase only, so a connected idle client is not
// disconnected by it (its silence is the keep-alive's business).
func TestConnectDeadlineClearedAfterHandshake(t *testing.T) {
	const timeout = 60 * time.Millisecond
	b := startBroker(t, Config{ConnectTimeout: timeout})
	c, _ := mustConnect(t, b.Addr(), confConnect{
		version:   mqtt.Version5,
		clientID:  "connect-deadline-cleared",
		clean:     true,
		keepAlive: 30,
	})

	// Six CONNECT deadlines pass. The read loop armed the keep-alive deadline
	// (45 s) instead, so the connection is still there and answers.
	time.Sleep(6 * timeout)
	c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	readConf[*mqtt.Pingresp](t, c)
}

// stubbornAuth is an AuthManager that ignores its context on purpose, which is
// the case Config.AuthTimeout has to enforce.
type stubbornAuth struct {
	release     chan struct{}
	calls       atomic.Int32
	sawDeadline atomic.Bool
}

func (a *stubbornAuth) Authenticate(ctx context.Context, _ ConnectInfo) error {
	a.calls.Add(1)
	if _, ok := ctx.Deadline(); ok {
		a.sawDeadline.Store(true)
	}
	<-a.release
	return nil
}

func (*stubbornAuth) RequiresOffload() bool { return false }

// TestAuthenticationTimeoutRefusesClient pins that an Authenticator which never
// returns is refused when Config.AuthTimeout expires: the client gets
// "not authorized", is not registered, and its hook received a context carrying
// the deadline.
func TestAuthenticationTimeoutRefusesClient(t *testing.T) {
	auth := &stubbornAuth{release: make(chan struct{})}
	t.Cleanup(func() { close(auth.release) })
	const timeout = 60 * time.Millisecond
	b := startBroker(t, Config{Auth: auth, AuthTimeout: timeout})

	start := time.Now()
	c, ack := dialConf(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "slow-auth", clean: true})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("CONNECT of a stuck Authenticator returned after %v, want about the %v timeout", elapsed, timeout)
	}
	if ack.ReasonCode != 0x87 {
		t.Fatalf("CONNACK reason code %#x, want 0x87 (Not authorized) for an authentication timeout", ack.ReasonCode)
	}
	expectClosed(t, c)
	if !auth.sawDeadline.Load() {
		t.Fatal("the context passed to Authenticate carried no deadline")
	}
	if n := b.connectionCount(); n != 0 {
		t.Fatalf("broker holds %d connections after the auth timeout, want 0", n)
	}
	b.mu.RLock()
	_, connected := b.clients[namespaceKey(b.namespace, "slow-auth")]
	b.mu.RUnlock()
	if connected {
		t.Fatal("a client whose Authenticate timed out was admitted")
	}
}

// stubbornACL is an AclManager that ignores its context on purpose.
type stubbornACL struct{ release chan struct{} }

func (a *stubbornACL) Check(context.Context, SessionInfo, string, AclAction) bool {
	<-a.release
	return true
}

func (*stubbornACL) RequiresOffload() bool { return false }

// TestACLTimeoutDenies pins that an ACL hook which never answers is a denial, not
// an admission: the SUBSCRIBE is refused with 0x87 and the PUBLISH is not
// acknowledged as accepted.
func TestACLTimeoutDenies(t *testing.T) {
	acl := &stubbornACL{release: make(chan struct{})}
	t.Cleanup(func() { close(acl.release) })
	b := startBroker(t, Config{ACL: acl, AuthTimeout: 60 * time.Millisecond})
	c, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "slow-acl", clean: true})

	if codes := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "acl/x", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 0x87 {
		t.Fatalf("SUBACK codes %v, want [0x87] for an ACL check that timed out", codes)
	}
	c.publish(confPublish{topic: "acl/x", payload: "x", qos: 1, pid: 2})
	if ack := readConf[*mqtt.Puback](t, c); ack.ReasonCode != 0x87 {
		t.Fatalf("PUBACK reason code %#x, want 0x87 for an ACL check that timed out", ack.ReasonCode)
	}
}

// connectionCount reports how many connections the broker currently holds.
func (b *Broker) connectionCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}
