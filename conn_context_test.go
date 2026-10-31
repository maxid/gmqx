package gmqx

// Connection-context tests: the Go equivalent of the reference broker's
// request-scoped context holder. Every hook of a connection must receive the same
// connection-scoped context, it must carry ConnInfo, it must inherit the values a
// caller put in through Config.ConnContext, and it must be cancelled when the
// connection ends.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// ctxValueKey marks the value Config.ConnContext adds in these tests.
type ctxValueKey struct{}

// hookEvent is one observed hook invocation with the context it received.
type hookEvent struct {
	hook        string
	info        ConnInfo
	hasInfo     bool
	value       any
	hasDeadline bool
	ctx         context.Context
}

// ctxRecorder collects hook invocations so a test can assert on each of them
// without depending on hook ordering between connections.
type ctxRecorder struct {
	mu     sync.Mutex
	events []hookEvent
}

func (r *ctxRecorder) add(hook string, ctx context.Context) {
	info, hasInfo := ConnFromContext(ctx)
	_, hasDeadline := ctx.Deadline()
	r.mu.Lock()
	r.events = append(r.events, hookEvent{
		hook:        hook,
		info:        info,
		hasInfo:     hasInfo,
		value:       ctx.Value(ctxValueKey{}),
		hasDeadline: hasDeadline,
		ctx:         ctx,
	})
	r.mu.Unlock()
}

// wait returns the first event of a hook, waiting up to three seconds.
func (r *ctxRecorder) wait(t *testing.T, hook string) hookEvent {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		for _, ev := range r.events {
			if ev.hook == hook {
				r.mu.Unlock()
				return ev
			}
		}
		r.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	r.mu.Lock()
	var seen []string
	for _, ev := range r.events {
		seen = append(seen, ev.hook)
	}
	r.mu.Unlock()
	t.Fatalf("hook %q was not observed (saw %v)", hook, seen)
	return hookEvent{}
}

type ctxAuth struct{ rec *ctxRecorder }

func (a ctxAuth) Authenticate(ctx context.Context, _ ConnectInfo) error {
	a.rec.add("auth", ctx)
	return nil
}

func (ctxAuth) RequiresOffload() bool { return false }

type ctxACL struct{ rec *ctxRecorder }

func (a ctxACL) Check(ctx context.Context, _ SessionInfo, _ string, action AclAction) bool {
	hook := "acl-subscribe"
	if action == AclPublish {
		hook = "acl-publish"
	}
	a.rec.add(hook, ctx)
	return true
}

func (ctxACL) RequiresOffload() bool { return false }

type ctxSink struct{ rec *ctxRecorder }

func (s ctxSink) OnConnect(ctx context.Context, _ ConnectInfo) { s.rec.add("connect", ctx) }
func (s ctxSink) OnDisconnect(ctx context.Context, _ string, _ error) {
	s.rec.add("disconnect", ctx)
}
func (s ctxSink) OnConnectionLost(ctx context.Context, _ string, _ error) {
	s.rec.add("connection-lost", ctx)
}
func (s ctxSink) OnPublish(ctx context.Context, _ PublishEvent) PublishResult {
	s.rec.add("publish", ctx)
	return PublishResult{Disposition: PublishAccepted}
}
func (s ctxSink) OnPing(ctx context.Context, _ string) { s.rec.add("ping", ctx) }

type ctxInterceptor struct{ rec *ctxRecorder }

func (i ctxInterceptor) OnPublish(ctx context.Context, _, _ string, _ []byte, _ byte) bool {
	i.rec.add("interceptor", ctx)
	return true
}

type ctxDispatcher struct {
	rec    *ctxRecorder
	broker *Broker
}

func (d *ctxDispatcher) Bind(b *Broker) { d.broker = b }

func (d *ctxDispatcher) Dispatch(ctx context.Context, pub Publication) error {
	d.rec.add("dispatch", ctx)
	for _, target := range d.broker.LocalTargets(pub) {
		_ = d.broker.SendLocal(ctx, target, pub)
	}
	return d.broker.SpreadPublication(ctx, pub)
}

func checkConnInfo(t *testing.T, ev hookEvent, clientID string) {
	t.Helper()
	if !ev.hasInfo {
		t.Fatalf("%s hook received a context without ConnInfo", ev.hook)
	}
	if ev.info.ClientID != clientID {
		t.Fatalf("%s hook ConnInfo.ClientID = %q, want %q", ev.hook, ev.info.ClientID, clientID)
	}
	if ev.info.Namespace != "tenant-a" {
		t.Fatalf("%s hook ConnInfo.Namespace = %q, want tenant-a", ev.hook, ev.info.Namespace)
	}
	if ev.info.ConnectionID == "" {
		t.Fatalf("%s hook ConnInfo.ConnectionID is empty", ev.hook)
	}
	if ev.info.RemoteAddr == "" {
		t.Fatalf("%s hook ConnInfo.RemoteAddr is empty", ev.hook)
	}
	if ev.info.ProtocolVersion != mqtt.Version311 {
		t.Fatalf("%s hook ConnInfo.ProtocolVersion = %d, want %d", ev.hook, ev.info.ProtocolVersion, mqtt.Version311)
	}
	if ev.value != "connect-value" {
		t.Fatalf("%s hook did not inherit the Config.ConnContext value: %v", ev.hook, ev.value)
	}
	if !ev.hasDeadline {
		t.Fatalf("%s hook context lost the deadline Config.ConnContext set", ev.hook)
	}
}

// TestConnectionContextFlowsToEveryHook drives one connection through CONNECT,
// SUBSCRIBE, PUBLISH, PING and DISCONNECT and asserts that every hook sees the
// same connection identity, the caller's values/ deadline, and finally a
// cancelled context.
func TestConnectionContextFlowsToEveryHook(t *testing.T) {
	rec := &ctxRecorder{}
	d := &ctxDispatcher{rec: rec}

	var cancelMu sync.Mutex
	var cancels []context.CancelFunc
	t.Cleanup(func() {
		cancelMu.Lock()
		defer cancelMu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
	})

	b := startBroker(t, Config{
		Namespace:    "tenant-a",
		Auth:         ctxAuth{rec: rec},
		ACL:          ctxACL{rec: rec},
		Sink:         ctxSink{rec: rec},
		Interceptors: []Interceptor{ctxInterceptor{rec: rec}},
		Dispatcher:   d,
		ConnContext: func(ctx context.Context, info ConnectInfo) context.Context {
			child, cancel := context.WithTimeout(ctx, time.Minute)
			cancelMu.Lock()
			cancels = append(cancels, cancel)
			cancelMu.Unlock()
			return context.WithValue(child, ctxValueKey{}, "connect-value")
		},
	})

	client := dialTestClient(t, b.Addr(), "ctx-client")
	checkConnInfo(t, rec.wait(t, "auth"), "ctx-client")
	connect := rec.wait(t, "connect")
	checkConnInfo(t, connect, "ctx-client")

	subscribeTestClient(t, client, "ctx/#")
	checkConnInfo(t, rec.wait(t, "acl-subscribe"), "ctx-client")

	pub := dialTestClient(t, b.Addr(), "ctx-pub")
	publishPlain(t, pub, "ctx/one", "hi")
	// The subscriber receives the publication on its own connection; consume it so
	// the PINGREQ below reads its PINGRESP.
	if _, topic, payload := readPublish(t, client); topic != "ctx/one" || string(payload) != "hi" {
		t.Fatalf("delivery = %q %q", topic, payload)
	}
	checkConnInfo(t, rec.wait(t, "acl-publish"), "ctx-pub")
	checkConnInfo(t, rec.wait(t, "interceptor"), "ctx-pub")
	checkConnInfo(t, rec.wait(t, "publish"), "ctx-pub")
	checkConnInfo(t, rec.wait(t, "dispatch"), "ctx-pub")

	client.writePacket(0xC0, nil) // PINGREQ
	if h, _, err := client.readPacket(); err != nil || h>>4 != 13 {
		t.Fatalf("PINGRESP = %#x err=%v", h, err)
	}
	checkConnInfo(t, rec.wait(t, "ping"), "ctx-client")

	client.writePacket(0xE0, nil) // DISCONNECT
	disconnect := rec.wait(t, "disconnect")
	checkConnInfo(t, disconnect, "ctx-client")
	// The connection is gone by the time the disconnect hook runs, so the context
	// it receives is already cancelled; the ConnInfo value is still there.
	if err := disconnect.ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("disconnect hook context error = %v, want context.Canceled", err)
	}
}

// TestConnectionContextCancelledOnConnectionLoss covers the abrupt close: the
// lost-connection hook sees ConnInfo and the same context is cancelled once the
// connection is gone.
func TestConnectionContextCancelledOnConnectionLoss(t *testing.T) {
	rec := &ctxRecorder{}
	b := startBroker(t, Config{Namespace: "tenant-a", Sink: ctxSink{rec: rec}})

	client := dialTestClient(t, b.Addr(), "lost-client")
	connect := rec.wait(t, "connect")
	if !connect.hasInfo || connect.info.ClientID != "lost-client" || connect.info.Namespace != "tenant-a" {
		t.Fatalf("connect ConnInfo = %+v (has info %v)", connect.info, connect.hasInfo)
	}

	_ = client.conn.Close()
	lost := rec.wait(t, "connection-lost")
	if !lost.hasInfo || lost.info.ClientID != "lost-client" || lost.info.Namespace != "tenant-a" {
		t.Fatalf("lost-connection ConnInfo = %+v (has info %v)", lost.info, lost.hasInfo)
	}
	if err := lost.ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost-connection context error = %v, want context.Canceled", err)
	}
}

// TestConnFromContext pins the accessor pair itself.
func TestConnFromContext(t *testing.T) {
	if _, ok := ConnFromContext(context.Background()); ok {
		t.Fatal("a context the broker did not create reported a ConnInfo")
	}
	want := ConnInfo{
		ClientID:        "dev-1",
		ConnectionID:    "42",
		Namespace:       "tenant-a",
		RemoteAddr:      "127.0.0.1:1234",
		ProtocolVersion: mqtt.Version5,
	}
	got, ok := ConnFromContext(ContextWithConn(context.Background(), want))
	if !ok || got != want {
		t.Fatalf("ConnFromContext = (%+v, %v), want (%+v, true)", got, ok, want)
	}
}
