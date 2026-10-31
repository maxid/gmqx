package client_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/mqtt"
)

// reconnectBrokerConfig is the broker setup of the reconnect tests: KICK lets a
// reconnect take over a connection the broker has not reaped yet, and a negative
// NotKickSeconds removes the anti-flap window, so the tests do not depend on how
// quickly the broker notices the dead socket.
func reconnectBrokerConfig() gmqx.Config {
	return gmqx.Config{ConnectMode: gmqx.ConnectModeKick, NotKickSeconds: -1}
}

// TestClientReconnectResubscribes kills the proxied connection of a clean-start
// client. The client reconnects with backoff, gets a new (empty) session, restores
// its subscription and delivers again.
func TestClientReconnectResubscribes(t *testing.T) {
	b := startBroker(t, reconnectBrokerConfig())
	ctx := testContext(t)
	proxy := newTestProxy(t, b.Addr())
	connects := make(chan bool, 8)
	disconnects := make(chan error, 8)
	rec := newRecorder(8)

	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Dialer:     proxy.DialContext,
		ClientID:   "reconnect-clean",
		Version:    mqtt.Version311,
		CleanStart: true,
		Reconnect: client.ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     20 * time.Millisecond,
			Jitter:       0,
		},
		OnMessage:    rec.OnMessage,
		OnConnect:    func(sp bool) { connects <- sp },
		OnDisconnect: func(err error) { disconnects <- err },
	})
	if sp := recvBool(t, connects, "the initial connect report"); sp {
		t.Fatal("a clean start reported a session")
	}
	if _, err := c.Subscribe(ctx, client.Subscription{Filter: "reconnect/topic", QoS: 1}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	proxy.killAll()

	if err := recvError(t, disconnects, "the disconnect report"); err == nil {
		t.Fatal("a lost connection reported a nil error")
	}
	if sp := recvBool(t, connects, "the reconnect report"); sp {
		t.Fatal("a clean-start reconnect must not resume a session")
	}
	waitFor(t, 3*time.Second, "the client to reconnect", func() bool { return c.Connected() })
	if proxy.Dials() != 2 {
		t.Fatalf("the client dialed %d times, want 2 (initial + reconnect)", proxy.Dials())
	}

	pub := dialClient(t, client.Config{Addr: b.Addr(), ClientID: "reconnect-pub", Version: mqtt.Version311, CleanStart: true})
	waitFor(t, 3*time.Second, "a delivery through the restored subscription", func() bool {
		if err := pub.Publish(ctx, "reconnect/topic", []byte("again"), 1, false); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case msg := <-rec.ch:
			if msg.Topic != "reconnect/topic" || string(msg.Payload) != "again" {
				t.Fatalf("delivered %q %q", msg.Topic, msg.Payload)
			}
			return true
		case <-time.After(50 * time.Millisecond):
			return false
		}
	})
	if c.SessionPresent() {
		t.Fatal("a clean-start reconnect must report Session Present = false")
	}
}

// TestClientReconnectPersistentSession reconnects a persistent MQTT 3.1.1 session:
// the broker reports Session Present, the client does not need to resubscribe and
// the subscription still delivers.
func TestClientReconnectPersistentSession(t *testing.T) {
	b := startBroker(t, reconnectBrokerConfig())
	ctx := testContext(t)
	proxy := newTestProxy(t, b.Addr())
	connects := make(chan bool, 8)
	disconnects := make(chan error, 8)
	rec := newRecorder(8)

	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Dialer:     proxy.DialContext,
		ClientID:   "reconnect-persistent",
		Version:    mqtt.Version311,
		CleanStart: false,
		Reconnect: client.ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     20 * time.Millisecond,
			Jitter:       0,
		},
		OnMessage:    rec.OnMessage,
		OnConnect:    func(sp bool) { connects <- sp },
		OnDisconnect: func(err error) { disconnects <- err },
	})
	if sp := recvBool(t, connects, "the initial connect report"); sp {
		t.Fatal("the first connection resumed a session it never created")
	}
	if _, err := c.Subscribe(ctx, client.Subscription{Filter: "persistent/topic", QoS: 1}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	proxy.killAll()
	if err := recvError(t, disconnects, "the disconnect report"); err == nil {
		t.Fatal("a lost connection reported a nil error")
	}
	if sp := recvBool(t, connects, "the reconnect report"); !sp {
		t.Fatal("the broker did not resume the persistent session")
	}
	waitFor(t, 3*time.Second, "the client to reconnect", func() bool { return c.Connected() })
	if !c.SessionPresent() {
		t.Fatal("SessionPresent() did not report the resumed session")
	}

	pub := dialClient(t, client.Config{Addr: b.Addr(), ClientID: "persistent-pub", Version: mqtt.Version311, CleanStart: true})
	waitFor(t, 3*time.Second, "a delivery through the resumed subscription", func() bool {
		if err := pub.Publish(ctx, "persistent/topic", []byte("resumed"), 1, false); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case msg := <-rec.ch:
			if msg.Topic != "persistent/topic" || string(msg.Payload) != "resumed" {
				t.Fatalf("delivered %q %q", msg.Topic, msg.Payload)
			}
			return true
		case <-time.After(50 * time.Millisecond):
			return false
		}
	})
}

// TestClientReconnectMaxAttempts makes every reconnect attempt fail by closing the
// proxy listener, then checks that MaxAttempts bounds the retries and that the
// client ends up disconnected instead of retrying forever.
func TestClientReconnectMaxAttempts(t *testing.T) {
	b := startBroker(t, reconnectBrokerConfig())
	proxy := newTestProxy(t, b.Addr())
	var disconnects atomic.Int64
	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Dialer:     proxy.DialContext,
		ClientID:   "reconnect-bounded",
		Version:    mqtt.Version311,
		CleanStart: true,
		Reconnect: client.ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 5 * time.Millisecond,
			MaxDelay:     10 * time.Millisecond,
			MaxAttempts:  3,
			Jitter:       0,
		},
		OnDisconnect: func(error) { disconnects.Add(1) },
	})
	if got := proxy.Dials(); got != 1 {
		t.Fatalf("initial dials = %d, want 1", got)
	}

	// Closing the listener turns every reconnect attempt into a dial failure.
	proxy.Close()

	// The initial connection plus MaxAttempts reconnect attempts.
	waitFor(t, 3*time.Second, "3 reconnect attempts", func() bool { return proxy.Dials() >= 4 })
	time.Sleep(200 * time.Millisecond)
	if got := proxy.Dials(); got != 4 {
		t.Fatalf("the client made %d dials, want 4 (initial + MaxAttempts)", got)
	}
	if c.Connected() {
		t.Fatal("the client reports a connection after its retry budget ended")
	}
	if got := disconnects.Load(); got != 1 {
		t.Fatalf("OnDisconnect ran %d times, want 1", got)
	}

	ctx := testContext(t)
	if err := c.Publish(ctx, "bounded/topic", nil, 0, false); !errors.Is(err, client.ErrNotConnected) {
		t.Fatalf("Publish after the retry budget = %v, want ErrNotConnected", err)
	}
}

// TestClientCloseDoesNotReconnect proves a user-initiated Close ends the client for
// good: no reconnect is attempted after it.
func TestClientCloseDoesNotReconnect(t *testing.T) {
	b := startBroker(t, reconnectBrokerConfig())
	proxy := newTestProxy(t, b.Addr())
	var connects atomic.Int64
	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Dialer:     proxy.DialContext,
		ClientID:   "close-no-reconnect",
		Version:    mqtt.Version311,
		CleanStart: true,
		Reconnect: client.ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 5 * time.Millisecond,
			MaxDelay:     10 * time.Millisecond,
			Jitter:       0,
		},
		OnConnect: func(bool) { connects.Add(1) },
	})
	if connects.Load() != 1 {
		t.Fatalf("OnConnect ran %d times, want 1", connects.Load())
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := proxy.Dials(); got != 1 {
		t.Fatalf("the client dialed %d times after Close, want 1", got)
	}
	if got := connects.Load(); got != 1 {
		t.Fatalf("OnConnect ran %d times after Close, want 1", got)
	}
	if c.Connected() {
		t.Fatal("a closed client reports a connection")
	}
}

// TestClientServerDisconnectDoesNotReconnect checks that a server DISCONNECT (the
// broker takes the session over) is reported with its reason code and is not
// treated as a transport failure: reconnecting would fight the connection that
// displaced this one.
func TestClientServerDisconnectDoesNotReconnect(t *testing.T) {
	b := startBroker(t, gmqx.Config{ConnectMode: gmqx.ConnectModeKick, NotKickSeconds: -1})
	proxy := newTestProxy(t, b.Addr())
	disconnects := make(chan error, 4)
	var connects atomic.Int64

	first := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Dialer:     proxy.DialContext,
		ClientID:   "takeover",
		Version:    mqtt.Version5,
		CleanStart: true,
		Reconnect: client.ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 5 * time.Millisecond,
			MaxDelay:     10 * time.Millisecond,
			Jitter:       0,
		},
		OnConnect:    func(bool) { connects.Add(1) },
		OnDisconnect: func(err error) { disconnects <- err },
	})

	// The same client id kicks the first connection: MQTT 5 gets DISCONNECT 0x8E.
	_ = dialClient(t, client.Config{Addr: b.Addr(), ClientID: "takeover", Version: mqtt.Version5, CleanStart: true})

	err := recvError(t, disconnects, "the server DISCONNECT report")
	var reason *client.ReasonError
	if !errors.As(err, &reason) {
		t.Fatalf("DISCONNECT error %v does not carry a *ReasonError", err)
	}
	if reason.Code != 0x8E {
		t.Fatalf("DISCONNECT reason = 0x%02x, want 0x8e (session taken over)", reason.Code)
	}
	time.Sleep(150 * time.Millisecond)
	if got := proxy.Dials(); got != 1 {
		t.Fatalf("the client reconnected %d times after a server DISCONNECT, want 0", got-1)
	}
	if connects.Load() != 1 {
		t.Fatalf("OnConnect ran %d times, want 1", connects.Load())
	}
	if first.Connected() {
		t.Fatal("the displaced client still reports a connection")
	}
}
