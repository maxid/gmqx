package client_test

import (
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/mqtt"
)

// TestClientMaxInflightBlocks proves the outbound window is a real block and not
// a drop: with MaxInflight 1 the second QoS 1 PUBLISH is not put on the wire until
// the first PUBACK was received (the proxy holds PUBACKs, so the first exchange
// stays unconfirmed).
func TestClientMaxInflightBlocks(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	proxy := newTestProxy(t, b.Addr())
	proxy.holdPuback.Store(true)
	ctx := testContext(t)

	c := dialClient(t, client.Config{
		Addr:        b.Addr(),
		Dialer:      proxy.DialContext,
		ClientID:    "inflight-one",
		Version:     mqtt.Version5,
		CleanStart:  true,
		MaxInflight: 1,
	})

	first := make(chan error, 1)
	go func() { first <- c.Publish(ctx, "inflight/one", []byte("one"), 1, false) }()
	waitFor(t, 3*time.Second, "the first PUBLISH on the wire", func() bool { return proxy.Publishes() == 1 })

	second := make(chan error, 1)
	go func() { second <- c.Publish(ctx, "inflight/two", []byte("two"), 1, false) }()

	// While the first PUBACK is held the window has no free slot, so a second
	// PUBLISH must not leave the client.
	time.Sleep(200 * time.Millisecond)
	if got := proxy.Publishes(); got != 1 {
		t.Fatalf("a window of 1 sent %d PUBLISHes before the first PUBACK", got)
	}

	proxy.holdPuback.Store(false)
	proxy.releaseHeld()

	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("first Publish: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first Publish did not complete after its PUBACK was released")
	}
	waitFor(t, 3*time.Second, "the second PUBLISH on the wire", func() bool { return proxy.Publishes() == 2 })
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("second Publish: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second Publish did not complete")
	}
}

// TestClientReceiveMaximumFromServer checks that the receive maximum the broker
// advertises in its CONNACK caps the client's window below its own MaxInflight:
// the broker advertises 1, the client is configured for 16, and only one PUBLISH
// is outstanding.
func TestClientReceiveMaximumFromServer(t *testing.T) {
	b := startBroker(t, gmqx.Config{ReceiveMaximum: 1})
	proxy := newTestProxy(t, b.Addr())
	proxy.holdPuback.Store(true)
	ctx := testContext(t)

	c := dialClient(t, client.Config{
		Addr:        b.Addr(),
		Dialer:      proxy.DialContext,
		ClientID:    "inflight-server-window",
		Version:     mqtt.Version5,
		CleanStart:  true,
		MaxInflight: 16,
	})
	if got := c.ServerInfo().ReceiveMaximum; got != 1 {
		t.Fatalf("ServerInfo().ReceiveMaximum = %d, want the advertised 1", got)
	}

	first := make(chan error, 1)
	go func() { first <- c.Publish(ctx, "inflight/one", []byte("one"), 1, false) }()
	waitFor(t, 3*time.Second, "the first PUBLISH on the wire", func() bool { return proxy.Publishes() == 1 })

	second := make(chan error, 1)
	go func() { second <- c.Publish(ctx, "inflight/two", []byte("two"), 1, false) }()
	time.Sleep(200 * time.Millisecond)
	if got := proxy.Publishes(); got != 1 {
		t.Fatalf("the server's receive maximum of 1 allowed %d PUBLISHes", got)
	}

	proxy.holdPuback.Store(false)
	proxy.releaseHeld()

	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("first Publish: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the first Publish did not complete")
	}
	waitFor(t, 3*time.Second, "the second PUBLISH on the wire", func() bool { return proxy.Publishes() == 2 })
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("second Publish: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second Publish did not complete")
	}
}
