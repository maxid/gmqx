package client_test

import (
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/mqtt"
)

// TestClientKeepalive checks both halves of the keepalive against the real broker:
// PINGREQ is actually sent at the configured interval, and a broker whose
// PINGRESP never arrives (the proxy drops them) is treated as dead, which leads to
// a reconnect. The intervals are milliseconds so the test does not wait for
// protocol seconds.
func TestClientKeepalive(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	proxy := newTestProxy(t, b.Addr())
	proxy.dropPingresp.Store(true)

	connects := make(chan bool, 8)
	disconnects := make(chan error, 8)
	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Dialer:     proxy.DialContext,
		ClientID:   "keepalive",
		Version:    mqtt.Version5,
		CleanStart: true,
		KeepAlive:  100 * time.Millisecond,
		Reconnect: client.ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     20 * time.Millisecond,
			Jitter:       0,
		},
		OnConnect:    func(sp bool) { connects <- sp },
		OnDisconnect: func(err error) { disconnects <- err },
	})
	recvBool(t, connects, "the initial connect report")

	waitFor(t, 3*time.Second, "a PINGREQ", func() bool { return proxy.Pings() >= 1 })

	// Stop dropping PINGRESPs as soon as the watchdog fired, so the reconnect that
	// follows can stay up.
	select {
	case err := <-disconnects:
		if err == nil {
			t.Fatal("the keepalive watchdog reported a nil error")
		}
		proxy.dropPingresp.Store(false)
	case <-time.After(5 * time.Second):
		t.Fatal("the keepalive watchdog never declared the connection dead")
	}
	recvBool(t, connects, "the reconnect report")
	waitFor(t, 3*time.Second, "the reconnected client", func() bool { return c.Connected() })

	// With PINGRESPs flowing again the connection must survive several intervals
	// (the keepalive timer keeps running on the new connection).
	time.Sleep(300 * time.Millisecond)
	if !c.Connected() {
		t.Fatal("the reconnected client did not survive its keepalive")
	}
	if proxy.Pings() < 2 {
		t.Fatalf("the client sent %d PINGREQs, want at least 2 across both connections", proxy.Pings())
	}
}
