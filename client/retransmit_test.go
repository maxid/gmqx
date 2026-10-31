package client_test

import (
	"errors"
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/mqtt"
)

// TestClientPublishRetransmitsWithDup holds the PUBACK, so the client's QoS 1
// exchange stays unconfirmed and its retransmission timer fires: the proxy must
// see the same PUBLISH again with the DUP flag set. Releasing the acknowledgement
// completes the publish.
func TestClientPublishRetransmitsWithDup(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	proxy := newTestProxy(t, b.Addr())
	proxy.holdPuback.Store(true)
	ctx := testContext(t)

	c := dialClient(t, client.Config{
		Addr:          b.Addr(),
		Dialer:        proxy.DialContext,
		ClientID:      "retransmit-dup",
		Version:       mqtt.Version5,
		CleanStart:    true,
		RetryInterval: 20 * time.Millisecond,
		MaxRetries:    3,
	})
	done := make(chan error, 1)
	go func() { done <- c.Publish(ctx, "retransmit/topic", []byte("dup"), 1, false) }()

	waitFor(t, 3*time.Second, "the first PUBLISH", func() bool { return proxy.Publishes() >= 1 })
	waitFor(t, 3*time.Second, "a DUP retransmission", func() bool { return proxy.Dups() >= 1 })
	select {
	case err := <-done:
		t.Fatalf("Publish completed before the PUBACK was released: %v", err)
	default:
	}

	proxy.holdPuback.Store(false)
	proxy.releaseHeld()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish after the PUBACK: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish did not complete after the PUBACK was released")
	}
}

// TestClientPublishRetryLimit checks the documented limit: an exchange that is
// never confirmed fails with ErrRetryLimit after MaxRetries retransmissions
// instead of retrying forever.
func TestClientPublishRetryLimit(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	proxy := newTestProxy(t, b.Addr())
	proxy.holdPuback.Store(true)
	ctx := testContext(t)

	c := dialClient(t, client.Config{
		Addr:          b.Addr(),
		Dialer:        proxy.DialContext,
		ClientID:      "retransmit-limit",
		Version:       mqtt.Version5,
		CleanStart:    true,
		RetryInterval: 15 * time.Millisecond,
		MaxRetries:    1,
	})
	err := c.Publish(ctx, "retransmit/topic", []byte("never-acked"), 1, false)
	if !errors.Is(err, client.ErrRetryLimit) {
		t.Fatalf("Publish = %v, want ErrRetryLimit", err)
	}
	if !c.Connected() {
		t.Fatal("an exhausted retransmission must not end the connection")
	}
	proxy.holdPuback.Store(false)
	proxy.releaseHeld()
}

// TestClientQoS2PubrelRetransmits holds the PUBCOMP, so the client's QoS 2
// exchange stays in the PUBREL phase and its timer retransmits the PUBREL. The
// broker releases the exchange when the PUBCOMP is finally delivered.
func TestClientQoS2PubrelRetransmits(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	proxy := newTestProxy(t, b.Addr())
	proxy.holdPubcomp.Store(true)
	ctx := testContext(t)

	c := dialClient(t, client.Config{
		Addr:          b.Addr(),
		Dialer:        proxy.DialContext,
		ClientID:      "qos2-pubrel",
		Version:       mqtt.Version5,
		CleanStart:    true,
		RetryInterval: 20 * time.Millisecond,
		MaxRetries:    3,
	})
	done := make(chan error, 1)
	go func() { done <- c.Publish(ctx, "retransmit/topic", []byte("pubrel"), 2, false) }()

	waitFor(t, 3*time.Second, "the first PUBREL", func() bool { return proxy.Pubrels() >= 1 })
	waitFor(t, 3*time.Second, "a PUBREL retransmission", func() bool { return proxy.Pubrels() >= 2 })

	proxy.holdPubcomp.Store(false)
	proxy.releaseHeld()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish(qos2): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the QoS 2 publish did not complete after the PUBCOMP was released")
	}
}

// TestClientReconnectResumesInflight kills the connection while a QoS 1 PUBLISH is
// unconfirmed: the reconnect replays the exchange, and the proxy sees the PUBLISH
// again with DUP set on the new connection.
func TestClientReconnectResumesInflight(t *testing.T) {
	b := startBroker(t, reconnectBrokerConfig())
	proxy := newTestProxy(t, b.Addr())
	proxy.holdPuback.Store(true)
	ctx := testContext(t)
	disconnects := make(chan error, 4)

	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Dialer:     proxy.DialContext,
		ClientID:   "resume-inflight",
		Version:    mqtt.Version311,
		CleanStart: true,
		Reconnect: client.ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     20 * time.Millisecond,
			Jitter:       0,
		},
		// Long enough that only the reconnect, not the retry timer, can replay it.
		RetryInterval: 5 * time.Second,
		OnDisconnect:  func(err error) { disconnects <- err },
	})
	done := make(chan error, 1)
	go func() { done <- c.Publish(ctx, "resume/topic", []byte("resume"), 1, false) }()

	waitFor(t, 3*time.Second, "the first PUBLISH", func() bool { return proxy.Publishes() >= 1 })
	proxy.killAll()
	_ = recvError(t, disconnects, "the disconnect report")
	waitFor(t, 3*time.Second, "the replayed PUBLISH with DUP", func() bool { return proxy.Dups() >= 1 })

	proxy.holdPuback.Store(false)
	proxy.releaseHeld()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish across the reconnect: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the resumed Publish did not complete")
	}
}

// TestClientReconnectResumesQoS2Pubrel kills the connection after the PUBREC, so
// the exchange is in its PUBREL phase. The reconnect must replay the PUBREL (the
// PUBLISH is already confirmed) and the broker answers the PUBCOMP.
func TestClientReconnectResumesQoS2Pubrel(t *testing.T) {
	b := startBroker(t, reconnectBrokerConfig())
	proxy := newTestProxy(t, b.Addr())
	proxy.holdPubcomp.Store(true)
	ctx := testContext(t)
	disconnects := make(chan error, 4)

	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Dialer:     proxy.DialContext,
		ClientID:   "resume-pubrel",
		Version:    mqtt.Version311,
		CleanStart: true,
		Reconnect: client.ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     20 * time.Millisecond,
			Jitter:       0,
		},
		RetryInterval: 5 * time.Second,
		OnDisconnect:  func(err error) { disconnects <- err },
	})
	done := make(chan error, 1)
	go func() { done <- c.Publish(ctx, "resume/topic", []byte("resume-qos2"), 2, false) }()

	waitFor(t, 3*time.Second, "the first PUBREL", func() bool { return proxy.Pubrels() >= 1 })
	proxy.killAll()
	_ = recvError(t, disconnects, "the disconnect report")
	waitFor(t, 3*time.Second, "the replayed PUBREL", func() bool { return proxy.Pubrels() >= 2 })

	proxy.holdPubcomp.Store(false)
	proxy.releaseHeld()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("QoS 2 Publish across the reconnect: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the resumed QoS 2 Publish did not complete")
	}
}
