//go:build !interop_without_paho

package interop

// The paho direction: a real third-party MQTT client implementation talking to
// the gmqx broker. Every case runs the broker in-process on an ephemeral
// loopback port, so only the client side is foreign.
//
// The file is compiled out when the interop_without_paho build tag is set;
// paho_skip_test.go then reports the same cases as skipped instead, which is how
// the matrix stays runnable on a machine that cannot build the third-party
// client (no network, no module cache).

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/maxid/gmqx"
)

// open is the paho OpenConnectionFunc shape. It gives the test the raw socket of
// a paho client, which is the only way to kill the connection without a
// DISCONNECT packet.
func (tap *connTap) open(uri *url.URL, _ mqtt.ClientOptions) (net.Conn, error) {
	return tap.dial(context.Background(), "tcp", uri.Host)
}

// pahoOptions is a paho client configuration with the timeouts of the matrix:
// bounded connect, bounded write, no automatic reconnection, so a lost
// connection is reported once instead of being retried behind the test's back.
func pahoOptions(clientID string) *mqtt.ClientOptions {
	return mqtt.NewClientOptions().
		SetClientID(clientID).
		SetConnectTimeout(connectTimeout).
		SetWriteTimeout(connectTimeout).
		SetKeepAlive(10 * time.Second).
		SetPingTimeout(5 * time.Second).
		SetAutoReconnect(false).
		SetConnectRetry(false)
}

// connectPaho connects a paho client to addr, fails the test when the handshake
// does, and disconnects it when the test ends.
func connectPaho(t *testing.T, addr string, opts *mqtt.ClientOptions) mqtt.Client {
	t.Helper()
	opts.AddBroker("tcp://" + addr)
	c := mqtt.NewClient(opts)
	waitToken(t, "CONNECT", c.Connect())
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

// pahoSubscriber connects a paho client whose default handler fills s.
func pahoSubscriber(t *testing.T, addr, clientID string, s *sink) mqtt.Client {
	t.Helper()
	return connectPaho(t, addr, pahoOptions(clientID).SetDefaultPublishHandler(
		func(_ mqtt.Client, m mqtt.Message) {
			s.push(delivery{
				topic:   m.Topic(),
				payload: m.Payload(),
				qos:     m.Qos(),
				retain:  m.Retained(),
				dup:     m.Duplicate(),
			})
		}))
}

// waitToken waits for one paho operation and turns a timeout or an error into a
// test failure.
func waitToken(t *testing.T, what string, tok mqtt.Token) {
	t.Helper()
	if !tok.WaitTimeout(deliveryTimeout) {
		t.Fatalf("paho %s: no answer within %s", what, deliveryTimeout)
	}
	if err := tok.Error(); err != nil {
		t.Fatalf("paho %s: %v", what, err)
	}
}

// TestPahoQoSRoundTrips publishes and subscribes at QoS 0, 1 and 2. QoS 2 is
// checked for exactly-once delivery: after the expected message the case
// listens for a duplicate and fails if one arrives. QoS 1 is allowed to
// duplicate by the protocol, so it is only checked for at-least-once delivery.
func TestPahoQoSRoundTrips(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	for _, qos := range []byte{0, 1, 2} {
		qos := qos
		t.Run(fmt.Sprintf("QoS%d", qos), func(t *testing.T) {
			topic := fmt.Sprintf("interop/paho/qos/%d", qos)
			payload := []byte(fmt.Sprintf("paho-qos%d-payload", qos))

			sub := newSink()
			subClient := pahoSubscriber(t, b.Addr(), fmt.Sprintf("paho-sub-qos%d", qos), sub)
			waitToken(t, "SUBSCRIBE", subClient.Subscribe(topic, qos, nil))

			pub := connectPaho(t, b.Addr(), pahoOptions(fmt.Sprintf("paho-pub-qos%d", qos)))
			waitToken(t, "PUBLISH", pub.Publish(topic, qos, false, payload))

			got := sub.next(t, fmt.Sprintf("the QoS %d delivery", qos))
			if got.topic != topic {
				t.Fatalf("delivered topic = %q, want %q", got.topic, topic)
			}
			if !bytes.Equal(got.payload, payload) {
				t.Fatalf("delivered payload = %s, want %q", describe(got.payload), payload)
			}
			if got.qos != qos {
				t.Fatalf("delivered QoS = %d, want %d", got.qos, qos)
			}
			if got.retain {
				t.Fatal("a live delivery carried the retain flag")
			}
			if qos == 2 {
				sub.expectNone(t, "QoS 2 delivered exactly once", quietWindow)
			}
		})
	}
}

// TestPahoRetainedMessage checks that a retained publication of a foreign client
// is stored and replayed to a subscriber that connects afterwards. The retained
// state is cleared at the end of the test.
func TestPahoRetainedMessage(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	topic := "interop/paho/retained"
	payload := []byte("paho-retained-payload")

	pub := connectPaho(t, b.Addr(), pahoOptions("paho-retained-pub"))
	waitToken(t, "retained PUBLISH", pub.Publish(topic, 1, true, payload))
	t.Cleanup(func() {
		// An empty payload clears the retained message; the token is not
		// interesting here.
		pub.Publish(topic, 1, true, []byte{})
	})

	// This subscriber did not exist when the message was published.
	sub := newSink()
	subClient := pahoSubscriber(t, b.Addr(), "paho-retained-sub", sub)
	waitToken(t, "SUBSCRIBE", subClient.Subscribe(topic, 1, nil))

	got := sub.next(t, "the retained message")
	if !bytes.Equal(got.payload, payload) {
		t.Fatalf("retained payload = %s, want %q", describe(got.payload), payload)
	}
	if !got.retain {
		t.Fatal("the replayed retained message did not carry the retain flag")
	}
	sub.expectNone(t, "the retained message replayed once", quietWindow)
}

// TestPahoWill checks the two halves of the will contract with a foreign client:
// a connection that dies without DISCONNECT publishes it, and a connection that
// says goodbye does not.
func TestPahoWill(t *testing.T) {
	t.Run("abrupt disconnect publishes the will", func(t *testing.T) {
		b := startBroker(t, gmqx.Config{})
		topic := "interop/paho/will/abrupt"
		willPayload := "paho-will-abrupt"

		sub := newSink()
		subClient := pahoSubscriber(t, b.Addr(), "paho-will-sub-abrupt", sub)
		waitToken(t, "SUBSCRIBE", subClient.Subscribe(topic, 1, nil))

		tap := &connTap{}
		opts := pahoOptions("paho-will-abrupt").
			SetWill(topic, willPayload, 1, false).
			SetCustomOpenConnectionFn(tap.open)
		willClient := connectPaho(t, b.Addr(), opts)
		if !willClient.IsConnectionOpen() {
			t.Fatal("the will client is not connected")
		}

		tap.kill(t)

		got := sub.next(t, "the will of the killed connection")
		if got.topic != topic {
			t.Fatalf("will topic = %q, want %q", got.topic, topic)
		}
		if string(got.payload) != willPayload {
			t.Fatalf("will payload = %s, want %q", describe(got.payload), willPayload)
		}
		if got.qos != 1 {
			t.Fatalf("will QoS = %d, want 1", got.qos)
		}
		sub.expectNone(t, "the will published exactly once", quietWindow)
	})

	t.Run("clean disconnect publishes nothing", func(t *testing.T) {
		b := startBroker(t, gmqx.Config{})
		topic := "interop/paho/will/clean"

		sub := newSink()
		subClient := pahoSubscriber(t, b.Addr(), "paho-will-sub-clean", sub)
		waitToken(t, "SUBSCRIBE", subClient.Subscribe(topic, 1, nil))

		opts := pahoOptions("paho-will-clean").
			SetWill(topic, "paho-will-clean", 1, false)
		willClient := connectPaho(t, b.Addr(), opts)
		willClient.Disconnect(100)

		sub.expectNone(t, "a will after a clean DISCONNECT", quietWindow)

		// The subscriber is still live: a normal publication reaches it.
		pub := connectPaho(t, b.Addr(), pahoOptions("paho-will-clean-pub"))
		waitToken(t, "PUBLISH", pub.Publish(topic, 1, false, "after-clean-disconnect"))
		got := sub.next(t, "the control message")
		if string(got.payload) != "after-clean-disconnect" {
			t.Fatalf("control payload = %s", describe(got.payload))
		}
	})
}

// TestPahoPersistentSession checks that a session of a foreign client outlives
// the connection: messages published while the client is away are queued by the
// broker and delivered after the reconnect.
//
// The test does not sleep to "wait for the session to be stored": it polls the
// broker's own SessionStore, which is passed in by the test. That makes both the
// store step and the offline queue observable, and removes the race between the
// DISCONNECT and the publication.
func TestPahoPersistentSession(t *testing.T) {
	store := gmqx.NewMemorySessionStore()
	b := startBroker(t, gmqx.Config{SessionStore: store})

	topic := "interop/paho/session"
	clientID := "paho-persistent"

	s := newSink()
	opts := func() *mqtt.ClientOptions {
		return pahoOptions(clientID).
			SetCleanSession(false).
			SetDefaultPublishHandler(func(_ mqtt.Client, m mqtt.Message) {
				s.push(delivery{
					topic:   m.Topic(),
					payload: m.Payload(),
					qos:     m.Qos(),
					retain:  m.Retained(),
					dup:     m.Duplicate(),
				})
			})
	}

	first := connectPaho(t, b.Addr(), opts())
	waitToken(t, "SUBSCRIBE", first.Subscribe(topic, 1, nil))
	first.Disconnect(100)

	// The session is stored only after the broker processed the DISCONNECT.
	waitFor(t, 5*time.Second, "the broker to store the persistent session", func() bool {
		state, ok := store.Load(clientID)
		return ok && len(state.Subscriptions) == 1
	})

	// Publish while the subscriber is away.
	pub := connectPaho(t, b.Addr(), pahoOptions("paho-persistent-pub"))
	const queued = 3
	for i := 0; i < queued; i++ {
		waitToken(t, "PUBLISH", pub.Publish(topic, 1, false, fmt.Sprintf("queued-%d", i)))
	}
	waitFor(t, 5*time.Second, "the broker to queue the offline messages", func() bool {
		state, ok := store.Load(clientID)
		return ok && len(state.Queue) == queued
	})

	// Reconnect with the same client id and the same clean-session flag: the
	// broker must resume the session and drain the queue.
	second := connectPaho(t, b.Addr(), opts())
	if !second.IsConnectionOpen() {
		t.Fatal("the reconnected client is not connected")
	}
	for i := 0; i < queued; i++ {
		got := s.next(t, fmt.Sprintf("queued message %d", i))
		want := fmt.Sprintf("queued-%d", i)
		if string(got.payload) != want {
			t.Fatalf("queued message %d = %s, want %q", i, describe(got.payload), want)
		}
		if got.qos != 1 {
			t.Fatalf("queued message %d arrived at QoS %d, want 1", i, got.qos)
		}
	}
	s.expectNone(t, "the offline queue drained exactly once", quietWindow)
}

// TestPahoLargePayload pushes 256 KiB through the broker at QoS 1 and compares
// the bytes, so a truncation or a packet-size limit that is too tight shows up.
func TestPahoLargePayload(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	topic := "interop/paho/large"
	payload := cleanPayload(256 << 10)

	sub := newSink()
	subClient := pahoSubscriber(t, b.Addr(), "paho-large-sub", sub)
	waitToken(t, "SUBSCRIBE", subClient.Subscribe(topic, 1, nil))

	pub := connectPaho(t, b.Addr(), pahoOptions("paho-large-pub"))
	waitToken(t, "PUBLISH", pub.Publish(topic, 1, false, payload))

	got := sub.next(t, "the 256 KiB delivery")
	if len(got.payload) != len(payload) {
		t.Fatalf("delivered %d bytes, want %d", len(got.payload), len(payload))
	}
	if !bytes.Equal(got.payload, payload) {
		t.Fatal("the delivered payload differs from the published one")
	}
}

// TestPahoOversizedPublish checks the broker's Maximum Packet Size from the
// client's point of view: a publication above it must end the connection. The
// broker is configured with a 4 KiB limit, so the 8 KiB payload cannot be
// accepted. After the disconnect a fresh client still works, which shows the
// broker dropped the connection rather than breaking.
func TestPahoOversizedPublish(t *testing.T) {
	const limit = 4 << 10
	b := startBroker(t, gmqx.Config{MaxPacketSize: limit})
	topic := "interop/paho/oversized"

	// A small round trip first: the connection is healthy before the oversized
	// packet, so the disconnect below is caused by the packet and not by
	// something else.
	sub := newSink()
	subClient := pahoSubscriber(t, b.Addr(), "paho-oversized-sub", sub)
	waitToken(t, "SUBSCRIBE", subClient.Subscribe(topic, 1, nil))

	lost := make(chan error, 1)
	opts := pahoOptions("paho-oversized").SetConnectionLostHandler(
		func(_ mqtt.Client, err error) {
			select {
			case lost <- err:
			default:
			}
		})
	pub := connectPaho(t, b.Addr(), opts)
	waitToken(t, "small PUBLISH", pub.Publish(topic, 1, false, []byte("small")))
	if got := sub.next(t, "the small delivery"); string(got.payload) != "small" {
		t.Fatalf("small delivery = %s", describe(got.payload))
	}

	// The oversized publication is not acknowledged: the broker cannot decode a
	// packet above its limit and closes the connection.
	pub.Publish(topic, 0, false, cleanPayload(2*limit))

	select {
	case err := <-lost:
		if err == nil {
			t.Fatal("the connection loss was reported with a nil error")
		}
		t.Logf("the client reported the disconnect after the oversized publish: %v", err)
	case <-time.After(deliveryTimeout):
		t.Fatalf("the client did not see a disconnect within %s after a %d-byte publish above the %d-byte limit",
			deliveryTimeout, 2*limit, limit)
	}

	// The broker is still serving: a fresh client completes a round trip.
	late := newSink()
	lateClient := pahoSubscriber(t, b.Addr(), "paho-oversized-late", late)
	waitToken(t, "SUBSCRIBE", lateClient.Subscribe(topic, 1, nil))
	fresh := connectPaho(t, b.Addr(), pahoOptions("paho-oversized-fresh"))
	waitToken(t, "PUBLISH", fresh.Publish(topic, 1, false, []byte("after-the-drop")))
	if got := late.next(t, "the delivery after the oversized publish"); string(got.payload) != "after-the-drop" {
		t.Fatalf("delivery after the drop = %s", describe(got.payload))
	}
}
