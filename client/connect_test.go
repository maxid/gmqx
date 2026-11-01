package client_test

// Wait policy for this file (and the rule a future change must keep): every wait
// is on an observable event, with a generous ceiling that only exists so a broken
// build fails instead of hanging. A broker-side event is observed through the
// EventSink the broker calls (OnDisconnect/OnConnectionLost/OnPublish) or through
// a channel the test controls; a client-side event is observed through the
// callbacks or through Connected(). No test in this file uses a sleep as its
// detection mechanism: the negative will assertion below waits for the broker to
// resolve the connection first, so a sleep can neither hide nor invent the outcome.
//
// TestClientWillNotPublishedOnClose is the reason for that rule: Close used to
// wake the supervisor before DISCONNECT was written, so teardown could close the
// socket first and the server would publish the will of an "abrupt" close. The
// test now watches the broker's own view of the close (a clean OnDisconnect, never
// OnConnectionLost, and no accepted publish on the will topic), which is what
// fails if the ordering regresses.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gnety/codec/mqtt"
)

// TestClientPublishSubscribeQoS runs the three QoS levels against the real broker
// for both protocol versions: the publisher's Publish returns after the handshake
// (PUBACK for QoS 1, PUBCOMP for QoS 2) and the subscriber receives the message
// with the granted QoS.
func TestClientPublishSubscribeQoS(t *testing.T) {
	for _, version := range []byte{mqtt.Version311, mqtt.Version5} {
		for _, qos := range []byte{0, 1, 2} {
			version, qos := version, qos
			t.Run(fmt.Sprintf("v%d/qos%d", version, qos), func(t *testing.T) {
				b := startBroker(t, gmqx.Config{})
				ctx := testContext(t)
				rec := newRecorder(4)
				sub := dialClient(t, client.Config{
					Addr:       b.Addr(),
					ClientID:   fmt.Sprintf("sub-v%d-q%d", version, qos),
					Version:    version,
					CleanStart: true,
					OnMessage:  rec.OnMessage,
				})
				codes, err := sub.Subscribe(ctx, client.Subscription{Filter: "test/topic", QoS: qos})
				if err != nil {
					t.Fatalf("Subscribe: %v", err)
				}
				if len(codes) != 1 || codes[0] != qos {
					t.Fatalf("granted codes = %v, want [%d]", codes, qos)
				}
				if sub.Connected() != true {
					t.Fatal("client is not connected after Subscribe")
				}
				if sub.SessionPresent() {
					t.Fatal("a clean start reported an existing session")
				}

				pub := dialClient(t, client.Config{
					Addr:       b.Addr(),
					ClientID:   fmt.Sprintf("pub-v%d-q%d", version, qos),
					Version:    version,
					CleanStart: true,
				})
				if err := pub.Publish(ctx, "test/topic", []byte("hello"), qos, false); err != nil {
					t.Fatalf("Publish(qos %d): %v", qos, err)
				}
				msg := rec.next(t)
				if msg.Topic != "test/topic" || string(msg.Payload) != "hello" {
					t.Fatalf("delivered %q %q, want test/topic hello", msg.Topic, msg.Payload)
				}
				if msg.QoS != qos {
					t.Fatalf("delivered QoS %d, want %d", msg.QoS, qos)
				}
				if msg.Retain || msg.Duplicate {
					t.Fatalf("unexpected retain/duplicate flags: %v/%v", msg.Retain, msg.Duplicate)
				}
			})
		}
	}
}

// TestClientUnsubscribe proves the client's SUBSCRIBE/UNSUBSCRIBE bookkeeping
// against the broker: after an UNSUBACK the broker no longer delivers the topic.
func TestClientUnsubscribe(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	ctx := testContext(t)
	rec := newRecorder(4)
	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		ClientID:   "unsub",
		Version:    mqtt.Version5,
		CleanStart: true,
		OnMessage:  rec.OnMessage,
	})
	if _, err := c.Subscribe(ctx, client.Subscription{Filter: "unsub/topic", QoS: 1}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := c.Publish(ctx, "unsub/topic", []byte("before"), 1, false); err != nil {
		t.Fatalf("Publish before unsubscribe: %v", err)
	}
	if msg := rec.next(t); string(msg.Payload) != "before" {
		t.Fatalf("unexpected first delivery %q", msg.Payload)
	}
	if err := c.Unsubscribe(ctx, "unsub/topic"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if err := c.Publish(ctx, "unsub/topic", []byte("after"), 1, false); err != nil {
		t.Fatalf("Publish after unsubscribe: %v", err)
	}
	rec.none(t, 300*time.Millisecond)
}

// TestClientRetainedMessage publishes a retained message and checks that a
// subscriber created afterwards receives it with the retain flag.
func TestClientRetainedMessage(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	ctx := testContext(t)

	pub := dialClient(t, client.Config{Addr: b.Addr(), ClientID: "retain-pub", Version: mqtt.Version5, CleanStart: true})
	if err := pub.PublishMessage(ctx, client.PublishOptions{
		Topic:   "state/device-1",
		Payload: []byte("online"),
		QoS:     1,
		Retain:  true,
	}); err != nil {
		t.Fatalf("retained Publish: %v", err)
	}

	rec := newRecorder(2)
	sub := dialClient(t, client.Config{
		Addr:       b.Addr(),
		ClientID:   "retain-sub",
		Version:    mqtt.Version5,
		CleanStart: true,
		OnMessage:  rec.OnMessage,
	})
	if _, err := sub.Subscribe(ctx, client.Subscription{Filter: "state/#", QoS: 1}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	msg := rec.next(t)
	if msg.Topic != "state/device-1" || string(msg.Payload) != "online" {
		t.Fatalf("retained replay = %q %q", msg.Topic, msg.Payload)
	}
	if !msg.Retain {
		t.Fatal("a retained replay must carry the retain flag")
	}
	if msg.QoS != 1 {
		t.Fatalf("retained replay QoS = %d, want 1", msg.QoS)
	}
}

// TestClientWillOnAbruptDisconnect registers a will and proves the real broker
// publishes it when the client's socket dies without a DISCONNECT (the proxy
// closes both ends of the connection).
func TestClientWillOnAbruptDisconnect(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	ctx := testContext(t)

	rec := newRecorder(2)
	sub := dialClient(t, client.Config{
		Addr:       b.Addr(),
		ClientID:   "will-sub",
		Version:    mqtt.Version311,
		CleanStart: true,
		OnMessage:  rec.OnMessage,
	})
	if _, err := sub.Subscribe(ctx, client.Subscription{Filter: "will/topic", QoS: 1}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	proxy := newTestProxy(t, b.Addr())
	dialed := make(chan struct{}, 1)
	willClient := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Dialer:     proxy.DialContext,
		ClientID:   "will-client",
		Version:    mqtt.Version311,
		CleanStart: true,
		Will: &client.Will{
			Topic:   "will/topic",
			Payload: []byte("gone"),
			QoS:     1,
		},
		OnConnect: func(bool) {
			select {
			case dialed <- struct{}{}:
			default:
			}
		},
	})
	select {
	case <-dialed:
	case <-time.After(3 * time.Second):
		t.Fatal("will client never reported its connection")
	}
	if !willClient.Connected() {
		t.Fatal("will client is not connected")
	}

	proxy.killAll()

	msg := rec.next(t)
	if msg.Topic != "will/topic" || string(msg.Payload) != "gone" {
		t.Fatalf("will = %q %q, want will/topic gone", msg.Topic, msg.Payload)
	}
}

// TestClientMQTT5Properties checks the client-set MQTT 5 properties end to end:
// content type, response topic, correlation data and user properties on a
// PUBLISH, user properties on a SUBSCRIBE, and the subscription identifier the
// broker attaches to the delivery.
func TestClientMQTT5Properties(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	ctx := testContext(t)
	rec := newRecorder(2)

	sub := dialClient(t, client.Config{
		Addr:       b.Addr(),
		ClientID:   "props-sub",
		Version:    mqtt.Version5,
		CleanStart: true,
		OnMessage:  rec.OnMessage,
	})
	codes, err := sub.Subscribe(ctx, client.Subscription{
		Filter:                 "props/topic",
		QoS:                    1,
		SubscriptionIdentifier: 42,
		Properties: mqtt.Properties{
			UserProperties: []mqtt.UserProperty{{Key: "sub", Value: "yes"}},
		},
	})
	if err != nil || len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("Subscribe = %v, %v", codes, err)
	}

	pub := dialClient(t, client.Config{Addr: b.Addr(), ClientID: "props-pub", Version: mqtt.Version5, CleanStart: true})
	format := byte(1)
	err = pub.PublishMessage(ctx, client.PublishOptions{
		Topic:   "props/topic",
		Payload: []byte(`{"ok":true}`),
		QoS:     1,
		Properties: mqtt.Properties{
			PayloadFormat:   &format,
			ContentType:     "application/json",
			ResponseTopic:   "props/reply",
			CorrelationData: []byte{0xde, 0xad},
			UserProperties:  []mqtt.UserProperty{{Key: "trace", Value: "abc"}},
		},
	})
	if err != nil {
		t.Fatalf("PublishMessage: %v", err)
	}

	msg := rec.next(t)
	if msg.Properties.ContentType != "application/json" {
		t.Fatalf("content type = %q", msg.Properties.ContentType)
	}
	if msg.Properties.ResponseTopic != "props/reply" {
		t.Fatalf("response topic = %q", msg.Properties.ResponseTopic)
	}
	if !bytes.Equal(msg.Properties.CorrelationData, []byte{0xde, 0xad}) {
		t.Fatalf("correlation data = %v", msg.Properties.CorrelationData)
	}
	if len(msg.Properties.UserProperties) != 1 ||
		msg.Properties.UserProperties[0] != (mqtt.UserProperty{Key: "trace", Value: "abc"}) {
		t.Fatalf("user properties = %v", msg.Properties.UserProperties)
	}
	if len(msg.Properties.SubscriptionIdentifiers) != 1 || msg.Properties.SubscriptionIdentifiers[0] != 42 {
		t.Fatalf("subscription identifiers = %v, want [42]", msg.Properties.SubscriptionIdentifiers)
	}
}

// TestClientAssignedClientID checks that an empty MQTT 5 client id is assigned by
// the server, surfaced through ServerInfo and reused by a reconnect.
func TestClientAssignedClientID(t *testing.T) {
	b := startBroker(t, gmqx.Config{})
	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Version:    mqtt.Version5,
		CleanStart: true,
	})
	info := c.ServerInfo()
	if info.AssignedClientID == "" {
		t.Fatal("the connection did not surface an assigned client id")
	}
	if got := c.ClientID(); got != info.AssignedClientID {
		t.Fatalf("ClientID() = %q, want the assigned %q", got, info.AssignedClientID)
	}
	if info.ReceiveMaximum == 0 {
		t.Fatal("the CONNACK receive maximum was not surfaced")
	}
	if info.MaximumPacketSize == 0 {
		t.Fatal("the CONNACK maximum packet size was not surfaced")
	}
	if info.MaximumQoS != 2 {
		t.Fatalf("maximum QoS = %d, want 2 (absent property)", info.MaximumQoS)
	}
	if !info.RetainAvailable || !info.WildcardSubscriptionAvailable {
		t.Fatal("the CONNACK capability flags were not surfaced")
	}
}

// denyPublish is an ACL that permits CONNECT and SUBSCRIBE and denies PUBLISH.
type denyPublish struct{}

func (denyPublish) Check(_ context.Context, _ gmqx.SessionInfo, _ string, action gmqx.AclAction) bool {
	return action != gmqx.AclPublish
}

func (denyPublish) RequiresOffload() bool { return false }

// TestClientNegativeAcknowledgement checks that a broker refusal is surfaced as a
// ReasonError with the server's reason code: PUBACK 0x87 for QoS 1 and PUBREC
// 0x87 for QoS 2.
func TestClientNegativeAcknowledgement(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		qos := qos
		t.Run(fmt.Sprintf("qos%d", qos), func(t *testing.T) {
			b := startBroker(t, gmqx.Config{ACL: denyPublish{}})
			ctx := testContext(t)
			c := dialClient(t, client.Config{
				Addr:       b.Addr(),
				ClientID:   fmt.Sprintf("denied-%d", qos),
				Version:    mqtt.Version5,
				CleanStart: true,
			})
			err := c.Publish(ctx, "denied/topic", []byte("nope"), qos, false)
			if err == nil {
				t.Fatal("a denied publish was reported as accepted")
			}
			var reason *client.ReasonError
			if !errors.As(err, &reason) {
				t.Fatalf("error %v is not a *ReasonError", err)
			}
			if reason.Code != 0x87 {
				t.Fatalf("reason code = 0x%02x, want 0x87 (not authorized)", reason.Code)
			}
			if reason.Packet != "PUBACK" && reason.Packet != "PUBREC" {
				t.Fatalf("reason packet = %q", reason.Packet)
			}
			if !c.Connected() {
				t.Fatal("a refused publish must not end the connection")
			}
		})
	}
}

// closeWatcher is an EventSink that records how the broker saw one connection end
// and what it published while the test ran. It is the observable condition the will
// test waits on: the broker resolves the will of a closing connection before it calls
// the disconnect hook, so once ended is closed no will publication can still be
// pending.
type closeWatcher struct {
	clientID string
	topic    string

	mu        sync.Mutex
	clean     int
	lost      int
	published []string
	once      sync.Once
	ended     chan struct{}
}

func newCloseWatcher(clientID, topic string) *closeWatcher {
	return &closeWatcher{clientID: clientID, topic: topic, ended: make(chan struct{})}
}

func (w *closeWatcher) OnConnect(context.Context, gmqx.ConnectInfo) {}

func (w *closeWatcher) OnPing(context.Context, string) {}

func (w *closeWatcher) OnDisconnect(_ context.Context, clientID string, _ error) {
	if clientID != w.clientID {
		return
	}
	w.mu.Lock()
	w.clean++
	w.mu.Unlock()
	w.once.Do(func() { close(w.ended) })
}

func (w *closeWatcher) OnConnectionLost(_ context.Context, clientID string, _ error) {
	if clientID != w.clientID {
		return
	}
	w.mu.Lock()
	w.lost++
	w.mu.Unlock()
	w.once.Do(func() { close(w.ended) })
}

func (w *closeWatcher) OnPublish(_ context.Context, ev gmqx.PublishEvent) gmqx.PublishResult {
	w.mu.Lock()
	if ev.Topic == w.topic {
		w.published = append(w.published, string(ev.Payload))
	}
	w.mu.Unlock()
	return gmqx.PublishResult{Disposition: gmqx.PublishAccepted}
}

func (w *closeWatcher) counts() (clean, lost int, published []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.clean, w.lost, append([]string(nil), w.published...)
}

// TestClientWillNotPublishedOnClose is the counterpart of the abrupt-disconnect
// will test: a client that closes in an orderly way sends DISCONNECT, so the
// broker does not publish its will.
//
// The wait is the broker's own view of the close: c.Close returns after the
// DISCONNECT is on the wire, and the test then waits (bounded, 10 s) for the broker
// to report the end of that connection. A clean OnDisconnect proves the server
// processed the DISCONNECT; OnConnectionLost proves it saw an abrupt close instead.
// The will is resolved before either hook runs, so "no accepted publish on the will
// topic" needs no settle period to be meaningful.
func TestClientWillNotPublishedOnClose(t *testing.T) {
	w := newCloseWatcher("no-will-client", "will/clean")
	b := startBroker(t, gmqx.Config{Sink: w})
	ctx := testContext(t)
	rec := newRecorder(2)

	sub := dialClient(t, client.Config{
		Addr:       b.Addr(),
		ClientID:   "no-will-sub",
		Version:    mqtt.Version311,
		CleanStart: true,
		OnMessage:  rec.OnMessage,
	})
	if _, err := sub.Subscribe(ctx, client.Subscription{Filter: "will/clean", QoS: 1}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		ClientID:   "no-will-client",
		Version:    mqtt.Version311,
		CleanStart: true,
		Will:       &client.Will{Topic: "will/clean", Payload: []byte("must-not-arrive"), QoS: 1},
	})
	if !c.Connected() {
		t.Fatal("the will client did not connect")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-w.ended:
	case <-time.After(10 * time.Second):
		t.Fatal("the broker did not report the end of the will client within 10s")
	}
	clean, lost, published := w.counts()
	if lost != 0 {
		t.Fatalf("the broker saw %d abrupt close(s) after an orderly Close: DISCONNECT was lost, so the will is published", lost)
	}
	if clean != 1 {
		t.Fatalf("the broker reported %d clean disconnects, want 1", clean)
	}
	if len(published) != 0 {
		t.Fatalf("the broker published the will %d time(s): %v", len(published), published)
	}
	// The broker-side checks above are the assertion; this only keeps the
	// subscriber honest about a delivery the broker said it never made.
	rec.none(t, 100*time.Millisecond)
}

// TestClientAssignedClientIDReusedOnReconnect checks the other half of the
// assigned-client-id rule: a reconnect of a client that sent an empty id must
// present the identifier the server assigned, not an empty one again.
func TestClientAssignedClientIDReusedOnReconnect(t *testing.T) {
	b := startBroker(t, reconnectBrokerConfig())
	proxy := newTestProxy(t, b.Addr())
	connects := make(chan bool, 4)
	disconnects := make(chan error, 4)

	c := dialClient(t, client.Config{
		Addr:       b.Addr(),
		Dialer:     proxy.DialContext,
		Version:    mqtt.Version5,
		CleanStart: true,
		Reconnect: client.ReconnectPolicy{
			Enabled:      true,
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     20 * time.Millisecond,
			Jitter:       0,
		},
		OnConnect:    func(sp bool) { connects <- sp },
		OnDisconnect: func(err error) { disconnects <- err },
	})
	assigned := c.ClientID()
	if assigned == "" {
		t.Fatal("the first connection did not surface an assigned client id")
	}
	recvBool(t, connects, "the initial connect report")

	proxy.killAll()
	_ = recvError(t, disconnects, "the disconnect report")
	recvBool(t, connects, "the reconnect report")
	if got := c.ClientID(); got != assigned {
		t.Fatalf("the reconnect used client id %q, want the assigned %q", got, assigned)
	}
	if !c.Connected() {
		t.Fatal("the client did not reconnect")
	}
}
