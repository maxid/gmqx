package client_test

// MQTT 3.1 support: the client rewrites its CONNECT to the older spelling and speaks
// 3.1.1 for everything else. These cases run against the real broker, so they prove
// both halves — the broker accepts the rewritten CONNECT, and the client's own
// decoding of a 3.1 session works.

import (
	"context"
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
)

func TestMQTT31RoundTrip(t *testing.T) {
	addr := startBroker(t, gmqx.Config{}).Addr()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	received := make(chan string, 1)
	c, err := client.Dial(ctx, client.Config{
		Addr:       addr,
		ClientID:   "v31-client",
		Version:    client.Version31,
		KeepAlive:  30 * time.Second,
		CleanStart: true,
		OnMessage: func(m *client.Message) {
			select {
			case received <- string(m.Payload):
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("dial with MQTT 3.1: %v", err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.Subscribe(ctx, client.Subscription{Filter: "v31/#", QoS: 1}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := c.PublishMessage(ctx, client.PublishOptions{Topic: "v31/one", Payload: []byte("hello"), QoS: 1}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case got := <-received:
		if got != "hello" {
			t.Fatalf("received %q", got)
		}
	case <-ctx.Done():
		t.Fatal("no delivery over an MQTT 3.1 connection")
	}
}
