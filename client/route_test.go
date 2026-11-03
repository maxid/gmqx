package client_test

// Handler-per-filter dispatch, end to end against the real broker: a subscription
// can carry its own handler, AddRoute registers one without a subscription, a
// publication matching several routes reaches every one of them, and OnMessage
// answers only what no route claimed.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
)

// collector records every topic its handler saw, so a case can look twice.
type collector struct {
	mu     sync.Mutex
	topics []string
}

func newCollector() *collector { return &collector{} }

func (c *collector) handle(m *client.Message) {
	c.mu.Lock()
	c.topics = append(c.topics, m.Topic)
	c.mu.Unlock()
}

func (c *collector) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.topics...)
}

// wait polls until want topics arrived or the deadline passes.
func (c *collector) wait(t *testing.T, name string, want int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := c.all(); len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: after 2s only %v arrived, want %d", name, c.all(), want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestSubscribeCarriesItsHandler(t *testing.T) {
	addr := startBroker(t, gmqx.Config{}).Addr()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	alpha, beta := newCollector(), newCollector()
	c, err := client.Dial(ctx, client.Config{Addr: addr, ClientID: "routes", Version: 5, CleanStart: true, KeepAlive: 30 * time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.Subscribe(ctx,
		client.Subscription{Filter: "route/alpha/#", QoS: 0, Handler: alpha.handle},
		client.Subscription{Filter: "route/beta/#", QoS: 0, Handler: beta.handle},
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	for _, topic := range []string{"route/alpha/1", "route/beta/1"} {
		if err := c.Publish(ctx, topic, []byte("x"), 0, false); err != nil {
			t.Fatalf("publish %s: %v", topic, err)
		}
	}

	if got := alpha.wait(t, "alpha", 1); len(got) != 1 || got[0] != "route/alpha/1" {
		t.Fatalf("alpha handler saw %v", got)
	}
	if got := beta.wait(t, "beta", 1); len(got) != 1 || got[0] != "route/beta/1" {
		t.Fatalf("beta handler saw %v", got)
	}
}

// A publication that two local routes cover is delivered once by the broker and
// dispatched to both handlers: that is paho's behaviour and the reason the route
// table exists.
func TestOverlappingRoutesBothRun(t *testing.T) {
	addr := startBroker(t, gmqx.Config{}).Addr()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wide, narrow := newCollector(), newCollector()
	c, err := client.Dial(ctx, client.Config{Addr: addr, ClientID: "overlap", Version: 5, CleanStart: true, KeepAlive: 30 * time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.Subscribe(ctx,
		client.Subscription{Filter: "over/#", QoS: 0, Handler: wide.handle},
		client.Subscription{Filter: "over/+", QoS: 0, Handler: narrow.handle},
	); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := c.Publish(ctx, "over/x", []byte("x"), 0, false); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if got := wide.wait(t, "wide", 1); len(got) != 1 {
		t.Fatalf("wide handler saw %v, want one publication", got)
	}
	if got := narrow.wait(t, "narrow", 1); len(got) != 1 {
		t.Fatalf("narrow handler saw %v, want the same publication", got)
	}
}

// OnMessage is the fallback: a route that does not match leaves the publication to
// it, and a route that does match keeps it away.
func TestOnMessageIsTheFallback(t *testing.T) {
	addr := startBroker(t, gmqx.Config{}).Addr()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	routed, fallback := newCollector(), newCollector()
	c, err := client.Dial(ctx, client.Config{
		Addr: addr, ClientID: "fallback", Version: 5, CleanStart: true, KeepAlive: 30 * time.Second,
		OnMessage: fallback.handle,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	// A route with no subscription of its own, and a subscription with no handler.
	c.AddRoute("fallback/routed/#", routed.handle)
	if _, err := c.Subscribe(ctx, client.Subscription{Filter: "fallback/#", QoS: 0}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := c.Publish(ctx, "fallback/routed/x", []byte("x"), 0, false); err != nil {
		t.Fatalf("publish routed: %v", err)
	}
	if got := routed.wait(t, "routed", 1); len(got) != 1 {
		t.Fatalf("routed handler saw %v", got)
	}

	if err := c.Publish(ctx, "fallback/plain", []byte("x"), 0, false); err != nil {
		t.Fatalf("publish plain: %v", err)
	}
	if got := fallback.wait(t, "fallback", 1); len(got) != 1 || got[0] != "fallback/plain" {
		t.Fatalf("fallback handler saw %v, want only the unclaimed publication", got)
	}
	// The routed publication belongs to the route, not to the fallback.
	time.Sleep(100 * time.Millisecond)
	if got := fallback.all(); len(got) != 1 {
		t.Fatalf("fallback handler saw %v, want exactly one publication", got)
	}
}
