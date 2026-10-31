package gmqx

// Dispatcher SPI tests: a custom dispatcher sees the publication and decides
// whether it is delivered, the routing seam it uses instead of re-implementing
// matching works, and a nil Config.Dispatcher selects the built-in policy that
// preserves the broker's earlier behavior.

import (
	"context"
	"sync"
	"testing"
	"time"
)

// recordedDispatch is one publication a custom dispatcher observed, with the
// ConnInfo of the context it was called with.
type recordedDispatch struct {
	pub   Publication
	info  ConnInfo
	hasIn bool
}

// recordingDispatcher is a Dispatcher that records every publication and either
// routes it through the broker's seam or suppresses it.
type recordingDispatcher struct {
	suppress bool

	mu      sync.Mutex
	broker  *Broker
	records []recordedDispatch
}

func (d *recordingDispatcher) Bind(b *Broker) {
	d.mu.Lock()
	d.broker = b
	d.mu.Unlock()
}

func (d *recordingDispatcher) Dispatch(ctx context.Context, pub Publication) error {
	info, hasInfo := ConnFromContext(ctx)
	d.mu.Lock()
	d.records = append(d.records, recordedDispatch{pub: pub, info: info, hasIn: hasInfo})
	broker := d.broker
	suppress := d.suppress
	d.mu.Unlock()
	if suppress || broker == nil {
		return nil
	}
	for _, target := range broker.LocalTargets(pub) {
		_ = broker.SendLocal(ctx, target, pub)
	}
	return broker.SpreadPublication(ctx, pub)
}

func (d *recordingDispatcher) snapshot() ([]recordedDispatch, *Broker) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]recordedDispatch(nil), d.records...), d.broker
}

func (d *recordingDispatcher) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.records)
}

// TestNilDispatcherKeepsDefaultRouting pins the compatibility promise: without a
// configured dispatcher the broker installs DefaultDispatcher and a
// publish/subscribe round trip is unchanged.
func TestNilDispatcherKeepsDefaultRouting(t *testing.T) {
	b := startBroker(t, Config{})
	if _, ok := b.dispatcher.(*DefaultDispatcher); !ok {
		t.Fatalf("nil Config.Dispatcher selected %T, want *DefaultDispatcher", b.dispatcher)
	}
	sub := dialTestClient(t, b.Addr(), "default-sub")
	subscribeTestClient(t, sub, "round/#")

	pub := dialTestClient(t, b.Addr(), "default-pub")
	publishPlain(t, pub, "round/x", "unchanged")

	_, topic, payload := readPublish(t, sub)
	if topic != "round/x" || string(payload) != "unchanged" {
		t.Fatalf("delivery = %q %q", topic, payload)
	}
}

// TestCustomDispatcherSeesPublicationAndRoutes checks that a custom dispatcher
// receives the accepted publication (identity, QoS and context included) and can
// route it through the broker's seam.
func TestCustomDispatcherSeesPublicationAndRoutes(t *testing.T) {
	d := &recordingDispatcher{}
	b := startBroker(t, Config{Namespace: "tenant-a", Dispatcher: d})
	if _, bound := d.snapshot(); bound != b {
		t.Fatalf("dispatcher bound to %p, want %p", bound, b)
	}

	sub := dialTestClient(t, b.Addr(), "dispatch-sub")
	subscribeTestClient(t, sub, "plant/+/temp")

	pub := dialTestClient(t, b.Addr(), "dispatch-pub")
	header, body := qos1Publish("plant/a/temp", 11, "42")
	pub.writePacket(header, body)
	if h, p, err := pub.readPacket(); err != nil || h != 0x40 || len(p) != 2 || p[1] != 11 {
		t.Fatalf("PUBACK = %#x %x err=%v", h, p, err)
	}

	// The dispatcher routed through the seam, so the subscriber still received
	// the publication.
	_, topic, payload := readPublish(t, sub)
	if topic != "plant/a/temp" || string(payload) != "42" {
		t.Fatalf("delivery = %q %q", topic, payload)
	}

	records, _ := d.snapshot()
	if len(records) != 1 {
		t.Fatalf("dispatcher saw %d publications, want 1", len(records))
	}
	p := records[0].pub
	if p.Namespace != "tenant-a" || p.Topic != "plant/a/temp" || string(p.Payload) != "42" ||
		p.QoS != 1 || p.Retain || p.Dup {
		t.Fatalf("publication = %+v", p)
	}
	if p.FromClientID != "dispatch-pub" || p.FromConnectionID == "" {
		t.Fatalf("publication identity = %+v", p)
	}
	if !records[0].hasIn || records[0].info.ClientID != "dispatch-pub" || records[0].info.Namespace != "tenant-a" {
		t.Fatalf("dispatcher context = %+v (has info %v)", records[0].info, records[0].hasIn)
	}
}

// TestCustomDispatcherCanSuppressDelivery checks the decision point: a
// dispatcher that routes nowhere suppresses delivery, while the sink's
// acknowledgement still goes out (routing happens after the sink accepted).
func TestCustomDispatcherCanSuppressDelivery(t *testing.T) {
	d := &recordingDispatcher{suppress: true}
	b := startBroker(t, Config{Dispatcher: d})

	sub := dialTestClient(t, b.Addr(), "suppress-sub")
	subscribeTestClient(t, sub, "quiet/#")

	pub := dialTestClient(t, b.Addr(), "suppress-pub")
	header, body := qos1Publish("quiet/x", 5, "drop-me")
	pub.writePacket(header, body)
	if h, p, err := pub.readPacket(); err != nil || h != 0x40 || len(p) != 2 || p[1] != 5 {
		t.Fatalf("PUBACK = %#x %x err=%v", h, p, err)
	}

	_ = sub.conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if h, _, err := sub.readPacket(); err == nil {
		t.Fatalf("suppressed publication was delivered, header %#x", h)
	}
	if got := d.count(); got != 1 {
		t.Fatalf("dispatcher saw %d publications, want 1", got)
	}
}

// TestDispatcherDecidesClusterSpread checks that the dispatcher is the single
// decision point for cluster routing too: the default-style delegation spreads,
// a suppressing dispatcher does not.
func TestDispatcherDecidesClusterSpread(t *testing.T) {
	t.Run("forwarding", func(t *testing.T) {
		cluster := newFakeCluster()
		d := &recordingDispatcher{}
		a := startBroker(t, Config{NodeID: "a", ClusterBackend: cluster, Dispatcher: d})
		b := startBroker(t, Config{NodeID: "b", ClusterBackend: cluster})

		sub := dialTestClient(t, b.Addr(), "cluster-sub")
		subscribeTestClient(t, sub, "cluster/#")

		pub := dialTestClient(t, a.Addr(), "cluster-pub")
		publishPlain(t, pub, "cluster/x", "spread")
		_, topic, payload := readPublish(t, sub)
		if topic != "cluster/x" || string(payload) != "spread" {
			t.Fatalf("cluster delivery = %q %q", topic, payload)
		}
		if got := d.count(); got != 1 {
			t.Fatalf("dispatcher saw %d publications, want 1", got)
		}
	})

	t.Run("suppressed", func(t *testing.T) {
		cluster := newFakeCluster()
		d := &recordingDispatcher{suppress: true}
		a := startBroker(t, Config{NodeID: "a", ClusterBackend: cluster, Dispatcher: d})
		b := startBroker(t, Config{NodeID: "b", ClusterBackend: cluster})

		sub := dialTestClient(t, b.Addr(), "cluster-sub")
		subscribeTestClient(t, sub, "cluster/#")

		pub := dialTestClient(t, a.Addr(), "cluster-pub")
		publishPlain(t, pub, "cluster/x", "not-spread")

		_ = sub.conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		if h, _, err := sub.readPacket(); err == nil {
			t.Fatalf("suppressed cluster publication was delivered, header %#x", h)
		}
		if got := d.count(); got != 1 {
			t.Fatalf("dispatcher saw %d publications, want 1", got)
		}
	})
}
