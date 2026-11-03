// Command broker-demo embeds a gmqx broker in this process and verifies it with
// the library's own client.
//
// The point is the broker, not the client: a first client publishes a retained
// message and disconnects, a second client subscribes afterwards and receives the
// retained message as a replay. Client details (QoS, MQTT version, reconnects) are
// examples/client-demo; the broker API used here is gmqx.New, Start, Addr and Stop.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
	"github.com/maxid/gmqx/hook"
)

const steps = 6

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "broker TCP listen address; port 0 picks an ephemeral port")
	topic := flag.String("topic", "demo/retained", "topic the retained message is published on and replayed from")
	message := flag.String("message", "hello from the gmqx broker demo", "payload published with the retain flag")
	qos := flag.Int("qos", 1, "QoS of the publish and the subscription (0, 1 or 2)")
	version := flag.Int("version", 5, "MQTT protocol level of both clients: 4 (3.1.1) or 5")
	timeout := flag.Duration("timeout", 10*time.Second, "overall deadline of the demo")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `broker-demo embeds a gmqx broker in this process, publishes one retained
message with the gmqx client, then subscribes from a second client to show the
replay a later subscriber gets. Every connect also makes the broker publish a
greeting of its own, on "<topic>_reply".

Examples:
  go run ./examples/broker-demo
  go run ./examples/broker-demo -topic demo/retained -message "hello from gmqx"
  go run ./examples/broker-demo -addr 127.0.0.1:1883 -qos 0 -version 4

Flags:
`)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *qos < 0 || *qos > 2 {
		log.Fatalf("broker-demo: -qos %d is not 0, 1 or 2", *qos)
	}
	if *version != 4 && *version != 5 {
		log.Fatalf("broker-demo: -version %d is not 4 or 5", *version)
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(sigCtx, *timeout)
	defer cancel()

	// The broker announces every accepted connection by publishing a greeting of its
	// own: a Sink sees the connect, PublishMessage injects the publication, and the
	// configured Dispatcher routes it like any other (see greeter below).
	reply := *topic + "_reply"
	greet := &greeter{topic: reply}

	// Keep the broker's own log quiet; the demo prints its progress.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	broker, err := gmqx.New(gmqx.Config{Addr: *addr, Logger: logger, Sink: greet})
	if err != nil {
		log.Fatalf("broker-demo: create broker: %v", err)
	}
	if err := broker.Start(ctx); err != nil {
		log.Fatalf("broker-demo: start broker: %v", err)
	}
	greet.bind(broker)
	// Stop is idempotent and takes the cancelled ctx off the table, so every exit
	// path — including Ctrl-C — leaves no listener behind.
	defer func() { _ = broker.Stop(context.Background()) }()
	fmt.Printf("[1/%d] broker listening on %s\n", steps, broker.Addr())

	pub, err := client.Dial(ctx, client.Config{
		Addr:       broker.Addr(),
		ClientID:   "broker-demo-pub",
		Version:    byte(*version),
		KeepAlive:  30 * time.Second,
		CleanStart: true,
	})
	if err != nil {
		log.Fatalf("broker-demo: dial publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	fmt.Printf("[2/%d] publisher %q connected (MQTT level %d)\n", steps, pub.ClientID(), *version)

	if err := pub.PublishMessage(ctx, client.PublishOptions{
		Topic:   *topic,
		Payload: []byte(*message),
		QoS:     byte(*qos),
		Retain:  true,
	}); err != nil {
		log.Fatalf("broker-demo: publish retained: %v", err)
	}
	fmt.Printf("[3/%d] published retained %q to %s (QoS %d)\n", steps, *message, *topic, *qos)

	// The subscriber connects after the publish, so anything it sees is the
	// retained replay and not a live delivery.
	replayed := make(chan *client.Message, 2)
	sub, err := client.Dial(ctx, client.Config{
		Addr:       broker.Addr(),
		ClientID:   "broker-demo-sub",
		Version:    byte(*version),
		KeepAlive:  30 * time.Second,
		CleanStart: true,
	})
	if err != nil {
		log.Fatalf("broker-demo: dial subscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	codes, err := sub.Subscribe(ctx,
		client.Subscription{
			Filter: *topic,
			QoS:    byte(*qos),
			// The handler travels with the subscription (see examples/client-demo); the
			// retained replay the broker sends after the SUBACK arrives here.
			Handler: collect(replayed),
		},
		// The greeting the broker published when this connection was accepted. It is
		// retained too, so a subscriber that arrives later still sees it.
		client.Subscription{Filter: reply, QoS: 0, Handler: collect(replayed)},
	)
	if err != nil {
		log.Fatalf("broker-demo: subscribe: %v", err)
	}
	fmt.Printf("[4/%d] subscriber %q subscribed to %s and %s (granted %v)\n", steps, sub.ClientID(), *topic, reply, codes)

	replayedMsg, err := await(replayed, ctx)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			fmt.Println("[stop] interrupted")
			return
		}
		log.Fatalf("broker-demo: wait for the retained replay: %v", err)
	}
	fmt.Printf("[5/%d] replayed %s to a subscriber that connected later (QoS %d, retain %v): %s\n",
		steps, replayedMsg.Topic, replayedMsg.QoS, replayedMsg.Retain, replayedMsg.Payload)

	greeting, err := await(replayed, ctx)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			fmt.Println("[stop] interrupted")
			return
		}
		log.Fatalf("broker-demo: wait for the greeting: %v", err)
	}
	fmt.Printf("[6/%d] the broker's own publish reached %s: %s\n", steps, greeting.Topic, greeting.Payload)

	if err := sub.Close(); err != nil {
		log.Fatalf("broker-demo: close subscriber: %v", err)
	}
	if err := pub.Close(); err != nil {
		log.Fatalf("broker-demo: close publisher: %v", err)
	}
	if err := broker.Stop(context.Background()); err != nil {
		log.Fatalf("broker-demo: stop broker: %v", err)
	}
	fmt.Println("[done] retained message replayed to a later subscriber; broker stopped")
}

// greeter is the broker's EventSink: it sees every accepted connection and answers
// with a greeting the broker publishes itself.
//
// The publication is retained and the Sink is bound after Start, because OnConnect
// runs before the client has subscribed to anything: a plain publish at that moment
// would have no target, while a retained one is there for whoever subscribes next.
type greeter struct {
	mu     sync.Mutex
	broker *gmqx.Broker
	topic  string
}

// bind hands the greeter the broker it publishes through. OnConnect runs on the
// connection's goroutine, so the pointer is read under the same lock that sets it.
func (g *greeter) bind(b *gmqx.Broker) {
	g.mu.Lock()
	g.broker = b
	g.mu.Unlock()
}

func (g *greeter) OnConnect(ctx context.Context, info hook.ConnectInfo) {
	g.mu.Lock()
	b := g.broker
	g.mu.Unlock()
	if b == nil {
		return
	}
	err := b.PublishMessage(ctx, gmqx.Publication{
		Topic:   g.topic,
		Payload: []byte("hi!"),
		Retain:  true,
	})
	if err != nil {
		// A greeting is not worth failing a connection over.
		fmt.Printf("broker-demo: greeting for %q: %v\n", info.ClientID, err)
	}
}

// The rest of the Sink interface: the demo only cares about connects, and a nil
// implementation of every other callback is the honest way to say so.
func (*greeter) OnDisconnect(context.Context, string, error)     {}
func (*greeter) OnConnectionLost(context.Context, string, error) {}
func (*greeter) OnPing(context.Context, string)                  {}
func (*greeter) OnPublish(context.Context, hook.PublishEvent) hook.PublishResult {
	return hook.PublishResult{Disposition: hook.PublishAccepted}
}

// collect is the handler every subscription of the demo uses: it copies the message
// (the payload is only valid during the callback) and hands it over.
func collect(ch chan<- *client.Message) client.MessageHandler {
	return func(m *client.Message) {
		copied := *m
		copied.Payload = append([]byte(nil), m.Payload...)
		select {
		case ch <- &copied:
		default:
		}
	}
}

// await waits for one delivery, or for the demo's context to end.
func await(ch <-chan *client.Message, ctx context.Context) (*client.Message, error) {
	select {
	case m := <-ch:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
