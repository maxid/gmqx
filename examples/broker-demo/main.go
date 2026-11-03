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
	"syscall"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
)

const steps = 5

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "broker TCP listen address; port 0 picks an ephemeral port")
	topic := flag.String("topic", "demo/retained", "topic the retained message is published on and replayed from")
	message := flag.String("message", "hello from the gmqx broker demo", "payload published with the retain flag")
	qos := flag.Int("qos", 1, "QoS of the publish and the subscription (0, 1 or 2)")
	version := flag.Int("version", 5, "MQTT protocol level of both clients: 4 (3.1.1) or 5")
	timeout := flag.Duration("timeout", 10*time.Second, "overall deadline of the demo")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `broker-demo embeds a gmqx broker in this process, publishes one
retained message with the gmqx client, then subscribes from a second client to
show the replay a later subscriber gets.

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

	// Keep the broker's own log quiet; the demo prints its progress.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	broker, err := gmqx.New(gmqx.Config{Addr: *addr, Logger: logger})
	if err != nil {
		log.Fatalf("broker-demo: create broker: %v", err)
	}
	if err := broker.Start(ctx); err != nil {
		log.Fatalf("broker-demo: start broker: %v", err)
	}
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
	replayed := make(chan *client.Message, 1)
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

	codes, err := sub.Subscribe(ctx, client.Subscription{
		Filter: *topic,
		QoS:    byte(*qos),
		// The handler travels with the subscription (see examples/client-demo); the
		// retained replay the broker sends after the SUBACK arrives here.
		Handler: func(m *client.Message) {
			// The payload is only valid during the callback, so the demo copies
			// what it keeps (the whole message is small).
			copied := *m
			copied.Payload = append([]byte(nil), m.Payload...)
			select {
			case replayed <- &copied:
			default:
			}
		},
	})
	if err != nil {
		log.Fatalf("broker-demo: subscribe: %v", err)
	}
	fmt.Printf("[4/%d] subscriber %q subscribed to %s (granted %v)\n", steps, sub.ClientID(), *topic, codes)

	select {
	case m := <-replayed:
		fmt.Printf("[5/%d] replayed %s to a subscriber that connected later (QoS %d, retain %v): %s\n",
			steps, m.Topic, m.QoS, m.Retain, m.Payload)
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			fmt.Println("[stop] interrupted")
			return
		}
		log.Fatalf("broker-demo: wait for the retained replay: %v", ctx.Err())
	}

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
