// Command client-demo runs one publish/subscribe round trip through the gmqx
// MQTT client.
//
// By default it starts an in-process gmqx broker on an ephemeral loopback port and
// runs the round trip against it, so go run needs no external server. With -addr
// it uses an existing broker instead.
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
	addr := flag.String("addr", "", "existing broker address; empty starts an in-process broker")
	topic := flag.String("topic", "demo/hello", "topic to subscribe to and publish on")
	message := flag.String("message", "hello from the gmqx MQTT client", "message body to publish")
	qos := flag.Int("qos", 1, "QoS of the subscription and the publish (0, 1 or 2)")
	version := flag.Int("version", 4, "MQTT protocol level: 4 (3.1.1) or 5")
	timeout := flag.Duration("timeout", 10*time.Second, "overall deadline of the demo")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `client-demo subscribes, publishes and prints the delivery using
github.com/maxid/gmqx/client.

Examples:
  go run ./examples/client-demo
  go run ./examples/client-demo -topic demo/hello -message "hello" -qos 2 -version 5
  go run ./examples/client-demo -addr 127.0.0.1:1883 -topic demo/hello

Flags:
`)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *qos < 0 || *qos > 2 {
		log.Fatalf("client-demo: -qos %d is not 0, 1 or 2", *qos)
	}
	if *version != 4 && *version != 5 {
		log.Fatalf("client-demo: -version %d is not 4 or 5", *version)
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(sigCtx, *timeout)
	defer cancel()

	brokerAddr := *addr
	if brokerAddr == "" {
		// Keep the broker's own log quiet; the demo prints its progress.
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
		broker, err := gmqx.New(gmqx.Config{Addr: "127.0.0.1:0", Logger: logger})
		if err != nil {
			log.Fatalf("client-demo: create broker: %v", err)
		}
		if err := broker.Start(ctx); err != nil {
			log.Fatalf("client-demo: start broker: %v", err)
		}
		// Stop takes the cancelled ctx off the table, so Ctrl-C still closes it.
		defer func() { _ = broker.Stop(context.Background()) }()
		brokerAddr = broker.Addr()
		fmt.Printf("[1/%d] in-process broker listening on %s\n", steps, brokerAddr)
	} else {
		fmt.Printf("[1/%d] using the existing broker on %s\n", steps, brokerAddr)
	}

	delivered := make(chan *client.Message, 1)
	c, err := client.Dial(ctx, client.Config{
		Addr:       brokerAddr,
		ClientID:   "client-demo",
		Version:    byte(*version),
		KeepAlive:  30 * time.Second,
		CleanStart: true,
		OnMessage: func(m *client.Message) {
			// The payload is only valid during the callback, so the demo copies
			// what it keeps (the whole message is small).
			copied := *m
			copied.Payload = append([]byte(nil), m.Payload...)
			select {
			case delivered <- &copied:
			default:
			}
		},
	})
	if err != nil {
		log.Fatalf("client-demo: dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	fmt.Printf("[2/%d] connected as %q (MQTT level %d)\n", steps, c.ClientID(), *version)

	codes, err := c.Subscribe(ctx, client.Subscription{Filter: *topic, QoS: byte(*qos)})
	if err != nil {
		log.Fatalf("client-demo: subscribe: %v", err)
	}
	fmt.Printf("[3/%d] subscribed to %s (granted %v)\n", steps, *topic, codes)

	if err := c.PublishMessage(ctx, client.PublishOptions{
		Topic:   *topic,
		Payload: []byte(*message),
		QoS:     byte(*qos),
	}); err != nil {
		log.Fatalf("client-demo: publish: %v", err)
	}
	fmt.Printf("[4/%d] published %q to %s (QoS %d)\n", steps, *message, *topic, *qos)

	select {
	case m := <-delivered:
		fmt.Printf("[5/%d] received %q on %s (QoS %d, retain %v)\n",
			steps, m.Payload, m.Topic, m.QoS, m.Retain)
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			fmt.Println("[stop] interrupted")
			return
		}
		log.Fatalf("client-demo: wait for the delivery: %v", ctx.Err())
	}

	if err := c.Close(); err != nil {
		log.Fatalf("client-demo: close: %v", err)
	}
	fmt.Println("[done] publish/subscribe round trip complete; client closed")
}
