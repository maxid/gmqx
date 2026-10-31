// Command client-demo connects, subscribes, publishes and prints the delivery
// using the gmqx MQTT client.
//
// By default it starts an in-process gmqx broker on an ephemeral loopback port and
// runs the round trip against it, so go run needs no external server. With -addr
// it uses an existing broker instead.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gmqx/client"
)

func main() {
	addr := flag.String("addr", "", "existing broker address; empty starts an in-process broker")
	topic := flag.String("topic", "demo/hello", "topic to subscribe to and publish on")
	message := flag.String("message", "hello from the gmqx MQTT client", "message body to publish")
	qos := flag.Int("qos", 1, "QoS of the subscription and the publish (0, 1 or 2)")
	version := flag.Int("version", 4, "MQTT protocol level: 4 (3.1.1) or 5")
	timeout := flag.Duration("timeout", 10*time.Second, "overall deadline of the demo")
	flag.Parse()

	if *qos < 0 || *qos > 2 {
		log.Fatalf("gmqx-client-demo: -qos %d is not 0, 1 or 2", *qos)
	}
	if *version != 4 && *version != 5 {
		log.Fatalf("gmqx-client-demo: -version %d is not 4 or 5", *version)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	brokerAddr := *addr
	if brokerAddr == "" {
		// Keep the broker's own log quiet; the demo prints its progress.
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
		broker, err := gmqx.New(gmqx.Config{Addr: "127.0.0.1:0", Logger: logger})
		if err != nil {
			log.Fatalf("gmqx-client-demo: create broker: %v", err)
		}
		if err := broker.Start(ctx); err != nil {
			log.Fatalf("gmqx-client-demo: start broker: %v", err)
		}
		defer func() { _ = broker.Stop(context.Background()) }()
		brokerAddr = broker.Addr()
		fmt.Printf("started in-process broker on %s\n", brokerAddr)
	}

	delivered := make(chan *client.Message, 1)
	c, err := client.Dial(ctx, client.Config{
		Addr:       brokerAddr,
		ClientID:   "gmqx-client-demo",
		Version:    byte(*version),
		KeepAlive:  30 * time.Second,
		CleanStart: true,
		OnMessage: func(m *client.Message) {
			// The payload is only valid during the callback, so the demo copies
			// what it keeps (the whole message is small).
			copyMessage := *m
			copyMessage.Payload = append([]byte(nil), m.Payload...)
			select {
			case delivered <- &copyMessage:
			default:
			}
		},
	})
	if err != nil {
		log.Fatalf("gmqx-client-demo: dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	fmt.Printf("connected to %s as %q (MQTT level %d)\n", brokerAddr, c.ClientID(), *version)

	codes, err := c.Subscribe(ctx, client.Subscription{Filter: *topic, QoS: byte(*qos)})
	if err != nil {
		log.Fatalf("gmqx-client-demo: subscribe: %v", err)
	}
	fmt.Printf("subscribed to %s (granted %v)\n", *topic, codes)

	if err := c.PublishMessage(ctx, client.PublishOptions{
		Topic:   *topic,
		Payload: []byte(*message),
		QoS:     byte(*qos),
	}); err != nil {
		log.Fatalf("gmqx-client-demo: publish: %v", err)
	}
	fmt.Printf("published %q to %s at QoS %d\n", *message, *topic, *qos)

	select {
	case m := <-delivered:
		fmt.Printf("received %q on %s (QoS %d, retain %v)\n", m.Payload, m.Topic, m.QoS, m.Retain)
	case <-ctx.Done():
		log.Fatalf("gmqx-client-demo: no delivery within %s: %v", *timeout, ctx.Err())
	}

	if err := c.Close(); err != nil {
		log.Fatalf("gmqx-client-demo: close: %v", err)
	}
	fmt.Println("closed")
}
