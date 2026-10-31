// Command broker-demo starts a gmqx broker and completes one publish/subscribe
// round trip against it.
//
// The demo acts as a raw MQTT 3.1.1 client through gnety/codec/mqtt, the wire
// codec the broker itself uses: CONNECT, SUBSCRIBE, PUBLISH and DISCONNECT are
// encoded and sent, then CONNACK, SUBACK and the delivered PUBLISH are decoded
// and printed. It deliberately uses the wire layer rather than
// github.com/maxid/gmqx/client, which examples/client-demo shows. Broker and
// client share one process, so go run shows the round trip without a third-party
// MQTT client.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/maxid/gmqx"
	"github.com/maxid/gnety/codec/mqtt"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "broker TCP listen address; port 0 picks an ephemeral port")
	topic := flag.String("topic", "demo/hello", "topic the demo subscribes to and publishes to")
	message := flag.String("message", "hello from the gmqx example", "message body to publish")
	timeout := flag.Duration("timeout", 5*time.Second, "overall deadline for the demo")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Keep the broker's startup log quiet; the demo prints its own progress.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	broker, err := gmqx.New(gmqx.Config{Addr: *addr, Logger: logger})
	if err != nil {
		log.Fatalf("gmqx: create broker: %v", err)
	}
	if err := broker.Start(ctx); err != nil {
		log.Fatalf("gmqx: start broker: %v", err)
	}
	defer func() { _ = broker.Stop(context.Background()) }()
	fmt.Printf("broker listening on %s\n", broker.Addr())

	conn, err := net.DialTimeout("tcp", broker.Addr(), *timeout)
	if err != nil {
		log.Fatalf("gmqx: dial broker: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(*timeout))
	decoder := mqtt.Decoder{Version: mqtt.Version311}
	reader := bufio.NewReader(conn)
	encode := func(m mqtt.Message) {
		if err := mqtt.Encode(conn, m); err != nil {
			log.Fatalf("gmqx: encode %T: %v", m, err)
		}
	}
	decode := func() mqtt.Message {
		msg, err := decoder.Decode(reader)
		if err != nil {
			log.Fatalf("gmqx: decode response: %v", err)
		}
		return msg
	}

	// CONNECT: clean session, 30s keepalive, explicit client id.
	encode(&mqtt.Connect{ClientID: []byte("gmqx-example"), CleanSession: true, KeepAlive: 30})
	msg := decode()
	if ack, ok := msg.(*mqtt.Connack); !ok || ack.ReasonCode != 0 {
		log.Fatalf("gmqx: broker refused CONNECT: %T %v", msg, msg)
	}
	fmt.Println("client connected")

	// SUBSCRIBE: packet id 1, one QoS 0 filter.
	encode(&mqtt.Subscribe{PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: *topic}}})
	msg = decode()
	if ack, ok := msg.(*mqtt.Suback); !ok || len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] == 0x80 {
		log.Fatalf("gmqx: broker refused SUBSCRIBE: %T %v", msg, msg)
	}
	fmt.Printf("client subscribed to %q\n", *topic)

	// PUBLISH (QoS 0): the broker delivers it back on the same connection.
	encode(&mqtt.Publish{Topic: *topic, Payload: []byte(*message)})
	msg = decode()
	delivered, ok := msg.(*mqtt.Publish)
	if !ok {
		log.Fatalf("gmqx: expected PUBLISH, got %T", msg)
	}
	fmt.Printf("received on %q: %s\n", delivered.Topic, delivered.Payload)

	// DISCONNECT closes the session without a will message.
	encode(&mqtt.Disconnect{})
	fmt.Println("round trip complete")
}
