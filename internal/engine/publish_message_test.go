package engine

// Broker.PublishMessage is the host's own publication — the seam an embedding
// program uses to say something the broker did not receive from a client (a
// greeting on connect, an alert, a command from another subsystem). It takes the
// ordinary accepted path: retained state is stored before routing and the
// Dispatcher decides where the live copy goes.

import (
	"context"
	"errors"
	"testing"

	"github.com/maxid/gnety/codec/mqtt"
)

func TestPublishMessageReachesSubscribers(t *testing.T) {
	b := startBroker(t, Config{})
	ctx := context.Background()

	sub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "host-sub", clean: true})
	sub.subscribe(1, mqtt.Properties{}, mqtt.Subscription{Filter: "host/#", QoS: 1})

	if err := b.PublishMessage(ctx, Publication{Topic: "host/reply", Payload: []byte("hi!"), QoS: 1}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	got := expectPublish(t, sub)
	if got.Topic != "host/reply" || string(got.Payload) != "hi!" {
		t.Fatalf("delivered %q %q", got.Topic, got.Payload)
	}
	sub.ack(got.PacketID)
}

// A retained publication from the host is stored like a client's: a subscriber that
// connects later still gets it.
func TestPublishMessageRetainedIsReplayed(t *testing.T) {
	b := startBroker(t, Config{})
	ctx := context.Background()

	if err := b.PublishMessage(ctx, Publication{Topic: "host/state", Payload: []byte("on"), Retain: true}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	late, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "host-late", clean: true})
	late.subscribe(1, mqtt.Properties{}, mqtt.Subscription{Filter: "host/#", QoS: 0})
	got := expectPublish(t, late)
	if got.Topic != "host/state" || string(got.Payload) != "on" || !got.Retain {
		t.Fatalf("replay = %q %q retain=%v", got.Topic, got.Payload, got.Retain)
	}
}

func TestPublishMessageRejectsBadTopics(t *testing.T) {
	b := startBroker(t, Config{})
	ctx := context.Background()

	for _, topic := range []string{"", "host/+", "host/#"} {
		if err := b.PublishMessage(ctx, Publication{Topic: topic}); !errors.Is(err, ErrBadTopic) {
			t.Fatalf("PublishMessage(%q) = %v, want ErrBadTopic", topic, err)
		}
	}
}
