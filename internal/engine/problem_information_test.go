package engine

// MQTT 5's Request Problem Information (0x17) says: when a client sets it to 0, the
// server must not send a Reason String or a User Property on any packet except
// PUBLISH, CONNACK and DISCONNECT — and this broker sends none of them anywhere.
//
// The broker does not parse the property today, and does not need to: nothing in the
// code builds a Reason String or a CONNACK/DISCONNECT user-property list (grep
// ReasonString under conn/). That makes the requirement vacuously satisfied, and
// this test is what keeps it that way: the day someone adds a reason string for
// diagnosability, this case fails and whoever added it has to read 0x17 first.

import (
	"testing"

	"github.com/maxid/gnety/codec/mqtt"
)

func TestBrokerSendsNoReasonStrings(t *testing.T) {
	b := startBroker(t, Config{})
	c, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "no-problem-info", clean: true})

	if ack.Properties.ReasonString != "" {
		t.Fatalf("CONNACK carries a Reason String %q", ack.Properties.ReasonString)
	}
	if len(ack.Properties.UserProperties) != 0 {
		t.Fatalf("CONNACK carries user properties %+v", ack.Properties.UserProperties)
	}

	suback := c.subscribe(1, mqtt.Properties{}, mqtt.Subscription{Filter: "elsewhere/#", QoS: 1})
	if suback.Properties.ReasonString != "" {
		t.Fatalf("SUBACK carries a Reason String %q", suback.Properties.ReasonString)
	}
	if len(suback.Properties.UserProperties) != 0 {
		t.Fatalf("SUBACK carries user properties %+v", suback.Properties.UserProperties)
	}

	// PUBLISH itself is the one packet 0x17 lets a server annotate freely, so it is
	// not asserted on; every acknowledgement is.
	puback := publishAndReadPuback(t, c, "pi/one", 1)
	if puback.Properties.ReasonString != "" {
		t.Fatalf("PUBACK carries a Reason String %q", puback.Properties.ReasonString)
	}
	if len(puback.Properties.UserProperties) != 0 {
		t.Fatalf("PUBACK carries user properties %+v", puback.Properties.UserProperties)
	}
	c.disconnect()
}

// publishAndReadPuback sends one QoS 1 publication and returns the acknowledgement,
// so a case can inspect its properties (mustPublish consumes the PUBACK itself).
func publishAndReadPuback(t *testing.T, c *confClient, topic string, pid uint16) *mqtt.Puback {
	t.Helper()
	c.publish(confPublish{topic: topic, payload: "x", qos: 1, pid: pid})
	msg, err := c.read()
	if err != nil {
		t.Fatalf("read PUBACK: %v", err)
	}
	puback, ok := msg.(*mqtt.Puback)
	if !ok {
		t.Fatalf("read %T, want *mqtt.Puback", msg)
	}
	return puback
}
