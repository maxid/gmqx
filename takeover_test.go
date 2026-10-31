package gmqx

// Client-id takeover: a CONNECT with a client id that is already connected
// displaces the live connection (ConnectModeKick), tells it why on MQTT 5 (0x8E
// Session taken over), and does the same across nodes — in both connect modes,
// because a client that reconnects to another node otherwise leaves a connection
// behind that duplicates every delivery until its keep-alive expires. The
// Config.NotKickSeconds window throttles the displacement so two ends of one
// client id cannot displace each other in a loop.

import (
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// newClusterBrokerPair starts two brokers of cfg on a connected TCP mesh.
func newClusterBrokerPair(t *testing.T, cfg Config) (*Broker, *Broker) {
	t.Helper()
	aAddr, bAddr := freeAddr(t), freeAddr(t)
	ca := NewTCPCluster(TCPClusterConfig{
		Addr: aAddr, Seeds: []string{bAddr}, ReconnectInterval: 20 * time.Millisecond, Logger: discardLogger(),
	})
	cb := NewTCPCluster(TCPClusterConfig{
		Addr: bAddr, Seeds: []string{aAddr}, ReconnectInterval: 20 * time.Millisecond, Logger: discardLogger(),
	})
	cfgA, cfgB := cfg, cfg
	cfgA.NodeID, cfgA.ClusterBackend = "a", ca
	cfgB.NodeID, cfgB.ClusterBackend = "b", cb
	a := startBroker(t, cfgA)
	b := startBroker(t, cfgB)
	waitPeers(t, ca, 1)
	waitPeers(t, cb, 1)
	return a, b
}

// TestTakeoverDisplacesLocalClient pins the local case: the second CONNECT wins,
// and the first connection is told it was taken over before its socket closes.
func TestTakeoverDisplacesLocalClient(t *testing.T) {
	// NotKickSeconds -1 disables the anti-flap window: this case is about the
	// displacement itself, not about its rate.
	b := startBroker(t, Config{ConnectMode: ConnectModeKick, NotKickSeconds: -1})

	first, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "takeover", clean: true})
	second, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "takeover", clean: true})

	expectDisconnect(t, first, ReasonSessionTakenOver)
	// The new connection owns the session and is fully usable.
	second.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	readConf[*mqtt.Pingresp](t, second)
}

// TestTakeoverOfMQTT311ClientClosesSilently pins the protocol difference: MQTT
// 3.1.1 has no server DISCONNECT, so a displaced client only sees its connection
// end.
func TestTakeoverOfMQTT311ClientClosesSilently(t *testing.T) {
	b := startBroker(t, Config{ConnectMode: ConnectModeKick, NotKickSeconds: -1})
	first, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version311, clientID: "takeover-311", clean: true})
	second, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version311, clientID: "takeover-311", clean: true})
	expectClosed(t, first)
	second.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}})
	readConf[*mqtt.Pingresp](t, second)
}

// TestTakeoverAntiFlapWindow pins Config.NotKickSeconds: a CONNECT that would
// displace a connection younger than the window is refused with 0x9F (Connection
// rate exceeded) on MQTT 5 and return code 3 (Server unavailable) on MQTT 3.1.1,
// and the incumbent keeps its session. Outside the window the takeover proceeds.
func TestTakeoverAntiFlapWindow(t *testing.T) {
	// One protocol second is 40 ms, so NotKickSeconds 1 is a 40 ms window.
	const window = 40 * time.Millisecond
	metrics := newDeliveryMetrics()
	b := startBroker(t, Config{
		ConnectMode:    ConnectModeKick,
		NotKickSeconds: 1,
		unit:           window,
		Metrics:        metrics,
	})

	incumbent, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "flap", clean: true})

	// Inside the window: refused, and the incumbent is untouched.
	early, ack := dialConf(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "flap", clean: true})
	if ack.ReasonCode != ReasonConnectionRateExceeded {
		t.Fatalf("CONNACK reason code %#x inside the window, want 0x9F (Connection rate exceeded)", ack.ReasonCode)
	}
	expectClosed(t, early)
	incumbent.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	readConf[*mqtt.Pingresp](t, incumbent)
	if got := metrics.count("takeover_rate_limit"); got == 0 {
		t.Fatal("the refused takeover was not counted")
	}

	// MQTT 3.1.1 gets the closest return code it has: 3 (Server unavailable).
	_, ack311 := dialConf(t, b.Addr(), confConnect{version: mqtt.Version311, clientID: "flap", clean: true})
	if ack311.ReasonCode != 3 {
		t.Fatalf("CONNACK return code %d inside the window, want 3 (Server unavailable)", ack311.ReasonCode)
	}

	// Outside the window the takeover proceeds, and the connection it installs is
	// itself protected by a fresh window.
	time.Sleep(2 * window)
	second, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "flap", clean: true})
	expectDisconnect(t, incumbent, ReasonSessionTakenOver)
	_, ack = dialConf(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "flap", clean: true})
	if ack.ReasonCode != ReasonConnectionRateExceeded {
		t.Fatalf("second CONNACK reason code %#x inside the new window, want 0x9F", ack.ReasonCode)
	}
	second.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	readConf[*mqtt.Pingresp](t, second)
}

// TestClusterTakeoverClosesZombieInUniqueMode pins the cross-node case the
// connect-time kick exists for: node a holds the connection, the same client id
// reconnects to node b, and b's CONNECT closes a's copy even though neither node
// is in ConnectModeKick. Without it the old connection would keep receiving
// deliveries until its keep-alive expired.
func TestClusterTakeoverClosesZombieInUniqueMode(t *testing.T) {
	// UNIQUE is the default connect mode. NotKickSeconds -1 disables the anti-flap
	// window on both nodes: the two connections of this case are milliseconds
	// apart, and the receive side applies the window too, so this case is about the
	// displacement itself.
	a, b := newClusterBrokerPair(t, Config{NotKickSeconds: -1})

	zombie, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "roam", clean: true})
	// This CONNECT spread a kick for "roam" too. Let the peer apply it before the
	// second connection exists: a kick still in flight when the newer connection
	// is registered would be refused as stale (it is older than that connection),
	// which is the ordering rule this test is not about.
	time.Sleep(200 * time.Millisecond)
	// The same client id on another node: node b has no local record for it, so
	// UNIQUE does not apply and b accepts it. Its CONNECT is spread, and a's
	// zombie is displaced.
	live, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "roam", clean: true})
	expectDisconnect(t, zombie, ReasonSessionTakenOver)
	waitDisconnected(t, a, "roam")

	// The live connection keeps working, and a publish to a subscription it holds
	// on b reaches it exactly once.
	subscribe(t, live, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "roam/#", QoS: 0})
	pub, _ := mustConnect(t, a.Addr(), confConnect{version: mqtt.Version5, clientID: "roam-pub", clean: true})
	pub.publish(confPublish{topic: "roam/x", payload: "once", qos: 0})
	if got := expectPublish(t, live); string(got.Payload) != "once" {
		t.Fatalf("delivery on the new node = %q, want once", got.Payload)
	}
	expectNoMessage(t, live, 150*time.Millisecond)
}
