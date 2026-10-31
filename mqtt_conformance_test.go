package gmqx

// Protocol-conformance suite: MQTT 3.1.1 and MQTT 5 feature rows driven over the
// wire against a real in-process broker. The packets are built and parsed with
// gnety/codec/mqtt (the same typed layer the broker uses), so a case asserts on
// the protocol, not on gmqx internals. The raw byte helpers are only used where
// the point of the case is a frame the codec refuses to build.
//
// Every row of the matrix is one subtest. A row that exposes a documented,
// deliberate limitation carries a t.Skip whose message quotes the limitation.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

func TestConformance(t *testing.T) {
	t.Run("MQTT311", testConformance311)
	t.Run("MQTT5", testConformance5)
}

// ---------------------------------------------------------------------------
// Wire client

// confClient is a minimal MQTT client: one TCP connection, typed packets,
// bounded read deadlines. It is deliberately not a client library — the suite
// needs to send malformed frames and observe the exact answer.
type confClient struct {
	t        *testing.T
	conn     net.Conn
	r        *bufio.Reader
	version  byte
	clientID string
	timeout  time.Duration
}

// confConnect describes a CONNECT the suite wants to send.
type confConnect struct {
	version   byte
	clientID  string
	clean     bool
	keepAlive uint16
	username  string
	password  []byte
	props     mqtt.Properties
	will      *mqtt.Will
}

// confPublish describes a PUBLISH the suite wants to send.
type confPublish struct {
	topic   string
	payload string
	qos     byte
	pid     uint16
	retain  bool
	dup     bool
	props   mqtt.Properties
}

func newConfClient(t *testing.T, conn net.Conn, version byte, clientID string) *confClient {
	t.Helper()
	if version == 0 {
		version = mqtt.Version311
	}
	c := &confClient{
		t:        t,
		conn:     conn,
		r:        bufio.NewReader(conn),
		version:  version,
		clientID: clientID,
		timeout:  2 * time.Second,
	}
	t.Cleanup(func() { _ = conn.Close() })
	return c
}

// dialConf dials, sends CONNECT and reads the CONNACK. A transport or decode
// error is fatal; a non-zero CONNACK reason code is not, because rejection is
// part of the matrix.
func dialConf(t *testing.T, addr string, cc confConnect) (*confClient, *mqtt.Connack) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	c := newConfClient(t, conn, cc.version, cc.clientID)
	c.sendConnect(cc)
	return c, readConf[*mqtt.Connack](t, c)
}

func mustConnect(t *testing.T, addr string, cc confConnect) (*confClient, *mqtt.Connack) {
	t.Helper()
	c, ack := dialConf(t, addr, cc)
	if ack.ReasonCode != 0 {
		t.Fatalf("CONNACK reason code %#x, want success", ack.ReasonCode)
	}
	return c, ack
}

func (c *confClient) sendConnect(cc confConnect) {
	c.t.Helper()
	connect := &mqtt.Connect{
		MessageHeader: mqtt.MessageHeader{Level: c.version},
		ClientID:      []byte(cc.clientID),
		KeepAlive:     cc.keepAlive,
		CleanSession:  cc.clean,
		Properties:    cc.props,
		Will:          cc.will,
	}
	if cc.username != "" || cc.password != nil {
		connect.Username = []byte(cc.username)
		connect.Password = cc.password
	}
	c.write(connect)
}

func (c *confClient) write(m mqtt.Message) {
	c.t.Helper()
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	if err := mqtt.Encode(c.conn, m); err != nil {
		c.t.Fatalf("write %T: %v", m, err)
	}
}

func (c *confClient) writeRaw(frame []byte) {
	c.t.Helper()
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	if _, err := c.conn.Write(frame); err != nil {
		c.t.Fatalf("write raw frame: %v", err)
	}
}

func (c *confClient) readFor(deadline time.Time) (mqtt.Message, error) {
	_ = c.conn.SetReadDeadline(deadline)
	d := mqtt.Decoder{Version: c.version}
	return d.Decode(c.r)
}

func (c *confClient) read() (mqtt.Message, error) {
	return c.readFor(time.Now().Add(c.timeout))
}

func readConf[T mqtt.Message](t *testing.T, c *confClient) T {
	t.Helper()
	msg, err := c.read()
	if err != nil {
		t.Fatalf("read packet: %v", err)
	}
	typed, ok := msg.(T)
	if !ok {
		var zero T
		t.Fatalf("read %T, want %T", msg, zero)
	}
	return typed
}

func (c *confClient) subscribe(pid uint16, props mqtt.Properties, filters ...mqtt.Subscription) *mqtt.Suback {
	c.t.Helper()
	c.write(&mqtt.Subscribe{
		MessageHeader: mqtt.MessageHeader{Level: c.version},
		PacketID:      pid,
		Properties:    props,
		Subscriptions: filters,
	})
	return readConf[*mqtt.Suback](c.t, c)
}

// subscribe is the free-function form of confClient.subscribe, so a case reads
// as one line.
func subscribe(t *testing.T, c *confClient, pid uint16, props mqtt.Properties, filters ...mqtt.Subscription) *mqtt.Suback {
	t.Helper()
	return c.subscribe(pid, props, filters...)
}

func (c *confClient) unsubscribe(pid uint16, filters ...string) *mqtt.Unsuback {
	c.t.Helper()
	c.write(&mqtt.Unsubscribe{
		MessageHeader: mqtt.MessageHeader{Level: c.version},
		PacketID:      pid,
		Filters:       filters,
	})
	return readConf[*mqtt.Unsuback](c.t, c)
}

func (c *confClient) publish(p confPublish) {
	c.t.Helper()
	c.write(&mqtt.Publish{
		MessageHeader: mqtt.MessageHeader{Level: c.version},
		Topic:         p.topic,
		Payload:       []byte(p.payload),
		QoS:           p.qos,
		PacketID:      p.pid,
		Retain:        p.retain,
		Dup:           p.dup,
		Properties:    p.props,
	})
}

func (c *confClient) disconnect() {
	c.t.Helper()
	c.write(&mqtt.Disconnect{MessageHeader: mqtt.MessageHeader{Level: c.version}})
}

func expectPublish(t *testing.T, c *confClient) *mqtt.Publish {
	t.Helper()
	return readConf[*mqtt.Publish](t, c)
}

func expectPuback(t *testing.T, c *confClient, pid uint16, reason byte) *mqtt.Puback {
	t.Helper()
	ack := readConf[*mqtt.Puback](t, c)
	if ack.PacketID != pid {
		t.Fatalf("PUBACK packet id %d, want %d", ack.PacketID, pid)
	}
	if ack.ReasonCode != reason {
		t.Fatalf("PUBACK reason %#x, want %#x", ack.ReasonCode, reason)
	}
	return ack
}

func expectPubrec(t *testing.T, c *confClient, pid uint16, reason byte) *mqtt.Pubrec {
	t.Helper()
	rec := readConf[*mqtt.Pubrec](t, c)
	if rec.PacketID != pid {
		t.Fatalf("PUBREC packet id %d, want %d", rec.PacketID, pid)
	}
	if rec.ReasonCode != reason {
		t.Fatalf("PUBREC reason %#x, want %#x", rec.ReasonCode, reason)
	}
	return rec
}

func expectPubcomp(t *testing.T, c *confClient, pid uint16) *mqtt.Pubcomp {
	t.Helper()
	comp := readConf[*mqtt.Pubcomp](t, c)
	if comp.PacketID != pid {
		t.Fatalf("PUBCOMP packet id %d, want %d", comp.PacketID, pid)
	}
	return comp
}

// mustPublish sends a QoS 1 PUBLISH and waits for its successful PUBACK.
func (c *confClient) mustPublish(t *testing.T, p confPublish) {
	t.Helper()
	if p.qos != 1 {
		t.Fatalf("mustPublish needs QoS 1, got %d", p.qos)
	}
	c.publish(p)
	expectPuback(t, c, p.pid, 0)
}

// collectPublishes reads PUBLISH packets until wait elapses without one. It lets
// a case about overlapping filters accept the one-or-more copies MQTT permits.
func collectPublishes(t *testing.T, c *confClient, wait time.Duration) []*mqtt.Publish {
	t.Helper()
	deadline := time.Now().Add(wait)
	var out []*mqtt.Publish
	for {
		msg, err := c.readFor(deadline)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return out
			}
			t.Fatalf("read while collecting PUBLISH: %v", err)
		}
		p, ok := msg.(*mqtt.Publish)
		if !ok {
			t.Fatalf("read %T while collecting PUBLISH", msg)
		}
		out = append(out, p)
		if time.Now().After(deadline) {
			return out
		}
	}
}

// expectNoMessage asserts that nothing arrives within d and that the connection
// is still usable (a close would be a different failure).
func expectNoMessage(t *testing.T, c *confClient, d time.Duration) {
	t.Helper()
	msg, err := c.readFor(time.Now().Add(d))
	if err == nil {
		t.Fatalf("expected silence for %v, got %T", d, msg)
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("connection failed while expecting silence: %v", err)
	}
}

// expectClosed asserts that the broker closes the connection and sends nothing
// first.
func expectClosed(t *testing.T, c *confClient) {
	t.Helper()
	b, err := c.r.ReadByte()
	if err == nil {
		t.Fatalf("expected connection close, read byte %#x", b)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("connection still open after %v", c.timeout)
	}
}

// expectDisconnect reads the server DISCONNECT, checks its reason code and then
// requires the connection to end.
func expectDisconnect(t *testing.T, c *confClient, reason byte) *mqtt.Disconnect {
	t.Helper()
	d := readConf[*mqtt.Disconnect](t, c)
	if d.ReasonCode != reason {
		t.Fatalf("DISCONNECT reason code %#x, want %#x", d.ReasonCode, reason)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	b, err := c.r.ReadByte()
	if err == nil {
		t.Fatalf("expected close after DISCONNECT, read byte %#x", b)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		t.Fatal("the connection stayed open after a server DISCONNECT")
	}
	return d
}

// waitFor polls a condition. The broker tears a session down on its serve
// goroutine, so a reconnect after a close must wait for that goroutine instead
// of assuming it has already run.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waitDisconnected(t *testing.T, b *Broker, clientID string) {
	t.Helper()
	waitFor(t, "client "+clientID+" to unregister", func() bool {
		b.mu.RLock()
		defer b.mu.RUnlock()
		_, connected := b.clients[clientID]
		return !connected
	})
}

// ---------------------------------------------------------------------------
// SPIs used by the cases

// confAuth adapts a function to AuthManager.
type confAuth func(ConnectInfo) error

func (f confAuth) Authenticate(_ context.Context, info ConnectInfo) error { return f(info) }
func (confAuth) RequiresOffload() bool                                    { return false }

// confACL adapts a function to AclManager.
type confACL func(SessionInfo, string, AclAction) bool

func (f confACL) Check(_ context.Context, s SessionInfo, topic string, action AclAction) bool {
	return f(s, topic, action)
}
func (confACL) RequiresOffload() bool { return false }

// confSink is an EventSink whose publish verdict is a function.
type confSink struct {
	publish func(PublishEvent) PublishResult
}

func (*confSink) OnConnect(context.Context, ConnectInfo)          {}
func (*confSink) OnDisconnect(context.Context, string, error)     {}
func (*confSink) OnConnectionLost(context.Context, string, error) {}
func (*confSink) OnPing(context.Context, string)                  {}

func (s *confSink) OnPublish(_ context.Context, ev PublishEvent) PublishResult {
	if s.publish == nil {
		return PublishResult{Disposition: PublishAccepted}
	}
	return s.publish(ev)
}

// ---------------------------------------------------------------------------
// MQTT 3.1.1 matrix

func testConformance311(t *testing.T) {
	t.Run("connect_clean_session", test311ConnectCleanSession)
	t.Run("connect_persistent_session", test311ConnectPersistentSession)
	t.Run("connect_client_id_rules", test311ClientIDRules)
	t.Run("connect_will_qos_retain", test311WillQoSRetain)
	t.Run("connect_will_topic_validation", test311WillTopicValidation)
	t.Run("connect_username_password", test311UsernamePassword)
	t.Run("subscribe_wildcards_and_overlap", test311SubscribeWildcards)
	t.Run("subscribe_suback_codes", test311SubackCodes)
	t.Run("subscribe_invalid_filter", test311SubscribeInvalidFilter)
	t.Run("unsubscribe", test311Unsubscribe)
	t.Run("publish_qos0", test311PublishQoS0)
	t.Run("publish_qos1", test311PublishQoS1)
	t.Run("publish_qos2", test311PublishQoS2)
	t.Run("retained_messages", test311Retained)
	t.Run("will_on_disconnect", test311WillOnDisconnect)
	t.Run("dup_retain_flags", test311DupRetainFlags)
	t.Run("keepalive_zero", test311KeepaliveZero)
	t.Run("keepalive_timeout", test311KeepaliveTimeout)
	t.Run("pingreq_pingresp", test311Ping)
	t.Run("oversized_packet", test311OversizedPacket)
	t.Run("topic_validation", test311TopicValidation)
}

func test311ConnectCleanSession(t *testing.T) {
	b := startBroker(t, Config{})

	first, ack := mustConnect(t, b.Addr(), confConnect{clientID: "clean-1", clean: true})
	if ack.SessionPresent {
		t.Fatal("clean session connection reports Session Present")
	}
	if codes := subscribe(t, first, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "clean/topic", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v, want [1]", codes)
	}
	first.disconnect()
	waitDisconnected(t, b, "clean-1")

	second, ack := mustConnect(t, b.Addr(), confConnect{clientID: "clean-1", clean: true})
	if ack.SessionPresent {
		t.Fatal("a clean session must not resume the previous session")
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "clean-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "clean/topic", payload: "x", qos: 1, pid: 1})
	expectNoMessage(t, second, 150*time.Millisecond)
}

func test311ConnectPersistentSession(t *testing.T) {
	b := startBroker(t, Config{})

	first, _ := mustConnect(t, b.Addr(), confConnect{clientID: "persist", clean: false})
	if codes := subscribe(t, first, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "persist/topic", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v, want [1]", codes)
	}
	first.disconnect()
	waitDisconnected(t, b, "persist")

	second, ack := mustConnect(t, b.Addr(), confConnect{clientID: "persist", clean: false})
	if !ack.SessionPresent {
		t.Fatal("persistent session reconnect must report Session Present")
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "persist-pub", clean: true})
	pub.mustPublish(t, confPublish{topic: "persist/topic", payload: "resumed", qos: 1, pid: 1})
	got := expectPublish(t, second)
	if got.Topic != "persist/topic" || string(got.Payload) != "resumed" || got.QoS != 1 {
		t.Fatalf("resumed subscription delivery = %q %q qos%d", got.Topic, got.Payload, got.QoS)
	}
	second.disconnect()
	waitDisconnected(t, b, "persist")

	// A clean connection discards the stored session.
	third, ack := mustConnect(t, b.Addr(), confConnect{clientID: "persist", clean: true})
	if ack.SessionPresent {
		t.Fatal("clean session must not resume a stored session")
	}
	pub.mustPublish(t, confPublish{topic: "persist/topic", payload: "dropped", qos: 1, pid: 2})
	expectNoMessage(t, third, 150*time.Millisecond)
	third.disconnect()
	waitDisconnected(t, b, "persist")

	fourth, ack := mustConnect(t, b.Addr(), confConnect{clientID: "persist", clean: false})
	if ack.SessionPresent {
		t.Fatal("the clean session must have deleted the stored session")
	}
	_ = fourth
}

func test311ClientIDRules(t *testing.T) {
	b := startBroker(t, Config{})

	if _, ack := dialConf(t, b.Addr(), confConnect{version: mqtt.Version311, clientID: "", clean: true}); ack.ReasonCode != 0 {
		t.Fatalf("empty client id with clean session: CONNACK %#x, want success", ack.ReasonCode)
	}
	emptyPersistent, ack := dialConf(t, b.Addr(), confConnect{version: mqtt.Version311, clientID: "", clean: false})
	if ack.ReasonCode != 2 {
		t.Fatalf("empty client id without clean session: CONNACK %#x, want 2", ack.ReasonCode)
	}
	expectClosed(t, emptyPersistent)

	if _, ack := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version311, clientID: "named-client", clean: true}); ack.SessionPresent {
		t.Fatal("new named client reports Session Present")
	}
}

func test311WillQoSRetain(t *testing.T) {
	b := startBroker(t, Config{})

	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "will-qos-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "will/#", QoS: 2}).ReasonCodes; len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK codes %v, want [2]", codes)
	}

	willClient, _ := mustConnect(t, b.Addr(), confConnect{
		clientID: "will-qos-client",
		clean:    true,
		will: &mqtt.Will{
			Topic:   "will/qos",
			Payload: []byte("gone"),
			QoS:     1,
			Retain:  true,
		},
	})
	_ = willClient.conn.Close()

	live := expectPublish(t, sub)
	if live.Topic != "will/qos" || string(live.Payload) != "gone" || live.QoS != 1 {
		t.Fatalf("will delivery = %q %q qos%d", live.Topic, live.Payload, live.QoS)
	}
	if live.Retain {
		t.Fatal("a will delivered to an existing subscription carries RETAIN=1")
	}

	later, _ := mustConnect(t, b.Addr(), confConnect{clientID: "will-qos-later", clean: true})
	if codes := subscribe(t, later, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "will/qos", QoS: 2}).ReasonCodes; len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK codes %v, want [2]", codes)
	}
	retained := expectPublish(t, later)
	if retained.Topic != "will/qos" || string(retained.Payload) != "gone" || retained.QoS != 1 {
		t.Fatalf("retained will = %q %q qos%d", retained.Topic, retained.Payload, retained.QoS)
	}
	if !retained.Retain {
		t.Fatal("a retained will replayed to a new subscription must carry RETAIN=1")
	}
}

func test311WillTopicValidation(t *testing.T) {
	b := startBroker(t, Config{})

	// MQTT 3.1.1 has no return code for an invalid will topic: the malformed
	// CONNECT closes the connection without a CONNACK.
	conn, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	raw := newConfClient(t, conn, mqtt.Version311, "will-bad-topic")
	raw.sendConnect(confConnect{
		clientID: "will-bad-topic",
		clean:    true,
		will:     &mqtt.Will{Topic: "will/#", Payload: []byte("x")},
	})
	if msg, err := raw.read(); err == nil {
		t.Fatalf("invalid 3.1.1 will topic answered with %T, want a closed connection", msg)
	}

	// MQTT 5 names the reason.
	bad, ack := dialConf(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "will-bad-topic-v5",
		clean:    true,
		will:     &mqtt.Will{Topic: "will/+", Payload: []byte("x")},
	})
	if ack.ReasonCode != 0x90 {
		t.Fatalf("invalid MQTT 5 will topic CONNACK %#x, want 0x90", ack.ReasonCode)
	}
	expectClosed(t, bad)
}

func test311UsernamePassword(t *testing.T) {
	var seen ConnectInfo
	auth := confAuth(func(info ConnectInfo) error {
		seen = info
		if info.Username == "alice" && string(info.Password) == "secret" {
			return nil
		}
		return ErrUnauthorized
	})
	b := startBroker(t, Config{Auth: auth})

	if _, ack := mustConnect(t, b.Addr(), confConnect{
		clientID: "auth-ok",
		clean:    true,
		username: "alice",
		password: []byte("secret"),
	}); ack.ReasonCode != 0 {
		t.Fatal("username/password connection was rejected")
	}
	if seen.Username != "alice" || string(seen.Password) != "secret" {
		t.Fatalf("auth saw username=%q password=%q", seen.Username, seen.Password)
	}

	bad, ack := dialConf(t, b.Addr(), confConnect{
		clientID: "auth-bad",
		clean:    true,
		username: "alice",
		password: []byte("wrong"),
	})
	if ack.ReasonCode != 5 {
		t.Fatalf("wrong password CONNACK %#x, want 5 (not authorized)", ack.ReasonCode)
	}
	expectClosed(t, bad)

	missing, ack := dialConf(t, b.Addr(), confConnect{clientID: "auth-missing", clean: true})
	if ack.ReasonCode != 5 {
		t.Fatalf("missing credentials CONNACK %#x, want 5", ack.ReasonCode)
	}
	expectClosed(t, missing)
}

func test311SubscribeWildcards(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "wild-sub", clean: true})
	ack := subscribe(t, sub, 1, mqtt.Properties{},
		mqtt.Subscription{Filter: "sport/+/player1", QoS: 1},
		mqtt.Subscription{Filter: "sport/#", QoS: 0},
	)
	if len(ack.ReasonCodes) != 2 || ack.ReasonCodes[0] != 1 || ack.ReasonCodes[1] != 0 {
		t.Fatalf("SUBACK codes %v, want [1 0]", ack.ReasonCodes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "wild-pub", clean: true})

	// Matches both filters: MQTT allows one copy per matching subscription.
	pub.publish(confPublish{topic: "sport/tennis/player1", payload: "both", qos: 0})
	both := collectPublishes(t, sub, 100*time.Millisecond)
	if len(both) == 0 {
		t.Fatal("no delivery for a topic matching two filters")
	}
	for _, p := range both {
		if p.Topic != "sport/tennis/player1" || string(p.Payload) != "both" {
			t.Fatalf("overlap delivery = %q %q", p.Topic, p.Payload)
		}
		if p.QoS > 1 {
			t.Fatalf("overlap delivery QoS %d exceeds the highest matching subscription (1)", p.QoS)
		}
	}

	// Matches only "sport/#".
	pub.publish(confPublish{topic: "sport/tennis/player2", payload: "hash", qos: 0})
	if got := collectPublishes(t, sub, 100*time.Millisecond); len(got) != 1 || string(got[0].Payload) != "hash" {
		t.Fatalf("sport/# delivery = %+v", got)
	}

	// "sport/#" also matches the parent level itself.
	pub.publish(confPublish{topic: "sport", payload: "parent", qos: 0})
	if got := collectPublishes(t, sub, 100*time.Millisecond); len(got) != 1 || string(got[0].Payload) != "parent" {
		t.Fatalf("sport/# parent delivery = %+v", got)
	}

	// A topic outside both filters is not delivered.
	pub.publish(confPublish{topic: "sports/x", payload: "nope", qos: 0})
	expectNoMessage(t, sub, 100*time.Millisecond)

	// A QoS 1 publication on the same overlapping filters is delivered at most
	// at the highest matching subscription QoS (1) and never above the publish
	// QoS.
	pub.mustPublish(t, confPublish{topic: "sport/tennis/player1", payload: "qos1", qos: 1, pid: 5})
	qos1 := collectPublishes(t, sub, 100*time.Millisecond)
	if len(qos1) == 0 {
		t.Fatal("no delivery for a QoS 1 publication matching two filters")
	}
	for _, p := range qos1 {
		if p.QoS > 1 {
			t.Fatalf("delivery QoS %d exceeds the highest matching subscription or the publish QoS", p.QoS)
		}
	}

	// A leading wildcard must not match reserved '$' topics.
	hash, _ := mustConnect(t, b.Addr(), confConnect{clientID: "hash-sub", clean: true})
	subscribe(t, hash, 3, mqtt.Properties{}, mqtt.Subscription{Filter: "#", QoS: 0})
	pub.publish(confPublish{topic: "$SYS/broker/uptime", payload: "1", qos: 0})
	expectNoMessage(t, hash, 100*time.Millisecond)
	pub.publish(confPublish{topic: "ordinary/x", payload: "visible", qos: 0})
	if got := expectPublish(t, hash); string(got.Payload) != "visible" {
		t.Fatalf("# delivery = %q", got.Payload)
	}

	// Zero-length topic level: "+" matches an empty level.
	zero, _ := mustConnect(t, b.Addr(), confConnect{clientID: "zero-sub", clean: true})
	if codes := subscribe(t, zero, 2, mqtt.Properties{}, mqtt.Subscription{Filter: "seg/+/end", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v, want [0]", codes)
	}
	pub.publish(confPublish{topic: "seg//end", payload: "empty-level", qos: 0})
	got := expectPublish(t, zero)
	if got.Topic != "seg//end" || string(got.Payload) != "empty-level" {
		t.Fatalf("zero-length segment delivery = %q %q", got.Topic, got.Payload)
	}
}

func test311SubackCodes(t *testing.T) {
	acl := confACL(func(_ SessionInfo, topic string, action AclAction) bool {
		return !(action == AclSubscribe && topic == "denied/x")
	})
	b := startBroker(t, Config{ACL: acl})
	c, _ := mustConnect(t, b.Addr(), confConnect{clientID: "suback", clean: true})
	ack := subscribe(t, c, 7, mqtt.Properties{},
		mqtt.Subscription{Filter: "allowed/#", QoS: 0},
		mqtt.Subscription{Filter: "denied/x", QoS: 1},
		mqtt.Subscription{Filter: "allowed/+", QoS: 2},
	)
	want := []byte{0, 0x80, 2}
	if len(ack.ReasonCodes) != len(want) {
		t.Fatalf("SUBACK codes %v, want %v", ack.ReasonCodes, want)
	}
	for i := range want {
		if ack.ReasonCodes[i] != want[i] {
			t.Fatalf("SUBACK codes %v, want %v", ack.ReasonCodes, want)
		}
	}
	if ack.PacketID != 7 {
		t.Fatalf("SUBACK packet id %d, want 7", ack.PacketID)
	}

	// The denied filter was not installed.
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "suback-pub", clean: true})
	pub.publish(confPublish{topic: "denied/x", payload: "denied", qos: 0})
	expectNoMessage(t, c, 100*time.Millisecond)

	// The per-session subscription cap also answers 0x80 on MQTT 3.1.1.
	capped := startBroker(t, Config{MaxTopicSubscriptions: 1})
	c2, _ := mustConnect(t, capped.Addr(), confConnect{clientID: "suback-capped", clean: true})
	capAck := subscribe(t, c2, 8, mqtt.Properties{},
		mqtt.Subscription{Filter: "cap/a", QoS: 0},
		mqtt.Subscription{Filter: "cap/b", QoS: 1},
	)
	if len(capAck.ReasonCodes) != 2 || capAck.ReasonCodes[0] != 0 || capAck.ReasonCodes[1] != 0x80 {
		t.Fatalf("SUBACK codes for a subscription cap = %v, want [0 0x80]", capAck.ReasonCodes)
	}
}

func test311SubscribeInvalidFilter(t *testing.T) {
	b := startBroker(t, Config{})
	for i, filter := range []string{"a/#/b", "a/b+", "+a/b", ""} {
		c, _ := mustConnect(t, b.Addr(), confConnect{clientID: fmt.Sprintf("bad-filter-%d", i), clean: true})
		c.write(&mqtt.Subscribe{
			MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311},
			PacketID:      1,
			Subscriptions: []mqtt.Subscription{{Filter: filter, QoS: 0}},
		})
		expectClosed(t, c)
	}
}

func test311Unsubscribe(t *testing.T) {
	b := startBroker(t, Config{})
	c, _ := mustConnect(t, b.Addr(), confConnect{clientID: "unsub", clean: true})
	if codes := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "u/#", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "unsub-pub", clean: true})
	pub.publish(confPublish{topic: "u/x", payload: "before", qos: 0})
	if got := expectPublish(t, c); string(got.Payload) != "before" {
		t.Fatalf("delivery before unsubscribe = %q", got.Payload)
	}

	ack := c.unsubscribe(2, "u/#")
	if ack.PacketID != 2 {
		t.Fatalf("UNSUBACK packet id %d, want 2", ack.PacketID)
	}
	if len(ack.ReasonCodes) != 0 {
		t.Fatalf("MQTT 3.1.1 UNSUBACK carries reason codes %v", ack.ReasonCodes)
	}
	pub.publish(confPublish{topic: "u/x", payload: "after", qos: 0})
	expectNoMessage(t, c, 100*time.Millisecond)

	// An UNSUBACK is still returned for a filter that was never subscribed.
	again := c.unsubscribe(3, "never/subscribed")
	if again.PacketID != 3 {
		t.Fatalf("UNSUBACK packet id %d, want 3", again.PacketID)
	}
}

func test311PublishQoS0(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "q0-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "q0/x", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "q0-pub", clean: true})
	pub.publish(confPublish{topic: "q0/x", payload: "zero", qos: 0})
	got := expectPublish(t, sub)
	if got.Topic != "q0/x" || string(got.Payload) != "zero" {
		t.Fatalf("QoS 0 delivery = %q %q", got.Topic, got.Payload)
	}
	if got.QoS != 0 || got.PacketID != 0 {
		t.Fatalf("QoS 0 delivery has QoS %d packet id %d", got.QoS, got.PacketID)
	}
	// QoS 0 is fire and forget: no acknowledgement comes back.
	expectNoMessage(t, pub, 100*time.Millisecond)
}

func test311PublishQoS1(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "q1-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "q1/x", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "q1-pub", clean: true})

	pub.publish(confPublish{topic: "q1/x", payload: "first", qos: 1, pid: 10})
	expectPuback(t, pub, 10, 0)
	first := expectPublish(t, sub)
	if first.Topic != "q1/x" || string(first.Payload) != "first" || first.QoS != 1 || first.PacketID == 0 {
		t.Fatalf("QoS 1 delivery = %+v", first)
	}
	if first.Dup {
		t.Fatal("a fresh QoS 1 delivery carries DUP=1")
	}

	// The subscription QoS is a cap: a QoS 0 subscription receives the QoS 1
	// publication as QoS 0.
	capped, _ := mustConnect(t, b.Addr(), confConnect{clientID: "q1-capped", clean: true})
	if codes := subscribe(t, capped, 2, mqtt.Properties{}, mqtt.Subscription{Filter: "q1/x", QoS: 0}).ReasonCodes; len(codes) != 1 || codes[0] != 0 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub.publish(confPublish{topic: "q1/x", payload: "capped", qos: 1, pid: 11})
	expectPuback(t, pub, 11, 0)
	cappedGot := expectPublish(t, capped)
	if cappedGot.QoS != 0 || cappedGot.PacketID != 0 {
		t.Fatalf("QoS 1 publication to a QoS 0 subscription = QoS %d packet id %d", cappedGot.QoS, cappedGot.PacketID)
	}
	if got := expectPublish(t, sub); string(got.Payload) != "capped" {
		t.Fatalf("QoS 1 subscription delivery = %q", got.Payload)
	}

	// A publisher retransmission is delivered again (at-least-once) and
	// acknowledged again.
	pub.publish(confPublish{topic: "q1/x", payload: "first", qos: 1, pid: 10, dup: true})
	expectPuback(t, pub, 10, 0)
	second := expectPublish(t, sub)
	if second.Topic != "q1/x" || string(second.Payload) != "first" {
		t.Fatalf("retransmit delivery = %q %q", second.Topic, second.Payload)
	}

	t.Run("broker_retransmission", func(t *testing.T) {
		t.Skip("broker-side QoS 1 retransmission and inflight tracking are not implemented: README.md says \"broker-side retransmission and inflight limits are still on the roadmap\"")
	})
}

func test311PublishQoS2(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "q2-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "q2/x", QoS: 2}).ReasonCodes; len(codes) != 1 || codes[0] != 2 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "q2-pub", clean: true})

	pub.publish(confPublish{topic: "q2/x", payload: "exactly-once", qos: 2, pid: 21})
	expectPubrec(t, pub, 21, 0)
	got := expectPublish(t, sub)
	if got.Topic != "q2/x" || string(got.Payload) != "exactly-once" || got.QoS != 2 {
		t.Fatalf("QoS 2 delivery = %+v", got)
	}

	// Retransmission of the same PUBLISH before PUBREL must be acknowledged
	// again but must not be delivered again.
	pub.publish(confPublish{topic: "q2/x", payload: "exactly-once", qos: 2, pid: 21, dup: true})
	expectPubrec(t, pub, 21, 0)
	expectNoMessage(t, sub, 150*time.Millisecond)

	pub.write(&mqtt.Pubrel{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: 21})
	expectPubcomp(t, pub, 21)

	// The packet id is free again: a new PUBLISH using it is a new message.
	pub.publish(confPublish{topic: "q2/x", payload: "second", qos: 2, pid: 21})
	expectPubrec(t, pub, 21, 0)
	if got := expectPublish(t, sub); string(got.Payload) != "second" {
		t.Fatalf("second QoS 2 delivery = %q", got.Payload)
	}
	pub.write(&mqtt.Pubrel{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}, PacketID: 21})
	expectPubcomp(t, pub, 21)

	t.Run("state_across_reconnect", func(t *testing.T) {
		t.Skip("QoS 2 state does not survive a reconnect: ROADMAP.md lists \"Full QoS 2 state machine and inflight limits\" as open")
	})
}

func test311Retained(t *testing.T) {
	b := startBroker(t, Config{})
	live, _ := mustConnect(t, b.Addr(), confConnect{clientID: "ret-live", clean: true})
	if codes := subscribe(t, live, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "ret/#", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "ret-pub", clean: true})

	pub.mustPublish(t, confPublish{topic: "ret/a", payload: "v1", qos: 1, pid: 1, retain: true})
	liveGot := expectPublish(t, live)
	if liveGot.Topic != "ret/a" || string(liveGot.Payload) != "v1" || liveGot.Retain {
		t.Fatalf("live retained delivery = %q %q retain=%v", liveGot.Topic, liveGot.Payload, liveGot.Retain)
	}

	later, _ := mustConnect(t, b.Addr(), confConnect{clientID: "ret-later", clean: true})
	if codes := subscribe(t, later, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "ret/#", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	replay := expectPublish(t, later)
	if replay.Topic != "ret/a" || string(replay.Payload) != "v1" || replay.QoS != 1 || !replay.Retain {
		t.Fatalf("retained replay = %q %q qos%d retain=%v", replay.Topic, replay.Payload, replay.QoS, replay.Retain)
	}

	// Replace the stored value.
	pub.mustPublish(t, confPublish{topic: "ret/a", payload: "v2", qos: 1, pid: 2, retain: true})
	if got := expectPublish(t, live); string(got.Payload) != "v2" {
		t.Fatalf("live replace delivery = %q", got.Payload)
	}
	replaced, _ := mustConnect(t, b.Addr(), confConnect{clientID: "ret-replaced", clean: true})
	subscribe(t, replaced, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "ret/a", QoS: 1})
	if got := expectPublish(t, replaced); string(got.Payload) != "v2" || !got.Retain {
		t.Fatalf("replaced retained value = %q retain=%v", got.Payload, got.Retain)
	}

	// An empty retained payload deletes the stored value. It is still delivered
	// to current subscribers as a normal message.
	pub.mustPublish(t, confPublish{topic: "ret/a", payload: "", qos: 1, pid: 3, retain: true})
	if got := expectPublish(t, live); len(got.Payload) != 0 || got.Retain {
		t.Fatalf("retained delete delivery = %q retain=%v", got.Payload, got.Retain)
	}
	gone, _ := mustConnect(t, b.Addr(), confConnect{clientID: "ret-gone", clean: true})
	subscribe(t, gone, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "ret/#", QoS: 1})
	expectNoMessage(t, gone, 150*time.Millisecond)
}

func test311WillOnDisconnect(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "will-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "will/event", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v", codes)
	}

	// A clean DISCONNECT drops the will.
	clean, _ := mustConnect(t, b.Addr(), confConnect{
		clientID: "will-clean",
		clean:    true,
		will:     &mqtt.Will{Topic: "will/event", Payload: []byte("must-not-appear")},
	})
	clean.disconnect()
	waitDisconnected(t, b, "will-clean")
	expectNoMessage(t, sub, 150*time.Millisecond)

	// An abnormal close publishes it.
	dropped, _ := mustConnect(t, b.Addr(), confConnect{
		clientID: "will-dropped",
		clean:    true,
		will:     &mqtt.Will{Topic: "will/event", Payload: []byte("lost")},
	})
	_ = dropped.conn.Close()
	got := expectPublish(t, sub)
	if string(got.Payload) != "lost" {
		t.Fatalf("will payload = %q, want lost", got.Payload)
	}
}

func test311DupRetainFlags(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "flags-sub", clean: true})
	if codes := subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "flags/#", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v", codes)
	}
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "flags-pub", clean: true})

	pub.mustPublish(t, confPublish{topic: "flags/fresh", payload: "fresh", qos: 1, pid: 1})
	fresh := expectPublish(t, sub)
	if fresh.Dup || fresh.Retain {
		t.Fatalf("fresh delivery flags DUP=%v RETAIN=%v, want both false", fresh.Dup, fresh.Retain)
	}

	pub.mustPublish(t, confPublish{topic: "flags/dup", payload: "dup", qos: 1, pid: 2, dup: true})
	dup := expectPublish(t, sub)
	if dup.Dup {
		t.Fatal("an inbound DUP flag must not be forwarded on a new delivery")
	}
	if dup.Retain {
		t.Fatal("an inbound DUP delivery must not set RETAIN")
	}

	pub.mustPublish(t, confPublish{topic: "flags/retained", payload: "kept", qos: 1, pid: 3, retain: true})
	live := expectPublish(t, sub)
	if live.Retain {
		t.Fatal("RETAIN must be 0 when a retained publish is delivered to an established subscription")
	}

	later, _ := mustConnect(t, b.Addr(), confConnect{clientID: "flags-later", clean: true})
	subscribe(t, later, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "flags/retained", QoS: 1})
	replay := expectPublish(t, later)
	if !replay.Retain || replay.Dup {
		t.Fatalf("retained replay flags DUP=%v RETAIN=%v, want false/true", replay.Dup, replay.Retain)
	}
}

func test311KeepaliveZero(t *testing.T) {
	b := startBroker(t, Config{})
	c, _ := mustConnect(t, b.Addr(), confConnect{clientID: "ka-zero", clean: true, keepAlive: 0})
	// No keep-alive timer: the connection stays usable while idle.
	time.Sleep(150 * time.Millisecond)
	c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version311}})
	if p := readConf[*mqtt.Pingresp](t, c); p.Type() != mqtt.TypePingresp {
		t.Fatalf("PINGRESP = %T", p)
	}
}

func test311KeepaliveTimeout(t *testing.T) {
	b := startBroker(t, Config{})
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "ka-timeout-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "ka/will", QoS: 0})
	c, _ := mustConnect(t, b.Addr(), confConnect{
		clientID:  "ka-timeout",
		clean:     true,
		keepAlive: 1,
		will:      &mqtt.Will{Topic: "ka/will", Payload: []byte("timeout")},
	})
	c.timeout = 5 * time.Second
	start := time.Now()
	_, err := c.read()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("the broker kept a silent connection with keepalive 1s open")
	}
	// A keep-alive expiry is an abnormal disconnect: the will is published.
	if got := expectPublish(t, sub); string(got.Payload) != "timeout" {
		t.Fatalf("will after keep-alive timeout = %q", got.Payload)
	}
	if elapsed < time.Second {
		t.Fatalf("keepalive timeout fired after %v, before 1.5x the 1s keepalive", elapsed)
	}
	if elapsed > 4*time.Second {
		t.Fatalf("keepalive timeout fired after %v, too late for a 1s keepalive", elapsed)
	}
}

func test311Ping(t *testing.T) {
	for _, version := range []byte{mqtt.Version311, mqtt.Version5} {
		b := startBroker(t, Config{})
		c, _ := mustConnect(t, b.Addr(), confConnect{version: version, clientID: "ping", clean: true, keepAlive: 30})
		c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: version}})
		p := readConf[*mqtt.Pingresp](t, c)
		if p.Type() != mqtt.TypePingresp {
			t.Fatalf("version %d: PINGRESP = %T", version, p)
		}
	}
}

func test311OversizedPacket(t *testing.T) {
	b := startBroker(t, Config{MaxPacketSize: 64})
	c, _ := mustConnect(t, b.Addr(), confConnect{clientID: "big", clean: true})
	c.publish(confPublish{topic: "big/topic", payload: string(make([]byte, 256)), qos: 0})
	expectClosed(t, c)

	// A CONNECT whose body exceeds the limit is closed before a CONNACK exists.
	conn, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatal(err)
	}
	raw := newConfClient(t, conn, mqtt.Version311, "huge")
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0x02, 0, 30}
	body = appendString(body, string(make([]byte, 256)))
	frame := appendRemainingLength([]byte{0x10}, len(body))
	raw.writeRaw(append(frame, body...))
	_ = raw.conn.SetReadDeadline(time.Now().Add(raw.timeout))
	if _, err := raw.r.ReadByte(); err == nil {
		t.Fatal("oversized CONNECT was answered instead of closed")
	} else {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("oversized CONNECT left the connection open")
		}
	}
}

func test311TopicValidation(t *testing.T) {
	b := startBroker(t, Config{})

	// An empty topic name in PUBLISH is a protocol violation; MQTT 3.1.1 has no
	// DISCONNECT, so the connection is closed.
	empty, _ := mustConnect(t, b.Addr(), confConnect{clientID: "topic-empty", clean: true})
	empty.publish(confPublish{topic: "", payload: "x", qos: 0})
	expectClosed(t, empty)

	// A wildcard in a PUBLISH topic is a protocol violation too.
	wild, _ := mustConnect(t, b.Addr(), confConnect{clientID: "topic-wild", clean: true})
	wild.publish(confPublish{topic: "a/+/b", payload: "x", qos: 0})
	expectClosed(t, wild)

	hash, _ := mustConnect(t, b.Addr(), confConnect{clientID: "topic-hash", clean: true})
	hash.publish(confPublish{topic: "a/#", payload: "x", qos: 0})
	expectClosed(t, hash)

	// A zero-length level is valid and routed verbatim.
	sub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "topic-sub", clean: true})
	subscribe(t, sub, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "a/+/b", QoS: 0})
	pub, _ := mustConnect(t, b.Addr(), confConnect{clientID: "topic-pub", clean: true})
	pub.publish(confPublish{topic: "a//b", payload: "ok", qos: 0})
	if got := expectPublish(t, sub); got.Topic != "a//b" || string(got.Payload) != "ok" {
		t.Fatalf("zero-length segment delivery = %q %q", got.Topic, got.Payload)
	}
}
