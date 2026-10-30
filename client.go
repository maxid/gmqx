package gmqx

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
	"github.com/maxid/gnety/codec/wire"
)

// subscription stores MQTT 5 subscription options.
type subscription struct {
	qos               byte
	noLocal           bool
	retainAsPublished bool
	retainHandling    byte
}

type client struct {
	broker *Broker
	conn   net.Conn
	connID ConnectionID
	r      *bufio.Reader
	wmu    sync.Mutex

	mu           sync.Mutex
	id           string
	info         ConnectInfo
	subs         map[string]subscription
	keepAlive    time.Duration
	closed       bool
	closeErr     error
	will         *retainedMessage
	username     string
	registered   bool
	lost         bool
	topicAliases map[uint16]string
	outPID       uint16

	// version is the protocol level of the connection: mqtt.Version311 or
	// mqtt.Version5. MQTT 3.1 clients decode and encode with the 3.1.1 layout,
	// which is byte-identical for everything except CONNECT.
	version byte
}

func newClient(b *Broker, conn net.Conn) *client {
	return &client{
		broker:       b,
		connID:       nextConnectionID(),
		conn:         conn,
		r:            bufio.NewReaderSize(conn, 4096),
		subs:         make(map[string]subscription),
		topicAliases: make(map[uint16]string),
		version:      mqtt.Version311,
	}
}

// header is the protocol level every packet this client sends carries.
func (c *client) header() mqtt.MessageHeader {
	return mqtt.MessageHeader{Level: c.version}
}

// readMessage reads and decodes one control packet. The typed codec owns the
// wire format, so gmqx never branches on a fixed-header byte.
func (c *client) readMessage() (mqtt.Message, error) {
	if m, ok := c.emptyDisconnect(); ok {
		return m, nil
	}
	d := mqtt.Decoder{Version: c.version, MaxPacketSize: c.broker.cfg.MaxPacketSize}
	return d.Decode(c.r)
}

// emptyDisconnect consumes the DISCONNECT an MQTT 5 client sends when it says
// goodbye: type and flags 0xE0, remaining length 0, nothing else.
//
// The typed decoder cannot take that frame. For a zero remaining length it gives
// its body reader a one-byte budget, so the decoder's own "no body left?" test
// sees a byte and reads the optional reason code — from whatever comes next on
// the stream. gmqx recognizes the two-byte frame instead of waiting for a byte a
// well-behaved client never sends. Every other DISCONNECT (with a reason code,
// with properties, or any MQTT 3.1.1 one) still goes through the decoder.
func (c *client) emptyDisconnect() (mqtt.Message, bool) {
	if c.version != mqtt.Version5 {
		return nil, false
	}
	head, err := c.r.Peek(2)
	if err != nil || head[0] != mqtt.TypeDisconnect<<4 || head[1] != 0 {
		return nil, false
	}
	if _, err := c.r.Discard(2); err != nil {
		return nil, false
	}
	return &mqtt.Disconnect{MessageHeader: c.header()}, true
}

// writeMessage encodes one packet and writes it, serialized per connection.
func (c *client) writeMessage(m mqtt.Message) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return mqtt.Encode(c.conn, m)
}

// readConnect reads the opening packet of a connection, which must be CONNECT.
//
// The frame is read first because the typed codec models MQTT 3.1.1 and 5.0
// only: MQTT 3.1 (protocol name "MQIsdp", level 3) shares the CONNECT layout, so
// a 3.1 body is rewritten to its 3.1.1 spelling and decoded by the codec like
// any other packet. v31 reports that the client asked for 3.1, which only the
// client-id rule cares about.
func (c *client) readConnect() (connect *mqtt.Connect, v31 bool, err error) {
	header, body, err := mqtt.ReadPacket(c.r, c.broker.cfg.MaxPacketSize)
	if err != nil {
		return nil, false, err
	}
	if header>>4 != mqtt.TypeConnect {
		return nil, false, fmt.Errorf("%w: first packet is not CONNECT", ErrMalformedPacket)
	}
	body, v31 = normalizeV31Connect(body)
	frame := mqtt.AppendRemainingLength([]byte{header}, len(body))
	frame = append(frame, body...)
	d := mqtt.Decoder{MaxPacketSize: c.broker.cfg.MaxPacketSize}
	msg, err := d.Decode(bufio.NewReader(bytes.NewReader(frame)))
	if err != nil {
		return nil, false, connectError(err)
	}
	connect, ok := msg.(*mqtt.Connect)
	if !ok {
		return nil, false, fmt.Errorf("%w: first packet is not CONNECT", ErrMalformedPacket)
	}
	return connect, v31, nil
}

// normalizeV31Connect rewrites an MQTT 3.1 CONNECT body ("MQIsdp", level 3) to
// the 3.1.1 spelling ("MQTT", level 4). Both versions lay out the connect flags,
// the keep-alive and the payload identically, so the protocol name and the level
// are the whole difference.
func normalizeV31Connect(body []byte) ([]byte, bool) {
	const name = "MQIsdp"
	if len(body) < 2+len(name)+1 {
		return body, false
	}
	if binary.BigEndian.Uint16(body) != uint16(len(name)) || string(body[2:2+len(name)]) != name || body[2+len(name)] != 3 {
		return body, false
	}
	out := make([]byte, 0, len(body)-2)
	out = append(out, 0, 4)
	out = append(out, "MQTT"...)
	out = append(out, mqtt.Version311)
	return append(out, body[2+len(name)+1:]...), true
}

// connectError keeps "not an MQTT version this broker speaks" distinguishable
// from "malformed CONNECT", as ErrUnsupportedProtocol documents.
func connectError(err error) error {
	if errors.Is(err, wire.ErrUnsupported) {
		return fmt.Errorf("%w: %w", ErrUnsupportedProtocol, err)
	}
	return err
}

// connectInfo maps a decoded CONNECT onto the public ConnectInfo the SPI sees.
func connectInfo(m *mqtt.Connect) ConnectInfo {
	info := ConnectInfo{
		ClientID:     string(m.ClientID),
		Username:     string(m.Username),
		Password:     m.Password,
		KeepAlive:    m.KeepAlive,
		CleanSession: m.CleanSession,
	}
	if m.Will != nil {
		info.WillTopic = m.Will.Topic
		info.WillPayload = m.Will.Payload
	}
	return info
}

func (c *client) handshake() error {
	connect, v31, err := c.readConnect()
	if err != nil {
		return err
	}
	c.version = connect.Version()
	info := connectInfo(connect)
	info.RemoteAddr = c.conn.RemoteAddr().String()
	if err := c.broker.cfg.Auth.Authenticate(context.Background(), info); err != nil {
		_ = c.writeConnack(connackNotAuthorized(c.version), "")
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	assigned := ""
	if info.ClientID == "" {
		if v31 || !info.CleanSession {
			_ = c.writeConnack(connackClientIDInvalid(c.version), "")
			return fmt.Errorf("%w: empty client id is not allowed", ErrMalformedPacket)
		}
		info.ClientID = randomID()
		assigned = info.ClientID
	}
	if v31 && len(info.ClientID) > 23 {
		_ = c.writeConnack(connackClientIDInvalid(c.version), "")
		return fmt.Errorf("%w: MQTT 3.1 client id exceeds 23 characters", ErrMalformedPacket)
	}
	c.mu.Lock()
	c.id = info.ClientID
	c.info = info
	c.keepAlive = time.Duration(info.KeepAlive) * time.Second
	c.username = info.Username
	if info.WillTopic != "" {
		c.will = &retainedMessage{topic: info.WillTopic, payload: info.WillPayload}
	}
	c.mu.Unlock()
	if err := c.broker.register(c); err != nil {
		code := byte(5)
		switch {
		case errors.Is(err, ErrClientIDInUse):
			code = connackClientIDInvalid(c.version)
		case errors.Is(err, ErrMaxConnections):
			code = connackQuotaExceeded(c.version)
		}
		_ = c.writeConnack(code, "")
		return err
	}
	if err := c.writeConnack(0, assigned); err != nil {
		return err
	}
	return nil
}

func (c *client) loop() {
	for {
		deadline := time.Time{}
		c.mu.Lock()
		if c.keepAlive > 0 {
			deadline = time.Now().Add(c.keepAlive * 3 / 2)
		}
		c.mu.Unlock()
		_ = c.conn.SetReadDeadline(deadline)

		msg, err := c.readMessage()
		if err != nil {
			c.mu.Lock()
			c.lost = true
			c.mu.Unlock()
			c.close(true)
			return
		}
		switch m := msg.(type) {
		case *mqtt.Publish:
			c.handlePublish(m)
		case *mqtt.Pubrel:
			c.handlePubrel(m)
		case *mqtt.Subscribe:
			c.handleSubscribe(m)
		case *mqtt.Unsubscribe:
			c.handleUnsubscribe(m)
		case *mqtt.Pingreq:
			_ = c.writeMessage(&mqtt.Pingresp{MessageHeader: c.header()})
			if c.broker.cfg.Sink != nil {
				c.broker.cfg.Sink.OnPing(context.Background(), c.id)
			}
		case *mqtt.Disconnect:
			c.mu.Lock()
			c.will = nil
			c.lost = false
			c.mu.Unlock()
			c.close(false)
			return
		default:
			c.broker.log.Debug("gmqx ignoring packet type", "type", msg.Type())
		}
	}
}

// handlePubrel releases an inbound QoS 2 publish: the client waits for the
// PUBCOMP before it reuses the packet id.
func (c *client) handlePubrel(m *mqtt.Pubrel) {
	_ = c.writeMessage(&mqtt.Pubcomp{MessageHeader: c.header(), PacketID: m.PacketID})
}

func (c *client) handlePublish(m *mqtt.Publish) {
	topic := m.Topic
	props := m.Properties
	// Topic Alias and Subscription Identifier are hop-local: the alias resolves
	// (and records) the topic name, and neither property is forwarded.
	alias := props.TopicAlias
	props.TopicAlias = 0
	props.SubscriptionIdentifiers = nil
	var wireProps []byte
	if c.version == mqtt.Version5 {
		if topic == "" {
			if alias == 0 {
				return
			}
			resolved, ok := c.topicAliases[alias]
			if !ok {
				return
			}
			topic = resolved
		} else if alias != 0 {
			c.topicAliases[alias] = topic
		}
		var err error
		if wireProps, err = encodePropertyBlock(props); err != nil {
			return
		}
	}
	if !validTopic(topic) {
		return
	}
	res := c.broker.publish(c, PublishEvent{
		ConnectionID: c.connID,
		ClientID:     c.id,
		Topic:        topic,
		Payload:      m.Payload,
		QoS:          m.QoS,
		PacketID:     m.PacketID,
		Retain:       m.Retain,
		Properties:   wireProps,
	}, props)
	switch m.QoS {
	case 1, 2:
		c.ackPublish(m.QoS, m.PacketID, res)
	}
}

func (c *client) handleSubscribe(m *mqtt.Subscribe) {
	codes := make([]byte, 0, len(m.Subscriptions))
	existed := make([]bool, 0, len(m.Subscriptions))
	for _, req := range m.Subscriptions {
		code, had := c.broker.subscribe(c, req.Filter, subscription{
			qos:               req.QoS,
			noLocal:           req.NoLocal,
			retainAsPublished: req.RetainAsPublished,
			retainHandling:    req.RetainHandling,
		})
		if c.version == mqtt.Version5 && code == 0x80 {
			code = 0x87
		}
		codes = append(codes, code)
		existed = append(existed, had)
	}
	_ = c.writeMessage(&mqtt.Suback{
		MessageHeader: c.header(),
		PacketID:      m.PacketID,
		ReasonCodes:   codes,
	})
	for i, req := range m.Subscriptions {
		if codes[i] == 0x80 || codes[i] == 0x87 {
			continue
		}
		if req.RetainHandling == 2 || (req.RetainHandling == 1 && existed[i]) {
			continue
		}
		c.broker.sendRetained(c, req.Filter, req.QoS, req.RetainAsPublished)
	}
}

func (c *client) handleUnsubscribe(m *mqtt.Unsubscribe) {
	// MQTT 5 answers with one reason code per filter, all Success: a filter that
	// was not subscribed is a no-op for this broker. MQTT 3.1.1 carries none.
	codes := make([]byte, len(m.Filters))
	for i, filter := range m.Filters {
		c.broker.unsubscribe(c, filter)
		codes[i] = 0
	}
	_ = c.writeMessage(&mqtt.Unsuback{
		MessageHeader: c.header(),
		PacketID:      m.PacketID,
		ReasonCodes:   codes,
	})
}

func (c *client) sendPublish(topic string, payload []byte, qos byte, retain bool, props mqtt.Properties) error {
	var pid uint16
	if qos > 0 {
		c.mu.Lock()
		c.outPID++
		pid = c.outPID
		c.mu.Unlock()
	}
	return c.writeMessage(&mqtt.Publish{
		MessageHeader: c.header(),
		Topic:         topic,
		Payload:       payload,
		QoS:           qos,
		PacketID:      pid,
		Retain:        retain,
		Properties:    props,
	})
}

func (c *client) writeConnack(code byte, assignedClientID string) error {
	ack := &mqtt.Connack{MessageHeader: c.header(), ReasonCode: code}
	if c.version == mqtt.Version5 && code == 0 {
		ack.Properties = connackProperties(assignedClientID, uint32(c.broker.cfg.MaxPacketSize))
	}
	return c.writeMessage(ack)
}

func (c *client) close(withWill bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	will := c.will
	if !withWill {
		will = nil
	}
	c.mu.Unlock()
	_ = c.conn.Close()
	if will != nil && c.broker != nil {
		_ = c.broker.publish(c, PublishEvent{Topic: will.topic, Payload: will.payload}, mqtt.Properties{})
	}
}

func connackNotAuthorized(version byte) byte {
	if version == mqtt.Version5 {
		return 0x87
	}
	return 5
}

func connackClientIDInvalid(version byte) byte {
	if version == mqtt.Version5 {
		return 0x85
	}
	return 2
}

func connackQuotaExceeded(version byte) byte {
	if version == mqtt.Version5 {
		return 0x97
	}
	return 5
}

// ackPublish acknowledges an accepted publish. A retryable or rejected publish
// gets no success ACK: MQTT 3.1.1 has no negative PUBACK, and an optimistic ACK
// would claim durability the broker does not have. MQTT 5 clients get the reason
// code instead.
func (c *client) ackPublish(qos byte, pid uint16, res PublishResult) {
	if res.Accepted() {
		_ = c.writePublishAck(qos, pid, 0)
		return
	}
	code := res.ReasonCode
	if code == 0 {
		if res.Disposition == PublishRetryable {
			code = ReasonQuotaExceeded
		} else {
			code = ReasonUnspecified
		}
	}
	if c.version == mqtt.Version5 {
		_ = c.writePublishAck(qos, pid, code)
	}
}

// writePublishAck writes the PUBACK of a QoS 1 publish or the PUBREC of a QoS 2
// one. The reason code is only encoded on MQTT 5.
func (c *client) writePublishAck(qos byte, pid uint16, reason byte) error {
	if qos == 2 {
		return c.writeMessage(&mqtt.Pubrec{MessageHeader: c.header(), PacketID: pid, ReasonCode: reason})
	}
	return c.writeMessage(&mqtt.Puback{MessageHeader: c.header(), PacketID: pid, ReasonCode: reason})
}

var connSeq atomic.Uint64

// nextConnectionID returns a process-unique connection identifier.
func nextConnectionID() ConnectionID {
	return ConnectionID(strconv.FormatUint(connSeq.Add(1), 10))
}
