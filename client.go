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

	// namespace is the broker's isolation scope, fixed for the connection.
	namespace string
	// connCtx is the cancellable context of the connection. ctx is connCtx with
	// ConnInfo attached: it is what every hook of this connection receives. Both
	// are written during the handshake, before the client is registered.
	connCtx context.Context
	ctx     context.Context
	// cancel cancels the context derived from Config.ConnContext, parentCancel
	// the base context it was derived from. stopCtx cancels both, once.
	cancel       context.CancelFunc
	parentCancel context.CancelFunc
	stopOnce     sync.Once

	mu             sync.Mutex
	id             string
	info           ConnectInfo
	subs           map[string]subscription
	keepAlive      time.Duration
	closed         bool
	closeErr       error
	will           *mqtt.Will
	username       string
	registered     bool
	lost           bool
	superseded     bool
	sessionPresent bool
	inboundQoS2    map[uint16]struct{}
	topicAliases   map[uint16]string
	outPID         uint16

	// version is the protocol level of the connection: mqtt.Version311 or
	// mqtt.Version5. MQTT 3.1 clients decode and encode with the 3.1.1 layout,
	// which is byte-identical for everything except CONNECT.
	version byte
}

func newClient(b *Broker, conn net.Conn) *client {
	connCtx, cancel := context.WithCancel(b.lifecycleContext())
	return &client{
		broker:       b,
		namespace:    b.namespace,
		connCtx:      connCtx,
		ctx:          connCtx,
		cancel:       cancel,
		parentCancel: cancel,
		connID:       nextConnectionID(),
		conn:         conn,
		r:            bufio.NewReaderSize(conn, 4096),
		subs:         make(map[string]subscription),
		inboundQoS2:  make(map[uint16]struct{}),
		topicAliases: make(map[uint16]string),
		version:      mqtt.Version311,
	}
}

// context returns the connection-scoped context every hook of this connection
// receives. It carries ConnInfo and is cancelled by stopContext when the
// connection ends. It is never nil.
func (c *client) context() context.Context { return c.ctx }

// attachConnectContext builds the connection context from the parsed CONNECT:
// the optional Config.ConnContext hook enriches it once, and ConnInfo is attached
// so every hook can identify the connection without a thread-local request
// scope. The context it creates is cancelled by stopContext, even when the hook
// returns a context that is not derived from the one it was given.
func (c *client) attachConnectContext(info ConnectInfo) {
	ctx := c.connCtx
	if hook := c.broker.cfg.ConnContext; hook != nil {
		if enriched := hook(ctx, info); enriched != nil {
			enrichedCtx, cancel := context.WithCancel(enriched)
			c.connCtx, c.cancel = enrichedCtx, cancel
			ctx = enrichedCtx
		}
	}
	c.ctx = ContextWithConn(ctx, c.connInfo(info))
}

// setConnInfo refreshes the ConnInfo value after the broker assigned a client
// id. The connection context itself, and therefore its cancellation, is kept.
func (c *client) setConnInfo(info ConnectInfo) {
	c.ctx = ContextWithConn(c.connCtx, c.connInfo(info))
}

func (c *client) connInfo(info ConnectInfo) ConnInfo {
	return ConnInfo{
		ClientID:        info.ClientID,
		ConnectionID:    string(c.connID),
		Namespace:       c.namespace,
		RemoteAddr:      info.RemoteAddr,
		ProtocolVersion: c.version,
	}
}

// stopContext cancels the connection context. close calls it as soon as the
// connection is gone (after a will was published, before the disconnect hooks),
// and serve calls it again when its goroutine ends, so the cancellation happens
// even for a connection that never got past the handshake. It is safe to call
// more than once.
func (c *client) stopContext() {
	c.stopOnce.Do(func() {
		c.parentCancel()
		c.cancel()
	})
}

// persistentSession reports whether the session state survives the connection.
// MQTT 3.1.1 keys it off Clean Session; MQTT 5 keys it off Clean Start plus a
// non-zero Session Expiry Interval (the absent property means 0, so the session
// ends with the connection).
func (c *client) persistentSession() bool {
	if c.info.CleanSession {
		return false
	}
	if c.version == mqtt.Version5 {
		return c.info.SessionExpiry > 0
	}
	return true
}

// resumesSession reports whether a CONNECT asks to resume existing state. A
// zero Session Expiry Interval does not prevent resuming: it only means the
// session ends again when this connection closes.
func (c *client) resumesSession() bool { return !c.info.CleanSession }

// header is the protocol level every packet this client sends carries.
func (c *client) header() mqtt.MessageHeader {
	return mqtt.MessageHeader{Level: c.version}
}

// readMessage reads and decodes one control packet. The typed codec owns the
// wire format, so gmqx never branches on a fixed-header byte.
func (c *client) readMessage() (mqtt.Message, error) {
	d := mqtt.Decoder{Version: c.version, MaxPacketSize: c.broker.cfg.MaxPacketSize}
	return d.Decode(c.r)
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
		ClientID:      string(m.ClientID),
		Username:      string(m.Username),
		Password:      m.Password,
		KeepAlive:     m.KeepAlive,
		CleanSession:  m.CleanSession,
		SessionExpiry: m.Properties.SessionExpiryInterval,
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
	c.attachConnectContext(info)
	if err := c.broker.cfg.Auth.Authenticate(c.context(), info); err != nil {
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
	if connect.Will != nil && !validTopic(connect.Will.Topic) {
		// A will topic is a topic name: no wildcards and not empty. MQTT 5 can
		// name the reason; MQTT 3.1.1 has no return code for it, so the
		// malformed CONNECT just closes the connection.
		if c.version == mqtt.Version5 {
			_ = c.writeConnack(connackTopicNameInvalid(c.version), "")
		}
		return fmt.Errorf("%w: invalid will topic %q", ErrMalformedPacket, connect.Will.Topic)
	}
	c.mu.Lock()
	c.id = info.ClientID
	c.info = info
	c.keepAlive = time.Duration(info.KeepAlive) * time.Second
	c.username = info.Username
	if connect.Will != nil {
		will := *connect.Will
		will.Payload = append([]byte(nil), connect.Will.Payload...)
		c.will = &will
	}
	c.mu.Unlock()
	// The client id can be assigned by the broker, so ConnInfo is refreshed with
	// the final identity before anything else observes the connection.
	c.setConnInfo(info)
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
			c.failProtocol(err)
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
				c.broker.cfg.Sink.OnPing(c.context(), c.id)
			}
		case *mqtt.Disconnect:
			// A DISCONNECT with reason code 0x04 asks the server to publish the
			// will even though the disconnect itself is orderly.
			withWill := c.version == mqtt.Version5 && m.ReasonCode == 0x04
			c.mu.Lock()
			if !withWill {
				c.will = nil
			}
			c.lost = false
			c.mu.Unlock()
			c.close(withWill)
			return
		default:
			c.broker.log.Debug("gmqx ignoring packet type", "type", msg.Type())
		}
	}
}

// failProtocol tells an MQTT 5 client why the connection is being closed. An
// MQTT 3.1.1 client cannot be told: the protocol has no server DISCONNECT, so
// the connection is simply closed. Transport errors (a closed connection or an
// expired keep-alive deadline) are not protocol errors and stay silent.
func (c *client) failProtocol(err error) {
	if c.version != mqtt.Version5 {
		return
	}
	if !errors.Is(err, mqtt.ErrMalformedPacket) {
		return
	}
	reason := byte(0x81) // Malformed Packet
	if wire.KindOf(err) == wire.KindTooLarge {
		reason = 0x95 // Packet too large
	}
	c.sendDisconnect(reason)
}

// protocolError answers an MQTT 5 protocol violation with a DISCONNECT and ends
// the connection. MQTT 3.1.1 closes the connection without a packet.
func (c *client) protocolError(reason byte) {
	if c.version == mqtt.Version5 {
		c.sendDisconnect(reason)
	}
	c.close(true)
}

func (c *client) sendDisconnect(reason byte) {
	_ = c.writeMessage(&mqtt.Disconnect{MessageHeader: c.header(), ReasonCode: reason})
}

// handlePubrel releases an inbound QoS 2 publish: the client waits for the
// PUBCOMP before it reuses the packet id. Completing the flow forgets the packet
// id, so the next PUBLISH that reuses it is delivered as a new message.
func (c *client) handlePubrel(m *mqtt.Pubrel) {
	c.mu.Lock()
	delete(c.inboundQoS2, m.PacketID)
	c.mu.Unlock()
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
		if alias > topicAliasMaximum {
			// The alias is above the Topic Alias Maximum this broker advertised.
			c.protocolError(0x94)
			return
		}
		if topic == "" {
			if alias == 0 {
				c.protocolError(0x82) // empty topic and no alias to resolve
				return
			}
			resolved, ok := c.topicAliases[alias]
			if !ok {
				c.protocolError(0x82) // alias that was never created
				return
			}
			topic = resolved
		} else if alias != 0 {
			c.topicAliases[alias] = topic
		}
		var err error
		if wireProps, err = encodePropertyBlock(props); err != nil {
			c.protocolError(0x81)
			return
		}
	}
	if !validTopic(topic) {
		// Empty or wildcard topic name in a PUBLISH is a protocol violation.
		c.protocolError(0x90)
		return
	}
	if m.QoS == 2 {
		// A retransmitted QoS 2 PUBLISH while the first one is still pending
		// (publisher waiting for PUBREL) must be acknowledged again but not
		// delivered again.
		c.mu.Lock()
		_, pending := c.inboundQoS2[m.PacketID]
		c.mu.Unlock()
		if pending {
			_ = c.writePublishAck(2, m.PacketID, 0)
			return
		}
	}
	res := c.broker.publish(c, PublishEvent{
		ConnectionID: c.connID,
		ClientID:     c.id,
		Topic:        topic,
		Payload:      m.Payload,
		QoS:          m.QoS,
		PacketID:     m.PacketID,
		Retain:       m.Retain,
		Dup:          m.Dup,
		Properties:   wireProps,
	}, props)
	switch m.QoS {
	case 1:
		c.ackPublish(m.QoS, m.PacketID, res)
	case 2:
		if res.Accepted() {
			c.mu.Lock()
			c.inboundQoS2[m.PacketID] = struct{}{}
			c.mu.Unlock()
		}
		c.ackPublish(m.QoS, m.PacketID, res)
	}
}

func (c *client) handleSubscribe(m *mqtt.Subscribe) {
	codes := make([]byte, 0, len(m.Subscriptions))
	existed := make([]bool, 0, len(m.Subscriptions))
	for _, req := range m.Subscriptions {
		if !validFilter(req.Filter) {
			// MQTT 3.1.1 has no per-filter rejection code for a malformed
			// filter, so the protocol violation closes the connection. MQTT 5
			// answers it with Topic Filter invalid and keeps the session.
			if c.version != mqtt.Version5 {
				c.protocolError(0)
				return
			}
			codes = append(codes, 0x8f)
			existed = append(existed, false)
			continue
		}
		code, had := c.broker.subscribe(c, req.Filter, subscription{
			qos:               req.QoS,
			noLocal:           req.NoLocal,
			retainAsPublished: req.RetainAsPublished,
			retainHandling:    req.RetainHandling,
		})
		codes = append(codes, code)
		existed = append(existed, had)
	}
	_ = c.writeMessage(&mqtt.Suback{
		MessageHeader: c.header(),
		PacketID:      m.PacketID,
		ReasonCodes:   codes,
	})
	for i, req := range m.Subscriptions {
		if codes[i] > 2 { // 0, 1 and 2 are the granted QoS values
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
		if c.outPID == 0 {
			// Packet id 0 is not a valid identifier.
			c.outPID = 1
		}
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
	if code == 0 {
		ack.SessionPresent = c.sessionPresent
	}
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
		props := willPublishProperties(will.Properties)
		wireProps, err := encodePropertyBlock(props)
		if err != nil {
			// A will whose properties do not belong on a PUBLISH is published
			// without them rather than not at all.
			props, wireProps = mqtt.Properties{}, nil
		}
		_ = c.broker.publish(c, PublishEvent{
			Topic:      will.Topic,
			Payload:    will.Payload,
			QoS:        will.QoS,
			Retain:     will.Retain,
			Properties: wireProps,
		}, props)
	}
	// The will exchange above still runs on a live connection context; once the
	// connection is gone, every later observer (the disconnect hooks) sees the
	// cancelled context.
	c.stopContext()
}

// willPublishProperties maps the will property block onto the PUBLISH properties
// the will message carries when it is published. Will Delay Interval is not a
// PUBLISH property: gmqx publishes the will as soon as the connection ends.
func willPublishProperties(p mqtt.Properties) mqtt.Properties {
	return mqtt.Properties{
		PayloadFormat:         p.PayloadFormat,
		MessageExpiryInterval: p.MessageExpiryInterval,
		ContentType:           p.ContentType,
		ResponseTopic:         p.ResponseTopic,
		CorrelationData:       p.CorrelationData,
		UserProperties:        p.UserProperties,
	}
}

func connackNotAuthorized(version byte) byte {
	if version == mqtt.Version5 {
		return 0x87
	}
	return 5
}

func connackTopicNameInvalid(version byte) byte {
	if version == mqtt.Version5 {
		return 0x90
	}
	return 2
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
