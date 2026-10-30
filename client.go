package gmqx

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
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
	version      byte
	props        connectProperties
	topicAliases map[uint16]string
	outPID       uint16
}

func newClient(b *Broker, conn net.Conn) *client {
	return &client{
		broker:       b,
		connID:       nextConnectionID(),
		conn:         conn,
		r:            bufio.NewReaderSize(conn, 4096),
		subs:         make(map[string]subscription),
		topicAliases: make(map[uint16]string),
	}
}

func (c *client) handshake() error {
	header, payload, err := c.readPacket()
	if err != nil {
		return err
	}
	if header>>4 != 1 {
		return fmt.Errorf("%w: first packet is not CONNECT", ErrMalformedPacket)
	}
	info, version, props, err := parseConnect(payload)
	if err != nil {
		return err
	}
	c.version = version
	c.props = props
	info.RemoteAddr = c.conn.RemoteAddr().String()
	if err := c.broker.cfg.Auth.Authenticate(context.Background(), info); err != nil {
		_ = c.writeConnack(version, connackNotAuthorized(version), "")
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	assigned := ""
	if info.ClientID == "" {
		if version == 3 || !info.CleanSession {
			_ = c.writeConnack(version, connackClientIDInvalid(version), "")
			return fmt.Errorf("%w: empty client id is not allowed", ErrMalformedPacket)
		}
		info.ClientID = randomID()
		assigned = info.ClientID
	}
	if version == 3 && len(info.ClientID) > 23 {
		_ = c.writeConnack(version, connackClientIDInvalid(version), "")
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
			code = connackClientIDInvalid(version)
		case errors.Is(err, ErrMaxConnections):
			code = connackQuotaExceeded(version)
		}
		_ = c.writeConnack(version, code, "")
		return err
	}
	if err := c.writeConnack(version, 0, assigned); err != nil {
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

		header, payload, err := c.readPacket()
		if err != nil {
			c.mu.Lock()
			c.lost = true
			c.mu.Unlock()
			c.close(true)
			return
		}
		switch header >> 4 {
		case 3: // PUBLISH
			c.handlePublish(header, payload)
		case 6: // PUBREL
			if len(payload) >= 2 {
				pid := uint16(payload[0])<<8 | uint16(payload[1])
				if c.version == 5 {
					_ = c.writePacket(0x70, []byte{byte(pid >> 8), byte(pid), 0, 0})
				} else {
					_ = c.writeAck(0x70, pid)
				}
			}
		case 8: // SUBSCRIBE
			c.handleSubscribe(payload)
		case 10: // UNSUBSCRIBE
			c.handleUnsubscribe(payload)
		case 12: // PINGREQ
			_ = c.writePacket(0xD0, nil)
			if c.broker.cfg.Sink != nil {
				c.broker.cfg.Sink.OnPing(context.Background(), c.id)
			}
		case 14: // DISCONNECT
			c.mu.Lock()
			c.will = nil
			c.lost = false
			c.mu.Unlock()
			c.close(false)
			return
		default:
			c.broker.log.Debug("gmqx ignoring packet type", "type", header>>4)
		}
	}
}

func (c *client) handlePublish(header byte, payload []byte) {
	qos := (header >> 1) & 0x3
	retain := header&0x1 == 1
	topic, rest, err := readString(payload)
	if err != nil {
		return
	}
	var pid uint16
	if qos > 0 {
		pid, rest, err = readUint16(rest)
		if err != nil {
			return
		}
	}
	var props []byte
	if c.version == 5 {
		block, next, err := readPropertyBlock(rest)
		if err != nil {
			return
		}
		var alias *uint16
		props, alias, err = filterPublishProperties(block)
		if err != nil {
			return
		}
		rest = next
		if topic == "" {
			if alias == nil {
				return
			}
			resolved, ok := c.topicAliases[*alias]
			if !ok {
				return
			}
			topic = resolved
		} else if alias != nil {
			c.topicAliases[*alias] = topic
		}
	}
	if !validTopic(topic) {
		return
	}
	res := c.broker.publish(c, PublishEvent{
		ConnectionID: c.connID,
		ClientID:     c.id,
		Topic:        topic,
		Payload:      rest,
		QoS:          qos,
		PacketID:     pid,
		Retain:       retain,
		Properties:   props,
	})
	switch qos {
	case 1:
		c.ackPublish(0x40, pid, res)
	case 2:
		c.ackPublish(0x50, pid, res)
	}
}

func (c *client) handleSubscribe(payload []byte) {
	pid, rest, err := readUint16(payload)
	if err != nil {
		return
	}
	if c.version == 5 {
		block, next, err := readPropertyBlock(rest)
		if err != nil {
			return
		}
		_ = block
		rest = next
	}
	type req struct {
		filter string
		sub    subscription
	}
	var reqs []req
	for len(rest) > 0 {
		filter, next, err := readString(rest)
		if err != nil || len(next) < 1 {
			return
		}
		opts := next[0]
		sub := subscription{qos: opts & 0x03}
		if c.version == 5 {
			sub.noLocal = opts&0x04 != 0
			sub.retainAsPublished = opts&0x08 != 0
			sub.retainHandling = (opts >> 4) & 0x03
		}
		reqs = append(reqs, req{filter: filter, sub: sub})
		rest = next[1:]
	}
	codes := make([]byte, 0, len(reqs))
	existed := make([]bool, 0, len(reqs))
	for _, r := range reqs {
		code, had := c.broker.subscribe(c, r.filter, r.sub)
		if c.version == 5 && code == 0x80 {
			code = 0x87
		}
		codes = append(codes, code)
		existed = append(existed, had)
	}
	suback := []byte{byte(pid >> 8), byte(pid)}
	if c.version == 5 {
		suback = append(suback, 0)
	}
	suback = append(suback, codes...)
	_ = c.writePacket(0x90, suback)
	for i, r := range reqs {
		if codes[i] == 0x80 || codes[i] == 0x87 {
			continue
		}
		if r.sub.retainHandling == 2 || (r.sub.retainHandling == 1 && existed[i]) {
			continue
		}
		c.broker.sendRetained(c, r.filter, r.sub.qos, r.sub.retainAsPublished)
	}
}

func (c *client) handleUnsubscribe(payload []byte) {
	pid, rest, err := readUint16(payload)
	if err != nil {
		return
	}
	if c.version == 5 {
		block, next, err := readPropertyBlock(rest)
		if err != nil {
			return
		}
		_ = block
		rest = next
	}
	for len(rest) > 0 {
		filter, next, err := readString(rest)
		if err != nil {
			return
		}
		c.broker.unsubscribe(c, filter)
		rest = next
	}
	if c.version == 5 {
		_ = c.writePacket(0xB0, []byte{byte(pid >> 8), byte(pid), 0})
	} else {
		_ = c.writeAck(0xB0, pid)
	}
}

func (c *client) sendPublish(topic string, payload []byte, qos byte, retain bool, props []byte) error {
	header := byte(0x30) | (qos << 1)
	if retain {
		header |= 1
	}
	body := appendString(nil, topic)
	if qos > 0 {
		c.mu.Lock()
		c.outPID++
		pid := c.outPID
		c.mu.Unlock()
		body = append(body, byte(pid>>8), byte(pid))
	}
	if c.version == 5 {
		body = append(body, prefixPropertyLength(props)...)
	}
	body = append(body, payload...)
	return c.writePacket(header, body)
}

func (c *client) writeConnack(version, code byte, assignedClientID string) error {
	if version == 5 {
		var props []byte
		if code == 0 {
			props = buildConnackProperties(assignedClientID, uint32(c.broker.cfg.MaxPacketSize), 16)
		} else {
			props = []byte{0}
		}
		return c.writePacket(0x20, append([]byte{0, code}, props...))
	}
	return c.writePacket(0x20, []byte{0, code})
}

func (c *client) writeAck(header byte, pid uint16) error {
	return c.writePacket(header, []byte{byte(pid >> 8), byte(pid)})
}

func (c *client) writeAckReason(header byte, pid uint16, reason byte) error {
	return c.writePacket(header, []byte{byte(pid >> 8), byte(pid), reason, 0})
}

func (c *client) writePacket(header byte, body []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return writePacket(c.conn, header, body)
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
		_ = c.broker.publish(c, PublishEvent{Topic: will.topic, Payload: will.payload})
	}
}

func connackNotAuthorized(version byte) byte {
	if version == 5 {
		return 0x87
	}
	return 5
}

func connackClientIDInvalid(version byte) byte {
	if version == 5 {
		return 0x85
	}
	return 2
}

func connackQuotaExceeded(version byte) byte {
	if version == 5 {
		return 0x97
	}
	return 5
}

// ackPublish acknowledges an accepted publish. A retryable or rejected publish
// gets no success ACK: MQTT 3.1.1 has no negative PUBACK, and an optimistic ACK
// would claim durability the broker does not have. MQTT 5 clients get the reason
// code instead.
func (c *client) ackPublish(header byte, pid uint16, res PublishResult) {
	if res.Accepted() {
		_ = c.writeAck(header, pid)
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
	if c.version == 5 {
		_ = c.writeAckReason(header, pid, code)
	}
}

var connSeq atomic.Uint64

// nextConnectionID returns a process-unique connection identifier.
func nextConnectionID() ConnectionID {
	return ConnectionID(strconv.FormatUint(connSeq.Add(1), 10))
}
