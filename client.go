package gmqx

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type client struct {
	broker *Broker
	conn   net.Conn
	r      *bufio.Reader
	wmu    sync.Mutex

	mu         sync.Mutex
	id         string
	info       ConnectInfo
	subs       map[string]byte
	keepAlive  time.Duration
	closed     bool
	closeErr   error
	will       *retainedMessage
	username   string
	registered bool
	lost       bool
	version    byte
}

func newClient(b *Broker, conn net.Conn) *client {
	return &client{
		broker: b,
		conn:   conn,
		r:      bufio.NewReaderSize(conn, 4096),
		subs:   make(map[string]byte),
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
	info, version, err := parseConnect(payload)
	if err != nil {
		return err
	}
	c.version = version
	info.RemoteAddr = c.conn.RemoteAddr().String()
	if err := c.broker.cfg.Auth.Authenticate(context.Background(), info); err != nil {
		_ = c.writeConnack(version, connackNotAuthorized(version))
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	if info.ClientID == "" {
		if !info.CleanSession {
			_ = c.writeConnack(version, connackClientIDInvalid(version))
			return fmt.Errorf("%w: empty client id requires clean session", ErrMalformedPacket)
		}
		info.ClientID = randomID()
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
		_ = c.writeConnack(version, code)
		return err
	}
	if err := c.writeConnack(version, 0); err != nil {
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
			_ = io.EOF
			c.close(true)
			return
		}
		switch header >> 4 {
		case 3: // PUBLISH
			c.handlePublish(header, payload)
		case 6: // PUBREL
			if len(payload) >= 2 {
				pid := uint16(payload[0])<<8 | uint16(payload[1])
				_ = c.writeAck(0x70, pid)
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
	if err != nil || topic == "" {
		return
	}
	var pid uint16
	if qos > 0 {
		pid, rest, err = readUint16(rest)
		if err != nil {
			return
		}
	}
	if c.version == 5 {
		if rest, err = skipProperties(rest); err != nil {
			return
		}
	}
	if !validTopic(topic) {
		return
	}
	allowed := c.broker.publish(c, topic, rest, qos, retain)
	switch qos {
	case 1:
		if c.version == 5 && !allowed {
			_ = c.writeAckReason(0x40, pid, 0x87)
		} else {
			_ = c.writeAck(0x40, pid)
		}
	case 2:
		if c.version == 5 && !allowed {
			_ = c.writeAckReason(0x50, pid, 0x87)
		} else {
			_ = c.writeAck(0x50, pid)
		}
	}
}

func (c *client) handleSubscribe(payload []byte) {
	pid, rest, err := readUint16(payload)
	if err != nil {
		return
	}
	if c.version == 5 {
		if rest, err = skipProperties(rest); err != nil {
			return
		}
	}
	type req struct {
		filter string
		qos    byte
	}
	var reqs []req
	for len(rest) > 0 {
		filter, next, err := readString(rest)
		if err != nil || len(next) < 1 {
			return
		}
		q := next[0] & 0x3
		reqs = append(reqs, req{filter: filter, qos: q})
		rest = next[1:]
	}
	codes := make([]byte, 0, len(reqs))
	for _, r := range reqs {
		code := c.broker.subscribe(c, r.filter, r.qos)
		if c.version == 5 && code == 0x80 {
			code = 0x87 // Not authorized
		}
		codes = append(codes, code)
	}
	suback := []byte{byte(pid >> 8), byte(pid)}
	if c.version == 5 {
		suback = append(suback, 0) // property length 0
	}
	suback = append(suback, codes...)
	_ = c.writePacket(0x90, suback)
	for i, r := range reqs {
		if codes[i] != 0x80 {
			c.broker.sendRetained(c, r.filter, r.qos)
		}
	}
}

func (c *client) handleUnsubscribe(payload []byte) {
	pid, rest, err := readUint16(payload)
	if err != nil {
		return
	}
	if c.version == 5 {
		if rest, err = skipProperties(rest); err != nil {
			return
		}
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

func (c *client) sendPublish(topic string, payload []byte, qos byte, retain bool) error {
	header := byte(0x30) | (qos << 1)
	if retain {
		header |= 1
	}
	body := appendString(nil, topic)
	if qos > 0 {
		pid := uint16(1)
		body = append(body, byte(pid>>8), byte(pid))
	}
	if c.version == 5 {
		body = append(body, 0) // property length 0
	}
	body = append(body, payload...)
	return c.writePacket(header, body)
}

func (c *client) writeConnack(version, code byte) error {
	if version == 5 {
		return c.writePacket(0x20, []byte{0, code, 0}) // session present, reason, properties
	}
	return c.writePacket(0x20, []byte{0, code})
}

func (c *client) writeAck(header byte, pid uint16) error {
	return c.writePacket(header, []byte{byte(pid >> 8), byte(pid)})
}

func (c *client) writeAckReason(header byte, pid uint16, reason byte) error {
	return c.writePacket(header, []byte{byte(pid >> 8), byte(pid), reason, 0})
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
		c.broker.publish(c, will.topic, will.payload, 0, false)
	}
}

func (c *client) publishWill() {
	c.mu.Lock()
	will := c.will
	c.mu.Unlock()
	if will != nil {
		c.broker.publish(c, will.topic, will.payload, 0, false)
	}
}
