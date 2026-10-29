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

	mu        sync.Mutex
	id        string
	info      ConnectInfo
	subs      map[string]byte
	keepAlive time.Duration
	closed    bool
	closeErr  error
	will      *retainedMessage
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
	info, err := parseConnect(payload)
	if err != nil {
		return err
	}
	info.RemoteAddr = c.conn.RemoteAddr().String()
	if err := c.broker.cfg.Auth.Authenticate(context.Background(), info); err != nil {
		_ = c.writeConnack(5)
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	if info.ClientID == "" {
		if !info.CleanSession {
			_ = c.writeConnack(2)
			return fmt.Errorf("%w: empty client id requires clean session", ErrMalformedPacket)
		}
		info.ClientID = randomID()
	}
	c.mu.Lock()
	c.id = info.ClientID
	c.info = info
	c.keepAlive = time.Duration(info.KeepAlive) * time.Second
	if info.WillTopic != "" {
		c.will = &retainedMessage{topic: info.WillTopic, payload: info.WillPayload}
	}
	c.mu.Unlock()
	if err := c.writeConnack(0); err != nil {
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
			if errors.Is(err, io.EOF) {
				c.close(true)
				return
			}
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
		case 14: // DISCONNECT
			c.mu.Lock()
			c.will = nil
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
	if !validTopic(topic) {
		return
	}
	switch qos {
	case 1:
		_ = c.writeAck(0x40, pid)
	case 2:
		_ = c.writeAck(0x50, pid)
	}
	c.broker.publish(c, topic, rest, qos, retain)
}

func (c *client) handleSubscribe(payload []byte) {
	pid, rest, err := readUint16(payload)
	if err != nil {
		return
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
		codes = append(codes, c.broker.subscribe(c, r.filter, r.qos))
	}
	_ = c.writePacket(0x90, append([]byte{byte(pid >> 8), byte(pid)}, codes...))
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
	for len(rest) > 0 {
		filter, next, err := readString(rest)
		if err != nil {
			return
		}
		c.broker.unsubscribe(c, filter)
		rest = next
	}
	_ = c.writeAck(0xB0, pid)
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
	body = append(body, payload...)
	return c.writePacket(header, body)
}

func (c *client) writeConnack(code byte) error {
	return c.writePacket(0x20, []byte{0, code})
}

func (c *client) writeAck(header byte, pid uint16) error {
	return c.writePacket(header, []byte{byte(pid >> 8), byte(pid)})
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
