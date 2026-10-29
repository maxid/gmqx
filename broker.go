// Package gmqx implements a small MQTT 3.1.1 broker used by XDMP.
//
// The broker is a general-purpose library: it has no knowledge of XDMP
// business concepts. Device authentication/ACL/telemetry semantics belong to
// the calling application (for XDMP: modules/protocol/mqtt adapter).
package gmqx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Config configures a Broker.
type Config struct {
	// Addr is the TCP listen address, e.g. ":1883" or "127.0.0.1:0".
	Addr string
	// MaxPacketSize limits incoming packet size in bytes (default 4 MiB).
	MaxPacketSize int
	// Auth authenticates connecting clients. Defaults to AllowAll.
	Auth Authenticator
	// ACL authorizes subscribe/publish. Defaults to AllowAll.
	ACL ACL
	// Sink receives lifecycle/message events. Optional.
	Sink EventSink
	// Logger is optional.
	Logger *slog.Logger
}

// Broker is a single-node MQTT 3.1.1 broker.
type Broker struct {
	cfg Config
	log *slog.Logger

	mu       sync.RWMutex
	clients  map[string]*client
	retained map[string]retainedMessage
	ln       net.Listener
	done     chan struct{}
	wg       sync.WaitGroup
	started  bool
}

type retainedMessage struct {
	topic   string
	payload []byte
	qos     byte
}

// New creates a Broker. Start must be called to listen.
func New(cfg Config) (*Broker, error) {
	if cfg.Addr == "" {
		cfg.Addr = ":1883"
	}
	if cfg.MaxPacketSize <= 0 {
		cfg.MaxPacketSize = 4 << 20
	}
	if cfg.Auth == nil {
		cfg.Auth = AllowAll{}
	}
	if cfg.ACL == nil {
		cfg.ACL = AllowAll{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Broker{
		cfg:      cfg,
		log:      logger,
		clients:  make(map[string]*client),
		retained: make(map[string]retainedMessage),
		done:     make(chan struct{}),
	}, nil
}

// Start begins listening. It returns once the listener is ready.
func (b *Broker) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.started {
		b.mu.Unlock()
		return errors.New("gmqx: broker already started")
	}
	ln, err := net.Listen("tcp", b.cfg.Addr)
	if err != nil {
		b.mu.Unlock()
		return fmt.Errorf("gmqx: listen %s: %w", b.cfg.Addr, err)
	}
	b.ln = ln
	b.started = true
	b.mu.Unlock()

	b.log.Info("gmqx broker listening", "addr", ln.Addr().String())
	b.wg.Add(1)
	go b.acceptLoop()
	go func() {
		select {
		case <-ctx.Done():
			_ = b.Stop(context.Background())
		case <-b.done:
		}
	}()
	return nil
}

// Addr returns the actual listen address (useful when Addr uses port 0).
func (b *Broker) Addr() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.ln == nil {
		return ""
	}
	return b.ln.Addr().String()
}

// Stop closes the listener and all client connections.
func (b *Broker) Stop(ctx context.Context) error {
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return nil
	}
	b.started = false
	close(b.done)
	ln := b.ln
	clients := make([]*client, 0, len(b.clients))
	for _, c := range b.clients {
		clients = append(clients, c)
	}
	b.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	for _, c := range clients {
		c.close(false)
	}
	wait := make(chan struct{})
	go func() { b.wg.Wait(); close(wait) }()
	select {
	case <-wait:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
	}
	return nil
}

func (b *Broker) acceptLoop() {
	defer b.wg.Done()
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			select {
			case <-b.done:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				continue
			}
			return
		}
		b.wg.Add(1)
		go b.serve(conn)
	}
}

func (b *Broker) serve(conn net.Conn) {
	defer b.wg.Done()
	c := newClient(b, conn)
	if err := c.handshake(); err != nil {
		b.log.Debug("gmqx handshake failed", "remote", conn.RemoteAddr(), "err", err)
		_ = conn.Close()
		return
	}
	b.register(c)
	defer b.unregister(c)
	c.loop()
}

func (b *Broker) register(c *client) {
	b.mu.Lock()
	if old, ok := b.clients[c.id]; ok && old != c {
		b.mu.Unlock()
		old.close(false)
		b.mu.Lock()
	}
	b.clients[c.id] = c
	b.mu.Unlock()
	if b.cfg.Sink != nil {
		b.cfg.Sink.OnConnect(context.Background(), c.info)
	}
}

func (b *Broker) unregister(c *client) {
	b.mu.Lock()
	if current, ok := b.clients[c.id]; ok && current == c {
		delete(b.clients, c.id)
	}
	b.mu.Unlock()
	if b.cfg.Sink != nil {
		b.cfg.Sink.OnDisconnect(context.Background(), c.id, c.closeErr)
	}
}

func (b *Broker) subscribe(c *client, filter string, qos byte) byte {
	if !b.cfg.ACL.CanSubscribe(context.Background(), c.id, filter) {
		return 0x80
	}
	c.mu.Lock()
	c.subs[filter] = qos
	c.mu.Unlock()
	return qos
}

func (b *Broker) unsubscribe(c *client, filter string) {
	c.mu.Lock()
	delete(c.subs, filter)
	c.mu.Unlock()
}

func (b *Broker) sendRetained(c *client, filter string, qos byte) {
	b.mu.RLock()
	var msgs []retainedMessage
	for topic, m := range b.retained {
		if matchTopic(filter, topic) {
			msgs = append(msgs, m)
		}
	}
	b.mu.RUnlock()
	for _, m := range msgs {
		_ = c.sendPublish(m.topic, m.payload, minQoS(qos, m.qos), true)
	}
}

func (b *Broker) publish(from *client, topic string, payload []byte, qos byte, retain bool) {
	if !b.cfg.ACL.CanPublish(context.Background(), from.id, topic) {
		return
	}
	if retain {
		b.mu.Lock()
		if len(payload) == 0 {
			delete(b.retained, topic)
		} else {
			b.retained[topic] = retainedMessage{topic: topic, payload: append([]byte(nil), payload...), qos: qos}
		}
		b.mu.Unlock()
	}
	if b.cfg.Sink != nil {
		b.cfg.Sink.OnPublish(context.Background(), from.id, topic, payload, qos)
	}
	b.mu.RLock()
	subs := make([]*client, 0)
	seen := make(map[*client]bool)
	for _, c := range b.clients {
		if c == from {
			continue
		}
		c.mu.Lock()
		granted := byte(0)
		found := false
		for filter, q := range c.subs {
			if matchTopic(filter, topic) {
				found = true
				if q > granted {
					granted = q
				}
			}
		}
		c.mu.Unlock()
		if found && !seen[c] {
			seen[c] = true
			subs = append(subs, c)
			_ = granted
		}
	}
	b.mu.RUnlock()
	for _, c := range subs {
		_ = c.sendPublish(topic, payload, qos, false)
	}
}

func minQoS(a, b byte) byte {
	if a < b {
		return a
	}
	return b
}

func randomID() string {
	buf := make([]byte, 8)
	_, _ = rand.Read(buf)
	return "gmqx-" + hex.EncodeToString(buf)
}
