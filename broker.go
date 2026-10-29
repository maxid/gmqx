// Package gmqx implements a small, embeddable MQTT 3.1.1 broker.
//
// The broker is protocol-focused: authentication, ACL, message handling and
// metrics are pluggable.
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

// Broker is a single-node MQTT 3.1.1 broker.
type Broker struct {
	cfg     Config
	log     *slog.Logger
	metrics MetricsManager
	limiter *rateLimiter

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
		port := cfg.Port
		if port == 0 {
			port = 1883
		}
		cfg.Addr = fmt.Sprintf(":%d", port)
	}
	if cfg.MaxPacketSize <= 0 {
		cfg.MaxPacketSize = 4 << 20
	}
	if cfg.ConnectMode == "" {
		cfg.ConnectMode = ConnectModeUnique
	}
	if cfg.NotKickSeconds <= 0 {
		cfg.NotKickSeconds = 30
	}
	if cfg.Auth == nil {
		cfg.Auth = AllowAll{}
	}
	if cfg.ACL == nil {
		cfg.ACL = AllowAll{}
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NoopMetrics{}
	}
	if cfg.WebsocketPath == "" {
		cfg.WebsocketPath = "/mqtt"
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	b := &Broker{
		cfg:      cfg,
		log:      logger,
		metrics:  cfg.Metrics,
		clients:  make(map[string]*client),
		retained: make(map[string]retainedMessage),
		done:     make(chan struct{}),
	}
	if cfg.ConnectionRateLimit > 0 {
		b.limiter = newRateLimiter(cfg.ConnectionRateLimit)
	}
	if cfg.TLS.Enabled || cfg.SecurePort > 0 || cfg.WebsocketPort > 0 || cfg.WebsocketSecurePort > 0 {
		logger.Warn("gmqx: TLS/WS listeners are configured but not implemented yet", "secure_port", cfg.SecurePort, "ws_port", cfg.WebsocketPort)
	}
	return b, nil
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
		if b.limiter != nil && !b.limiter.allow() {
			b.metrics.RecordDroppedMessage("connection_rate_limit")
			_ = conn.Close()
			continue
		}
		if b.cfg.MaxConnections > 0 {
			b.mu.RLock()
			n := len(b.clients)
			b.mu.RUnlock()
			if n >= b.cfg.MaxConnections {
				b.metrics.RecordDroppedMessage("max_connections")
				b.metrics.RecordOverflow("connections")
				_ = conn.Close()
				continue
			}
		}
		b.wg.Add(1)
		go b.serve(conn)
	}
}

func (b *Broker) serve(conn net.Conn) {
	defer b.wg.Done()
	c := newClient(b, conn)
	if err := c.handshake(); err != nil {
		if c.registered {
			b.unregister(c)
		}
		b.log.Debug("gmqx handshake failed", "remote", conn.RemoteAddr(), "err", err)
		_ = conn.Close()
		return
	}
	defer b.unregister(c)
	c.loop()
}

// register enforces connect mode and connection limits, then records the
// session and calls the dispatcher.
func (b *Broker) register(c *client) error {
	b.mu.Lock()
	if old, ok := b.clients[c.id]; ok && old != c {
		if b.cfg.ConnectMode == ConnectModeUnique {
			b.mu.Unlock()
			return ErrClientIDInUse
		}
		delete(b.clients, c.id)
		b.mu.Unlock()
		old.close(false)
		b.mu.Lock()
	}
	if b.cfg.MaxConnections > 0 && len(b.clients) >= b.cfg.MaxConnections {
		b.mu.Unlock()
		return ErrMaxConnections
	}
	b.clients[c.id] = c
	c.registered = true
	n := len(b.clients)
	b.mu.Unlock()

	b.metrics.IncrementConnections()
	b.metrics.RecordConnections(n)
	if b.cfg.Sink != nil {
		b.cfg.Sink.OnConnect(context.Background(), c.info)
	}
	return nil
}

func (b *Broker) unregister(c *client) {
	if !c.registered {
		return
	}
	c.registered = false
	b.mu.Lock()
	if current, ok := b.clients[c.id]; ok && current == c {
		delete(b.clients, c.id)
	}
	n := len(b.clients)
	b.mu.Unlock()

	b.metrics.DecrementConnections()
	b.metrics.IncrementDisconnectEvents()
	b.metrics.RecordConnections(n)
	if b.cfg.Sink != nil {
		if c.lost {
			b.cfg.Sink.OnConnectionLost(context.Background(), c.id, c.closeErr)
		} else {
			b.cfg.Sink.OnDisconnect(context.Background(), c.id, c.closeErr)
		}
	}
}

func (b *Broker) subscribe(c *client, filter string, qos byte) byte {
	if !b.cfg.ACL.Check(context.Background(), SessionInfo{ClientID: c.id, Username: c.username}, filter, AclSubscribe) {
		b.metrics.RecordDroppedMessage("acl_subscribe")
		return 0x80
	}
	c.mu.Lock()
	if b.cfg.MaxTopicSubscriptions > 0 && len(c.subs) >= b.cfg.MaxTopicSubscriptions {
		if _, exists := c.subs[filter]; !exists {
			c.mu.Unlock()
			b.metrics.RecordDroppedMessage("max_subscriptions")
			return 0x80
		}
	}
	c.subs[filter] = qos
	c.mu.Unlock()
	b.metrics.IncrementSubscribeEvents()
	return qos
}

func (b *Broker) unsubscribe(c *client, filter string) {
	c.mu.Lock()
	_, existed := c.subs[filter]
	delete(c.subs, filter)
	c.mu.Unlock()
	if existed {
		b.metrics.DecrementSubscribeEvents()
	}
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
	if !b.cfg.ACL.Check(context.Background(), SessionInfo{ClientID: from.id, Username: from.username}, topic, AclPublish) {
		b.metrics.RecordDroppedMessage("acl_publish")
		return
	}
	for _, it := range b.cfg.Interceptors {
		if !it.OnPublish(context.Background(), from.id, topic, payload, qos) {
			b.metrics.RecordDroppedMessage("interceptor")
			return
		}
	}
	if retain {
		b.mu.Lock()
		if len(payload) == 0 {
			delete(b.retained, topic)
		} else {
			_, exists := b.retained[topic]
			if b.cfg.MaxRetainMessages <= 0 || exists || int64(len(b.retained)) < b.cfg.MaxRetainMessages {
				b.retained[topic] = retainedMessage{topic: topic, payload: append([]byte(nil), payload...), qos: qos}
			} else {
				b.metrics.RecordDroppedMessage("max_retain")
			}
		}
		b.mu.Unlock()
	}
	if b.cfg.Sink != nil {
		b.cfg.Sink.OnPublish(context.Background(), from.id, topic, payload, qos)
	}
	b.metrics.IncrementPublishedMessages()

	b.mu.RLock()
	targets := make([]*client, 0)
	for _, c := range b.clients {
		if c == from {
			continue
		}
		c.mu.Lock()
		found := false
		for filter := range c.subs {
			if matchTopic(filter, topic) {
				found = true
				break
			}
		}
		c.mu.Unlock()
		if found {
			targets = append(targets, c)
		}
	}
	b.mu.RUnlock()
	for _, c := range targets {
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

// rateLimiter is a simple token bucket used for connection rate limiting.
type rateLimiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newRateLimiter(rate int) *rateLimiter {
	return &rateLimiter{rate: float64(rate), burst: float64(rate), tokens: float64(rate), last: time.Now()}
}

func (r *rateLimiter) allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.tokens += now.Sub(r.last).Seconds() * r.rate
	if r.tokens > r.burst {
		r.tokens = r.burst
	}
	r.last = now
	if r.tokens < 1 {
		return false
	}
	r.tokens--
	return true
}
