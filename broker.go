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

	"github.com/maxid/gnety"
)

// Broker is a single-node MQTT 3.1.1 broker.
type Broker struct {
	cfg     Config
	log     *slog.Logger
	metrics MetricsManager
	limiter *rateLimiter
	nodeID  string

	mu       sync.RWMutex
	clients  map[string]*client
	retained map[string]retainedMessage
	servers  []*gnety.Server
	addrs    map[string]string
	done     chan struct{}
	started  bool
	closed   bool
}

type retainedMessage struct {
	topic   string
	payload []byte
	qos     byte
	props   []byte
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
	if cfg.NodeID == "" {
		cfg.NodeID = randomID()
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
		addrs:    make(map[string]string),
		done:     make(chan struct{}),
		nodeID:   cfg.NodeID,
	}
	if cfg.ConnectionRateLimit > 0 {
		b.limiter = newRateLimiter(cfg.ConnectionRateLimit)
	}
	return b, nil
}

// Start begins all configured listeners. It returns once they are ready.
func (b *Broker) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.started {
		b.mu.Unlock()
		return errors.New("gmqx: broker already started")
	}
	if b.closed {
		b.mu.Unlock()
		return errors.New("gmqx: broker is closed")
	}
	b.started = true
	b.mu.Unlock()

	specs, err := b.listenerSpecs()
	if err != nil {
		_ = b.Stop(context.Background())
		return err
	}
	var errs []error
	var started []*gnety.Server
	for _, spec := range specs {
		srv, err := gnety.New(gnety.Config{
			Network:     spec.network,
			Addr:        spec.addr,
			Path:        spec.path,
			Subprotocol: spec.subprotocol,
			TLS:         spec.tls,
			RawHandler:  b.handleRaw,
		})
		if err == nil {
			err = srv.Start(ctx)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("gmqx: %s listener: %w", spec.name, err))
			for _, startedSrv := range started {
				_ = startedSrv.Stop(context.Background())
			}
			_ = b.Stop(context.Background())
			return errors.Join(errs...)
		}
		started = append(started, srv)
		b.mu.Lock()
		b.servers = append(b.servers, srv)
		b.addrs[spec.name] = srv.Addr()
		b.mu.Unlock()
		b.log.Info("gmqx listener started", "transport", spec.name, "addr", srv.Addr())
	}
	if len(started) == 0 {
		_ = b.Stop(context.Background())
		return errors.New("gmqx: no listeners configured")
	}
	if b.cfg.ClusterBackend != nil {
		if err := b.cfg.ClusterBackend.Start(ctx, b.nodeID, b); err != nil {
			_ = b.Stop(context.Background())
			return fmt.Errorf("gmqx: cluster start: %w", err)
		}
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = b.Stop(context.Background())
		case <-b.done:
		}
	}()
	return nil
}

type listenerSpec struct {
	name        string
	network     string
	addr        string
	path        string
	subprotocol string
	tls         gnety.TLSConfig
}

func (b *Broker) listenerSpecs() ([]listenerSpec, error) {
	var specs []listenerSpec
	if b.cfg.Addr != "" {
		specs = append(specs, listenerSpec{name: "tcp", network: "tcp", addr: b.cfg.Addr})
	}
	if b.cfg.TLS.Enabled {
		if b.cfg.TLS.Cert == "" || b.cfg.TLS.Key == "" {
			return nil, errors.New("gmqx: TLS requires both Cert and Key")
		}
		addr := addrOrPort(b.cfg.SecureAddr, b.cfg.SecurePort)
		if addr == "" {
			addr = ":8883"
		}
		specs = append(specs, listenerSpec{
			name:    "tls",
			network: "tcp-tls",
			addr:    addr,
			tls:     b.gnetyTLS(),
		})
	}
	if addr := addrOrPort(b.cfg.WebsocketAddr, b.cfg.WebsocketPort); addr != "" {
		specs = append(specs, listenerSpec{
			name:        "ws",
			network:     "ws",
			addr:        addr,
			path:        b.cfg.WebsocketPath,
			subprotocol: "mqtt",
		})
	}
	if addr := addrOrPort(b.cfg.WebsocketSecureAddr, b.cfg.WebsocketSecurePort); addr != "" {
		if !b.cfg.TLS.Enabled {
			return nil, errors.New("gmqx: WSS requires TLS.Enabled and certificate")
		}
		specs = append(specs, listenerSpec{
			name:        "wss",
			network:     "wss",
			addr:        addr,
			path:        b.cfg.WebsocketPath,
			subprotocol: "mqtt",
			tls:         b.gnetyTLS(),
		})
	}
	return specs, nil
}

func (b *Broker) gnetyTLS() gnety.TLSConfig {
	return gnety.TLSConfig{
		CertFile:          b.cfg.TLS.Cert,
		KeyFile:           b.cfg.TLS.Key,
		CAFile:            b.cfg.TLS.CA,
		RequireClientCert: b.cfg.TLS.RequireClientCert,
	}
}

func (b *Broker) handleRaw(conn net.Conn) {
	if b.limiter != nil && !b.limiter.allow() {
		b.metrics.RecordDroppedMessage("connection_rate_limit")
		_ = conn.Close()
		return
	}
	if b.cfg.MaxConnections > 0 {
		b.mu.RLock()
		n := len(b.clients)
		b.mu.RUnlock()
		if n >= b.cfg.MaxConnections {
			b.metrics.RecordDroppedMessage("max_connections")
			b.metrics.RecordOverflow("connections")
			_ = conn.Close()
			return
		}
	}
	b.serve(conn)
}

func addrOrPort(addr string, port int) string {
	if addr != "" {
		return addr
	}
	if port > 0 {
		return fmt.Sprintf(":%d", port)
	}
	return ""
}

// Addr returns the TCP listen address.
func (b *Broker) Addr() string { return b.addr("tcp") }

// SecureAddr returns the TLS listen address.
func (b *Broker) SecureAddr() string { return b.addr("tls") }

// WebsocketAddr returns the WebSocket listen address.
func (b *Broker) WebsocketAddr() string { return b.addr("ws") }

// WebsocketSecureAddr returns the WSS listen address.
func (b *Broker) WebsocketSecureAddr() string { return b.addr("wss") }

// Addrs returns a copy of all listening addresses by transport.
func (b *Broker) Addrs() map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]string, len(b.addrs))
	for k, v := range b.addrs {
		out[k] = v
	}
	return out
}

func (b *Broker) addr(name string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.addrs[name]
}

// Stop closes all listeners and client connections.
func (b *Broker) Stop(ctx context.Context) error {
	b.mu.Lock()
	if !b.started || b.closed {
		b.mu.Unlock()
		return nil
	}
	b.started = false
	b.closed = true
	close(b.done)
	servers := append([]*gnety.Server(nil), b.servers...)
	clients := make([]*client, 0, len(b.clients))
	for _, c := range b.clients {
		clients = append(clients, c)
	}
	b.mu.Unlock()

	var errs []error
	if b.cfg.ClusterBackend != nil {
		if err := b.cfg.ClusterBackend.Stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	for _, srv := range servers {
		if err := srv.Stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	for _, c := range clients {
		c.close(false)
	}
	return errors.Join(errs...)
}

func (b *Broker) serve(conn net.Conn) {
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

func (b *Broker) subscribe(c *client, filter string, sub subscription) (byte, bool) {
	if !b.cfg.ACL.Check(context.Background(), SessionInfo{ClientID: c.id, Username: c.username}, filter, AclSubscribe) {
		b.metrics.RecordDroppedMessage("acl_subscribe")
		return 0x80, false
	}
	c.mu.Lock()
	_, exists := c.subs[filter]
	if b.cfg.MaxTopicSubscriptions > 0 && len(c.subs) >= b.cfg.MaxTopicSubscriptions && !exists {
		c.mu.Unlock()
		b.metrics.RecordDroppedMessage("max_subscriptions")
		return 0x80, exists
	}
	c.subs[filter] = sub
	c.mu.Unlock()
	b.metrics.IncrementSubscribeEvents()
	return sub.qos, exists
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

func (b *Broker) sendRetained(c *client, filter string, qos byte, retainAsPublished bool) {
	b.mu.RLock()
	var msgs []retainedMessage
	for topic, m := range b.retained {
		if matchTopic(filter, topic) {
			msgs = append(msgs, m)
		}
	}
	b.mu.RUnlock()
	for _, m := range msgs {
		_ = c.sendPublish(m.topic, m.payload, minQoS(qos, m.qos), true, m.props)
	}
}

func (b *Broker) publish(from *client, topic string, payload []byte, qos byte, retain bool, props []byte) bool {
	if !b.cfg.ACL.Check(context.Background(), SessionInfo{ClientID: from.id, Username: from.username}, topic, AclPublish) {
		b.metrics.RecordDroppedMessage("acl_publish")
		return false
	}
	for _, it := range b.cfg.Interceptors {
		if !it.OnPublish(context.Background(), from.id, topic, payload, qos) {
			b.metrics.RecordDroppedMessage("interceptor")
			return false
		}
	}
	if retain {
		b.mu.Lock()
		if len(payload) == 0 {
			delete(b.retained, topic)
		} else {
			_, exists := b.retained[topic]
			if b.cfg.MaxRetainMessages <= 0 || exists || int64(len(b.retained)) < b.cfg.MaxRetainMessages {
				b.retained[topic] = retainedMessage{topic: topic, payload: append([]byte(nil), payload...), qos: qos, props: append([]byte(nil), props...)}
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

	b.deliverLocal(topic, payload, qos, from, props, retain)
	b.spreadPublish(topic, payload, qos, retain, props)
	return true
}

func (b *Broker) deliverLocal(topic string, payload []byte, qos byte, from *client, props []byte, retain bool) int {
	b.mu.RLock()
	type target struct {
		client *client
		rap    bool
	}
	targets := make([]target, 0)
	for _, c := range b.clients {
		c.mu.Lock()
		found := false
		rap := false
		for filter, sub := range c.subs {
			if !matchTopic(filter, topic) {
				continue
			}
			if sub.noLocal && c == from {
				continue
			}
			found = true
			if sub.retainAsPublished {
				rap = true
			}
		}
		c.mu.Unlock()
		if found {
			targets = append(targets, target{client: c, rap: rap})
		}
	}
	b.mu.RUnlock()
	for _, t := range targets {
		ret := false
		outProps := props
		if t.rap && retain {
			ret = true
		}
		_ = t.client.sendPublish(topic, payload, qos, ret, outProps)
	}
	return len(targets)
}

func (b *Broker) spreadPublish(topic string, payload []byte, qos byte, retain bool, props []byte) {
	if b.cfg.ClusterBackend == nil {
		return
	}
	msg := ClusterMessage{
		Type: ClusterPublish, NodeID: b.nodeID, Topic: topic,
		Payload: append([]byte(nil), payload...), QoS: qos, Retain: retain,
		Props: append([]byte(nil), props...),
	}
	if err := b.cfg.ClusterBackend.Spread(context.Background(), msg); err != nil {
		b.log.Warn("gmqx cluster spread failed", "err", err)
	}
}

// OnRemotePublish implements ClusterHandler.
func (b *Broker) OnRemotePublish(msg ClusterMessage) {
	b.deliverLocal(msg.Topic, msg.Payload, msg.QoS, nil, msg.Props, msg.Retain)
}

// OnRemoteKick implements ClusterHandler.
func (b *Broker) OnRemoteKick(msg ClusterMessage) {
	b.mu.RLock()
	c := b.clients[msg.ClientID]
	b.mu.RUnlock()
	if c != nil {
		c.close(false)
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
