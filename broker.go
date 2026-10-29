// Package gmqx implements a small, embeddable MQTT 3.1.1 broker.
//
// The broker is protocol-focused: authentication, ACL, message handling and
// metrics are pluggable.
package gmqx

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Broker is a single-node MQTT 3.1.1 broker.
type Broker struct {
	cfg     Config
	log     *slog.Logger
	metrics MetricsManager
	limiter *rateLimiter

	mu          sync.RWMutex
	clients     map[string]*client
	retained    map[string]retainedMessage
	listeners   []net.Listener
	httpServers []*http.Server
	addrs       map[string]string
	done        chan struct{}
	wg          sync.WaitGroup
	started     bool
	closed      bool
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
		addrs:    make(map[string]string),
		done:     make(chan struct{}),
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

	var errs []error
	started := 0

	if addr := b.cfg.Addr; addr != "" {
		if err := b.startListener("tcp", addr, nil); err != nil {
			errs = append(errs, err)
		} else {
			started++
		}
	}
	if b.cfg.TLS.Enabled {
		addr := addrOrPort(b.cfg.SecureAddr, b.cfg.SecurePort)
		if addr == "" {
			addr = ":8883"
		}
		tlsCfg, err := b.tlsConfig()
		if err != nil {
			errs = append(errs, err)
		} else if err := b.startListener("tls", addr, tlsCfg); err != nil {
			errs = append(errs, err)
		} else {
			started++
		}
	}
	if addr := addrOrPort(b.cfg.WebsocketAddr, b.cfg.WebsocketPort); addr != "" {
		if err := b.startWS("ws", addr, nil); err != nil {
			errs = append(errs, err)
		} else {
			started++
		}
	}
	if addr := addrOrPort(b.cfg.WebsocketSecureAddr, b.cfg.WebsocketSecurePort); addr != "" {
		if !b.cfg.TLS.Enabled {
			errs = append(errs, errors.New("gmqx: WSS requires TLS.Enabled and certificate"))
		} else if tlsCfg, err := b.tlsConfig(); err != nil {
			errs = append(errs, err)
		} else if err := b.startWS("wss", addr, tlsCfg); err != nil {
			errs = append(errs, err)
		} else {
			started++
		}
	}
	if len(errs) > 0 {
		_ = b.Stop(context.Background())
		return errors.Join(errs...)
	}
	if started == 0 {
		_ = b.Stop(context.Background())
		return errors.New("gmqx: no listeners configured")
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

func addrOrPort(addr string, port int) string {
	if addr != "" {
		return addr
	}
	if port > 0 {
		return fmt.Sprintf(":%d", port)
	}
	return ""
}

func (b *Broker) startListener(name, addr string, tlsCfg *tls.Config) error {
	var (
		ln  net.Listener
		err error
	)
	if tlsCfg != nil {
		ln, err = tls.Listen("tcp", addr, tlsCfg)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("gmqx: %s listen %s: %w", name, addr, err)
	}
	b.mu.Lock()
	b.listeners = append(b.listeners, ln)
	b.addrs[name] = ln.Addr().String()
	b.mu.Unlock()
	b.log.Info("gmqx listener started", "transport", name, "addr", ln.Addr().String())
	b.wg.Add(1)
	go b.acceptLoop(ln)
	return nil
}

func (b *Broker) startWS(name, addr string, tlsCfg *tls.Config) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("gmqx: %s listen %s: %w", name, addr, err)
	}
	if tlsCfg != nil {
		ln = tls.NewListener(ln, tlsCfg)
	}
	srv := &http.Server{Handler: b.wsHandler(), ReadHeaderTimeout: 10 * time.Second}
	b.mu.Lock()
	b.listeners = append(b.listeners, ln)
	b.httpServers = append(b.httpServers, srv)
	b.addrs[name] = ln.Addr().String()
	b.mu.Unlock()
	b.log.Info("gmqx listener started", "transport", name, "addr", ln.Addr().String(), "path", b.cfg.WebsocketPath)
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		_ = srv.Serve(ln)
	}()
	return nil
}

func (b *Broker) wsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if b.cfg.WebsocketPath != "" && r.URL.Path != b.cfg.WebsocketPath {
			http.NotFound(w, r)
			return
		}
		if !strings.Contains(strings.ToLower(r.Header.Get("Sec-WebSocket-Protocol")), "mqtt") {
			http.Error(w, "missing mqtt WebSocket subprotocol", http.StatusBadRequest)
			return
		}
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols:       []string{"mqtt"},
			InsecureSkipVerify: true, // broker accepts clients from arbitrary origins
		})
		if err != nil {
			return
		}
		c.SetReadLimit(int64(b.cfg.MaxPacketSize))
		conn := websocket.NetConn(context.Background(), c, websocket.MessageBinary)
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.serve(conn)
		}()
	})
}

func (b *Broker) tlsConfig() (*tls.Config, error) {
	if b.cfg.TLS.Cert == "" || b.cfg.TLS.Key == "" {
		return nil, errors.New("gmqx: TLS requires both Cert and Key")
	}
	cert, err := tls.LoadX509KeyPair(b.cfg.TLS.Cert, b.cfg.TLS.Key)
	if err != nil {
		return nil, fmt.Errorf("gmqx: load TLS key pair: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if b.cfg.TLS.CA != "" {
		pem, err := os.ReadFile(b.cfg.TLS.CA)
		if err != nil {
			return nil, fmt.Errorf("gmqx: read TLS CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("gmqx: invalid TLS CA PEM")
		}
		cfg.ClientCAs = pool
		if b.cfg.TLS.RequireClientCert {
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}
	return cfg, nil
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
	listeners := append([]net.Listener(nil), b.listeners...)
	servers := append([]*http.Server(nil), b.httpServers...)
	clients := make([]*client, 0, len(b.clients))
	for _, c := range b.clients {
		clients = append(clients, c)
	}
	b.mu.Unlock()

	for _, srv := range servers {
		_ = srv.Close()
	}
	for _, ln := range listeners {
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

func (b *Broker) acceptLoop(ln net.Listener) {
	defer b.wg.Done()
	for {
		conn, err := ln.Accept()
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
		go func() {
			defer b.wg.Done()
			b.serve(conn)
		}()
	}
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

func (b *Broker) publish(from *client, topic string, payload []byte, qos byte, retain bool) bool {
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
	return true
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
