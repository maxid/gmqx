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
	"sort"
	"sync"
	"time"

	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/mqtt"
)

// Broker is a single-node MQTT 3.1.1 broker.
type Broker struct {
	cfg        Config
	log        *slog.Logger
	metrics    MetricsManager
	limiter    *rateLimiter
	nodeID     string
	namespace  string
	dispatcher Dispatcher

	mu       sync.RWMutex
	ctx      context.Context // broker lifetime context, set by Start
	clients  map[string]*client
	sessions map[string]*session
	retained map[string]retainedMessage
	servers  []*gnety.Server
	addrs    map[string]string
	done     chan struct{}
	started  bool
	closed   bool
}

type retainedMessage struct {
	namespace string
	topic     string
	payload   []byte
	qos       byte
	props     mqtt.Properties
}

// session is the state a persistent session keeps while no connection owns it:
// the subscriptions the client asked the server to remember. A reconnecting
// client with the clean-session (MQTT 5: clean start) flag cleared resumes it and
// the CONNACK reports Session Present.
type session struct {
	subs map[string]subscription
}

func copySubscriptions(subs map[string]subscription) map[string]subscription {
	if len(subs) == 0 {
		return nil
	}
	out := make(map[string]subscription, len(subs))
	for filter, sub := range subs {
		out[filter] = sub
	}
	return out
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
		cfg:       cfg,
		log:       logger,
		metrics:   cfg.Metrics,
		namespace: cfg.Namespace,
		clients:   make(map[string]*client),
		sessions:  make(map[string]*session),
		retained:  make(map[string]retainedMessage),
		addrs:     make(map[string]string),
		done:      make(chan struct{}),
		nodeID:    cfg.NodeID,
	}
	b.dispatcher = newDispatcher(cfg.Dispatcher, b)
	if cfg.ConnectionRateLimit > 0 {
		b.limiter = newRateLimiter(cfg.ConnectionRateLimit)
	}
	return b, nil
}

// newDispatcher selects the routing policy and hands it the broker's routing
// seam. A nil Dispatcher selects DefaultDispatcher.
func newDispatcher(d Dispatcher, b *Broker) Dispatcher {
	if d == nil {
		d = NewDefaultDispatcher()
	}
	if binder, ok := d.(DispatcherBinder); ok {
		binder.Bind(b)
	}
	return d
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
	b.ctx = ctx
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
			Network:        spec.network,
			Addr:           spec.addr,
			Path:           spec.path,
			Subprotocol:    spec.subprotocol,
			AllowAnyOrigin: spec.allowAnyOrigin,
			TLS:            spec.tls,
			RawHandler:     b.handleRaw,
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
		if scoped, ok := b.cfg.ClusterBackend.(namespaceScoped); ok {
			// The broker's namespace is authoritative for a backend attached to
			// it: events this broker produces are stamped by the broker.
			scoped.setNamespace(b.namespace)
		}
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
	name           string
	network        string
	addr           string
	path           string
	subprotocol    string
	allowAnyOrigin bool
	tls            gnety.TLSConfig
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
			name:           "ws",
			network:        "ws",
			addr:           addr,
			path:           b.cfg.WebsocketPath,
			subprotocol:    "mqtt",
			allowAnyOrigin: true, // device gateways are not browser clients; Origin is not a security boundary
		})
	}
	if addr := addrOrPort(b.cfg.WebsocketSecureAddr, b.cfg.WebsocketSecurePort); addr != "" {
		if !b.cfg.TLS.Enabled {
			return nil, errors.New("gmqx: WSS requires TLS.Enabled and certificate")
		}
		specs = append(specs, listenerSpec{
			name:           "wss",
			network:        "wss",
			addr:           addr,
			path:           b.cfg.WebsocketPath,
			subprotocol:    "mqtt",
			allowAnyOrigin: true, // device gateways are not browser clients; Origin is not a security boundary
			tls:            b.gnetyTLS(),
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
	// A safety net: close cancels the context as soon as the connection is gone,
	// this covers a connection that failed before close was reached.
	defer c.stopContext()
	if err := c.handshake(); err != nil {
		if c.registered {
			// The connection is gone, so the disconnect hook observes the cancelled
			// context, exactly as it does on the normal path.
			c.stopContext()
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
// session and calls the dispatcher. A persistent session (clean session / clean
// start cleared) is restored here: the stored subscriptions become the new
// connection's subscriptions and the CONNACK that follows reports Session
// Present. A clean connection discards any stored session.
func (b *Broker) register(c *client) error {
	key := namespaceKey(b.namespace, c.id)
	b.mu.Lock()
	var takeover map[string]subscription
	if old, ok := b.clients[key]; ok && old != c {
		if b.cfg.ConnectMode == ConnectModeUnique {
			b.mu.Unlock()
			return ErrClientIDInUse
		}
		delete(b.clients, key)
		// In KICK mode the replacement takes the old session over. The old
		// connection must neither save that state again nor erase the new
		// session when its own serve goroutine unwinds.
		old.mu.Lock()
		old.superseded = true
		if old.persistentSession() {
			if stored, ok := b.sessions[key]; ok {
				takeover = copySubscriptions(stored.subs)
			} else {
				takeover = copySubscriptions(old.subs)
			}
		}
		old.mu.Unlock()
		b.mu.Unlock()
		old.close(false)
		b.mu.Lock()
	}
	if b.cfg.MaxConnections > 0 && len(b.clients) >= b.cfg.MaxConnections {
		b.mu.Unlock()
		return ErrMaxConnections
	}

	if c.resumesSession() {
		if stored, ok := b.sessions[key]; ok {
			c.subs = copySubscriptions(stored.subs)
			c.sessionPresent = true
		} else if takeover != nil {
			c.subs = takeover
			c.sessionPresent = true
		}
	} else {
		delete(b.sessions, key)
	}

	b.clients[key] = c
	c.registered = true
	n := len(b.clients)
	b.mu.Unlock()
	if len(c.subs) > 0 {
		b.subscriptionsChanged()
	}

	// KICK mode: tell peer nodes to drop any session with the same client id.
	// This makes client-id takeover work across nodes, not just locally.
	if b.cfg.ConnectMode == ConnectModeKick {
		b.spreadKick(c.context(), c.id)
	}

	b.metrics.IncrementConnections()
	b.metrics.RecordConnections(n)
	if b.cfg.Sink != nil {
		b.cfg.Sink.OnConnect(c.context(), c.info)
	}
	return nil
}

func (b *Broker) unregister(c *client) {
	if !c.registered {
		return
	}
	c.registered = false
	key := namespaceKey(b.namespace, c.id)
	b.mu.Lock()
	if current, ok := b.clients[key]; ok && current == c {
		delete(b.clients, key)
	}
	n := len(b.clients)
	c.mu.Lock()
	hadSubscriptions := len(c.subs) > 0
	if !c.superseded {
		if c.persistentSession() {
			b.sessions[key] = &session{subs: copySubscriptions(c.subs)}
		} else {
			delete(b.sessions, key)
		}
	}
	c.mu.Unlock()
	b.mu.Unlock()

	b.metrics.DecrementConnections()
	b.metrics.IncrementDisconnectEvents()
	b.metrics.RecordConnections(n)
	if hadSubscriptions {
		// The session's filters left the routing summary with it.
		b.subscriptionsChanged()
	}
	if b.cfg.Sink != nil {
		// The disconnect hooks observe the cancelled connection context (the
		// connection is already gone) but it still carries its ConnInfo, so a hook
		// can attribute the event to the exact connection.
		if c.lost {
			b.cfg.Sink.OnConnectionLost(c.context(), c.id, c.closeErr)
		} else {
			b.cfg.Sink.OnDisconnect(c.context(), c.id, c.closeErr)
		}
	}
}

func (b *Broker) subscribe(c *client, filter string, sub subscription) (byte, bool) {
	if !b.cfg.ACL.Check(c.context(), SessionInfo{ClientID: c.id, Username: c.username}, filter, AclSubscribe) {
		b.metrics.RecordDroppedMessage("acl_subscribe")
		return subscribeFailure(c.version, 0x87), false
	}
	c.mu.Lock()
	_, exists := c.subs[filter]
	if b.cfg.MaxTopicSubscriptions > 0 && len(c.subs) >= b.cfg.MaxTopicSubscriptions && !exists {
		c.mu.Unlock()
		b.metrics.RecordDroppedMessage("max_subscriptions")
		return subscribeFailure(c.version, 0x97), exists
	}
	c.subs[filter] = sub
	c.mu.Unlock()
	b.metrics.IncrementSubscribeEvents()
	if !exists {
		b.subscriptionsChanged()
	}
	return sub.qos, exists
}

// subscribeFailure maps a subscription refusal onto the protocol level: MQTT 5
// names the reason (0x87 not authorized, 0x97 quota exceeded), while MQTT 3.1.1
// has the single 0x80 failure code.
func subscribeFailure(version, reason byte) byte {
	if version == mqtt.Version5 {
		return reason
	}
	return 0x80
}

func (b *Broker) unsubscribe(c *client, filter string) {
	c.mu.Lock()
	_, existed := c.subs[filter]
	delete(c.subs, filter)
	c.mu.Unlock()
	if existed {
		b.metrics.DecrementSubscribeEvents()
		b.subscriptionsChanged()
	}
}

// subscriptionFilters implements subscriptionProvider: every topic filter a
// local session of this broker's namespace is subscribed to, sorted and
// de-duplicated. The built-in TCP cluster advertises it so peers can route a
// publish only to the nodes that have a matching subscriber; the advertised
// summary carries the namespace, so peers of another namespace ignore it.
func (b *Broker) subscriptionFilters() []string {
	b.mu.RLock()
	clients := make([]*client, 0, len(b.clients))
	for _, c := range b.clients {
		if c.namespace == b.namespace {
			clients = append(clients, c)
		}
	}
	b.mu.RUnlock()

	seen := make(map[string]struct{}, len(clients))
	for _, c := range clients {
		c.mu.Lock()
		for filter := range c.subs {
			seen[filter] = struct{}{}
		}
		c.mu.Unlock()
	}
	out := make([]string, 0, len(seen))
	for filter := range seen {
		out = append(out, filter)
	}
	sort.Strings(out)
	return out
}

// subscriptionsChanged tells a cluster backend that can push subscription
// summaries that the local filter set changed.
func (b *Broker) subscriptionsChanged() {
	notifier, ok := b.cfg.ClusterBackend.(subscriptionChangeNotifier)
	if !ok {
		return
	}
	notifier.subscriptionsChanged()
}

// storeRetained applies one retained publish to the retained store, whether it
// came from a local session or from a peer node. Both paths obey the same
// rules: an empty payload removes the topic, and MaxRetainMessages bounds how
// many topics are stored (a full store still accepts a new value for a topic it
// already holds). The QoS and the typed property set travel with the message,
// so a retained delivery later reproduces the publish that set it.
func (b *Broker) storeRetained(namespace, topic string, payload []byte, qos byte, props mqtt.Properties) {
	key := namespaceKey(namespace, topic)
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(payload) == 0 {
		delete(b.retained, key)
		return
	}
	if _, exists := b.retained[key]; !exists && b.cfg.MaxRetainMessages > 0 && int64(len(b.retained)) >= b.cfg.MaxRetainMessages {
		b.metrics.RecordDroppedMessage("max_retain")
		return
	}
	b.retained[key] = retainedMessage{namespace: namespace, topic: topic, payload: append([]byte(nil), payload...), qos: qos, props: props}
}

func (b *Broker) sendRetained(c *client, filter string, qos byte, retainAsPublished bool) {
	b.mu.RLock()
	var msgs []retainedMessage
	for _, m := range b.retained {
		if m.namespace != c.namespace {
			continue
		}
		if matchTopic(filter, m.topic) {
			msgs = append(msgs, m)
		}
	}
	b.mu.RUnlock()
	for _, m := range msgs {
		_ = c.sendPublish(m.topic, m.payload, minQoS(qos, m.qos), true, m.props)
	}
}

// publish authorizes, hands the message to the sink and only then routes it.
// The returned verdict decides whether the caller may acknowledge, so nothing
// here may claim durability the sink did not confirm.
//
// ev.Properties is the payload of the SPI contract (one MQTT 5 property block,
// no length prefix); props is the same set in typed form, which is what routing,
// retained state and outgoing packets use.
//
// Every hook that runs here receives the connection-scoped context of from, so a
// hook can read the ConnInfo and is cancelled when the connection ends. Retained
// state is written here, before routing: a Dispatcher decides where an accepted
// publication goes, not what is retained.
func (b *Broker) publish(from *client, ev PublishEvent, props mqtt.Properties) PublishResult {
	ctx := from.context()
	ev.ConnectionID = from.connID
	ev.ClientID = from.id
	if !b.cfg.ACL.Check(ctx, SessionInfo{ClientID: from.id, Username: from.username}, ev.Topic, AclPublish) {
		b.metrics.RecordDroppedMessage("acl_publish")
		return PublishResult{Disposition: PublishRejected, ReasonCode: ReasonNotAuthorized}
	}
	for _, it := range b.cfg.Interceptors {
		if !it.OnPublish(ctx, from.id, ev.Topic, ev.Payload, ev.QoS) {
			b.metrics.RecordDroppedMessage("interceptor")
			return PublishResult{Disposition: PublishRejected, ReasonCode: ReasonNotAuthorized}
		}
	}
	if res := b.callSink(ctx, ev); !res.Accepted() {
		reason := "sink_rejected"
		if res.Disposition == PublishRetryable {
			reason = "sink_retryable"
		}
		b.metrics.RecordDroppedMessage(reason)
		return res
	}
	// Retain is stored only for accepted publishes: a rejected message must not
	// become the retained value new subscribers receive.
	if ev.Retain {
		b.storeRetained(from.namespace, ev.Topic, ev.Payload, ev.QoS, props)
	}
	b.metrics.IncrementPublishedMessages()

	pub := Publication{
		Namespace:        from.namespace,
		Topic:            ev.Topic,
		Payload:          ev.Payload,
		QoS:              ev.QoS,
		Retain:           ev.Retain,
		Dup:              ev.Dup,
		FromClientID:     from.id,
		FromConnectionID: string(from.connID),
		Props:            ev.Properties,
		props:            props,
		propsSet:         true,
	}
	if err := b.dispatcher.Dispatch(ctx, pub); err != nil {
		b.log.Warn("gmqx dispatch failed", "err", err)
	}
	return PublishResult{Disposition: PublishAccepted}
}

// callSink runs the sink, optionally with a bounded wait. Without a configured
// timeout the call is synchronous and allocation-free on the hot path. The
// caller's connection context parents the bounded wait, so a connection that
// ends stops the wait as well.
func (b *Broker) callSink(ctx context.Context, ev PublishEvent) PublishResult {
	if b.cfg.Sink == nil {
		return PublishResult{Disposition: PublishAccepted}
	}
	timeout := b.cfg.PublishCallbackTimeout
	if timeout <= 0 {
		return b.cfg.Sink.OnPublish(ctx, ev)
	}
	sinkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan PublishResult, 1)
	go func() { done <- b.cfg.Sink.OnPublish(sinkCtx, ev) }()
	select {
	case res := <-done:
		return res
	case <-sinkCtx.Done():
		return PublishResult{Disposition: PublishRetryable, ReasonCode: ReasonQuotaExceeded}
	}
}

// DefaultDispatcher is the built-in routing policy: deliver to the local
// sessions a publication matches and spread it to the cluster backend. It is
// exactly the hardcoded path the broker used before the Dispatcher SPI existed,
// and it is what Config.Dispatcher == nil selects.
type DefaultDispatcher struct {
	broker *Broker
}

// NewDefaultDispatcher returns an unbound DefaultDispatcher. New binds it to the
// broker it belongs to; a Dispatcher that wraps the default policy can call Bind
// itself.
func NewDefaultDispatcher() *DefaultDispatcher { return &DefaultDispatcher{} }

// Bind implements DispatcherBinder.
func (d *DefaultDispatcher) Bind(b *Broker) { d.broker = b }

// Dispatch implements Dispatcher. Local deliveries stay best-effort (a failed
// write to one session does not stop the others), exactly as before the SPI; a
// cluster spread failure is returned so the broker can log it.
func (d *DefaultDispatcher) Dispatch(ctx context.Context, pub Publication) error {
	if d == nil || d.broker == nil {
		return errors.New("gmqx: default dispatcher is not bound to a broker")
	}
	for _, target := range d.broker.LocalTargets(pub) {
		_ = d.broker.SendLocal(ctx, target, pub)
	}
	return d.broker.SpreadPublication(ctx, pub)
}

// LocalTargets is the read-only half of the routing seam a Dispatcher uses: the
// local sessions whose subscriptions match pub, with the per-session QoS cap and
// retain flag already applied, and no-local subscriptions of the publishing
// connection excluded. It re-reads live subscriptions on every call; it does not
// cache a routing table. A publication of another namespace has no local target
// here.
func (b *Broker) LocalTargets(pub Publication) []LocalTarget {
	if pub.Namespace != b.namespace {
		return nil
	}
	from := ConnectionID(pub.FromConnectionID)
	b.mu.RLock()
	defer b.mu.RUnlock()
	targets := make([]LocalTarget, 0)
	for _, c := range b.clients {
		if c.namespace != pub.Namespace {
			continue
		}
		c.mu.Lock()
		found := false
		rap := false
		var maxQoS byte
		for filter, sub := range c.subs {
			if !matchTopic(filter, pub.Topic) {
				continue
			}
			if sub.noLocal && c.connID == from {
				continue
			}
			found = true
			if sub.qos > maxQoS {
				maxQoS = sub.qos
			}
			if sub.retainAsPublished {
				rap = true
			}
		}
		target := LocalTarget{
			ClientID:     c.id,
			ConnectionID: c.connID,
			QoS:          minQoS(pub.QoS, maxQoS),
			Retain:       rap && pub.Retain,
			client:       c,
		}
		c.mu.Unlock()
		if found {
			// The subscription's QoS is a maximum: a QoS 1 publication on a QoS 0
			// subscription must go out as QoS 0.
			targets = append(targets, target)
		}
	}
	return targets
}

// SendLocal is the write half of the routing seam: it writes pub to one target
// returned by LocalTargets. The send is synchronous and best-effort, like the
// broker's own delivery path; ctx is the publication's context (the publishing
// connection's context for an accepted local publication) and is carried so a
// dispatcher can observe cancellation, not to abort a write that is already on
// the wire.
func (b *Broker) SendLocal(ctx context.Context, target LocalTarget, pub Publication) error {
	if target.client == nil {
		return errors.New("gmqx: local target was not produced by this broker")
	}
	props := pub.props
	if !pub.propsSet {
		decoded, err := decodePropertyBlock(pub.Props)
		if err != nil {
			return fmt.Errorf("gmqx: local delivery properties: %w", err)
		}
		props = decoded
	}
	return target.client.sendPublish(pub.Topic, pub.Payload, target.QoS, target.Retain, props)
}

// SpreadPublication is the cluster half of the routing seam: it hands pub to the
// configured ClusterBackend with this broker's node id. The publication's
// namespace travels with the event; a built-in backend stamps an empty namespace
// with its own and refuses to produce an event for a different one. It is a
// no-op without a backend.
func (b *Broker) SpreadPublication(ctx context.Context, pub Publication) error {
	if b.cfg.ClusterBackend == nil {
		return nil
	}
	msg := ClusterMessage{
		Type:      ClusterPublish,
		NodeID:    b.nodeID,
		Namespace: pub.Namespace,
		Topic:     pub.Topic,
		Payload:   append([]byte(nil), pub.Payload...),
		QoS:       pub.QoS,
		Retain:    pub.Retain,
		Props:     pub.Props,
	}
	if err := b.cfg.ClusterBackend.Spread(ctx, msg); err != nil {
		return fmt.Errorf("gmqx: cluster spread: %w", err)
	}
	return nil
}

// spreadKick tells peer nodes of this namespace to drop a kicked client.
func (b *Broker) spreadKick(ctx context.Context, clientID string) {
	if b.cfg.ClusterBackend == nil || clientID == "" {
		return
	}
	msg := ClusterMessage{Type: ClusterKick, NodeID: b.nodeID, Namespace: b.namespace, ClientID: clientID}
	if err := b.cfg.ClusterBackend.Spread(ctx, msg); err != nil {
		b.log.Warn("gmqx cluster kick spread failed", "err", err)
	}
}

// OnRemotePublish implements ClusterHandler.
//
// A remote retained publish updates the retained store before it is delivered
// locally, with the same rules as a local retained publish: an empty payload
// removes the topic, MaxRetainMessages bounds the store, and the QoS and
// properties carried by the message are kept. That is what makes the cluster's
// broadcast of retained publishes useful: a subscriber that connects to this
// node later receives the value from the store even though the publish happened
// on another node. The publish was already authorized and accepted by the node
// that produced it, so it is not run through this node's ACL, interceptors or
// sink again, and it is not handed to the Dispatcher again: it was already
// routed by its origin, and re-dispatching it would send it back to the cluster.
//
// Namespace isolation is enforced here as well as in the built-in backends: an
// event whose namespace is not this broker's namespace never reaches a session
// or the retained store. The backends drop such an event before calling this
// method; the check also covers a third-party backend.
//
// The store update is applied once per event: a backend drops a duplicate
// (Origin, Seq) before calling this method, and replaying the same retained
// value would converge on the same state anyway.
//
// Ordering: a remote retained publish can arrive after a local subscriber was
// admitted, so that subscriber may have missed it — sendRetained ran at
// SUBSCRIBE time, before the event arrived. The guarantee is that the state is
// eventually present in the store, not that a session already connected is
// replayed the value.
func (b *Broker) OnRemotePublish(msg ClusterMessage) {
	if msg.Namespace != b.namespace {
		b.metrics.RecordDroppedMessage("cluster_namespace")
		b.log.Debug("gmqx remote publish dropped", "namespace", msg.Namespace, "topic", msg.Topic)
		return
	}
	props, err := decodePropertyBlock(msg.Props)
	if err != nil {
		b.log.Warn("gmqx cluster publish properties", "err", err)
		return
	}
	if msg.Retain {
		b.storeRetained(msg.Namespace, msg.Topic, msg.Payload, msg.QoS, props)
	}
	pub := Publication{
		Namespace: msg.Namespace,
		Topic:     msg.Topic,
		Payload:   msg.Payload,
		QoS:       msg.QoS,
		Retain:    msg.Retain,
		Props:     msg.Props,
		props:     props,
		propsSet:  true,
	}
	ctx := b.lifecycleContext()
	for _, target := range b.LocalTargets(pub) {
		_ = b.SendLocal(ctx, target, pub)
	}
}

// OnRemoteKick implements ClusterHandler. A kick for another namespace is
// dropped: a client id is only unique inside its namespace, so applying it would
// close an unrelated session that happens to share the id.
func (b *Broker) OnRemoteKick(msg ClusterMessage) {
	if msg.Namespace != b.namespace {
		b.metrics.RecordDroppedMessage("cluster_namespace")
		b.log.Debug("gmqx remote kick dropped", "namespace", msg.Namespace, "client", msg.ClientID)
		return
	}
	b.mu.RLock()
	c := b.clients[namespaceKey(b.namespace, msg.ClientID)]
	b.mu.RUnlock()
	if c != nil {
		c.close(false)
	}
}

// Namespace returns the broker's isolation scope. The empty string is the
// default namespace.
func (b *Broker) Namespace() string { return b.namespace }

// lifecycleContext is the parent of work that belongs to the broker rather than
// to a connection: the per-connection context derives from it, and a remote
// publication's local fan-out runs under it. It is the context Start received,
// or context.Background() for a broker that never started.
func (b *Broker) lifecycleContext() context.Context {
	b.mu.RLock()
	ctx := b.ctx
	b.mu.RUnlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
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
