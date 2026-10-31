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
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	store      SessionStore
	// index is the routing table: which local session (live or stored) is
	// subscribed to which filter. It is the only structure a publication is
	// matched against; see routing_index.go for its shape and invariants.
	index *subscriptionIndex
	// presence is the optional cluster view of which node holds a client id. The
	// backend that implements it is told about every connect and disconnect, so a
	// directed publish can be routed instead of broadcast.
	presence clientPresence
	// sharedNodes is the optional cluster view of which nodes hold a live member of
	// a shared subscription group. It is what lets one node deliver a shared
	// publication instead of every node delivering a copy (see shared.go).
	sharedNodes sharedDirectory
	// sharedMu guards sharedRotation, the per-group counter that rotates the node
	// election of a shared group so successive publications of one group do not all
	// land on the same node. The table is bounded by maxSharedRotationEntries.
	sharedMu       sync.Mutex
	sharedRotation map[string]uint64

	// readBytes and writeBytes are the broker-wide byte-rate buckets: every byte
	// this process reads from a client connection or writes to one is charged to
	// them, which is what makes MaxTotalReadByteRate/MaxTotalWriteByteRate a limit
	// on the whole broker instead of on one connection. A disabled limit is a nil
	// bucket (see byte_rate.go).
	readBytes  *byteLimiter
	writeBytes *byteLimiter

	mu       sync.RWMutex
	ctx      context.Context // broker lifetime context, set by Start
	clients  map[string]*client
	retained map[string]retainedMessage
	servers  []*gnety.Server
	addrs    map[string]string
	done     chan struct{}
	started  bool
	closed   bool
	// offlineQueued counts the messages stored for offline sessions across all
	// of them, which is what MaxTotalOfflineMessages bounds. It is maintained by
	// the broker under mu and follows every message that enters or leaves a stored
	// queue.
	offlineQueued int
	// background tracks the broker's own goroutines (the session sweeper) so Stop
	// returns only once they are gone.
	background sync.WaitGroup
	// wills holds the wills whose Will Delay Interval has not elapsed yet, keyed
	// like clients. willMu is the only lock that guards the map; it is never held
	// across a publish, so a will timer can publish while a connection connects.
	wills  map[string]*pendingWill
	willMu sync.Mutex
}

type retainedMessage struct {
	namespace string
	topic     string
	payload   []byte
	qos       byte
	props     mqtt.Properties
	// expiresAt is when the store may drop the message: the time it was stored
	// plus its MQTT 5 Message Expiry Interval. The zero time means it never
	// expires — no interval on the publish, or MQTT 3.1.1, which has none.
	expiresAt time.Time
}

// copySubscriptions returns a detached copy of a session's subscription map, so
// a stored session and a live connection never share one.
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

// Defaults for the delivery limits Config leaves at zero. They are bounded on
// purpose: an unbounded offline queue and an unbounded inflight window both turn
// a slow or absent client into unbounded broker memory.
const (
	defaultMaxOfflineQueueSize     = 1000
	defaultMaxTotalOfflineMessages = 100000
	defaultMaxInflightQoS1         = 64
	defaultMaxInflightQoS2         = 64
	defaultMaxInflightQueueSize    = 256
	defaultMaxOutboundQueueSize    = 256
	defaultWriteTimeout            = 5 * time.Second
	defaultSessionSweepInterval    = 30 * time.Second
	defaultQoSRetryInterval        = 5 * time.Second
	defaultQoSMaxRetries           = 5
	defaultConnectTimeout          = 10 * time.Second
	defaultAuthTimeout             = 5 * time.Second
	defaultMaxWillDelayInterval    = 5 * time.Minute
	defaultNotKickSeconds          = 30
	defaultReceiveMaximum          = 64
	// defaultPortFallback is how many ports above the configured one a listener
	// may try before Start fails.
	defaultPortFallback = 16
)

// maxReceiveMaximum is the protocol default and the highest value the MQTT 5
// Receive Maximum property can carry: an absent property means this window.
const maxReceiveMaximum = 65535

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
	if cfg.NotKickSeconds == 0 {
		cfg.NotKickSeconds = defaultNotKickSeconds
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = defaultConnectTimeout
	}
	if cfg.AuthTimeout == 0 {
		cfg.AuthTimeout = defaultAuthTimeout
	}
	if cfg.MaxWillDelayInterval == 0 {
		cfg.MaxWillDelayInterval = defaultMaxWillDelayInterval
	}
	if cfg.unit <= 0 {
		cfg.unit = time.Second
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
	if cfg.NodeID == "" && cfg.Cluster.Node != "" {
		// Cluster.Node is the cluster-named node id; an explicit Config.NodeID
		// always wins over it. Without either, the random id below is used, so a
		// gossip cluster never collides by default.
		cfg.NodeID = cfg.Cluster.Node
	}
	if cfg.Namespace == "" && cfg.Cluster.Namespace != "" {
		// A namespace can be named on the cluster block; the broker's
		// Config.Namespace stays authoritative once it is set, because the broker
		// stamps every event and summary it produces.
		cfg.Namespace = cfg.Cluster.Namespace
	}
	if cfg.NodeID == "" {
		cfg.NodeID = randomID()
	}
	if cfg.SessionStore == nil {
		cfg.SessionStore = NewMemorySessionStore()
	}
	if cfg.MaxOfflineQueueSize == 0 {
		cfg.MaxOfflineQueueSize = defaultMaxOfflineQueueSize
	}
	if cfg.MaxTotalOfflineMessages == 0 {
		cfg.MaxTotalOfflineMessages = defaultMaxTotalOfflineMessages
	}
	if cfg.MaxInflightQoS1 == 0 {
		cfg.MaxInflightQoS1 = defaultMaxInflightQoS1
	}
	if cfg.MaxInflightQoS2 == 0 {
		cfg.MaxInflightQoS2 = defaultMaxInflightQoS2
	}
	if cfg.MaxInflightQueueSize <= 0 {
		cfg.MaxInflightQueueSize = defaultMaxInflightQueueSize
	}
	if cfg.ReceiveMaximum == 0 {
		cfg.ReceiveMaximum = defaultReceiveMaximum
	}
	if cfg.MaxReadByteRate == 0 {
		cfg.MaxReadByteRate = defaultMaxReadByteRate
	}
	if cfg.MaxWriteByteRate == 0 {
		cfg.MaxWriteByteRate = defaultMaxWriteByteRate
	}
	if cfg.MaxTotalReadByteRate == 0 {
		cfg.MaxTotalReadByteRate = defaultMaxTotalReadByteRate
	}
	if cfg.MaxTotalWriteByteRate == 0 {
		cfg.MaxTotalWriteByteRate = defaultMaxTotalWriteByteRate
	}
	if cfg.MaxOutboundQueueSize <= 0 {
		cfg.MaxOutboundQueueSize = defaultMaxOutboundQueueSize
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = defaultWriteTimeout
	}
	if cfg.SessionSweepInterval == 0 {
		cfg.SessionSweepInterval = defaultSessionSweepInterval
	}
	if cfg.QoSRetryInterval <= 0 {
		cfg.QoSRetryInterval = defaultQoSRetryInterval
	}
	if cfg.QoSMaxRetries == 0 {
		cfg.QoSMaxRetries = defaultQoSMaxRetries
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	// Cluster.Enabled selects the default backend only when no backend object was
	// supplied: an explicit Config.ClusterBackend always wins. The gossip backend
	// takes its listen address and seeds from the same ClusterConfig, so a
	// deployment that only sets Cluster.Enabled/Addr/Seeds gets a working
	// cluster, while NewTCPCluster and NewRedisCluster stay available for the
	// static-mesh and shared-store deployments.
	if cfg.ClusterBackend == nil && cfg.Cluster.Enabled {
		cfg.ClusterBackend = NewGossipCluster(GossipClusterConfig{
			Addr:                cfg.Cluster.Addr,
			Seeds:               append([]string(nil), cfg.Cluster.Seeds...),
			PingInterval:        cfg.Cluster.PingInterval,
			PingTimeout:         cfg.Cluster.PingTimeout,
			SuspicionMultiplier: cfg.Cluster.SuspicionMultiplier,
			Fanout:              cfg.Cluster.Fanout,
			Namespace:           cfg.Namespace,
			Metrics:             cfg.Metrics,
			Logger:              logger,
		})
	}
	b := &Broker{
		cfg:       cfg,
		log:       logger,
		metrics:   cfg.Metrics,
		namespace: cfg.Namespace,
		clients:   make(map[string]*client),
		store:     cfg.SessionStore,
		index:     newSubscriptionIndex(),
		retained:  make(map[string]retainedMessage),
		wills:     make(map[string]*pendingWill),
		addrs:     make(map[string]string),
		done:      make(chan struct{}),
		nodeID:    cfg.NodeID,
	}
	// The broker-wide half of the byte-rate shaping: one bucket each for every
	// byte read from and written to every connection of this process. A disabled
	// limit (negative) is a nil bucket, which the shaper skips.
	b.readBytes = newByteLimiter(cfg.MaxTotalReadByteRate)
	b.writeBytes = newByteLimiter(cfg.MaxTotalWriteByteRate)
	b.presence, _ = cfg.ClusterBackend.(clientPresence)
	b.sharedNodes, _ = cfg.ClusterBackend.(sharedDirectory)
	b.dispatcher = newDispatcher(cfg.Dispatcher, b)
	// A store may have been restored or preloaded before the broker was created;
	// those sessions are local targets from the first publication on, exactly as
	// they were when routing scanned the store on every publish. Sessions the
	// broker saves later are indexed when they are saved.
	b.loadStoredSessions()
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
		srv, resolved, err := b.bindListener(ctx, spec)
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
		b.addrs[spec.name] = resolved
		b.mu.Unlock()
		b.log.Info("gmqx listener started", "transport", spec.name, "addr", resolved)
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
	b.background.Add(1)
	go func() {
		defer b.background.Done()
		b.sessionSweeper()
	}()
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

// bindListener creates and starts the gnety server of one listener spec. It
// tries the configured address first and then the addresses Config.PortFallback
// allows, in order, until one bind succeeds. The bind is the probe: there is no
// check for a free port before binding, so two brokers starting at the same time
// cannot both pick the same port and one of them lose a race it already won. The
// transport (network, TLS, WebSocket path and subprotocol) is the spec's, only the
// port changes.
func (b *Broker) bindListener(ctx context.Context, spec listenerSpec) (*gnety.Server, string, error) {
	addrs := fallbackAddrs(spec.addr, b.portFallback())
	var lastErr error
	for i, addr := range addrs {
		srv, err := gnety.New(gnety.Config{
			Network:        spec.network,
			Addr:           addr,
			Path:           spec.path,
			Subprotocol:    spec.subprotocol,
			AllowAnyOrigin: spec.allowAnyOrigin,
			TLS:            spec.tls,
			RawHandler:     b.handleRaw,
		})
		if err == nil {
			err = srv.Start(ctx)
		}
		if err == nil {
			if i > 0 {
				b.log.Warn("gmqx listener port in use, bound the next free port",
					"transport", spec.name, "requested", spec.addr, "addr", srv.Addr())
			}
			return srv, srv.Addr(), nil
		}
		lastErr = err
		if !addrInUse(err) {
			// A configuration error (a missing certificate, a bad address, an
			// unsupported network) is not something the next port would fix.
			return nil, addr, err
		}
	}
	return nil, spec.addr, lastErr
}

// addrInUse reports whether err is the "address already in use" failure of a bind,
// the only listener error a port fallback can fix. The syscall comparison covers
// the Unix systems; the message check covers Windows, whose socket error
// (WSAEADDRINUSE) does not compare equal to syscall.EADDRINUSE.
func addrInUse(err error) bool {
	if errors.Is(err, syscall.EADDRINUSE) {
		return true
	}
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "address already in use") ||
		strings.Contains(msg, "only one usage of each socket address")
}

// portFallback is how many ports above the configured one a listener may try:
// Config.PortFallback, the documented default when it is zero, and none when it
// is negative.
func (b *Broker) portFallback() int {
	if b.cfg.PortFallback < 0 {
		return 0
	}
	if b.cfg.PortFallback == 0 {
		return defaultPortFallback
	}
	return b.cfg.PortFallback
}

// fallbackAddrs returns addr followed by up to extra addresses whose port is
// one, two, ... above it. An address without a usable port (an ephemeral bind on
// port 0, a unix socket, a malformed address) has no fallback: the kernel already
// picked a free port, or there is no port to advance.
func fallbackAddrs(addr string, extra int) []string {
	addrs := []string{addr}
	if extra <= 0 {
		return addrs
	}
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return addrs
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 {
		return addrs
	}
	for i := 1; i <= extra; i++ {
		next := port + i
		if next > 65535 {
			break
		}
		addrs = append(addrs, net.JoinHostPort(host, strconv.Itoa(next)))
	}
	return addrs
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

// second is the length of one MQTT protocol second for the intervals this
// broker turns into timers (Config.unit, time.Second in production; the
// package's tests shorten it to keep a seconds-resolution case fast).
func (b *Broker) second() time.Duration {
	if unit := b.cfg.unit; unit > 0 {
		return unit
	}
	return time.Second
}

// connectTimeout is how long the CONNECT phase of a connection may take.
func (b *Broker) connectTimeout() time.Duration {
	if timeout := b.cfg.ConnectTimeout; timeout > 0 {
		return timeout
	}
	return defaultConnectTimeout
}

// receiveMaximum is the effective inbound receive maximum of this broker: how
// many unacknowledged QoS 1/2 exchanges one client may have outstanding toward
// it. It is also the value the MQTT 5 CONNACK advertises, which is why the two
// inputs are folded into one number instead of being advertised and enforced
// separately: Config.ReceiveMaximum is the receive maximum itself (default 64,
// negative and therefore unlimited advertised as the protocol default 65535) and
// Config.MaxInflightQoS2 has always bounded the inbound QoS 2 exchanges of a
// connection, so the smaller of the two is what the broker really enforces.
func (b *Broker) receiveMaximum() int {
	limit := b.cfg.ReceiveMaximum
	if limit == 0 {
		limit = defaultReceiveMaximum
	}
	if limit < 0 || limit > maxReceiveMaximum {
		limit = maxReceiveMaximum
	}
	if qos2 := b.cfg.MaxInflightQoS2; qos2 > 0 && qos2 < limit {
		limit = qos2
	}
	return limit
}

// receiveMaxFull counts an inbound QoS 1/2 PUBLISH the broker refused to deliver
// and acknowledge because its own receive maximum was full. The connection stays
// open: the client is expected to retransmit the QoS 1 publish later, and a QoS 2
// one was answered with PUBREC 0x97 and never started an exchange.
func (b *Broker) receiveMaxFull(topic string) {
	b.metrics.RecordDroppedMessage("receive_maximum_full")
	b.metrics.RecordOverflow("receive_maximum")
	if b.log != nil {
		b.log.Debug("gmqx inbound receive maximum full", "topic", topic, "limit", b.receiveMaximum())
	}
}

// willDelayLimit is the longest Will Delay Interval this broker honours, or 0
// when the configured bound is disabled.
func (b *Broker) maxWillDelayInterval() time.Duration {
	if b.cfg.MaxWillDelayInterval < 0 {
		return 0
	}
	if limit := b.cfg.MaxWillDelayInterval; limit > 0 {
		return limit
	}
	return defaultMaxWillDelayInterval
}

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
	// The wills whose delay has not elapsed go with the broker: there is nowhere
	// left to publish them, and their timers must not outlive Stop.
	b.dropPendingWills()
	// The sweeper waits on done, which is closed above; waiting here makes Stop
	// mean "every goroutine this broker started has ended".
	b.background.Wait()
	return errors.Join(errs...)
}

func (b *Broker) serve(conn net.Conn) {
	// Byte-rate shaping is applied by the transport wrapper, so every byte read
	// from or written to this connection — the CONNECT phase, the packet codec
	// and the shutdown DISCONNECT alike — passes one bucket (see byte_rate.go).
	conn = b.shapeConn(conn)
	c := newClient(b, conn)
	// A safety net: close cancels the context as soon as the connection is gone,
	// this covers a connection that failed before close was reached.
	defer c.stopContext()
	// The writer goroutine is joined as well: a connection that never reaches
	// close (a handshake that fails after the CONNACK) still ends it here.
	defer c.stopWriter()
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
//
// The stored session, when there is one, moves from the store to the connection:
// from here on the connection owns its subscriptions and its queue, and the store
// no longer holds the key. That is what keeps a publication from reaching a
// session twice (once live, once through a stale queue). An expired session is
// never resumed, even when the sweeper has not deleted it yet.
//
// A CONNECT that displaces a live connection is a takeover. In ConnectModeKick it
// proceeds only outside Config.NotKickSeconds, and the displaced connection gets
// a DISCONNECT 0x8E (Session taken over) on MQTT 5 before it is closed; in
// ConnectModeUnique it is refused. Either way the connection that wins the client
// id spreads a kick to the cluster, so a duplicate connection on another node is
// closed instead of surviving until its keep-alive expires (see spreadKick).
func (b *Broker) register(c *client) error {
	key := namespaceKey(b.namespace, c.id)
	b.mu.Lock()
	var takeover *session
	if old, ok := b.clients[key]; ok && old != c {
		if b.cfg.ConnectMode == ConnectModeUnique {
			b.mu.Unlock()
			return ErrClientIDInUse
		}
		if !b.takeoverAllowed(old) {
			b.mu.Unlock()
			b.metrics.RecordDroppedMessage("takeover_rate_limit")
			return ErrTakeoverRateExceeded
		}
		delete(b.clients, key)
		// In KICK mode the replacement takes the old session over. The old
		// connection must neither save that state again nor erase the new
		// session when its own serve goroutine unwinds.
		old.mu.Lock()
		old.superseded = true
		if old.persistentSession() {
			if stored, ok := b.takeStoredSession(key); ok {
				takeover = stored
			} else {
				// The old connection owns the state, so the replacement takes it over
				// directly — the QoS 2 state of both directions included, exactly as a
				// store would have carried it.
				inboundQoS2, inflight := old.snapshotQoS2Locked()
				takeover = &session{
					clientID:    old.id,
					subs:        copySubscriptions(old.subs),
					queue:       old.queue,
					inboundQoS2: inboundQoS2,
					inflight:    inflight,
				}
				old.queue = nil
			}
		}
		old.mu.Unlock()
		b.mu.Unlock()
		old.displace()
		b.mu.Lock()
	}
	if b.cfg.MaxConnections > 0 && len(b.clients) >= b.cfg.MaxConnections {
		b.mu.Unlock()
		return ErrMaxConnections
	}

	if c.resumesSession() {
		if stored, ok := b.takeStoredSession(key); ok {
			c.subs = stored.subs
			c.queue = stored.queue
			c.restoreQoS2State(stored)
			c.sessionPresent = true
		} else if takeover != nil {
			c.subs = takeover.subs
			c.queue = takeover.queue
			c.restoreQoS2State(takeover)
			c.sessionPresent = true
		}
	} else {
		b.deleteStoredSession(key)
	}
	if c.subs == nil {
		// A resumed session without subscriptions (or a store that returns none)
		// must still be able to accept a SUBSCRIBE.
		c.subs = make(map[string]subscription)
	}

	b.clients[key] = c
	c.registered = true
	c.connectedAt = time.Now()
	// The routing table follows the session into the connection: the filters it
	// restored (or the empty set of a clean session) are routable before the
	// CONNACK goes out, and the stored entries of a session that was taken over
	// stop being targets here, under the same lock that moves the session.
	b.index.setLive(key, c, c.subs)
	n := len(b.clients)
	b.mu.Unlock()
	if len(c.subs) > 0 {
		b.subscriptionsChanged()
	}

	// A new connection to this session cancels the will its previous connection
	// asked to delay: MQTT 5 forbids publishing it once the session is resumed. A
	// CONNECT that discards the session instead (Clean Start set) ends it right
	// here, so the pending will of that session goes out now.
	if c.resumesSession() {
		b.cancelPendingWill(key)
	} else {
		b.flushPendingWill(key)
	}

	// Every successful connect is spread to the cluster, in both connect modes:
	// a client that reconnects to another node leaves a connection behind on the
	// old one, and that connection must go now instead of surviving until its
	// keep-alive expires and duplicating every delivery in the meantime. The
	// event names a client id, not a topic, and the built-in backends stamp it
	// with this node's identity, namespace and sequence number, so their
	// deduplication and namespace rules apply unchanged (see spreadKick).
	b.spreadKick(c.context(), c.id, c.connectedAt)
	// A backend that tracks client-id presence is told about the connection, so a
	// directed publish for this id can address this node instead of every node.
	// The call only enqueues the presence write: no store or network call runs on
	// the CONNECT path under the broker lock (see clientPresence).
	if b.presence != nil {
		b.presence.clientConnected(c.id)
	}

	b.metrics.IncrementConnections()
	b.metrics.RecordConnections(n)
	if b.cfg.Sink != nil {
		b.cfg.Sink.OnConnect(c.context(), c.info)
	}
	return nil
}

// takeoverAllowed reports whether a KICK-mode CONNECT may displace the live
// connection old, which is how Config.NotKickSeconds works as the anti-flap
// window it is meant to be: the session a client id just installed is protected
// for that long, so two ends of one client id cannot displace each other in a
// loop. The caller holds b.mu, which is what orders the write of connectedAt in
// register with this read.
func (b *Broker) takeoverAllowed(old *client) bool {
	return !b.insideNotKickWindow(old.connectedAt)
}

// insideNotKickWindow reports whether a connection that began at startedAt is
// still inside the Config.NotKickSeconds anti-flap window, so nothing may
// displace it. It is the single definition of the window: a CONNECT applies it to
// the connection it would take over (takeoverAllowed) and a peer's kick applies it
// to the connection it would close (OnRemoteKick), so the two ends of one client
// id are protected by the same rule no matter which node they reach.
//
// The comparison is local to this process — both times come from this node's
// clock, and time.Since uses the monotonic reading when there is one — so it is
// unaffected by clock skew. A negative window is the documented "disabled".
func (b *Broker) insideNotKickWindow(startedAt time.Time) bool {
	window := b.cfg.NotKickSeconds
	if window < 0 {
		// The window is disabled: every CONNECT takes the session over, which is
		// what MQTT prescribes and what this broker did before the window existed.
		return false
	}
	if window == 0 {
		window = defaultNotKickSeconds
	}
	return time.Since(startedAt) <= time.Duration(window)*b.second()
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
	superseded := c.superseded
	kicked := c.kicked
	persistent := c.persistentSession()
	// movedAway reports that the session of this connection is not this node's any
	// more: a local take-over installed a replacement here, or a peer's kick moved
	// the client id away. With a store the nodes share, the node that holds the
	// client id owns the stored session too, so this node must neither write its
	// copy back (that would resurrect a session the peer is already serving, and
	// let two nodes serve one session) nor delete the key (that would erase the
	// state the peer saved when its own connection went away). With a node-local
	// store the pre-existing behavior stays: the stored state of a kicked session
	// is kept here, because only this node can ever reach it.
	movedAway := superseded || (kicked && b.sharedStore())
	if !movedAway {
		if persistent {
			b.saveStoredSession(key, c)
		} else {
			b.deleteStoredSession(key)
		}
	}
	c.mu.Unlock()
	if movedAway {
		// A displaced connection whose replacement is not on this node (a peer's
		// connect-time kick moved the session away) must not leave a live routing
		// entry behind: nothing here can deliver to it any more. A local take-over
		// already installed the replacement's entries under this lock, so the entry
		// is kept in that case.
		if _, connected := b.clients[key]; !connected {
			b.index.remove(key)
		}
	}
	b.mu.Unlock()

	// A backend that tracks client-id presence is told that this node no longer
	// holds the id — but only when it really does not:
	//
	//   - a connection that another one displaced on this node (a local take-over)
	//     leaves the id here, and the replacement already refreshed the record, so
	//     nothing is reported;
	//   - a persistent session that merely went offline stays here (its filters and
	//     its queue do), and keeps its presence so a directed publish is still
	//     routed to this node and queued;
	//   - a session that ended (a clean connection) or moved to another node (the
	//     kick that displaced it) is forgotten, so this node stops refreshing a
	//     record that would otherwise fight the new owner's. With a node-local store
	//     the stored state of a kicked persistent session stays behind — a queue is
	//     node-local, exactly as it is for a normal publish — while a shared store
	//     leaves the session to the node that now holds the client id. Either way the
	//     client id itself belongs to the node the client is connected to now.
	if !persistent || superseded || kicked {
		b.mu.RLock()
		_, connected := b.clients[key]
		b.mu.RUnlock()
		if !connected {
			b.forgetClientPresence(c.id)
		}
	}

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
	// A shared subscription is authorized by the filter it delivers: "$share/g/t"
	// receives the publications of "t", so an ACL that allows "t" allows it and a
	// rule written for the group name would not be consulted. The full filter is
	// what the session stores, the index keys and an UNSUBSCRIBE names.
	aclFilter := filter
	if shared, isShared, ok := parseSharedFilter(filter); isShared && ok {
		aclFilter = shared.filter
	}
	if !b.checkACL(c.context(), SessionInfo{ClientID: c.id, Username: c.username}, aclFilter, AclSubscribe) {
		b.metrics.RecordDroppedMessage("acl_subscribe")
		return subscribeFailure(c.version, ReasonNotAuthorized), false
	}
	c.mu.Lock()
	_, exists := c.subs[filter]
	if b.cfg.MaxTopicSubscriptions > 0 && len(c.subs) >= b.cfg.MaxTopicSubscriptions && !exists {
		c.mu.Unlock()
		b.metrics.RecordDroppedMessage("max_subscriptions")
		return subscribeFailure(c.version, ReasonQuotaExceeded), exists
	}
	c.subs[filter] = sub
	c.mu.Unlock()
	// The subscription is routable before the SUBACK is written: the connection
	// goroutine updates the index here, and the SUBACK follows in handleSubscribe.
	b.index.add(namespaceKey(b.namespace, c.id), c, filter, sub)
	b.metrics.IncrementSubscribeEvents()
	if !exists {
		b.subscriptionsChanged()
	}
	return sub.qos, exists
}

// subscribeFailure maps a subscription refusal onto the protocol level: MQTT 5
// names the reason (0x87 Not authorized, 0x97 Quota exceeded), while MQTT 3.1.1
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
		// The filter stops routing before the UNSUBACK is written.
		b.index.removeFilter(namespaceKey(b.namespace, c.id), c, filter)
		b.metrics.DecrementSubscribeEvents()
		b.subscriptionsChanged()
	}
}

// subscriptionFilters implements subscriptionProvider: every topic filter a
// live local session of this broker is subscribed to, sorted and de-duplicated.
// It is read from the routing index, so it costs one pass over the distinct
// filters and touches no connection; stored sessions are not advertised, because
// a publication they match is queued by the node that holds them. The built-in
// TCP cluster advertises the summary so peers can route a publish only to the
// nodes that have a matching subscriber; the advertised summary carries the
// namespace, so peers of another namespace ignore it.
func (b *Broker) subscriptionFilters() []string {
	return b.index.liveFilters()
}

// sharedSubscriptionGroups implements sharedSubscriptionProvider: the shared
// subscription groups this node holds at least one live member of. It is the
// shared half of the routing summary, and it is what lets one node elect the node
// that delivers a publication for a group instead of every node delivering a copy
// (see Broker.dispatchShared). Stored sessions are not advertised: an offline
// member receives no shared delivery.
func (b *Broker) sharedSubscriptionGroups() []string {
	return b.index.liveSharedGroups()
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
//
// The MQTT 5 Message Expiry Interval is stored as a deadline, not as a value:
// the interval counts from the moment the message was published, so a replay has
// to subtract the time the message spent in the store. A retained message
// without an interval never expires. A message replicated from a peer node is
// stamped with its arrival here, because the cluster event does not carry the
// origin's publish time (its age is the peer transport latency, which the
// one-second resolution of the property already dwarfs).
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
	m := retainedMessage{namespace: namespace, topic: topic, payload: append([]byte(nil), payload...), qos: qos, props: props}
	if expiry := props.MessageExpiryInterval; expiry > 0 {
		m.expiresAt = time.Now().Add(time.Duration(expiry) * b.second())
	}
	b.retained[key] = m
}

// sendRetained replays the retained messages a new subscription matches, in its
// own namespace only. ids are the Subscription Identifiers of the subscription
// that caused the replay: a replayed PUBLISH is a delivery like any other, so it
// carries them. A shared subscription is not replayed at all (§4.8.2: "No Retained
// Messages are sent to the Session when it first subscribes"), so the caller never
// reaches here for one.
//
// A message whose Message Expiry Interval has elapsed is not replayed: it is
// deleted from the store on the way, because that is the only moment the broker
// learns it has become stale (a retained message nobody subscribes to costs one
// map entry, not a timer). A message still inside its interval is replayed with
// the interval it has left, rounded up: the property counts from the publish and
// the subscriber has to see the time the message may still live here, and a
// rounding down to zero would mean "never expires" on the wire.
func (b *Broker) sendRetained(c *client, filter string, qos byte, retainAsPublished bool, ids []uint32) {
	now := time.Now()
	type replay struct {
		topic   string
		payload []byte
		qos     byte
		props   mqtt.Properties
	}
	var msgs []replay
	b.mu.Lock()
	for key, m := range b.retained {
		if m.namespace != c.namespace {
			continue
		}
		if !matchTopic(filter, m.topic) {
			continue
		}
		props := m.props
		if !m.expiresAt.IsZero() {
			remaining := m.expiresAt.Sub(now)
			if remaining <= 0 {
				delete(b.retained, key)
				b.metrics.RecordDroppedMessage("retained_expired")
				continue
			}
			props.MessageExpiryInterval = b.expiryInterval(remaining)
		}
		msgs = append(msgs, replay{topic: m.topic, payload: m.payload, qos: m.qos, props: props})
	}
	b.mu.Unlock()
	for _, m := range msgs {
		props, dropped := withSubscriptionIDs(m.props, ids)
		if dropped {
			b.metrics.RecordDroppedMessage("subscription_identifier_limit")
		}
		_ = c.sendPublish(m.topic, m.payload, minQoS(qos, m.qos), true, props)
	}
}

// expiryInterval converts the time a message has left into the value its MQTT 5
// Message Expiry Interval carries: whole protocol seconds, rounded up and never 0
// (0 on the wire means "no expiry at all").
func (b *Broker) expiryInterval(remaining time.Duration) uint32 {
	unit := b.second()
	seconds := uint64((remaining + unit - 1) / unit)
	if seconds == 0 {
		seconds = 1
	}
	if seconds > uint64(^uint32(0)) {
		seconds = uint64(^uint32(0))
	}
	return uint32(seconds)
}

// publish authorizes, hands the message to the sink and only then routes it.
// The returned verdict decides whether the caller may acknowledge, so nothing
// here may claim durability the sink did not confirm.
//
// ev.Properties is the payload of the SPI contract (one MQTT 5 property block,
// no length prefix); props is the same set in typed form, which is what routing,
// retained state and outgoing packets use.
//
// Every hook that runs here receives the context of the publication's origin, so
// a hook can read the ConnInfo and is cancelled when the connection ends. A will
// whose connection is long gone brings the context its scheduler captured
// instead (see will.go). Retained state is written here, before routing: a
// Dispatcher decides where an accepted publication goes, not what is retained.
func (b *Broker) publish(from origin, ev PublishEvent, props mqtt.Properties) PublishResult {
	ctx := from.ctx
	ev.ConnectionID = from.connID
	ev.ClientID = from.clientID
	if !b.checkACL(ctx, SessionInfo{ClientID: from.clientID, Username: from.username}, ev.Topic, AclPublish) {
		b.metrics.RecordDroppedMessage("acl_publish")
		return PublishResult{Disposition: PublishRejected, ReasonCode: ReasonNotAuthorized}
	}
	for _, it := range b.cfg.Interceptors {
		if !it.OnPublish(ctx, from.clientID, ev.Topic, ev.Payload, ev.QoS) {
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
		FromClientID:     from.clientID,
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

// origin is the identity a publication is made on behalf of: a live connection
// hands in its own (client.origin), and a will that outlived its connection hands
// in the one captured when it was scheduled, because the connection context is
// cancelled by then.
type origin struct {
	ctx       context.Context
	clientID  string
	connID    ConnectionID
	username  string
	namespace string
}

// checkACL runs the ACL hook of an authorization decision under Config.AuthTimeout.
// An expired call is a denial: a client whose ACL cannot answer in time is not
// given the benefit of the doubt. The built-in AllowAll cannot block, so it takes
// the synchronous path and keeps the hot publish path free of a goroutine.
func (b *Broker) checkACL(ctx context.Context, session SessionInfo, topic string, action AclAction) bool {
	if _, builtin := b.cfg.ACL.(AllowAll); builtin {
		return b.cfg.ACL.Check(ctx, session, topic, action)
	}
	timeout := b.cfg.AuthTimeout
	if timeout <= 0 {
		return b.cfg.ACL.Check(ctx, session, topic, action)
	}
	aclCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- b.cfg.ACL.Check(aclCtx, session, topic, action) }()
	select {
	case allowed := <-done:
		return allowed
	case <-aclCtx.Done():
		if errors.Is(aclCtx.Err(), context.DeadlineExceeded) {
			if b.log != nil {
				b.log.Warn("gmqx ACL check deadline exceeded", "client", session.ClientID,
					"topic", topic, "action", action, "timeout", timeout)
			}
			b.metrics.RecordDroppedMessage("acl_timeout")
		}
		return false
	}
}

// authenticate calls the authentication hook under Config.AuthTimeout. An expired
// call is an authentication failure: a client whose Authenticator does not answer
// in time is refused, never admitted.
func (b *Broker) authenticate(ctx context.Context, info ConnectInfo) error {
	switch b.cfg.Auth.(type) {
	case AllowAll, DenyAll:
		return b.cfg.Auth.Authenticate(ctx, info)
	}
	timeout := b.cfg.AuthTimeout
	if timeout <= 0 {
		return b.cfg.Auth.Authenticate(ctx, info)
	}
	authCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.cfg.Auth.Authenticate(authCtx, info) }()
	select {
	case err := <-done:
		return err
	case <-authCtx.Done():
		if errors.Is(authCtx.Err(), context.DeadlineExceeded) {
			if b.log != nil {
				b.log.Warn("gmqx authentication deadline exceeded", "client", info.ClientID,
					"remote", info.RemoteAddr, "timeout", timeout)
			}
			b.metrics.RecordDroppedMessage("auth_timeout")
			return fmt.Errorf("%w: authentication timeout after %v", ErrUnauthorized, timeout)
		}
		return fmt.Errorf("%w: connection closed during authentication", ErrUnauthorized)
	}
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
// sessions whose subscriptions match pub, with the per-session QoS cap and retain
// flag already applied, and no-local subscriptions of the publishing connection
// excluded. A publication of another namespace has no local target here.
//
// It answers from the routing index (see routing_index.go), not by scanning the
// client table and the store: a literal topic is one map lookup plus one lookup
// per distinct wildcard filter, and the SessionStore is not consulted at all. The
// broker's own lock is not taken, so a publish never waits for a connection that
// is registering or leaving.
//
// A persistent session with no live connection is a local target too: its stored
// subscriptions are matched and SendLocal queues the publication for it (see
// LocalTarget.Offline). The two halves are disjoint — a session is either live in
// the index or stored in it — because the index entry of a session is replaced,
// not duplicated, when the session changes hands. A publication therefore reaches
// one session once.
func (b *Broker) LocalTargets(pub Publication) []LocalTarget {
	if pub.Namespace != b.namespace {
		return nil
	}
	return b.index.targets(pub.Topic, ConnectionID(pub.FromConnectionID), pub.QoS, pub.Retain)
}

// SendLocal is the write half of the routing seam: it writes pub to one target
// returned by LocalTargets, or queues it for a target that is an offline
// persistent session. The send is synchronous and best-effort, like the broker's
// own delivery path; ctx is the publication's context (the publishing
// connection's context for an accepted local publication) and is carried so a
// dispatcher can observe cancellation, not to abort a write that is already on
// the wire.
func (b *Broker) SendLocal(ctx context.Context, target LocalTarget, pub Publication) error {
	if target.client == nil && target.sessionKey == "" {
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
	// The identifiers belong to the subscriptions the target was reached
	// through, so they are attached here, once per delivery, and never travel in
	// Publication.Props (see subscription_id.go).
	var dropped bool
	props, dropped = withSubscriptionIDs(props, target.subscriptionIDs)
	if dropped {
		b.metrics.RecordDroppedMessage("subscription_identifier_limit")
	}
	if target.client == nil {
		b.enqueueOffline(target, pub, props)
		return nil
	}
	return target.client.sendPublish(pub.Topic, pub.Payload, target.QoS, target.Retain, props)
}

// SpreadPublication is the cluster half of the routing seam: it hands pub to the
// configured ClusterBackend with this broker's node id. The publication's
// namespace travels with the event; a built-in backend stamps an empty namespace
// with its own and refuses to produce an event for a different one. It is a
// no-op without a backend.
//
// It also routes shared subscriptions (see shared.go): deciding which node
// delivers a shared publication is part of the same hand-off, so the groups this
// node was elected for are delivered before the event leaves and the ones a peer
// must deliver travel in ClusterMessage.Shared. A publication that matches no
// shared group costs nothing extra.
func (b *Broker) SpreadPublication(ctx context.Context, pub Publication) error {
	shared := b.dispatchShared(ctx, pub)
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
		Shared:    shared,
	}
	if err := b.cfg.ClusterBackend.Spread(ctx, msg); err != nil {
		return fmt.Errorf("gmqx: cluster spread: %w", err)
	}
	return nil
}

// forgetClientPresence tells a presence-tracking cluster backend that this node no
// longer holds the client id: the session ended with its connection (a clean
// session), expired, or moved to another node (the connect-time kick of a peer).
// Nothing here can deliver a directed publish to the client any more. A persistent
// session that merely went offline keeps its presence record, because the node
// still holds its subscriptions and its queue.
func (b *Broker) forgetClientPresence(clientID string) {
	if b.presence != nil {
		b.presence.clientDisconnected(clientID)
	}
}

// PublishToClient delivers one publication to a single client id, wherever it
// lives: on this node (live, or a persistent session with no connection), or on
// another node of the cluster. It is the directed counterpart of a PUBLISH: it
// does not go through the ACL, the interceptors or the EventSink, because it is a
// server-side call rather than a client publication, and it does not touch the
// retained store — it is a delivery to one session, not a broadcast that a future
// subscriber should see. The caller owns the authorization of the call.
//
// Resolution order:
//
//  1. A live local connection receives the publication through the same
//     SendLocal path the local fan-out uses, at pub.QoS (there is no subscription
//     to cap it) and with the QoS 1/2 inflight window of the connection respected.
//  2. A local persistent session with no connection is queued for, exactly like a
//     matching publication would be: it drains right after the session's next
//     CONNACK, under the same queue bounds and counters. A queue is node-local, so
//     a node that kept the stored session of a client which reconnected elsewhere
//     queues the publication there — the same thing a normal publish does on that
//     node.
//  3. Otherwise the event goes to the cluster backend as a ClusterTarget event
//     and the call returns nil once the backend accepted it. A backend with a
//     presence view (Redis) addresses the node that holds the client id; the TCP
//     mesh broadcasts it and every peer applies it only if it holds the id.
//
// If nobody holds the client id the call is a successful no-op: delivery is
// fire-and-forget, at-most-once (the cluster transport is), and the fact that the
// client id was not found is counted with
// Metrics.RecordDroppedMessage("target_unknown") — by this broker when it has no
// cluster backend, by the Redis backend when its presence view has no record, and
// by each TCP peer that does not hold the id. An error is returned only for a
// caller mistake (an empty client id, an invalid topic or QoS, a publication of
// another namespace) or when the cluster backend refused the event (for example
// Redis is unreachable), in which case the caller may retry.
//
// pub.Namespace may be empty (this broker's namespace); a publication explicitly
// stamped with another namespace is refused, because a client id is only unique
// inside its namespace and delivering it here would cross the isolation boundary.
func (b *Broker) PublishToClient(ctx context.Context, clientID string, pub Publication) error {
	if clientID == "" {
		return ErrClientIDRequired
	}
	if pub.Namespace == "" {
		pub.Namespace = b.namespace
	}
	if pub.Namespace != b.namespace {
		return fmt.Errorf("%w: publication namespace %q, broker namespace %q",
			ErrNamespaceMismatch, pub.Namespace, b.namespace)
	}
	if !validTopic(pub.Topic) {
		// A directed publish is a PUBLISH the broker produces itself, so it has to
		// carry a topic name a subscriber could have subscribed to.
		return fmt.Errorf("%w: directed publish topic %q", ErrMalformedPacket, pub.Topic)
	}
	if pub.QoS > 2 {
		return fmt.Errorf("%w: directed publish QoS %d", ErrMalformedPacket, pub.QoS)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	delivered, err := b.deliverLocal(ctx, clientID, pub)
	if delivered {
		return err
	}
	if b.cfg.ClusterBackend == nil {
		b.metrics.RecordDroppedMessage("target_unknown")
		return nil
	}
	return b.spreadTarget(ctx, clientID, pub)
}

// deliverLocal is the local half of PublishToClient: it delivers to the live
// connection of clientID or queues for its stored session, and reports whether
// the client id is a local session at all. A client id that is not in the routing
// index is not local, which is what makes the caller hand the publication to the
// cluster.
func (b *Broker) deliverLocal(ctx context.Context, clientID string, pub Publication) (bool, error) {
	s, ok := b.index.session(namespaceKey(b.namespace, clientID))
	if !ok {
		return false, nil
	}
	target := LocalTarget{
		ClientID:     s.clientID,
		ConnectionID: s.connID,
		QoS:          pub.QoS,
		Retain:       pub.Retain,
		client:       s.client,
	}
	if s.client == nil {
		// An offline session has no connection to name: SendLocal queues through
		// the session key, and its re-check makes a session that reconnected in
		// the meantime receive the publication live.
		target.ConnectionID = ""
		target.sessionKey = s.key
	}
	return true, b.SendLocal(ctx, target, pub)
}

// spreadTarget hands a directed publication to the cluster backend. The event
// carries the client id and the publication; namespace scoping, the origin/seq
// stamp and duplicate suppression are the ones every cluster event uses.
func (b *Broker) spreadTarget(ctx context.Context, clientID string, pub Publication) error {
	msg := ClusterMessage{
		Type:      ClusterTarget,
		NodeID:    b.nodeID,
		Namespace: pub.Namespace,
		ClientID:  clientID,
		Topic:     pub.Topic,
		Payload:   append([]byte(nil), pub.Payload...),
		QoS:       pub.QoS,
		Retain:    pub.Retain,
		Props:     pub.Props,
	}
	if err := b.cfg.ClusterBackend.Spread(ctx, msg); err != nil {
		return fmt.Errorf("gmqx: cluster target spread: %w", err)
	}
	return nil
}

// spreadKick tells peer nodes of this namespace to drop the connection they hold
// for clientID. Every successful CONNECT spreads one, in both connect modes: the
// session a client takes over may live on another node, and there it has to be
// closed now — the reference implementation of this broker family spreads a
// CLOSE on connect for exactly this reason. Without it a reconnect to a different
// node leaves a zombie connection behind until its keep-alive expires, and every
// delivery is duplicated until then.
//
// The event names a client id, not a topic, so it goes to every peer rather than
// only to the nodes with a matching subscription summary. Namespace and
// deduplication are the ones every cluster event uses: the namespace travels with
// the event and a backend refuses to produce it for another scope; the built-in
// backends stamp Origin and Seq on locally produced events and drop an event
// whose origin is the receiving node, so a re-broadcast topology cannot loop and
// a redelivered kick is applied once.
//
// connectTime is the epoch of the event: the moment this node registered the
// connection that made the client id its own. It travels as
// ClusterMessage.ConnectTime so the receiving node can order the kick against the
// connection it holds, instead of closing one that is newer than the one the
// CONNECT actually took over (see OnRemoteKick).
func (b *Broker) spreadKick(ctx context.Context, clientID string, connectTime time.Time) {
	if b.cfg.ClusterBackend == nil || clientID == "" {
		return
	}
	msg := ClusterMessage{
		Type:        ClusterKick,
		NodeID:      b.nodeID,
		Namespace:   b.namespace,
		ClientID:    clientID,
		ConnectTime: connectTime.UnixNano(),
	}
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
	// The origin elected this node for some shared groups: deliver one member of
	// each. A node the event does not name delivers no shared copy, which is what
	// keeps a publication from reaching one member per node.
	for _, group := range b.sharedGroupsFor(msg.Shared) {
		b.deliverSharedGroup(ctx, group, pub)
	}
}

// OnRemoteKick implements ClusterHandler. A peer node reports that a client id it
// accepted is connected elsewhere (every successful CONNECT spreads one, see
// spreadKick), so a connection this node holds for that id is usually a zombie and
// is displaced: an MQTT 5 client gets DISCONNECT 0x8E (Session taken over) and the
// socket, then the will is not published (the session moved, it did not die).
//
// A kick is applied only when it is newer than the connection it would close, so a
// delayed or reordered kick cannot displace a connection that is younger than the
// one the peer's CONNECT actually replaced:
//
//   - ClusterMessage.ConnectTime is the origin connection's registration time, and
//     the incumbent here is closed only when that epoch is strictly greater than
//     this node's own connectedAt for the client id;
//   - equal epochs are broken deterministically by node id — the connection on the
//     lexicographically greater node id wins — so two nodes that accepted the same
//     id at the same instant reach the same decision instead of kicking each other
//     in a loop;
//   - the Config.NotKickSeconds anti-flap window applies here exactly as it does to
//     a local CONNECT (see insideNotKickWindow): a kick that arrives while the
//     incumbent is still inside its window is refused, so a peer can never close a
//     younger connection;
//   - a kick that carries no epoch comes from a peer built before kick epochs. It
//     is applied unconditionally, which is the behaviour of that peer's build, and
//     counted as "cluster_kick_legacy": a mixed cluster degrades to the previous
//     semantics instead of refusing every kick.
//
// A refused kick is counted ("cluster_kick_stale" for a lost epoch comparison,
// "cluster_kick_rate_limit" for the anti-flap window) and logged at Debug; it
// never displaces, never cancels a pending will and never touches presence, because
// the connection it names is not the one the peer replaced.
//
// A kick for another namespace is dropped: a client id is only unique inside its
// namespace, so applying it would close an unrelated session that happens to
// share the id.
func (b *Broker) OnRemoteKick(msg ClusterMessage) {
	if msg.Namespace != b.namespace {
		b.metrics.RecordDroppedMessage("cluster_namespace")
		b.log.Debug("gmqx remote kick dropped", "namespace", msg.Namespace, "client", msg.ClientID)
		return
	}
	key := namespaceKey(b.namespace, msg.ClientID)
	// The incumbent's registration time is written under b.mu (register), so it is
	// read under the same lock here.
	b.mu.RLock()
	c := b.clients[key]
	var incumbent time.Time
	if c != nil {
		incumbent = c.connectedAt
	}
	b.mu.RUnlock()

	verdict := b.kickVerdict(msg, c != nil, incumbent)
	if verdict != kickApply && verdict != kickLegacy {
		b.metrics.RecordDroppedMessage(verdict.dropReason())
		b.log.Debug("gmqx remote kick refused",
			"reason", verdict.dropReason(), "client", msg.ClientID, "origin", kickOrigin(msg),
			"kick_time", msg.ConnectTime, "incumbent_time", incumbent.UnixNano())
		return
	}
	if verdict == kickLegacy {
		// Applied, not dropped: the counter exists so an operator can see a cluster
		// that still holds a peer from before kick epochs, whose kicks cannot be
		// ordered.
		b.metrics.RecordDroppedMessage(verdict.dropReason())
		b.log.Debug("gmqx remote kick applied without an epoch", "client", msg.ClientID, "origin", kickOrigin(msg))
	}

	// The peer accepted this client id, so its session is alive: a will this node
	// was still holding for it (from the connection a previous take-over ended)
	// belongs to a session that has come back.
	b.cancelPendingWill(key)
	if c != nil {
		// The displaced connection's teardown drops the routing entry and the
		// presence claim of this node (see unregister).
		c.displace()
		return
	}
	// No connection here holds the id. A stored session of this node stays behind
	// (its queue is local to the node), but the client id itself belongs to the
	// peer that just accepted it: this node must stop refreshing a presence record
	// that would fight the new owner's, or a directed publish would keep being
	// routed to a node whose session has no connection.
	b.forgetClientPresence(msg.ClientID)
}

// kickVerdict is what a peer kick may do to the connection this node holds for a
// client id.
type kickVerdict int

const (
	// kickApply displaces the incumbent: the kick is newer than it, or there is no
	// incumbent to compare with.
	kickApply kickVerdict = iota
	// kickLegacy displaces the incumbent without an ordering check, because the
	// peer that sent the kick predates kick epochs.
	kickLegacy
	// kickStale refuses the kick: the incumbent is not older than the connection
	// that produced it.
	kickStale
	// kickRateLimited refuses the kick: the incumbent is still inside its
	// Config.NotKickSeconds anti-flap window.
	kickRateLimited
)

// dropReason is the Metrics.RecordDroppedMessage reason a verdict is counted
// under. A refused kick is a drop; a legacy kick is counted as well, although it
// is applied.
func (v kickVerdict) dropReason() string {
	switch v {
	case kickLegacy:
		return "cluster_kick_legacy"
	case kickStale:
		return "cluster_kick_stale"
	case kickRateLimited:
		return "cluster_kick_rate_limit"
	}
	return ""
}

// kickVerdict decides what a peer kick may do to the connection this node holds
// for a client id. See OnRemoteKick for the rules; they exist so that a kick is
// only ever applied to a connection that is older than the connection that
// produced it.
//
// The order of the checks is deliberate. A kick without an epoch comes first and
// is applied unconditionally: its sender predates epochs, applies every kick it
// receives, and refusing it would leave the zombie the event exists to remove
// while the peer never learns why. Both refusals therefore only exist between two
// nodes that can compare epochs.
func (b *Broker) kickVerdict(msg ClusterMessage, hasIncumbent bool, incumbent time.Time) kickVerdict {
	if !hasIncumbent {
		// Nothing to close; the event still carries the presence signal the caller
		// acts on.
		return kickApply
	}
	if msg.ConnectTime == 0 {
		return kickLegacy
	}
	if b.insideNotKickWindow(incumbent) {
		// The peer accepted the id, but the connection here is younger than the
		// anti-flap window, so it cannot be the connection the peer replaced. This
		// is the receive-side half of Config.NotKickSeconds: two nodes cannot take
		// a client id away from each other faster than the window allows.
		return kickRateLimited
	}
	kickAt := time.Unix(0, msg.ConnectTime)
	switch {
	case kickAt.After(incumbent):
		// Newer than the connection this node holds: the peer replaced it.
		return kickApply
	case kickAt.Before(incumbent):
		// Older than the connection this node holds, so this node's connection is
		// the one that replaced the peer's: the kick is a leftover.
		return kickStale
	}
	// The same instant: break the tie by node id, the same way on both nodes, so
	// the two ends of one client id agree on which connection survives instead of
	// both closing or both keeping. The connection on the greater node id wins.
	if kickOrigin(msg) > b.nodeID {
		return kickApply
	}
	return kickStale
}

// kickOrigin names the node that produced a kick: a backend stamps Origin, and a
// backend that only stamps NodeID still identifies the sender.
func kickOrigin(msg ClusterMessage) string {
	if msg.Origin != "" {
		return msg.Origin
	}
	return msg.NodeID
}

// OnRemoteTarget implements targetHandler: a peer node handed this node a
// directed publication (Broker.PublishToClient) for a client id that was not local
// to it.
// The publication is delivered exactly like a local directed publish: to the live
// connection of the client id, or queued for its persistent session when it is
// offline. Properties travel with the event, so an MQTT 5 subscriber sees the
// same PUBLISH a local directed publish would produce.
//
// The event is applied only when the client id is a session of this node's
// namespace: a target event carries the namespace of the publication, and one of
// another namespace never reaches a session — the client id of another scope is an
// unrelated client. A node that does not hold the id drops the event and counts it
// with Metrics.RecordDroppedMessage("target_unknown"): the TCP backend broadcasts
// a target event to every peer, so most receivers are expected not to hold it,
// while a presence-routed backend (Redis) only sends it to the node that does.
//
// The publication was authorized by the caller of PublishToClient, so it is not
// run through this node's ACL, interceptors or sink, and it is not re-dispatched:
// it is delivered to one session, not routed again.
func (b *Broker) OnRemoteTarget(msg ClusterMessage) {
	if msg.Namespace != b.namespace {
		b.metrics.RecordDroppedMessage("cluster_namespace")
		b.log.Debug("gmqx remote target dropped", "namespace", msg.Namespace, "client", msg.ClientID)
		return
	}
	if msg.ClientID == "" {
		b.metrics.RecordDroppedMessage("target_unknown")
		return
	}
	props, err := decodePropertyBlock(msg.Props)
	if err != nil {
		b.log.Warn("gmqx cluster target properties", "err", err)
		return
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
	delivered, err := b.deliverLocal(ctx, msg.ClientID, pub)
	if !delivered {
		b.metrics.RecordDroppedMessage("target_unknown")
		b.log.Debug("gmqx cluster target dropped", "client", msg.ClientID, "reason", "no local session")
		return
	}
	if err != nil {
		b.log.Debug("gmqx cluster target delivery failed", "client", msg.ClientID, "err", err)
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
