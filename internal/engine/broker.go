package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/maxid/gmqx/conn"
	"github.com/maxid/gmqx/hook"
	mqtt5 "github.com/maxid/gmqx/internal/mqtt5"
	ns "github.com/maxid/gmqx/internal/namespace"
	topicmatch "github.com/maxid/gmqx/internal/topic"
	retainedstore "github.com/maxid/gmqx/retained"
	"github.com/maxid/gmqx/routing"
	"github.com/maxid/gmqx/session"
	sessionstate "github.com/maxid/gmqx/session"
	sessionstore "github.com/maxid/gmqx/session"
	"github.com/maxid/gnety"
	"github.com/maxid/gnety/codec/mqtt"
	"github.com/maxid/gplugin"
)

// Broker is a single-node MQTT 3.1.1 broker.
type Broker struct {
	cfg Config
	log *slog.Logger
	// ext is the current snapshot of the swappable extensions: auth, ACL, sink,
	// metrics, interceptors and payload transcoders (see plugin.go). It is atomic
	// because a plugin may swap one while the broker serves; extMu serialises the
	// writers.
	ext   atomic.Pointer[extensions]
	extMu sync.Mutex
	// codecs is the registry of payload encodings the loaded plugins contributed,
	// and plugins the registry that controls them. The broker never picks a codec
	// itself.
	codecs     *hook.CodecSet
	plugins    *gplugin.Registry[hook.Host]
	limiter    *rateLimiter
	nodeID     string
	namespace  string
	dispatcher Dispatcher
	store      SessionStore
	// index is the routing table: which local session (live or stored) is
	// subscribed to which filter. It is the only structure a publication is
	// matched against; gmqx/routing owns its shape and invariants.
	index *routing.Index
	// presence is the optional cluster view of which node holds a client id. The
	// backend that implements it is told about every connect and disconnect, so a
	// directed publish can be routed instead of broadcast.
	presence clientPresence
	// sharedNodes is the optional cluster view of which nodes hold a live member of
	// a shared subscription group. It is what lets one node deliver a shared
	// publication instead of every node delivering a copy (see shared.go).
	sharedNodes sharedDirectory
	// sharedElection rotates the node election of each shared group, so successive
	// publications of one group do not all land on the same node (see shared.go).
	// The zero value is an empty election.
	sharedElection routing.Election

	// readBytes and writeBytes are the broker-wide byte-rate buckets: every byte
	// this process reads from a client connection or writes to one is charged to
	// them, which is what makes MaxTotalReadByteRate/MaxTotalWriteByteRate a limit
	// on the whole broker instead of on one connection. A disabled limit is nil
	// (the shaper itself lives in gmqx/conn).
	readBytes  *conn.Buckets
	writeBytes *conn.Buckets

	mu       sync.RWMutex
	ctx      context.Context // broker lifetime context, set by Start
	clients  map[string]*conn.Client
	retained *retainedstore.Store
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
	// Defaults of the byte-rate limits. They are high enough to leave normal
	// traffic untouched: a device publishing a few kilobytes per second is three
	// orders of magnitude below them, and the burst absorbs any single
	// maximum-size packet (the shaping itself lives in gmqx/conn).
	defaultMaxReadByteRate       = 8 << 20   // 8 MiB/s per connection
	defaultMaxWriteByteRate      = 16 << 20  // 16 MiB/s per connection
	defaultMaxTotalReadByteRate  = 256 << 20 // 256 MiB/s for the whole broker
	defaultMaxTotalWriteByteRate = 256 << 20
	// defaultPortFallback is how many ports above the configured one a listener
	// may try before Start fails.
	defaultPortFallback = 16
)

// maxReceiveMaximum is the protocol default and the highest value the MQTT 5
// Receive Maximum property can carry: an absent property means this window.
const maxReceiveMaximum = 65535

// New creates a Broker. Start must be called to listen.
// defaultGossipConfig maps the broker's ClusterConfig onto the gossip backend
// configuration. It is a function of its own because the backend lives in
// gmqx/cluster: a test in this package can pin the mapping without naming the
// unexported backend type.
func defaultGossipConfig(c ClusterConfig, namespace string, metrics hook.MetricsManager, logger *slog.Logger) GossipClusterConfig {
	return GossipClusterConfig{
		Addr:                c.Addr,
		Seeds:               append([]string(nil), c.Seeds...),
		PingInterval:        c.PingInterval,
		PingTimeout:         c.PingTimeout,
		SuspicionMultiplier: c.SuspicionMultiplier,
		Fanout:              c.Fanout,
		Namespace:           namespace,
		Metrics:             metrics,
		Logger:              logger,
	}
}

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
	if !cfg.Namespace.IsZero() {
		// A namespace that becomes part of a key must be valid before it is used:
		// gns owns the rule, the broker owns what the name means.
		if err := cfg.Namespace.Validate(); err != nil {
			return nil, fmt.Errorf("gmqx: %w", err)
		}
	}
	if cfg.NodeID == "" {
		cfg.NodeID = conn.RandomID()
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
		cfg.ClusterBackend = NewGossipCluster(defaultGossipConfig(cfg.Cluster, cfg.Namespace.Name, cfg.Metrics, logger))
	}
	b := &Broker{
		cfg:       cfg,
		log:       logger,
		namespace: cfg.Namespace.Name,
		clients:   make(map[string]*conn.Client),
		store:     cfg.SessionStore,
		index:     routing.NewIndex(),
		retained:  retainedstore.NewStore(),
		wills:     make(map[string]*pendingWill),
		addrs:     make(map[string]string),
		done:      make(chan struct{}),
		nodeID:    cfg.NodeID,
	}
	// The swappable half of the configuration. Config keeps the initial values, so a
	// broker created from a Config behaves exactly as before; the snapshot is what
	// the serving path reads, which is what lets a plugin replace a hook at run time.
	b.ext.Store(&extensions{
		auth:         cfg.Auth,
		acl:          cfg.ACL,
		sink:         cfg.Sink,
		metrics:      cfg.Metrics,
		interceptors: interceptorSlots(cfg.Interceptors),
	})
	b.codecs = hook.NewCodecSet()
	b.plugins = gplugin.NewRegistry[hook.Host](b)
	// The broker-wide half of the byte-rate shaping: one bucket each for every
	// byte read from and written to every connection of this process. A disabled
	// limit (negative) is a nil bucket, which the shaper skips.
	b.readBytes = conn.NewBuckets(cfg.MaxTotalReadByteRate, 0)
	b.writeBytes = conn.NewBuckets(0, cfg.MaxTotalWriteByteRate)
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
			scoped.SetNamespace(b.namespace)
		}
		if err := b.cfg.ClusterBackend.Start(ctx, b.nodeID, backendHandler{b: b}); err != nil {
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
	// maxFrameSize is the largest inbound WebSocket message this listener
	// accepts. gnety caps a reassembled message at 4 MiB when it is left at zero,
	// which would make Config.MaxPacketSize above 4 MiB unreachable over ws/wss:
	// that 4 MiB is a library default, not a protocol limit (RFC 6455 has none,
	// and the MQTT spec lets one frame carry several or partial packets), so the
	// listener carries the broker's own packet limit instead. It stays zero for
	// the TCP and TLS listeners, which have no message framing of their own.
	maxFrameSize int
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
			maxFrameSize:   b.cfg.MaxPacketSize,
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
			maxFrameSize:   b.cfg.MaxPacketSize,
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
			MaxFrameSize:   spec.maxFrameSize,
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

// wsaEAddrInUse is WSAEADDRINUSE, the Winsock error of a busy port. It is not
// syscall.EADDRINUSE: on Windows the latter is an invented MSVCRT value
// (APPLICATION_ERROR+2), so the two do not compare equal.
const wsaEAddrInUse = 10048

// addrInUse reports whether err is the "address already in use" failure of a bind,
// the only listener error a port fallback can fix. Three shapes reach this: the
// Unix errno, the Winsock errno (a plain comparison, because it has no portable
// name in package syscall), and an error that only carries the message (a wrapped
// or synthesized error). The message fallback is last because it is the fragile
// one: Windows localizes it, and the numeric checks do not.
func addrInUse(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EADDRINUSE) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) && uintptr(errno) == wsaEAddrInUse {
		return true
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
		b.metrics().RecordDroppedMessage("connection_rate_limit")
		_ = conn.Close()
		return
	}
	if b.cfg.MaxConnections > 0 {
		b.mu.RLock()
		n := len(b.clients)
		b.mu.RUnlock()
		if n >= b.cfg.MaxConnections {
			b.metrics().RecordDroppedMessage("max_connections")
			b.metrics().RecordOverflow("connections")
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
	b.metrics().RecordDroppedMessage("receive_maximum_full")
	b.metrics().RecordOverflow("receive_maximum")
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
	clients := make([]*conn.Client, 0, len(b.clients))
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
		c.Close()
	}
	// The wills whose delay has not elapsed go with the broker: there is nowhere
	// left to publish them, and their timers must not outlive Stop.
	b.dropPendingWills()
	// The sweeper waits on done, which is closed above; waiting here makes Stop
	// mean "every goroutine this broker started has ended".
	b.background.Wait()
	return errors.Join(errs...)
}

func (b *Broker) serve(netConn net.Conn) {
	// Byte-rate shaping is applied by the transport wrapper, so every byte read
	// from or written to this connection — the CONNECT phase, the packet codec
	// and the shutdown DISCONNECT alike — passes one bucket (see byte_rate.go).
	netConn = b.shapeConn(netConn)
	c := conn.New(b, netConn)
	// A safety net: close cancels the context as soon as the connection is gone,
	// this covers a connection that failed before close was reached.
	defer c.StopContext()
	// The writer goroutine is joined as well: a connection that never reaches
	// close (a handshake that fails after the CONNACK) still ends it here.
	defer c.StopWriter()
	if err := c.Handshake(); err != nil {
		if c.Registered() {
			// The connection is gone, so the disconnect hook observes the cancelled
			// context, exactly as it does on the normal path.
			c.StopContext()
			b.unregister(c)
		}
		b.log.Debug("gmqx handshake failed", "remote", netConn.RemoteAddr(), "err", err)
		_ = netConn.Close()
		return
	}
	defer b.unregister(c)
	c.Loop()
}

// shapeConn applies the broker's byte-rate limits to one accepted connection. The
// shaper itself lives in gmqx/conn, which is where the connection I/O is: this is
// the broker's half — the per-listener settings and the broker-wide buckets.
func (b *Broker) shapeConn(netConn net.Conn) net.Conn {
	if b.cfg.MaxReadByteRate <= 0 && b.cfg.MaxWriteByteRate <= 0 && b.readBytes == nil && b.writeBytes == nil {
		// The common case: no shaping at all, and no wrapper around the socket.
		return netConn
	}
	return conn.Shape(netConn, conn.Shaping{
		ReadRate:   b.cfg.MaxReadByteRate,
		WriteRate:  b.cfg.MaxWriteByteRate,
		ReadTotal:  b.readBytes,
		WriteTotal: b.writeBytes,
		Metrics:    b.metrics(),
	})
}

// ByteRateMetrics is the optional extension of hook.MetricsManager that observes
// byte-rate shaping. It is defined with the shaper in gmqx/conn and re-exported
// here, so a metrics implementation names the same type through either import.
type ByteRateMetrics = conn.ByteRateMetrics

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
func (b *Broker) register(c *conn.Client) error {
	key := namespaceKey(b.namespace, c.ID())
	b.mu.Lock()
	var takeover *sessionstate.State
	if old, ok := b.clients[key]; ok && old != c {
		if b.cfg.ConnectMode == ConnectModeUnique {
			b.mu.Unlock()
			return ErrClientIDInUse
		}
		if !b.takeoverAllowed(old) {
			b.mu.Unlock()
			b.metrics().RecordDroppedMessage("takeover_rate_limit")
			return ErrTakeoverRateExceeded
		}
		delete(b.clients, key)
		// In KICK mode the replacement takes the old session over. The old
		// connection must neither save that state again nor erase the new
		// session when its own serve goroutine unwinds.
		old.MarkSuperseded()
		if old.PersistentSession() {
			old.TakeOver(func(h *conn.Handoff) bool {
				if stored, ok := b.takeStoredSession(key); ok {
					takeover = stored
					return false
				}
				// The old connection owns the state, so the replacement takes it over
				// directly — the QoS 2 state of both directions included, exactly as a
				// store would have carried it. The deliveries it still owed its inflight
				// window move into the queue it hands over instead of being lost with
				// it: a client that reconnects before unregister has stored the session
				// gets them now, one that reconnects later takes them from the store.
				takeover = &sessionstate.State{
					ClientID:    h.ClientID,
					Subs:        h.Subs,
					Queue:       b.adoptQueue(h),
					ExpiresAt:   h.ExpiresAt,
					InboundQoS2: h.InboundQoS2,
					Inflight:    h.Inflight,
				}
				return true
			})
		}
		b.mu.Unlock()
		old.Displace()
		b.mu.Lock()
	}
	if b.cfg.MaxConnections > 0 && len(b.clients) >= b.cfg.MaxConnections {
		b.mu.Unlock()
		return ErrMaxConnections
	}

	if c.ResumesSession() {
		if stored, ok := b.takeStoredSession(key); ok {
			c.Adopt(stored)
		} else if takeover != nil {
			c.Adopt(takeover)
		}
	} else {
		b.deleteStoredSession(key)
	}
	// A resumed session without subscriptions (or a store that returns none) must
	// still be able to accept a SUBSCRIBE.
	c.EnsureSubscriptions()

	b.clients[key] = c
	c.MarkRegistered(time.Now())
	// The routing table follows the session into the connection: the filters it
	// restored (or the empty set of a clean session) are routable before the
	// CONNACK goes out, and the stored entries of a session that was taken over
	// stop being targets here, under the same lock that moves the session.
	b.index.SetLive(key, c, c.Subscriptions())
	n := len(b.clients)
	b.mu.Unlock()
	if c.HasSubscriptions() {
		b.subscriptionsChanged()
	}

	// A new connection to this session cancels the will its previous connection
	// asked to delay: MQTT 5 forbids publishing it once the session is resumed. A
	// CONNECT that discards the session instead (Clean Start set) ends it right
	// here, so the pending will of that session goes out now.
	if c.ResumesSession() {
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
	b.spreadKick(c.Context(), c.ID(), c.ConnectedAt())
	// A backend that tracks client-id presence is told about the connection, so a
	// directed publish for this id can address this node instead of every node.
	// The call only enqueues the presence write: no store or network call runs on
	// the CONNECT path under the broker lock (see clientPresence).
	if b.presence != nil {
		b.presence.ClientConnected(c.ID())
	}

	b.metrics().IncrementConnections()
	b.metrics().RecordConnections(n)
	if b.sink() != nil {
		b.sink().OnConnect(c.Context(), c.Info())
	}
	return nil
}

// takeoverAllowed reports whether a KICK-mode CONNECT may displace the live
// connection old, which is how Config.NotKickSeconds works as the anti-flap
// window it is meant to be: the session a client id just installed is protected
// for that long, so two ends of one client id cannot displace each other in a
// loop. The caller holds b.mu, which is what orders the write of connectedAt in
// register with this read.
func (b *Broker) takeoverAllowed(old *conn.Client) bool {
	return !b.insideNotKickWindow(old.ConnectedAt())
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

func (b *Broker) unregister(c *conn.Client) {
	if !c.MarkUnregistered() {
		return
	}
	key := namespaceKey(b.namespace, c.ID())
	b.mu.Lock()
	if current, ok := b.clients[key]; ok && current == c {
		delete(b.clients, key)
	}
	n := len(b.clients)
	hadSubscriptions := c.HasSubscriptions()
	superseded := c.Superseded()
	kicked := c.Kicked()
	persistent := c.PersistentSession()
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
	switch {
	case movedAway:
		// The session belongs to another connection now, so this node writes nothing to
		// the store: it neither saves its own copy back nor deletes the key (see
		// above). A delivery that still arrives with a target of this connection is
		// queued through the offline path — which reaches the replacement when it is a
		// local one — and a clean session, which has no queue anywhere, ends with the
		// connection as it always did.
		if persistent {
			dropped := c.Discard()
			for i := 0; i < dropped; i++ {
				b.metrics().RecordDroppedMessage("offline_session_missing")
			}
		} else {
			c.DropPending()
		}
	case persistent:
		// The deliveries the connection still owed its inflight window are not lost
		// with it: they join the session queue before it is stored (adoptQueue), so
		// an inflight overflow — which is what takes a session offline in the first
		// place — keeps what the publisher was already acknowledged for. The store
		// write and the handoff happen under the connection's lock, so no delivery
		// can slip between them.
		c.HandOff(func(h *conn.Handoff) { b.saveStoredSession(key, h) })
	default:
		b.deleteStoredSession(key)
		// A clean session has no queue and no store entry: what it still owed ends
		// with the connection, which is what declaring the session clean means.
		c.DropPending()
	}
	if movedAway {
		// A displaced connection whose replacement is not on this node (a peer's
		// connect-time kick moved the session away) must not leave a live routing
		// entry behind: nothing here can deliver to it any more. A local take-over
		// already installed the replacement's entries under this lock, so the entry
		// is kept in that case.
		if _, connected := b.clients[key]; !connected {
			b.index.Remove(key)
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
			b.forgetClientPresence(c.ID())
		}
	}

	b.metrics().DecrementConnections()
	b.metrics().IncrementDisconnectEvents()
	b.metrics().RecordConnections(n)
	if hadSubscriptions {
		// The session's filters left the routing summary with it.
		b.subscriptionsChanged()
	}
	if b.sink() != nil {
		// The disconnect hooks observe the cancelled connection context (the
		// connection is already gone) but it still carries its ConnInfo, so a hook
		// can attribute the event to the exact connection.
		if c.Lost() {
			b.sink().OnConnectionLost(c.Context(), c.ID(), c.CloseErr())
		} else {
			b.sink().OnDisconnect(c.Context(), c.ID(), c.CloseErr())
		}
	}
}

func (b *Broker) subscribe(c *conn.Client, filter string, sub sessionstate.Subscription) (byte, bool) {
	// A shared subscription is authorized by the filter it delivers: "$share/g/t"
	// receives the publications of "t", so an ACL that allows "t" allows it and a
	// rule written for the group name would not be consulted. The full filter is
	// what the session stores, the index keys and an UNSUBSCRIBE names.
	aclFilter := filter
	if shared, isShared, ok := topicmatch.ParseSharedFilter(filter); isShared && ok {
		aclFilter = shared.Filter
	}
	if !b.checkACL(c.Context(), SessionInfo{ClientID: c.ID(), Username: c.Username()}, aclFilter, AclSubscribe) {
		b.metrics().RecordDroppedMessage("acl_subscribe")
		return subscribeFailure(c.Version(), ReasonNotAuthorized), false
	}
	granted, exists := c.InstallSubscription(filter, sub, b.cfg.MaxTopicSubscriptions)
	if !granted {
		b.metrics().RecordDroppedMessage("max_subscriptions")
		return subscribeFailure(c.Version(), ReasonQuotaExceeded), exists
	}
	// The subscription is routable before the SUBACK is written: the connection
	// goroutine updates the index here, and the SUBACK follows in handleSubscribe.
	b.index.Add(namespaceKey(b.namespace, c.ID()), c, filter, sub)
	b.metrics().IncrementSubscribeEvents()
	if !exists {
		b.subscriptionsChanged()
	}
	return sub.QoS, exists
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

func (b *Broker) unsubscribe(c *conn.Client, filter string) {
	if c.RemoveSubscription(filter) {
		// The filter stops routing before the UNSUBACK is written.
		b.index.RemoveFilter(namespaceKey(b.namespace, c.ID()), c, filter)
		b.metrics().DecrementSubscribeEvents()
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
	return b.index.LiveFilters()
}

// sharedSubscriptionGroups implements sharedSubscriptionProvider: the shared
// subscription groups this node holds at least one live member of. It is the
// shared half of the routing summary, and it is what lets one node elect the node
// that delivers a publication for a group instead of every node delivering a copy
// (see Broker.dispatchShared). Stored sessions are not advertised: an offline
// member receives no shared delivery.
func (b *Broker) sharedSubscriptionGroups() []string {
	return b.index.LiveSharedGroups()
}

// subscriptionsChanged tells a cluster backend that can push subscription
// summaries that the local filter set changed.
func (b *Broker) subscriptionsChanged() {
	notifier, ok := b.cfg.ClusterBackend.(subscriptionChangeNotifier)
	if !ok {
		return
	}
	notifier.SubscriptionsChanged()
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
	if len(payload) == 0 {
		b.retained.Delete(key)
		return
	}
	m := retainedstore.Message{
		Namespace: namespace,
		Topic:     topic,
		Payload:   append([]byte(nil), payload...),
		QoS:       qos,
		Props:     props,
	}
	if expiry := props.MessageExpiryInterval; expiry > 0 {
		m.ExpiresAt = time.Now().Add(time.Duration(expiry) * b.second())
	}
	if !b.retained.Put(key, m, b.cfg.MaxRetainMessages) {
		b.metrics().RecordDroppedMessage("max_retain")
	}
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
func (b *Broker) sendRetained(c *conn.Client, filter string, qos byte, retainAsPublished bool, ids []uint32) {
	now := time.Now()
	type replay struct {
		topic   string
		payload []byte
		qos     byte
		props   mqtt.Properties
	}
	var msgs []replay
	b.retained.Range(func(key string, m retainedstore.Message) bool {
		if m.Namespace != c.Namespace() {
			return true
		}
		if !topicmatch.Match(filter, m.Topic) {
			return true
		}
		props := m.Props
		if !m.ExpiresAt.IsZero() {
			remaining := m.ExpiresAt.Sub(now)
			if remaining <= 0 {
				b.metrics().RecordDroppedMessage("retained_expired")
				return false
			}
			props.MessageExpiryInterval = b.expiryInterval(remaining)
		}
		msgs = append(msgs, replay{topic: m.Topic, payload: m.Payload, qos: m.QoS, props: props})
		return true
	})
	for _, m := range msgs {
		props, dropped := mqtt5.WithSubscriptionIDs(m.props, ids)
		if dropped {
			b.metrics().RecordDroppedMessage("subscription_identifier_limit")
		}
		_ = c.DeliverPublish(m.topic, m.payload, minQoS(qos, m.qos), true, props)
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
func (b *Broker) publish(from conn.Origin, ev PublishEvent, props mqtt.Properties) PublishResult {
	ctx := from.Context
	ev.ConnectionID = from.ConnID
	ev.ClientID = from.ClientID
	if !b.checkACL(ctx, SessionInfo{ClientID: from.ClientID, Username: from.Username}, ev.Topic, AclPublish) {
		b.metrics().RecordDroppedMessage("acl_publish")
		return PublishResult{Disposition: PublishRejected, ReasonCode: ReasonNotAuthorized}
	}
	for _, it := range b.exts().interceptors {
		if !it.fn.OnPublish(ctx, from.ClientID, ev.Topic, ev.Payload, ev.QoS) {
			b.metrics().RecordDroppedMessage("interceptor")
			return PublishResult{Disposition: PublishRejected, ReasonCode: ReasonNotAuthorized}
		}
	}
	if res := b.callSink(ctx, ev); !res.Accepted() {
		reason := "sink_rejected"
		if res.Disposition == PublishRetryable {
			reason = "sink_retryable"
		}
		b.metrics().RecordDroppedMessage(reason)
		return res
	}
	// Payload transcoding is the application's encoding seam (JSON in, binary out):
	// it runs after the sink accepted the publication, so a rejected or retryable
	// message is never transformed, and it changes only the payload — topic, QoS,
	// retain and properties stay what the client sent, which is what keeps MQTT
	// semantics intact. A broker without transcoders skips this entirely.
	if trans := b.exts().transcoders; len(trans) > 0 {
		for _, tr := range trans {
			payload, ok := tr.fn.Transcode(ctx, ev)
			if !ok {
				b.metrics().RecordDroppedMessage("transcoder")
				return PublishResult{Disposition: PublishRejected}
			}
			ev.Payload = payload
		}
	}
	// Retain is stored only for accepted publishes: a rejected message must not
	// become the retained value new subscribers receive.
	if ev.Retain {
		b.storeRetained(from.Namespace, ev.Topic, ev.Payload, ev.QoS, props)
	}
	b.metrics().IncrementPublishedMessages()

	pub := Publication{
		Namespace:        from.Namespace,
		Topic:            ev.Topic,
		Payload:          ev.Payload,
		QoS:              ev.QoS,
		Retain:           ev.Retain,
		Dup:              ev.Dup,
		FromClientID:     from.ClientID,
		FromConnectionID: string(from.ConnID),
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

// checkACL runs the ACL hook of an authorization decision under Config.AuthTimeout.
// An expired call is a denial: a client whose ACL cannot answer in time is not
// given the benefit of the doubt. The built-in AllowAll cannot block, so it takes
// the synchronous path and keeps the hot publish path free of a goroutine.
func (b *Broker) checkACL(ctx context.Context, session SessionInfo, topic string, action AclAction) bool {
	if _, builtin := b.acl().(AllowAll); builtin {
		return b.acl().Check(ctx, session, topic, action)
	}
	timeout := b.cfg.AuthTimeout
	if timeout <= 0 {
		return b.acl().Check(ctx, session, topic, action)
	}
	aclCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- b.acl().Check(aclCtx, session, topic, action) }()
	select {
	case allowed := <-done:
		return allowed
	case <-aclCtx.Done():
		if errors.Is(aclCtx.Err(), context.DeadlineExceeded) {
			if b.log != nil {
				b.log.Warn("gmqx ACL check deadline exceeded", "client", session.ClientID,
					"topic", topic, "action", action, "timeout", timeout)
			}
			b.metrics().RecordDroppedMessage("acl_timeout")
		}
		return false
	}
}

// authenticate calls the authentication hook under Config.AuthTimeout. An expired
// call is an authentication failure: a client whose Authenticator does not answer
// in time is refused, never admitted.
func (b *Broker) authenticate(ctx context.Context, info ConnectInfo) error {
	switch b.auth().(type) {
	case AllowAll, DenyAll:
		return b.auth().Authenticate(ctx, info)
	}
	timeout := b.cfg.AuthTimeout
	if timeout <= 0 {
		return b.auth().Authenticate(ctx, info)
	}
	authCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.auth().Authenticate(authCtx, info) }()
	select {
	case err := <-done:
		return err
	case <-authCtx.Done():
		if errors.Is(authCtx.Err(), context.DeadlineExceeded) {
			if b.log != nil {
				b.log.Warn("gmqx authentication deadline exceeded", "client", info.ClientID,
					"remote", info.RemoteAddr, "timeout", timeout)
			}
			b.metrics().RecordDroppedMessage("auth_timeout")
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
	if b.sink() == nil {
		return PublishResult{Disposition: PublishAccepted}
	}
	timeout := b.cfg.PublishCallbackTimeout
	if timeout <= 0 {
		return b.sink().OnPublish(ctx, ev)
	}
	sinkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan PublishResult, 1)
	go func() { done <- b.sink().OnPublish(sinkCtx, ev) }()
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
	d.broker.eachLocalTarget(pub, func(target LocalTarget) {
		_ = d.broker.SendLocal(ctx, target, pub)
	})
	return d.broker.SpreadPublication(ctx, pub)
}

// LocalTargets is the read-only half of the routing seam a Dispatcher uses: the
// sessions whose subscriptions match pub, with the per-session QoS cap and retain
// flag already applied, and no-local subscriptions of the publishing connection
// excluded. A publication of another namespace has no local target here.
//
// It answers from the routing index (see gmqx/routing), not by scanning the
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
	return b.index.Targets(pub.Topic, ConnectionID(pub.FromConnectionID), pub.QoS, pub.Retain)
}

// eachLocalTarget is LocalTargets without the slice: it hands each matching
// session to fn, in the order the index collected them, after the index lock has
// been released. The built-in dispatcher uses it, so a publication does not
// allocate a target slice of its own.
func (b *Broker) eachLocalTarget(pub Publication, fn func(LocalTarget)) {
	if pub.Namespace != b.namespace {
		return
	}
	b.index.EachTarget(pub.Topic, ConnectionID(pub.FromConnectionID), pub.QoS, pub.Retain, fn)
}

// SendLocal is the write half of the routing seam: it writes pub to one target
// returned by LocalTargets, or queues it for a target that is an offline
// persistent session. The send is synchronous and best-effort, like the broker's
// own delivery path; ctx is the publication's context (the publishing
// connection's context for an accepted local publication) and is carried so a
// dispatcher can observe cancellation, not to abort a write that is already on
// the wire.
func (b *Broker) SendLocal(ctx context.Context, target LocalTarget, pub Publication) error {
	if target.Endpoint() == nil && target.SessionKey() == "" {
		return errors.New("gmqx: local target was not produced by this broker")
	}
	props := pub.props
	if !pub.propsSet {
		decoded, err := mqtt5.DecodePropertyBlock(pub.Props)
		if err != nil {
			return fmt.Errorf("gmqx: local delivery properties: %w", err)
		}
		props = decoded
	}
	// The identifiers belong to the subscriptions the target was reached
	// through, so they are attached here, once per delivery, and never travel in
	// Publication.Props (see subscription_id.go).
	var dropped bool
	props, dropped = mqtt5.WithSubscriptionIDs(props, target.SubscriptionIDs())
	if dropped {
		b.metrics().RecordDroppedMessage("subscription_identifier_limit")
	}
	if target.Endpoint() == nil {
		b.enqueueOffline(target, pub, props)
		return nil
	}
	return target.Endpoint().DeliverPublish(pub.Topic, pub.Payload, target.QoS, target.Retain, props)
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
		b.presence.ClientDisconnected(clientID)
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
	if !topicmatch.ValidTopic(pub.Topic) {
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
		b.metrics().RecordDroppedMessage("target_unknown")
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
	target, ok := b.index.Target(namespaceKey(b.namespace, clientID), pub.QoS, pub.Retain)
	if !ok {
		return false, nil
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
		b.metrics().RecordDroppedMessage("cluster_namespace")
		b.log.Debug("gmqx remote publish dropped", "namespace", msg.Namespace, "topic", msg.Topic)
		return
	}
	props, err := mqtt5.DecodePropertyBlock(msg.Props)
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
		b.metrics().RecordDroppedMessage("cluster_namespace")
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
		incumbent = c.ConnectedAt()
	}
	b.mu.RUnlock()

	verdict := b.kickVerdict(msg, c != nil, incumbent)
	if verdict != kickApply && verdict != kickLegacy {
		b.metrics().RecordDroppedMessage(verdict.dropReason())
		b.log.Debug("gmqx remote kick refused",
			"reason", verdict.dropReason(), "client", msg.ClientID, "origin", kickOrigin(msg),
			"kick_time", msg.ConnectTime, "incumbent_time", incumbent.UnixNano())
		return
	}
	if verdict == kickLegacy {
		// Applied, not dropped: the counter exists so an operator can see a cluster
		// that still holds a peer from before kick epochs, whose kicks cannot be
		// ordered.
		b.metrics().RecordDroppedMessage(verdict.dropReason())
		b.log.Debug("gmqx remote kick applied without an epoch", "client", msg.ClientID, "origin", kickOrigin(msg))
	}

	// The peer accepted this client id, so its session is alive: a will this node
	// was still holding for it (from the connection a previous take-over ended)
	// belongs to a session that has come back.
	b.cancelPendingWill(key)
	if c != nil {
		// The displaced connection's teardown drops the routing entry and the
		// presence claim of this node (see unregister).
		c.Displace()
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
		b.metrics().RecordDroppedMessage("cluster_namespace")
		b.log.Debug("gmqx remote target dropped", "namespace", msg.Namespace, "client", msg.ClientID)
		return
	}
	if msg.ClientID == "" {
		b.metrics().RecordDroppedMessage("target_unknown")
		return
	}
	props, err := mqtt5.DecodePropertyBlock(msg.Props)
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
		b.metrics().RecordDroppedMessage("target_unknown")
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

// The key-space rules live in ./internal/namespace; these wrappers keep the root
// package's call sites and tests unchanged.
func namespaceKey(namespace, name string) string { return ns.Key(namespace, name) }

func keyInNamespace(namespace, key string) bool { return ns.InNamespace(namespace, key) }

// The host side of the connection seam
//
// gmqx/conn owns the protocol state machine; everything it cannot know — the
// namespace, the resolved per-connection limits, the swappable extension
// snapshots, the publish/subscribe/retained path, the session handoff and the
// delayed-will scheduler — is this file. Most methods are one-line adapters over
// the broker's own helpers, so the engine keeps a single implementation of each
// behavior and the connection only sees the interface.
//
// Naming: the interface method is the exported spelling (Metrics, Sink, Register),
// the broker's internal helper keeps its lower-case name (metrics, sink, register).
// A method pair that differs only in case is deliberate here: the exported one is
// the seam, the unexported one is what the rest of the engine calls.

// MaxAuthSteps is the number of AUTH challenges one exchange may run; the limit is
// enforced by the connection's exchange loop, and the constant lives with it.
const MaxAuthSteps = conn.MaxAuthSteps

// Second implements conn.Host: the protocol second a connection measures
// keep-alive, session expiry and will delay in.
func (b *Broker) Second() time.Duration { return b.second() }

// Limits implements conn.Host: the per-connection settings, with the broker's
// defaults already applied by the helpers that own them.
func (b *Broker) Limits() conn.Limits {
	return conn.Limits{
		MaxPacketSize:           b.cfg.MaxPacketSize,
		ReceiveMaximum:          b.receiveMaximum(),
		MaxInflightQoS1:         b.cfg.MaxInflightQoS1,
		MaxInflightQoS2:         b.cfg.MaxInflightQoS2,
		MaxInflightQueueSize:    b.cfg.MaxInflightQueueSize,
		MaxOfflineQueueSize:     b.cfg.MaxOfflineQueueSize,
		MaxTotalOfflineMessages: b.cfg.MaxTotalOfflineMessages,
		MaxOutboundQueueSize:    b.cfg.MaxOutboundQueueSize,
		WriteTimeout:            b.cfg.WriteTimeout,
		// The two resolved settings: a negative will delay disables delayed wills
		// and a zero connect timeout means the documented default, so the broker
		// decides them once (see connectTimeout, maxWillDelayInterval).
		ConnectTimeout:       b.connectTimeout(),
		MaxWillDelayInterval: b.maxWillDelayInterval(),
		AuthTimeout:          b.cfg.AuthTimeout,
		QoSRetryInterval:     b.cfg.QoSRetryInterval,
		QoSMaxRetries:        b.cfg.QoSMaxRetries,
		ConnContext:          b.cfg.ConnContext,
	}
}

// Metrics implements conn.Host.
func (b *Broker) Metrics() hook.MetricsManager { return b.metrics() }

// Sink implements conn.Host.
func (b *Broker) Sink() hook.EventSink { return b.sink() }

// Logger implements conn.Host.
func (b *Broker) Logger() *slog.Logger { return b.log }

// LifecycleContext implements conn.Host.
func (b *Broker) LifecycleContext() context.Context { return b.lifecycleContext() }

// AuthExchange implements conn.Host.
func (b *Broker) AuthExchange(method string) hook.AuthExchange { return b.authExchange(method) }

// Authenticate implements conn.Host.
func (b *Broker) Authenticate(ctx context.Context, info hook.ConnectInfo) error {
	return b.authenticate(ctx, info)
}

// Register implements conn.Host.
func (b *Broker) Register(c *conn.Client) error { return b.register(c) }

// Unregister implements conn.Host.
func (b *Broker) Unregister(c *conn.Client) { b.unregister(c) }

// Publish implements conn.Host.
func (b *Broker) Publish(from conn.Origin, ev hook.PublishEvent, props mqtt.Properties) hook.PublishResult {
	return b.publish(from, ev, props)
}

// Subscribe implements conn.Host.
func (b *Broker) Subscribe(c *conn.Client, filter string, sub session.Subscription) (byte, bool) {
	return b.subscribe(c, filter, sub)
}

// Unsubscribe implements conn.Host.
func (b *Broker) Unsubscribe(c *conn.Client, filter string) { b.unsubscribe(c, filter) }

// SendRetained implements conn.Host.
func (b *Broker) SendRetained(c *conn.Client, filter string, qos byte, retainAsPublished bool, ids []uint32) {
	b.sendRetained(c, filter, qos, retainAsPublished, ids)
}

// ReceiveMaxFull implements conn.Host.
func (b *Broker) ReceiveMaxFull(topic string) { b.receiveMaxFull(topic) }

// EnqueueOffline implements conn.Host.
func (b *Broker) EnqueueOffline(target routing.Target, msg conn.Message) {
	b.enqueueOffline(target, Publication{
		Topic:    msg.Topic,
		Payload:  msg.Payload,
		QoS:      msg.QoS,
		Retain:   msg.Retain,
		props:    msg.Props,
		propsSet: true,
	}, msg.Props)
}

// ScheduleWill implements conn.Host.
func (b *Broker) ScheduleWill(c *conn.Client, delay time.Duration, ev hook.PublishEvent, props mqtt.Properties) {
	b.scheduleWill(c, namespaceKey(b.namespace, c.ID()), delay, ev, props)
}

// Persistent-session bookkeeping
//
// The state itself — the subscriptions to restore, the messages queued while the
// session was offline, the QoS 2 state of both directions and the expiry deadline —
// lives in gmqx/session (State, with Stored/StateFromStored as the projection to
// the SessionStore form). This file is the broker side of it: when the state is
// taken from a store, written back, swept, and how the offline queue and its caps
// follow it.
//
// The broker stores a session only when it has no live connection. While a
// connection owns the session its subscriptions live on the connection and the
// connection has taken the queued messages over (they drain right after the
// CONNACK), so the store and Broker.clients never hold the same key at the same
// time. That invariant is what makes a publication reach an offline session
// exactly once: LocalTargets reads the offline half, SendLocal re-checks it, and a
// session that reconnected in between receives the message on its live connection
// instead of a stale queue.
//
// The reference broker of this family also queues for clean sessions; gmqx does
// not, because a clean session is deleted with its connection and recreated empty
// by the next CONNECT — a queued message would be delivered to a session that
// declared it wanted none.

// takeStoredSession removes the stored session of key and hands it to a
// connection. The caller holds b.mu. The global offline-queue counter follows the
// messages that leave the store, because those messages are now owned by the
// connection.
//
// An expired session is never handed over, even when the sweeper has not reached
// it yet: the Session Expiry Interval is a deadline, not a hint.
//
// The removal is atomic when the store provides Take (a shared store must: see
// sessionTaker), so two nodes that reconnect the same client id cannot both be
// handed the session. A node-local store is taken with Load followed by Delete,
// which its own broker lock already serialises.
func (b *Broker) takeStoredSession(key string) (*sessionstate.State, bool) {
	state, ok := b.takeSessionState(key)
	if !ok {
		return nil, false
	}
	b.offlineQueued = max(0, b.offlineQueued-len(state.Queue))
	if !state.ExpiresAt.IsZero() && !time.Now().Before(state.ExpiresAt) {
		b.metrics().RecordDroppedMessage("session_expired")
		return nil, false
	}
	s, dropped := sessionstate.StateFromStored(state)
	if dropped > 0 {
		b.log.Warn("gmqx dropped queued messages with an undecodable property block",
			"client", s.ClientID, "count", dropped)
		b.metrics().RecordDroppedMessage("offline_queue_corrupt")
	}
	return s, true
}

// takeSessionState removes the state stored under key and returns it: one atomic
// operation for a store that implements Take, Load and Delete otherwise.
func (b *Broker) takeSessionState(key string) (SessionState, bool) {
	if taker, ok := b.store.(sessionTaker); ok {
		return taker.Take(key)
	}
	state, ok := b.store.Load(key)
	if !ok {
		return SessionState{}, false
	}
	b.store.Delete(key)
	return state, true
}

// sharedStore reports whether Config.SessionStore is read and written by several
// broker nodes, which is what decides whether a connection displaced by a peer's
// kick may still write to it (see unregister).
func (b *Broker) sharedStore() bool {
	shared, ok := b.store.(SharedSessionStore)
	return ok && shared.SharedAcrossNodes()
}

// deleteStoredSession removes key and forgets its queued messages. The caller
// holds b.mu. The session leaves the routing index with it: an expired or
// discarded session must not be a target any more.
func (b *Broker) deleteStoredSession(key string) {
	if state, ok := b.store.Load(key); ok {
		b.offlineQueued = max(0, b.offlineQueued-len(state.Queue))
	}
	b.store.Delete(key)
	b.index.Remove(key)
}

// saveStoredSession writes the state the connection c handed over: its
// subscriptions, the queue it did not manage to drain plus the deliveries it
// still owed, and its expiry deadline. The caller holds b.mu, and the connection
// runs it from inside conn.Client.HandOff, so no delivery can observe the handoff
// before the state is stored.
//
// The session's filters move from live to stored in the routing index here: from
// this point a matching publication is queued for it instead of written to a
// connection, and no store call is needed to find out that it is a target.
func (b *Broker) saveStoredSession(key string, h *conn.Handoff) {
	if stale, ok := b.store.Load(key); ok {
		// Defensive: a stored session of a live connection should not exist, but
		// the counter must follow what the store actually holds.
		b.offlineQueued = max(0, b.offlineQueued-len(stale.Queue))
	}
	// One deadline for both halves: the store the sweeper enforces and the index
	// that decides whether a publication still finds a target.
	queue := b.adoptQueue(h)
	state := (&sessionstate.State{
		ClientID:    h.ClientID,
		Subs:        h.Subs,
		Queue:       queue,
		ExpiresAt:   h.ExpiresAt,
		InboundQoS2: h.InboundQoS2,
		Inflight:    h.Inflight,
	}).Stored()
	b.store.Save(key, state)
	b.offlineQueued += len(queue)
	// The index is fed the same subscription set the store just got, so a publish
	// that matches the session is routed exactly like a restored one.
	b.index.SetStored(key, state.ClientID, state.Subscriptions, h.ExpiresAt)
}

// adoptQueue merges the deliveries a connection still owed its inflight window
// into the queue it hands over, in the order they were queued. They were
// acknowledged to their publisher and never written, so a persistent session must
// not lose them with the connection: they take their place behind whatever the
// offline queue still holds, which is where the next CONNECT drains them from.
//
// This queue is what a persistent session gets instead of a drop when its inflight
// queue overflows (see conn's sendPublish), and it is also what a connection that
// simply went away leaves behind. The queue's own bounds apply exactly as they do
// to a message published while the session is offline — Config.MaxOfflineQueueSize
// drops the oldest messages of the combined queue, Config.MaxTotalOfflineMessages
// rejects the newest — and they are counted with the same reasons
// ("offline_queue_full", "offline_queue_total"), so the offline queue's capacity
// stays the single, documented loss point of a persistent session.
//
// The caller holds b.mu.
func (b *Broker) adoptQueue(h *conn.Handoff) []sessionstate.Pending {
	queue := h.Queue
	rejected := 0
	for _, m := range h.Pending {
		if limit := b.cfg.MaxTotalOfflineMessages; limit > 0 && b.offlineQueued+len(queue) >= limit {
			rejected++
			continue
		}
		queue = append(queue, m)
	}
	if maxQueue := b.cfg.MaxOfflineQueueSize; maxQueue > 0 && len(queue) > maxQueue {
		queue = queue[len(queue)-maxQueue:]
		b.metrics().RecordDroppedMessage("offline_queue_full")
	}
	for i := 0; i < rejected; i++ {
		b.metrics().RecordDroppedMessage("offline_queue_total")
	}
	return queue
}

// loadStoredSessions indexes the sessions a store already held when the broker
// was created: a deployment restores session state before New, and those sessions
// have to be local targets from the first publication on. A session whose deadline
// already passed is skipped — nothing is queued for it, and the sweeper deletes
// it. A store that is written behind the broker's back while it runs is not
// re-read: the broker indexes what it saves, hands over and expires.
func (b *Broker) loadStoredSessions() {
	now := time.Now()
	for _, key := range b.store.Keys() {
		if !keyInNamespace(b.namespace, key) {
			// A store may be shared with a broker of another namespace.
			continue
		}
		state, ok := b.store.Load(key)
		if !ok {
			continue
		}
		if !state.ExpiresAt.IsZero() && !now.Before(state.ExpiresAt) {
			continue
		}
		b.index.SetStored(key, state.ClientID, state.Subscriptions, state.ExpiresAt)
	}
}

// maxSessionSweepPerTick bounds how many stored sessions one sweep tick examines,
// so the sweeper's cost per tick stays bounded no matter how many sessions a
// broker holds. Map iteration order is randomized, so a session that is not
// reached in one tick is reached by a later one.
const maxSessionSweepPerTick = 512

// sessionSweeper deletes the sessions whose Session Expiry Interval elapsed. One
// goroutine per broker, started by Start and ended by Stop; no timer per session.
func (b *Broker) sessionSweeper() {
	interval := b.cfg.SessionSweepInterval
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.done:
			return
		case <-ticker.C:
			b.sweepExpiredSessions()
		}
	}
}

// sweepExpiredSessions removes the stored sessions whose deadline has passed.
// Subscriptions and queued messages go with them: the session is gone.
func (b *Broker) sweepExpiredSessions() {
	now := time.Now()
	checked := 0
	for _, key := range b.store.Keys() {
		if checked >= maxSessionSweepPerTick {
			return
		}
		if !keyInNamespace(b.namespace, key) {
			// A store may be shared with a broker of another namespace; its
			// sessions are that broker's to expire.
			continue
		}
		checked++
		stored, ok := b.store.Load(key)
		if !ok || stored.ExpiresAt.IsZero() || now.Before(stored.ExpiresAt) {
			continue
		}
		b.mu.Lock()
		if _, connected := b.clients[key]; connected {
			// The session has an owner; its next disconnect stamps a new deadline.
			b.mu.Unlock()
			continue
		}
		// Re-read under the lock: a reconnect or a new publish may have changed
		// the entry since the check above.
		current, ok := b.store.Load(key)
		if ok && !current.ExpiresAt.IsZero() && !now.Before(current.ExpiresAt) {
			b.deleteStoredSession(key)
			// The session is gone, so no directed publish can be queued for it
			// here any more: a presence-tracking cluster backend stops routing it to
			// this node.
			b.forgetClientPresence(current.ClientID)
			b.metrics().RecordDroppedMessage("session_expired")
			b.metrics().RecordQueueDepth("offline", b.offlineQueued)
		}
		b.mu.Unlock()
	}
}

// enqueueOffline appends one publication to the queue of the persistent session
// target names. It is the write half of an offline LocalTarget.
//
// Routing is a snapshot, so the session may have reconnected since LocalTargets
// ran: then the message goes to the live connection instead of a queue nobody
// would drain. A session that is gone (cleaned or expired) has nothing to queue
// for. The global cap rejects the newest message, the per-session cap drops the
// oldest one, and both are counted through the metrics drop hook.
func (b *Broker) enqueueOffline(target LocalTarget, pub Publication, props mqtt.Properties) {
	key := target.SessionKey()
	b.mu.Lock()
	if live, ok := b.clients[key]; ok {
		b.mu.Unlock()
		// A live connection owns the session now, so delivery is live again.
		_ = live.DeliverPublish(pub.Topic, pub.Payload, target.QoS, target.Retain, props)
		return
	}
	state, ok := b.store.Load(key)
	if !ok {
		b.mu.Unlock()
		b.metrics().RecordDroppedMessage("offline_session_missing")
		return
	}
	if !state.ExpiresAt.IsZero() && !time.Now().Before(state.ExpiresAt) {
		// The session is past its deadline and only waits for the sweeper.
		b.mu.Unlock()
		b.metrics().RecordDroppedMessage("offline_session_missing")
		return
	}
	if limit := b.cfg.MaxTotalOfflineMessages; limit > 0 && b.offlineQueued >= limit {
		b.mu.Unlock()
		b.metrics().RecordDroppedMessage("offline_queue_total")
		return
	}
	queue := make([]QueuedMessage, 0, len(state.Queue)+1)
	queue = append(queue, state.Queue...)
	// The queued message keeps the MQTT 5 property block of the delivery, which
	// carries the target's subscription identifiers when it has any: they are part
	// of the subscription state and have to be there when the session drains its
	// queue after the next CONNACK. Without them the block the publisher sent is
	// stored unchanged, so an ordinary message is not re-encoded.
	wireProps := pub.Props
	if len(props.SubscriptionIdentifiers) > 0 {
		if encoded, err := mqtt5.EncodePropertyBlock(props); err == nil {
			wireProps = encoded
		}
	}
	queue = append(queue, QueuedMessage{
		Topic:   pub.Topic,
		Payload: append([]byte(nil), pub.Payload...),
		QoS:     target.QoS,
		Retain:  target.Retain,
		Props:   append([]byte(nil), wireProps...),
	})
	queued := 1
	evicted := false
	if maxQueue := b.cfg.MaxOfflineQueueSize; maxQueue > 0 && len(queue) > maxQueue {
		dropped := len(queue) - maxQueue
		queue = queue[dropped:]
		queued -= dropped
		evicted = true
	}
	b.offlineQueued += queued
	state.Queue = queue
	b.store.Save(key, state)
	depth := b.offlineQueued
	b.mu.Unlock()

	if evicted {
		b.metrics().RecordDroppedMessage("offline_queue_full")
	}
	b.metrics().RecordQueueDepth("offline", depth)
}

// Delayed wills (MQTT 5 Will Delay Interval)
//
// A will is the last message a client asks the server to publish for it. MQTT
// 3.1.1 publishes it as soon as the connection is gone; MQTT 5 lets the client
// ask for a delay, so a device that reconnects after a network blip does not
// announce its own death to the world.
//
// The rule is the one MQTT 5 states in §3.1.3.2.2: the will is published when the
// earlier of two things happens — the Will Delay Interval elapses, or the session
// ends. Everything else follows from that:
//
//   - A session that ends with its connection (Clean Start set, or a zero Session
//     Expiry Interval) publishes its will immediately, because the delay cannot
//     outlive the session it belongs to.
//   - A session with a Session Expiry Interval publishes its will at the expiry
//     when that comes first, so a delayed will is never left waiting for a session
//     that no longer exists.
//   - A CONNECT that resumes the session (same client id) cancels the pending
//     will [MQTT-3.1.2-28]: a session that comes back must not announce that it
//     died. That CONNECT is either local (register cancels directly) or on another
//     node, where the cluster kick it spreads cancels it here (OnRemoteKick).
//   - A CONNECT with Clean Start set discards the session instead of resuming it,
//     so the session ends there and the will is published at that moment.
//   - A broker that stops drops what is pending: it is shutting down, so there is
//     nowhere to publish the will and no timer may outlive Stop.
//
// Scheduling is one timer per pending will, not a broker-wide ticker: a delayed
// will is rare, and a timer that exists only while the will waits costs nothing
// when nobody asks for one. The timer fires on its own goroutine, publishes under
// the broker's lifetime context (the connection's own context is cancelled by
// then) and removes itself from the map before publishing, so a will is published
// at most once even when a cancellation and the timer race.
//
// What the connection decides (the delay, the identity the will is published
// under) lives in gmqx/conn; what outlives the connection — the map and its
// timers — stays here.

// pendingWill is a will waiting for its Will Delay Interval to elapse. It is
// stored under the namespace-scoped client id it belongs to, which is what a
// CONNECT of the same session cancels.
type pendingWill struct {
	// origin is the identity the will is published under once it fires: the
	// connection is closed by then, so it carries the broker's lifetime context
	// with the connection's hook.ConnInfo instead of the cancelled connection context.
	origin conn.Origin
	ev     hook.PublishEvent
	props  mqtt.Properties
	timer  *time.Timer
	// delay is the wait that was scheduled, for the log line.
	delay time.Duration
}

// scheduleWill records a delayed will and arms its timer. A will already pending
// for the same client id is replaced: a session has one will, and the newest
// connection ended with it.
func (b *Broker) scheduleWill(c *conn.Client, key string, delay time.Duration, ev hook.PublishEvent, props mqtt.Properties) {
	pw := &pendingWill{origin: c.WillOrigin(), ev: ev, props: props, delay: delay}
	// A broker that is stopping has nowhere left to publish the will and must not
	// arm a timer behind Stop's back; the wills Stop was already holding are
	// dropped the same way.
	b.mu.RLock()
	stopped := b.closed
	b.mu.RUnlock()
	if stopped {
		if b.log != nil {
			b.log.Debug("gmqx delayed will dropped: broker is stopping", "client", pw.origin.ClientID,
				"topic", ev.Topic)
		}
		return
	}
	b.willMu.Lock()
	if old, ok := b.wills[key]; ok {
		old.timer.Stop()
	}
	// The timer is created under the lock and its callback takes the same lock
	// first, so the callback cannot observe the map before pw is in it.
	pw.timer = time.AfterFunc(delay, func() { b.firePendingWill(key, pw) })
	b.wills[key] = pw
	b.willMu.Unlock()
	if b.log != nil {
		b.log.Debug("gmqx will delayed", "client", pw.origin.ClientID,
			"connection", string(pw.origin.ConnID), "topic", ev.Topic, "delay", delay)
	}
}

// firePendingWill publishes a delayed will whose interval elapsed. It removes the
// entry first, so a will is published at most once even when a cancellation and
// the timer race.
func (b *Broker) firePendingWill(key string, want *pendingWill) {
	b.willMu.Lock()
	pw, ok := b.wills[key]
	if !ok || pw != want {
		b.willMu.Unlock()
		return
	}
	delete(b.wills, key)
	b.willMu.Unlock()
	_ = b.publish(pw.origin, pw.ev, pw.props)
}

// flushPendingWill publishes a delayed will right away, because the session it
// belongs to is ending before the delay elapsed: a CONNECT with Clean Start set
// discards the session [MQTT-3.1.2-29], and the will is published when the
// session ends.
func (b *Broker) flushPendingWill(key string) {
	b.willMu.Lock()
	pw, ok := b.wills[key]
	if ok {
		delete(b.wills, key)
	}
	b.willMu.Unlock()
	if !ok {
		return
	}
	pw.timer.Stop()
	_ = b.publish(pw.origin, pw.ev, pw.props)
}

// cancelPendingWill drops the will a closed connection left waiting for its
// delay. A CONNECT that resumes the session calls it, because a session that
// comes back must not announce that it died.
func (b *Broker) cancelPendingWill(key string) {
	b.willMu.Lock()
	pw, ok := b.wills[key]
	if ok {
		delete(b.wills, key)
	}
	b.willMu.Unlock()
	if ok {
		pw.timer.Stop()
		if b.log != nil {
			b.log.Debug("gmqx delayed will cancelled by a reconnect", "client", pw.origin.ClientID,
				"connection", string(pw.origin.ConnID), "delay", pw.delay)
		}
	}
}

// dropPendingWills forgets every delayed will. Stop calls it: the broker is
// shutting down, so there is no subscriber left to publish to, and no timer may
// outlive the broker.
func (b *Broker) dropPendingWills() {
	b.willMu.Lock()
	pending := b.wills
	b.wills = make(map[string]*pendingWill)
	b.willMu.Unlock()
	for _, pw := range pending {
		pw.timer.Stop()
	}
}

// extensions is the mutable half of a broker's configuration: the hooks an
// application can swap while the broker runs. It is immutable once stored, so a
// publish reads one consistent snapshot instead of a field that another goroutine
// is rewriting; a setter clones the snapshot, changes it and swaps it.
//
// Everything that is not here stays configuration: a listener address, a limit or a
// cluster backend cannot be swapped after Start, and pretending otherwise would
// make the broker's behaviour depend on call order.
type extensions struct {
	auth         hook.AuthManager
	acl          hook.AclManager
	sink         hook.EventSink
	metrics      hook.MetricsManager
	interceptors []*interceptorSlot
	transcoders  []*transcoderSlot
}

// interceptorSlot and transcoderSlot wrap a hook so Add* can hand back a remove
// function: slots are compared by pointer, because a hook's dynamic type may be a
// function type and interfaces holding functions are not comparable.
type interceptorSlot struct{ fn hook.Interceptor }

type transcoderSlot struct{ fn hook.Transcoder }

// interceptorSlots wraps the configured interceptors into slots, so a
// configuration-time interceptor and one a plugin adds later behave the same.
func interceptorSlots(interceptors []Interceptor) []*interceptorSlot {
	if len(interceptors) == 0 {
		return nil
	}
	slots := make([]*interceptorSlot, 0, len(interceptors))
	for _, interceptor := range interceptors {
		slots = append(slots, &interceptorSlot{fn: interceptor})
	}
	return slots
}

// exts returns the current extension snapshot.
func (b *Broker) exts() *extensions { return b.ext.Load() }

// metrics returns the metrics hook of the current snapshot.
func (b *Broker) metrics() hook.MetricsManager { return b.exts().metrics }

// sink returns the event sink of the current snapshot.
func (b *Broker) sink() hook.EventSink { return b.exts().sink }

// acl returns the authorization hook of the current snapshot.
func (b *Broker) acl() hook.AclManager { return b.exts().acl }

// auth returns the authentication hook of the current snapshot.
func (b *Broker) auth() hook.AuthManager { return b.exts().auth }

// mutateExtensions applies change to a clone of the current snapshot and publishes
// it. Setters go through here so a half-written snapshot is never visible.
func (b *Broker) mutateExtensions(change func(*extensions)) {
	b.extMu.Lock()
	defer b.extMu.Unlock()
	current := b.ext.Load()
	next := *current
	next.interceptors = append([]*interceptorSlot(nil), current.interceptors...)
	next.transcoders = append([]*transcoderSlot(nil), current.transcoders...)
	change(&next)
	b.ext.Store(&next)
}

// SetMetricsManager replaces the metrics hook of a running broker. It implements
// hook.Host, so a plugin can install one; an application that swaps metrics while
// connections are live is expected to keep both generations working.
func (b *Broker) SetMetricsManager(metrics hook.MetricsManager) {
	b.mutateExtensions(func(e *extensions) { e.metrics = metrics })
}

// SetEventSink replaces the lifecycle/publication sink of a running broker.
func (b *Broker) SetEventSink(sink hook.EventSink) {
	b.mutateExtensions(func(e *extensions) { e.sink = sink })
}

// SetAclManager replaces the authorization hook of a running broker.
func (b *Broker) SetAclManager(acl hook.AclManager) {
	b.mutateExtensions(func(e *extensions) { e.acl = acl })
}

// SetAuthManager replaces the CONNECT authentication hook of a running broker.
func (b *Broker) SetAuthManager(auth hook.AuthManager) {
	b.mutateExtensions(func(e *extensions) { e.auth = auth })
}

// AddInterceptor appends an interceptor and returns the function that removes it.
// Interceptors run in registration order; the returned function is safe to call
// twice.
func (b *Broker) AddInterceptor(interceptor hook.Interceptor) (remove func()) {
	slot := &interceptorSlot{fn: interceptor}
	b.mutateExtensions(func(e *extensions) { e.interceptors = append(e.interceptors, slot) })
	removed := false
	return func() {
		if removed {
			return
		}
		removed = true
		b.mutateExtensions(func(e *extensions) {
			kept := e.interceptors[:0]
			for _, existing := range e.interceptors {
				if existing != slot {
					kept = append(kept, existing)
				}
			}
			e.interceptors = kept
		})
	}
}

// AddTranscoder appends a payload transcoder and returns the function that removes
// it. Transcoders run in registration order, after the sink accepted a publication
// and before it is retained or routed.
func (b *Broker) AddTranscoder(transcoder hook.Transcoder) (remove func()) {
	slot := &transcoderSlot{fn: transcoder}
	b.mutateExtensions(func(e *extensions) { e.transcoders = append(e.transcoders, slot) })
	removed := false
	return func() {
		if removed {
			return
		}
		removed = true
		b.mutateExtensions(func(e *extensions) {
			kept := e.transcoders[:0]
			for _, existing := range e.transcoders {
				if existing != slot {
					kept = append(kept, existing)
				}
			}
			e.transcoders = kept
		})
	}
}

// Codecs returns the payload encodings this broker offers to its plugins. The
// broker never picks a codec itself: an application-side plugin registers the
// encodings it understands and wires them into a transcoder or a dispatcher.
func (b *Broker) Codecs() *hook.CodecSet { return b.codecs }

// Plugins returns the registry of application plugins attached to this broker.
//
// It is created with the broker, so a plugin can be added before or after Start:
//
//	broker.Plugins().Add(jsonCodecPlugin)
//	if err := broker.Plugins().Start("json-codec"); err != nil { ... }
//	for _, status := range broker.Plugins().Snapshot() { ... }   // what runs, and why not
//
// The host a plugin receives is the broker itself (hook.Host), which is what makes
// the slots above reachable without importing the broker's internals.
func (b *Broker) Plugins() *gplugin.Registry[hook.Host] { return b.plugins }

// Publication is one accepted publication the broker routes. It is the payload
// of the routing SPI: QoS and Retain are the values the client published, Dup
// reports a retransmission, and FromClientID/FromConnectionID name the session
// that produced it (both are empty for an event that came from a peer node).
// Props is the MQTT 5 PUBLISH property block without its length prefix, nil for
// MQTT 3.1.1 traffic.
type Publication struct {
	Namespace        string
	Topic            string
	Payload          []byte
	QoS              byte
	Retain           bool
	Dup              bool
	FromClientID     string
	FromConnectionID string
	Props            []byte

	// props is the typed mirror of Props for publications the broker built
	// itself. SendLocal uses it directly instead of decoding Props once per
	// local target; a Publication built by a dispatcher only carries Props.
	props    mqtt.Properties
	propsSet bool
}

// Dispatcher is the single decision point for where an accepted publication
// goes. The broker authorizes, runs every Interceptor and calls the EventSink
// first; only a publication the sink accepted reaches Dispatch. Suppressing it
// here therefore suppresses delivery, not the acknowledgement the sink already
// granted.
//
// A Dispatcher must not re-implement topic matching. It routes through the
// broker's read-only seam instead:
//
//	targets := broker.LocalTargets(pub)        // sessions whose subscriptions match
//	for _, t := range targets {
//	    _ = broker.SendLocal(ctx, t, pub)      // one PUBLISH to one of them
//	}
//	return broker.SpreadPublication(ctx, pub)  // forward to the cluster backend
//
// A dispatcher that needs the seam implements DispatcherBinder; the broker binds
// itself before the first publication is routed. Retained state is not part of
// routing: the broker stores an accepted retained publication before Dispatch, so
// a dispatcher decides where live publications go, not what is retained.
//
// Config.Dispatcher == nil selects DefaultDispatcher, which is the behavior of a
// broker without the SPI: local delivery plus a cluster spread.
type Dispatcher interface {
	Dispatch(ctx context.Context, pub Publication) error
}

// DispatcherBinder is implemented by a Dispatcher that needs the broker's
// routing seam (LocalTargets, SendLocal, SpreadPublication). The broker calls
// Bind once, from New, before any connection can publish.
type DispatcherBinder interface {
	Bind(b *Broker)
}

// AllowAll permits every connection/subscription/publish.
type AllowAll struct{}

func (AllowAll) Authenticate(context.Context, ConnectInfo) error            { return nil }
func (AllowAll) RequiresOffload() bool                                      { return false }
func (AllowAll) Check(context.Context, SessionInfo, string, AclAction) bool { return true }

// DenyAll rejects every connection.
type DenyAll struct{}

func (DenyAll) Authenticate(context.Context, ConnectInfo) error { return ErrUnauthorized }
func (DenyAll) RequiresOffload() bool                           { return false }

// The enhanced-authentication seam: the broker owns the factory and the method it
// advertises, the connection (gmqx/conn) owns the packet flow of one exchange.

// authExchange returns the exchange of method, or nil when this broker does not
// support the method. Nil is the answer for every method when
// Config.Authenticator is unset, for a method outside a non-empty
// Config.AuthMethods, and for a factory that declines the method or returns an
// exchange that reports a different one.
func (b *Broker) authExchange(method string) hook.AuthExchange {
	if method == "" || b.cfg.Authenticator == nil {
		return nil
	}
	if len(b.cfg.AuthMethods) > 0 {
		supported := false
		for _, advertised := range b.cfg.AuthMethods {
			if advertised == method {
				supported = true
				break
			}
		}
		if !supported {
			return nil
		}
	}
	exchange := b.cfg.Authenticator(method)
	if exchange == nil {
		return nil
	}
	if got := exchange.Method(); got != method {
		// A factory that answers one method with an exchange for another is a
		// configuration error: the broker would name the wrong method in the AUTH
		// packets, and the client's CONNACK would not match its CONNECT.
		if b.log != nil {
			b.log.Warn("gmqx authenticator returned an exchange for another method",
				"method", method, "exchange_method", got)
		}
		return nil
	}
	return exchange
}

// The extension contracts live in ./hook. Every name below is an alias, so a consumer
// keeps importing this package and still gets the hook type: one definition, two import
// paths. New code may import github.com/maxid/gmqx/hook directly — both spellings are the
// same type, so they mix freely.
type (
	// LocalTarget is one local session a publication matches. Its fields are
	// read-only reporting; pass the value back to Broker.SendLocal to write the
	// PUBLISH. A LocalTarget is only valid for the broker that produced it.
	//
	// The type is defined in gmqx/routing (the routing table is what produces it)
	// and re-exported here as an alias, so a Dispatcher keeps naming it the same way.
	LocalTarget = routing.Target

	ConnectInfo    = hook.ConnectInfo
	AuthManager    = hook.AuthManager
	AuthStatus     = hook.AuthStatus
	AuthStep       = hook.AuthStep
	AuthExchange   = hook.AuthExchange
	AclAction      = hook.AclAction
	SessionInfo    = hook.SessionInfo
	AclManager     = hook.AclManager
	ConnectionID   = hook.ConnectionID
	ConnInfo       = hook.ConnInfo
	Disposition    = hook.Disposition
	PublishResult  = hook.PublishResult
	PublishEvent   = hook.PublishEvent
	EventSink      = hook.EventSink
	MetricsManager = hook.MetricsManager
	NoopMetrics    = hook.NoopMetrics
	Interceptor    = hook.Interceptor
)

const (
	AuthContinue  = hook.AuthContinue
	AuthSucceeded = hook.AuthSucceeded
	AuthFailed    = hook.AuthFailed

	AclSubscribe = hook.AclSubscribe
	AclPublish   = hook.AclPublish

	PublishAccepted  = hook.PublishAccepted
	PublishRetryable = hook.PublishRetryable
	PublishRejected  = hook.PublishRejected

	ReasonUnspecified            = hook.ReasonUnspecified
	ReasonProtocolError          = hook.ReasonProtocolError
	ReasonNotAuthorized          = hook.ReasonNotAuthorized
	ReasonBadAuthMethod          = hook.ReasonBadAuthMethod
	ReasonSessionTakenOver       = hook.ReasonSessionTakenOver
	ReasonQuotaExceeded          = hook.ReasonQuotaExceeded
	ReasonConnectionRateExceeded = hook.ReasonConnectionRateExceeded
)

// ContextWithConn forwards to hook.ContextWithConn, so a consumer needs one import.
func ContextWithConn(ctx context.Context, info ConnInfo) context.Context {
	return hook.ContextWithConn(ctx, info)
}

// ConnFromContext forwards to hook.ConnFromContext.
func ConnFromContext(ctx context.Context) (ConnInfo, bool) { return hook.ConnFromContext(ctx) }

// The session store lives in gmqx/session (docs/design/source-layout.md §2, rule L3):
// the root package re-exports its types as aliases and forwards the constructors, so
// Config.SessionStore, an implementation outside this module (contrib/sessionstore-
// gcache) and the stores this package ships all name the same types as before.

type (
	// SessionStore is the persistence seam of persistent-session state.
	SessionStore = sessionstore.SessionStore
	// SharedSessionStore is a SessionStore that several broker nodes share.
	SharedSessionStore = sessionstore.SharedSessionStore
	// SessionState is the state of one persistent session with no live connection.
	SessionState = sessionstore.SessionState
	// InflightStage is where a stored outbound exchange stood when its connection
	// ended.
	InflightStage = sessionstore.InflightStage
	// SessionInflight is one outbound QoS 1/2 exchange a session owes its client.
	SessionInflight = sessionstore.SessionInflight
	// SessionSubscription is one subscription of a stored sessionstore.
	SessionSubscription = sessionstore.SessionSubscription
	// QueuedMessage is one message queued for an offline persistent sessionstore.
	QueuedMessage = sessionstore.QueuedMessage
	// MemorySessionStore is the default node-local store.
	MemorySessionStore = sessionstore.MemorySessionStore
	// RedisSessionStore is the store shared by the nodes of a Redis cluster.
	RedisSessionStore = sessionstore.RedisSessionStore
	// RedisSessionStoreConfig configures the Redis-backed store.
	RedisSessionStoreConfig = sessionstore.RedisSessionStoreConfig
)

const (
	InflightAwaitPuback  = sessionstore.InflightAwaitPuback
	InflightAwaitPubrec  = sessionstore.InflightAwaitPubrec
	InflightAwaitPubcomp = sessionstore.InflightAwaitPubcomp
)

// NewMemorySessionStore builds the default node-local store.
func NewMemorySessionStore() *MemorySessionStore { return sessionstore.NewMemorySessionStore() }

// NewRedisSessionStore builds the Redis-backed store and validates its
// configuration.
func NewRedisSessionStore(cfg RedisSessionStoreConfig) (*RedisSessionStore, error) {
	return sessionstore.NewRedisSessionStore(cfg)
}

// NewRedisClusterWithSessionStore builds the Redis cluster backend and the Redis
// session store of one cluster from one RedisClusterConfig: both use the same
// address, database and key prefix, so the sessions a node stores are the ones its
// peers take over.
func NewRedisClusterWithSessionStore(cfg RedisClusterConfig) (Cluster, *RedisSessionStore, error) {
	return sessionstore.NewRedisClusterWithSessionStore(cfg)
}

// sessionTaker is implemented by a SessionStore that can remove a stored session
// atomically and hand it to one caller. A shared store needs it: Load followed by
// Delete is two round trips, so two nodes reconnecting the same client id at the
// same time could both see the session and both serve it. The method is exported
// so an implementation outside this module can provide it; the interface is
// unexported because only the broker decides when to take a sessionstore.
type sessionTaker interface {
	Take(key string) (SessionState, bool)
}
