package gmqx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisClusterWireVersion is the version of the event frames published on the
// per-node channels. Unlike the TCP backend's handshake there is no negotiation:
// membership is a Redis key, so the version only lets a rolling upgrade drop a
// frame it cannot interpret instead of misapplying it.
const redisClusterWireVersion = 1

const (
	// defaultRedisChannelPrefix namespaces every channel and key this backend
	// owns.
	defaultRedisChannelPrefix = "gmqx:cluster"
	// redisTTLFactor turns SummaryInterval into the default key TTLs: a key has
	// to survive a few refresh periods so scheduling jitter cannot evict a node
	// that is alive.
	redisTTLFactor = 3
	// redisMemberScanCount is the SCAN COUNT hint used to enumerate member keys.
	// It bounds one round trip, not the view size.
	redisMemberScanCount = 100
	// redisDialTimeout bounds one connection attempt, so a black-holed Redis
	// cannot pin a Spread or a Stop forever.
	redisDialTimeout = 2 * time.Second
	// redisIOTimeout bounds one Refresh/Spread/Members round trip.
	redisIOTimeout = 2 * time.Second
	// redisMinBackoff and redisMaxBackoff bound the receive loop's retry delay:
	// a Redis outage must not turn into a dial storm, and recovery must not wait
	// longer than redisMaxBackoff.
	redisMinBackoff = 50 * time.Millisecond
	redisMaxBackoff = time.Second
)

// redisOwnerRenewScript renews this node's ownership lease, or takes the key
// when nobody holds it. KEYS[1] is the ownership key, ARGV[1] the value this
// process writes and ARGV[2] the TTL in milliseconds. It returns 1 when this
// process owns the key afterwards and 0 when another live process holds it.
//
// The compare-and-set is what keeps a paused process from stealing an id back:
// only the current holder may extend the lease, and an absent key is taken with
// SET NX, which lets a restart succeed once the previous owner's TTL expired.
const redisOwnerRenewScript = `
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
if cur == false then
  if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
    return 1
  end
end
return 0
`

// redisOwnerReleaseScript deletes the ownership key only when this process still
// holds it, so a Stop that races a new owner cannot release an id it no longer
// owns.
const redisOwnerReleaseScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// redisPresenceReleaseScript deletes a client id's presence record only when it
// still names this process. A client that reconnected to another node has
// overwritten the record there, and the disconnect of its previous connection must
// not erase the presence of the new one.
const redisPresenceReleaseScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// RedisClusterConfig configures the Redis cluster backend, the centralized
// alternative to the TCP mesh: a node publishes an event once per target node to
// that node's own channel, so N nodes need one Redis connection each instead of
// N² links, and a publish is not delivered to nodes that cannot use it.
//
// Zero values select the documented default, exactly as in TCPClusterConfig; a
// negative duration, size or DB is refused by Start.
type RedisClusterConfig struct {
	// Addr is the Redis server address ("host:port"). Required. Start fails when
	// Redis cannot be reached, and a node whose Redis connection dies at runtime
	// keeps retrying until Stop instead of pretending to be clustered.
	Addr string
	// Password is the Redis password; empty means no AUTH.
	Password string
	// DB is the Redis database index (default 0). Every node of a cluster must
	// use the same Addr and DB.
	DB int
	// ChannelPrefix is the namespace of every channel and key this backend owns
	// (default "gmqx:cluster"). It must be identical on every node of the
	// cluster and must not contain SCAN glob characters (`*`, `?`, `[`, `]`,
	// `\`), because it is used verbatim in the member key pattern.
	ChannelPrefix string

	// SummaryInterval is how often a node rewrites its own membership and
	// subscription keys and re-reads the routing view from Redis (default 1s).
	// A local subscription change rewrites the keys immediately instead of
	// waiting for the next interval.
	SummaryInterval time.Duration
	// SummaryTTL is how long a node's subscription summary survives without a
	// refresh (default 3×SummaryInterval; it must exceed SummaryInterval). An
	// expired summary is unknown, and a node whose summary is unknown receives
	// every publish.
	SummaryTTL time.Duration
	// NodeTTL is how long a node's member key and its node id ownership key
	// survive without a refresh (default 3×SummaryInterval; it must exceed
	// SummaryInterval). The TTL is the membership authority: a node that stops
	// refreshing leaves the view on its own, with no eviction protocol and no
	// assumption that it shut down cleanly. It is also the escape hatch for a
	// node id whose owner died: the ownership key expires NodeTTL after the last
	// renewal, so a restart with the same id succeeds once it is gone.
	NodeTTL time.Duration
	// HeartbeatInterval is how often a node PINGs Redis to notice a lost
	// connection and log the transition (default 1s). It extends no key: the two
	// TTLs above are the authority. Spreading an event also exercises Redis, so a
	// publish-heavy node needs no heartbeat at all.
	HeartbeatInterval time.Duration
	// PresenceTTL is how long the presence record of one client id survives
	// without a refresh (default 3×SummaryInterval; it must exceed
	// SummaryInterval). The broker reports every CONNECT and every disconnect, so
	// a record is written when a client connects here and deleted when it leaves;
	// the TTL is the safety net for a node that dies without saying goodbye, and
	// the records of connected clients are refreshed every SummaryInterval so a
	// live client never expires from the view. A directed publish reads the record
	// of its client id and is sent to that node's channel only (see
	// Broker.PublishToClient); a missing record means "no node claims this client
	// id" and the publish is counted and dropped instead of broadcast.
	PresenceTTL time.Duration

	// DedupSize is the number of event identities remembered for duplicate
	// suppression (default 4096). The oldest identity is evicted first, so the
	// table is O(DedupSize) no matter how much traffic arrives.
	DedupSize int
	// DedupTTL is how long an event identity suppresses duplicates (default
	// 30s).
	DedupTTL time.Duration

	// MaxSummaryFilters caps how many filters a node advertises, and how many it
	// keeps from a peer's summary (default 256). When a node has more, the
	// filters that have been present longest are left out so new subscriptions
	// keep routing; the cost is that a publish matching only an evicted filter is
	// not routed to that node.
	MaxSummaryFilters int

	// Metrics, when set, receives RecordDroppedMessage for events the cluster
	// drops ("cluster_duplicate", "cluster_loop", "cluster_forward",
	// "cluster_namespace", and "target_unknown" for a directed publish whose client
	// id no presence record claims), the same reasons the TCP backend reports.
	Metrics MetricsManager
	// Logger receives membership and connectivity decisions (default
	// slog.Default()).
	Logger *slog.Logger
	// Namespace scopes the traffic of this backend: an event or subscription
	// summary of another namespace is dropped. When the backend is passed to a
	// Broker, the broker's Config.Namespace is authoritative and overrides this
	// value at Start. The empty string is the default namespace.
	Namespace string
}

// redisMemberRecord is the value of a node's member key. The key's TTL — not the
// value — decides whether the node is a member: Redis removes the key NodeTTL
// after the node's last refresh, so a crashed node disappears without any
// handshake.
type redisMemberRecord struct {
	NodeID      string `json:"node"`
	Incarnation string `json:"inc"`
	Since       int64  `json:"since"` // when this incarnation joined, unix nanoseconds
}

// redisOwnerRecord is the value of a node id's ownership key. The key is a
// lease, not a membership record: it exists to keep two live processes from
// using the same node id, is taken with SET NX at Start and renewed with the
// member key. The value names the process that holds it, so a refused Start and
// an operator can see which incarnation owns the id.
type redisOwnerRecord struct {
	NodeID      string `json:"node"`
	Incarnation string `json:"inc"`
	Since       int64  `json:"since"` // when this incarnation took the id, unix nanoseconds
}

// redisSummaryRecord is the value of a node's subscription summary key. An
// absent key means "unknown", not "no subscriptions": a node whose handler
// cannot describe its sessions advertises nothing, and peers then send it every
// publish.
type redisSummaryRecord struct {
	NodeID  string   `json:"node"`
	Filters []string `json:"filters"`
	// Namespace scopes the summary: a peer only uses summaries of its own
	// namespace. The field is additive on the wire; an old value without it is
	// the default namespace.
	Namespace string `json:"ns,omitempty"`
	Updated   int64  `json:"updated"` // unix nanoseconds
}

// Redis presence is a plain string value per client id, not a JSON record: it is
// read on the directed-publish path and compared byte for byte by the
// compare-and-delete script, and the node id plus the process incarnation are the
// whole content. The namespace travels in the key, which is what keeps two
// namespaces sharing one Redis from claiming each other's client ids.
//
// redisFrame is the envelope of one event on a node channel. The node channels
// carry events only — membership and summaries are keys, not messages — so the
// envelope is deliberately smaller than the TCP backend's frame.
type redisFrame struct {
	Version int             `json:"v"`
	NodeID  string          `json:"node"`
	Event   *ClusterMessage `json:"event"`
}

// redisPeer is one node in the routing view: the public Member plus the
// subscription summary Redis currently holds for it.
type redisPeer struct {
	Member
	filters      []string
	filtersKnown bool
}

// NewRedisCluster creates the Redis cluster backend: directed per-node delivery
// over Redis pub/sub, with membership and subscription summaries stored in
// Redis under TTL'd keys.
func NewRedisCluster(cfg RedisClusterConfig) Cluster {
	return &redisCluster{
		cfg:          cfg,
		namespace:    cfg.Namespace,
		local:        newSummarySet(),
		refreshCh:    make(chan struct{}, 1),
		presence:     make(map[string]time.Time),
		presenceGone: make(map[string]struct{}),
		presenceCh:   make(chan struct{}, 1),
	}
}

// redisCluster is one node's view of a Redis-backed cluster.
//
// Key and channel layout under the configured prefix:
//
//	<prefix>:node:<nodeID>    pub/sub channel of one node; the only channel a
//	                          node subscribes to and the only one its peers
//	                          publish to
//	<prefix>:owner:<nodeID>   JSON redisOwnerRecord with the NodeTTL: the node
//	                          id's ownership lease, so two live processes cannot
//	                          use the same id
//	<prefix>:member:<nodeID>  JSON redisMemberRecord with the NodeTTL; its
//	                          existence is membership, its expiry is eviction
//	<prefix>:subs:<nodeID>    JSON redisSummaryRecord with the SummaryTTL; its
//	                          absence means "summary unknown"
//	<prefix>:client:<key>     presence of one client id, with the PresenceTTL:
//	                          "<nodeID>\x00<incarnation>", where <key> is
//	                          namespaceKey(Config.Namespace, ClientID). It is what
//	                          routes a directed publish to the one node that holds
//	                          the client.
//
// Everything Redis holds is shared state; the process-local cache (`view`) is
// only a bounded-time copy of it, refreshed from Redis and never used to decide
// membership on its own.
type redisCluster struct {
	cfg         RedisClusterConfig
	log         *slog.Logger
	rdb         *redis.Client
	dedup       *dedupTable
	nodeID      string
	namespace   string
	incarnation string
	since       time.Time
	handler     ClusterHandler

	// ownerValue is the value this Start writes to the node id's ownership key;
	// every renewal and the release compare against it. It is set before any
	// goroutine starts and never changes afterwards.
	ownerValue []byte
	// owned reports whether this process currently holds the ownership key. It
	// only gates the logging of lease transitions: losing and regaining the lease
	// are logged once each instead of once per refresh. The member key refresh
	// itself is gated by the result of the renewal, not by this flag.
	owned atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	running bool
	sub     *redis.PubSub

	// viewMu guards the routing view cache and serialises its refreshes, so
	// concurrent publishes share one SCAN instead of stampeding Redis.
	viewMu sync.Mutex
	view   map[string]*redisPeer
	viewAt time.Time

	// summaryMu guards the local advertised summary.
	summaryMu sync.Mutex
	local     *summarySet

	// refreshCh wakes the housekeeping loop after a local subscription change.
	// It is buffered and never blocks the SUBSCRIBE/UNSUBSCRIBE path: a change
	// that arrives while a refresh is queued rides along with it.
	refreshCh chan struct{}

	// presenceMu guards the local presence view. presence holds the client ids
	// this node currently holds, with the time their Redis record was last
	// written (the zero time means "not written yet"); presenceGone holds the ids
	// whose record still has to be deleted. Both are reconciled by the
	// housekeeping loop, so a CONNECT never waits for Redis.
	presenceMu   sync.Mutex
	presence     map[string]time.Time
	presenceGone map[string]struct{}
	// presenceCh wakes the housekeeping loop after a connect or disconnect. It is
	// buffered and never blocks: a change that arrives while a flush is queued
	// rides along with it.
	presenceCh chan struct{}

	healthy atomic.Bool
	seq     atomic.Uint64
	drops   atomic.Uint64

	wg sync.WaitGroup
}

// setNamespace implements namespaceScoped: the broker's Config.Namespace is
// authoritative for a backend attached to a broker. It must run before Start;
// the broker calls it that way.
func (c *redisCluster) setNamespace(namespace string) { c.namespace = namespace }

// Start implements Cluster. It connects to Redis, takes ownership of the node
// id, subscribes to this node's channel and only then registers the node, so a
// peer that sees the member key can already reach a live subscription.
//
// Ownership is a lease: the node id's key is taken with SET NX and carries
// NodeTTL, so a second live process with the same id is refused with
// ErrNodeIDInUse instead of silently sharing the channel and applying every
// event twice. A process that died without releasing the key blocks the id only
// until the lease expires.
func (c *redisCluster) Start(ctx context.Context, nodeID string, handler ClusterHandler) error {
	if err := c.validate(nodeID); err != nil {
		return err
	}
	c.log = c.cfg.Logger
	if c.log == nil {
		c.log = slog.Default()
	}
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return errors.New("gmqx: redis cluster is already running")
	}
	c.nodeID, c.handler = nodeID, handler
	c.incarnation = randomIncarnation()
	c.since = time.Now()
	c.ownerValue = nil
	c.dedup = newDedupTable(c.cfg.DedupSize, c.cfg.DedupTTL)
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.rdb = redis.NewClient(c.options())
	c.running = true
	rdb := c.rdb
	c.mu.Unlock()

	c.summaryMu.Lock()
	c.local = newSummarySet()
	c.summaryMu.Unlock()
	c.viewMu.Lock()
	c.view, c.viewAt = nil, time.Time{}
	c.viewMu.Unlock()

	// A node that cannot reach Redis is not a cluster member: report it instead
	// of starting a node that silently delivers nothing.
	if err := rdb.Ping(c.ctx).Err(); err != nil {
		c.abort()
		return fmt.Errorf("gmqx: redis cluster %s: %w", c.cfg.Addr, err)
	}
	c.healthy.Store(true)

	// Take the node id before subscribing: a process that lost the ownership
	// race must never have applied an event from the channel it was about to
	// share. Two live processes with the same id would both apply every event and
	// Members() would collapse them into one entry; only an ownership key can see
	// both processes.
	owner, err := c.ownerRecord()
	if err != nil {
		c.abort()
		return fmt.Errorf("gmqx: redis cluster ownership record %s: %w", c.nodeID, err)
	}
	c.mu.Lock()
	c.ownerValue = owner
	c.mu.Unlock()
	acquireCtx, acquireCancel := context.WithTimeout(c.ctx, redisIOTimeout)
	err = c.acquireOwnership(acquireCtx)
	acquireCancel()
	if err != nil {
		c.abort()
		return err
	}

	// Subscribe before registering. The member key is what makes peers route to
	// this node, so it must not exist before the subscription is live; the other
	// order loses every event published in between.
	sub, err := c.subscribe()
	if err != nil {
		c.abort()
		return fmt.Errorf("gmqx: redis cluster subscribe %s: %w", c.nodeChannel(c.nodeID), err)
	}
	if err := c.refreshLocal(c.ctx); err != nil {
		_ = sub.Close()
		c.abort()
		return fmt.Errorf("gmqx: redis cluster register %s: %w", c.nodeID, err)
	}

	c.wg.Add(2)
	go c.receiveLoop(sub)
	go c.housekeeping()
	c.log.Info("gmqx redis cluster started", "node", nodeID, "addr", c.cfg.Addr,
		"channel", c.nodeChannel(nodeID), "incarnation", c.incarnation)
	return nil
}

// abort tears down a Start that failed: nothing the attempt started may outlive
// it, and the receiver is expected to discard or retry the node.
func (c *redisCluster) abort() {
	c.mu.Lock()
	c.running = false
	cancel := c.cancel
	sub := c.sub
	c.sub = nil
	rdb := c.rdb
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if sub != nil {
		_ = sub.Close()
	}
	if rdb != nil {
		// Give the id back before closing the client: a retry of Start must not
		// have to wait for NodeTTL. The release is a compare-and-delete, so a
		// failed Start can never release an id another process now owns.
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), redisIOTimeout)
		_, _ = c.releaseOwnership(releaseCtx)
		releaseCancel()
		_ = rdb.Close()
	}
}

// Stop implements Cluster. It is idempotent, leaves no goroutine behind and
// deregisters the node so peers stop routing to it immediately instead of after
// NodeTTL. It also releases the node id's ownership lease, so a restart with the
// same id does not have to wait for the TTL.
func (c *redisCluster) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = false
	// Cancel inside the lock: a receive loop that is about to install a new
	// subscription either does so before this point (and Stop closes it) or sees
	// a cancelled context and gives up. Cancelling first would leave a window in
	// which a fresh subscription is installed after Stop snapshotted the old one.
	if c.cancel != nil {
		c.cancel()
	}
	sub := c.sub
	c.sub = nil
	rdb := c.rdb
	c.mu.Unlock()

	if sub != nil {
		_ = sub.Close()
	}
	c.wg.Wait()

	if rdb == nil {
		return nil
	}
	// Deregister with a context that outlives the caller's cancellation: Stop has
	// to finish the teardown even when it is called from a cancelled context.
	delCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), redisIOTimeout)
	owned, err := c.releaseOwnership(delCtx)
	if err != nil {
		// The release could not be confirmed; fall back to the unconditional
		// deregistration, which is what Stop did before the ownership key existed.
		owned = true
	}
	if owned {
		// Only the process that still owns the id deregisters it: when the lease
		// was lost, the member and summary keys belong to the process that took
		// the id over.
		_, _ = rdb.Del(delCtx, c.memberKey(c.nodeID), c.summaryKey(c.nodeID)).Result()
	}
	// Directed publishes must stop being routed here now, not after the TTL. The
	// delete is conditional on this process still owning each record, so a client
	// that already moved to another node keeps its presence there.
	clearCtx, clearCancel := context.WithTimeout(context.WithoutCancel(ctx), redisIOTimeout)
	c.clearPresence(clearCtx)
	clearCancel()
	cancel()
	c.log.Info("gmqx redis cluster stopped", "node", c.nodeID)
	return rdb.Close()
}

// Members implements MemberView. The view is read from the member keys Redis
// holds, so every node sees the same membership and the TTLs — not this
// process — decide who is a member; peers only, this node is not reported to
// itself, exactly like the TCP backend.
//
// Members returns an empty slice when Redis cannot be read: an unreadable view is
// reported as "no peers" rather than as a stale one, so a caller cannot mistake
// a remembered member for a live node. It also returns an empty slice before
// Start and after Stop.
func (c *redisCluster) Members() []Member {
	view, err := c.loadView()
	if err != nil {
		if c.log != nil {
			c.log.Debug("gmqx redis cluster member view unreadable", "err", err)
		}
		return []Member{}
	}
	out := make([]Member, 0, len(view))
	for _, p := range view {
		out = append(out, p.Member)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// Spread implements Cluster.
//
// Only locally produced events leave a node. Spread stamps Origin, NodeID and
// Seq, refuses to forward an event that carries another node's origin, and drops
// a received event whose origin is this node; a received event is never
// republished, so a Redis topology in which a node accidentally subscribes to
// another node's channel cannot loop.
//
// A publish is written only to the channel of each node whose advertised summary
// matches the topic, one PUBLISH per target — never to a shared channel. A node
// whose summary is unknown receives the publish anyway (that node alone, not
// everyone): a missing summary must not cost a delivery. Retained publishes and
// kicks go to every known node channel, for the same reasons as in the TCP
// backend: retained state must exist on every node for its future subscribers,
// and a kick names a client id rather than a topic, so only the node holding
// that session can act on it.
//
// A directed publish (ClusterTarget) is routed through the presence view: its
// client id's record names the node that holds it, and the event is written to
// that node's channel only. A missing record means no node claims the client id
// (it is offline, it never connected, or its CONNECT is younger than the presence
// refresh), and the event is counted and dropped instead of being broadcast —
// presence routing is the point of the Redis backend, and a broadcast would make
// every node pay for a message at most one can use. A kick deliberately does not
// use presence: a stale record must not leave a zombie connection alive on the
// node the client actually moved to.
//
// Delivery is at-most-once: a failed publish is not retried here, because a
// retry loop would stall the broker's publish path. A locally produced event
// returns ErrClusterUnavailable so the caller can decide, and the receive loop
// keeps reconnecting in the background.
func (c *redisCluster) Spread(ctx context.Context, msg ClusterMessage) error {
	if !c.isRunning() {
		return errors.New("gmqx: cluster is not running")
	}
	if msg.Type != ClusterPublish && msg.Type != ClusterKick && msg.Type != ClusterTarget {
		return fmt.Errorf("gmqx: unknown cluster event type %q", msg.Type)
	}
	if msg.Origin != "" && msg.Origin != c.nodeID {
		// Invariant: a received event is never forwarded. Dropping it here (rather
		// than returning an error) keeps a misbehaving caller from retrying a
		// message that must not leave this node at all.
		c.recordDrop("cluster_forward", "refusing to forward a received event")
		return nil
	}
	// A locally produced event belongs to this node's namespace. An empty value
	// is stamped, exactly as Origin is; an explicit value of another namespace
	// cannot be produced here and is dropped instead of leaking into a scope this
	// node does not serve.
	if msg.Namespace == "" {
		msg.Namespace = c.namespace
	} else if msg.Namespace != c.namespace {
		c.recordDrop("cluster_namespace", "locally produced event for another namespace")
		return nil
	}
	msg.Origin = c.nodeID
	msg.NodeID = c.nodeID
	msg.Seq = c.seq.Add(1)

	payload, err := json.Marshal(redisFrame{Version: redisClusterWireVersion, NodeID: c.nodeID, Event: &msg})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, redisIOTimeout)
	defer cancel()

	if msg.Type == ClusterTarget {
		return c.spreadTarget(ctx, msg, payload)
	}

	view, err := c.routingView(ctx)
	if err != nil {
		c.noteErr(err)
		return fmt.Errorf("%w: read routing view: %w", ErrClusterUnavailable, err)
	}
	targets := c.routingTargets(view, msg)
	if len(targets) == 0 {
		// No peer can use the event: a single-node cluster, or a publish that only
		// a node without a matching summary could use and there is none. Nothing is
		// written, so this is a successful no-op rather than an error.
		return nil
	}
	rdb := c.client()
	if rdb == nil {
		return errors.New("gmqx: cluster is not running")
	}
	pipe := rdb.Pipeline()
	for _, nodeID := range targets {
		pipe.Publish(ctx, c.nodeChannel(nodeID), payload)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		c.noteErr(err)
		return fmt.Errorf("%w: publish to %d node channel(s): %w", ErrClusterUnavailable, len(targets), err)
	}
	c.noteOK()
	return nil
}

// spreadTarget writes one directed publish to the channel of the node that holds
// the client id, according to Redis presence. It reports nil for a client id no
// node claims: a directed publish is fire-and-forget, and the drop is counted.
func (c *redisCluster) spreadTarget(ctx context.Context, msg ClusterMessage, payload []byte) error {
	rdb := c.client()
	if rdb == nil {
		return errors.New("gmqx: cluster is not running")
	}
	nodeID, err := c.targetNode(ctx, msg.ClientID)
	if err != nil {
		c.noteErr(err)
		return fmt.Errorf("%w: read client presence: %w", ErrClusterUnavailable, err)
	}
	if nodeID == "" {
		c.recordDrop("target_unknown", "no presence record for the client id")
		return nil
	}
	if nodeID == c.nodeID {
		// The client reconnected to this node after the caller resolved it as
		// remote. Writing to this node's own channel would only come back as a
		// loop, and the caller cannot be told to deliver locally any more, so the
		// event is counted and dropped: at-most-once delivery.
		c.recordDrop("target_unknown", "client moved to this node during the spread")
		return nil
	}
	if err := rdb.Publish(ctx, c.nodeChannel(nodeID), payload).Err(); err != nil {
		c.noteErr(err)
		return fmt.Errorf("%w: publish to node %q: %w", ErrClusterUnavailable, nodeID, err)
	}
	c.noteOK()
	return nil
}

// targetNode returns the node id the presence record of clientID names, or "" when
// no record exists (the TTL elapsed, or no node ever claimed the id).
func (c *redisCluster) targetNode(ctx context.Context, clientID string) (string, error) {
	rdb := c.client()
	if rdb == nil {
		return "", errors.New("gmqx: redis cluster is not running")
	}
	if clientID == "" {
		return "", nil
	}
	raw, err := rdb.Get(ctx, c.presenceKey(c.namespace, clientID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	value := raw
	if i := strings.IndexByte(value, 0); i >= 0 {
		value = value[:i]
	}
	return value, nil
}

// clientConnected implements clientPresence: it records that this node holds the
// client id and wakes the housekeeping loop to write the presence record. Nothing
// here waits for Redis, because the caller is the CONNECT path of the broker.
func (c *redisCluster) clientConnected(clientID string) {
	if clientID == "" {
		return
	}
	c.presenceMu.Lock()
	if c.presence == nil {
		c.presence = make(map[string]time.Time)
	}
	c.presence[clientID] = time.Time{}
	delete(c.presenceGone, clientID)
	c.presenceMu.Unlock()
	c.signalPresence()
}

// clientDisconnected implements clientPresence: the client id left this node, so
// its record is deleted — by the compare-and-delete script, because the client may
// already have reconnected to another node and its record there must survive.
func (c *redisCluster) clientDisconnected(clientID string) {
	if clientID == "" {
		return
	}
	c.presenceMu.Lock()
	delete(c.presence, clientID)
	if c.presenceGone == nil {
		c.presenceGone = make(map[string]struct{})
	}
	c.presenceGone[clientID] = struct{}{}
	c.presenceMu.Unlock()
	c.signalPresence()
}

// signalPresence wakes the housekeeping loop without blocking the caller.
func (c *redisCluster) signalPresence() {
	if !c.isRunning() {
		return
	}
	select {
	case c.presenceCh <- struct{}{}:
	default:
	}
}

// presenceValue is the value of this node's presence records: the node id, a NUL
// separator and the process incarnation. The compare-and-delete script matches it
// byte for byte, which is what confines a disconnect to the record of the
// connection that ended.
func (c *redisCluster) presenceValue() []byte {
	return []byte(c.nodeID + "\x00" + c.incarnation)
}

// presenceKey is one client id's presence key. The namespace is part of the key
// (namespaceKey), so two namespaces sharing one Redis never claim each other's
// client ids.
func (c *redisCluster) presenceKey(namespace, clientID string) string {
	return c.prefix() + ":client:" + namespaceKey(namespace, clientID)
}

// flushPresence writes the records of the client ids this node holds and deletes
// the records of the ones that left. A record is rewritten when it has not been
// written for half the PresenceTTL, which keeps a connected client in the view
// without rewriting every record on every tick.
//
// The reconciliation is deliberately not a per-event transaction: a connect or
// disconnect only marks the id, and the loop applies the marks. A crash between
// the two costs one TTL of stale presence, and a publish that races a CONNECT by
// less than one refresh is counted as target_unknown instead of being broadcast.
func (c *redisCluster) flushPresence() {
	rdb := c.client()
	if rdb == nil || !c.isRunning() {
		return
	}
	now := time.Now()
	ttl := c.presenceTTL()
	half := ttl / 2
	c.presenceMu.Lock()
	var writes, deletes []string
	for id, at := range c.presence {
		if at.IsZero() || now.Sub(at) >= half {
			writes = append(writes, id)
		}
	}
	for id := range c.presenceGone {
		if _, live := c.presence[id]; !live {
			deletes = append(deletes, id)
		}
	}
	c.presenceMu.Unlock()
	if len(writes) == 0 && len(deletes) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(c.ctx, redisIOTimeout)
	defer cancel()
	value := c.presenceValue()
	pipe := rdb.Pipeline()
	for _, id := range writes {
		pipe.Set(ctx, c.presenceKey(c.namespace, id), value, ttl)
	}
	for _, id := range deletes {
		pipe.Eval(ctx, redisPresenceReleaseScript, []string{c.presenceKey(c.namespace, id)}, value)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		c.noteErr(err)
		c.log.Debug("gmqx redis cluster presence refresh failed", "err", err)
		return
	}
	c.noteOK()
	c.presenceMu.Lock()
	for _, id := range writes {
		if at, ok := c.presence[id]; ok && (at.IsZero() || now.Sub(at) >= half) {
			c.presence[id] = now
		}
	}
	for _, id := range deletes {
		if _, live := c.presence[id]; !live {
			delete(c.presenceGone, id)
		}
	}
	c.presenceMu.Unlock()
}

// clearPresence deletes the records of the client ids this node holds. Stop uses
// it so peers stop routing directed publishes here at once instead of after
// PresenceTTL.
func (c *redisCluster) clearPresence(ctx context.Context) {
	rdb := c.client()
	if rdb == nil {
		return
	}
	c.presenceMu.Lock()
	ids := make([]string, 0, len(c.presence)+len(c.presenceGone))
	for id := range c.presence {
		ids = append(ids, id)
	}
	for id := range c.presenceGone {
		ids = append(ids, id)
	}
	c.presence = make(map[string]time.Time)
	c.presenceGone = make(map[string]struct{})
	c.presenceMu.Unlock()
	if len(ids) == 0 {
		return
	}
	value := c.presenceValue()
	pipe := rdb.Pipeline()
	for _, id := range ids {
		pipe.Eval(ctx, redisPresenceReleaseScript, []string{c.presenceKey(c.namespace, id)}, value)
	}
	_, _ = pipe.Exec(ctx)
}

// routingTargets picks the node channels one event is published to.
func (c *redisCluster) routingTargets(view map[string]*redisPeer, msg ClusterMessage) []string {
	out := make([]string, 0, len(view))
	for nodeID, p := range view {
		if nodeID == c.nodeID || nodeID == "" {
			continue
		}
		switch {
		case msg.Type == ClusterKick || msg.Retain:
			out = append(out, nodeID)
		case !p.filtersKnown:
			// Unknown summary: send to this node only. Flooding every node would
			// defeat directed routing; sending nothing would lose messages.
			out = append(out, nodeID)
		case summaryMatches(p.filters, msg.Topic):
			out = append(out, nodeID)
		}
	}
	sort.Strings(out)
	return out
}

// routingView returns the cached routing view, re-reading it from Redis when it
// is older than one SummaryInterval. Concurrent callers share a single refresh.
//
// A read error returns the last known view together with the error; Spread
// reports the error for the locally produced event, so a caller is never told an
// event was routed while Redis is unreachable.
func (c *redisCluster) routingView(ctx context.Context) (map[string]*redisPeer, error) {
	c.viewMu.Lock()
	defer c.viewMu.Unlock()
	if c.view != nil && time.Since(c.viewAt) < c.summaryInterval() {
		return c.view, nil
	}
	view, err := c.loadViewCtx(ctx)
	if err != nil {
		return c.view, err
	}
	c.view, c.viewAt = view, time.Now()
	return view, nil
}

// loadView reads the routing view with a bounded context of its own (Members has
// no context to inherit).
func (c *redisCluster) loadView() (map[string]*redisPeer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisIOTimeout)
	defer cancel()
	return c.loadViewCtx(ctx)
}

// loadViewCtx builds the routing view from Redis: the member keys are the node
// list, and each member's summary key refines it. Two round trips plus one SCAN,
// independent of the number of subscriptions.
func (c *redisCluster) loadViewCtx(ctx context.Context) (map[string]*redisPeer, error) {
	rdb := c.client()
	if rdb == nil {
		return nil, errors.New("gmqx: redis cluster is not running")
	}

	keys, err := c.memberKeys(ctx, rdb)
	if err != nil {
		return nil, err
	}
	view := make(map[string]*redisPeer, len(keys))
	if len(keys) == 0 {
		return view, nil
	}
	members, err := rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	peers := make([]*redisPeer, 0, len(members))
	sumKeys := make([]string, 0, len(members))
	for _, v := range members {
		raw, ok := v.(string)
		if !ok {
			// The key expired between SCAN and MGET: the node is gone.
			continue
		}
		var rec redisMemberRecord
		if err := json.Unmarshal([]byte(raw), &rec); err != nil || rec.NodeID == "" {
			// A foreign key matching the member pattern: not ours, ignore it.
			continue
		}
		if rec.NodeID == c.nodeID {
			continue
		}
		peers = append(peers, &redisPeer{Member: Member{
			NodeID:      rec.NodeID,
			Incarnation: rec.Incarnation,
			Since:       time.Unix(0, rec.Since),
		}})
		sumKeys = append(sumKeys, c.summaryKey(rec.NodeID))
	}
	if len(peers) == 0 {
		return view, nil
	}
	summaries, err := rdb.MGet(ctx, sumKeys...).Result()
	if err != nil {
		return nil, err
	}
	for i, p := range peers {
		view[p.NodeID] = p
		raw, ok := summaries[i].(string)
		if !ok {
			// Unknown summary: routed every publish until it is advertised.
			continue
		}
		var rec redisSummaryRecord
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			continue
		}
		if rec.Namespace != c.namespace {
			// A summary of another namespace is not usable for directed routing:
			// the peer stays unknown, receives every publish, and drops it on the
			// receive path.
			continue
		}
		p.filters = capFilters(rec.Filters, c.maxSummaryFilters())
		p.filtersKnown = true
	}
	return view, nil
}

// memberKeys enumerates the member keys with SCAN: the member list is bounded by
// the number of nodes, and SCAN does not block Redis the way KEYS would.
func (c *redisCluster) memberKeys(ctx context.Context, rdb *redis.Client) ([]string, error) {
	keys := make([]string, 0, 8)
	var cursor uint64
	for {
		batch, next, err := rdb.Scan(ctx, cursor, c.memberPattern(), redisMemberScanCount).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		if next == 0 {
			return keys, nil
		}
		cursor = next
	}
}

// acquireOwnership takes or renews this node id's ownership lease and returns a
// classified error when another live process holds it.
func (c *redisCluster) acquireOwnership(ctx context.Context) error {
	owned, err := c.renewOwnership(ctx)
	if err != nil {
		return fmt.Errorf("gmqx: redis cluster node %q: take ownership: %w", c.nodeID, err)
	}
	if owned {
		c.owned.Store(true)
		return nil
	}
	holder, err := c.readOwner(ctx)
	if err == nil && holder.Incarnation != "" {
		return fmt.Errorf("gmqx: redis cluster node %q: %w (held by incarnation %q since %s)",
			c.nodeID, ErrNodeIDInUse, holder.Incarnation,
			time.Unix(0, holder.Since).UTC().Format(time.RFC3339Nano))
	}
	return fmt.Errorf("gmqx: redis cluster node %q: %w", c.nodeID, ErrNodeIDInUse)
}

// renewOwnership extends this node id's ownership lease, taking the key when it
// is absent. It reports whether this process owns the key afterwards; false
// means another live process holds it.
func (c *redisCluster) renewOwnership(ctx context.Context) (bool, error) {
	rdb := c.client()
	if rdb == nil {
		return false, errors.New("gmqx: redis cluster is not running")
	}
	value := c.ownershipValue()
	if len(value) == 0 {
		return false, errors.New("gmqx: redis cluster ownership value is not set")
	}
	n, err := rdb.Eval(ctx, redisOwnerRenewScript, []string{c.ownerKey(c.nodeID)},
		string(value), c.nodeTTL().Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// releaseOwnership deletes the ownership key when this process still holds it.
// It reports whether the key was ours, so Stop only deregisters keys it still
// owns.
func (c *redisCluster) releaseOwnership(ctx context.Context) (bool, error) {
	rdb := c.client()
	value := c.ownershipValue()
	if rdb == nil || len(value) == 0 {
		return false, nil
	}
	n, err := rdb.Eval(ctx, redisOwnerReleaseScript, []string{c.ownerKey(c.nodeID)}, string(value)).Int64()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// ownershipValue returns the value this Start wrote to the ownership key. Start
// sets it before the backend starts any goroutine; the mutex keeps a concurrent
// Stop from observing a half-updated field.
func (c *redisCluster) ownershipValue() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ownerValue
}

// ownerRecord is the value of this Start's ownership key: the same identity as
// the member record, so an operator or a refused Start can name the process that
// holds the node id.
func (c *redisCluster) ownerRecord() ([]byte, error) {
	return json.Marshal(redisOwnerRecord{
		NodeID:      c.nodeID,
		Incarnation: c.incarnation,
		Since:       c.since.UnixNano(),
	})
}

// readOwner reads the current ownership record, for the error a refused Start
// reports. A missing or unreadable record is reported as the zero record.
func (c *redisCluster) readOwner(ctx context.Context) (redisOwnerRecord, error) {
	rdb := c.client()
	if rdb == nil {
		return redisOwnerRecord{}, errors.New("gmqx: redis cluster is not running")
	}
	raw, err := rdb.Get(ctx, c.ownerKey(c.nodeID)).Result()
	if err != nil {
		return redisOwnerRecord{}, err
	}
	var rec redisOwnerRecord
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return redisOwnerRecord{}, err
	}
	return rec, nil
}

// refreshLocal renews this node's ownership lease and writes its membership and
// subscription summary to Redis with their TTLs. It is the heartbeat of the
// node's presence: as long as it succeeds, peers see the node and route to it.
//
// The lease is renewed first, and the member key is not written when it cannot
// be renewed: at that point another live process holds the id, so the member and
// summary keys belong to that process, not to this one.
func (c *redisCluster) refreshLocal(ctx context.Context) error {
	rdb := c.client()
	if rdb == nil {
		return errors.New("gmqx: redis cluster is not running")
	}
	ctx, cancel := context.WithTimeout(ctx, redisIOTimeout)
	defer cancel()

	owned, err := c.renewOwnership(ctx)
	if err != nil {
		return err
	}
	if !owned {
		if c.owned.CompareAndSwap(true, false) {
			c.log.Warn("gmqx redis cluster node id ownership lost", "node", c.nodeID, "key", c.ownerKey(c.nodeID))
		}
		return fmt.Errorf("gmqx: redis cluster node %q: %w", c.nodeID, ErrNodeIDInUse)
	}
	if c.owned.CompareAndSwap(false, true) {
		c.log.Info("gmqx redis cluster node id ownership regained", "node", c.nodeID)
	}

	member, err := json.Marshal(redisMemberRecord{
		NodeID:      c.nodeID,
		Incarnation: c.incarnation,
		Since:       c.since.UnixNano(),
	})
	if err != nil {
		return err
	}
	filters, known := c.localFilters()
	var summary []byte
	if known {
		summary, err = json.Marshal(redisSummaryRecord{
			NodeID:    c.nodeID,
			Filters:   filters,
			Namespace: c.namespace,
			Updated:   time.Now().UnixNano(),
		})
		if err != nil {
			return err
		}
	}
	pipe := rdb.Pipeline()
	pipe.Set(ctx, c.memberKey(c.nodeID), member, c.nodeTTL())
	if known {
		pipe.Set(ctx, c.summaryKey(c.nodeID), summary, c.summaryTTL())
	} else {
		// A handler that cannot describe its sessions advertises nothing, so peers
		// treat this node as unknown and send every publish to it.
		pipe.Del(ctx, c.summaryKey(c.nodeID))
	}
	_, err = pipe.Exec(ctx)
	return err
}

// localFilters recomputes the summary this node advertises. A handler that does
// not implement subscriptionProvider never advertises, which makes peers treat
// it as unknown (availability over precision).
func (c *redisCluster) localFilters() ([]string, bool) {
	provider, ok := c.handler.(subscriptionProvider)
	if !ok {
		return nil, false
	}
	current := provider.subscriptionFilters()
	c.summaryMu.Lock()
	defer c.summaryMu.Unlock()
	return c.local.refresh(current, c.maxSummaryFilters()), true
}

// subscriptionsChanged implements subscriptionChangeNotifier. The broker calls it
// before it acknowledges a SUBSCRIBE/UNSUBSCRIBE, so the new summary is queued
// immediately instead of waiting up to one SummaryInterval. The write itself
// stays off the broker's path: a slow or unreachable Redis must not stall
// connection handling, and the next SummaryInterval retries anyway.
func (c *redisCluster) subscriptionsChanged() {
	if !c.isRunning() {
		return
	}
	select {
	case c.refreshCh <- struct{}{}:
	default:
	}
}

// housekeeping refreshes this node's keys, keeps the routing view warm and probes
// Redis.
func (c *redisCluster) housekeeping() {
	defer c.wg.Done()
	summary := time.NewTicker(c.summaryInterval())
	defer summary.Stop()
	heartbeat := time.NewTicker(c.heartbeatInterval())
	defer heartbeat.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.refreshCh:
			c.refreshKeys()
		case <-c.presenceCh:
			c.flushPresence()
		case <-summary.C:
			c.refreshKeys()
			c.refreshView()
			c.flushPresence()
		case <-heartbeat.C:
			c.probe()
		}
	}
}

// refreshKeys rewrites the local keys and logs a failure instead of returning it:
// the caller is a background loop that keeps retrying. A lost ownership lease is
// already logged as a transition by refreshLocal, so it is not reported as a
// Redis outage.
func (c *redisCluster) refreshKeys() {
	if err := c.refreshLocal(c.ctx); err != nil {
		if !errors.Is(err, ErrNodeIDInUse) {
			c.noteErr(err)
		}
		c.log.Debug("gmqx redis cluster refresh failed", "err", err)
		return
	}
	c.noteOK()
}

// refreshView re-reads the routing view and reports membership transitions, so an
// operator can see nodes joining and leaving.
func (c *redisCluster) refreshView() {
	c.viewMu.Lock()
	before := c.view
	view, err := c.loadViewCtx(c.ctx)
	if err != nil {
		c.viewMu.Unlock()
		c.noteErr(err)
		c.log.Debug("gmqx redis cluster view refresh failed", "err", err)
		return
	}
	c.view, c.viewAt = view, time.Now()
	c.viewMu.Unlock()
	c.noteOK()
	c.logViewChanges(before, view)
}

// logViewChanges logs joins, leaves and restarts at Debug, the same events the
// TCP backend logs from its handshake.
func (c *redisCluster) logViewChanges(before, after map[string]*redisPeer) {
	for id, p := range after {
		old, ok := before[id]
		switch {
		case !ok:
			c.log.Debug("gmqx redis cluster member joined", "node", id, "incarnation", p.Incarnation, "since", p.Since)
		case old.Incarnation != p.Incarnation:
			c.log.Debug("gmqx redis cluster member restarted", "node", id, "incarnation", p.Incarnation, "since", p.Since)
		}
	}
	for id, p := range before {
		if _, ok := after[id]; !ok {
			c.log.Debug("gmqx redis cluster member removed", "node", id, "incarnation", p.Incarnation, "reason", "member key expired")
		}
	}
}

// probe pings Redis so a lost connection is noticed (and logged) even when the
// node is idle.
func (c *redisCluster) probe() {
	rdb := c.client()
	if rdb == nil {
		return
	}
	ctx, cancel := context.WithTimeout(c.ctx, redisIOTimeout)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		c.noteErr(err)
		return
	}
	c.noteOK()
}

// noteErr records a Redis failure and logs the transition into the degraded
// state. Only transitions are logged: a node publishing thousands of events per
// second must not log thousands of lines while Redis is down.
func (c *redisCluster) noteErr(err error) {
	if c.healthy.CompareAndSwap(true, false) {
		c.log.Warn("gmqx redis cluster unreachable", "addr", c.cfg.Addr, "err", err)
	}
}

// noteOK records the transition back to a reachable Redis.
func (c *redisCluster) noteOK() {
	if c.healthy.CompareAndSwap(false, true) {
		c.log.Info("gmqx redis cluster reachable again", "addr", c.cfg.Addr)
	}
}

// receiveLoop keeps this node's channel subscription alive. One goroutine reads
// one subscription and applies one event at a time: events are ordered, and a
// slow consumer cannot grow an unbounded queue — the Redis socket buffer bounds
// the wait and a lost connection is re-established with a bounded backoff. The
// read itself has no deadline (a normal blocking pub/sub read); a connection that
// dies is reported by the socket, and the heartbeat probe reports an outage the
// socket has not noticed yet.
func (c *redisCluster) receiveLoop(sub *redis.PubSub) {
	defer c.wg.Done()
	backoff := redisMinBackoff
	for {
		if sub == nil {
			var err error
			if sub, err = c.subscribe(); err != nil {
				if c.ctx.Err() != nil {
					return
				}
				c.noteErr(err)
				c.log.Debug("gmqx redis cluster subscribe failed", "err", err)
				if !sleepCtx(c.ctx, backoff) {
					return
				}
				if backoff *= 2; backoff > redisMaxBackoff {
					backoff = redisMaxBackoff
				}
				continue
			}
			backoff = redisMinBackoff
			c.noteOK()
		}
		err := c.consume(sub)
		c.detach(sub)
		_ = sub.Close()
		sub = nil
		if c.ctx.Err() != nil {
			return
		}
		// Debug, not Warn: the outage itself is logged once by noteErr, and a
		// backoff-driven reconnect must not turn one outage into a log flood.
		c.noteErr(err)
		c.log.Debug("gmqx redis cluster subscription lost", "channel", c.nodeChannel(c.nodeID), "err", err)
		if !sleepCtx(c.ctx, backoff) {
			return
		}
		if backoff *= 2; backoff > redisMaxBackoff {
			backoff = redisMaxBackoff
		}
	}
}

// subscribe opens and confirms this node's channel subscription. The
// confirmation matters: PubSub.Subscribe only writes the command, so without
// waiting for the Subscription reply a peer that already saw the member key could
// publish into a channel nobody is listening on yet.
func (c *redisCluster) subscribe() (*redis.PubSub, error) {
	channel := c.nodeChannel(c.nodeID)
	rdb := c.client()
	if rdb == nil {
		return nil, errors.New("gmqx: redis cluster is not running")
	}
	ctx, cancel := context.WithTimeout(c.ctx, redisIOTimeout)
	defer cancel()
	sub := rdb.Subscribe(ctx, channel)
	if !c.attach(sub) {
		_ = sub.Close()
		return nil, context.Canceled
	}
	for {
		v, err := sub.Receive(ctx)
		if err != nil {
			_ = sub.Close()
			return nil, err
		}
		switch m := v.(type) {
		case *redis.Subscription:
			if m.Kind == "subscribe" && m.Channel == channel {
				return sub, nil
			}
		case *redis.Message:
			// An event arrived before the confirmation: apply it instead of
			// dropping it, exactly as the loop would.
			c.handlePayload(m.Channel, m.Payload)
		}
	}
}

// consume applies events until the subscription breaks or the cluster stops.
func (c *redisCluster) consume(sub *redis.PubSub) error {
	for {
		msg, err := sub.ReceiveMessage(c.ctx)
		if err != nil {
			return err
		}
		c.handlePayload(msg.Channel, msg.Payload)
	}
}

// attach publishes the current subscription to Stop. It returns false once the
// cluster is stopping, so the receive loop can never install a subscription that
// Stop has already passed.
func (c *redisCluster) attach(sub *redis.PubSub) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx.Err() != nil {
		return false
	}
	c.sub = sub
	return true
}

// detach forgets a subscription the receive loop has given up on.
func (c *redisCluster) detach(sub *redis.PubSub) {
	c.mu.Lock()
	if c.sub == sub {
		c.sub = nil
	}
	c.mu.Unlock()
}

// handlePayload decodes one frame from this node's channel and applies it.
func (c *redisCluster) handlePayload(channel, payload string) {
	if channel != c.nodeChannel(c.nodeID) {
		// Defence in depth: the node subscribes to its own channel only.
		return
	}
	var f redisFrame
	if err := json.Unmarshal([]byte(payload), &f); err != nil {
		c.log.Debug("gmqx redis cluster frame dropped", "channel", channel, "err", err)
		return
	}
	if f.Version != redisClusterWireVersion {
		c.log.Debug("gmqx redis cluster frame dropped", "channel", channel,
			"version", f.Version, "want", redisClusterWireVersion)
		return
	}
	if f.Event == nil {
		return
	}
	c.dispatchEvent(*f.Event)
}

// dispatchEvent hands one received event to the broker.
//
// The namespace is the first filter: an event of another namespace is dropped
// before it touches the dedup table or the handler, so a Redis deployment that
// carries several namespaces never applies another scope's publish, retained
// update or kick.
//
// The order of the two identity filters is the loop guard: an event this node
// produced is dropped before it touches the dedup table, so own traffic can never
// pollute the table and evict the identities of real remote events. A redelivered
// event is dropped by (Origin, Seq) instead of being applied twice. An event is
// never forwarded.
//
// A directed publish (ClusterTarget) reaches a handler that implements
// targetHandler — the broker does — and is counted as undeliverable otherwise. It
// is deduplicated exactly like a publish or a kick, so a presence race that sends
// the same event twice delivers it once.
func (c *redisCluster) dispatchEvent(msg ClusterMessage) {
	if c.handler == nil {
		return
	}
	if msg.Type != ClusterPublish && msg.Type != ClusterKick && msg.Type != ClusterTarget {
		return
	}
	if msg.Namespace != c.namespace {
		c.recordDrop("cluster_namespace", "event belongs to another namespace")
		return
	}
	if msg.Origin == "" {
		msg.Origin = msg.NodeID
	}
	if msg.Origin == c.nodeID {
		c.recordDrop("cluster_loop", "event originated on this node")
		return
	}
	if msg.Seq != 0 && c.dedup.seen(dedupKey{origin: msg.Origin, seq: msg.Seq}) {
		c.recordDrop("cluster_duplicate", "event already applied")
		return
	}
	switch msg.Type {
	case ClusterPublish:
		c.handler.OnRemotePublish(msg)
	case ClusterKick:
		c.handler.OnRemoteKick(msg)
	case ClusterTarget:
		handler, ok := c.handler.(targetHandler)
		if !ok {
			c.recordDrop("target_unknown", "handler cannot receive a directed publish")
			return
		}
		handler.OnRemoteTarget(msg)
	}
}

// recordDrop counts a dropped event and reports it when a metrics sink is
// configured, using the TCP backend's reasons.
func (c *redisCluster) recordDrop(reason, detail string) {
	c.drops.Add(1)
	if c.cfg.Metrics != nil {
		c.cfg.Metrics.RecordDroppedMessage(reason)
	}
	c.log.Debug("gmqx redis cluster event dropped", "reason", reason, "detail", detail)
}

func (c *redisCluster) isRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

// client returns the Redis client, or nil before Start.
func (c *redisCluster) client() *redis.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rdb
}

// options builds the go-redis client configuration. Client-level retries are
// disabled: the backend decides when to retry (the receive loop, with a bounded
// backoff) and a publish must return its error to the caller instead of silently
// burning retries.
func (c *redisCluster) options() *redis.Options {
	return &redis.Options{
		Addr:         strings.TrimSpace(c.cfg.Addr),
		Password:     c.cfg.Password,
		DB:           c.cfg.DB,
		MaxRetries:   -1,
		DialTimeout:  redisDialTimeout,
		ReadTimeout:  redisIOTimeout,
		WriteTimeout: redisIOTimeout,
		ClientName:   "gmqx-cluster",
		// Honour the deadlines this backend derives from redisIOTimeout instead of
		// letting a command run until ReadTimeout/WriteTimeout expires; the two are
		// equal here, so the effective bound is the documented one either way, but
		// an honouring client never outlives its caller's context.
		ContextTimeoutEnabled: true,
	}
}

// validate refuses a configuration that cannot work rather than starting a node
// that looks clustered but delivers nothing.
func (c *redisCluster) validate(nodeID string) error {
	if nodeID == "" {
		return errors.New("gmqx: cluster node id is required")
	}
	if strings.TrimSpace(c.cfg.Addr) == "" {
		return errors.New("gmqx: redis cluster Addr is required")
	}
	if c.cfg.DB < 0 {
		return fmt.Errorf("gmqx: redis cluster DB must not be negative, got %d", c.cfg.DB)
	}
	if prefix := c.prefix(); strings.ContainsAny(prefix, `*?[]\`) {
		return fmt.Errorf("gmqx: redis cluster ChannelPrefix must not contain SCAN glob characters, got %q", prefix)
	}
	if c.cfg.SummaryInterval < 0 {
		return fmt.Errorf("gmqx: redis cluster SummaryInterval must not be negative, got %v", c.cfg.SummaryInterval)
	}
	if c.cfg.SummaryTTL < 0 {
		return fmt.Errorf("gmqx: redis cluster SummaryTTL must not be negative, got %v", c.cfg.SummaryTTL)
	}
	if c.cfg.NodeTTL < 0 {
		return fmt.Errorf("gmqx: redis cluster NodeTTL must not be negative, got %v", c.cfg.NodeTTL)
	}
	if c.cfg.HeartbeatInterval < 0 {
		return fmt.Errorf("gmqx: redis cluster HeartbeatInterval must not be negative, got %v", c.cfg.HeartbeatInterval)
	}
	if c.cfg.PresenceTTL < 0 {
		return fmt.Errorf("gmqx: redis cluster PresenceTTL must not be negative, got %v", c.cfg.PresenceTTL)
	}
	if c.cfg.DedupTTL < 0 {
		return fmt.Errorf("gmqx: redis cluster DedupTTL must not be negative, got %v", c.cfg.DedupTTL)
	}
	if c.cfg.DedupSize < 0 {
		return fmt.Errorf("gmqx: redis cluster DedupSize must not be negative, got %d", c.cfg.DedupSize)
	}
	if c.cfg.MaxSummaryFilters < 0 {
		return fmt.Errorf("gmqx: redis cluster MaxSummaryFilters must not be negative, got %d", c.cfg.MaxSummaryFilters)
	}
	// A key that expires before the next refresh makes the node flap in the
	// routing view: the member key would drop the node (lost deliveries) and the
	// summary key would turn directed routing into flooding.
	if ttl := c.nodeTTL(); ttl <= c.summaryInterval() {
		return fmt.Errorf("gmqx: redis cluster NodeTTL %v must exceed SummaryInterval %v", ttl, c.summaryInterval())
	}
	if ttl := c.summaryTTL(); ttl <= c.summaryInterval() {
		return fmt.Errorf("gmqx: redis cluster SummaryTTL %v must exceed SummaryInterval %v", ttl, c.summaryInterval())
	}
	if ttl := c.presenceTTL(); ttl <= c.summaryInterval() {
		return fmt.Errorf("gmqx: redis cluster PresenceTTL %v must exceed SummaryInterval %v", ttl, c.summaryInterval())
	}
	return nil
}

// nodeChannel is one node's private event channel.
func (c *redisCluster) nodeChannel(nodeID string) string { return c.prefix() + ":node:" + nodeID }

// ownerKey holds one node id's ownership lease.
func (c *redisCluster) ownerKey(nodeID string) string { return c.prefix() + ":owner:" + nodeID }

// summaryKey holds one node's advertised subscription summary.
func (c *redisCluster) summaryKey(nodeID string) string { return c.prefix() + ":subs:" + nodeID }

// memberKey holds one node's liveness and identity.
func (c *redisCluster) memberKey(nodeID string) string { return c.prefix() + ":member:" + nodeID }

// memberPattern is the SCAN pattern for the member keys.
func (c *redisCluster) memberPattern() string { return c.prefix() + ":member:*" }

func (c *redisCluster) prefix() string {
	if p := strings.TrimSpace(c.cfg.ChannelPrefix); p != "" {
		return p
	}
	return defaultRedisChannelPrefix
}

func (c *redisCluster) summaryInterval() time.Duration {
	if c.cfg.SummaryInterval > 0 {
		return c.cfg.SummaryInterval
	}
	return defaultSummaryInterval
}

func (c *redisCluster) summaryTTL() time.Duration {
	if c.cfg.SummaryTTL > 0 {
		return c.cfg.SummaryTTL
	}
	return redisTTLFactor * c.summaryInterval()
}

func (c *redisCluster) nodeTTL() time.Duration {
	if c.cfg.NodeTTL > 0 {
		return c.cfg.NodeTTL
	}
	return redisTTLFactor * c.summaryInterval()
}

func (c *redisCluster) presenceTTL() time.Duration {
	if c.cfg.PresenceTTL > 0 {
		return c.cfg.PresenceTTL
	}
	return redisTTLFactor * c.summaryInterval()
}

func (c *redisCluster) heartbeatInterval() time.Duration {
	if c.cfg.HeartbeatInterval > 0 {
		return c.cfg.HeartbeatInterval
	}
	return defaultHeartbeatInterval
}

func (c *redisCluster) maxSummaryFilters() int {
	if c.cfg.MaxSummaryFilters > 0 {
		return c.cfg.MaxSummaryFilters
	}
	return defaultMaxSummaryFilters
}

// viewSummary reports a peer's cached routing summary and whether it is known
// (used by tests).
func (c *redisCluster) viewSummary(nodeID string) ([]string, bool) {
	c.viewMu.Lock()
	defer c.viewMu.Unlock()
	p, ok := c.view[nodeID]
	if !ok || !p.filtersKnown {
		return nil, false
	}
	return append([]string(nil), p.filters...), true
}

// viewMembers reports the cached routing view node ids, sorted (used by tests).
func (c *redisCluster) viewMembers() []string {
	c.viewMu.Lock()
	defer c.viewMu.Unlock()
	out := make([]string, 0, len(c.view))
	for id := range c.view {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// dropCount reports dropped events (used by tests).
func (c *redisCluster) dropCount() uint64 { return c.drops.Load() }
