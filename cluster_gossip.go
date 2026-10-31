package gmqx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxid/gnety"
)

// GossipClusterConfig configures the built-in gossip cluster backend.
//
// The backend is a SWIM-style membership service with an epidemic event
// dissemination layer on top of it, speaking JSON frames over gnety's
// length-prefixed TCP transport (the same transport and codec the TCP mesh
// backend uses). It needs no external service: every node is seeded with at
// least one reachable peer address and learns the rest of the cluster from the
// membership gossip.
//
// Zero values select the documented default. Addr must be explicit (no ":0")
// and must differ from every MQTT listener address.
type GossipClusterConfig struct {
	// Addr is the cluster listen address this node advertises. Peers dial it and
	// compare it with their own view of this node, so it must be reachable under
	// exactly this spelling from every peer (configured seeds, no DNS aliases).
	Addr string
	// Seeds are the peer cluster addresses to dial. One reachable seed is enough
	// for a new node to join: the seed's ping/ack carries its member view, and
	// the new node then gossips with the members it learned. A node with no
	// seeds starts alone, serves local traffic and logs that it is alone.
	Seeds []string

	// PingInterval is how often a node probes one random member directly
	// (default 1s).
	PingInterval time.Duration
	// PingTimeout is how long a direct probe may take before the node asks the
	// other members to probe the target indirectly (default 500ms). A failed
	// indirect round costs another PingTimeout before the target becomes suspect.
	PingTimeout time.Duration
	// SuspicionMultiplier is how many PingIntervals a member may stay suspect
	// before it is declared dead (default 3). The suspect window starts at the
	// first failed probe and is never extended by later failed probes, so a
	// suspect member is resolved in bounded time.
	SuspicionMultiplier int
	// DeadTimeout is how long a dead member stays in the view before it is
	// removed (default 3× the suspect timeout). Removal is what lets a node that
	// died and restarted with the same node id rejoin: the tombstone stops
	// overriding the fresh incarnation once it is gone.
	DeadTimeout time.Duration

	// Fanout is how many peers one node writes one event to (default
	// min(len(members), 3), evaluated at send time). Receivers forward to Fanout
	// peers they did not receive the event from, so a cluster of N nodes sees
	// O(N·Fanout) frames per event instead of N². When the member count is at or
	// below Fanout the origin writes every peer and each receiver has nothing
	// left to forward to: a 2- or 3-node cluster therefore degenerates to a full
	// broadcast, which is exactly what it wants.
	Fanout int

	// ReconnectInterval is the wait before a failed or closed member link is
	// dialed again (default 500ms).
	ReconnectInterval time.Duration
	// DialTimeout bounds one outbound dial (default 2s).
	DialTimeout time.Duration

	// DedupSize is the number of event identities remembered for duplicate
	// suppression (default 4096). The oldest identity is evicted first, so the
	// table is O(DedupSize) no matter how much traffic arrives.
	DedupSize int
	// DedupTTL is how long an event identity suppresses duplicates (default
	// 30s).
	DedupTTL time.Duration

	// SummaryInterval is how often a node re-advertises the topic filters its
	// local sessions are subscribed to (default 1s). A summary not refreshed
	// within three intervals is treated as unknown, and publishes fall back to
	// the epidemic path for that peer.
	SummaryInterval time.Duration
	// MaxSummaryFilters caps how many filters one node advertises, and how many
	// it keeps from a peer's summary (default 256). When a node has more, the
	// filters present longest are left out so new subscriptions keep routing.
	MaxSummaryFilters int

	// PresenceTTL is how long the presence claim of one client id survives
	// without being refreshed (default 30s). The broker reports every CONNECT and
	// disconnect, so a claim is created or tombstoned immediately; the TTL is the
	// safety net for a node that dies without saying goodbye, and the bound on how
	// long a stale claim can steer a directed publish.
	PresenceTTL time.Duration

	// MaxGossipDelta bounds how many member records one ping, ack or update
	// frame carries, this node's own record included (default 16). A larger
	// cluster converges through the rotating anti-entropy slice instead of one
	// unbounded frame.
	MaxGossipDelta int
	// MaxPresenceDelta bounds how many client-id presence claims one frame
	// carries (default 16).
	MaxPresenceDelta int
	// MaxPresenceEntries bounds the presence table (default 4096). A client id
	// beyond the bound is not remembered, which costs presence routing for it but
	// never correctness: an unknown owner falls back to epidemic forwarding.
	MaxPresenceEntries int

	// Metrics, when set, receives RecordDroppedMessage for events the cluster
	// drops ("cluster_duplicate", "cluster_loop", "cluster_forward",
	// "cluster_namespace", "cluster_backpressure", "cluster_unreachable" and
	// "target_unknown" for a directed publish this node cannot route).
	Metrics MetricsManager
	// Logger receives membership decisions and the alone notice (default
	// slog.Default()).
	Logger *slog.Logger
	// Namespace scopes the traffic of this backend: an event, summary or
	// presence claim of another namespace is dropped. When the backend is passed
	// to a Broker, the broker's Config.Namespace is authoritative and overrides
	// this value at Start. The empty string is the default namespace.
	Namespace string
}

// SubscriptionSummary is the routing metadata one gossip node advertises about
// its local sessions: the topic filters they are subscribed to and the shared
// subscription groups they hold a live member of, in the namespace the summary
// was produced for.
//
// It travels as membership metadata — on ping, ack and update frames — and is
// stored on the peer's member record. Directed routing consults it before an
// epidemic spread: a peer whose summary matches the topic is preferred, a peer
// whose summary is known and does not match is skipped, and a peer whose summary
// is unknown or stale is sent the publish anyway so the epidemic can reach a
// subscriber that connected before its summary arrived. A summary is therefore
// an optimisation, never a correctness requirement; shared group election (see
// shared.go) does need the shared half, and a node without it is simply not a
// shared candidate.
//
// The summary reuses the broker's summarySet (see cluster_summary.go): the
// filters are ordered oldest first and capped, so the eviction policy is the same
// one the TCP and Redis backends use.
type SubscriptionSummary struct {
	Filters []string `json:"filters,omitempty"`
	// Shared lists the shared subscription group keys the node holds a live
	// member of.
	Shared []string `json:"shared,omitempty"`
	// Namespace scopes the summary. A peer only trusts summaries of its own
	// namespace; an empty value is the default namespace.
	Namespace string `json:"ns,omitempty"`
	// Updated is the sender's wall-clock stamp, informational only: a receiver
	// dates the summary at arrival so clock skew cannot make it look fresh.
	Updated int64 `json:"updated,omitempty"`
}

const (
	gossipWireVersion = 1

	// Gossip frame kinds. Membership and routing metadata ride on ping, ack and
	// update frames; events travel in their own lean frame.
	gossipFramePing    = "ping"
	gossipFrameAck     = "ack"
	gossipFramePingReq = "pingreq"
	gossipFrameUpdate  = "update"
	gossipFrameEvent   = "event"

	defaultPingInterval        = time.Second
	defaultPingTimeout         = 500 * time.Millisecond
	defaultSuspicionMultiplier = 3
	defaultGossipFanout        = 3
	defaultMaxGossipDelta      = 16
	defaultMaxPresenceDelta    = 16
	defaultMaxPresenceEntries  = 4096
	defaultPresenceTTL         = 30 * time.Second
	defaultGossipDialTimeout   = 2 * time.Second
	defaultReconnectGossip     = 500 * time.Millisecond

	// indirectProbeFanout is the k of a SWIM indirect probe: how many other
	// members a node asks to ping a target that did not answer directly.
	indirectProbeFanout = 3

	// gossipLinkQueue bounds the frames waiting to be written on one link. A
	// full queue drops the frame and counts it; it never blocks a publisher, a
	// receiver or a membership tick.
	gossipLinkQueue = 32

	// maxGossipProbes bounds the outstanding direct/indirect probes.
	maxGossipProbes = 64
	// maxIndirectPings bounds the pings a node performs on behalf of others and
	// has not answered yet.
	maxIndirectPings = 256

	// maxGossipMembers bounds the member view. A peer can gossip any node id it
	// likes, so without a bound the view would be an unbounded map fed by the
	// network. Direct peers are also bounded by the listener's MaxSessions.
	maxGossipMembers = 4096
	// maxGossipSessions bounds concurrent member links of one node.
	maxGossipSessions = 1024

	// summaryStaleFactor is shared with the TCP backend: a summary older than
	// this many refresh intervals is treated as unknown.
	gossipSummaryStaleFactor = summaryStaleFactor

	// gossipLinkAttr ties an inbound gnety session to its gossip link.
	gossipLinkAttr = "gmqx.gossip.link"
)

// NewGossipCluster creates the built-in gossip cluster backend: SWIM-style
// membership and epidemic event dissemination over gnety's length-prefixed TCP
// transport.
//
// TLS: the TCP mesh backend this backend is modelled on has no TLS knobs
// (TCPClusterConfig carries none), so neither does this one; member links are
// plain TCP and gnety's TLS listener/client configuration is not exposed here.
// Deployments that need encrypted cluster traffic put the links on a private
// network or a TLS terminator in front of the cluster addresses.
func NewGossipCluster(cfg GossipClusterConfig) Cluster {
	return newGossipCluster(cfg)
}

func newGossipCluster(cfg GossipClusterConfig) *gossipCluster {
	return &gossipCluster{
		cfg:           cfg,
		namespace:     cfg.Namespace,
		links:         map[*gossipLink]struct{}{},
		linksByNode:   map[string]*gossipLink{},
		members:       map[string]*gossipMember{},
		presence:      map[string]*gossipPresence{},
		pendingProbes: map[uint64]*gossipProbe{},
		indirect:      map[uint64]*gossipIndirect{},
		local:         newSummarySet(),
		sharedLocal:   newSummarySet(),
		updateCh:      make(chan struct{}, 1),
		dialCh:        make(chan gossipDialRequest, 64),
		dialing:       map[string]bool{},
		dialNext:      map[string]time.Time{},
		addrNodes:     map[string]string{},
	}
}

// gossipCluster is one node's view of a gossip cluster.
//
// Concurrency: mu guards membership, links, the probe bookkeeping, the presence
// table and the advertised summary. It is never held across a network write:
// frames are built under the lock (they are snapshots), then handed to a link's
// bounded queue after it is released. The broker's handler is called without the
// lock as well.
type gossipCluster struct {
	cfg         GossipClusterConfig
	log         *slog.Logger
	handler     ClusterHandler
	namespace   string
	nodeID      string
	incarnation string
	since       time.Time
	server      *gnety.Server

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	running bool
	started bool

	// members is the gossip member view. links is every physical connection,
	// linksByNode the one connection that currently backs each identified
	// member. A member without a link is still a member: the link is re-dialed.
	members     map[string]*gossipMember
	links       map[*gossipLink]struct{}
	linksByNode map[string]*gossipLink

	// incNumber is this process's SWIM incarnation number, incremented when a
	// gossip record accuses it of being suspect or dead (see refuteSelfLocked).
	incNumber uint64

	// pendingProbes correlates a probe nonce with its target and deadline.
	// probeOrder bounds the table by insertion order.
	pendingProbes map[uint64]*gossipProbe
	probeOrder    []uint64
	// indirect holds the indirect pings this node performs on behalf of another
	// member, keyed by the nonce of the ping it sent to the target.
	indirect map[uint64]*gossipIndirect

	// presence is the client-id -> node view. presenceSeq is the Lamport clock
	// every claim's version is drawn from.
	presence    map[string]*gossipPresence
	presenceSeq uint64

	// local and sharedLocal are the summary sets this node advertises; summary is
	// the snapshot carried by the next metadata frame, built at most every
	// SummaryInterval and immediately on a subscription change.
	local        *summarySet
	sharedLocal  *summarySet
	summary      SubscriptionSummary
	summaryKnown bool
	summaryAt    time.Time
	// memberCursor rotates the anti-entropy slice of membership records.
	memberCursor int

	// addrNodes maps a dialed address to the node id behind it, so a seed or
	// member dial stops while one link already backs that node.
	addrNodes map[string]string
	// dialing and dialNext bound outbound dials: one in flight per address and a
	// backoff between attempts.
	dialing  map[string]bool
	dialNext map[string]time.Time

	seq      atomic.Uint64
	nonce    atomic.Uint64
	dedup    *dedupTable
	drops    atomic.Uint64
	forwards atomic.Uint64
	// targetRouted and targetFlooded count directed publishes by the routing
	// decision that was taken (presence owner vs epidemic fallback).
	targetRouted  atomic.Uint64
	targetFlooded atomic.Uint64
	// targetForwards counts received directed publishes this node passed on to
	// another node (the presence owner, or the epidemic when the owner is unknown).
	targetForwards atomic.Uint64
	// eventWrites counts the peer targets a locally produced event was addressed
	// to. It is what makes the fan-out of the origin observable.
	eventWrites atomic.Uint64

	aloneLogged bool
	updateCh    chan struct{}
	dialCh      chan gossipDialRequest

	// nowFn is the clock the membership timers compare against. It is
	// time.Now in production and replaceable in the package's tests (before
	// Start) so a test can advance a suspicion without sleeping.
	nowFn func() time.Time

	wg sync.WaitGroup
}

// gossipMember is one node in the view: the public Member plus the SWIM state and
// the routing metadata the node advertised.
type gossipMember struct {
	Member
	incNumber uint64
	// changedAt is when this record last changed; a recent change is gossiped in
	// the next frames (the delta), an older one rides the rotating anti-entropy
	// slice.
	changedAt time.Time
	suspectAt time.Time
	deadAt    time.Time

	summary      SubscriptionSummary
	summaryAt    time.Time
	summaryKnown bool
}

// gossipPresence is one client-id claim: which node holds the client id, on which
// process incarnation, at which Lamport version. An empty nodeID is a tombstone
// ("no node holds it") that suppresses older claims until it expires.
type gossipPresence struct {
	clientID    string
	nodeID      string
	incarnation string
	version     uint64
	expiresAt   time.Time
	changedAt   time.Time
	// local marks a claim this node produced: it is refreshed while the client is
	// connected here, and a tombstone is left to expire.
	local bool
}

type gossipProbe struct {
	target   string
	nonce    uint64
	started  time.Time
	deadline time.Time
	indirect bool
}

type gossipIndirect struct {
	requester    string
	target       string
	requestNonce uint64
	deadline     time.Time
}

type gossipDialRequest struct {
	addr   string
	expect string
}

// setNamespace implements namespaceScoped: the broker's Config.Namespace is
// authoritative for a backend attached to a broker. It must run before Start.
func (c *gossipCluster) setNamespace(namespace string) { c.namespace = namespace }

// Start implements Cluster. It listens on the configured address, starts the
// probe, gossip and dial workers and seeds the member view by dialing Seeds.
//
// Duplicate node ids: this backend has no shared store to arbitrate a node id
// before Start, so Start cannot return ErrNodeIDInUse. A peer that announces this
// node's own id (or this node's listen address) is refused at the link, with a
// warning; the id stays with the process that already serves it. Two processes
// that both start with the same id therefore never merge, and neither applies the
// other's events.
func (c *gossipCluster) Start(ctx context.Context, nodeID string, handler ClusterHandler) error {
	if err := c.validate(nodeID); err != nil {
		return err
	}
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("gmqx: gossip cluster already started")
	}
	c.started = true
	c.mu.Unlock()
	c.log = c.cfg.Logger
	if c.log == nil {
		c.log = slog.Default()
	}
	c.nodeID = nodeID
	c.handler = handler
	c.incarnation = randomIncarnation()
	c.incNumber = 1
	c.since = c.now()
	c.dedup = newDedupTable(c.cfg.DedupSize, c.cfg.DedupTTL)
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.refreshSummary()

	c.mu.Lock()
	c.running = true
	c.mu.Unlock()

	srv, err := gnety.New(gnety.Config{
		Network:      "tcp",
		Addr:         c.cfg.Addr,
		Codec:        gnety.LengthFieldCodec{},
		Handler:      gnety.HandlerFuncs{Open: c.onLinkOpen, Message: c.onLinkMessage, Close: c.onLinkClose},
		WriteTimeout: clusterWriteTimeout,
		MaxSessions:  maxGossipSessions,
	})
	if err != nil {
		c.cancel()
		c.mu.Lock()
		c.running, c.started = false, false
		c.mu.Unlock()
		return err
	}
	if err := srv.Start(c.ctx); err != nil {
		c.cancel()
		c.mu.Lock()
		c.running, c.started = false, false
		c.mu.Unlock()
		return fmt.Errorf("gmqx: gossip cluster listen %s: %w", c.cfg.Addr, err)
	}
	c.server = srv

	c.wg.Add(2)
	go c.housekeeping()
	go c.dialWorker()

	seeds := c.seedList()
	for _, seed := range seeds {
		c.requestDial(seed, "")
	}
	c.log.Info("gmqx gossip cluster started", "node", nodeID, "addr", c.cfg.Addr,
		"incarnation", c.incarnation, "seeds", seeds, "alone", len(seeds) == 0)
	return nil
}

// Stop implements Cluster. It is idempotent, closes every link and waits for the
// workers.
func (c *gossipCluster) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = false
	cancel := c.cancel
	links := make([]*gossipLink, 0, len(c.links))
	for l := range c.links {
		links = append(links, l)
	}
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, l := range links {
		c.closeLink(l, "cluster stopped", false)
	}
	var err error
	if c.server != nil {
		err = c.server.Stop(ctx)
	}
	c.wg.Wait()
	return err
}

// Members implements MemberView. The view contains one entry per member this node
// knows, sorted by node id and without this node itself. A gossip member carries
// its incarnation and its state (alive, suspect or dead); a dead member stays in
// the view until DeadTimeout elapses, because the dead record has to outlive the
// suspicion that produced it, and then it is removed and a restarted process can
// take the node id again.
//
// A member is reported without a live link too: a member this node has heard of
// but not yet dialed is still a member, and dialing is what the probe loop is for.
func (c *gossipCluster) Members() []Member {
	c.mu.Lock()
	out := make([]Member, 0, len(c.members))
	for _, m := range c.members {
		out = append(out, m.Member)
	}
	c.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// Spread implements Cluster.
//
// Only locally produced events leave a node. The cluster stamps Origin, NodeID
// and Seq on them, refuses to forward an event that already carries another
// node's origin, and drops a received event whose origin is this node.
//
// Dissemination is epidemic: this node writes the event to at most Fanout peers,
// and every receiver forwards it to Fanout peers it did not receive it from. The
// existing (Origin, Seq) dedup table is what stops the epidemic — a node applies
// (and forwards) one event at most once — so the cost is O(N·Fanout) frames per
// event instead of one frame per pair of nodes. With N <= Fanout the origin
// writes every peer and the receivers have nothing left to forward, which turns
// the epidemic into a full broadcast; that is the intended behaviour for the 2-
// and 3-node clusters this backend is aimed at.
//
// The fan-out is not blind for ordinary publishes. A peer that advertised a
// matching subscription summary is always preferred; a peer whose summary does not
// match is skipped; a peer whose summary is unknown or stale is included so the
// event can still reach it through the epidemic. Retained publishes and kicks are
// disseminated to every member (through the same bounded fan-out, then by
// forwarding) for the reasons the TCP backend documents: retained state must exist
// on every node for its future subscribers, and a kick names a client id rather
// than a topic, so only the node holding that session can act on it.
//
// A directed publish (ClusterTarget) is routed through the gossip presence view
// when it knows the owner: it is written to the one node that holds the client id
// and not applied anywhere else. A missing or expired claim — the owner never
// registered, or its claim died with it — falls back to the epidemic, so a client
// that reconnected to a node whose claim has not arrived yet is still found.
func (c *gossipCluster) Spread(_ context.Context, msg ClusterMessage) error {
	if !c.isRunning() {
		return errors.New("gmqx: cluster is not running")
	}
	if msg.Type != ClusterPublish && msg.Type != ClusterKick && msg.Type != ClusterTarget {
		return fmt.Errorf("gmqx: unknown cluster event type %q", msg.Type)
	}
	if msg.Origin != "" && msg.Origin != c.nodeID {
		// Received events are never forwarded.
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

	now := c.now()
	targets, mode := c.spreadTargets(&msg, now)
	switch mode {
	case "routed":
		c.targetRouted.Add(1)
	case "flooded":
		c.targetFlooded.Add(1)
	case "target_unknown":
		c.recordDrop("target_unknown", "presence names this node for a client id it does not hold")
		return nil
	}
	if len(targets) == 0 {
		return nil
	}
	data, err := c.eventFrame(&msg)
	if err != nil {
		return err
	}
	for _, nodeID := range targets {
		c.eventWrites.Add(1)
		c.sendDataTo(nodeID, data)
	}
	return nil
}

// spreadTargets decides which nodes one locally produced event is written to and
// why. It is the only place the presence, summary and fan-out rules interact;
// forwardEvent uses the same helpers for a received event.
func (c *gossipCluster) spreadTargets(msg *ClusterMessage, now time.Time) ([]string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch msg.Type {
	case ClusterTarget:
		owner := c.presenceOwnerLocked(msg.ClientID, now)
		switch {
		case owner == c.nodeID:
			// The broker delivered locally before spreading; the claim is stale.
			return nil, "target_unknown"
		case owner != "":
			if m := c.members[owner]; m != nil && m.State != MemberDead && c.linksByNode[owner] != nil {
				return []string{owner}, "routed"
			}
			// The owner left, died or is not connected yet: forward
			// epidemically so a node that can reach it still delivers.
		}
		return c.floodTargetsLocked("", now), "flooded"
	case ClusterKick:
		return c.floodTargetsLocked("", now), ""
	case ClusterPublish:
		if msg.Retain {
			return c.floodTargetsLocked("", now), ""
		}
		return c.routedTargetsLocked(msg, "", now), ""
	}
	return nil, ""
}

// routedTargetsLocked picks the peers an ordinary publish is written to: the
// peers elected for a shared group first, then the peers whose summary matches the
// topic, then the peers whose summary is unknown or stale. This is the directed
// half of routing on top of the epidemic; the cap is the fan-out, and the peers
// beyond the cap are reached by forwarding from the peers that did receive it.
func (c *gossipCluster) routedTargetsLocked(msg *ClusterMessage, from string, now time.Time) []string {
	alive := c.targetableLocked(from, msg.Origin)
	if len(alive) == 0 {
		return nil
	}
	limit := c.fanoutLocked(len(alive))
	elected := make(map[string]struct{}, len(msg.Shared))
	for _, d := range msg.Shared {
		elected[d.Node] = struct{}{}
	}
	var named, matching, unknown []string
	for _, id := range alive {
		m := c.members[id]
		if _, ok := elected[id]; ok {
			named = append(named, id)
			continue
		}
		if !c.summaryFreshLocked(m, now) {
			unknown = append(unknown, id)
			continue
		}
		if summaryMatches(m.summary.Filters, msg.Topic) {
			matching = append(matching, id)
		}
	}
	out := make([]string, 0, limit)
	out = append(out, named...)
	for _, group := range [][]string{matching, unknown} {
		if len(out) >= limit {
			break
		}
		out = append(out, rotateStrings(group, randIntN(len(group)))...)
		if len(out) > limit {
			out = out[:limit]
		}
	}
	return out
}

// floodTargetsLocked picks up to Fanout peers, excluding the node the event came
// from. It is used for retained publishes, kicks and a directed publish whose
// owner is unknown: all three must eventually reach every member, and the
// epidemic is what does it without an N² link set.
func (c *gossipCluster) floodTargetsLocked(from string, now time.Time) []string {
	return c.floodTargetsExcludingLocked(from, "", now)
}

// floodTargetsExcludingLocked is floodTargetsLocked for a forwarded event, which
// must not consume a fan-out slot on the node that produced the event: the origin
// drops its own event, so writing it back is a wasted frame.
func (c *gossipCluster) floodTargetsExcludingLocked(from, origin string, now time.Time) []string {
	alive := c.targetableLocked(from, origin)
	if len(alive) == 0 {
		return nil
	}
	limit := c.fanoutLocked(len(alive))
	return rotateStrings(alive, randIntN(len(alive)))[:limit]
}

// targetableLocked returns the sorted node ids a locally produced or forwarded
// event may be written to: every member that is not this node, not dead and not
// the node it came from.
func (c *gossipCluster) targetableLocked(from, origin string) []string {
	out := make([]string, 0, len(c.members))
	for id, m := range c.members {
		if id == "" || id == c.nodeID || id == from || id == origin {
			continue
		}
		if m.State == MemberDead {
			continue
		}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// handleEvent applies one received event and forwards it onward. It is the
// forwarding half of the epidemic: the (Origin, Seq) dedup table is what makes
// forwarding terminate, because a node forwards an event at most once.
func (c *gossipCluster) handleEvent(l *gossipLink, msg ClusterMessage) {
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
	from := l.nodeID
	if msg.Type == ClusterTarget {
		c.handleTargetEvent(msg, from)
		return
	}
	c.deliverEvent(msg)
	c.forwardEvent(&msg, from)
}

// handleTargetEvent applies a received directed publish only where it belongs: on
// the node the presence view names, or — when no claim exists — locally (the
// broker drops it and counts target_unknown when it holds no such session) and
// onward through the epidemic. A node that knows the owner but is not it forwards
// the event to the owner alone, which is what keeps a presence-routed directed
// publish from being applied twice.
func (c *gossipCluster) handleTargetEvent(msg ClusterMessage, from string) {
	now := c.now()
	c.mu.Lock()
	owner := c.presenceOwnerLocked(msg.ClientID, now)
	c.mu.Unlock()
	switch {
	case owner == c.nodeID:
		c.deliverEvent(msg)
	case owner != "":
		if owner != from {
			c.targetForwards.Add(1)
			c.forwardEventTo(&msg, []string{owner})
		}
	default:
		c.deliverEvent(msg)
		c.forwardEvent(&msg, from)
	}
}

// deliverEvent hands one event to the broker.
func (c *gossipCluster) deliverEvent(msg ClusterMessage) {
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

// forwardEvent forwards a received event to the peers the dissemination rules
// select for it.
func (c *gossipCluster) forwardEvent(msg *ClusterMessage, from string) {
	now := c.now()
	c.mu.Lock()
	var targets []string
	switch {
	case msg.Type == ClusterKick || msg.Retain:
		targets = c.floodTargetsExcludingLocked(from, msg.Origin, now)
	case msg.Type == ClusterTarget:
		// A target event only reaches this path when no presence claim exists:
		// the epidemic is the fallback that finds a node which holds the id but
		// whose claim has not arrived yet.
		targets = c.floodTargetsExcludingLocked(from, msg.Origin, now)
	default:
		targets = c.routedTargetsLocked(msg, from, now)
	}
	c.mu.Unlock()
	if len(targets) == 0 {
		return
	}
	c.forwards.Add(1)
	c.forwardEventTo(msg, targets)
}

// forwardEventTo writes an event frame to the given nodes.
func (c *gossipCluster) forwardEventTo(msg *ClusterMessage, targets []string) {
	data, err := c.eventFrame(msg)
	if err != nil {
		return
	}
	for _, nodeID := range targets {
		c.sendDataTo(nodeID, data)
	}
}

// eventFrame builds the lean frame an event travels in. Events do not carry
// membership metadata: pings and acks do, and the event path is the hot one.
func (c *gossipCluster) eventFrame(msg *ClusterMessage) ([]byte, error) {
	return json.Marshal(gossipFrame{
		Kind:        gossipFrameEvent,
		Version:     gossipWireVersion,
		NodeID:      c.nodeID,
		Incarnation: c.incarnation,
		Addr:        c.cfg.Addr,
		Event:       msg,
	})
}

// refreshSummary rebuilds the advertised summary from the handler's local
// subscriptions. It is the same summarySet the TCP and Redis backends use, so the
// oldest-first eviction and the shared group set behave identically.
func (c *gossipCluster) refreshSummary() {
	provider, ok := c.handler.(subscriptionProvider)
	if !ok {
		c.mu.Lock()
		c.summaryKnown = false
		c.summary = SubscriptionSummary{}
		c.summaryAt = c.now()
		c.mu.Unlock()
		return
	}
	current := provider.subscriptionFilters()
	var groups []string
	if sp, ok := c.handler.(sharedSubscriptionProvider); ok {
		groups = sp.sharedSubscriptionGroups()
	}
	now := c.now()
	c.mu.Lock()
	filters := c.local.refresh(current, c.maxSummaryFilters())
	shared := c.sharedLocal.refresh(groups, c.maxSummaryFilters())
	c.summary = SubscriptionSummary{
		Filters:   filters,
		Shared:    shared,
		Namespace: c.namespace,
		Updated:   now.UnixNano(),
	}
	c.summaryKnown = true
	c.summaryAt = now
	c.mu.Unlock()
}

func (c *gossipCluster) refreshSummaryIfStale(now time.Time) {
	c.mu.Lock()
	stale := !c.summaryKnown || now.Sub(c.summaryAt) >= c.summaryInterval()
	c.mu.Unlock()
	if stale {
		c.refreshSummary()
	}
}

// summaryLocked returns the summary to attach to a metadata frame, or nil when
// this node has no description of its subscriptions (peers then treat its summary
// as unknown and fall back to the epidemic).
func (c *gossipCluster) summaryLocked() *SubscriptionSummary {
	if !c.summaryKnown {
		return nil
	}
	s := c.summary
	s.Filters = append([]string(nil), s.Filters...)
	s.Shared = append([]string(nil), s.Shared...)
	return &s
}

// summaryFreshLocked reports whether a peer's advertised summary is still usable
// for routing.
func (c *gossipCluster) summaryFreshLocked(m *gossipMember, now time.Time) bool {
	if m == nil || !m.summaryKnown {
		return false
	}
	return now.Sub(m.summaryAt) <= gossipSummaryStaleFactor*c.summaryInterval()
}

// handleSummary records the summary a peer advertised in a metadata frame. A
// summary of another namespace is ignored: routing only trusts filters that belong
// to the same scope, so the peer is treated as unknown and still receives the
// epidemic's copy, which it drops on the receive path.
func (c *gossipCluster) handleSummary(f gossipFrame, now time.Time) {
	if f.Summary == nil {
		return
	}
	if f.Summary.Namespace != c.namespace {
		c.recordDrop("cluster_namespace", "subscription summary for another namespace")
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[f.NodeID]
	if m == nil {
		return
	}
	m.summary = SubscriptionSummary{
		Filters:   capFilters(f.Summary.Filters, c.maxSummaryFilters()),
		Shared:    capFilters(f.Summary.Shared, c.maxSummaryFilters()),
		Namespace: f.Summary.Namespace,
		Updated:   f.Summary.Updated,
	}
	m.summaryKnown = true
	m.summaryAt = now
}

// subscriptionsChanged implements subscriptionChangeNotifier. The broker calls it
// after a SUBSCRIBE/UNSUBSCRIBE and after a session with subscriptions goes away,
// so the new summary is pushed as an update frame instead of waiting for the next
// ping.
func (c *gossipCluster) subscriptionsChanged() {
	if !c.isRunning() {
		return
	}
	c.refreshSummary()
	c.signalUpdate()
}

// sharedGroups implements sharedDirectory: the shared subscription group keys each
// peer advertised, per node id. A peer whose summary is unknown contributes no
// groups, so a group it holds is not elected to it until the summary arrives; the
// broker pushes a summary on every subscription change, so that window is one
// update frame. Only peers with a live link are candidates: a node this node cannot
// reach cannot deliver the shared copy.
func (c *gossipCluster) sharedGroups() map[string][]string {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.members) == 0 {
		return nil
	}
	out := make(map[string][]string, 4)
	for id, m := range c.members {
		if id == "" || id == c.nodeID || m.State == MemberDead {
			continue
		}
		if c.linksByNode[id] == nil || !c.summaryFreshLocked(m, now) {
			continue
		}
		for _, group := range m.summary.Shared {
			out[group] = append(out[group], id)
		}
	}
	return out
}

// clientConnected implements clientPresence: the client id is held by this node,
// and a claim is gossiped so a directed publish from another node can address this
// node directly. The call only takes the local lock and signals the housekeeping
// loop; it never performs a network write on the CONNECT path.
func (c *gossipCluster) clientConnected(clientID string) {
	if clientID == "" || !c.isRunning() {
		return
	}
	c.mu.Lock()
	c.claimPresenceLocked(clientID, c.nodeID)
	c.mu.Unlock()
	c.signalUpdate()
}

// clientDisconnected implements clientPresence: the client id left this node, so a
// tombstone with a newer version is gossiped. A claim of another node with a
// higher version still wins, which is what keeps a client that reconnected
// elsewhere from being tombstoned by its old node.
func (c *gossipCluster) clientDisconnected(clientID string) {
	if clientID == "" || !c.isRunning() {
		return
	}
	c.mu.Lock()
	p := c.presence[clientID]
	// Only a claim this process made may be tombstoned. The broker also tells a
	// node to forget a client id it never held - a kick for a session that moved
	// to another node - and tombstoning that would delete the new owner's claim.
	if p != nil && p.nodeID == c.nodeID && p.incarnation == c.incarnation {
		c.claimPresenceLocked(clientID, "")
	}
	c.mu.Unlock()
	c.signalUpdate()
}

// claimPresenceLocked writes a local presence claim with the next Lamport
// version. An empty node id is a tombstone.
func (c *gossipCluster) claimPresenceLocked(clientID, nodeID string) {
	now := c.now()
	p := c.presence[clientID]
	version := c.presenceSeq + 1
	if p != nil && p.version >= version {
		version = p.version + 1
	}
	c.presenceSeq = version
	if p == nil {
		if len(c.presence) >= c.maxPresenceEntries() {
			if !c.evictPresenceLocked(now) {
				c.log.Debug("gmqx gossip presence table full", "client", clientID)
				return
			}
		}
		p = &gossipPresence{clientID: clientID}
		c.presence[clientID] = p
	}
	p.nodeID = nodeID
	p.incarnation = c.incarnation
	p.version = version
	p.expiresAt = now.Add(c.presenceTTL())
	p.changedAt = now
	p.local = nodeID != ""
}

// presenceOwnerLocked returns the node id that currently claims clientID, or ""
// when no live claim exists. A claim is invalid when it expired, when its owner is
// dead or gone, or when it names an incarnation the owner no longer runs: a
// restarted process does not inherit the client ids of its predecessor.
func (c *gossipCluster) presenceOwnerLocked(clientID string, now time.Time) string {
	if clientID == "" {
		return ""
	}
	p := c.presence[clientID]
	if p == nil || p.nodeID == "" || now.After(p.expiresAt) {
		return ""
	}
	// This node is not in its own member table: a local claim is valid while the
	// process incarnation it names is still this one.
	if p.nodeID == c.nodeID {
		if p.incarnation != "" && p.incarnation != c.incarnation {
			return ""
		}
		return c.nodeID
	}
	m := c.members[p.nodeID]
	if m == nil || m.State == MemberDead {
		return ""
	}
	if p.incarnation != "" && p.incarnation != m.Incarnation {
		return ""
	}
	return p.nodeID
}

// presenceOwner is the locking form of presenceOwnerLocked (used by tests and by
// the receive path).
func (c *gossipCluster) presenceOwner(clientID string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.presenceOwnerLocked(clientID, c.now())
}

// mergePresence folds the presence claims one frame carried into the local view.
// A claim with a higher Lamport version wins; at the same version a live claim
// beats a tombstone and the lexicographically greater node id wins, so two nodes
// that claim one client id concurrently converge on the same owner without
// coordinating. Expiry is dated at arrival, never by the sender's clock.
func (c *gossipCluster) mergePresence(claims []gossipPresenceWire) {
	if len(claims) == 0 {
		return
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range claims {
		if w.ClientID == "" {
			continue
		}
		if w.Namespace != c.namespace {
			c.recordDrop("cluster_namespace", "presence claim for another namespace")
			continue
		}
		if w.NodeID == c.nodeID && w.Incarnation != c.incarnation {
			c.log.Warn("gmqx gossip cluster duplicate node id", "id", w.NodeID, "incarnation", w.Incarnation)
			continue
		}
		p := c.presence[w.ClientID]
		if p != nil {
			switch {
			case w.Version < p.version:
				continue
			case w.Version == p.version:
				if w.NodeID == p.nodeID && w.Incarnation == p.incarnation {
					// A refresh of the same claim: extend its TTL.
					p.expiresAt = now.Add(c.presenceTTL())
					continue
				}
				if presenceWinsLocked(w, p) == false {
					continue
				}
			}
		} else if len(c.presence) >= c.maxPresenceEntries() && !c.evictPresenceLocked(now) {
			c.log.Debug("gmqx gossip presence table full", "client", w.ClientID)
			continue
		}
		if w.Version > c.presenceSeq {
			c.presenceSeq = w.Version
		}
		if p == nil {
			p = &gossipPresence{clientID: w.ClientID}
			c.presence[w.ClientID] = p
		}
		changed := p.nodeID != w.NodeID || p.incarnation != w.Incarnation
		p.nodeID = w.NodeID
		p.incarnation = w.Incarnation
		p.version = w.Version
		p.expiresAt = now.Add(c.presenceTTL())
		p.local = false
		if changed {
			p.changedAt = now
		}
	}
}

// presenceWinsLocked reports whether an incoming claim beats the stored one at the
// same Lamport version: a live claim beats a tombstone, otherwise the greater node
// id wins.
func presenceWinsLocked(w gossipPresenceWire, p *gossipPresence) bool {
	if p.nodeID == w.NodeID {
		return false
	}
	if p.nodeID == "" {
		return true
	}
	if w.NodeID == "" {
		return false
	}
	return w.NodeID > p.nodeID
}

// purgePresenceOwnerLocked forgets the presence claims of a node that left or was
// declared dead. Directed publishes for those client ids then fall back to the
// epidemic instead of pointing at a node that cannot deliver them.
func (c *gossipCluster) purgePresenceOwnerLocked(nodeID string) {
	for id, p := range c.presence {
		if p.nodeID == nodeID {
			delete(c.presence, id)
		}
	}
}

// purgePresenceIncarnationLocked forgets the claims of one process incarnation: a
// restarted node does not inherit the client ids of its predecessor.
func (c *gossipCluster) purgePresenceIncarnationLocked(nodeID, incarnation string) {
	for id, p := range c.presence {
		if p.nodeID == nodeID && p.incarnation == incarnation {
			delete(c.presence, id)
		}
	}
}

// evictPresenceLocked makes room in a full presence table: expired entries first,
// then the oldest non-local claim. It reports whether room was made.
func (c *gossipCluster) evictPresenceLocked(now time.Time) bool {
	for id, p := range c.presence {
		if !now.Before(p.expiresAt) {
			delete(c.presence, id)
			return true
		}
	}
	var oldest *gossipPresence
	for _, p := range c.presence {
		if p.local {
			continue
		}
		if oldest == nil || p.changedAt.Before(oldest.changedAt) {
			oldest = p
		}
	}
	if oldest == nil {
		return false
	}
	delete(c.presence, oldest.clientID)
	return true
}

// sweepPresenceLocked drops expired claims. A connected client's claim is
// refreshed by tickingPresence, so an expiry means its owner stopped refreshing it.
func (c *gossipCluster) sweepPresenceLocked(now time.Time) {
	for id, p := range c.presence {
		if !now.Before(p.expiresAt) {
			delete(c.presence, id)
		}
	}
}

// refreshPresenceLocked extends the TTL of the claims this node owns. A tombstone
// is not refreshed: it exists to outlive older claims and is then swept.
func (c *gossipCluster) refreshPresenceLocked(now time.Time) {
	ttl := c.presenceTTL()
	for _, p := range c.presence {
		if !p.local || p.nodeID == "" {
			continue
		}
		if p.expiresAt.Sub(now) < ttl/2 {
			p.expiresAt = now.Add(ttl)
		}
	}
}

// presenceDeltaLocked picks the claims one metadata frame carries: recently
// changed claims first (the delta), then a bounded slice of the rest so a peer that
// missed a change still converges.
func (c *gossipCluster) presenceDeltaLocked(now time.Time, limit int) []gossipPresenceWire {
	if limit <= 0 || len(c.presence) == 0 {
		return nil
	}
	out := make([]gossipPresenceWire, 0, limit)
	seen := make(map[string]struct{}, limit)
	hot := 2 * c.pingInterval()
	for id, p := range c.presence {
		if len(out) >= limit {
			break
		}
		if now.After(p.expiresAt) || now.Sub(p.changedAt) >= hot {
			continue
		}
		out = append(out, p.wireLocked(c.namespace))
		seen[id] = struct{}{}
	}
	for id, p := range c.presence {
		if len(out) >= limit {
			break
		}
		if _, ok := seen[id]; ok || now.After(p.expiresAt) {
			continue
		}
		out = append(out, p.wireLocked(c.namespace))
	}
	return out
}

func (p *gossipPresence) wireLocked(namespace string) gossipPresenceWire {
	return gossipPresenceWire{
		ClientID:    p.clientID,
		NodeID:      p.nodeID,
		Incarnation: p.incarnation,
		Version:     p.version,
		Namespace:   namespace,
	}
}

// signalUpdate wakes the housekeeping loop so a summary or presence change is
// pushed to every link now instead of riding the next ping. It never blocks: a
// change that arrives while an update is queued rides along with it.
func (c *gossipCluster) signalUpdate() {
	select {
	case c.updateCh <- struct{}{}:
	default:
	}
}

// broadcastUpdate sends a metadata frame to every handshaked link. It is the
// "push" half of the gossip: pings carry the same metadata to one random member
// each interval, and this carries a change to everyone immediately.
func (c *gossipCluster) broadcastUpdate() {
	if !c.isRunning() {
		return
	}
	f := c.metadataFrame(gossipFrameUpdate, 0, "")
	data, err := json.Marshal(f)
	if err != nil {
		return
	}
	for _, l := range c.snapshotLinks(true) {
		c.enqueue(l, data)
	}
}

// recordDrop counts a dropped event and reports it when a metrics sink is
// configured.
func (c *gossipCluster) recordDrop(reason, detail string) {
	c.drops.Add(1)
	if c.cfg.Metrics != nil {
		c.cfg.Metrics.RecordDroppedMessage(reason)
	}
	if c.log != nil {
		c.log.Debug("gmqx gossip cluster event dropped", "reason", reason, "detail", detail)
	}
}

func (c *gossipCluster) isRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

// now is the backend's clock. Tests replace nowFn before Start to drive the
// membership timers deterministically.
func (c *gossipCluster) now() time.Time {
	if c.nowFn != nil {
		return c.nowFn()
	}
	return time.Now()
}

// housekeeping drives probes, timers and pushed updates. A summary is pushed to
// every link once per SummaryInterval (the same cadence the TCP backend re-advertises
// on) so a peer's summary does not depend on being the random probe target of this
// interval; an update is pushed immediately when local subscriptions or presence
// change.
func (c *gossipCluster) housekeeping() {
	defer c.wg.Done()
	ping := time.NewTicker(c.pingInterval())
	defer ping.Stop()
	summary := time.NewTicker(c.summaryInterval())
	defer summary.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ping.C:
			c.tick()
		case <-summary.C:
			c.refreshSummaryIfStale(c.now())
			c.broadcastUpdate()
		case <-c.updateCh:
			c.broadcastUpdate()
		}
	}
}

// tick runs one period of the membership protocol: it expires probes, resolves
// suspects and dead members, refreshes the summary and presence TTLs, dials the
// members and seeds without a link and probes one random member.
//
// It is a method with no arguments so a test can call it directly with a
// replacement clock; the production loop calls it from the PingInterval ticker.
func (c *gossipCluster) tick() {
	if !c.isRunning() {
		return
	}
	now := c.now()
	c.refreshSummaryIfStale(now)
	c.expireProbes(now)
	c.sweepMembers(now)
	c.mu.Lock()
	c.refreshPresenceLocked(now)
	c.mu.Unlock()
	c.dialMissing()
	if target := c.probeTarget(); target != "" {
		c.startProbe(target)
	}
	c.noteAlone()
}

// noteAlone logs once that this node has no peers, which is the expected state of
// a single-node cluster and the first thing an operator needs to know when a
// seeded node cannot reach its seeds.
func (c *gossipCluster) noteAlone() {
	if len(c.seedList()) > 0 {
		return
	}
	c.mu.Lock()
	if c.aloneLogged || len(c.members) > 0 {
		c.mu.Unlock()
		return
	}
	c.aloneLogged = true
	c.mu.Unlock()
	c.log.Info("gmqx gossip cluster is alone: no seeds and no peers", "node", c.nodeID)
}

func (c *gossipCluster) pingInterval() time.Duration {
	if c.cfg.PingInterval > 0 {
		return c.cfg.PingInterval
	}
	return defaultPingInterval
}

func (c *gossipCluster) pingTimeout() time.Duration {
	if c.cfg.PingTimeout > 0 {
		return c.cfg.PingTimeout
	}
	return defaultPingTimeout
}

func (c *gossipCluster) suspicionMultiplier() int {
	if c.cfg.SuspicionMultiplier > 0 {
		return c.cfg.SuspicionMultiplier
	}
	return defaultSuspicionMultiplier
}

// suspectTimeout is how long a member may stay suspect before it is declared dead.
func (c *gossipCluster) suspectTimeout() time.Duration {
	return time.Duration(c.suspicionMultiplier()) * c.pingInterval()
}

func (c *gossipCluster) deadTimeout() time.Duration {
	if c.cfg.DeadTimeout > 0 {
		return c.cfg.DeadTimeout
	}
	return 3 * c.suspectTimeout()
}

func (c *gossipCluster) reconnectInterval() time.Duration {
	if c.cfg.ReconnectInterval > 0 {
		return c.cfg.ReconnectInterval
	}
	return defaultReconnectGossip
}

func (c *gossipCluster) dialTimeout() time.Duration {
	if c.cfg.DialTimeout > 0 {
		return c.cfg.DialTimeout
	}
	return defaultGossipDialTimeout
}

func (c *gossipCluster) summaryInterval() time.Duration {
	if c.cfg.SummaryInterval > 0 {
		return c.cfg.SummaryInterval
	}
	return defaultSummaryInterval
}

func (c *gossipCluster) maxSummaryFilters() int {
	if c.cfg.MaxSummaryFilters > 0 {
		return c.cfg.MaxSummaryFilters
	}
	return defaultMaxSummaryFilters
}

func (c *gossipCluster) maxGossipDelta() int {
	if c.cfg.MaxGossipDelta > 0 {
		return c.cfg.MaxGossipDelta
	}
	return defaultMaxGossipDelta
}

func (c *gossipCluster) maxPresenceDelta() int {
	if c.cfg.MaxPresenceDelta > 0 {
		return c.cfg.MaxPresenceDelta
	}
	return defaultMaxPresenceDelta
}

func (c *gossipCluster) maxPresenceEntries() int {
	if c.cfg.MaxPresenceEntries > 0 {
		return c.cfg.MaxPresenceEntries
	}
	return defaultMaxPresenceEntries
}

func (c *gossipCluster) presenceTTL() time.Duration {
	if c.cfg.PresenceTTL > 0 {
		return c.cfg.PresenceTTL
	}
	return defaultPresenceTTL
}

// fanoutLocked is how many peers one event is written to. The default tracks the
// live member count so a small cluster broadcasts and a large one gossip-fans;
// with members <= fanout the epidemic has no forwarding left to do.
func (c *gossipCluster) fanoutLocked(members int) int {
	fanout := c.cfg.Fanout
	if fanout <= 0 {
		fanout = defaultGossipFanout
	}
	if fanout > members {
		fanout = members
	}
	return fanout
}

// seedList is the configured seed list, trimmed and without this node's own
// address.
func (c *gossipCluster) seedList() []string {
	out := make([]string, 0, len(c.cfg.Seeds))
	seen := make(map[string]struct{}, len(c.cfg.Seeds))
	for _, seed := range c.cfg.Seeds {
		seed = strings.TrimSpace(seed)
		if seed == "" || seed == c.cfg.Addr {
			continue
		}
		if _, ok := seen[seed]; ok {
			continue
		}
		seen[seed] = struct{}{}
		out = append(out, seed)
	}
	return out
}

// validate checks the configuration before a listener is opened. Zero selects a
// documented default; a negative value is a caller mistake and is refused instead
// of being silently clamped.
func (c *gossipCluster) validate(nodeID string) error {
	switch {
	case nodeID == "":
		return errors.New("gmqx: cluster node id is required")
	case strings.TrimSpace(c.cfg.Addr) == "":
		return errors.New("gmqx: gossip cluster Addr is required")
	case strings.HasSuffix(c.cfg.Addr, ":0"):
		return fmt.Errorf("gmqx: gossip cluster Addr must be explicit, got %q", c.cfg.Addr)
	case c.cfg.PingInterval < 0:
		return fmt.Errorf("gmqx: gossip PingInterval must not be negative, got %v", c.cfg.PingInterval)
	case c.cfg.PingTimeout < 0:
		return fmt.Errorf("gmqx: gossip PingTimeout must not be negative, got %v", c.cfg.PingTimeout)
	case c.cfg.SuspicionMultiplier < 0:
		return fmt.Errorf("gmqx: gossip SuspicionMultiplier must not be negative, got %d", c.cfg.SuspicionMultiplier)
	case c.cfg.DeadTimeout < 0:
		return fmt.Errorf("gmqx: gossip DeadTimeout must not be negative, got %v", c.cfg.DeadTimeout)
	case c.cfg.Fanout < 0:
		return fmt.Errorf("gmqx: gossip Fanout must not be negative, got %d", c.cfg.Fanout)
	case c.cfg.ReconnectInterval < 0:
		return fmt.Errorf("gmqx: gossip ReconnectInterval must not be negative, got %v", c.cfg.ReconnectInterval)
	case c.cfg.DialTimeout < 0:
		return fmt.Errorf("gmqx: gossip DialTimeout must not be negative, got %v", c.cfg.DialTimeout)
	case c.cfg.DedupSize < 0:
		return fmt.Errorf("gmqx: gossip DedupSize must not be negative, got %d", c.cfg.DedupSize)
	case c.cfg.DedupTTL < 0:
		return fmt.Errorf("gmqx: gossip DedupTTL must not be negative, got %v", c.cfg.DedupTTL)
	case c.cfg.SummaryInterval < 0:
		return fmt.Errorf("gmqx: gossip SummaryInterval must not be negative, got %v", c.cfg.SummaryInterval)
	case c.cfg.MaxSummaryFilters < 0:
		return fmt.Errorf("gmqx: gossip MaxSummaryFilters must not be negative, got %d", c.cfg.MaxSummaryFilters)
	case c.cfg.PresenceTTL < 0:
		return fmt.Errorf("gmqx: gossip PresenceTTL must not be negative, got %v", c.cfg.PresenceTTL)
	case c.cfg.PresenceTTL > 0 && c.cfg.PresenceTTL < c.pingInterval():
		return fmt.Errorf("gmqx: gossip PresenceTTL %v must be at least PingInterval %v",
			c.cfg.PresenceTTL, c.pingInterval())
	case c.cfg.MaxGossipDelta < 0:
		return fmt.Errorf("gmqx: gossip MaxGossipDelta must not be negative, got %d", c.cfg.MaxGossipDelta)
	case c.cfg.MaxPresenceDelta < 0:
		return fmt.Errorf("gmqx: gossip MaxPresenceDelta must not be negative, got %d", c.cfg.MaxPresenceDelta)
	case c.cfg.MaxPresenceEntries < 0:
		return fmt.Errorf("gmqx: gossip MaxPresenceEntries must not be negative, got %d", c.cfg.MaxPresenceEntries)
	}
	return nil
}

// randIntN returns a value in [0,n); n<=0 yields 0 so callers can rotate by a
// constant-safe offset.
func randIntN(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.IntN(n)
}

// rotateStrings returns a rotated copy of ids, starting at offset.
func rotateStrings(ids []string, offset int) []string {
	if len(ids) == 0 {
		return nil
	}
	offset %= len(ids)
	out := make([]string, 0, len(ids))
	out = append(out, ids[offset:]...)
	out = append(out, ids[:offset]...)
	return out
}

// Test-visible accessors. They exist for the package's own tests, like the TCP
// backend's peerCount and dropCount.

// linkCount reports the number of physical links.
func (c *gossipCluster) linkCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.links)
}

// dropCount reports dropped events.
func (c *gossipCluster) dropCount() uint64 { return c.drops.Load() }

// forwardCount reports how many received events were forwarded.
func (c *gossipCluster) forwardCount() uint64 { return c.forwards.Load() }

// targetForwardCount reports how many directed publishes were passed on.
func (c *gossipCluster) targetForwardCount() uint64 { return c.targetForwards.Load() }

// eventWriteCount reports how many peer targets locally produced events addressed.
func (c *gossipCluster) eventWriteCount() uint64 { return c.eventWrites.Load() }

// targetRoutedCount reports directed publishes routed to a presence owner.
func (c *gossipCluster) targetRoutedCount() uint64 { return c.targetRouted.Load() }

// targetFloodedCount reports directed publishes that fell back to the epidemic.
func (c *gossipCluster) targetFloodedCount() uint64 { return c.targetFlooded.Load() }

// dedupLen reports how many event identities the dedup table remembers.
func (c *gossipCluster) dedupLen() int {
	if c.dedup == nil {
		return 0
	}
	return c.dedup.len()
}

// memberSummary reports a peer's advertised filters and whether they are known.
func (c *gossipCluster) memberSummary(nodeID string) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[nodeID]
	if m == nil || !m.summaryKnown {
		return nil, false
	}
	return append([]string(nil), m.summary.Filters...), true
}

// memberState reports a peer's membership state.
func (c *gossipCluster) memberState(nodeID string) (MemberState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[nodeID]
	if m == nil {
		return "", false
	}
	return m.State, true
}

// memberIncNumber reports a peer's SWIM incarnation number (used by tests).
func (c *gossipCluster) memberIncNumber(nodeID string) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[nodeID]
	if m == nil {
		return 0, false
	}
	return m.incNumber, true
}

// selfIncNumber reports this node's SWIM incarnation number (used by tests).
func (c *gossipCluster) selfIncNumber() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.incNumber
}
