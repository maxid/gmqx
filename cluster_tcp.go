package gmqx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxid/gnety"
)

// clusterWireVersion is the version of the internal cluster frames. It is part
// of the handshake: a peer that speaks another version is refused instead of
// being admitted as a second logical member, because the version decides both
// the framing and the ordering rules that collapse duplicate links.
const clusterWireVersion = 1

// Internal frame kinds. Only the envelope is versioned on the wire; the event
// payload is ClusterMessage.
const (
	frameHello = "hello"
	framePing  = "ping"
	framePong  = "pong"
	frameSubs  = "subs"
	frameEvent = "event"
)

const (
	defaultReconnectInterval   = 500 * time.Millisecond
	defaultHeartbeatInterval   = time.Second
	defaultHeartbeatMissBudget = 3
	defaultDedupSize           = 4096
	defaultDedupTTL            = 30 * time.Second
	defaultSummaryInterval     = time.Second
	defaultMaxSummaryFilters   = 256

	// summaryStaleFactor converts SummaryInterval into the age after which a
	// peer's summary is stale; routing then falls back to sending it every
	// publish instead of trusting a filter set that may be out of date.
	summaryStaleFactor = 3

	// minRejectBackoff is the wait before a dial loop retries a peer that
	// refused its link (duplicate or non-canonical direction). Without it two
	// nodes that both dial would reconnect aggressively.
	minRejectBackoff = time.Second

	// clusterWriteTimeout bounds one cluster frame write, so a stuck peer cannot
	// pin a sender forever.
	clusterWriteTimeout = 2 * time.Second

	// clusterLinkAttr is the session attribute that ties an inbound gnety
	// session to its cluster link.
	clusterLinkAttr = "gmqx.cluster.link"
)

// TCPClusterConfig configures the built-in TCP cluster backend.
//
// Every node listens on Addr and dials seeds that sort after its own advertised
// address, so a symmetric seed list — every node listing every other node with
// the same spelling that node uses in Addr — yields one link per pair. Addr must
// be explicit (no ":0") and must differ from the MQTT listener address.
//
// Zero values select the documented default.
type TCPClusterConfig struct {
	// Addr is the cluster listen address this node advertises. Peers must spell
	// it exactly as it appears in their Seeds, because the link-ordering rule
	// compares advertised addresses.
	Addr string
	// Seeds are the peer cluster addresses to dial.
	Seeds []string
	// ReconnectInterval is how long a dial loop waits after a failed dial or a
	// closed link (default 500ms).
	ReconnectInterval time.Duration

	// HeartbeatInterval is how often a node pings every link (default 1s).
	HeartbeatInterval time.Duration
	// HeartbeatMissBudget is how many heartbeat intervals a link may stay
	// silent before its member is evicted and the link closed (default 3). A
	// link that never completes the handshake within the same window is closed.
	HeartbeatMissBudget int

	// DedupSize is the number of event identities remembered for duplicate
	// suppression (default 4096). The oldest identity is evicted first, so the
	// table is O(DedupSize) no matter how much traffic arrives.
	DedupSize int
	// DedupTTL is how long an event identity suppresses duplicates (default
	// 30s).
	DedupTTL time.Duration

	// SummaryInterval is how often a node re-advertises the topic filters its
	// local sessions are subscribed to (default 1s). A peer that has not
	// refreshed within three intervals is treated as unknown, so publishes are
	// still sent to it.
	SummaryInterval time.Duration
	// MaxSummaryFilters caps how many filters one node advertises (default
	// 256). When a node has more, the filters that have been present longest are
	// left out of the summary, so new subscriptions keep routing; the cost is
	// that a publish matching only an evicted filter is not routed to that node.
	MaxSummaryFilters int

	// Metrics, when set, receives RecordDroppedMessage for events the cluster
	// drops ("cluster_duplicate", "cluster_loop", "cluster_forward",
	// "cluster_namespace"). Without it the drops are only counted internally.
	Metrics MetricsManager
	// Logger receives membership decisions such as link rejection and eviction
	// (default slog.Default()).
	Logger *slog.Logger
	// Namespace scopes the traffic of this backend: an event or subscription
	// summary of another namespace is dropped. When the backend is passed to a
	// Broker, the broker's Config.Namespace is authoritative and overrides this
	// value at Start. The empty string is the default namespace.
	Namespace string
}

// clusterFrame is the internal wire envelope. Events, membership and routing
// metadata share one JSON frame so every message carries the version the
// ordering rules were built for.
type clusterFrame struct {
	Kind        string            `json:"kind"`
	Version     int               `json:"v"`
	NodeID      string            `json:"node,omitempty"`
	Addr        string            `json:"addr,omitempty"`
	Incarnation string            `json:"inc,omitempty"`
	View        []clusterPeerWire `json:"view,omitempty"`
	Filters     []string          `json:"filters,omitempty"`
	Namespace   string            `json:"ns,omitempty"`
	Seq         uint64            `json:"seq,omitempty"`
	Event       *ClusterMessage   `json:"event,omitempty"`
}

// clusterPeerWire is one entry of the member view a handshake carries.
type clusterPeerWire struct {
	NodeID      string `json:"node"`
	Addr        string `json:"addr"`
	Incarnation string `json:"inc"`
}

// NewTCPCluster creates the built-in TCP cluster backend: JSON frames over
// gnety's length-prefixed TCP transport.
func NewTCPCluster(cfg TCPClusterConfig) Cluster {
	return &tcpCluster{
		cfg:       cfg,
		namespace: cfg.Namespace,
		links:     map[*clusterLink]struct{}{},
		members:   map[string]*clusterMember{},
		seeds:     map[string]string{},
		local:     newSummarySet(),
	}
}

// clusterLink is one physical connection. A link becomes a member once its
// handshake is accepted; inbound and outbound links are symmetric, the only
// difference is who opened the socket.
type clusterLink struct {
	outbound bool
	seed     string // outbound only: the configured seed this link dials
	sendFn   func(any) error
	closeFn  func() error

	lastSeen atomic.Int64 // unix nanoseconds of the last frame received
	rejected atomic.Bool  // the peer refused or superseded this link

	// Identity of the peer, written once under tcpCluster.mu when the handshake
	// is accepted and read under the same lock.
	nodeID      string
	addr        string
	incarnation string

	closeOnce sync.Once
}

func (l *clusterLink) touch()          { l.lastSeen.Store(time.Now().UnixNano()) }
func (l *clusterLink) seen() time.Time { return time.Unix(0, l.lastSeen.Load()) }
func (l *clusterLink) close()          { l.closeOnce.Do(func() { _ = l.closeFn() }) }

// clusterMember is one logical node: the public Member plus the link that
// currently backs it and the subscription summary it advertised.
type clusterMember struct {
	Member
	link         *clusterLink
	filters      []string
	filtersKnown bool
	filtersAt    time.Time
}

type tcpCluster struct {
	cfg         TCPClusterConfig
	log         *slog.Logger
	nodeID      string
	namespace   string
	incarnation string
	ctx         context.Context
	cancel      context.CancelFunc
	handler     ClusterHandler
	server      *gnety.Server

	mu      sync.Mutex
	running bool
	// links holds every physical connection; members holds exactly one entry
	// per node id.
	links   map[*clusterLink]struct{}
	members map[string]*clusterMember
	// seeds maps a dialed seed address to the node id behind it, so a dial loop
	// stops dialing while another link already backs that node.
	seeds map[string]string
	// local is the advertised filter summary, oldest first.
	local *summarySet
	// summaryMu serialises summary computation and broadcast, so a peer cannot
	// end up with an older snapshot than the one already sent.
	summaryMu sync.Mutex

	seq   atomic.Uint64
	dedup *dedupTable
	drops atomic.Uint64

	wg sync.WaitGroup
}

// setNamespace implements namespaceScoped: the broker's Config.Namespace is
// authoritative for a backend attached to a broker. It must run before Start;
// the broker calls it that way.
func (c *tcpCluster) setNamespace(namespace string) { c.namespace = namespace }

// Start implements Cluster. It listens on the configured address, exchanges a
// handshake on every link and starts the heartbeat, summary refresh and dial
// loops.
func (c *tcpCluster) Start(ctx context.Context, nodeID string, handler ClusterHandler) error {
	if nodeID == "" {
		return errors.New("gmqx: cluster node id is required")
	}
	if c.cfg.Addr == "" {
		return errors.New("gmqx: cluster Addr is required")
	}
	if strings.HasSuffix(c.cfg.Addr, ":0") {
		return fmt.Errorf("gmqx: cluster Addr must be explicit, got %q", c.cfg.Addr)
	}
	c.log = c.cfg.Logger
	if c.log == nil {
		c.log = slog.Default()
	}
	c.nodeID, c.handler = nodeID, handler
	c.incarnation = randomIncarnation()
	c.dedup = newDedupTable(c.cfg.DedupSize, c.cfg.DedupTTL)
	c.ctx, c.cancel = context.WithCancel(ctx)

	srv, err := gnety.New(gnety.Config{
		Network:      "tcp",
		Addr:         c.cfg.Addr,
		Codec:        gnety.LengthFieldCodec{},
		Handler:      gnety.HandlerFuncs{Open: c.onOpen, Message: c.onMessage, Close: c.onClose},
		WriteTimeout: clusterWriteTimeout,
	})
	if err != nil {
		c.cancel()
		return err
	}
	if err := srv.Start(c.ctx); err != nil {
		c.cancel()
		return fmt.Errorf("gmqx: cluster listen %s: %w", c.cfg.Addr, err)
	}
	c.server = srv

	c.mu.Lock()
	c.running = true
	c.mu.Unlock()

	c.wg.Add(1)
	go c.housekeeping()

	for _, seed := range c.cfg.Seeds {
		seed = strings.TrimSpace(seed)
		// The node with the lower advertised address dials; symmetric seed lists
		// therefore produce one connection per pair without duplicate delivery.
		// A pair configured asymmetrically is still connected: whoever dials is
		// admitted, and a second link collapses on the handshake.
		if seed == "" || seed == c.cfg.Addr || seed < c.cfg.Addr {
			continue
		}
		c.wg.Add(1)
		go c.dialLoop(seed)
	}
	c.log.Info("gmqx cluster started", "node", nodeID, "addr", c.cfg.Addr, "incarnation", c.incarnation)
	return nil
}

// Stop implements Cluster.
func (c *tcpCluster) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = false
	cancel := c.cancel
	links := make([]*clusterLink, 0, len(c.links))
	for l := range c.links {
		links = append(links, l)
	}
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, l := range links {
		c.dropLink(l, "cluster stopped")
	}
	var err error
	if c.server != nil {
		err = c.server.Stop(ctx)
	}
	c.wg.Wait()
	return err
}

// Members implements MemberView. The view contains one entry per node id the
// handshake admitted, sorted by node id.
func (c *tcpCluster) Members() []Member {
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
// Only locally produced events leave a node. The cluster stamps Origin,
// NodeID and Seq on them, refuses to forward an event that already carries
// another node's origin, and drops a received event whose origin is this node,
// so a routing loop is impossible even if a future topology re-broadcasts.
//
// Publishes go only to the peers whose advertised subscription summary matches
// the topic. A peer whose summary is unknown or stale receives the publish
// anyway — never every peer, just that one — because a stale view must not cost
// a delivery. Retained publishes and kicks go to every peer: retained state must
// exist on every node for future subscribers, and a kick names a client id, not
// a topic, so only the node holding that session can act on it.
//
// Delivery stays best-effort: a failed peer is dropped and its dial loop
// reconnects.
func (c *tcpCluster) Spread(_ context.Context, msg ClusterMessage) error {
	if !c.isRunning() {
		return errors.New("gmqx: cluster is not running")
	}
	if msg.Type != ClusterPublish && msg.Type != ClusterKick {
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

	frame := clusterFrame{Kind: frameEvent, Version: clusterWireVersion, NodeID: c.nodeID, Event: &msg}
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	for _, l := range c.routingTargets(msg) {
		if err := l.sendFn(data); err != nil {
			if errors.Is(err, gnety.ErrWriteBusy) {
				// The peer is saturated: skip this event instead of tearing the
				// link down. Delivery is best-effort.
				c.recordDrop("cluster_backpressure", "peer write queue full")
				continue
			}
			c.dropLink(l, "send failed")
		}
	}
	return nil
}

// routingTargets picks the links one event is written to.
func (c *tcpCluster) routingTargets(msg ClusterMessage) []*clusterLink {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	staleAfter := c.summaryStaleAfter()
	out := make([]*clusterLink, 0, len(c.members))
	for _, m := range c.members {
		if m.link == nil {
			continue
		}
		switch {
		case msg.Type == ClusterKick || msg.Retain:
			out = append(out, m.link)
		case !m.filtersKnown || now.Sub(m.filtersAt) > staleAfter:
			// Unknown or stale summary: send to this peer only. Falling back to
			// a full flood would defeat directed routing; falling back to
			// nothing would lose messages.
			out = append(out, m.link)
		case summaryMatches(m.filters, msg.Topic):
			out = append(out, m.link)
		}
	}
	return out
}

// summaryMatches reports whether any advertised filter matches topic.
func summaryMatches(filters []string, topic string) bool {
	if topic == "" {
		return false
	}
	for _, f := range filters {
		if f != "" && matchTopic(f, topic) {
			return true
		}
	}
	return false
}

func (c *tcpCluster) onOpen(s *gnety.Session) {
	l := &clusterLink{sendFn: s.Send, closeFn: s.Close}
	l.touch()
	s.Set(clusterLinkAttr, l)
	if !c.addLink(l) {
		_ = s.Close()
		return
	}
	if err := c.sendFrame(l, c.helloFrame()); err != nil {
		c.dropLink(l, "handshake send failed")
	}
}

func (c *tcpCluster) onClose(s *gnety.Session, _ error) {
	v, ok := s.Get(clusterLinkAttr)
	if !ok {
		return
	}
	if l, ok := v.(*clusterLink); ok {
		c.dropLink(l, "connection closed")
	}
}

func (c *tcpCluster) onMessage(s *gnety.Session, payload any) {
	raw, ok := payload.([]byte)
	if !ok {
		return
	}
	v, ok := s.Get(clusterLinkAttr)
	if !ok {
		return
	}
	if l, ok := v.(*clusterLink); ok {
		c.handleFrame(l, raw)
	}
}

// handleFrame decodes one internal frame. Every frame refreshes the link's
// liveness, including frames from peers whose handshake was rejected.
func (c *tcpCluster) handleFrame(l *clusterLink, raw []byte) {
	var f clusterFrame
	if err := json.Unmarshal(raw, &f); err != nil {
		return
	}
	if f.Version != clusterWireVersion {
		c.rejectLink(l, fmt.Sprintf("wire version %d, want %d", f.Version, clusterWireVersion))
		return
	}
	l.touch()
	switch f.Kind {
	case frameHello:
		c.handleHello(l, f)
	case framePing:
		_ = c.sendFrame(l, clusterFrame{Kind: framePong, Version: clusterWireVersion, NodeID: c.nodeID})
	case framePong:
		// nothing to do: liveness is the touch above
	case frameSubs:
		c.handleSummary(l, f)
	case frameEvent:
		if f.Event != nil {
			c.dispatchEvent(*f.Event)
		}
	}
}

// handleHello admits a peer as a member. Exactly one link per node id survives:
// a second link to the same process is collapsed deterministically, and a link
// to a new incarnation of the node id replaces the old one.
func (c *tcpCluster) handleHello(l *clusterLink, f clusterFrame) {
	switch {
	case f.NodeID == "":
		c.rejectLink(l, "handshake without a node id")
		return
	case f.Addr == "":
		c.rejectLink(l, "handshake without a listen address")
		return
	case f.NodeID == c.nodeID:
		c.rejectLink(l, "handshake from this node")
		return
	case f.Addr == c.cfg.Addr:
		c.rejectLink(l, "handshake reuses this node's listen address")
		return
	}
	// The view is advisory: it lets a node notice that a peer knows it under a
	// different address than it advertises, which is exactly the case where both
	// ends may dial and the address ordering is ambiguous.
	if peer, ok := findViewEntry(f.View, c.nodeID); ok && peer.Addr != c.cfg.Addr {
		c.log.Warn("gmqx cluster address alias", "node", c.nodeID, "addr", c.cfg.Addr, "peer", f.NodeID, "peer_sees", peer.Addr)
	}

	c.mu.Lock()
	if l.nodeID != "" {
		same := l.nodeID == f.NodeID && l.incarnation == f.Incarnation
		c.mu.Unlock()
		if !same {
			c.rejectLink(l, "handshake changed the peer identity")
		}
		return
	}
	l.nodeID, l.addr, l.incarnation = f.NodeID, f.Addr, f.Incarnation
	if l.outbound {
		c.seeds[l.seed] = f.NodeID
	}
	member, replaced := c.admitLocked(l)
	c.mu.Unlock()

	if member == nil {
		c.rejectLink(l, "superseded by another link to the same node")
		return
	}
	if replaced != nil {
		c.dropLink(replaced, "superseded by a new link")
	}
	c.log.Debug("gmqx cluster member joined", "node", member.NodeID, "addr", member.Addr,
		"incarnation", member.Incarnation, "since", member.Since, "outbound", l.outbound)
	c.advertiseSummary(l)
}

// admitLocked installs l as the link backing its node id, or reports that the
// link loses against the link already there.
func (c *tcpCluster) admitLocked(l *clusterLink) (member *clusterMember, replaced *clusterLink) {
	existing := c.members[l.nodeID]
	if existing != nil && existing.link != nil && existing.link != l {
		if !c.winsLocked(existing, l) {
			return nil, nil
		}
		replaced = existing.link
	}
	since := time.Now()
	m := &clusterMember{
		Member: Member{NodeID: l.nodeID, Addr: l.addr, Incarnation: l.incarnation, Since: since},
		link:   l,
	}
	if existing != nil && existing.Incarnation == l.incarnation && existing.link != l {
		// Same process over a new link: keep the join time and the summary so
		// routing does not blink while the link is replaced.
		m.Since = existing.Since
		m.filters, m.filtersKnown, m.filtersAt = existing.filters, existing.filtersKnown, existing.filtersAt
	}
	c.members[l.nodeID] = m
	return m, replaced
}

// winsLocked reports whether the freshly handshaked link l replaces m.link.
// Both ends of a pair evaluate the same rules, so they converge on the same
// physical connection: a restarted peer always wins, and two links to the same
// process are collapsed to the one dialed by the lower advertised address.
func (c *tcpCluster) winsLocked(m *clusterMember, l *clusterLink) bool {
	if m.Incarnation != l.incarnation {
		return true
	}
	canonicalOutbound := c.cfg.Addr < l.addr
	if m.link.outbound != l.outbound {
		return l.outbound == canonicalOutbound
	}
	// Same direction (e.g. two seeds that resolve to the same node): keep the
	// established link. The loser backs off and retries later.
	return false
}

// handleSummary records the subscription summary a peer advertised. A summary
// of another namespace is ignored: directed routing only trusts filters that
// belong to the same scope, so the peer is treated as unknown and still receives
// every publish, which it then drops on the receive path.
func (c *tcpCluster) handleSummary(l *clusterLink, f clusterFrame) {
	if f.Namespace != c.namespace {
		c.recordDrop("cluster_namespace", "subscription summary for another namespace")
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.members[l.nodeID]
	if m == nil || m.link != l {
		return
	}
	m.filters = capFilters(f.Filters, c.maxSummaryFilters())
	m.filtersKnown = true
	m.filtersAt = time.Now()
}

// refreshLocalFilters recomputes the advertised summary and reports whether the
// handler offers one at all. A handler that cannot describe its subscriptions
// never advertises, so peers keep sending it every publish.
func (c *tcpCluster) refreshLocalFilters() ([]string, bool) {
	provider, ok := c.handler.(subscriptionProvider)
	if !ok {
		return nil, false
	}
	current := provider.subscriptionFilters()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.local.refresh(current, c.maxSummaryFilters()), true
}

func (c *tcpCluster) advertiseSummary(l *clusterLink) {
	c.summaryMu.Lock()
	defer c.summaryMu.Unlock()
	sum, ok := c.refreshLocalFilters()
	if !ok {
		return
	}
	if err := c.sendFrame(l, clusterFrame{Kind: frameSubs, Version: clusterWireVersion, NodeID: c.nodeID, Namespace: c.namespace, Filters: sum}); err != nil {
		c.dropLink(l, "summary send failed")
	}
}

// subscriptionsChanged implements subscriptionChangeNotifier. The broker calls
// it from the SUBSCRIBE/UNSUBSCRIBE path before it acknowledges, so the new
// summary is on the wire before a subscriber is told its subscription is live.
func (c *tcpCluster) subscriptionsChanged() {
	if !c.isRunning() {
		return
	}
	c.broadcastSummary()
}

// broadcastSummary advertises the current local summary to every member.
func (c *tcpCluster) broadcastSummary() {
	c.summaryMu.Lock()
	defer c.summaryMu.Unlock()
	sum, ok := c.refreshLocalFilters()
	if !ok {
		return
	}
	frame := clusterFrame{Kind: frameSubs, Version: clusterWireVersion, NodeID: c.nodeID, Namespace: c.namespace, Filters: sum}
	for _, l := range c.snapshotLinks(true) {
		if err := c.sendFrame(l, frame); err != nil {
			c.dropLink(l, "summary send failed")
		}
	}
}

// dispatchEvent hands one received event to the broker. Events from an unknown
// or evicted member are still handled — availability over strictness — but only
// a handshake can make that node a member again.
//
// Namespace isolation is the first filter: an event of another namespace is
// dropped before it can touch the dedup table or reach the handler, so a cluster
// that carries several namespaces never applies another scope's publish, retained
// update or kick.
func (c *tcpCluster) dispatchEvent(msg ClusterMessage) {
	if c.handler == nil {
		return
	}
	if msg.Type != ClusterPublish && msg.Type != ClusterKick {
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
	}
}

// recordDrop counts a dropped event and reports it when a metrics sink is
// configured.
func (c *tcpCluster) recordDrop(reason, detail string) {
	c.drops.Add(1)
	if c.cfg.Metrics != nil {
		c.cfg.Metrics.RecordDroppedMessage(reason)
	}
	c.log.Debug("gmqx cluster event dropped", "reason", reason, "detail", detail)
}

// housekeeping drives heartbeats and the periodic summary refresh.
func (c *tcpCluster) housekeeping() {
	defer c.wg.Done()
	heartbeat := time.NewTicker(c.heartbeatInterval())
	defer heartbeat.Stop()
	summary := time.NewTicker(c.summaryInterval())
	defer summary.Stop()
	for {
		select {
		case <-c.ctx.Done():
			// Close the links so a dial loop blocked in Receive wakes up even
			// when the context is cancelled without a Stop.
			for _, l := range c.snapshotLinks(false) {
				c.dropLink(l, "cluster context done")
			}
			return
		case <-heartbeat.C:
			c.sendHeartbeats()
		case <-summary.C:
			c.broadcastSummary()
		}
	}
}

// sendHeartbeats pings every link and evicts the members that stayed silent for
// the whole miss budget. Reconnect keeps working: an evicted outbound link is
// re-dialed, an evicted inbound one is re-opened by the peer.
func (c *tcpCluster) sendHeartbeats() {
	now := time.Now()
	window := c.missWindow()
	ping := clusterFrame{Kind: framePing, Version: clusterWireVersion, NodeID: c.nodeID}
	for _, l := range c.snapshotLinks(false) {
		if now.Sub(l.seen()) > window {
			c.dropLink(l, "heartbeat timeout")
			continue
		}
		if err := c.sendFrame(l, ping); err != nil {
			c.dropLink(l, "heartbeat send failed")
		}
	}
}

// snapshotLinks copies the link set; handshakedOnly skips links that have not
// been admitted yet.
func (c *tcpCluster) snapshotLinks(handshakedOnly bool) []*clusterLink {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*clusterLink, 0, len(c.links))
	for l := range c.links {
		if handshakedOnly && l.nodeID == "" {
			continue
		}
		out = append(out, l)
	}
	return out
}

func (c *tcpCluster) addLink(l *clusterLink) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return false
	}
	c.links[l] = struct{}{}
	return true
}

// detachLink forgets a link and the member it backs, reporting whether a member
// disappeared.
func (c *tcpCluster) detachLink(l *clusterLink) (member bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.links, l)
	if l.nodeID == "" {
		return false
	}
	if m, ok := c.members[l.nodeID]; ok && m.link == l {
		delete(c.members, l.nodeID)
		return true
	}
	return false
}

// dropLink detaches a link, closes it and logs the membership change.
func (c *tcpCluster) dropLink(l *clusterLink, reason string) {
	wasMember := c.detachLink(l)
	l.close()
	if wasMember {
		c.log.Debug("gmqx cluster member removed", "node", l.nodeID, "addr", l.addr,
			"incarnation", l.incarnation, "reason", reason)
		return
	}
	c.log.Debug("gmqx cluster link closed", "node", l.nodeID, "addr", l.addr, "reason", reason)
}

// rejectLink closes a link that may not become a member and makes its dial loop
// back off instead of reconnecting immediately.
func (c *tcpCluster) rejectLink(l *clusterLink, reason string) {
	l.rejected.Store(true)
	c.log.Warn("gmqx cluster link rejected", "node", l.nodeID, "addr", l.addr,
		"outbound", l.outbound, "reason", reason)
	c.dropLink(l, reason)
}

// helloFrame is this node's handshake: identity, listen address, process
// incarnation and the member view the receiver can check its own rules against.
func (c *tcpCluster) helloFrame() clusterFrame {
	c.mu.Lock()
	view := make([]clusterPeerWire, 0, len(c.members))
	for _, m := range c.members {
		view = append(view, clusterPeerWire{NodeID: m.NodeID, Addr: m.Addr, Incarnation: m.Incarnation})
	}
	c.mu.Unlock()
	sort.Slice(view, func(i, j int) bool { return view[i].NodeID < view[j].NodeID })
	return clusterFrame{
		Kind:        frameHello,
		Version:     clusterWireVersion,
		NodeID:      c.nodeID,
		Addr:        c.cfg.Addr,
		Incarnation: c.incarnation,
		View:        view,
	}
}

func (c *tcpCluster) sendFrame(l *clusterLink, f clusterFrame) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return l.sendFn(data)
}

// seedConnected reports whether a link already backs the node behind seed, so a
// dial loop can stay idle instead of adding a duplicate connection.
func (c *tcpCluster) seedConnected(seed string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.seeds[seed]
	if !ok {
		return false
	}
	m, ok := c.members[id]
	return ok && m.link != nil
}

// dialLoop keeps one outbound link to seed alive.
func (c *tcpCluster) dialLoop(seed string) {
	defer c.wg.Done()
	interval := c.reconnectInterval()
	for c.ctx.Err() == nil {
		if c.seedConnected(seed) {
			if !sleepCtx(c.ctx, interval) {
				return
			}
			continue
		}
		client, err := gnety.Dial(c.ctx, gnety.ClientConfig{
			Network:      "tcp",
			Addr:         seed,
			Codec:        gnety.LengthFieldCodec{},
			Timeout:      2 * time.Second,
			WriteTimeout: clusterWriteTimeout,
		})
		if err != nil {
			c.log.Debug("gmqx cluster dial failed", "seed", seed, "err", err)
			if !sleepCtx(c.ctx, interval) {
				return
			}
			continue
		}
		link := &clusterLink{outbound: true, seed: seed, sendFn: client.Send, closeFn: client.Close}
		link.touch()
		if !c.addLink(link) {
			_ = client.Close()
			return
		}
		if err := c.sendFrame(link, c.helloFrame()); err != nil {
			c.dropLink(link, "handshake send failed")
		} else {
			// The connection is bidirectional: remote frames arrive on this
			// client. The read is not bound to c.ctx because gnety sets a read
			// deadline whenever the context has one; cancellation and Stop both
			// close the link, which unblocks the read.
			for {
				payload, rerr := client.Receive(context.Background())
				if rerr != nil {
					break
				}
				if raw, ok := payload.([]byte); ok {
					c.handleFrame(link, raw)
				}
			}
			c.dropLink(link, "connection closed")
		}
		_ = client.Close()

		wait := interval
		if link.rejected.Load() {
			wait = c.rejectBackoff()
		}
		if !sleepCtx(c.ctx, wait) {
			return
		}
	}
}

func (c *tcpCluster) isRunning() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

// peerCount reports the number of open links (used by tests).
func (c *tcpCluster) peerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.links)
}

// memberSummary reports a member's advertised filters and whether they are
// known (used by tests).
func (c *tcpCluster) memberSummary(nodeID string) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.members[nodeID]
	if !ok || !m.filtersKnown {
		return nil, false
	}
	return append([]string(nil), m.filters...), true
}

// dropCount reports dropped events (used by tests).
func (c *tcpCluster) dropCount() uint64 { return c.drops.Load() }

func (c *tcpCluster) reconnectInterval() time.Duration {
	if c.cfg.ReconnectInterval > 0 {
		return c.cfg.ReconnectInterval
	}
	return defaultReconnectInterval
}

func (c *tcpCluster) heartbeatInterval() time.Duration {
	if c.cfg.HeartbeatInterval > 0 {
		return c.cfg.HeartbeatInterval
	}
	return defaultHeartbeatInterval
}

func (c *tcpCluster) missBudget() int {
	if c.cfg.HeartbeatMissBudget > 0 {
		return c.cfg.HeartbeatMissBudget
	}
	return defaultHeartbeatMissBudget
}

// missWindow is how long a link may stay silent before it is evicted.
func (c *tcpCluster) missWindow() time.Duration {
	return time.Duration(c.missBudget()) * c.heartbeatInterval()
}

func (c *tcpCluster) summaryInterval() time.Duration {
	if c.cfg.SummaryInterval > 0 {
		return c.cfg.SummaryInterval
	}
	return defaultSummaryInterval
}

// summaryStaleAfter is how long an advertised summary stays authoritative.
func (c *tcpCluster) summaryStaleAfter() time.Duration {
	return summaryStaleFactor * c.summaryInterval()
}

func (c *tcpCluster) maxSummaryFilters() int {
	if c.cfg.MaxSummaryFilters > 0 {
		return c.cfg.MaxSummaryFilters
	}
	return defaultMaxSummaryFilters
}

// rejectBackoff is the pause before re-dialing a peer that refused the link.
func (c *tcpCluster) rejectBackoff() time.Duration {
	if window := c.missWindow(); window > minRejectBackoff {
		return window
	}
	return minRejectBackoff
}

// findViewEntry looks a node up in the member view a handshake carries.
func findViewEntry(view []clusterPeerWire, nodeID string) (clusterPeerWire, bool) {
	for _, e := range view {
		if e.NodeID == nodeID {
			return e, true
		}
	}
	return clusterPeerWire{}, false
}

// randomIncarnation returns a random process incarnation for this Start.
func randomIncarnation() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
