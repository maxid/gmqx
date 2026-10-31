package gmqx

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxid/gnety"
)

// gossipLink is one physical connection between two gossip nodes. Connections are
// established on demand — from the seed list, from a member record learned through
// gossip, or because an event has to be written to a member — and then reused for
// every later frame. The frame queue is bounded, so a slow peer can only lose
// frames (counted as cluster_backpressure); it never blocks a publisher, a
// receiver or a membership tick, and no goroutine is created per message.
type gossipLink struct {
	outbound bool
	// dialAddr is the address this link was dialed at, when it is outbound. It
	// also keys the dial backoff after the link closes.
	dialAddr string
	// expect is the node id an outbound dial expects to reach, or "" when the
	// address is a seed and the node id is not known yet.
	expect string

	send    func(any) error
	closeFn func() error
	queue   chan []byte
	done    chan struct{}

	closeOnce sync.Once
	rejected  atomic.Bool

	// Identity of the peer, written under gossipCluster.mu when the link is
	// identified and read under the same lock.
	nodeID      string
	addr        string
	incarnation string
}

func newGossipLink(outbound bool, dialAddr, expect string, send func(any) error, closeFn func() error) *gossipLink {
	return &gossipLink{
		outbound: outbound,
		dialAddr: dialAddr,
		expect:   expect,
		send:     send,
		closeFn:  closeFn,
		queue:    make(chan []byte, gossipLinkQueue),
		done:     make(chan struct{}),
	}
}

func (l *gossipLink) close() { l.closeOnce.Do(func() { close(l.done); _ = l.closeFn() }) }

// preferredGossipLink reports whether l is the canonical link of the pair: the node
// with the lexicographically lower node id dials, and the other side keeps the
// inbound link. Both ends evaluate the same rule, so two nodes that dialed each
// other converge on the same physical connection instead of churning; when only the
// non-canonical link exists it is used until the canonical one arrives.
func preferredGossipLink(local, peer string, l *gossipLink) bool {
	return l.outbound == (local < peer)
}

// onLinkOpen registers an inbound gnety session as a gossip link. The peer is not
// a member yet: its first frame carries its identity.
func (c *gossipCluster) onLinkOpen(s *gnety.Session) {
	l := newGossipLink(false, "", "", s.Send, s.Close)
	s.Set(gossipLinkAttr, l)
	if !c.addLink(l) {
		_ = s.Close()
	}
}

func (c *gossipCluster) onLinkClose(s *gnety.Session, _ error) {
	v, ok := s.Get(gossipLinkAttr)
	if !ok {
		return
	}
	if l, ok := v.(*gossipLink); ok {
		c.dropLink(l, "connection closed")
	}
}

func (c *gossipCluster) onLinkMessage(s *gnety.Session, payload any) {
	raw, ok := payload.([]byte)
	if !ok {
		return
	}
	v, ok := s.Get(gossipLinkAttr)
	if !ok {
		return
	}
	if l, ok := v.(*gossipLink); ok {
		c.handleFrame(l, raw)
	}
}

// addLink registers a link and starts its writer. The writer is one goroutine per
// link, not per message; it exits when the link closes.
func (c *gossipCluster) addLink(l *gossipLink) bool {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return false
	}
	c.links[l] = struct{}{}
	c.mu.Unlock()
	c.wg.Add(1)
	go c.linkWriter(l)
	return true
}

func (c *gossipCluster) linkWriter(l *gossipLink) {
	defer c.wg.Done()
	for {
		select {
		case <-l.done:
			return
		case data := <-l.queue:
			if err := l.send(data); err != nil {
				c.dropLink(l, "write failed")
				return
			}
		}
	}
}

// enqueue appends one encoded frame to a link's bounded queue. A full queue drops
// the frame; it never blocks the caller.
func (c *gossipCluster) enqueue(l *gossipLink, data []byte) bool {
	if l == nil {
		return false
	}
	select {
	case <-l.done:
		return false
	default:
	}
	select {
	case l.queue <- data:
		return true
	case <-l.done:
		return false
	default:
		return false
	}
}

// handleFrame decodes and dispatches one gossip frame. The sender is identified
// before anything else, so no frame from an unidentified or rejected link can
// create a member or touch the dedup table.
func (c *gossipCluster) handleFrame(l *gossipLink, raw []byte) {
	var f gossipFrame
	if err := json.Unmarshal(raw, &f); err != nil {
		return
	}
	if f.Version != gossipWireVersion {
		c.rejectLink(l, fmt.Sprintf("wire version %d, want %d", f.Version, gossipWireVersion))
		return
	}
	if !c.identifyLink(l, f) {
		return
	}
	c.observePeerFrame(f)
	c.mergeDeltas(f.Deltas)
	c.mergePresence(f.Presence)
	switch f.Kind {
	case gossipFramePing:
		c.sendFrameToLink(l, c.metadataFrame(gossipFrameAck, f.Nonce, ""))
	case gossipFrameAck:
		c.handleAck(f)
	case gossipFramePingReq:
		c.handlePingReq(f)
	case gossipFrameUpdate:
		// The metadata was merged above; an update carries no reply.
	case gossipFrameEvent:
		if f.Event != nil {
			c.handleEvent(l, *f.Event)
		}
	}
}

// identifyLink binds a link to the peer identity its frame carries and keeps
// exactly one link per node id. The rules, evaluated identically on both ends:
//
//   - the frame must carry a node id, an incarnation and a listen address;
//   - a frame that names this node's id or listen address is refused, which is
//     how a duplicate node id is kept out of the cluster;
//   - an outbound dial that expected one node id refuses another;
//   - a new incarnation on a new link wins over the old one (the peer restarted);
//   - two links to the same incarnation collapse to the one dialed by the lower
//     node id, and a link in the non-canonical direction is kept only while the
//     canonical one does not exist.
func (c *gossipCluster) identifyLink(l *gossipLink, f gossipFrame) bool {
	if f.NodeID == "" || f.Incarnation == "" || f.Addr == "" {
		c.rejectLink(l, "frame without a complete identity")
		return false
	}
	if f.NodeID == c.nodeID {
		c.rejectLink(l, "frame claims this node's id (duplicate node id)")
		return false
	}
	if f.Addr == c.cfg.Addr {
		c.rejectLink(l, "frame claims this node's listen address")
		return false
	}

	var supersede *gossipLink
	var reason string
	c.mu.Lock()
	switch {
	case !c.running:
		reason = "cluster is not running"
	case l.nodeID != "" && l.nodeID != f.NodeID:
		reason = "peer changed its node id on one link"
	case l.nodeID != "" && l.incarnation != f.Incarnation:
		reason = "peer changed its incarnation on one link"
	case l.nodeID == "" && l.expect != "" && l.expect != f.NodeID:
		reason = fmt.Sprintf("dialed node %q but reached %q", l.expect, f.NodeID)
	default:
		if l.nodeID == "" {
			existing := c.linksByNode[f.NodeID]
			replace := existing != nil && existing != l
			switch {
			case !replace:
			case existing.incarnation != f.Incarnation:
				// The peer restarted: its new process replaces the old link.
			case preferredGossipLink(c.nodeID, f.NodeID, l) && !preferredGossipLink(c.nodeID, f.NodeID, existing):
			default:
				reason = "another link already backs this member"
				replace = false
			}
			if reason == "" {
				if replace {
					supersede = existing
					delete(c.linksByNode, f.NodeID)
				}
				l.nodeID, l.addr, l.incarnation = f.NodeID, f.Addr, f.Incarnation
				c.linksByNode[f.NodeID] = l
				if l.dialAddr != "" {
					c.addrNodes[l.dialAddr] = f.NodeID
				}
			}
		}
	}
	c.mu.Unlock()

	if reason != "" {
		c.rejectLink(l, reason)
		return false
	}
	if supersede != nil {
		c.closeLink(supersede, "superseded by the canonical link", false)
	}
	return true
}

// rejectLink closes a link that may not become a member. The dial backoff of its
// address is extended so two nodes that keep offering an unacceptable identity do
// not reconnect in a tight loop.
func (c *gossipCluster) rejectLink(l *gossipLink, reason string) {
	l.rejected.Store(true)
	c.mu.Lock()
	nodeID, addr := l.nodeID, l.addr
	c.mu.Unlock()
	c.log.Warn("gmqx gossip cluster link rejected", "node", nodeID, "addr", addr,
		"outbound", l.outbound, "reason", reason)
	c.closeLink(l, reason, false)
}

// dropLink closes a link that stopped working. When it was the link backing a
// member, the member becomes suspect: the probe state machine then decides whether
// it answers again (a reconnect) or is declared dead.
func (c *gossipCluster) dropLink(l *gossipLink, reason string) {
	c.closeLink(l, reason, true)
}

// closeLink detaches a link under the cluster lock, optionally marking the member it
// backed as suspect, then closes the connection. Closing is idempotent, so a
// rejected and a broken link can both be closed by their own path.
func (c *gossipCluster) closeLink(l *gossipLink, reason string, suspect bool) {
	now := c.now()
	var nodeID string
	var currentMember string
	c.mu.Lock()
	if _, ok := c.links[l]; !ok {
		c.mu.Unlock()
		l.close()
		return
	}
	delete(c.links, l)
	if l.nodeID != "" && c.linksByNode[l.nodeID] == l {
		delete(c.linksByNode, l.nodeID)
		currentMember = l.nodeID
	}
	nodeID = l.nodeID
	if l.dialAddr != "" {
		backoff := c.reconnectInterval()
		if l.rejected.Load() {
			backoff = c.rejectBackoff()
		}
		c.dialNext[l.dialAddr] = now.Add(backoff)
	}
	// A peer can legitimately close one of two links that both ends opened; as
	// long as another link to the same peer (identified, or an unidentified dial
	// to its address) is still open, losing this one is not a connectivity loss
	// and must not start a suspicion.
	if suspect && currentMember != "" && !c.hasOtherLinkLocked(l, currentMember, l.addr) {
		c.markSuspectLocked(currentMember, reason, now)
	}
	c.mu.Unlock()
	l.close()
	c.log.Debug("gmqx gossip link closed", "node", nodeID, "reason", reason)
}

// hasOtherLinkLocked reports whether another open link covers the same peer: either
// an identified link to nodeID or an unidentified outbound dial to addr.
func (c *gossipCluster) hasOtherLinkLocked(except *gossipLink, nodeID, addr string) bool {
	for other := range c.links {
		if other == except {
			continue
		}
		if other.nodeID == nodeID {
			return true
		}
		if addr != "" && (other.addr == addr || other.dialAddr == addr) {
			return true
		}
	}
	return false
}

// snapshotLinks copies the link set; identifiedOnly skips links whose peer has not
// sent its identity yet.
func (c *gossipCluster) snapshotLinks(identifiedOnly bool) []*gossipLink {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*gossipLink, 0, len(c.links))
	for l := range c.links {
		if identifiedOnly && l.nodeID == "" {
			continue
		}
		out = append(out, l)
	}
	return out
}

// sendFrameToLink encodes f and queues it on one link.
func (c *gossipCluster) sendFrameToLink(l *gossipLink, f gossipFrame) bool {
	data, err := json.Marshal(f)
	if err != nil {
		return false
	}
	if !c.enqueue(l, data) {
		c.recordDrop("cluster_backpressure", "link queue full")
		return false
	}
	return true
}

// sendFrameToNode encodes f and queues it on the link that backs nodeID, dialing
// the member if no link exists.
func (c *gossipCluster) sendFrameToNode(nodeID string, f gossipFrame) bool {
	data, err := json.Marshal(f)
	if err != nil {
		return false
	}
	return c.sendDataTo(nodeID, data)
}

// sendDataTo writes one encoded frame to the link of nodeID. A missing link is a
// dropped frame (delivery is best-effort and the dial is requested for later), and
// a full queue is a dropped frame too; both are counted rather than blocking the
// caller.
func (c *gossipCluster) sendDataTo(nodeID string, data []byte) bool {
	c.mu.Lock()
	l := c.linksByNode[nodeID]
	var addr string
	if m := c.members[nodeID]; m != nil {
		addr = m.Addr
	}
	c.mu.Unlock()
	if l == nil {
		if addr != "" {
			c.requestDial(addr, nodeID)
		}
		c.recordDrop("cluster_unreachable", "no link to "+nodeID)
		return false
	}
	if !c.enqueue(l, data) {
		c.recordDrop("cluster_backpressure", "link queue full")
		return false
	}
	return true
}

// requestDial asks the dial worker to connect an address. One dial per address is
// in flight at a time, and a failed or closed address backs off for
// ReconnectInterval, so a member that is down costs one failed dial per interval,
// not one per event.
func (c *gossipCluster) requestDial(addr, expect string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	now := c.now()
	c.mu.Lock()
	if !c.running || c.dialing[addr] {
		c.mu.Unlock()
		return
	}
	if next, ok := c.dialNext[addr]; ok && now.Before(next) {
		c.mu.Unlock()
		return
	}
	if c.hasLinkAddrLocked(addr) {
		c.mu.Unlock()
		return
	}
	c.dialing[addr] = true
	c.mu.Unlock()

	select {
	case c.dialCh <- gossipDialRequest{addr: addr, expect: expect}:
	default:
		c.mu.Lock()
		delete(c.dialing, addr)
		c.mu.Unlock()
	}
}

// hasLinkAddrLocked reports whether a link already covers addr, either because it
// was dialed there or because a peer identified itself under that advertised
// address.
func (c *gossipCluster) hasLinkAddrLocked(addr string) bool {
	for l := range c.links {
		if l.addr == addr || l.dialAddr == addr {
			return true
		}
	}
	if id, ok := c.addrNodes[addr]; ok {
		if _, ok := c.linksByNode[id]; ok {
			return true
		}
	}
	return false
}

// dialMissing requests a dial for every seed and every member without a link. It
// runs each probe tick, so a returned, restarted or newly learned member is
// connected without waiting for an event that needs it.
func (c *gossipCluster) dialMissing() {
	var reqs []gossipDialRequest
	c.mu.Lock()
	for _, seed := range c.seedList() {
		if c.hasLinkAddrLocked(seed) {
			continue
		}
		reqs = append(reqs, gossipDialRequest{addr: seed})
	}
	for id, m := range c.members {
		if m.State == MemberDead || m.Addr == "" || c.linksByNode[id] != nil {
			continue
		}
		reqs = append(reqs, gossipDialRequest{addr: m.Addr, expect: id})
	}
	c.mu.Unlock()
	for _, req := range reqs {
		c.requestDial(req.addr, req.expect)
	}
}

// dialWorker is the single goroutine that performs outbound dials, so a connect
// attempt never runs on the publish path or on a receive goroutine.
func (c *gossipCluster) dialWorker() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case req := <-c.dialCh:
			c.dial(req)
		}
	}
}

// dial connects one address, starts the link's writer and read loop, and sends the
// identity ping that lets both ends identify each other.
func (c *gossipCluster) dial(req gossipDialRequest) {
	ctx, cancel := context.WithTimeout(c.ctx, c.dialTimeout())
	client, err := gnety.Dial(ctx, gnety.ClientConfig{
		Network:      "tcp",
		Addr:         req.addr,
		Codec:        gnety.LengthFieldCodec{},
		Timeout:      c.dialTimeout(),
		WriteTimeout: clusterWriteTimeout,
	})
	cancel()

	now := c.now()
	c.mu.Lock()
	delete(c.dialing, req.addr)
	if err != nil {
		c.dialNext[req.addr] = now.Add(c.reconnectInterval())
		c.mu.Unlock()
		c.log.Debug("gmqx gossip dial failed", "addr", req.addr, "err", err)
		return
	}
	c.dialNext[req.addr] = now.Add(c.reconnectInterval())
	c.mu.Unlock()

	l := newGossipLink(true, req.addr, req.expect, client.Send, client.Close)
	if !c.addLink(l) {
		_ = client.Close()
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() { _ = client.Close() }()
		// The read is not bound to c.ctx: cancel and Stop both close the link,
		// which unblocks the read.
		for {
			payload, rerr := client.Receive(context.Background())
			if rerr != nil {
				break
			}
			if raw, ok := payload.([]byte); ok {
				c.handleFrame(l, raw)
			}
		}
		c.dropLink(l, "connection closed")
	}()
	// The dialer speaks first, so the peer learns this node from the ping and this
	// node learns the peer from the ack.
	c.sendFrameToLink(l, c.metadataFrame(gossipFramePing, c.nonce.Add(1), ""))
}

// rejectBackoff is the pause before re-dialing a peer that refused the link. It is
// at least the heartbeat window so a duplicate node id cannot make two processes
// reconnect in a tight loop.
func (c *gossipCluster) rejectBackoff() time.Duration {
	if window := c.suspectTimeout(); window > minRejectBackoff {
		return window
	}
	return minRejectBackoff
}
