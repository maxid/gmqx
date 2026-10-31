package gmqx

import (
	"encoding/json"
	"sort"
	"time"
)

// gossipFrame is the internal wire envelope of the gossip backend. Membership,
// routing metadata and events share one versioned JSON frame so the ordering
// rules are the same on every kind.
//
// Every frame carries the sender's own member record (NodeID, Incarnation, Addr,
// IncNumber, Since) plus a bounded membership delta and a bounded presence delta.
// The self record is what lets a receiver learn or refresh the sender without a
// separate handshake; the deltas are what gossips a change to the rest of the
// cluster.
type gossipFrame struct {
	Kind    string `json:"kind"`
	Version int    `json:"v"`

	// The sender's own member record.
	NodeID      string `json:"node,omitempty"`
	Incarnation string `json:"inc,omitempty"`
	Addr        string `json:"addr,omitempty"`
	IncNumber   uint64 `json:"incn,omitempty"`
	Since       int64  `json:"since,omitempty"`

	// Nonce correlates a ping with its ack (and a pingreq with the indirect
	// ack). Target names the member an indirect probe is about.
	Nonce  uint64 `json:"nonce,omitempty"`
	Target string `json:"target,omitempty"`

	// Deltas is the bounded membership gossip: records that changed recently,
	// followed by a rotating anti-entropy slice.
	Deltas []gossipMemberWire `json:"deltas,omitempty"`
	// Summary is the sender's subscription summary; nil means "this node cannot
	// describe its subscriptions", which peers treat as unknown.
	Summary *SubscriptionSummary `json:"summary,omitempty"`
	// Presence is the bounded client-id claim gossip.
	Presence []gossipPresenceWire `json:"presence,omitempty"`

	Event *ClusterMessage `json:"event,omitempty"`
}

// gossipMemberWire is one member record on the wire: the public member fields plus
// the SWIM incarnation number and state.
type gossipMemberWire struct {
	NodeID      string `json:"node"`
	Addr        string `json:"addr"`
	Incarnation string `json:"inc"`
	IncNumber   uint64 `json:"incn,omitempty"`
	Status      string `json:"status,omitempty"`
	Since       int64  `json:"since,omitempty"`
}

// gossipPresenceWire is one client-id claim on the wire. NodeID is empty for a
// tombstone; Version is a Lamport counter compared before anything else.
type gossipPresenceWire struct {
	ClientID    string `json:"client"`
	NodeID      string `json:"node,omitempty"`
	Incarnation string `json:"inc,omitempty"`
	Version     uint64 `json:"version,omitempty"`
	Namespace   string `json:"ns,omitempty"`
}

// metadataFrame builds a ping, ack, pingreq or update frame with this node's
// current metadata.
func (c *gossipCluster) metadataFrame(kind string, nonce uint64, target string) gossipFrame {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metadataFrameLocked(kind, nonce, target)
}

// metadataFrameLocked is metadataFrame for callers that already hold mu.
func (c *gossipCluster) metadataFrameLocked(kind string, nonce uint64, target string) gossipFrame {
	now := c.now()
	return gossipFrame{
		Kind:        kind,
		Version:     gossipWireVersion,
		NodeID:      c.nodeID,
		Incarnation: c.incarnation,
		Addr:        c.cfg.Addr,
		IncNumber:   c.incNumber,
		Since:       c.since.UnixNano(),
		Nonce:       nonce,
		Target:      target,
		Deltas:      c.deltasLocked(now, c.maxGossipDelta()),
		Summary:     c.summaryLocked(),
		Presence:    c.presenceDeltaLocked(now, c.maxPresenceDelta()),
	}
}

// selfWireLocked is this node's member record.
func (c *gossipCluster) selfWireLocked() gossipMemberWire {
	return gossipMemberWire{
		NodeID:      c.nodeID,
		Addr:        c.cfg.Addr,
		Incarnation: c.incarnation,
		IncNumber:   c.incNumber,
		Status:      string(MemberAlive),
		Since:       c.since.UnixNano(),
	}
}

func memberWire(m *gossipMember) gossipMemberWire {
	return gossipMemberWire{
		NodeID:      m.NodeID,
		Addr:        m.Addr,
		Incarnation: m.Incarnation,
		IncNumber:   m.incNumber,
		Status:      string(m.State),
		Since:       m.Since.UnixNano(),
	}
}

// deltasLocked returns the bounded membership gossip one frame carries: this
// node's own record, the records that changed within the last two probe intervals
// (the delta), and then a rotating slice of the remaining members so a node that
// joined from one seed converges on the whole view.
func (c *gossipCluster) deltasLocked(now time.Time, limit int) []gossipMemberWire {
	if limit <= 0 {
		return nil
	}
	out := make([]gossipMemberWire, 0, limit)
	out = append(out, c.selfWireLocked())
	seen := map[string]struct{}{c.nodeID: {}}

	ids := make([]string, 0, len(c.members))
	for id := range c.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	hot := 2 * c.pingInterval()
	for _, id := range ids {
		if len(out) >= limit {
			return out
		}
		m := c.members[id]
		if now.Sub(m.changedAt) >= hot {
			continue
		}
		out = append(out, memberWire(m))
		seen[id] = struct{}{}
	}
	for i := 0; i < len(ids) && len(out) < limit; i++ {
		id := ids[(c.memberCursor+i)%len(ids)]
		if _, ok := seen[id]; ok {
			continue
		}
		out = append(out, memberWire(c.members[id]))
	}
	c.memberCursor++
	return out
}

// observePeerFrame merges the sender's own member record and subscription summary
// from a frame. A frame is proof that the sender's incarnation is running, so it
// revives a suspect or dead record of the same incarnation with an incarnation
// number at least as high; a different incarnation is the sender's new process and
// replaces the old record (its summary and presence claims do not survive a
// restart).
func (c *gossipCluster) observePeerFrame(f gossipFrame) {
	now := c.now()
	inc := f.IncNumber
	if inc == 0 {
		inc = 1
	}
	since := timeFromUnixNano(f.Since, now)
	c.mu.Lock()
	m := c.members[f.NodeID]
	switch {
	case m == nil:
		if !c.roomForMemberLocked() {
			c.mu.Unlock()
			c.recordDrop("cluster_member_limit", "member view is full")
			return
		}
		m = &gossipMember{
			Member: Member{
				NodeID:      f.NodeID,
				Addr:        f.Addr,
				Incarnation: f.Incarnation,
				Since:       since,
				State:       MemberAlive,
			},
			incNumber: inc,
			changedAt: now,
		}
		c.members[f.NodeID] = m
		c.log.Debug("gmqx gossip member joined", "node", f.NodeID, "addr", f.Addr,
			"incarnation", f.Incarnation, "since", since)
	case m.Incarnation != f.Incarnation:
		c.purgePresenceIncarnationLocked(f.NodeID, m.Incarnation)
		oldInc := m.Incarnation
		m.Member = Member{
			NodeID:      f.NodeID,
			Addr:        f.Addr,
			Incarnation: f.Incarnation,
			Since:       since,
			State:       MemberAlive,
		}
		m.incNumber = inc
		m.suspectAt, m.deadAt = time.Time{}, time.Time{}
		m.summary, m.summaryAt, m.summaryKnown = SubscriptionSummary{}, time.Time{}, false
		m.changedAt = now
		c.log.Debug("gmqx gossip member restarted", "node", f.NodeID, "addr", f.Addr,
			"incarnation", f.Incarnation, "old_incarnation", oldInc, "since", since)
	default:
		changed := false
		if inc > m.incNumber {
			m.incNumber = inc
			changed = true
		}
		if f.Addr != "" && f.Addr != m.Addr {
			m.Addr = f.Addr
			changed = true
		}
		if m.State != MemberAlive {
			m.State = MemberAlive
			m.suspectAt, m.deadAt = time.Time{}, time.Time{}
			changed = true
			c.log.Debug("gmqx gossip member refuted suspicion", "node", f.NodeID,
				"incarnation", f.Incarnation, "incarnation_number", m.incNumber)
		}
		if changed {
			m.changedAt = now
		}
	}
	c.mu.Unlock()
	c.handleSummary(f, now)
}

// mergeDeltas folds the membership gossip one frame carried into the local view.
// A record about this node itself is handled first: a suspicion or death record
// with an incarnation number at least as high as this node's own is refuted by
// bumping the number, which is the SWIM mechanic that repairs a false suspicion.
func (c *gossipCluster) mergeDeltas(records []gossipMemberWire) {
	if len(records) == 0 {
		return
	}
	now := c.now()
	var toProbe []string
	var toClose []*gossipLink
	c.mu.Lock()
	for _, rec := range records {
		if rec.NodeID == "" || rec.Addr == "" || rec.Incarnation == "" {
			continue
		}
		if rec.NodeID == c.nodeID {
			c.refuteSelfLocked(rec)
			continue
		}
		link, probe := c.mergeRecordLocked(rec, now)
		if link != nil {
			toClose = append(toClose, link)
		}
		if probe {
			toProbe = append(toProbe, rec.NodeID)
		}
	}
	c.mu.Unlock()
	for _, l := range toClose {
		c.closeLink(l, "member restarted", false)
	}
	for _, id := range toProbe {
		c.startProbe(id)
	}
}

// refuteSelfLocked consumes a gossip record about this node. A record naming
// another incarnation of the same node id is a duplicate node id, not a statement
// about this process, and is warned about instead of being adopted. An accusation
// with an incarnation number at least as high as this node's own is answered by
// incrementing the number: the next frame this node sends carries the refutation,
// and the accuser (and everyone it told) accepts it as the newer record.
func (c *gossipCluster) refuteSelfLocked(rec gossipMemberWire) {
	if rec.Incarnation != c.incarnation {
		c.log.Warn("gmqx gossip cluster duplicate node id", "id", rec.NodeID,
			"incarnation", rec.Incarnation, "local_incarnation", c.incarnation)
		c.recordDrop("cluster_duplicate_node", "gossip record names another process with this node id")
		return
	}
	if memberStateFromWire(rec.Status) == MemberAlive {
		return
	}
	if rec.IncNumber < c.incNumber {
		// A stale accusation of an older incarnation number: ignore it, the
		// refutation that superseded it is newer.
		return
	}
	c.incNumber = rec.IncNumber + 1
	c.log.Info("gmqx gossip cluster refuted suspicion", "node", c.nodeID,
		"incarnation", c.incarnation, "incarnation_number", c.incNumber, "accused_as", rec.Status)
}

// roomForMemberLocked makes room in a full member view by removing the dead member
// whose tombstone has been waiting longest. It reports whether a new member may be
// added.
func (c *gossipCluster) roomForMemberLocked() bool {
	if len(c.members) < maxGossipMembers {
		return true
	}
	var oldest *gossipMember
	for _, m := range c.members {
		if m.State != MemberDead {
			continue
		}
		if oldest == nil || m.deadAt.Before(oldest.deadAt) {
			oldest = m
		}
	}
	if oldest == nil {
		return false
	}
	delete(c.members, oldest.NodeID)
	c.purgePresenceOwnerLocked(oldest.NodeID)
	c.log.Info("gmqx gossip member removed", "node", oldest.NodeID, "reason", "member view full")
	return true
}

// mergeRecordLocked merges one gossiped member record. The record that wins is the
// newer one (see memberRecordNewerLocked): a different incarnation wins by its join
// time (then by incarnation id), the same incarnation by its incarnation number and
// then by status rank (dead > suspect > alive). The returned link must be closed
// outside the lock when a restart supersedes it; the bool asks the caller to probe
// the node so a suspect or dead record can be refuted.
func (c *gossipCluster) mergeRecordLocked(rec gossipMemberWire, now time.Time) (*gossipLink, bool) {
	state := memberStateFromWire(rec.Status)
	inc := rec.IncNumber
	if inc == 0 {
		inc = 1
	}
	since := timeFromUnixNano(rec.Since, now)
	m := c.members[rec.NodeID]
	if m == nil {
		if !c.roomForMemberLocked() {
			c.recordDrop("cluster_member_limit", "member view is full")
			return nil, false
		}
		m = &gossipMember{
			Member: Member{
				NodeID:      rec.NodeID,
				Addr:        rec.Addr,
				Incarnation: rec.Incarnation,
				Since:       since,
				State:       state,
			},
			incNumber: inc,
			changedAt: now,
		}
		switch state {
		case MemberSuspect:
			m.suspectAt = now
		case MemberDead:
			m.deadAt = now.Add(c.deadTimeout())
			c.purgePresenceOwnerLocked(rec.NodeID)
		}
		c.members[rec.NodeID] = m
		c.log.Debug("gmqx gossip member learned", "node", rec.NodeID, "addr", rec.Addr,
			"incarnation", rec.Incarnation, "status", state)
		return nil, state != MemberAlive
	}
	if !memberRecordNewerLocked(rec, m, state, inc, since) {
		return nil, false
	}

	oldState, oldInc := m.State, m.Incarnation
	var closeLink *gossipLink
	if oldInc != rec.Incarnation {
		c.purgePresenceIncarnationLocked(rec.NodeID, oldInc)
		if l := c.linksByNode[rec.NodeID]; l != nil {
			delete(c.linksByNode, rec.NodeID)
			closeLink = l
		}
		m.Incarnation = rec.Incarnation
		m.Since = since
		m.summary, m.summaryAt, m.summaryKnown = SubscriptionSummary{}, time.Time{}, false
	}
	m.Addr = rec.Addr
	if inc > m.incNumber {
		m.incNumber = inc
	}
	m.State = state
	switch state {
	case MemberAlive:
		m.suspectAt, m.deadAt = time.Time{}, time.Time{}
	case MemberSuspect:
		if oldState != MemberSuspect {
			m.suspectAt = now
		}
		m.deadAt = time.Time{}
	case MemberDead:
		m.deadAt = now.Add(c.deadTimeout())
		c.purgePresenceOwnerLocked(rec.NodeID)
	}
	m.changedAt = now
	if oldState != m.State || oldInc != m.Incarnation {
		c.log.Debug("gmqx gossip member state", "node", rec.NodeID, "from", oldState,
			"to", m.State, "incarnation", m.Incarnation, "incarnation_number", m.incNumber)
	}
	return closeLink, state != MemberAlive
}

// memberRecordNewerLocked reports whether an incoming record supersedes the stored
// one. Incarnations are ordered by join time (the wire value is the sender's wall
// clock; a restart necessarily starts later on a cluster whose clocks are roughly
// synchronised) and tie-broken by the incarnation id. Within one incarnation the
// SWIM incarnation number is the truth; equal numbers are ordered by status so
// suspect and dead propagate without a number change.
func memberRecordNewerLocked(rec gossipMemberWire, m *gossipMember, state MemberState, inc uint64, since time.Time) bool {
	if rec.Incarnation != m.Incarnation {
		if rec.Since <= 0 {
			return false
		}
		if !since.Equal(m.Since) {
			return since.After(m.Since)
		}
		return rec.Incarnation > m.Incarnation
	}
	if inc != m.incNumber {
		return inc > m.incNumber
	}
	return memberStatusRank(state) > memberStatusRank(m.State)
}

func memberStateFromWire(status string) MemberState {
	switch MemberState(status) {
	case MemberSuspect:
		return MemberSuspect
	case MemberDead:
		return MemberDead
	default:
		return MemberAlive
	}
}

func memberStatusRank(state MemberState) int {
	switch state {
	case MemberSuspect:
		return 1
	case MemberDead:
		return 2
	default:
		return 0
	}
}

// timeFromUnixNano converts a wire timestamp, falling back to now for a frame that
// carries none (a build without the field).
func timeFromUnixNano(nanos int64, now time.Time) time.Time {
	if nanos <= 0 {
		return now
	}
	return time.Unix(0, nanos)
}

// markSuspectLocked moves an alive member to suspect. The suspect window starts
// once and is never extended by another failed probe: a member that stays silent is
// declared dead after one SuspectTimeout, not after every retry.
func (c *gossipCluster) markSuspectLocked(nodeID, reason string, now time.Time) {
	m := c.members[nodeID]
	if m == nil || m.State != MemberAlive {
		return
	}
	m.State = MemberSuspect
	m.suspectAt = now
	m.deadAt = time.Time{}
	m.changedAt = now
	c.log.Debug("gmqx gossip member suspect", "node", nodeID, "reason", reason,
		"incarnation", m.Incarnation, "suspect_timeout", c.suspectTimeout())
}

// sweepMembers advances the membership timers: a suspect member whose window
// elapsed is declared dead and its link closed, a dead member is removed after
// DeadTimeout, and expired presence claims are dropped.
func (c *gossipCluster) sweepMembers(now time.Time) {
	var closeLinks []*gossipLink
	c.mu.Lock()
	for id, m := range c.members {
		switch m.State {
		case MemberSuspect:
			if m.suspectAt.IsZero() || now.Sub(m.suspectAt) < c.suspectTimeout() {
				continue
			}
			m.State = MemberDead
			m.deadAt = now.Add(c.deadTimeout())
			m.changedAt = now
			c.purgePresenceOwnerLocked(id)
			if l := c.linksByNode[id]; l != nil {
				delete(c.linksByNode, id)
				closeLinks = append(closeLinks, l)
			}
			c.log.Info("gmqx gossip member declared dead", "node", id, "addr", m.Addr,
				"incarnation", m.Incarnation, "reason", "suspect timeout")
		case MemberDead:
			if m.deadAt.IsZero() || now.Before(m.deadAt) {
				continue
			}
			delete(c.members, id)
			c.purgePresenceOwnerLocked(id)
			c.log.Info("gmqx gossip member removed", "node", id, "incarnation", m.Incarnation,
				"reason", "dead timeout")
		}
	}
	c.sweepPresenceLocked(now)
	c.sweepIndirectLocked(now)
	// addrNodes is a dial shortcut, not state: rebuild it when it grows past the
	// member bound so a node that restarts at a new address cannot keep adding
	// entries forever.
	if len(c.addrNodes) > maxGossipMembers {
		clear(c.addrNodes)
		for id, m := range c.members {
			if m.Addr != "" {
				c.addrNodes[m.Addr] = id
			}
		}
	}
	c.mu.Unlock()
	for _, l := range closeLinks {
		c.closeLink(l, "member declared dead", false)
	}
}

// probeTarget picks the member the next period probes: a suspect member first (its
// window is what decides dead or alive), then a member whose dead record still has
// a live link (it may be a false death to refute), then one random alive member.
func (c *gossipCluster) probeTarget() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.members) == 0 {
		return ""
	}
	var suspects, dead, alive []string
	for id, m := range c.members {
		if id == c.nodeID || c.probePendingLocked(id) {
			continue
		}
		switch m.State {
		case MemberSuspect:
			suspects = append(suspects, id)
		case MemberDead:
			if c.linksByNode[id] != nil {
				dead = append(dead, id)
			}
		default:
			alive = append(alive, id)
		}
	}
	sort.Strings(suspects)
	sort.Strings(dead)
	sort.Strings(alive)
	switch {
	case len(suspects) > 0:
		return suspects[0]
	case len(dead) > 0:
		return dead[0]
	case len(alive) > 0:
		return alive[randIntN(len(alive))]
	}
	return ""
}

// startProbe sends one direct ping to target and starts its probe timer. A target
// without a link is dialed instead; the ping goes out when the link is up and a
// later tick probes it.
func (c *gossipCluster) startProbe(target string) bool {
	now := c.now()
	c.mu.Lock()
	m := c.members[target]
	if m == nil {
		c.mu.Unlock()
		return false
	}
	link := c.linksByNode[target]
	if link == nil {
		addr := m.Addr
		c.mu.Unlock()
		c.requestDial(addr, target)
		return false
	}
	if c.probePendingLocked(target) {
		c.mu.Unlock()
		return false
	}
	nonce := c.nonce.Add(1)
	c.addProbeLocked(&gossipProbe{
		target:   target,
		nonce:    nonce,
		started:  now,
		deadline: now.Add(c.pingTimeout()),
	})
	f := c.metadataFrameLocked(gossipFramePing, nonce, "")
	c.mu.Unlock()

	data, err := json.Marshal(f)
	if err != nil {
		c.removeProbe(nonce)
		return false
	}
	if !c.enqueue(link, data) {
		c.removeProbe(nonce)
		return false
	}
	return true
}

// probePendingLocked reports whether a probe of the target is already in flight.
func (c *gossipCluster) probePendingLocked(target string) bool {
	for _, p := range c.pendingProbes {
		if p.target == target {
			return true
		}
	}
	return false
}

// addProbeLocked inserts a probe, keeping both the table and its insertion order
// slice bounded. Completed probes are pruned from the front here, so the slice is
// at most one probe interval of insertions plus the table bound.
func (c *gossipCluster) addProbeLocked(p *gossipProbe) {
	for len(c.probeOrder) > 0 {
		if _, ok := c.pendingProbes[c.probeOrder[0]]; ok && len(c.pendingProbes) < maxGossipProbes {
			break
		}
		oldest := c.probeOrder[0]
		c.probeOrder = c.probeOrder[1:]
		delete(c.pendingProbes, oldest)
	}
	c.pendingProbes[p.nonce] = p
	c.probeOrder = append(c.probeOrder, p.nonce)
}

func (c *gossipCluster) removeProbe(nonce uint64) {
	c.mu.Lock()
	delete(c.pendingProbes, nonce)
	c.mu.Unlock()
}

// expireProbes advances the probe state machine. A direct probe that timed out
// turns into k indirect probes through other members (the SWIM ping-req); an
// indirect round that times out marks the target suspect. The suspect window then
// belongs to sweepMembers.
func (c *gossipCluster) expireProbes(now time.Time) {
	type request struct {
		peer   string
		target string
		nonce  uint64
	}
	var requests []request
	var suspects []string
	c.mu.Lock()
	for nonce, p := range c.pendingProbes {
		if now.Before(p.deadline) {
			continue
		}
		if !p.indirect {
			peers := c.indirectPeersLocked(p.target)
			if len(peers) == 0 {
				delete(c.pendingProbes, nonce)
				suspects = append(suspects, p.target)
				continue
			}
			p.indirect = true
			p.deadline = now.Add(c.pingTimeout())
			for _, peer := range peers {
				requests = append(requests, request{peer: peer, target: p.target, nonce: p.nonce})
			}
			continue
		}
		delete(c.pendingProbes, nonce)
		suspects = append(suspects, p.target)
	}
	for _, id := range suspects {
		c.markSuspectLocked(id, "probe timeout", now)
	}
	c.mu.Unlock()

	for _, r := range requests {
		f := c.metadataFrame(gossipFramePingReq, r.nonce, r.target)
		c.sendFrameToNode(r.peer, f)
	}
	for _, id := range suspects {
		// A member that is already dead and still has a link did not refute its own
		// death record: close the link instead of probing it forever. Otherwise
		// probe again with the suspicion record in the delta, so a node that only
		// missed one ack refutes immediately instead of waiting for the next random
		// turn.
		c.mu.Lock()
		m := c.members[id]
		dead := m != nil && m.State == MemberDead
		var link *gossipLink
		if dead {
			link = c.linksByNode[id]
			if link != nil {
				delete(c.linksByNode, id)
			}
		}
		c.mu.Unlock()
		if link != nil {
			c.closeLink(link, "dead member did not answer", false)
		}
		if !dead {
			c.startProbe(id)
		}
	}
}

// indirectPeersLocked picks the up to indirectProbeFanout other members that can
// probe target on this node's behalf.
func (c *gossipCluster) indirectPeersLocked(target string) []string {
	out := make([]string, 0, len(c.members))
	for id, m := range c.members {
		if id == "" || id == c.nodeID || id == target || m.State == MemberDead {
			continue
		}
		if c.linksByNode[id] == nil {
			continue
		}
		out = append(out, id)
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	out = rotateStrings(out, randIntN(len(out)))
	if len(out) > indirectProbeFanout {
		out = out[:indirectProbeFanout]
	}
	return out
}

// handleAck completes a probe. An ack addressed to this node completes the direct
// probe; an ack this node asked another member for is forwarded back to the member
// that asked for the indirect probe. Either way the direct contact already refreshed
// the sender through observePeerFrame.
func (c *gossipCluster) handleAck(f gossipFrame) {
	now := c.now()
	c.mu.Lock()
	if ind := c.indirect[f.Nonce]; ind != nil {
		delete(c.indirect, f.Nonce)
		frame := c.metadataFrameLocked(gossipFrameAck, ind.requestNonce, ind.target)
		requester := ind.requester
		c.mu.Unlock()
		c.sendFrameToNode(requester, frame)
		return
	}
	p := c.pendingProbes[f.Nonce]
	if p == nil {
		c.mu.Unlock()
		return
	}
	if p.indirect && f.Target != p.target {
		c.mu.Unlock()
		return
	}
	delete(c.pendingProbes, f.Nonce)
	if m := c.members[p.target]; m != nil && m.State != MemberAlive {
		m.State = MemberAlive
		m.suspectAt, m.deadAt = time.Time{}, time.Time{}
		m.changedAt = now
		c.log.Debug("gmqx gossip member answered a probe", "node", p.target,
			"incarnation", m.Incarnation, "indirect", p.indirect)
	}
	c.mu.Unlock()
}

// handlePingReq performs one indirect probe on request: it pings the target and
// remembers the requester and its nonce, so the target's ack can be relayed back as
// an ack of the original probe.
func (c *gossipCluster) handlePingReq(f gossipFrame) {
	target := f.Target
	if target == "" || target == c.nodeID {
		return
	}
	now := c.now()
	c.mu.Lock()
	m := c.members[target]
	if m == nil {
		c.mu.Unlock()
		return
	}
	link := c.linksByNode[target]
	if link == nil {
		addr := m.Addr
		c.mu.Unlock()
		c.requestDial(addr, target)
		return
	}
	if len(c.indirect) >= maxIndirectPings {
		c.mu.Unlock()
		c.recordDrop("cluster_backpressure", "indirect probe table full")
		return
	}
	nonce := c.nonce.Add(1)
	c.indirect[nonce] = &gossipIndirect{
		requester:    f.NodeID,
		target:       target,
		requestNonce: f.Nonce,
		deadline:     now.Add(c.pingTimeout()),
	}
	frame := c.metadataFrameLocked(gossipFramePing, nonce, "")
	c.mu.Unlock()

	data, err := json.Marshal(frame)
	if err != nil {
		c.mu.Lock()
		delete(c.indirect, nonce)
		c.mu.Unlock()
		return
	}
	if !c.enqueue(link, data) {
		c.mu.Lock()
		delete(c.indirect, nonce)
		c.mu.Unlock()
	}
}

// sweepIndirectLocked drops indirect probes whose relay window elapsed.
func (c *gossipCluster) sweepIndirectLocked(now time.Time) {
	for nonce, ind := range c.indirect {
		if now.Before(ind.deadline) {
			continue
		}
		delete(c.indirect, nonce)
	}
}
