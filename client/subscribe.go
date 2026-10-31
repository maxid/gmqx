package client

import (
	"context"
	"fmt"

	"github.com/maxid/gnety/codec/mqtt"
)

// request is one SUBSCRIBE or UNSUBSCRIBE waiting for its acknowledgement. A
// request whose done channel is nil was sent to restore a subscription after a
// reconnect: nobody waits for it, the SUBACK only releases the packet id.
type request struct {
	done chan requestResult
}

// requestResult is what the reader hands a waiting request.
type requestResult struct {
	codes  []byte
	reason string
	err    error
}

// Subscribe sends one or more topic filters and returns the granted QoS/reason
// codes in the order of subs: a granted QoS (0, 1 or 2) or a failure code
// (0x80 on MQTT 3.1.1, any code at or above 0x80 on MQTT 5). It waits for the
// SUBACK. Subscriptions granted by the server are remembered, so a later
// reconnect can restore them.
//
// A subscription carries a Subscription Identifier, which is a SUBSCRIBE packet
// property: filters with different identifiers are split into several SUBSCRIBE
// packets (still in the order of this call), and the returned codes follow the
// original order.
func (c *Client) Subscribe(ctx context.Context, subs ...Subscription) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(subs) == 0 {
		return nil, fmt.Errorf("%w: Subscribe needs at least one subscription", ErrInvalidConfig)
	}
	for _, s := range subs {
		if s.Filter == "" {
			return nil, fmt.Errorf("%w: empty subscription filter", ErrBadTopic)
		}
		if s.QoS > 2 {
			return nil, fmt.Errorf("%w: subscription QoS %d", ErrBadQoS, s.QoS)
		}
		if s.RetainHandling > 2 {
			return nil, fmt.Errorf("%w: retain handling %d is reserved", ErrInvalidConfig, s.RetainHandling)
		}
	}

	groups := groupSubscriptions(subs)
	codes := make([]byte, len(subs))
	for _, group := range groups {
		granted, err := c.subscribeGroup(ctx, group)
		if err != nil {
			return nil, err
		}
		if len(granted) != len(group.subs) {
			return nil, fmt.Errorf("%w: SUBACK carries %d codes for %d filters",
				ErrProtocol, len(granted), len(group.subs))
		}
		c.mu.Lock()
		for i, sub := range group.subs {
			codes[group.indexes[i]] = granted[i]
			if granted[i] < 0x80 {
				c.storeSubscriptionLocked(sub)
			}
		}
		c.mu.Unlock()
	}
	return codes, nil
}

// Unsubscribe removes topic filters. It waits for the UNSUBACK; on MQTT 5 a
// reason code at or above 0x80 (a filter that could not be removed) is returned
// as a *ReasonError. The filters are forgotten locally only when the server
// confirms them, so a reconnect does not resurrect a filter the client no longer
// wants.
func (c *Client) Unsubscribe(ctx context.Context, filters ...string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(filters) == 0 {
		return fmt.Errorf("%w: Unsubscribe needs at least one filter", ErrInvalidConfig)
	}
	for _, filter := range filters {
		if filter == "" {
			return fmt.Errorf("%w: empty unsubscribe filter", ErrBadTopic)
		}
	}

	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return ErrClosed
	}
	cs, connected := c.conn, c.connected
	if cs == nil || !connected {
		c.mu.Unlock()
		return ErrNotConnected
	}
	pid, err := c.allocPIDLocked()
	if err != nil {
		c.mu.Unlock()
		return err
	}
	req := &request{done: make(chan requestResult, 1)}
	c.requests[pid] = req
	c.mu.Unlock()

	packet := &mqtt.Unsubscribe{
		MessageHeader: c.header(),
		PacketID:      pid,
		Filters:       append([]string(nil), filters...),
	}
	if err := c.checkPacketSize(packet); err != nil {
		c.discardRequest(pid)
		return err
	}
	if err := c.sendOn(ctx, cs, packet); err != nil {
		c.discardRequest(pid)
		return err
	}
	res, err := c.waitRequest(ctx, pid, req)
	if err != nil {
		return err
	}

	c.mu.Lock()
	for i, filter := range filters {
		if c.cfg.Version == mqtt.Version5 && i < len(res.codes) && res.codes[i] >= 0x80 {
			continue
		}
		c.forgetSubscriptionLocked(filter)
	}
	c.mu.Unlock()
	if c.cfg.Version == mqtt.Version5 {
		for _, code := range res.codes {
			if code >= 0x80 {
				return &ReasonError{Packet: "UNSUBACK", Code: code, Reason: res.reason}
			}
		}
	}
	return nil
}

// subGroup is the set of subscriptions of one call that share a Subscription
// Identifier, and therefore one SUBSCRIBE packet.
type subGroup struct {
	id      uint32
	subs    []Subscription
	indexes []int
}

// groupSubscriptions splits a Subscribe call into one group per Subscription
// Identifier, preserving the order of first appearance and the original index of
// every filter.
func groupSubscriptions(subs []Subscription) []subGroup {
	groups := make([]subGroup, 0, 1)
	byID := make(map[uint32]int, 1)
	for i, sub := range subs {
		pos, ok := byID[sub.SubscriptionIdentifier]
		if !ok {
			pos = len(groups)
			byID[sub.SubscriptionIdentifier] = pos
			groups = append(groups, subGroup{id: sub.SubscriptionIdentifier})
		}
		groups[pos].subs = append(groups[pos].subs, sub)
		groups[pos].indexes = append(groups[pos].indexes, i)
	}
	return groups
}

// subscribeGroup sends one SUBSCRIBE packet and waits for its SUBACK.
func (c *Client) subscribeGroup(ctx context.Context, group subGroup) ([]byte, error) {
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	cs, connected := c.conn, c.connected
	if cs == nil || !connected {
		c.mu.Unlock()
		return nil, ErrNotConnected
	}
	pid, err := c.allocPIDLocked()
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	req := &request{done: make(chan requestResult, 1)}
	c.requests[pid] = req
	c.mu.Unlock()

	packet := c.subscribePacket(pid, group)
	if err := c.checkPacketSize(packet); err != nil {
		c.discardRequest(pid)
		return nil, err
	}
	if err := c.sendOn(ctx, cs, packet); err != nil {
		c.discardRequest(pid)
		return nil, err
	}
	res, err := c.waitRequest(ctx, pid, req)
	if err != nil {
		return nil, err
	}
	return res.codes, nil
}

// subscribePacket builds the SUBSCRIBE packet of one identifier group. The packet
// carries the group's Subscription Identifier and the user properties of its
// first subscription; MQTT has one property block per packet and one subscription
// identifier per packet.
func (c *Client) subscribePacket(pid uint16, group subGroup) *mqtt.Subscribe {
	packet := &mqtt.Subscribe{
		MessageHeader: c.header(),
		PacketID:      pid,
		Subscriptions: make([]mqtt.Subscription, 0, len(group.subs)),
	}
	for _, sub := range group.subs {
		packet.Subscriptions = append(packet.Subscriptions, mqtt.Subscription{
			Filter:            sub.Filter,
			QoS:               sub.QoS,
			NoLocal:           sub.NoLocal,
			RetainAsPublished: sub.RetainAsPublished,
			RetainHandling:    sub.RetainHandling,
		})
	}
	if c.cfg.Version == mqtt.Version5 {
		if group.id != 0 {
			packet.Properties.SubscriptionIdentifiers = []uint32{group.id}
		}
		if len(group.subs) > 0 {
			packet.Properties.UserProperties = group.subs[0].Properties.UserProperties
		}
	}
	return packet
}

// waitRequest waits for a SUBACK/UNSUBACK, the caller's context or Close. A
// context that ends removes the request so a late acknowledgement is ignored.
func (c *Client) waitRequest(ctx context.Context, pid uint16, req *request) (requestResult, error) {
	select {
	case res := <-req.done:
		return res, res.err
	case <-ctx.Done():
		c.discardRequest(pid)
		return requestResult{}, ctx.Err()
	case <-c.closedCh:
		return requestResult{}, ErrClosed
	}
}

// discardRequest forgets a pending request if the reader has not answered it yet.
func (c *Client) discardRequest(pid uint16) {
	c.mu.Lock()
	delete(c.requests, pid)
	c.mu.Unlock()
}

// handleSuback answers the matching SUBSCRIBE.
func (c *Client) handleSuback(m *mqtt.Suback) {
	c.mu.Lock()
	req, ok := c.requests[m.PacketID]
	if ok {
		delete(c.requests, m.PacketID)
	}
	c.mu.Unlock()
	if !ok || req.done == nil {
		return
	}
	req.done <- requestResult{codes: m.ReasonCodes, reason: m.Properties.ReasonString}
}

// handleUnsuback answers the matching UNSUBSCRIBE.
func (c *Client) handleUnsuback(m *mqtt.Unsuback) {
	c.mu.Lock()
	req, ok := c.requests[m.PacketID]
	if ok {
		delete(c.requests, m.PacketID)
	}
	c.mu.Unlock()
	if !ok || req.done == nil {
		return
	}
	req.done <- requestResult{codes: m.ReasonCodes, reason: m.Properties.ReasonString}
}

// storeSubscriptionLocked remembers a granted subscription for a later
// reconnect. The caller holds c.mu.
func (c *Client) storeSubscriptionLocked(sub Subscription) {
	if _, exists := c.subs[sub.Filter]; !exists {
		c.subOrder = append(c.subOrder, sub.Filter)
	}
	c.subs[sub.Filter] = sub
}

// forgetSubscriptionLocked drops a filter. The caller holds c.mu.
func (c *Client) forgetSubscriptionLocked(filter string) {
	if _, exists := c.subs[filter]; !exists {
		return
	}
	delete(c.subs, filter)
	for i, other := range c.subOrder {
		if other == filter {
			c.subOrder = append(c.subOrder[:i], c.subOrder[i+1:]...)
			return
		}
	}
}

// sendRestoredSubscriptions re-registers the remembered subscriptions after a
// reconnect that found no session. It is fire-and-forget: the SUBACKs release
// their packet ids and nobody waits for them, because the client is already
// reconnected and a failed restore is retried by the next reconnect.
func (c *Client) sendRestoredSubscriptions(cs *connState, subs []Subscription) {
	if len(subs) == 0 {
		return
	}
	for _, group := range groupSubscriptions(subs) {
		c.mu.Lock()
		pid, err := c.allocPIDLocked()
		if err == nil {
			c.requests[pid] = &request{}
		}
		c.mu.Unlock()
		if err != nil {
			return
		}
		if err := c.sendControl(cs, c.subscribePacket(pid, group)); err != nil {
			c.discardRequest(pid)
			return
		}
	}
}

// failRequests fails every pending SUBSCRIBE/UNSUBSCRIBE. It is used when the
// connection they were sent on is gone: a request is not replayed, because its
// SUBACK could not be matched to the caller's call any more.
func (c *Client) failRequests(err error) {
	c.mu.Lock()
	c.failRequestsLocked(err)
	c.mu.Unlock()
}

// failRequestsLocked is failRequests without the lock. The caller holds c.mu.
func (c *Client) failRequestsLocked(err error) {
	for pid, req := range c.requests {
		delete(c.requests, pid)
		if req.done != nil {
			req.done <- requestResult{err: err}
		}
	}
}
