package client

import (
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// reconnect runs the backoff loop of the reconnect policy. It returns the new
// connection, or nil with the last error when the policy gave up or Close ended
// the client.
//
// The attempt count is consecutive failures since the last successful connection:
// a connection that worked and later failed starts again at attempt 1. Before each
// attempt the client waits the exponential delay plus jitter, so a server that is
// still down is not hammered and a cluster of clients does not reconnect in step.
func (c *Client) reconnect(cause error) (*connState, error) {
	policy := c.cfg.Reconnect
	lastErr := cause
	for attempt := 1; ; attempt++ {
		if policy.MaxAttempts > 0 && attempt > policy.MaxAttempts {
			return nil, lastErr
		}
		if !c.backoff(policy.delay(attempt)) {
			return nil, ErrClosed
		}
		cs, _, err := c.establish(c.ctx)
		if err == nil {
			return cs, nil
		}
		lastErr = err
	}
}

// backoff sleeps before a reconnect attempt. It reports false when Close ended the
// wait.
func (c *Client) backoff(delay time.Duration) bool {
	if c.closed.Load() {
		return false
	}
	if delay <= 0 {
		select {
		case <-c.closedCh:
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return !c.closed.Load()
	case <-c.closedCh:
		return false
	}
}

// restore re-arms the session state after a connection was established, before
// the client is reported connected: the unconfirmed QoS 1/2 exchanges are
// retransmitted in their original order (DUP for a PUBLISH, a repeated PUBREL for
// an exchange awaiting PUBCOMP) and, when the server did not keep a session, the
// remembered subscriptions are registered again.
//
// The order matters: a new publish cannot be issued before connected is set, so
// the retransmissions are always ahead of new traffic, as §4.4 requires.
func (c *Client) restore(cs *connState, sessionPresent bool) {
	c.mu.Lock()
	// Topic aliases are hop-local: the mapping of a previous connection means
	// nothing on this one.
	c.aliases = make(map[uint16]string)
	if !sessionPresent {
		// The server started a new session, so it remembered no inbound QoS 2
		// exchange either.
		c.inboundQoS2 = make(map[uint16]struct{})
	}
	now := time.Now()
	var resend []mqtt.Message
	for _, id := range c.order {
		entry := c.inflight[id]
		if entry == nil {
			continue
		}
		entry.attempts = 0
		if c.cfg.MaxRetries >= 0 {
			entry.next = now.Add(c.cfg.RetryInterval)
		} else {
			entry.next = time.Time{}
		}
		if entry.pubrel {
			resend = append(resend, &mqtt.Pubrel{MessageHeader: c.header(), PacketID: id})
			continue
		}
		dup := entry.msg
		dup.Dup = true
		resend = append(resend, &dup)
	}
	var subs []Subscription
	if !sessionPresent {
		for _, filter := range c.subOrder {
			if sub, ok := c.subs[filter]; ok {
				subs = append(subs, sub)
			}
		}
	}
	c.mu.Unlock()

	for _, packet := range resend {
		if err := c.sendControl(cs, packet); err != nil {
			return
		}
	}
	c.sendRestoredSubscriptions(cs, subs)
	c.wakeRetry()
}
