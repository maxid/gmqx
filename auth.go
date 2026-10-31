package gmqx

// MQTT 5 enhanced authentication (AUTH). The broker owns the packet flow — the
// AUTH 0x18 challenges, the CONNACK or AUTH 0x00 at the end, the deadlines and
// the step limit — and an AuthExchange only decides what the next step is. The
// packet flow runs inline with the read loop on the connection's own goroutine, so
// no step of an exchange is ever handled concurrently with a PUBLISH, a SUBSCRIBE
// or a second AUTH of the same client.
//
// The state machine is one loop over the step an exchange returned:
//
//	Begin ──> AuthSucceeded ──> CONNACK 0x00 (CONNECT) / AUTH 0x00 (re-auth)
//	  │
//	  ├──> AuthFailed ──> CONNACK / DISCONNECT with the reason code, close
//	  │
//	  └──> AuthContinue ──> AUTH 0x18 (method, data)
//	                        client AUTH 0x18 ──> Continue ──> back to the verdicts above
//
// A Continue that asks for yet another challenge after MaxAuthSteps challenges is
// refused with 0x87 instead of looping.
//
// The same loop serves the CONNECT phase and a re-authentication; only the packet
// that ends it differs.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// MaxAuthSteps is the number of AUTH challenges one exchange may run. An exchange
// that still asks to continue after this many challenges is refused with CONNACK
// 0x87 instead of being allowed to loop forever; the limit bounds both how long one
// connection may hold the CONNECT phase and how many round trips a
// re-authentication may cost (Metrics.RecordDroppedMessage("auth_steps")).
//
// The number is a documented constant rather than a Config knob because no
// deployment needs a longer exchange: a SASL-style mechanism (SCRAM, a challenge
// with a proof, a token refresh) finishes in two or three steps.
const MaxAuthSteps = 8

// MQTT 5 AUTH reason codes (§3.15.2.1) the broker itself writes.
const (
	// authReasonSuccess ends a re-authentication that succeeded.
	authReasonSuccess byte = 0x00
	// authReasonContinue asks the client for the next step of the exchange.
	authReasonContinue byte = 0x18
	// authReasonReauthenticate starts a fresh exchange on a live connection.
	authReasonReauthenticate byte = 0x19
)

// errAuthDenied reports an exchange that refused the connection. The reason code
// the client is told lives in the authReject that wraps it.
var errAuthDenied = errors.New("gmqx: authentication exchange refused the connection")

// authReject is an exchange-level refusal that still has a reason code to tell the
// client: the CONNACK code during CONNECT, the DISCONNECT code on a live
// connection. Any other error from an exchange means the connection cannot be
// answered at all (the socket failed, the client stalled) and is closed without a
// packet.
type authReject struct {
	code  byte
	cause error
}

func (e authReject) Error() string {
	if e.cause == nil {
		return fmt.Sprintf("gmqx: authentication refused with reason code %#x", e.code)
	}
	return fmt.Sprintf("%v (reason code %#x)", e.cause, e.code)
}

func (e authReject) Unwrap() error { return e.cause }

// rejectAuth builds the error of a refused exchange step. code 0 selects 0x87,
// the code MQTT 5 gives "the credentials or the method were not accepted".
func rejectAuth(code byte, cause error) error {
	if code == 0 {
		code = ReasonNotAuthorized
	}
	if cause == nil {
		cause = errAuthDenied
	}
	return authReject{code: code, cause: cause}
}

// authExchange returns the exchange of method, or nil when this broker does not
// support the method. Nil is the answer for every method when
// Config.Authenticator is unset, for a method outside a non-empty
// Config.AuthMethods, and for a factory that declines the method or returns an
// exchange that reports a different one.
func (b *Broker) authExchange(method string) AuthExchange {
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

// runAuth drives one complete exchange and reports the step that succeeded, or an
// error:
//
//   - an authReject error carries the reason code the connection must be refused
//     with (CONNACK during CONNECT, DISCONNECT on a live connection);
//   - any other error is a transport or protocol failure the connection cannot be
//     answered for; the caller closes it without a packet.
//
// A step that fails on its own is turned into an authReject carrying its reason
// code, and an exchange call that returns an error is a failed authentication
// (0x87) rather than an admitted client: an implementation that cannot answer is
// never taken as a success.
func (c *client) runAuth(exchange AuthExchange, info ConnectInfo) (AuthStep, error) {
	method := exchange.Method()
	step, err := c.callAuth(method, func(ctx context.Context) (AuthStep, error) {
		return exchange.Begin(ctx, info)
	})
	if err != nil {
		return AuthStep{}, rejectAuth(ReasonNotAuthorized, err)
	}
	for challenges := 0; ; challenges++ {
		switch step.Status {
		case AuthSucceeded:
			return step, nil
		case AuthFailed:
			return AuthStep{}, rejectAuth(step.ReasonCode, errAuthDenied)
		case AuthContinue:
			if challenges >= MaxAuthSteps {
				c.warn("gmqx authentication exchange exceeded its step budget",
					"client", info.ClientID, "method", method, "steps", MaxAuthSteps)
				c.broker.metrics.RecordDroppedMessage("auth_steps")
				return AuthStep{}, rejectAuth(ReasonNotAuthorized,
					fmt.Errorf("%w: more than %d challenges", errAuthDenied, MaxAuthSteps))
			}
			if err := c.sendAuth(authReasonContinue, method, step.Data); err != nil {
				return AuthStep{}, err
			}
			data, err := c.readAuthAnswer(method)
			if err != nil {
				return AuthStep{}, err
			}
			step, err = c.callAuth(method, func(ctx context.Context) (AuthStep, error) {
				return exchange.Continue(ctx, data)
			})
			if err != nil {
				return AuthStep{}, rejectAuth(ReasonNotAuthorized, err)
			}
		default:
			c.warn("gmqx authentication exchange returned an unknown status",
				"client", info.ClientID, "method", method, "status", int(step.Status))
			return AuthStep{}, rejectAuth(ReasonNotAuthorized,
				fmt.Errorf("%w: unknown step status %d", errAuthDenied, step.Status))
		}
	}
}

// callAuth runs one call into an AuthExchange under Config.AuthTimeout, exactly as
// Broker.authenticate bounds an AuthManager call: the exchange receives a context
// carrying the deadline, an expired call is a failed authentication and never a
// success, and a non-positive AuthTimeout calls it synchronously (the exchange then
// has to honour its context itself).
func (c *client) callAuth(method string, call func(context.Context) (AuthStep, error)) (AuthStep, error) {
	timeout := c.broker.cfg.AuthTimeout
	if timeout <= 0 {
		return call(c.context())
	}
	ctx, cancel := context.WithTimeout(c.context(), timeout)
	defer cancel()
	type result struct {
		step AuthStep
		err  error
	}
	done := make(chan result, 1)
	go func() {
		step, err := call(ctx)
		done <- result{step: step, err: err}
	}()
	select {
	case res := <-done:
		return res.step, res.err
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			c.warn("gmqx authentication exchange deadline exceeded",
				"client", c.id, "method", method, "timeout", timeout)
			c.broker.metrics.RecordDroppedMessage("auth_timeout")
			return AuthStep{}, fmt.Errorf("%w: authentication exchange timeout after %v", ErrUnauthorized, timeout)
		}
		return AuthStep{}, fmt.Errorf("%w: connection closed during authentication", ErrUnauthorized)
	}
}

// sendAuth writes one AUTH packet of the exchange. During the CONNECT phase the
// writer goroutine does not exist yet, so the packet is written synchronously;
// after it the packet joins the connection's queue like every other one.
func (c *client) sendAuth(reason byte, method string, data []byte) error {
	props := mqtt.Properties{AuthenticationMethod: method}
	if len(data) > 0 {
		props.AuthenticationData = data
	}
	return c.send(&mqtt.Auth{MessageHeader: c.header(), ReasonCode: reason, Properties: props})
}

// readAuthAnswer reads the client's answer to a server challenge. Only an AUTH
// with reason 0x18 (Continue authentication) is an answer, and it may name the
// negotiated method or none at all; anything else — another packet, AUTH 0x19 in
// the middle of an exchange, or a different method — is a Protocol Error
// (MQTT 5 §3.15.2, §4.12.1), answered with 0x82.
func (c *client) readAuthAnswer(method string) ([]byte, error) {
	msg, err := c.readMessage()
	if err != nil {
		return nil, err
	}
	m, ok := msg.(*mqtt.Auth)
	if !ok {
		return nil, rejectAuth(ReasonProtocolError,
			fmt.Errorf("%w: expected AUTH, got %T", ErrMalformedPacket, msg))
	}
	if m.ReasonCode != authReasonContinue {
		return nil, rejectAuth(ReasonProtocolError,
			fmt.Errorf("%w: AUTH reason %#x inside an exchange, want %#x",
				ErrMalformedPacket, m.ReasonCode, authReasonContinue))
	}
	if got := m.Properties.AuthenticationMethod; got != "" && got != method {
		return nil, rejectAuth(ReasonProtocolError,
			fmt.Errorf("%w: AUTH names method %q inside the %q exchange", ErrMalformedPacket, got, method))
	}
	return m.Properties.AuthenticationData, nil
}

// mergeAuthIdentity folds the identity a successful CONNECT exchange established
// into the ConnectInfo the connection is registered with. Only the two identity
// fields can be replaced: an AUTH packet carries nothing else, so every other field
// stays the CONNECT's. An empty field keeps what the client sent, which is what
// lets an exchange authenticate a connection without also renaming it.
func mergeAuthIdentity(info ConnectInfo, identity *ConnectInfo) ConnectInfo {
	if identity == nil {
		return info
	}
	if identity.Username != "" {
		info.Username = identity.Username
	}
	if identity.ClientID != "" {
		info.ClientID = identity.ClientID
	}
	return info
}

// updateAuthIdentity applies the identity of a successful re-authentication to the
// live connection. Only the Username can change: the ClientID is the session key of
// a registered connection and renaming it mid-session would move the session, which
// a re-authentication must not do (MQTT 5 §4.12.1 keeps the session state
// untouched). ConnInfo carries no username, so the connection context is unchanged.
func (c *client) updateAuthIdentity(identity *ConnectInfo) {
	if identity == nil || identity.Username == "" {
		return
	}
	c.mu.Lock()
	c.username = identity.Username
	c.info.Username = identity.Username
	c.mu.Unlock()
}

// handleAuth handles an AUTH packet on an established connection: either the start
// of a re-authentication (reason 0x19) or a Protocol Error.
//
// It runs on the connection's read goroutine and reads the following AUTH packets
// itself, which is what keeps the exchange in order with the client's other
// packets: a PUBLISH the client sends behind its AUTH is read only after the
// exchange is over. The session is not touched — a successful re-authentication
// keeps the subscriptions, the offline queue and the inflight exchanges — and a
// failed one ends the connection with a DISCONNECT.
func (c *client) handleAuth(m *mqtt.Auth) {
	method := c.authMethod
	if method == "" {
		// Enhanced authentication was not negotiated: the CONNECT carried no
		// Authentication Method, so an AUTH packet is a Protocol Error (MQTT 5
		// §3.15.2) rather than something to ignore.
		c.protocolError(ReasonProtocolError)
		return
	}
	if m.ReasonCode != authReasonReauthenticate {
		// A server-initiated challenge is not out; only the client starts a
		// re-authentication, and only with 0x19.
		c.protocolError(ReasonProtocolError)
		return
	}
	if got := m.Properties.AuthenticationMethod; got != "" && got != method {
		c.protocolError(ReasonProtocolError)
		return
	}
	exchange := c.broker.authExchange(method)
	if exchange == nil {
		// The method was negotiated but the broker no longer builds an exchange for
		// it; that is a "bad authentication method" for this connection.
		c.protocolError(ReasonBadAuthMethod)
		return
	}
	c.mu.Lock()
	info := c.info
	c.mu.Unlock()
	info.AuthMethod = method
	info.AuthData = m.Properties.AuthenticationData
	// The re-authentication has no CONNECT deadline to live inside any more, so it
	// gets the auth timeout as the read deadline of the whole exchange: a client
	// that stalls on a challenge is disconnected instead of blocking its read loop
	// (and therefore its keep-alive) forever. The loop re-arms its own deadline on
	// the next packet.
	if timeout := c.broker.cfg.AuthTimeout; timeout > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	}
	step, err := c.runAuth(exchange, info)
	if err != nil {
		var refused authReject
		if errors.As(err, &refused) {
			c.warn("gmqx re-authentication refused", "client", c.id, "method", method,
				"reason", refused.code, "err", err)
			c.protocolError(refused.code)
			return
		}
		// The client stalled or the connection failed: the exchange cannot be
		// completed, so the connection ends like any other lost one.
		c.warn("gmqx re-authentication failed", "client", c.id, "method", method, "err", err)
		c.mu.Lock()
		c.lost = true
		c.mu.Unlock()
		c.close(true)
		return
	}
	c.updateAuthIdentity(step.Info)
	if err := c.sendAuth(authReasonSuccess, method, step.Data); err != nil {
		c.close(true)
	}
}
