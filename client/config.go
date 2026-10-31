package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand/v2"
	"net"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// Defaults applied to a Config that leaves a field at its zero value. They are
// bounded on purpose: an unbounded writer queue turns a stalled peer into
// unbounded client memory, and an unbounded inflight window turns a slow broker
// into the same.
const (
	defaultConnectTimeout   = 10 * time.Second
	defaultWriteTimeout     = 10 * time.Second
	defaultMaxInflight      = 64
	defaultOutboundQueue    = 256
	defaultRetryInterval    = 5 * time.Second
	defaultMaxRetries       = 5
	defaultInitialDelay     = time.Second
	defaultMaxDelay         = 30 * time.Second
	defaultJitter           = 0.2
	maxReceiveMaximum       = 65535
	defaultMaxInboundPacket = 8 << 20 // 8 MiB memory guard when MaximumPacketSize is 0
)

// Config configures a Client.
type Config struct {
	// Addr is the server address in "host:port" form. It is passed to Dialer
	// together with the "tcp" network.
	Addr string
	// ClientID is the MQTT client identifier. It must be non-empty unless
	// CleanStart is set (the server then assigns one, which a reconnect reuses).
	ClientID string
	// Username and Password authenticate the CONNECT. A password requires a
	// user name, as the protocol does.
	Username string
	Password []byte
	// KeepAlive is the interval of the client's keepalive: a PINGREQ is sent
	// every KeepAlive and a PINGRESP that does not arrive within one further
	// interval marks the connection dead. Zero disables keepalive. The value is
	// rounded up to whole seconds on the wire (the CONNECT field is in seconds);
	// the client's own timers use the exact duration, which is what makes
	// millisecond intervals usable in tests. The server's Server Keep Alive
	// property, when present, overrides it for that connection.
	KeepAlive time.Duration
	// CleanStart is the MQTT 3.1.1 Clean Session flag and the MQTT 5 Clean Start
	// flag. Every CONNECT of this client carries it; a clean connection discards
	// the server-side session, so a reconnect starts a new one and the client
	// resubscribes.
	CleanStart bool
	// SessionExpiry is the MQTT 5 Session Expiry Interval in seconds. Zero means
	// the session ends with the connection. It is ignored on MQTT 3.1.1, which
	// keys session persistence off CleanSession.
	SessionExpiry uint32
	// Version is the protocol level to speak: mqtt.Version311 (the zero value)
	// or mqtt.Version5.
	Version byte
	// TLS, when non-nil, wraps the connection in TLS after Dialer returns and
	// completes the handshake before CONNECT is written.
	TLS *tls.Config
	// Dialer opens the transport connection. Nil selects net.Dialer. It is called
	// for the initial connection and for every reconnect attempt, with a context
	// that Close cancels and ConnectTimeout bounds.
	Dialer func(ctx context.Context, network, addr string) (net.Conn, error)
	// ConnectTimeout bounds the whole opening handshake (dial, TLS handshake,
	// CONNECT/CONNACK): a socket that does not finish it is closed. A
	// non-positive value selects 10 s; there is no unlimited setting.
	ConnectTimeout time.Duration
	// WriteTimeout is the write deadline of one packet. A socket that cannot take
	// a packet within it is treated as failed, so a wedged peer cannot hold the
	// writer (or Close) forever. A non-positive value selects 10 s.
	WriteTimeout time.Duration
	// MaxInflight bounds the QoS 1 and QoS 2 exchanges that may be unconfirmed at
	// once (a QoS 1 PUBLISH until PUBACK, a QoS 2 PUBLISH until PUBCOMP). A
	// publish that would exceed the window waits for a slot; it is never dropped
	// or reordered. Zero selects 64 and a negative value means unlimited, capped
	// by the server's Receive Maximum in any case.
	MaxInflight int
	// ReceiveMaximum is the MQTT 5 Receive Maximum sent in CONNECT: how many
	// unacknowledged QoS 1/2 PUBLISHes this client accepts from the server. Zero
	// omits the property (the protocol default, 65535).
	ReceiveMaximum uint16
	// MaximumPacketSize is the MQTT 5 Maximum Packet Size sent in CONNECT: the
	// largest packet this client accepts. Zero omits the property on the wire and
	// leaves the client's decoder at its 8 MiB memory guard.
	MaximumPacketSize uint32
	// Reconnect is the backoff policy used when an established connection is
	// lost. Close never reconnects.
	Reconnect ReconnectPolicy
	// OnMessage receives every inbound PUBLISH, in the order the server sent it.
	// It runs on the client's delivery goroutine, never while a client method
	// holds a lock, and it must return promptly: a slow callback backpressures the
	// connection (the server's TCP window and keepalive then decide what happens).
	// It may call Publish and Subscribe, but it must not call Close. A callback
	// that blocks for long — including a QoS 1/2 publish made from the callback
	// while the delivery queue is already full — stalls the connection, so a
	// callback that has real work to do should hand it to its own goroutine. Nil
	// discards inbound messages after acknowledging them.
	OnMessage func(*Message)
	// OnConnect is called after the opening CONNECT of the first connection and
	// of every reconnect, with the Session Present flag of the CONNACK. On the
	// first connection it runs before Dial returns. It may call Publish and
	// Subscribe; it must not call Close. Nil skips the report.
	OnConnect func(sessionPresent bool)
	// OnDisconnect is called once for each established connection that ends
	// unexpectedly — a transport error, a keepalive timeout or a server
	// DISCONNECT — with the reason, before a reconnect is attempted. A
	// user-initiated Close does not call it. It must not call Close. Nil skips the
	// report.
	OnDisconnect func(error)

	// Will is the last will this client asks the server to publish when its
	// connection dies without a DISCONNECT. It travels in every CONNECT, so it is
	// re-armed by a reconnect. Nil means no will.
	Will *Will
	// OutboundQueueSize bounds the packets queued for the writer and the messages
	// queued for the delivery goroutine. A full queue blocks its producer (see the
	// package documentation); it never drops a packet. Zero selects 256.
	OutboundQueueSize int
	// RetryInterval is the base interval of the outbound QoS 1/2 retransmission
	// timer: the first retransmission is sent after one interval and every further
	// one after twice the previous wait. A non-positive value selects 5 s.
	RetryInterval time.Duration
	// MaxRetries bounds the retransmissions of one exchange; the exchange then
	// fails with ErrRetryLimit and its Publish returns that error. Zero selects 5
	// and a negative value disables retransmission (the exchange then waits for
	// its acknowledgement until the caller's context or Close ends the wait).
	MaxRetries int
}

// ReconnectPolicy is the exponential backoff of a reconnect attempt.
//
// The delay before reconnection attempt n (1-based) is
// InitialDelay * 2^(n-1), capped at MaxDelay, multiplied by a random factor in
// [1-Jitter, 1+Jitter]. MaxAttempts bounds the consecutive attempts that may
// fail; zero means unlimited and a successful connection resets the count.
type ReconnectPolicy struct {
	// Enabled turns reconnection on. Close still ends the client for good and
	// never reconnects.
	Enabled bool
	// InitialDelay is the delay before the first reconnect attempt. A
	// non-positive value selects 1 s.
	InitialDelay time.Duration
	// MaxDelay caps the exponential growth. A non-positive value selects 30 s; a
	// value below InitialDelay is raised to it.
	MaxDelay time.Duration
	// MaxAttempts bounds the number of consecutive failed reconnect attempts.
	// Zero or a negative value means unlimited; a successful connection resets
	// the count.
	MaxAttempts int
	// Jitter is the fraction of the delay that is randomised, in [0, 1]. Zero
	// selects 0.2; a value outside [0, 1] is a configuration error.
	Jitter float64
}

// withDefaults returns cfg with every documented zero default applied. It does
// not validate: validate runs afterwards.
func (c Config) withDefaults() Config {
	if c.Version == 0 {
		c.Version = mqtt.Version311
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = defaultConnectTimeout
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.MaxInflight == 0 {
		c.MaxInflight = defaultMaxInflight
	}
	if c.OutboundQueueSize <= 0 {
		c.OutboundQueueSize = defaultOutboundQueue
	}
	if c.RetryInterval <= 0 {
		c.RetryInterval = defaultRetryInterval
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = defaultMaxRetries
	}
	if c.Reconnect.Enabled {
		if c.Reconnect.InitialDelay <= 0 {
			c.Reconnect.InitialDelay = defaultInitialDelay
		}
		if c.Reconnect.MaxDelay <= 0 {
			c.Reconnect.MaxDelay = defaultMaxDelay
		}
		if c.Reconnect.MaxDelay < c.Reconnect.InitialDelay {
			c.Reconnect.MaxDelay = c.Reconnect.InitialDelay
		}
		if c.Reconnect.Jitter == 0 {
			c.Reconnect.Jitter = defaultJitter
		}
		if c.Reconnect.MaxAttempts < 0 {
			c.Reconnect.MaxAttempts = 0
		}
	}
	return c
}

// validate reports a configuration a Client cannot be built from.
func (c Config) validate() error {
	if c.Addr == "" {
		return fmt.Errorf("%w: Addr is required", ErrInvalidConfig)
	}
	if c.Version != mqtt.Version311 && c.Version != mqtt.Version5 {
		return fmt.Errorf("%w: protocol version %d is not MQTT 3.1.1 or 5", ErrInvalidConfig, c.Version)
	}
	if len(c.Password) > 0 && c.Username == "" {
		return fmt.Errorf("%w: a password requires a user name", ErrInvalidConfig)
	}
	if c.ClientID == "" && !c.CleanStart {
		return fmt.Errorf("%w: an empty ClientID requires CleanStart", ErrInvalidConfig)
	}
	if c.MaxInflight > maxReceiveMaximum {
		return fmt.Errorf("%w: MaxInflight %d exceeds %d", ErrInvalidConfig, c.MaxInflight, maxReceiveMaximum)
	}
	if c.Reconnect.Enabled && (c.Reconnect.Jitter < 0 || c.Reconnect.Jitter > 1) {
		return fmt.Errorf("%w: reconnect jitter %v is outside [0,1]", ErrInvalidConfig, c.Reconnect.Jitter)
	}
	if c.Will != nil {
		if c.Will.Topic == "" {
			return fmt.Errorf("%w: the will topic is empty", ErrInvalidConfig)
		}
		if c.Will.QoS > 2 {
			return fmt.Errorf("%w: will QoS %d is reserved", ErrInvalidConfig, c.Will.QoS)
		}
	}
	return nil
}

// delay is the wait before reconnect attempt number attempt (1-based).
func (p ReconnectPolicy) delay(attempt int) time.Duration {
	initial, maxDelay := p.InitialDelay, p.MaxDelay
	if initial <= 0 {
		initial = defaultInitialDelay
	}
	if maxDelay <= 0 {
		maxDelay = defaultMaxDelay
	}
	if maxDelay < initial {
		maxDelay = initial
	}
	d := initial
	for i := 1; i < attempt && d < maxDelay; i++ {
		if d > maxDelay/2 {
			d = maxDelay
			break
		}
		d *= 2
	}
	if d > maxDelay {
		d = maxDelay
	}
	jitter := p.Jitter
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	if jitter > 0 {
		// Uniform in [1-jitter, 1+jitter].
		factor := 1 - jitter + 2*jitter*rand.Float64()
		d = time.Duration(float64(d) * factor)
	}
	if d < 0 {
		d = 0
	}
	return d
}
