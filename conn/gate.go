package conn

// Admission: the optional gate a host may put in front of a connection
//
// A broker that enforces shared limits (github.com/maxid/glimit) implements Gate;
// a broker that does not, and every test host, keeps working unchanged. The
// connection asks the gate at two points and owns the protocol answer:
//
//   - every inbound control packet is charged once, so a packet-rate limit is
//     decided before the packet has any effect; a denial ends the connection
//     (MQTT 5 gets a DISCONNECT 0x97).
//   - every inbound PUBLISH is charged its wire size, and a denial refuses that
//     one publication the way an exhausted receive maximum does — PUBREC 0x97 on
//     MQTT 5 QoS 2, no acknowledgement on QoS 1, silent drop on QoS 0. The stream
//     is never cut for a byte budget: see byte_rate.go for why a hole in an MQTT
//     stream is a corruption bug rather than a limit.

import (
	"errors"
	"fmt"

	"github.com/maxid/glimit"
)

// ErrLimit reports traffic a Gate refused. The connection wraps it with the
// limiter's reason; errors.Is matches either form.
var ErrLimit = errors.New("gmqx: limit exceeded")

// LimitDenied is a refusal with the limiter's reason, so the connection can pick
// the protocol answer (a CONNACK code, a DISCONNECT 0x97, a refused PUBLISH).
type LimitDenied struct {
	Reason glimit.Reason
}

// Error implements error.
func (e LimitDenied) Error() string { return fmt.Sprintf("%v: %s", ErrLimit, e.Reason) }

// Unwrap reports ErrLimit, so errors.Is(err, ErrLimit) is true for every denial.
func (e LimitDenied) Unwrap() error { return ErrLimit }

// Gate is implemented by a host that admits this connection's traffic through a
// shared limiter. Both methods return a LimitDenied when they refuse.
//
// AdmitInbound charges one inbound control packet. AdmitPublish charges one
// inbound publication's wire size in bytes (topic and payload, plus the fixed
// header); it is the message-sized budget, not the socket transfer the byte-rate
// shaper measures.
type Gate interface {
	AdmitInbound(c *Client) error
	AdmitPublish(c *Client, size int) error
}
