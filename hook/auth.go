// Package hook is the extension contract of this broker: the types and interfaces an
// application implements to authenticate, authorize, observe or intercept traffic.
//
// It exists as its own package so the extension surface is one visible, reviewable
// directory instead of a section of a large implementation file. The broker imports it;
// it imports nothing from the broker, so an implementation cannot depend on internals it
// is not supposed to see. The root package re-exports every name here as an alias, which
// is why existing consumers keep importing the root package without a single change.
package hook

import "context"

// ConnectInfo describes an MQTT CONNECT request.
type ConnectInfo struct {
	ClientID     string
	Username     string
	Password     []byte
	KeepAlive    uint16
	CleanSession bool
	// SessionExpiry is the MQTT 5 Session Expiry Interval in seconds. It is
	// zero for MQTT 3.1.1 and for MQTT 5 clients that omit the property.
	SessionExpiry uint32
	WillTopic     string
	WillPayload   []byte
	RemoteAddr    string
	// AuthMethod is the MQTT 5 Authentication Method the CONNECT named, or ""
	// when the connection negotiates none (always "" on MQTT 3.1.1, which has no
	// property block). A non-empty value selects the enhanced-authentication path:
	// Config.Authenticator builds the AuthExchange of the connection and
	// AuthManager.Authenticate is skipped for it (see AuthExchange).
	AuthMethod string
	// AuthData is the MQTT 5 Authentication Data the CONNECT carried: the client's
	// first message of the exchange, nil when there is none. It is handed to
	// AuthExchange.Begin and means nothing to AuthManager.
	AuthData []byte
}

// AuthManager validates a connecting client.
//
// It is the authentication hook of a CONNECT that carries no MQTT 5
// Authentication Method. A connection that does negotiate a method is
// authenticated by its AuthExchange instead and this hook is not called at all
// (see AuthExchange for the precedence); AllowAll and DenyAll therefore keep
// their meaning for every other connection.
type AuthManager interface {
	Authenticate(ctx context.Context, info ConnectInfo) error
	// RequiresOffload is kept for SPI compatibility; Go runs each connection in
	// its own goroutine, so callers may ignore it.
	RequiresOffload() bool
}

// AuthStatus is the verdict of one step of an AuthExchange.
type AuthStatus int

const (
	// AuthContinue asks for another step: the broker sends AUTH 0x18 (Continue
	// authentication) carrying the step's data and waits for the client's answer,
	// which it hands to AuthExchange.Continue. It is the zero value, so a step an
	// implementation left unset keeps the exchange going instead of admitting the
	// client; the exchange is bounded by MaxAuthSteps.
	AuthContinue AuthStatus = iota
	// AuthSucceeded completes the exchange and admits the connection. Data is the
	// final Authentication Data the broker sends with the CONNACK (or with the
	// AUTH 0x00 of a re-authentication) and Info, when set, is the authenticated
	// identity.
	AuthSucceeded
	// AuthFailed refuses the connection: the broker answers with CONNACK (during
	// CONNECT) or DISCONNECT (on a live connection) carrying ReasonCode and closes
	// it. Data is ignored on a failure.
	AuthFailed
)

// AuthStep is one step of an AuthExchange: what the exchange decided and the
// Authentication Data that goes with it.
type AuthStep struct {
	// Status is what the broker does next.
	Status AuthStatus
	// Data is the Authentication Data of this step: the server's challenge when
	// Status is AuthContinue, the final data that accompanies a successful CONNACK
	// or AUTH 0x00 when it is AuthSucceeded. Empty data omits the property.
	Data []byte
	// Info, on a step that succeeds, is the identity of the authenticated
	// connection: its Username and ClientID replace the CONNECT's when they are
	// set, and an empty field keeps the value the client sent. Every other field
	// is not used, because an AUTH packet can only carry Authentication Method and
	// Authentication Data (see AuthExchange). Info on a continuing or a failing
	// step is ignored.
	Info *ConnectInfo
	// ReasonCode is the reason code of a failed step: the CONNACK reason code
	// during CONNECT, the DISCONNECT reason code on a live connection. 0 selects
	// 0x87 (Not authorized); 0x86 (Bad User Name or Password), 0x87 and 0x8C (Bad
	// authentication method) are the codes that describe an authentication
	// failure.
	ReasonCode byte
}

// AuthExchange is one MQTT 5 enhanced-authentication exchange (AUTH) of one
// connection, in the spirit of the other hooks: the broker owns the packet flow,
// the deadline and the limits, and the implementation only decides the next step.
//
// Lifetime and calls. The broker obtains an exchange from Config.Authenticator
// and drives it from the connection's read path, one call at a time: it waits for
// each step before it reads the next packet, so no step of an exchange ever runs
// concurrently with another packet of the same client:
//
//   - CONNECT: Begin receives the CONNECT's ConnectInfo — with AuthMethod and
//     AuthData filled from the authentication properties of the packet — and its
//     step is the exchange's first verdict. A step that continues makes the
//     broker send AUTH 0x18 with the step's data; each client AUTH 0x18 then calls
//     Continue with the client's Authentication Data. Success sends CONNACK 0x00
//     with the method and the step's data, failure sends CONNACK with
//     step.ReasonCode and closes.
//   - Re-authentication: an AUTH 0x19 on a live connection starts a fresh
//     exchange (a new value from Config.Authenticator, then Begin with the
//     connection's ConnectInfo and the AUTH's Authentication Data, then Continue
//     exactly as above). Success answers AUTH 0x00 and leaves the session — its
//     subscriptions, its queue and its inflight state — untouched; failure ends
//     the connection with a DISCONNECT.
//
// Bounds. One exchange runs at most MaxAuthSteps challenges, and the whole
// CONNECT exchange is inside Config.ConnectTimeout. Every call into the exchange
// is bounded by Config.AuthTimeout: it receives a context carrying the deadline
// (a positive timeout may run the call on a helper goroutine the broker waits
// for), and an expired call is a failed authentication — never a success. An
// error returned by Begin or Continue is a failed authentication answered with
// 0x87; only a returned step can name another reason code.
//
// Identity. A successful step may return a ConnectInfo; that is how an exchange
// authenticates a device under a name the client did not send. The returned
// Username and ClientID win over the CONNECT's, and because the exchange *is* the
// authentication of this connection, AuthManager.Authenticate is skipped for it
// (see AuthManager). Authorization (AclManager) still runs for every publish and
// subscribe, against the identity the exchange established.
//
// The Authentication Method a connection negotiated cannot change afterwards: an
// AUTH that names another method is a Protocol Error (DISCONNECT 0x82), and the
// broker only ever sends the negotiated method in its own AUTH and CONNACK
// packets.
type AuthExchange interface {
	// Method is the MQTT 5 Authentication Method this exchange implements. It must
	// be the method Config.Authenticator was asked for and the one the client
	// named in its CONNECT: the broker writes it into every AUTH and CONNACK packet
	// of the connection. A factory that returns an exchange for another method is
	// a misconfiguration, and the broker treats the method as unsupported (CONNACK
	// 0x8C).
	Method() string
	// Begin starts the exchange. info carries the CONNECT's identity and its
	// authentication properties (AuthMethod, AuthData). On a re-authentication,
	// info is the connection's ConnectInfo with AuthMethod set to the negotiated
	// method and AuthData set to the data of the client's AUTH 0x19.
	Begin(ctx context.Context, info ConnectInfo) (AuthStep, error)
	// Continue advances the exchange with the Authentication Data of the client's
	// AUTH 0x18. data is nil when the packet carried no Authentication Data.
	Continue(ctx context.Context, data []byte) (AuthStep, error)
}
