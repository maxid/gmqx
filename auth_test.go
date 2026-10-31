package gmqx

// Enhanced authentication (MQTT 5 AUTH) cases. The broker used to advertise no
// Authentication Method and answer every AUTH with DISCONNECT 0x82; with a
// Config.Authenticator it now runs a Config-driven exchange. These cases drive
// the exchange over the wire with the conformance client and assert the packets,
// the reason codes, the identity precedence and the bounds.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maxid/gnety/codec/mqtt"
)

// scriptedAuth is a test AuthExchange: the method plus the two callbacks, with
// every call recorded so a case can assert what the broker asked and with which
// data. It is safe for concurrent use because the broker may run a call on the
// timeout goroutine (Config.AuthTimeout).
type scriptedAuth struct {
	method string
	begin  func(context.Context, ConnectInfo) (AuthStep, error)
	cont   func(context.Context, []byte) (AuthStep, error)

	mu        sync.Mutex
	begins    []ConnectInfo
	continues [][]byte
}

func (a *scriptedAuth) Method() string { return a.method }

func (a *scriptedAuth) Begin(ctx context.Context, info ConnectInfo) (AuthStep, error) {
	a.mu.Lock()
	a.begins = append(a.begins, info)
	a.mu.Unlock()
	if a.begin == nil {
		return AuthStep{Status: AuthFailed}, nil
	}
	return a.begin(ctx, info)
}

func (a *scriptedAuth) Continue(ctx context.Context, data []byte) (AuthStep, error) {
	a.mu.Lock()
	a.continues = append(a.continues, append([]byte(nil), data...))
	a.mu.Unlock()
	if a.cont == nil {
		return AuthStep{Status: AuthFailed}, nil
	}
	return a.cont(ctx, data)
}

func (a *scriptedAuth) beginCalls() []ConnectInfo {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]ConnectInfo(nil), a.begins...)
}

func (a *scriptedAuth) continueCalls() [][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([][]byte(nil), a.continues...)
}

// authFactory is the Config.Authenticator of a broker that supports one exchange.
func authFactory(exchange AuthExchange) func(string) AuthExchange {
	return func(method string) AuthExchange {
		if method == exchange.Method() {
			return exchange
		}
		return nil
	}
}

// authProps is the CONNECT property block of an enhanced-authentication CONNECT.
func authProps(method string, data []byte) mqtt.Properties {
	return mqtt.Properties{AuthenticationMethod: method, AuthenticationData: data}
}

// dialAuth dials, sends a CONNECT carrying the given authentication properties and
// leaves the answer unread: an exchange may answer AUTH 0x18, not just CONNACK.
func dialAuth(t *testing.T, b *Broker, clientID string, props mqtt.Properties) *confClient {
	t.Helper()
	conn, err := net.Dial("tcp", b.Addr())
	if err != nil {
		t.Fatalf("dial %s: %v", b.Addr(), err)
	}
	c := newConfClient(t, conn, mqtt.Version5, clientID)
	c.sendConnect(confConnect{version: mqtt.Version5, clientID: clientID, clean: true, props: props})
	return c
}

// readRawFrame reads one complete control packet as its wire bytes, so a case can
// compare a server frame byte for byte.
func readRawFrame(t *testing.T, c *confClient) []byte {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	header, err := c.r.ReadByte()
	if err != nil {
		t.Fatalf("read frame header: %v", err)
	}
	n, err := readRemainingLength(c.r)
	if err != nil {
		t.Fatalf("read remaining length: %v", err)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(c.r, body); err != nil {
		t.Fatalf("read frame body: %v", err)
	}
	return append(mqtt.AppendRemainingLength([]byte{header}, n), body...)
}

// liveClient returns the registered connection of a client id.
func liveClient(t *testing.T, b *Broker, clientID string) *client {
	t.Helper()
	var found *client
	waitFor(t, "session "+clientID, func() bool {
		b.mu.RLock()
		defer b.mu.RUnlock()
		found = b.clients[namespaceKey(b.namespace, clientID)]
		return found != nil
	})
	return found
}

// connectRecorder is an EventSink that keeps the ConnectInfo of every accepted
// connection, which is the identity the SPI observes.
type connectRecorder struct {
	confSink
	mu    sync.Mutex
	infos []ConnectInfo
}

func (r *connectRecorder) OnConnect(_ context.Context, info ConnectInfo) {
	r.mu.Lock()
	r.infos = append(r.infos, info)
	r.mu.Unlock()
}

func (r *connectRecorder) connected() []ConnectInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ConnectInfo(nil), r.infos...)
}

// TestEnhancedAuthOneStep pins the simplest exchange: the client names a method
// and the server admits it in the first step. The CONNACK carries the negotiated
// method and the final data, the identity is the one the exchange returned — also
// for a client id the client did not send — and AuthManager is not consulted at
// all, which DenyAll would make fatal.
func TestEnhancedAuthOneStep(t *testing.T) {
	sink := &connectRecorder{}
	exchange := &scriptedAuth{
		method: "token",
		begin: func(_ context.Context, info ConnectInfo) (AuthStep, error) {
			if info.AuthMethod != "token" {
				t.Errorf("Begin AuthMethod = %q, want token", info.AuthMethod)
			}
			if string(info.AuthData) != "good-token" {
				t.Errorf("Begin AuthData = %q, want good-token", info.AuthData)
			}
			return AuthStep{
				Status: AuthSucceeded,
				Data:   []byte("welcome"),
				// The identity the credential authenticated: a device key that is
				// not the client id the wire carried.
				Info: &ConnectInfo{ClientID: "device-42", Username: "device-42"},
			}, nil
		},
	}
	b := startBroker(t, Config{
		Auth:          DenyAll{}, // must not be consulted for an AUTH connection
		Authenticator: authFactory(exchange),
		AuthMethods:   []string{"token"},
		Sink:          sink,
	})
	c, ack := mustConnect(t, b.Addr(), confConnect{
		version:  mqtt.Version5,
		clientID: "wire-id",
		clean:    true,
		props:    authProps("token", []byte("good-token")),
	})
	// CONNACK 0x00: the method the client used and the exchange's final data.
	if ack.ReasonCode != 0 {
		t.Fatalf("CONNACK reason %#x, want success", ack.ReasonCode)
	}
	if ack.Properties.AuthenticationMethod != "token" {
		t.Fatalf("CONNACK Authentication Method = %q, want token", ack.Properties.AuthenticationMethod)
	}
	if string(ack.Properties.AuthenticationData) != "welcome" {
		t.Fatalf("CONNACK Authentication Data = %q, want welcome", ack.Properties.AuthenticationData)
	}

	// The session lives under the identity the exchange returned, not under the
	// client id of the CONNECT.
	live := liveClient(t, b, "device-42")
	if live.username != "device-42" {
		t.Fatalf("session username = %q, want device-42", live.username)
	}
	b.mu.RLock()
	_, wireID := b.clients["wire-id"]
	b.mu.RUnlock()
	if wireID {
		t.Fatal("the connection was registered under the client id of the CONNECT, not the authenticated identity")
	}
	// The SPI sees the authenticated identity too.
	if infos := sink.connected(); len(infos) != 1 || infos[0].Username != "device-42" || infos[0].ClientID != "device-42" {
		t.Fatalf("OnConnect identities = %+v, want the exchange's device-42", infos)
	}
	// One Begin call, no challenge and no Continue.
	if calls := exchange.beginCalls(); len(calls) != 1 {
		t.Fatalf("Begin calls = %d, want 1", len(calls))
	}
	if calls := exchange.continueCalls(); len(calls) != 0 {
		t.Fatalf("Continue calls = %d, want 0", len(calls))
	}
	// The connection is usable.
	c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	readConf[*mqtt.Pingresp](t, c)
}

// TestEnhancedAuthTwoStep pins the challenge/response frames byte for byte: the
// server answers the CONNECT with AUTH 0x18 (method + challenge), the client
// answers with AUTH 0x18 (method + response), and the exchange ends with CONNACK
// 0x00 carrying the method and the final data.
func TestEnhancedAuthTwoStep(t *testing.T) {
	exchange := &scriptedAuth{
		method: "token",
		begin: func(_ context.Context, info ConnectInfo) (AuthStep, error) {
			if string(info.AuthData) != "hello" {
				return AuthStep{}, fmt.Errorf("Begin data %q, want hello", info.AuthData)
			}
			return AuthStep{Status: AuthContinue, Data: []byte("challenge")}, nil
		},
		cont: func(_ context.Context, data []byte) (AuthStep, error) {
			if string(data) != "answer" {
				return AuthStep{}, fmt.Errorf("Continue data %q, want answer", data)
			}
			return AuthStep{Status: AuthSucceeded, Data: []byte("done")}, nil
		},
	}
	b := startBroker(t, Config{Authenticator: authFactory(exchange), AuthMethods: []string{"token"}})
	c := dialAuth(t, b, "two-step", authProps("token", []byte("hello")))

	// The first server frame is the challenge, exactly:
	//   f0 16        AUTH, 22 bytes
	//   18           reason 0x18 (Continue authentication)
	//   14           property length 20
	//   15 00 05 token     Authentication Method
	//   16 00 09 challenge Authentication Data
	want := []byte{
		0xf0, 0x16, 0x18, 0x14,
		0x15, 0x00, 0x05, 't', 'o', 'k', 'e', 'n',
		0x16, 0x00, 0x09, 'c', 'h', 'a', 'l', 'l', 'e', 'n', 'g', 'e',
	}
	got := readRawFrame(t, c)
	if !bytes.Equal(got, want) {
		t.Fatalf("server AUTH frame = % x, want % x", got, want)
	}

	// The client answers with its own 0x18, naming the method again (the codec
	// omits an empty Authentication Data, so this frame exercises both properties).
	c.write(&mqtt.Auth{
		MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5},
		ReasonCode:    0x18,
		Properties:    authProps("token", []byte("answer")),
	})
	ack := readConf[*mqtt.Connack](t, c)
	if ack.ReasonCode != 0 {
		t.Fatalf("CONNACK reason %#x, want success", ack.ReasonCode)
	}
	if ack.Properties.AuthenticationMethod != "token" {
		t.Fatalf("CONNACK Authentication Method = %q, want token", ack.Properties.AuthenticationMethod)
	}
	if string(ack.Properties.AuthenticationData) != "done" {
		t.Fatalf("CONNACK Authentication Data = %q, want done", ack.Properties.AuthenticationData)
	}
	if calls := exchange.continueCalls(); len(calls) != 1 || string(calls[0]) != "answer" {
		t.Fatalf("Continue calls = %q, want [answer]", calls)
	}
	if live := liveClient(t, b, "two-step"); live.username != "" {
		t.Fatalf("session username = %q, want the CONNECT's empty username", live.username)
	}
}

// TestEnhancedAuthDenied pins the failure path: the exchange refuses, the CONNACK
// carries its reason code, the connection closes and no session is registered.
func TestEnhancedAuthDenied(t *testing.T) {
	t.Run("default_code", func(t *testing.T) {
		exchange := &scriptedAuth{method: "token", begin: func(context.Context, ConnectInfo) (AuthStep, error) {
			return AuthStep{Status: AuthFailed}, nil
		}}
		b := startBroker(t, Config{Authenticator: authFactory(exchange)})
		c, ack := dialConf(t, b.Addr(), confConnect{
			version: mqtt.Version5, clientID: "denied", clean: true,
			props: authProps("token", []byte("bad")),
		})
		if ack.ReasonCode != 0x87 {
			t.Fatalf("CONNACK reason %#x, want 0x87 Not authorized", ack.ReasonCode)
		}
		expectClosed(t, c)
		assertNoSessions(t, b)
	})
	t.Run("bad_credentials_code", func(t *testing.T) {
		exchange := &scriptedAuth{method: "token", begin: func(context.Context, ConnectInfo) (AuthStep, error) {
			return AuthStep{Status: AuthFailed, ReasonCode: 0x86}, nil
		}}
		b := startBroker(t, Config{Authenticator: authFactory(exchange)})
		c, ack := dialConf(t, b.Addr(), confConnect{
			version: mqtt.Version5, clientID: "denied-86", clean: true,
			props: authProps("token", nil),
		})
		if ack.ReasonCode != 0x86 {
			t.Fatalf("CONNACK reason %#x, want 0x86 Bad User Name or Password", ack.ReasonCode)
		}
		expectClosed(t, c)
		assertNoSessions(t, b)
	})
	t.Run("exchange_error", func(t *testing.T) {
		exchange := &scriptedAuth{method: "token", begin: func(context.Context, ConnectInfo) (AuthStep, error) {
			return AuthStep{}, errors.New("credential store unavailable")
		}}
		b := startBroker(t, Config{Authenticator: authFactory(exchange)})
		c, ack := dialConf(t, b.Addr(), confConnect{
			version: mqtt.Version5, clientID: "denied-err", clean: true,
			props: authProps("token", nil),
		})
		if ack.ReasonCode != 0x87 {
			t.Fatalf("CONNACK reason %#x, want 0x87 for an exchange that could not answer", ack.ReasonCode)
		}
		expectClosed(t, c)
		assertNoSessions(t, b)
	})
}

// assertNoSessions pins that a refused connection left no session behind.
func assertNoSessions(t *testing.T, b *Broker) {
	t.Helper()
	b.mu.RLock()
	n := len(b.clients)
	b.mu.RUnlock()
	if n != 0 {
		t.Fatalf("%d sessions registered after a refused connection, want 0", n)
	}
	if keys := b.cfg.SessionStore.Keys(); len(keys) != 0 {
		t.Fatalf("session store keys = %v after a refused connection, want none", keys)
	}
}

// TestEnhancedAuthUnknownMethod pins the 0x8C refusal: no factory at all, a
// factory that declines the method, and a method outside Config.AuthMethods.
func TestEnhancedAuthUnknownMethod(t *testing.T) {
	exchange := &scriptedAuth{method: "token", begin: func(context.Context, ConnectInfo) (AuthStep, error) {
		return AuthStep{Status: AuthSucceeded}, nil
	}}
	cases := map[string]Config{
		"no_factory":       {},
		"factory_declines": {Authenticator: func(string) AuthExchange { return nil }},
		"not_advertised":   {Authenticator: authFactory(exchange), AuthMethods: []string{"scram-sha-256"}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			b := startBroker(t, cfg)
			c, ack := dialConf(t, b.Addr(), confConnect{
				version: mqtt.Version5, clientID: "unknown-method", clean: true,
				props: authProps("token", nil),
			})
			if ack.ReasonCode != 0x8C {
				t.Fatalf("CONNACK reason %#x, want 0x8C Bad authentication method", ack.ReasonCode)
			}
			if ack.Properties.AuthenticationMethod != "" {
				t.Fatalf("CONNACK names method %q on a refused connection", ack.Properties.AuthenticationMethod)
			}
			expectClosed(t, c)
			assertNoSessions(t, b)
		})
	}
}

// TestEnhancedAuthStepLimit pins the bound on the exchange: an exchange that always
// asks for another step gets exactly MaxAuthSteps challenges and is then refused
// with 0x87, instead of being allowed to loop until a timeout.
func TestEnhancedAuthStepLimit(t *testing.T) {
	exchange := &scriptedAuth{
		method: "token",
		begin: func(context.Context, ConnectInfo) (AuthStep, error) {
			return AuthStep{Status: AuthContinue, Data: []byte("again")}, nil
		},
		cont: func(context.Context, []byte) (AuthStep, error) {
			return AuthStep{Status: AuthContinue, Data: []byte("again")}, nil
		},
	}
	b := startBroker(t, Config{Authenticator: authFactory(exchange), AuthMethods: []string{"token"}})
	c := dialAuth(t, b, "step-limit", authProps("token", nil))

	challenges := 0
	for {
		msg, err := c.read()
		if err != nil {
			t.Fatalf("read packet after %d challenges: %v", challenges, err)
		}
		switch m := msg.(type) {
		case *mqtt.Auth:
			if m.ReasonCode != 0x18 {
				t.Fatalf("server AUTH reason %#x, want 0x18", m.ReasonCode)
			}
			if m.Properties.AuthenticationMethod != "token" {
				t.Fatalf("challenge method = %q, want token", m.Properties.AuthenticationMethod)
			}
			challenges++
			if challenges > 2*MaxAuthSteps {
				t.Fatal("the broker kept challenging past MaxAuthSteps")
			}
			c.write(&mqtt.Auth{
				MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5},
				ReasonCode:    0x18,
				Properties:    mqtt.Properties{AuthenticationData: []byte("more")},
			})
		case *mqtt.Connack:
			if m.ReasonCode != 0x87 {
				t.Fatalf("CONNACK reason %#x after the step budget, want 0x87", m.ReasonCode)
			}
			if challenges != MaxAuthSteps {
				t.Fatalf("challenges = %d, want the documented maximum %d", challenges, MaxAuthSteps)
			}
			if calls := exchange.continueCalls(); len(calls) != MaxAuthSteps {
				t.Fatalf("Continue calls = %d, want %d", len(calls), MaxAuthSteps)
			}
			expectClosed(t, c)
			assertNoSessions(t, b)
			return
		default:
			t.Fatalf("read %T during the exchange, want AUTH or CONNACK", msg)
		}
	}
}

// TestEnhancedAuthReauthenticate pins re-authentication on a live connection: AUTH
// 0x19 starts a fresh exchange, AUTH 0x00 ends it, the subscriptions survive and
// the identity may change; an AUTH naming another method is a Protocol Error.
func TestEnhancedAuthReauthenticate(t *testing.T) {
	var (
		mu     sync.Mutex
		begins int
	)
	exchange := &scriptedAuth{
		method: "token",
		begin: func(_ context.Context, info ConnectInfo) (AuthStep, error) {
			if string(info.AuthData) != "ok" {
				return AuthStep{}, fmt.Errorf("Begin data %q, want ok", info.AuthData)
			}
			mu.Lock()
			begins++
			n := begins
			mu.Unlock()
			name := fmt.Sprintf("device-%d", n)
			return AuthStep{Status: AuthSucceeded, Data: []byte(name), Info: &ConnectInfo{Username: name}}, nil
		},
	}
	b := startBroker(t, Config{Authenticator: authFactory(exchange), AuthMethods: []string{"token"}})
	c, ack := mustConnect(t, b.Addr(), confConnect{
		version: mqtt.Version5, clientID: "reauth", clean: true,
		props: authProps("token", []byte("ok")),
	})
	if ack.Properties.AuthenticationMethod != "token" || string(ack.Properties.AuthenticationData) != "device-1" {
		t.Fatalf("CONNACK method/data = %q/%q, want token/device-1",
			ack.Properties.AuthenticationMethod, ack.Properties.AuthenticationData)
	}
	live := liveClient(t, b, "reauth")
	if live.username != "device-1" {
		t.Fatalf("session username = %q, want device-1", live.username)
	}
	if codes := subscribe(t, c, 1, mqtt.Properties{}, mqtt.Subscription{Filter: "reauth/keep", QoS: 1}).ReasonCodes; len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK codes %v, want [1]", codes)
	}

	// Re-authenticate: AUTH 0x19 with the negotiated method and the new credential.
	c.write(&mqtt.Auth{
		MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5},
		ReasonCode:    0x19,
		Properties:    authProps("token", []byte("ok")),
	})
	// The answer is AUTH 0x00 (Success) with the method and the final data, not a
	// second CONNACK: the connection is already established.
	done := readConf[*mqtt.Auth](t, c)
	if done.ReasonCode != 0x00 {
		t.Fatalf("re-authentication AUTH reason %#x, want 0x00 Success", done.ReasonCode)
	}
	if done.Properties.AuthenticationMethod != "token" {
		t.Fatalf("re-authentication method = %q, want token", done.Properties.AuthenticationMethod)
	}
	if string(done.Properties.AuthenticationData) != "device-2" {
		t.Fatalf("re-authentication data = %q, want device-2", done.Properties.AuthenticationData)
	}
	if calls := exchange.beginCalls(); len(calls) != 2 {
		t.Fatalf("Begin calls = %d, want 2 (CONNECT and re-authentication)", len(calls))
	}
	// The identity the second exchange authenticated replaced the first.
	waitFor(t, "the re-authenticated username", func() bool {
		live.mu.Lock()
		defer live.mu.Unlock()
		return live.username == "device-2"
	})

	// The session is untouched: the subscription still delivers.
	pub, _ := mustConnect(t, b.Addr(), confConnect{version: mqtt.Version5, clientID: "reauth-pub", clean: true})
	pub.publish(confPublish{topic: "reauth/keep", payload: "still here", qos: 0})
	if got := expectPublish(t, c); string(got.Payload) != "still here" {
		t.Fatalf("delivery after re-authentication = %q, want the session's subscription to survive", got.Payload)
	}

	// A re-authentication that names a different method is a Protocol Error.
	c.write(&mqtt.Auth{
		MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5},
		ReasonCode:    0x19,
		Properties:    mqtt.Properties{AuthenticationMethod: "scram-sha-256"},
	})
	expectDisconnect(t, c, 0x82)
}

// TestEnhancedAuthConnectStall pins that a client that never answers a challenge is
// disconnected inside Config.ConnectTimeout instead of holding the handshake (and
// its goroutine, slot and session) forever.
func TestEnhancedAuthConnectStall(t *testing.T) {
	const connectTimeout = 60 * time.Millisecond
	exchange := &scriptedAuth{
		method: "token",
		begin: func(context.Context, ConnectInfo) (AuthStep, error) {
			return AuthStep{Status: AuthContinue, Data: []byte("challenge")}, nil
		},
	}
	b := startBroker(t, Config{
		ConnectTimeout: connectTimeout,
		Authenticator:  authFactory(exchange),
		AuthMethods:    []string{"token"},
	})
	c := dialAuth(t, b, "stalled", authProps("token", nil))
	if challenge := readConf[*mqtt.Auth](t, c); challenge.ReasonCode != 0x18 {
		t.Fatalf("challenge reason %#x, want 0x18", challenge.ReasonCode)
	}

	start := time.Now()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if got, err := c.r.ReadByte(); err == nil {
		t.Fatalf("read byte %#x instead of a close after a stalled AUTH", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("a stalled exchange took %v, want a disconnect inside the %v CONNECT deadline", elapsed, connectTimeout)
	}
	assertNoSessions(t, b)
}

// TestEnhancedAuthProtocolErrors pins the packets that cannot be part of an
// exchange: Authentication Data without a Method, an answer that is not AUTH 0x18,
// an answer naming another method, and a live AUTH that does not ask to
// re-authenticate.
func TestEnhancedAuthProtocolErrors(t *testing.T) {
	challenging := func() *scriptedAuth {
		return &scriptedAuth{
			method: "token",
			begin: func(context.Context, ConnectInfo) (AuthStep, error) {
				return AuthStep{Status: AuthContinue, Data: []byte("challenge")}, nil
			},
			cont: func(context.Context, []byte) (AuthStep, error) {
				return AuthStep{Status: AuthSucceeded}, nil
			},
		}
	}
	t.Run("data_without_method", func(t *testing.T) {
		b := startBroker(t, Config{})
		c, ack := dialConf(t, b.Addr(), confConnect{
			version: mqtt.Version5, clientID: "data-only", clean: true,
			props: mqtt.Properties{AuthenticationData: []byte("orphan")},
		})
		if ack.ReasonCode != 0x82 {
			t.Fatalf("CONNACK reason %#x, want 0x82 Protocol Error", ack.ReasonCode)
		}
		expectClosed(t, c)
		assertNoSessions(t, b)
	})
	t.Run("answer_is_not_auth", func(t *testing.T) {
		b := startBroker(t, Config{Authenticator: authFactory(challenging())})
		c := dialAuth(t, b, "not-auth", authProps("token", nil))
		if challenge := readConf[*mqtt.Auth](t, c); challenge.ReasonCode != 0x18 {
			t.Fatalf("challenge reason %#x, want 0x18", challenge.ReasonCode)
		}
		c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
		ack := readConf[*mqtt.Connack](t, c)
		if ack.ReasonCode != 0x82 {
			t.Fatalf("CONNACK reason %#x, want 0x82 Protocol Error", ack.ReasonCode)
		}
		expectClosed(t, c)
	})
	t.Run("answer_names_another_method", func(t *testing.T) {
		b := startBroker(t, Config{Authenticator: authFactory(challenging())})
		c := dialAuth(t, b, "other-method", authProps("token", nil))
		if challenge := readConf[*mqtt.Auth](t, c); challenge.ReasonCode != 0x18 {
			t.Fatalf("challenge reason %#x, want 0x18", challenge.ReasonCode)
		}
		c.write(&mqtt.Auth{
			MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5},
			ReasonCode:    0x18,
			Properties:    authProps("scram-sha-256", []byte("x")),
		})
		ack := readConf[*mqtt.Connack](t, c)
		if ack.ReasonCode != 0x82 {
			t.Fatalf("CONNACK reason %#x, want 0x82 Protocol Error", ack.ReasonCode)
		}
		expectClosed(t, c)
	})
	t.Run("live_auth_without_reauthenticate", func(t *testing.T) {
		b := startBroker(t, Config{Authenticator: authFactory(&scriptedAuth{
			method: "token",
			begin: func(context.Context, ConnectInfo) (AuthStep, error) {
				return AuthStep{Status: AuthSucceeded}, nil
			},
		})})
		c, _ := mustConnect(t, b.Addr(), confConnect{
			version: mqtt.Version5, clientID: "live-0x18", clean: true,
			props: authProps("token", nil),
		})
		c.write(&mqtt.Auth{
			MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5},
			ReasonCode:    0x18,
			Properties:    authProps("token", []byte("unexpected")),
		})
		expectDisconnect(t, c, 0x82)
	})
}

// TestEnhancedAuthLeavesThePlainPathAlone pins that a broker with an
// Authenticator still sends every connection without an Authentication Method
// through AuthManager: DenyAll still refuses it and AllowAll still admits it, and
// the exchange is never built for it.
func TestEnhancedAuthLeavesThePlainPathAlone(t *testing.T) {
	exchange := &scriptedAuth{method: "token", begin: func(context.Context, ConnectInfo) (AuthStep, error) {
		return AuthStep{Status: AuthSucceeded}, nil
	}}
	denying := startBroker(t, Config{Auth: DenyAll{}, Authenticator: authFactory(exchange)})
	_, ack := dialConf(t, denying.Addr(), confConnect{version: mqtt.Version5, clientID: "plain-denied", clean: true})
	if ack.ReasonCode != 0x87 {
		t.Fatalf("CONNACK reason %#x for a method-less CONNECT under DenyAll, want 0x87", ack.ReasonCode)
	}

	allowing := startBroker(t, Config{Auth: AllowAll{}, Authenticator: authFactory(exchange)})
	c, ack := mustConnect(t, allowing.Addr(), confConnect{version: mqtt.Version5, clientID: "plain-allowed", clean: true})
	if ack.Properties.AuthenticationMethod != "" {
		t.Fatalf("CONNACK names method %q for a method-less CONNECT", ack.Properties.AuthenticationMethod)
	}
	c.write(&mqtt.Pingreq{MessageHeader: mqtt.MessageHeader{Level: mqtt.Version5}})
	readConf[*mqtt.Pingresp](t, c)
	if calls := exchange.beginCalls(); len(calls) != 0 {
		t.Fatalf("Begin calls = %d for connections without a method, want 0", len(calls))
	}
}
