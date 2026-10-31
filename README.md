English | [简体中文](README.zh-CN.md)

# gmqx

A lightweight, embeddable **MQTT 3.1.1 broker** written in Go.

> Status: **development snapshot (untagged)** — MQTT 3.1.1/5 broker; TCP/TLS/WebSocket/WSS transport via gnety,
> pluggable authentication/ACL/event/metrics hooks, connect modes and limits.
> API may change before v1.0.

## Features

- MQTT 3.1, 3.1.1 and basic MQTT 5 over TCP, TLS, WebSocket and WSS
- CONNECT/CONNACK, SUBSCRIBE/SUBACK, PUBLISH, PING, DISCONNECT
- Persistent sessions (Clean Session / Clean Start, Session Present); the
  subscriptions of a persistent session survive a disconnect and a clean
  connection discards them
- MQTT 5: CONNACK capability properties, Topic Alias, User Property forwarding,
  subscription options (no-local / retain-as-published / retain-handling), reason
  codes and DISCONNECT on protocol errors
- Persistent-session storage with a bounded offline queue and MQTT 5 Session
  Expiry Interval: a session with no live connection keeps what its subscriptions
  match (FIFO, caps per session and globally), drains it right after the next
  CONNACK, and is deleted by a bounded sweeper when its expiry interval elapses.
  The state lives behind the `SessionStore` seam (`Load`/`Save`/`Delete`/`Keys`),
  whose default is the in-memory store of earlier versions.
- QoS 0/1/2 flows with per-subscription QoS capping, broker-side outbound QoS 1/2
  retransmission (DUP on PUBLISH retries, PUBREL retries until PUBCOMP, one timer
  per connection, capped exponential backoff) and inflight windows backed by a
  bounded per-connection queue that delivers FIFO as slots free, instead of
  blocking or dropping. An inbound QoS 2 handshake suppresses duplicates and is
  held to `MaxInflightQoS2` with PUBREC 0x97.
- One writer goroutine per connection with a bounded outbound queue and a write
  deadline per packet (`MaxOutboundQueueSize`, `WriteTimeout`), so a peer that
  stops reading cannot hold the publisher, a will publish or the cluster receive
  goroutine.
- Retained messages with MQTT 5 Message Expiry Interval (a replayed message
  carries what is left of it, an elapsed one is not replayed and is deleted),
  wildcard subscriptions (`+`/`#`), will messages with their QoS, retain flag and
  properties, and MQTT 5 Will Delay Interval (published after the delay, cancelled
  by a reconnect, cut short by session expiry), keepalive
- CONNECT-phase and authorization deadlines: `ConnectTimeout` closes a socket
  that never completes its CONNECT, `AuthTimeout` bounds `Authenticate`, the steps
  of an `AuthExchange` and `ACL.Check` and treats an expired call as a refusal
- MQTT 5 enhanced authentication (AUTH): an `AuthExchange` SPI (`Begin`/`Continue`)
  behind a multi-method `Config.Authenticator`, CONNACK 0x8C for an unknown method,
  an identity the exchange authenticates, re-authentication on a live connection
  and a bounded number of steps (see **Enhanced authentication** below)
- Wire-level MQTT 3.1.1/5 conformance suite (`TestConformance`)
- Pluggable hooks: `AuthManager`, `AclManager`, `EventSink` (lifecycle/messages),
  `MetricsManager`, `Interceptor`
- Pluggable routing: `Dispatcher` receives a `Publication` for every accepted
  publish and decides where it goes; the nil dispatcher keeps the built-in local
  delivery plus cluster spread (`DefaultDispatcher`), and the broker exposes a
  read-only seam (`LocalTargets`, `SendLocal`, `SpreadPublication`) so a
  dispatcher never re-implements topic matching
- Subscription index for routing: local matching is one map lookup for a literal
  filter plus one match per distinct wildcard filter, so a publish no longer
  enumerates the connections or the stored sessions and never calls
  `SessionStore` on the publish path
- Directed publish: `Broker.PublishToClient(ctx, clientID, Publication)` delivers
  to one client id — the live local connection, the local persistent session
  (queued, drained on its next CONNECT) or the cluster node that holds it; the
  Redis backend routes it through a TTL'd client-id presence view, the TCP mesh
  broadcasts it and each peer applies it only if it holds the id
- Connection-scoped context for every hook: `ConnInfo` (client id, connection
  id, namespace, remote address, protocol level) through
  `ContextWithConn`/`ConnFromContext`, enriched once at CONNECT by
  `Config.ConnContext` and cancelled when the connection ends
- Namespace isolation: `Config.Namespace` scopes the session table, subscription
  matching, retained state and the cluster subscription summaries/events, so a
  publish of one namespace never reaches a session of another; the default empty
  namespace keeps single-namespace deployments unchanged
- Connect modes: `UNIQUE` / `KICK` (client id takeover, DISCONNECT 0x8E for the
  displaced MQTT 5 client, `NotKickSeconds` anti-flap window on both the CONNECT and
  the peer side, and a connect-time kick that carries the connect time of the
  connection that produced it, so a duplicate connection on another node is closed
  while a delayed kick cannot close a newer one)
- Shared persistent sessions across the nodes of a cluster: the opt-in
  `RedisSessionStore` keeps subscriptions and the offline queue in the same Redis the
  cluster backend uses, so a client that reconnects to another node resumes them
- Shared subscriptions (MQTT 5 §4.8.2): `$share/{ShareName}/{filter}` is validated
  per the ShareName rules (a malformed one is refused with SUBACK 0x8F, a filter
  that merely starts with `$share` stays a literal filter, and No Local on a shared
  subscription is refused), routed to exactly one live member of each group round
  robin, and — across a cluster — to exactly one node per group, elected by the node
  that produced the publication from the nodes that advertise a member (see
  **Shared subscriptions** below). A group's members on other nodes do not get a
  second copy, offline members get nothing (shared state is never queued), and no
  retained message is replayed when a session subscribes
- Subscription identifiers (MQTT 5 property 0x0B): one per SUBSCRIBE is stored with
  the subscription, survives a persistent session, and is attached to every PUBLISH
  that subscription causes — live, retained replay, offline-queue drain and shared
  group delivery alike, deduplicated across the subscriptions that matched and
  bounded per delivery (see **Subscription identifiers** below)
- The CONNACK advertises Retain, Wildcard Subscriptions, Subscription Identifiers and
  Shared Subscriptions as available, and all four are implemented
- Limits: max connections, max subscriptions, max retained messages, inflight
  windows per direction, offline queue size per session and globally, connection
  rate limiting
- Graceful shutdown and client-id session takeover
- Pluggable `Cluster` interface with a built-in TCP backend (static seeds,
  handshake membership with heartbeats and eviction, event deduplication and
  subscription-aware directed routing, cross-node publish/kick, reconnect) and a
  Redis backend (per-node channels instead of a mesh, TTL membership in Redis,
  the same deduplication and routing rules)

## Install

```bash
go get github.com/maxid/gmqx
```

## Quick start

```go
broker, _ := gmqx.New(gmqx.Config{Addr: ":1883"})
_ = broker.Start(context.Background())
defer broker.Stop(context.Background())
```

With hooks:

```go
broker, _ := gmqx.New(gmqx.Config{
    Addr: ":1883",
    Auth: myAuth{},          // implements gmqx.AuthManager
    ACL:  myACL{},           // implements gmqx.AclManager
    Sink: myDispatcher{},    // implements gmqx.EventSink
    Metrics: gmqx.NoopMetrics{},
    ConnectMode: gmqx.ConnectModeKick,
})
```

## Examples

[`examples/broker-demo`](examples/broker-demo/main.go) starts a broker on an
ephemeral loopback port, connects one client and completes a publish/subscribe
round trip. gmqx ships no client API, so the demo acts as an MQTT 3.1.1 client
through the gnety MQTT codec (the wire layer the broker itself uses) and prints
the PUBLISH the broker delivers back:

```sh
go run ./examples/broker-demo
go run ./examples/broker-demo -addr 127.0.0.1:1883 -topic demo/hello
```

Broker and client share one process, so no third-party MQTT client is needed.

## Configuration (selected)

| Field | Default | Description |
|---|---|---|
| `Addr` / `Port` | `:1883` / `1883` | TCP listen address |
| `SecureAddr` / `SecurePort` | — / 8883 | TLS listener (requires `TLS`) |
| `WebsocketAddr` / `WebsocketPort` | — / 1884 | WebSocket listener |
| `WebsocketSecureAddr` / `WebsocketSecurePort` | — / 8884 | WSS listener |
| `WebsocketPath` | `/mqtt` | WebSocket path |
| `TLS` | disabled | `Enabled`, `Cert`, `Key`, `CA`, `RequireClientCert` |
| `MaxPacketSize` | 4 MiB | inbound packet limit |
| `ConnectMode` | `UNIQUE` | reject or kick an existing client id |
| `NotKickSeconds` | 30 | anti-flap window of a takeover, enforced for a local CONNECT and for a peer's kick (negative: no window) |
| `ConnectTimeout` | 10 s | CONNECT-phase read deadline; a socket that says nothing is closed |
| `AuthTimeout` | 5 s | bound on `Authenticate`, each `AuthExchange` call and `ACL.Check`; an expiry is a refusal (negative: no bound) |
| `MaxWillDelayInterval` | 5 min | largest MQTT 5 Will Delay Interval honoured (negative: unbounded) |
| `MaxConnections` | 0 (unlimited) | concurrent connection limit |
| `MaxTopicSubscriptions` | 0 | per-session subscription limit |
| `MaxRetainMessages` | 0 | retained message count limit |
| `ConnectionRateLimit` | 0 | connections/second token bucket |
| `MaxInflightQoS1` | 64 | unconfirmed QoS 1 PUBLISHes per connection (negative: unlimited) |
| `MaxInflightQoS2` | 64 | QoS 2 exchanges per connection and direction (negative: unlimited) |
| `MaxInflightQueueSize` | 256 | QoS 1/2 deliveries per connection waiting for a free inflight slot |
| `MaxOutboundQueueSize` | 256 | packets queued for a connection's writer goroutine |
| `WriteTimeout` | 5 s | write deadline of one packet; a wedged socket is closed |
| `QoSRetryInterval` | 5 s | base retransmission interval, doubled per attempt |
| `QoSMaxRetries` | 5 | retransmissions per exchange before giving up (negative: none) |
| `MaxOfflineQueueSize` | 1000 | messages queued per offline session; the oldest is dropped |
| `MaxTotalOfflineMessages` | 100000 | messages queued for all offline sessions; new ones are rejected |
| `SessionSweepInterval` | 30 s | how often expired sessions are deleted (negative: never) |
| `SessionStore` | `MemorySessionStore` | persistence seam for offline session state; `NewRedisSessionStore` shares it across the nodes of a Redis cluster |
| `Auth` / `ACL` / `Sink` / `Metrics` | allow-all / no-op | pluggable SPI |
| `Authenticator` / `AuthMethods` | nil / empty | MQTT 5 enhanced authentication: the `AuthExchange` factory per method and the methods the broker supports (a CONNECT naming another method is refused with 0x8C) |
| `Namespace` | `""` (default) | isolation scope for sessions, retained state and cluster traffic |
| `Dispatcher` | `DefaultDispatcher` | routing SPI: local delivery plus cluster spread by default |
| `ConnContext` | nil | enrich the per-connection context at CONNECT |

The delivery limits that came with the offline queue and the inflight windows are
bounded on purpose: an absent value selects the documented default, because an
unbounded queue and an unbounded window both turn a slow client into unbounded
broker memory. A negative value means unlimited (`MaxInflightQoS1`,
`MaxInflightQoS2`, `MaxOfflineQueueSize`, `MaxTotalOfflineMessages`), no
retransmission (`QoSMaxRetries`) or no sweeper (`SessionSweepInterval`).
`MaxInflightQueueSize`, `MaxOutboundQueueSize`, `WriteTimeout` and
`ConnectTimeout` have no unlimited setting: a non-positive value selects the
default, because those four are the guards that keep one slow or silent client
from costing the broker unbounded memory, a stalled goroutine or a connection slot
that never completes its CONNECT.

Remaining MQTT 5 properties/semantics (receive maximum, packet limits), QoS 2 state
across a reconnect and the client library are on the roadmap (see [ROADMAP.md](ROADMAP.md)).

## Dispatcher, connection context and namespaces

`Config.Dispatcher` is the single decision point for where an accepted
publication goes. The broker authorizes, runs the interceptors and the sink
first; retained state is written before routing, so a dispatcher decides where
live publications go, not what is retained. `DefaultDispatcher` (the nil
default) delivers to local matching sessions and spreads to the cluster backend.
A custom dispatcher routes through the broker's read-only seam instead of
matching topics itself:

```go
type myDispatcher struct{ broker *gmqx.Broker }

func (d *myDispatcher) Bind(b *gmqx.Broker) { d.broker = b }

func (d *myDispatcher) Dispatch(ctx context.Context, pub gmqx.Publication) error {
    for _, target := range d.broker.LocalTargets(pub) {
        _ = d.broker.SendLocal(ctx, target, pub)
    }
    return d.broker.SpreadPublication(ctx, pub)
}
```

Every hook receives a connection-scoped context. Go has no thread-local state,
so the connection identity travels as a context value: `gmqx.ConnFromContext`
returns the `gmqx.ConnInfo` of the connection a hook runs for. The context is
created when the connection is accepted, enriched once at CONNECT by
`Config.ConnContext` (values and deadlines put there reach every hook of that
connection), reused for the whole exchange, and cancelled when the connection
ends.

`Config.Namespace` is the isolation scope of a broker. Sessions, subscription
matching, the retained store and the advertised subscription summaries are
partitioned by it, and a remote cluster event that belongs to another namespace
is dropped by both built-in backends. The empty string is the default namespace
(the zero value), so a deployment that does not set it behaves exactly as before,
including the documented Redis key layout.

## Directed publish

`PublishToClient` delivers one publication to one client id, wherever it lives:

```go
err := broker.PublishToClient(ctx, "device-42", gmqx.Publication{
    Topic:   "cmd/reboot",
    Payload: []byte("now"),
    QoS:     1,
})
```

- A live local connection receives it through the same `SendLocal` path as the
  local fan-out, at the publication's QoS (there is no subscription to cap it) and
  with the connection's QoS 1/2 inflight window respected.
- A local persistent session with no connection is queued for, exactly like a
  matching publication: it drains after the next CONNACK and obeys
  `MaxOfflineQueueSize` and `MaxTotalOfflineMessages`.
- Otherwise the publication becomes a `ClusterTarget` event and the call returns
  nil once the backend accepted it. The Redis backend routes it to the node that
  holds the client id; the TCP mesh broadcasts it and each peer applies it only if
  it holds that client id.

If nobody holds the client id the call still succeeds: delivery is
fire-and-forget (at-most-once, like the cluster transport) and the miss is counted
with `RecordDroppedMessage("target_unknown")`. The call returns an error only for a
caller mistake — an empty client id (`ErrClientIDRequired`), an invalid topic or
QoS, a publication stamped with another namespace (`ErrNamespaceMismatch`) — or
when the cluster backend refused the event (for example Redis is unreachable), in
which case the caller may retry.

A directed publish is a server-side delivery, not a client PUBLISH: it does not run
through the ACL, the interceptors or the `EventSink` (the caller owns the
authorization of the call), it does not touch the retained store, and a target
event a peer receives is not re-dispatched through its `Dispatcher`.

## Shared subscriptions and subscription identifiers

Both MQTT 5 capabilities are implemented and advertised in the CONNACK
(`SharedSubscriptionAvailable` and `SubscriptionIdentifierAvailable`).

### Shared subscriptions

A shared subscription is a filter of the form `$share/{ShareName}/{filter}`
(MQTT 5 §4.8.2). It is validated when the SUBSCRIBE arrives:

- the ShareName must be non-empty and must not contain `/`, `+` or `#`, and the
  rest must be a valid topic filter (`validFilter`); a `$share/...` filter that
  breaks either rule is refused **per filter** with SUBACK `0x8F` (Topic Filter
  invalid) and the connection stays usable. MQTT 3.1.1, whose SUBACK has only
  `0x80`, closes the connection instead, like it does for any malformed filter;
- a filter that merely starts with `$share` (`$share` alone, `$sharex/g/t`) is an
  ordinary literal filter and stays one;
- No Local on a shared subscription is refused (MQTT-3.8.3-4), also with `0x8F`
  per filter.

Routing, on one node: a publication matching a group goes to **exactly one** live
member, chosen round robin over the group's members (sorted by session key, so the
choice is stable and easy to test). Two groups with the same filter are independent
and each get one copy, and a normal subscriber on the same filter gets its own copy
on top. The group's round-robin cursor lives with the group in the routing index
and disappears with its last member, so a client that invents group names cannot
grow the index. Characteristics worth knowing:

- only live sessions are members. A persistent session that subscribed to a group
  keeps the subscription (a resume restores it) but is not a member while it is
  offline, and **shared state is never queued** for it: the other members take the
  publications instead (§4.8.2: a publication goes to one of the sessions, not to a
  mailbox). On its return the session rejoins the group;
- no retained message is replayed when a session subscribes to a group (§4.8.2);
  later matching publications are delivered normally;
- the identifier of a shared subscription (see below) travels with the delivery the
  elected member receives.

Across a cluster, a naive "every node delivers its own copy" would give a group one
copy **per node**. Instead the node that produces the publication elects exactly one
node per group:

```
candidates = peer nodes that advertise a live member of the group
           + this node, when its index holds one      (sorted)
chosen     = candidates[(fnv64(groupKey) + seat) % len(candidates)]
```

`seat` is a per-group counter of the producing node, so successive publications of
one group visit every node that holds a member; the hash is the group's affinity, so
different groups spread over different nodes without coordination. No shared state
and no round trip are involved, which is why the same rule works for the TCP mesh
and the Redis backend: the chosen node is named in `ClusterMessage.Shared` (one
entry per node, `SharedDelivery`), the election is the routing decision, and a node
that is not named delivers nothing for that group. The chosen node then picks the
member round robin locally, exactly as a single node does.

Consequences, deliberately chosen:

- a publication is delivered **at most once per group, cluster-wide**;
- if the chosen node lost its last member in the meantime (a disconnect, or a
  summary one refresh old), it **drops** the publication and counts it with
  `Metrics.RecordDroppedMessage("shared_no_member")`. It is never handed to another
  node's member: delivery is at-most-once, and re-routing could duplicate a copy the
  chosen node already delivered. The same counter covers a group whose members have
  all left while a peer still advertises it;
- a group that no node advertises any more is not a shared publication at all: the
  publication simply matches no subscriber and nothing is counted;
- both built-in backends advertise the group keys in their subscription summary
  (`subscriptionFilters` carries the normal filters, `sharedSubscriptionGroups` the
  groups); a `Cluster` implementation outside this package does not, and shared
  subscriptions then stay local to the node that produced the publication. A peer
  running an older build ignores `ClusterMessage.Shared` and delivers no shared copy
  across nodes instead of a duplicate.

### Subscription identifiers

A SUBSCRIBE may carry a Subscription Identifier (property `0x0B`, value
1..268435455). The identifier is state of the **subscription**, not of the
connection: it is stored with the filter, written to the `SessionStore` with it
(`SessionSubscription.SubscriptionIdentifiers`), restored on a resume, and copied
into every PUBLISH that subscription causes:

- a live delivery, including one that matched several subscriptions of the session
  — one PUBLISH carrying all of their identifiers, deduplicated (§3.3.2.3.8);
- a retained replay;
- a message drained from the offline queue after the next CONNACK;
- a shared-group delivery, for the member the group picked.

A value of 0 (and any value above 268435455) is a Protocol Error and ends the
connection with DISCONNECT `0x82`. The gnety codec refuses the zero while parsing
(it is the only place the value is visible — gmqx keeps no property table, see
`props.go`), so the broker maps that one refusal onto `0x82` and every other
unparsable packet stays `0x81` (Malformed Packet).

Limits: `maxSubscriptionIdentifiersPerSubscription` (8) bounds what one
subscription stores, `maxSubscriptionIdentifiersPerDelivery` (8) bounds one outgoing
PUBLISH property block, and the identifiers a delivery had to leave out are counted
with `Metrics.RecordDroppedMessage("subscription_identifier_limit")`. The property is
hop-local: a PUBLISH from a client that carries it is stripped, and a delivery's
identifiers are attached by the node that writes the PUBLISH, never carried through
`Publication.Props` or across the cluster.


## Enhanced authentication (MQTT 5 AUTH)

MQTT 5 lets a client authenticate with a multi-step exchange instead of a single
user name and password: its CONNECT names an **Authentication Method** and the two
ends trade AUTH packets. gmqx used to advertise no method and answer every AUTH
with DISCONNECT 0x82; it now drives a real exchange through an `AuthExchange`, the
sibling of `AuthManager`:

```go
type AuthExchange interface {
    Method() string
    Begin(ctx context.Context, info gmqx.ConnectInfo) (gmqx.AuthStep, error)
    Continue(ctx context.Context, data []byte) (gmqx.AuthStep, error)
}

broker, _ := gmqx.New(gmqx.Config{
    AuthMethods:   []string{"scram-sha-256"},
    Authenticator: func(method string) gmqx.AuthExchange { /* nil ⇒ unsupported */ },
    ACL:           myACL{},
})
```

`Config.Authenticator` is a factory, not a single exchange: a broker can support
several methods, the CONNECT's method selects one, and `nil` for a method means
"this broker does not support it". `Config.AuthMethods` is the advertised set and
gates the CONNECT before the factory is asked; an empty list accepts whatever the
factory builds.

An `AuthStep` says what happens next:

| `AuthStep.Status` | Broker reaction |
|---|---|
| `AuthContinue` | sends AUTH 0x18 (Continue authentication) with the method and `Data`, then waits for the client's AUTH 0x18 and calls `Continue` with its data |
| `AuthSucceeded` | CONNACK 0x00 with the method and `Data` (or AUTH 0x00 on a live connection); `Info`, when set, is the authenticated identity |
| `AuthFailed` | CONNACK (or DISCONNECT) with `ReasonCode` and close; 0 selects 0x87, 0x86 is the code for bad credentials, 0x8C for a method the broker cannot continue |

The state machine, for a broker that supports `token`:

```
client                                   broker
  │ CONNECT method=token, data=hello       │
  │                                        │ exchange.Begin(ctx, info{AuthMethod, AuthData})
  │ <────────── AUTH 0x18 method=token ────│  (step: continue, data="challenge")
  │            data=challenge              │
  │ AUTH 0x18 method=token, data=answer ──>│ exchange.Continue(ctx, "answer")
  │ <────────── CONNACK 0x00 method=token ─│  (step: succeeded, data="done")
```

With a `Begin` that succeeds immediately there is no AUTH at all: the CONNECT is
answered with CONNACK 0x00 carrying the method and the final data.

Boundaries:

- **Methods are negotiated, not listed.** MQTT 5 §3.2.2.3.8 makes the CONNACK
  Authentication Method an echo of the client's CONNECT (and forbids including one
  the client did not send), so the CONNACK names exactly the method the client
  used. A client that names a method the broker does not support is refused with
  CONNACK 0x8C (Bad authentication method) and the connection closes — that
  refusal is how a client learns it guessed wrong. Authentication Data on a
  CONNECT that carries no method is a Protocol Error (CONNACK 0x82).
- **A connection that used AUTH never reaches `AuthManager`.** The exchange *is*
  the authentication. `AuthManager.Authenticate` is skipped for it, so `DenyAll`
  in the `Auth` field does not block an enhanced-authenticated client; every
  connection without a method still goes through `AuthManager` (and `AllowAll` /
  `DenyAll` behave as before). Authorization is unchanged: `AclManager.Check` runs
  for every publish and subscribe of the connection, against the identity the
  exchange established.
- **The exchange's identity wins.** The `ConnectInfo` of a successful step
  replaces the CONNECT's `Username` and `ClientID` when they are set, and that
  identity is what the broker registers, what `EventSink`/`AclManager` see and what
  the session is keyed by. An empty field keeps the client's value. Every other
  field stays the CONNECT's, because an AUTH packet can carry nothing but the
  method and the data.
- **Bounded.** One exchange runs at most `MaxAuthSteps` (8) challenges and is then
  refused with 0x87 (`RecordDroppedMessage("auth_steps")`) instead of looping. The
  whole CONNECT exchange lives inside `ConnectTimeout`, so a client that never
  answers a challenge is disconnected at that deadline. Each `Begin`/`Continue`
  call is bounded by `AuthTimeout`, and an expired or failing call is a failed
  authentication (0x87), never a success.
- **Re-authentication.** An AUTH 0x19 on a live connection starts a fresh exchange
  (a new value from the factory, then `Begin` with the connection's `ConnectInfo`
  and the packet's data). Success answers AUTH 0x00 and leaves the session — its
  subscriptions, its queued messages and its inflight exchanges — untouched; the
  new exchange may update the `Username` (the `ClientID` is a live session key and
  does not change). A failure ends the connection with a DISCONNECT carrying the
  step's reason code, and a re-authentication that names another method, or an AUTH
  that does not ask to re-authenticate, is a Protocol Error (DISCONNECT 0x82).
  The exchange runs on the connection's own goroutine, so no AUTH packet is ever
  handled concurrently with a PUBLISH or SUBSCRIBE from the same client.

What an implementation has to do: report the method it was asked for from
`Method`, treat `info.AuthData` (CONNECT) and `Continue`'s `data` as opaque
credential bytes, and return exactly one verdict per call. The broker invokes the
callbacks one at a time from the connection's read path and waits for each verdict
before it reads the next packet, so an exchange is ordered with the client's other
packets. Each call receives a context carrying the `AuthTimeout` deadline (with a
positive timeout it runs on a helper goroutine the broker waits for): a callback
may block on a store or a directory, and must respect that context. Returning an
error is a failed authentication, and the broker honours `Info` only on the step
that succeeds.

Behaviour changes for existing deployments:

- a CONNECT that names an Authentication Method when the broker has no exchange
  for it is now refused with CONNACK 0x8C. It used to be accepted (the CONNACK
  carried no method) and the AUTH that followed was answered with 0x82;
- Authentication Data without an Authentication Method is now refused with CONNACK
  0x82 rather than ignored;
- a connection that negotiates no method behaves exactly as before: the CONNACK
  carries no Authentication Method and an AUTH is DISCONNECT 0x82.

## Persistent sessions, the offline queue and QoS 1/2 retransmission

A persistent session (Clean Session cleared; on MQTT 5, Clean Start cleared and a
non-zero Session Expiry Interval) keeps state while it has no live connection:
its subscriptions, the messages published to those subscriptions, and the
deadline after which the state ends. All of it lives behind `SessionStore`, and
the state of a session with no owner is what the store holds — a live connection
owns its session, so a publication reaches a session exactly once.

`SessionStore` (`Load`, `Save`, `Delete`, `Keys`) exists so a deployment can move
session state out of process. `New` installs `MemorySessionStore` when
`Config.SessionStore` is nil, which is exactly the in-memory behavior of earlier
versions and stays the default; `NewRedisSessionStore` is the other implementation
this package ships (see **Shared sessions** below). The broker calls the
store with its own session lock held, so an implementation must not call back
into the broker. Routing does not read the store at all: the broker indexes a
session's subscriptions when it saves it and removes them when it takes it over or
expires it. `Keys` is still required — `New` uses it once to index sessions a
deployment restored before the broker was created, and the expiry sweeper uses it
to find the deadlines to enforce. A store written behind a running broker's back
is not re-read.

**Shared sessions.** By default a session lives on the node that holds it, so a
client that reconnects to another node starts from nothing.
`NewRedisSessionStore` moves that state into Redis, and
`NewRedisClusterWithSessionStore(cfg)` builds the Redis cluster backend and the
store from one `RedisClusterConfig`, so both use the same address, database and
`ChannelPrefix`:

```go
redisCluster, sessions, err := gmqx.NewRedisClusterWithSessionStore(gmqx.RedisClusterConfig{
    Addr: "redis:6379",
})
if err != nil {
    return err
}
defer sessions.Close()
broker, _ := gmqx.New(gmqx.Config{
    NodeID:         "n1",
    ClusterBackend: redisCluster,
    SessionStore:   sessions,
})
```

The store keeps one key per stored session under the cluster prefix:

| Key | Contents | TTL |
|---|---|---|
| `<prefix>:session:<namespaceKey(ns, clientID)>` | JSON `{v, client, expires, subs, queue}`: the session's subscriptions with their options, its queued messages with payload, QoS, retain flag and property block, and its deadline | the session's own deadline |

The key is the one the broker asks for — already namespace-qualified — so two
namespaces sharing one Redis never collide, and the segment `session` shares
nothing with the cluster's own `node`, `owner`, `member`, `subs` and `client`
keys. The TTL is the session, not a cache lifetime: a session with a Session Expiry
Interval expires from Redis at its deadline even if no sweeper runs, a session
without one (MQTT 3.1.1) is written without a TTL, and a state whose deadline
already passed is deleted instead of written. A value that does not decode is
removed and counted (`RecordDroppedMessage("session_store_corrupt")`) so one
broken record cannot fail a CONNECT, and a failed Redis command is counted as
`"session_store_error"` and degrades to "no session" (at-most-once, like the
cluster transport).

Two rules keep one session from being served by two nodes:

- Taking a session is atomic (`Take`, one Redis round trip that reads and deletes),
  so two nodes reconnecting the same client id at the same time cannot both resume
  it. The store also satisfies the optional `SharedSessionStore` interface, which
  the broker uses when a peer's kick displaces a connection: the displaced node
  neither saves its own copy back (which would resurrect a session the peer is
  already serving) nor deletes the key (which would erase the state the peer saved).
  With the default in-memory store the previous behavior is unchanged — the state of
  a kicked session stays where it is, because only that node can reach it.
- A clean CONNECT discards the shared session wherever it is received, and the
  sweeper of each node only expires the keys of its own namespace.

The store carries state, not routing: a publication is queued for a stored session
by a node that has it in its routing index (the node that stored it, or one that
started with it present). As with the node-local store, a publication another node
receives does not queue for an offline session here unless the cluster routes it
there (see the limitations below). `Save` replaces the whole record, and the
broker's session lock only serialises the writers of one node, so two nodes
queueing for the same stored session at the same instant can lose one of them —
the queue of an offline session is effectively owned by the node that stored it.

**Expiry.** MQTT 5's Session Expiry Interval is a deadline:
`ConnectInfo.SessionExpiry` is stamped on disconnect as `now + interval`, and a
bounded sweeper (`SessionSweepInterval`, at most 512 stored sessions per tick)
deletes the session with its subscriptions and its queued messages when the
deadline passes. The CONNECT path checks the deadline as well, so a session that
expired is never resumed even before the sweeper reaches it. A zero interval on
a non-clean session keeps the previous behavior and never expires — that is the
MQTT 3.1.1 case (the protocol has no interval), and on MQTT 5 a zero interval
means the session ends with the connection, so nothing is stored and there is
nothing to expire.

**Queue.** Only a persistent session queues (the reference broker of this family
queues for clean sessions too, which delivers messages to a session that asked
for none; gmqx does not copy that). The queue is a per-session FIFO of what a
PUBLISH needs — topic, QoS, retain flag, payload, MQTT 5 properties — with no
packet id: ids are assigned at drain time. `MaxOfflineQueueSize` bounds one
session and drops its oldest message; `MaxTotalOfflineMessages` bounds the broker
and rejects the newest message. Both drops are counted
(`RecordDroppedMessage("offline_queue_full")`, `RecordDroppedMessage("offline_queue_total")`),
and `RecordQueueDepth("offline", n)` reports the current depth. The queue drains
in order immediately after the CONNACK of the next CONNECT. A message that cannot
get a slot in the inflight window or a free packet id is delivered at QoS 0
instead of stalling the session, which is the degradation the reference broker
applies when its packet ids run out.

**Retransmission.** An outbound QoS 1/2 delivery is kept in a per-connection
inflight table keyed by packet id until the client confirms it. One timer per
connection (not one per message) drives every retransmission: the first retry is
sent after `QoSRetryInterval` (default 5 s), every further attempt doubles the
wait, and `QoSMaxRetries` (default 5) caps the attempts, after which the exchange
is left unconfirmed and its packet id stays reserved. A PUBLISH retransmission
carries the DUP flag and the original packet id (the reference broker never sets
DUP — not copied). QoS 2 runs PUBLISH → PUBREC → PUBREL → PUBCOMP, and the PUBREL
is retried until the PUBCOMP arrives. `MaxInflightQoS1` bounds the unconfirmed
QoS 1 PUBLISHes of a connection; `MaxInflightQoS2` bounds its QoS 2 exchanges in
each direction (an inbound QoS 2 PUBLISH over the window is answered with PUBREC
0x97 Quota exceeded on MQTT 5).

**Inflight queue.** A QoS 1/2 delivery that would exceed its window waits in the
connection's inflight queue, in arrival order, and is sent when the
acknowledgement of an older message frees a slot — a slow subscriber costs memory,
not messages, and the publisher is never blocked. The queue is bounded
(`MaxInflightQueueSize`, default 256): only a delivery that finds it full is
dropped, and that drop is counted twice
(`RecordDroppedMessage("inflight_overflow")`, `RecordOverflow("inflight")`) and
logged at Warn level with the client id, because the publisher's PUBACK has
already promised the message to the sink. The queue is per connection, so it ends
with the connection; its messages are not stored in the session like the offline
queue.

**Writer.** Every packet leaves through one writer goroutine per connection, in
the order it was queued: PUBLISHes, PUBACK/PUBREC/PUBREL/PUBCOMP, SUBACK, PINGRESP.
The queue behind it is bounded (`MaxOutboundQueueSize`, default 256 packets) and
every write carries a deadline (`WriteTimeout`, default 5 s), so a peer that stops
reading cannot hold the publisher's read loop, the will publish of a closing
connection or the single cluster receive goroutine of the node. A full queue drops
a QoS 0 PUBLISH and counts it (`RecordDroppedMessage("outbound_queue_full")`,
`RecordOverflow("outbound_queue")`); for a QoS 1/2 PUBLISH or an acknowledgement it
disconnects the slow consumer instead (`RecordDroppedMessage("slow_consumer")` and
a Warn log), which is the trade-off that prefers a visible reconnect over an
invisible loss. The queue is bounded in packets, not in bytes, so one connection
holds at most `MaxOutboundQueueSize` packets of at most `MaxPacketSize` bytes.
Both goroutines and the retransmission timer end with the connection and are
joined by `close`; the session sweeper is waited for by `Stop`.

Deliberate limitations, all documented above with their reason: an inflight
exchange and the inflight queue behind it are per connection, so a reconnect
resumes the offline queue but not the unconfirmed or queued deliveries of the
previous connection; an inbound QoS 2 exchange is dropped when the connection
closes (as in the reference); the queue is local to the node that received the
publication, and the subscription summary a cluster peer advertises covers live
sessions only, so a publication on another node does not queue for an offline
session here (a shared session store moves the state between nodes, not this
routing).

## Takeover, delayed wills and retained expiry

**Takeover.** A CONNECT with a client id that is already connected is a takeover.
In `UNIQUE` mode the new connection is refused (CONNACK 0x85 on MQTT 5, return
code 2 on 3.1.1). In `KICK` mode it takes the session over: the displaced
connection gets a server `DISCONNECT` with reason code 0x8E (Session taken over)
on MQTT 5 and is closed — MQTT 3.1.1 has no server DISCONNECT and only sees the
close — and its session state moves to the new connection.

`NotKickSeconds` (default 30) is the anti-flap window of that displacement: a
takeover of a connection that began less than the window ago is refused with
CONNACK 0x9F (Connection rate exceeded) on MQTT 5 and return code 3 (Server
unavailable) on 3.1.1, so two ends of one client id cannot displace each other in
a loop. 0x9F is the reason code MQTT 5 keeps for a rate-limited connection; 0x8A
(Banned) would claim an administrative ban, 0x89 (Server busy) a whole-server
overload and 0x9C (Use another server) a redirection this broker cannot offer. A
negative `NotKickSeconds` disables the window and makes every CONNECT take the
session over immediately — the behavior before the window existed, and the one
MQTT itself prescribes.

**Takeover across nodes.** Every successful CONNECT spreads a kick to the
cluster, in both connect modes: a client that reconnects to another node
otherwise leaves a connection behind that duplicates every delivery until its
keep-alive expires. A kick that displaces a connection closes it exactly as a
local takeover does (DISCONNECT 0x8E on MQTT 5) and cancels a delayed will the
receiving node was holding for that client id. The event names a client id, not a
topic, so it goes to every peer, and the usual namespace and `(Origin, Seq)`
deduplication rules apply.

*Kick epochs.* A kick also carries the node that produced it and the connect time
of the connection that produced it (`ClusterMessage.ConnectTime`, Unix nanoseconds
from that node's wall clock), and the receiving node applies it only to a
connection that is strictly older:

| Incumbent on this node | Kick | Result |
|---|---|---|
| older than the kick's epoch | newer | applied (displaced) |
| newer than the kick's epoch | older | refused, `RecordDroppedMessage("cluster_kick_stale")` |
| same instant, this node id greater | equal | refused (this connection wins) |
| same instant, this node id smaller | equal | applied (the peer's connection wins) |
| no epoch (`ConnectTime` 0) | legacy | applied, `RecordDroppedMessage("cluster_kick_legacy")` |

A delayed or reordered kick therefore cannot close a connection that is newer than
the one it was meant to replace, and two nodes that accepted the same client id at
the same instant make the same decision instead of closing both. Refused kicks are
counted and logged at Debug (never silently ignored).
`Config.NotKickSeconds` applies on this side too: a kick that arrives while the
connection it names is still younger than the window is refused
(`RecordDroppedMessage("cluster_kick_rate_limit")`), so two nodes cannot take a
client id away from each other faster than the window allows.

*Clock assumption.* The epoch is a wall-clock reading, because a monotonic reading
means nothing in another process. Cross-node ordering assumes the clocks of a
cluster are NTP-synchronised to within `NotKickSeconds`; skew larger than that
cannot revive a displaced connection (the window refusal bounds it), it can only
make a peer apply or refuse a kick it would have decided differently. Inside one
node nothing crosses clocks: the local window compares the connection's monotonic
start time with `time.Since`.

*Rolling upgrade.* A kick without an epoch comes from a peer built before kick
epochs. It is applied unconditionally — the behavior of that peer's build — and
counted as `"cluster_kick_legacy"`, so a mixed cluster degrades to the previous
semantics instead of refusing every kick. A new node's kick reaching an old peer is
just a kick with an extra JSON field the old build ignores, so the wire stays
compatible in both directions.

**Will Delay Interval.** MQTT 5 lets the WILL of a connection ask for a delay. The
will is published when the earlier of two things happens: the delay elapses, or
the session ends.

- A session that ends with its connection (Clean Start set, or a zero Session
  Expiry Interval) publishes its will immediately, because the delay cannot
  outlive the session it belongs to.
- Otherwise the will is scheduled for `min(Will Delay Interval,
  MaxWillDelayInterval, remaining Session Expiry Interval)`: a session that
  expires inside the delay publishes its will at the expiry.
- A CONNECT that resumes the session cancels the pending will, locally or — via
  the connect-time kick — on the node that still holds it. A CONNECT with Clean
  Start set discards the session instead of resuming it, so the will of the
  previous connection is published at that CONNECT.
- A broker that stops drops what is still pending: there is nowhere left to
  publish it and no timer may outlive `Stop`.

Scheduling is one timer per pending will (not a broker-wide ticker) and it exists
only while the will waits. A delay longer than `MaxWillDelayInterval` (default
5 min) is clamped to the bound and counted with `RecordOverflow("will_delay")`.
The wire value is whole seconds, so a millisecond-scale test shortens the broker's
protocol second (`Config.unit`, unexported, `time.Second` in production).

**Retained message expiry.** The MQTT 5 Message Expiry Interval of a retained
publish is stored as a deadline (publish time plus the interval), not as a value.
On replay the broker skips and deletes a message whose deadline passed
(`RecordDroppedMessage("retained_expired")`) and advertises what is left of the
interval — rounded up, because a rounded-down zero would mean "never expires" on
the wire — instead of replaying the stale published value. A retained message
without an interval never expires. Deletion happens when the broker looks at the
message (the replay of a matching SUBSCRIBE), so a stale retained message nobody
subscribes to costs one map entry until the next matching subscription. A
replicated retained publish is stamped with its arrival on the receiving node,
because the cluster event does not carry the origin's publish time.

**CONNECT and authorization deadlines.** `ConnectTimeout` (default 10 s) arms a
read deadline before the first packet is read and clears it once the CONNACK is
written; the read loop then arms the keep-alive deadline it owns. A client that
opens the socket and sends nothing, or sends its CONNECT too slowly, is therefore
disconnected instead of holding a connection slot, a goroutine and a session
forever. `AuthTimeout` (default 5 s) bounds `AuthManager.Authenticate`, each
`AuthExchange.Begin`/`Continue` call and `AclManager.Check`: the hook runs in its
own goroutine with a context that carries the deadline, and an expired call is a
refusal — a failed authentication (CONNACK 0x87) or a denied authorization
(SUBACK 0x87 / no success PUBACK) — never a success. On a live connection the same
bound is the read deadline of a whole re-authentication exchange, which has no
CONNECT deadline left to live inside. The built-in `AllowAll` hooks take the synchronous path, so a
broker that keeps the defaults pays no goroutine per publish; a negative
`AuthTimeout` disables the bound and calls the hook synchronously, which then has
to honour its context itself.

## Transport layering

TCP/TLS/WebSocket/WSS listeners are provided by the
[`gnety`](https://github.com/maxid/gnety) transport framework (`RawHandler`
mode). The MQTT wire format lives in `gnety/codec/mqtt`: every packet is read
with its typed `Decoder` and written with `mqtt.Encode`, so gmqx owns the broker
state machine and no packet layout of its own.

## Cluster backend

### Gossip backend (default)

`Config.Cluster.Enabled` with no explicit `ClusterBackend` selects the built-in
gossip backend. It needs no external service: a node is seeded with one reachable
peer address and learns the rest of the cluster from the membership gossip.

```go
broker, _ := gmqx.New(gmqx.Config{
    Addr: "10.0.0.1:1883",
    Cluster: gmqx.ClusterConfig{
        Enabled:             true,
        Addr:                "10.0.0.1:7946", // explicit, never :0
        Seeds:               []string{"10.0.0.2:7946"},
        PingInterval:        time.Second,
        PingTimeout:         500 * time.Millisecond,
        SuspicionMultiplier: 3,
        Fanout:              3,
    },
})
```

`ClusterConfig` maps onto `GossipClusterConfig`: `Addr` and `Seeds` are the
cluster listen address and the seed list, `PingInterval` and `PingTimeout` are the
probe timers, `SuspicionMultiplier` defines `SuspectTimeout = SuspicionMultiplier ×
PingInterval` (a suspected member that stays silent that long is declared dead),
and `Fanout` is the epidemic fan-out. `Cluster.Node` and `Cluster.Namespace` seed
`Config.NodeID` and `Config.Namespace` when those are empty. An explicit
`Config.ClusterBackend` always wins, so `NewTCPCluster` and `NewRedisCluster` stay
available (`gmqx.NewGossipCluster` builds the default backend directly).

- **Membership** is SWIM-style and needs no shared store. Every node has a random
  incarnation; each `PingInterval` it probes one random member. A probe with no ack
  within `PingTimeout` is retried indirectly: up to three other members are asked to
  ping the target. A member that also misses that window becomes suspect, and a
  suspect that stays silent for `SuspectTimeout` is declared dead. Membership
  changes travel as bounded deltas piggybacked on every ping/ack (merged by
  incarnation, with the sender's own record always present), so a new node that
  knows one seed converges on the whole view. A false suspicion heals: a node that
  learns it is suspect increments its incarnation number and the newer record wins.
  A dead member stays in the view for `DeadTimeout` and is then removed, which lets
  a restarted process rejoin under the same id.
- **Dissemination** is epidemic. The origin writes an event to at most `Fanout`
  peers (default `min(len(members), 3)`), and each receiver forwards it to `Fanout`
  peers it did not receive it from. The existing `(Origin, Seq)` dedup table makes
  every node apply and forward an event at most once, so one event costs
  O(N·Fanout) frames instead of one frame per pair. With N at or below `Fanout` the
  origin writes every peer and the receivers have nothing left to forward: a 2- or
  3-node cluster degenerates to a full broadcast, which is what it wants.
- **Routing** sits on top of the epidemic. A node advertises its subscription
  summary (filters plus shared-group keys, the same `summarySet` the other backends
  use) as membership metadata. A publish is preferred to peers whose summary
  matches, is skipped for peers whose summary is known not to match, and is sent to
  peers whose summary is unknown (or stale after three `SummaryInterval`s) so the
  epidemic can still reach them. Retained publishes and kicks ignore summaries:
  retained state must exist on every node for its future subscribers, and a kick
  names a client id rather than a topic.
- **Presence** is gossiped too. Every admitted CONNECT claims its client id for
  this node (a tombstone is gossiped on disconnect), with a Lamport version, the
  owning process incarnation and a TTL. `PublishToClient` for a client id held
  elsewhere is written to the claimed owner only; when the claim is absent, expired
  or names a dead/left node, the event falls back to the epidemic so a node that
  adopted the client id still receives it. Claims are namespace-scoped and events
  are deduplicated like every other cluster event.
- **Bounds**: one frame carries at most `MaxGossipDelta` member records and
  `MaxPresenceDelta` presence claims; the dedup table, the presence table and the
  member view are bounded; one link queues at most 32 frames (a full queue drops
  and counts `cluster_backpressure`); fan-out is bounded; there is one writer
  goroutine per link, never one per message.

`Members()` (through `MemberView`) reports `Member{NodeID, Addr, Incarnation,
Since, State}` sorted by node id. `State` is `alive`, `suspect` or `dead`; the TCP
and Redis backends leave it empty, which means alive. A node with no seeds starts
alone, serves local traffic and logs that it is alone.

Member links are plain gnety TCP, exactly like the TCP backend: neither backend
exposes TLS settings for the cluster link.

### TCP backend

`gmqx.NewTCPCluster` connects broker nodes over gnety TCP with versioned JSON
frames over length-prefixed framing:

```go
cluster := gmqx.NewTCPCluster(gmqx.TCPClusterConfig{
    Addr:  "10.0.0.1:7946",
    Seeds: []string{"10.0.0.2:7946", "10.0.0.3:7946"},
})
broker, _ := gmqx.New(gmqx.Config{NodeID: "n1", ClusterBackend: cluster})
```

`Addr` must be explicit (no `:0`) and is the address peers must use in their
`Seeds`. Every node dials peers whose advertised address sorts after its own, so
listing all peers in `Seeds` on every node yields one connection per pair. A pair
that both ends dial (for example because a seed spells the peer differently than
the peer's `Addr`) is collapsed by the handshake, so a node id never has two
members. Failed peers are dropped and retried.

### TCP membership

Every link starts with a versioned handshake frame carrying the node id, the
advertised listen address, the process incarnation (random per `Start`) and the
sender's current member view. The view is advisory — it lets a node notice that a
peer knows it under a different address — while the ordering rules use the
advertised `(NodeID, Addr, Incarnation)` triple:

- A peer that speaks another wire version is refused at the handshake; frames
  from incompatible versions never create a member.
- Exactly one logical member exists per node id. A second link to the same
  incarnation is collapsed to the link dialed by the lower advertised address
  (the same rule that picks the dialer), and the loser is closed and logged.
- A handshake with a new incarnation means the peer restarted: it replaces the
  old link and resets `Since`.
- Heartbeats are sent every `HeartbeatInterval`; a member that misses
  `HeartbeatMissBudget` consecutive intervals is evicted and its link closed. A
  link that never completes the handshake within that window is closed too.
  Reconnect keeps working: the dial loop re-establishes an outbound link and the
  peer re-opens an inbound one.
- Events from a node that is unknown or was evicted are still handed to the
  broker (availability over strictness), but only a handshake can make that node
  a member again.
- `Members()` (optional `MemberView` interface) exposes `Member{NodeID, Addr,
  Incarnation, Since}`; the `Cluster` interface itself is unchanged:

  ```go
  if view, ok := cluster.(gmqx.MemberView); ok {
      for _, m := range view.Members() {
          log.Printf("%s at %s since %s", m.NodeID, m.Addr, m.Since)
      }
  }
  ```

### TCP deduplication and loops

Every locally produced event is stamped with `Origin` (the producing node) and
`Seq` (that origin's monotonically increasing sequence). Receivers keep a bounded,
TTL'd table of applied `(Origin, Seq)` identities and apply a duplicate once. The
table holds `DedupSize` identities (oldest evicted first) for `DedupTTL`; expiry is
swept lazily on insert, so memory stays O(DedupSize). Dropped duplicates, looped
events and forwarded events are counted and reported through
`TCPClusterConfig.Metrics` when one is set.

A received event is never forwarded, and an event whose origin is the receiving
node is dropped, so a re-broadcasting topology cannot loop.

### TCP directed routing

Each node advertises the set of topic filters its local sessions are subscribed
to, refreshed every `SummaryInterval` and immediately when a subscription changes.
A publish is written only to the peers whose summary matches its topic (using the
broker's normal `+`/`#` matching). A peer whose summary is unknown or older than
three intervals receives the publish anyway — that peer alone, never a flood to
everyone. Retained publishes and kicks still go to every peer: retained state must
exist on every node for future subscribers, and a kick names a client id rather
than a topic, so only the node holding that session can act on it.

A directed publish (`Broker.PublishToClient` for a client id this node does not
hold) takes the kick path instead: it names a client id rather than a topic, and
this backend keeps no presence, so it is broadcast to every peer. Each peer
applies it only if it holds that client id in its own namespace, queues it when
that session is offline and persistent, and counts it with
`RecordDroppedMessage("target_unknown")` otherwise. That costs one frame per peer
for a message at most one node can use, and the backend counts the extra frame
writes internally so the trade-off is measurable.

A node advertises at most `MaxSummaryFilters` filters; when it has more, the
filters present longest are left out (newer subscriptions keep routing), at the
cost of not routing publishes that only match an evicted filter. The same frame
carries the shared subscription groups the node holds a live member of (in a summary
set of its own, so many normal filters cannot push the groups out): they are what the
publishing node elects a node per shared group from, and a publish that names a peer
in `ClusterMessage.Shared` is routed to that peer even when its topic filters do not
match.

A retained publish received from a peer is applied to the receiving node's own
retained store with the rules of a local retained publish: an empty payload
deletes the topic, `MaxRetainMessages` bounds the store, and the QoS and
properties carried by the message are kept. That is what makes the broadcast
useful — a subscriber that connects to that node later receives the value from
its store. The guarantee is eventual presence in the store: a remote retained
publish can arrive after a subscriber on that node was already admitted, and the
broker does not replay the value to a session that is already connected.

| Field | Default | Description |
|---|---|---|
| `Addr` / `Seeds` | — | advertised listen address / peers to dial |
| `ReconnectInterval` | 500ms | wait before re-dialing a peer |
| `HeartbeatInterval` | 1s | heartbeat period per link |
| `HeartbeatMissBudget` | 3 | missed intervals before eviction |
| `DedupSize` / `DedupTTL` | 4096 / 30s | duplicate-suppression table bounds |
| `SummaryInterval` | 1s | subscription summary refresh period |
| `MaxSummaryFilters` | 256 | advertised filters per node |
| `Metrics` / `Logger` | none / `slog.Default()` | drop counters / membership log |

Every successful connect broadcasts a kick, in both connect modes, so a client-id
takeover drops the old session even when it lives on another node (`UNIQUE`
decides locally only, because a node cannot see its peers' sessions). The
displaced connection gets a DISCONNECT 0x8E on MQTT 5, and a delayed will the
peer was holding for that client id is cancelled. See the takeover section above.

Delivery between nodes stays best-effort: a publish that arrives while a peer's
summary is unknown is sent to that peer, but a subscriber whose subscription is
not yet visible to the publishing node (for at most one `SummaryInterval`) can
miss a message. The cluster does not synchronise subscriptions across nodes.

## Redis cluster backend

`gmqx.NewRedisCluster` replaces the TCP mesh with a centralized fan-out. Nodes do
not connect to each other at all: they share Redis, and everything the cluster
knows — membership and the routing table included — lives there instead of in
N² peer links.

```go
redisCluster := gmqx.NewRedisCluster(gmqx.RedisClusterConfig{
    Addr:     "redis:6379",
    Password: os.Getenv("REDIS_PASSWORD"), // optional
    DB:       0,                            // optional
})
broker, _ := gmqx.New(gmqx.Config{NodeID: "n1", ClusterBackend: redisCluster})
```

The same Redis can also hold the sessions of the cluster, so a client that
reconnects to another node resumes its subscriptions and its queue instead of
starting from nothing. That is opt-in and built from the same configuration:

```go
redisCluster, sessions, err := gmqx.NewRedisClusterWithSessionStore(cfg) // cfg: RedisClusterConfig
if err != nil {
    return err
}
defer sessions.Close()
broker, _ := gmqx.New(gmqx.Config{NodeID: "n1", ClusterBackend: redisCluster, SessionStore: sessions})
```

See **Shared sessions** in the persistent-session section for the key layout, the
TTL rule and what a takeover does to a shared session.

### Keys and channels

Every key and channel sits under one prefix (`ChannelPrefix`, default
`gmqx:cluster`) that must be identical on every node:

| Key / channel | Contents | TTL |
|---|---|---|
| `<prefix>:node:<nodeID>` | pub/sub channel of one node | — |
| `<prefix>:owner:<nodeID>` | JSON `{node, inc, since}`: node id ownership lease | `NodeTTL` |
| `<prefix>:member:<nodeID>` | JSON `{node, inc, since}`: liveness and identity | `NodeTTL` |
| `<prefix>:subs:<nodeID>` | JSON `{node, filters, shared, updated}`: subscription summary, `shared` being the node's shared subscription groups | `SummaryTTL` |
| `<prefix>:client:<namespaceKey(ns, clientID)>` | `<nodeID>\x00<incarnation>`: which node holds the client id | `PresenceTTL` |
| `<prefix>:session:<namespaceKey(ns, clientID)>` | JSON session record, written by `RedisSessionStore` when it is configured (see [Shared sessions](#persistent-sessions-the-offline-queue-and-qos-12-retransmission)) | the session's deadline |

A node subscribes to its own channel and to nothing else. A publish is one
`PUBLISH` per interested node, so N nodes cost O(N) Redis operations rather than
O(N²) links, and there is no shared channel that every node has to receive and
filter. Only the node whose own channel carries the event applies it.

### Membership

The member key is the whole membership protocol. A node rewrites it every
`SummaryInterval` (and on a local subscription change), and Redis deletes it
`NodeTTL` after the last refresh, so a crashed node leaves the view without a
handshake, an eviction pass or a clean shutdown. `Start` refuses to run when
Redis is unreachable, and a node registers only after its channel subscription is
confirmed, so a peer that sees the member key can already reach it.

`Members()` (the optional `MemberView` interface) reads the member keys, so every
node reports the same membership without sharing process memory: `[]Member`
sorted by node id, one entry per incarnation, `Member.Addr` empty (Redis has no
peer address) and `Since` the moment this incarnation registered. It reports no
peers when Redis cannot be read, rather than a stale view. A restarted node
reappears as a new incarnation; stopping a node deletes its keys so peers drop it
immediately instead of after `NodeTTL`.

Node id ownership is enforced separately from membership. `Start` takes
`<prefix>:owner:<nodeID>` with `SET NX` and `NodeTTL`, renews it with the member
key, and releases it on `Stop`. A second live process that starts with the same
id fails with `gmqx.ErrNodeIDInUse` (naming the incarnation that holds it)
instead of subscribing to the same channel, applying every event twice and
collapsing into one `Members()` entry. The lease is a TTL, so a process that died
without releasing it blocks a restart only until `NodeTTL` expires. The TCP
backend has no such key: its handshake already keeps exactly one member per node
id and it has no shared store in which to arbitrate a lease.

### Directed routing

Each node writes its subscription summary to `<prefix>:subs:<nodeID>` and peers
read it when they build their routing view, which is refreshed at most once per
`SummaryInterval` (a refresh is also what makes `Spread` return an error while
Redis is unreachable). The rules are the TCP backend's rules:

- a publish goes only to the nodes whose advertised summary matches its topic;
- a node whose summary is unknown (no key, or a handler that cannot describe its
  sessions) receives the publish anyway — that node alone, never a flood, because
  a missing summary must not cost a delivery;
- retained publishes and kicks go to every known node channel: retained state has
  to exist on every node for its future subscribers, and a kick names a client id
  rather than a topic, so only the node holding that session can act on it. A
  receiving node stores the retained publish with the same rules as a local one
  (empty payload deletes, `MaxRetainMessages` bounds the store, QoS and properties
  are kept); the guarantee is eventual presence in the store, not a replay to a
  session already connected;
- a node advertises at most `MaxSummaryFilters` filters, and the filters present
  longest are the ones left out;
- the same record carries the shared subscription groups the node holds a live
  member of (`shared` beside `filters`, capped the same way). A publish whose shared
  election names a peer is routed to that peer's channel whether or not its topic
  filters match, exactly as the TCP backend routes it; a peer with an unknown
  summary contributes no groups, so a group it holds is elected to nobody until its
  next summary refresh (at most one `SummaryInterval`), and the publication is then
  dropped and counted at the elected node with `shared_no_member`.

A directed publish (`Broker.PublishToClient`) is routed by presence instead: the
broker reports every CONNECT and every session that really ends, the backend
writes a `client:<id>` record naming this node, refreshes it while the client
stays (and while a persistent session stays stored here), and deletes it with a
compare-and-delete that cannot erase a record another node wrote for the same
client id — and a connect-time kick from a peer releases this node's claim even
when a persistent session stays stored here, because the queue is node-local while
the client itself is connected to the peer. `Spread` then reads that one record and
writes the event to that node's channel only. A missing record means nobody claims the client id — the event is
not broadcast, it is counted with `RecordDroppedMessage("target_unknown")` and
dropped; a directed publish that races a CONNECT by less than one refresh is lost
the same way, which is the at-most-once trade-off of the whole cluster transport.
Kicks deliberately keep their broadcast: a stale presence record must not leave a
zombie connection alive on the node a client actually moved to.

### Deduplication and loops

Identical to the TCP backend and implemented by the same code: every locally
produced event is stamped with `Origin` and `Seq`, receivers drop an event whose
`Origin` is their own node before it reaches the dedup table (so own traffic
cannot pollute it), drop a duplicate `(Origin, Seq)` through the shared bounded
TTL table, and never forward a received event. `RedisClusterConfig.Metrics`
receives the same `cluster_duplicate`, `cluster_loop` and `cluster_forward` drop
reasons.

### Failure behaviour

- `Start` returns an error when `Addr` is empty, a TTL/size is negative, a TTL
  cannot outlive `SummaryInterval`, the node id is owned by another live process
  (`gmqx.ErrNodeIDInUse`), or Redis cannot be reached.
- At runtime a locally produced `Spread` returns `gmqx.ErrClusterUnavailable`
  (wrapping the cause) instead of retrying in place; the broker logs it and moves
  on. A received event is never re-sent, so nothing retries a remote event.
- The receive loop re-subscribes with a bounded backoff (50ms doubling to 1s)
  until `Stop`, keeps exactly one goroutine and one subscription, and logs only the
  transition into and out of the degraded state.
- `Stop` is idempotent, deregisters the node's keys, and waits for both loops, so
  it returns while Redis is down and leaves no goroutine behind.

| Field | Default | Description |
|---|---|---|
| `Addr` / `Password` / `DB` | — / — / 0 | Redis endpoint; `Addr` is required |
| `ChannelPrefix` | `gmqx:cluster` | namespace of every channel and key |
| `SummaryInterval` | 1s | key refresh and routing-view refresh period |
| `SummaryTTL` | 3×`SummaryInterval` | summary key lifetime; expiry means "unknown summary" |
| `NodeTTL` | 3×`SummaryInterval` | member key lifetime; expiry evicts the node |
| `PresenceTTL` | 3×`SummaryInterval` | client-id presence record lifetime; expiry means "no node claims this id" |
| `HeartbeatInterval` | 1s | Redis `PING` period for outage detection and logging |
| `DedupSize` / `DedupTTL` | 4096 / 30s | duplicate-suppression table bounds |
| `MaxSummaryFilters` | 256 | advertised and accepted filters per node |
| `Metrics` / `Logger` | none / `slog.Default()` | drop counters / membership log |

Redis delivery is at-most-once, exactly like TCP delivery: an event published
while a peer is between incarnations is lost, not queued. Node ids must be unique
per cluster: the Redis backend refuses a second live process that starts with an
already-held id (`ErrNodeIDInUse`), and the TCP mesh collapses duplicate links
into one member per node id.

## Versioning

See [VERSIONING.md](VERSIONING.md).

## License

MIT. See [LICENSE](LICENSE).
