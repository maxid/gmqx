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
  that never completes its CONNECT, `AuthTimeout` bounds `Authenticate` and
  `ACL.Check` and treats an expired call as a refusal
- Wire-level MQTT 3.1.1/5 conformance suite (`TestConformance`)
- Pluggable hooks: `AuthManager`, `AclManager`, `EventSink` (lifecycle/messages),
  `MetricsManager`, `Interceptor`
- Pluggable routing: `Dispatcher` receives a `Publication` for every accepted
  publish and decides where it goes; the nil dispatcher keeps the built-in local
  delivery plus cluster spread (`DefaultDispatcher`), and the broker exposes a
  read-only seam (`LocalTargets`, `SendLocal`, `SpreadPublication`) so a
  dispatcher never re-implements topic matching
- Connection-scoped context for every hook: `ConnInfo` (client id, connection
  id, namespace, remote address, protocol level) through
  `ContextWithConn`/`ConnFromContext`, enriched once at CONNECT by
  `Config.ConnContext` and cancelled when the connection ends
- Namespace isolation: `Config.Namespace` scopes the session table, subscription
  matching, retained state and the cluster subscription summaries/events, so a
  publish of one namespace never reaches a session of another; the default empty
  namespace keeps single-namespace deployments unchanged
- Connect modes: `UNIQUE` / `KICK` (client id takeover, DISCONNECT 0x8E for the
  displaced MQTT 5 client, `NotKickSeconds` anti-flap window, and a connect-time
  kick that closes a duplicate connection on another node in both modes)
- Capabilities the CONNACK advertises as unavailable are refused, not silently
  ignored: a `$share/...` filter gets SUBACK 0x9E (0x80 on MQTT 3.1.1) and a
  SUBSCRIBE carrying a Subscription Identifier gets DISCONNECT 0x9A
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
| `NotKickSeconds` | 30 | anti-flap window of a takeover in `KICK` mode (negative: no window) |
| `ConnectTimeout` | 10 s | CONNECT-phase read deadline; a socket that says nothing is closed |
| `AuthTimeout` | 5 s | bound on `Authenticate` and `ACL.Check`; an expiry is a refusal (negative: no bound) |
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
| `SessionStore` | `MemorySessionStore` | persistence seam for offline session state |
| `Auth` / `ACL` / `Sink` / `Metrics` | allow-all / no-op | pluggable SPI |
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
versions; this package ships no network or disk backend. The broker calls the
store with its own session lock held, so an implementation must not call back
into the broker, and `Keys` is required because routing to offline sessions and
expiring them both need to enumerate the stored keys.

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
session here.

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
keep-alive expires. The receiving node displaces its copy exactly as a local
takeover does (DISCONNECT 0x8E on MQTT 5) and cancels a delayed will it was
holding for that client id. The event names a client id, not a topic, so it goes
to every peer, and the usual namespace and `(Origin, Seq)` deduplication rules
apply. `NotKickSeconds` is enforced on the node that receives the CONNECT only:
the peer side cannot know how old this node's connection is, which would need a
cluster presence view (see [ROADMAP.md](ROADMAP.md)).

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
forever. `AuthTimeout` (default 5 s) bounds `AuthManager.Authenticate` and
`AclManager.Check`: the hook runs in its own goroutine with a context that carries
the deadline, and an expired call is a refusal — a failed authentication
(CONNACK 0x87) or a denied authorization (SUBACK 0x87 / no success PUBACK) —
never a success. The built-in `AllowAll` hooks take the synchronous path, so a
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

### Membership

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

### Deduplication and loops

Every locally produced event is stamped with `Origin` (the producing node) and
`Seq` (that origin's monotonically increasing sequence). Receivers keep a bounded,
TTL'd table of applied `(Origin, Seq)` identities and apply a duplicate once. The
table holds `DedupSize` identities (oldest evicted first) for `DedupTTL`; expiry is
swept lazily on insert, so memory stays O(DedupSize). Dropped duplicates, looped
events and forwarded events are counted and reported through
`TCPClusterConfig.Metrics` when one is set.

A received event is never forwarded, and an event whose origin is the receiving
node is dropped, so a re-broadcasting topology cannot loop.

### Directed routing

Each node advertises the set of topic filters its local sessions are subscribed
to, refreshed every `SummaryInterval` and immediately when a subscription changes.
A publish is written only to the peers whose summary matches its topic (using the
broker's normal `+`/`#` matching). A peer whose summary is unknown or older than
three intervals receives the publish anyway — that peer alone, never a flood to
everyone. Retained publishes and kicks still go to every peer: retained state must
exist on every node for future subscribers, and a kick names a client id rather
than a topic, so only the node holding that session can act on it.

A node advertises at most `MaxSummaryFilters` filters; when it has more, the
filters present longest are left out (newer subscriptions keep routing), at the
cost of not routing publishes that only match an evicted filter.

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

### Keys and channels

Every key and channel sits under one prefix (`ChannelPrefix`, default
`gmqx:cluster`) that must be identical on every node:

| Key / channel | Contents | TTL |
|---|---|---|
| `<prefix>:node:<nodeID>` | pub/sub channel of one node | — |
| `<prefix>:owner:<nodeID>` | JSON `{node, inc, since}`: node id ownership lease | `NodeTTL` |
| `<prefix>:member:<nodeID>` | JSON `{node, inc, since}`: liveness and identity | `NodeTTL` |
| `<prefix>:subs:<nodeID>` | JSON `{node, filters, updated}`: subscription summary | `SummaryTTL` |

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
  longest are the ones left out.

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
