# gmqx

A lightweight, embeddable **MQTT 3.1.1 broker** written in Go.

> Status: **development snapshot (untagged)** — MQTT 3.1.1/5 broker; TCP/TLS/WebSocket/WSS transport via gnety,
> pluggable authentication/ACL/event/metrics hooks, connect modes and limits.
> API may change before v1.0.

## Features

- MQTT 3.1, 3.1.1 and basic MQTT 5 over TCP, TLS, WebSocket and WSS
- CONNECT/CONNACK, SUBSCRIBE/SUBACK, PUBLISH, PING, DISCONNECT
- MQTT 5: CONNACK capability properties, Topic Alias, User Property forwarding,
  subscription options (no-local / retain-as-published / retain-handling), reason codes
- QoS 0/1 basic flows and QoS 2 handshake
- Retained messages, wildcard subscriptions (`+`/`#`), will messages, keepalive
- Pluggable hooks: `AuthManager`, `AclManager`, `EventSink` (lifecycle/messages),
  `MetricsManager`, `Interceptor`
- Connect modes: `UNIQUE` / `KICK` (client id takeover)
- Limits: max connections, max subscriptions, max retained messages,
  connection rate limiting
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
| `MaxConnections` | 0 (unlimited) | concurrent connection limit |
| `MaxTopicSubscriptions` | 0 | per-session subscription limit |
| `MaxRetainMessages` | 0 | retained message count limit |
| `ConnectionRateLimit` | 0 | connections/second token bucket |
| `Auth` / `ACL` / `Sink` / `Metrics` | allow-all / no-op | pluggable SPI |

Full MQTT 5 properties/semantics, full QoS 2 and the client library are on the roadmap (see [ROADMAP.md](ROADMAP.md)).

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

With `ConnectModeKick`, each successful connect broadcasts a kick, so client-id
takeover drops the old session even when it lives on another node.

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
  rather than a topic, so only the node holding that session can act on it;
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
  cannot outlive `SummaryInterval`, or Redis cannot be reached.
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
per cluster; two processes sharing a node id would share a channel and both apply
every event.

## Versioning

See [VERSIONING.md](VERSIONING.md).

## License

MIT. See [LICENSE](LICENSE).
