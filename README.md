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
  subscription-aware directed routing, cross-node publish/kick, reconnect)

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

## Versioning

See [VERSIONING.md](VERSIONING.md).

## License

MIT. See [LICENSE](LICENSE).
