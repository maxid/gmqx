# Changelog

> Development snapshots are untagged. Entries are grouped by the date the change
> landed, newest first, and each date records the final state of that day's work:
> superseded steps fold into the entry that says where things ended up, not one entry
> per step.

## 2026-11-03

### Added

- **`broker-demo -serve` holds the broker open for a real MQTT client.** The demo
  ended after its scripted steps, which left nothing for MQTTX to connect to. With
  `-serve` it skips the demo and serves an empty embedded broker on `127.0.0.1:1883`,
  moving to the next free port when that one is taken (`gnety.AvailablePortOn`) and
  announcing the move, until Ctrl-C; every accepted connection still gets the
  retained greeting on `<topic>_reply`. The scripted path is unchanged.

- **`Broker.PublishMessage` injects a publication from the host.** An embedding
  program had no way to publish something the broker did not receive from a client —
  a greeting on connect, an alert, a command from another subsystem. It takes the
  ordinary accepted path: a retained publication is stored (and persisted) before
  routing, and `Config.Dispatcher` decides where the live copy goes, so a cluster
  spreads it like any other. It runs no `Interceptor` and no `EventSink.OnPublish`,
  which belong to an inbound connection. `broker-demo` uses it (`EventSink` answers
  every accepted connection with a retained `hi!` on `<topic>_reply`), and
  `cluster-node` uses it to make the cluster observable: the greeting is replicated
  to the peers, so a client subscribing on another node still finds it.

- **Handlers are bound to filters, the way paho binds them.** `client.Subscription`
  gained `Handler`, `Client.AddRoute(filter, handler)` registers a route without a
  subscription, and `Config.OnMessage` became the fallback for publications no route
  claimed (paho's `DefaultPublishHandler`). Every route matching a publication runs,
  in registration order, on the delivery goroutine; a shared subscription registers
  under the inner filter. `docs/CLIENT.md` (and its Chinese version) carries the
  paho → gmqx mapping table, including what a `Token`-based caller has to change.

- **The CI gate covers what local runs covered.** The workflow gained a `-race` job
  (the broker is a concurrency program), an `integration` job that runs the
  container-backed matrix with a Docker daemon, and a short fuzz job over the two
  parsers a peer controls: `FuzzMatch` for topic filters and
  `FuzzDecodePropertyBlock` for MQTT 5 property blocks.

- **`gmqx/client` speaks MQTT 3.1.** `client.Version31` asks for it; the client writes
  its CONNECT as `MQIsdp`/3 and talks 3.1.1 for everything else, which is the mirror
  image of the broker's `normalizeV31Connect` and needs no codec change.

- **`docs/DEPLOYMENT.md`** (and its Chinese version) is the cluster recipe: a
  three-node seed list, the persistence choice with its locker, the `NotKickSeconds`
  takeover window, the listener knobs and what a rolling upgrade does and does not
  keep.

- **A taking node releases the session's route on its peers.** `ClusterSessionReleased`
  (a new cluster event, `session-released`) is broadcast when a node takes a stored
  session, so the node that wrote it drops the offline routing entry it can no longer
  serve instead of queuing publications into a key nobody reads. The receiving node
  checks its own store first and keeps the route when the session is still there, so
  a node-local store behaves exactly as before; a peer that does not know the event
  (an older build) drops it, which is also the old behaviour. The event touches all
  three built-in backends — TCP and gossip flood it like a kick, Redis publishes it —
  and `cluster.SessionReleaseHandler` is the optional handler half, next to
  `TargetHandler`.

- **The cluster × persistence matrix runs against real backends.** Under
  `-tags=integration`, `internal/engine/cluster_persistence_integration_test.go`
  starts `redis:7-alpine` and `ghcr.io/olric-data/olric:latest` with testcontainers and
  runs the same scenarios against the node-local default and both shared stores: a
  retained publish reaches the other node in every case (the cluster replicates it),
  it survives a restart only on a shared store, and a session resumes on another node
  only on a shared store. `TestSoakClusterSharedStore` keeps the soak's connection
  churn and adds sessions migrating between two nodes; the benchmarks in
  `cluster_persistence_bench_integration_test.go` price both paths and
  [PERFORMANCE.md](docs/PERFORMANCE.md) records the numbers.
  [PERSISTENCE.md](docs/PERSISTENCE.md) now states the rule these cases pin: a cluster
  does not pick a store — `Config.Store` stays node-local when unset, cluster or no
  cluster. Test-only dependencies: `testcontainers-go`, `gcache/olric`,
  `olric-data/olric`; `go test ./...` still needs no Docker.

- **A hook can see which listener accepted a connection.** `hook.ConnInfo` gained
  `Transport` — `"tcp"`, `"tls"`, `"ws"` or `"wss"`, the same names `Addrs()` reports
  — so an audit trail, a metrics sink or an ACL can tell a device on the TLS port
  from one on the WebSocket port without keeping a second copy of the broker's
  configuration. The listener name travels from the listener spec through
  `conn.Client` (`SetTransport`, called before the handshake); an embedder that hands
  a connection over itself reports an empty transport.

- **`ClientConfig.TLS` derives the server name from `Addr`.** A configuration with a
  root pool and no `ServerName` used to fail with "either ServerName or
  InsecureSkipVerify must be specified", a requirement the field's documentation did
  not state; the client now fills it from `Addr` the way `tls.Dial` does. An explicit
  `ServerName` is used as given, and the caller's `tls.Config` is never mutated.

- **The interoperability matrix covers the secure and WebSocket listeners.** paho
  runs the same publish/subscribe exchange over TLS, WS, WSS and mutual TLS against
  this broker, and the reference-broker direction gained a TLS listener (with the
  negative case: a client that does not trust the CA must fail).

### Changed

- **The examples teach the client's dispatch model.** `client-demo`, `broker-demo`
  and `plugin-codec` pass their handler on the subscription
  (`client.Subscription.Handler`) instead of `Config.OnMessage`, which is now the
  fallback for publications no route claimed — the same table `Client.AddRoute`
  fills. `client-demo` also takes `-version 3`, so the MQTT 3.1 path the client
  gained has a runnable example.

- **The examples teach the client's dispatch model.** `client-demo`, `broker-demo`
  and `plugin-codec` pass their handler on the subscription
  (`client.Subscription.Handler`) instead of `Config.OnMessage`, which is now the
  fallback for publications no route claimed — the same table `Client.AddRoute`
  fills. `client-demo` also takes `-version 3`, so the MQTT 3.1 path the client
  gained has a runnable example.

- **One persistence seam for sessions and retained messages.** `Config.Store` is a
  [`gcache.RemoteStore`](https://github.com/maxid/gcache) and `Config.Locker` makes it
  a store several nodes write; the broker writes a persistent session (subscriptions,
  offline queue, QoS 2 state, expiry deadline) and a retained message (payload, QoS,
  properties, Message Expiry) through the same key scheme and versioned JSON encoding,
  with each record's own deadline as its TTL. A store that implements `gcache.Scanner`
  is read at start-up (sessions are indexed, retained messages fill the hot table); the
  expiry sweep walks the routing index, so it does not need enumeration. The default is
  gcache's in-process memory store, i.e. exactly the previous in-memory behavior.
  **Breaking:** the `SessionStore` SPI, `MemorySessionStore`, `RedisSessionStore`,
  `RedisSessionStoreConfig`, `SessionState`/`SessionInflight`/`SessionSubscription`/
  `QueuedMessage`/`InflightStage`, `retained.Store`/`NewStore` and `Config.SessionStore`
  are gone; the `contrib/sessionstore-gcache` module is deleted (its logic lives in
  `internal/persist`). A deployment that used `RedisSessionStore` passes `gcache/redis`'s
  store as `Config.Store` and its locker as `Config.Locker`; retained messages now
  survive a restart when a shared backend is configured.

- **The examples are one set of programs with one output shape.** `examples/broker-demo`
  no longer hand-encodes MQTT 3.1.1 frames over `net.Dial`: it embeds the broker and
  drives it with `gmqx/client`, publishing one retained message and letting a second
  client subscribe afterwards, so the demo now shows the retained replay instead of
  repeating what the client library already does. All four examples print one
  `[n/N] action … result` line per step and a closing `[done] …`, stop on Ctrl-C with
  `[stop] interrupted` and exit 0, and take `-h` with commands that can be copied
  verbatim. `examples/README.md` and its Chinese version say, per demo, what it
  answers, how to run it and what it prints.

### Fixed

- **A failed multi-listener `Start` no longer reports addresses of listeners it
  already stopped.** When the second listener failed to bind, `Addrs()` kept the
  address of the first one, which had been stopped: a supervisor could read a port
  that nothing was listening on. The rollback now clears the servers and addresses,
  which is what "a failed Start leaves nothing behind" already meant for the
  single-listener path.

- **`MaxPacketSize` reaches the WebSocket listeners.** The WS and WSS listeners
  passed no message limit to gnety, so they silently kept gnety's 4 MiB default: a
  broker configured with `MaxPacketSize: 8 MiB` served a 5 MiB PUBLISH over TCP and
  TLS and closed the same PUBLISH over WS/WSS with WebSocket status 1009. All four
  transports now take their limit from `Config.MaxPacketSize` — 4 MiB is a library
  default, not a protocol limit (RFC 6455 has no message size limit, and the MQTT
  spec lets one frame carry several or partial control packets), and the other
  transports already obeyed that single number. The limit counts one whole
  WebSocket message, so a sender that bundles several packets into one message is
  held to their sum. `internal/engine/transport_frame_size_test.go` is the
  acceptance case: 5 MiB over ws and wss at an 8 MiB limit, a 2 MiB message
  refused at a 1 MiB limit.

## 2026-11-02

### Added

- **A backend's internals are observable without a test reaching into it.**
  `StatsView` (`Stats() ClusterStats`) replaces the eight per-backend counter
  accessors with one snapshot: drops, the duplicate-table size, forwards, the
  directed-publish routing counters and the fan-out of a target broadcast.
  `LinkView` (`Links() []Link`) reports every physical connection with an `ID`
  that a replacement never reuses, which is what makes "the link set stopped
  changing" observable from outside. `PresenceView` (`Presence(clientID)`) reports
  the node a live presence claim names, and `RoutingView` (`Routed()`) reports what
  every backend routes through a cached view — the Redis fan-out — would do with an
  event right now, which lags `Members()` by up to one `SummaryInterval`.
  `Member` gained `Filters`, `Shared` and `SummaryKnown`, so the advertised
  subscription summary is part of the member view instead of a separate accessor.
  The package's own tests now use these views instead of unexported fields and
  accessors, which is what lets the cluster backends move to `gmqx/cluster` while
  the tests that stay here keep working.

- **Extensions are swappable while the broker runs, and payload encodings are a plugin
  concern.** `Broker.Plugins()` returns a `gplugin.Registry[hook.Host]`; `hook.Host` is one
  setter per slot (`SetAuthManager`, `SetAclManager`, `SetEventSink`, `SetMetricsManager`)
  plus `AddInterceptor`/`AddTranscoder` (each returning its remove function) and `Codecs()`.
  The hooks moved into one immutable snapshot (`Broker.ext`) that is swapped atomically, so
  the publish path still reads one consistent value instead of a field another goroutine
  rewrites. `hook.Codec` (`MediaType`/`Encode`/`Decode`), `hook.CodecSet` and
  `hook.Transcoder` add the data-protocol seam: an application-side plugin registers JSON,
  CSV or a binary format and transcodes a payload on its way out, while the broker keeps
  MQTT opaque — no codec is built in, and a broker without transcoders behaves exactly as
  before. `plugin_codec_test.go` is the acceptance case (JSON→binary, CSV→JSON, and the
  byte-for-byte passthrough after unloading the plugin).

- **The cluster backends live in `gmqx/cluster`.** The gossip, TCP and Redis backends,
  the configuration types, `ErrNodeIDInUse` and the handler collision interfaces moved
  there; the root package re-exports every type and constant as an alias, and keeps the
  constructor forwards (`NewGossipCluster`, `NewTCPCluster`, `NewRedisCluster`,
  `NewRedisClusterWithSessionStore`), so both import paths name the same types and no
  consumer changes. The root package is down to 21 implementation files from 26, and
  the backends carry their own tests (including -race) beside them.

- **`Config.Namespace` is a `gns.Namespace`.** The isolation scope of a broker is now
  the gns namespace value, so a process that runs several brokers — or several
  middlewares — declares each namespace once with `gns.New` and passes the same value
  everywhere. The broker reads `Name` for every key it builds and `Labels` only for
  logs and metrics. The zero value is the default namespace (bare keys, no isolation,
  as before); a non-zero value must pass `gns.Namespace.Validate`, which `New` checks,
  so an invalid name fails at startup instead of producing a key that cannot be read
  back. `Config.Cluster.Namespace` is no longer inherited by the broker: it now only
  scopes a backend that is built on its own, because inheriting it would let two
  brokers sharing one Redis land in the same isolation domain by accident. gmqx now
  depends on gns, a leaf library (R15).

- **The live session state lives in `gmqx/session`, not in the engine.**
  `server/session.go` declared the unexported type `session` next to the store's
  `SessionState`, and the package it lived in was also called `session` — the
  same-name debt of `docs/design/source-layout.md` §4.2. The type is now
  `session.State`: the subscriptions (`session.Subscription`), the queued messages
  (`session.Pending`), the inbound QoS 2 packet ids and the outbound exchanges as an
  `Inflight` projection of the persisted fields. `State.Stored` and
  `StateFromStored` are the single place where typed MQTT 5 properties become the
  wire blocks a store persists, and they are what
  `session/state_test.go` pins: the round trip, the packet-id ordering that makes a
  stored session reproducible, and what a broker refuses to trust from a store it
  did not write (an exchange that cannot have been a QoS 1/2 PUBLISH, an undecodable
  property block, an unknown stage). `server/session.go` keeps only the broker side —
  take, save, sweep and the offline queue's caps — so the stored format, the wire
  behavior and the public API are unchanged.

- **The routing table and the shared-group election live in `gmqx/routing`.**
  `routing.Index` is the broker's routing table (exact and wildcard buckets, live
  filter counts, shared groups with their round-robin cursor, stored-session
  entries), and it no longer holds the engine's connection type: a live session is
  reachable through `routing.Endpoint` — client id, connection id, delivery handle —
  and nothing else, which is also what the displacement rule needs (two endpoints
  compare by identity). `routing.Target` is what a match returns; `LocalTarget`
  stays the name a `Dispatcher` uses (`server.LocalTarget` and `gmqx.LocalTarget`
  are aliases of it), and `SendLocal` accepts only a target the same broker
  produced, now checked through the type's accessors. `routing.Election` is the
  cluster-wide choice of which node delivers a shared publication, with the
  per-group rotation that makes successive publications visit every candidate; the
  broker keeps the local index, the peer view, the metrics and the send as thin
  calls. No stored format, wire behavior or public API changes.

- **The connection state machine lives in `gmqx/conn`.** One physical connection —
  the CONNECT/AUTH handshake, the read loop, the write-behind queue and its writer,
  the outbound QoS 1/2 window with its retransmission timer, and the will it leaves
  behind — was the unexported `type client` inside `gmqx/server`; it is now
  `conn.Client`, and the broker implements `conn.Host` to serve it: the namespace,
  the resolved per-connection `Limits`, the swappable metrics/sink/logger
  snapshots, the authentication entry points, register/unregister, the
  publish/subscribe/retained path, the offline enqueue and the delayed-will
  scheduler. The session handoff stays one critical section (`HandOff`,
  `TakeOver`): the state is taken and the connection stops owning its queue
  together, and the broker writes the store inside that callback, so a delivery can
  never be written to a socket whose session was just stored. The engine keeps the
  pending-will map and its timers, the exchange factory and the session
  bookkeeping, and the byte-rate shaper is `conn`'s as well (`conn.Shape`, with
  `ByteRateMetrics` an alias of conn's identical interface). The connect-refusal
  errors and `MaxAuthSteps` moved with the code that produces them and are
  re-exported, so `errors.Is`, the error values and the root package's exported API
  are unchanged.

- **The engine is `internal/engine` and the root package is one `facade.go`.** What
  assembles the broker — the `conn.Host` implementation, the session bookkeeping,
  the pending-will map and its timers, the plugin host, the hook aliases, the
  session-store facade and the auth exchange factory — lives in one package that
  consumers cannot import, and the root package is a single `facade.go` (plus
  `version.go`) re-exporting every name as an alias, so `gmqx.Broker` ==
  `engine.Broker` and no consumer changes. `gmqx/server`, an intermediate public
  package whose file names duplicated the root facade, is gone. The public API is
  unchanged, verified against the previous commit with `go doc -all` (identifier
  set diff: 0) and with an out-of-module consumer that implements
  `gmqx.Dispatcher` and `gmqx.EventSink`, embeds `gmqx.NoopMetrics` and builds a
  broker.

- `hook.SinkFuncs`: implement only the events that matter; a nil field is skipped and a
  sink without a publish hook accepts the publication.

- `hook.InterceptFunc`: a one-closure `Interceptor`.

- `hook.NoopMetrics` (formerly `NoopMetrics` in the root package): embed it and override
  only the counters that matter.

### Changed

- **The extension contracts moved to `gmqx/hook`.** `ConnectInfo`, `AuthManager`,
  `AuthExchange` (`AuthStatus`/`AuthStep`), `AclManager` (`AclAction`/`SessionInfo`),
  `EventSink` (`PublishEvent`/`PublishResult`/`Disposition`), `MetricsManager`,
  `Interceptor`, `ConnInfo` and the MQTT 5 reason codes are defined there and re-exported
  by the root package as type aliases, so every existing import, name and type assertion
  keeps working unchanged. The contracts now form one reviewable directory instead of a
  section of `hooks.go`, and `gmqx/hook` imports nothing from the broker.

## 2026-11-01

### Added

- **The inbound PUBLISH no longer allocates a packet, a topic string and a payload
  per publish.** The read loop decodes a PUBLISH with `codec/mqtt`'s
  `ReadPublishInto` into one grow-only buffer per connection (`client.readBuf`,
  bounded by the broker's `MaxPacketSize`), and hands everything else — PUBACK,
  SUBSCRIBE, PINGREQ, a read error — to the typed decoder exactly as before. The
  buffer is reused by the next packet, so the publication owns one copy of the
  payload: `client.handlePublish` copies it once, immediately before
  `Broker.publish`, and that is the only place it is copied on the hot path (the
  retained store, the offline queue and the cluster spread keep their own copies,
  as they always did). Everything the publication carries is what it was: the
  same validation and the same DISCONNECT reason codes (an oversize packet is still
  0x95 because the size check still happens before any body byte is read), the same
  topic-alias handling, the same inflight and queueing behaviour, the same
  `PublishEvent` a sink sees, with a payload that outlives the call. Measured on
  the sequential benchmarks: `BenchmarkPublishQoS0` 928 B/op and 9 allocs/op →
  528 B/op and 6 allocs/op, `BenchmarkPublishQoS1` 1,667/13 → 1,264/9,
  `BenchmarkClientPublishQoS1` 3,121/22 → 2,721/18; `ns/op` and the 200-connection
  delivered rate stayed inside their run-to-run noise bands, because at that load
  the profile is syscall-bound, not decode-bound (see `docs/PERFORMANCE.md`).

- **A full inflight queue no longer drops an acknowledged QoS 1/2 delivery of a
  persistent session.** The delivery joins the connection's pending queue and the
  session is taken offline: the connection is closed the way a lost one is (no
  DISCONNECT, nothing written), the normal `unregister` → `SessionStore` path stores
  the session with everything it still owed (the unconfirmed exchanges, the remaining
  offline queue and the pending deliveries behind it, in order), and the next CONNECT
  drains it. The hand-off is counted with `RecordOverflow("inflight_overflow")` and
  logged at Warn level with the client id and that reason; no drop counter moves. A
  delivery that arrives while the hand-off is in flight waits in the same queue or is
  queued through the offline path once the session is stored, so a publisher holding a
  stale target of a closing connection loses nothing. A **clean** session, which has
  nowhere to queue, keeps the drop on one explicit path with its own reason
  (`RecordDroppedMessage("inflight_overflow_clean")`, replacing the drop reason
  `"inflight_overflow"`, which is now an overflow counter, not a drop). The pending
  deliveries of a persistent connection that simply goes away (a lost socket, a
  slow-consumer disconnect) also move into the session queue now, and a delivery that
  fails to reach a closing writer goes back to the front of the pending queue instead
  of being lost. The offline queue's own caps (`MaxOfflineQueueSize` drop-oldest,
  `MaxTotalOfflineMessages` reject-newest) are the only remaining loss points of a
  persistent session, and they were already counted (`offline_queue_full`,
  `offline_queue_total`). No store change was needed: the hand-off is one `Save` of the
  complete state under the broker's session lock, so the Redis store keeps its single
  Lua take.

- Persistence and availability blueprint (`docs/PERSISTENCE.md`, `docs/PERSISTENCE.zh-CN.md`):
  what must survive a process restart and what may not, the `SessionStore` contract a
  backing implementation has to satisfy (atomic take, per-key TTL from the session
  expiry, namespace-scoped keys, exact round-trip, corrupt values dropped and counted)
  with a compilable custom-store example, the shipped Redis blueprint and its failure
  modes, a SQL sketch with the transaction that makes a take atomic, why a cache
  library cannot be a session store, the availability model of a shared store plus a
  gossip cluster, and a sizing worksheet for `MaxOfflineQueueSize` /
  `MaxTotalOfflineMessages`.

- Interoperability matrix in `interop/`, its own module so the library keeps zero
  third-party dependencies: a third-party client against this broker, this client
  against a reference broker in docker, and reference command-line clients against
  this broker. Skips cleanly when docker is unavailable.

- Benchmarks and a soak test with a recorded baseline (`docs/PERFORMANCE.md`):
  publish throughput for QoS 0/1, fanout to 10 and 100 subscribers, 10k
  subscriptions, retained replay and a client round trip, plus a churn soak that
  asserts empty tables, a bounded goroutine count and a delivery ledger.

- **The MQTT client library is here: `github.com/maxid/gmqx/client`.**

  *New package.* `client/` is a full MQTT 3.1.1/5 client built on the standard
  library and `gnety/codec/mqtt` only: `Dial`, `Close`, `Publish`,
  `PublishMessage`, `Subscribe`, `Unsubscribe`, `Connected`, `SessionPresent`,
  `ClientID` and `ServerInfo`, with `Config` (keepalive, clean start/session
  expiry, TLS, dialer, timeouts, inflight window, MQTT 5 receive maximum/maximum
  packet size, reconnect policy, will, callbacks) and a
  `Message`/`Subscription`/`PublishOptions`/`Will`/`ReconnectPolicy`/`ServerInfo`/
  `ReasonError` model.

  *State machines, not a wrapper.* QoS 1 keeps an exchange until PUBACK and QoS 2
  runs PUBLISH/PUBREC/PUBREL/PUBCOMP, retransmitting with DUP and a repeated
  PUBREL up to `MaxRetries` (`ErrRetryLimit` after that) and blocking on the
  inflight window (`MaxInflight`, capped by the server's Receive Maximum) instead
  of dropping or reordering. Inbound QoS 1 is PUBACKed and QoS 2 is delivered
  exactly once per packet id until PUBREL. An unexpected connection loss is
  reported to `OnDisconnect` and, with `Reconnect.Enabled`, reconnected with
  exponential backoff and jitter up to `MaxAttempts`; the client reports
  `OnConnect(sessionPresent)`, replays unconfirmed exchanges and restores
  subscriptions when the server did not keep the session. `Close` never
  reconnects and a server DISCONNECT is not retried. Keepalive sends PINGREQ and
  treats a missing PINGRESP as a dead connection.

  *Bounded, ordered transport.* One reader and one writer goroutine per
  connection, one delivery goroutine and one supervisor, all joined by `Close`;
  no goroutine per message. Every packet crosses one bounded FIFO
  (`OutboundQueueSize`, 256 by default) in issue order and a full queue blocks
  the caller (honouring its context) instead of dropping the packet, so memory is
  bounded and a QoS 1/2 publish is never silently lost.

  *MQTT 5.* Session Expiry, Receive Maximum and Maximum Packet Size in CONNECT;
  user properties and subscription identifiers on SUBSCRIBE; content type,
  response topic, correlation data, user properties, payload format and message
  expiry on PUBLISH; the CONNACK's assigned client id, reason codes, capability
  flags and flow-control limits are surfaced through `ClientID()`/`ServerInfo()`,
  a refusal is a `*ReasonError`, and a packet over the server's Maximum Packet
  Size fails with `ErrPacketTooLarge`.

  *Examples and docs.* `examples/client-demo` runs a subscribe/publish/print/close
  round trip against an in-process broker (or `-addr`), and the client test suite
  covers the QoS handshakes, retained messages, wills, reconnection, keepalive,
  inflight and maximum-packet-size paths against the real broker.

- **MQTT 5 flow control in both directions, QoS 2 state across a reconnect,
  byte-rate shaping and listener port fallback.**

  *Receive Maximum (both directions).* The Receive Maximum a client sends in
  CONNECT now caps the broker's outbound windows: the effective window of each QoS
  is `min(MaxInflightQoS1/2, ReceiveMaximum)`, the two QoS levels share the client's
  window (a PUBLISH confirmed with PUBREC no longer occupies it), and a delivery
  that cannot enter the window waits in the existing inflight queue
  (`MaxInflightQueueSize`) instead of being dropped. The broker's own receive
  maximum is `Config.ReceiveMaximum` (default 64, lowered by `MaxInflightQoS2` when
  that is smaller), it is advertised in the CONNACK and enforced on inbound
  publishes: a QoS 1 publish over it is neither delivered nor acknowledged (the
  client retransmits later) and a QoS 2 one is answered with PUBREC 0x97 and never
  starts an exchange. Drops are counted `receive_maximum_full` /
  `RecordOverflow("receive_maximum")`; the previous `inflight_qos2_full` /
  `inflight_qos2` names are gone.

  *Client Maximum Packet Size.* The CONNECT Maximum Packet Size is stored per
  connection and every packet is measured against it before it is written. A
  packet that does not fit is dropped and counted (`max_packet_size_too_large`) —
  a PUBLISH, a retained replay and a control packet such as a SUBACK take the same
  path — and the connection stays usable. The broker's own inbound limit
  (`MaxPacketSize`) is unchanged and still advertised.

  *QoS 2 across a reconnect.* The inbound packet ids a client published at QoS 2
  and has not released with PUBREL, and the outbound exchanges a session has not
  confirmed, are now part of the stored session (`SessionState.InboundQoS2`,
  `SessionState.Inflight` with `SessionInflight`/`InflightStage`). A resumed session
  suppresses a retransmitted inbound PUBLISH and re-sends the packets it still owes
  under their original packet ids — DUP for a PUBLISH, a PUBREL for a confirmed QoS
  2 publish — ahead of the offline queue, as §4.4 requires. A session that ends
  (Clean Start or expiry) drops both halves. The Redis store round-trips the new
  fields; records written by an older build decode as "no QoS 2 state".

  *Byte-rate shaping.* `MaxReadByteRate`, `MaxWriteByteRate`,
  `MaxTotalReadByteRate` and `MaxTotalWriteByteRate` (defaults 8 MiB/s and 16 MiB/s
  per connection, 256 MiB/s each in total; negative disables) are token buckets with
  a one-second burst (at least 16 KiB). The excess is delayed, not dropped and not
  truncated: a read waits after its bytes arrived and a write before its bytes
  leave, so a burst is slowed by TCP backpressure and the session survives a
  disconnect would have thrown away. Throttled bytes are reported through the new
  optional `ByteRateMetrics` interface and counted as
  `RecordOverflow("byte_rate_read"/"byte_rate_write")`.

  *Port fallback.* `Config.PortFallback` (default 16, negative disables) makes a
  listener whose port is already in use try the next ports instead of failing
  `Start`, for TCP, TLS, WS and WSS alike. Every attempt binds the next port for
  real (no check-then-bind race), the resolved address is logged and reported by
  `Addrs()`, and an ephemeral `:0` bind never falls back.

## 2026-10-31

### Added

- **The gossip cluster backend is the default.** `NewGossipCluster` implements
  `Cluster`/`MemberView` with SWIM membership and epidemic dissemination over gnety
  TCP: a random probe per `PingInterval`, a suspect window, bounded member deltas and
  `Fanout` receivers per event (`min(len(members), 3)` by default), so N nodes cost
  O(N·Fanout) frames instead of N². Subscription summaries and client-id presence ride
  along as membership metadata, which is what lets `PublishToClient` reach the one node
  holding the client id. `Config.Cluster.Enabled` without a backend now builds it from
  `ClusterConfig.Addr`/`Seeds`/`PingInterval`/`PingTimeout`/`SuspicionMultiplier`/`Fanout`;
  a deployment that only set `Enabled` must now set `Addr`, `Cluster.Node` and
  `Cluster.Namespace` take effect instead of being ignored, and the never-read
  `Cluster.URL`/`Cluster.Port`/`Cluster.ClusterMessageBufferSize` are gone.

- **MQTT 5 enhanced authentication works.** `Config.Authenticator` builds an
  `AuthExchange` per connection (`Method`/`Begin`/`Continue`), `Config.AuthMethods` is
  the advertised set and `AuthStep` reports continue (AUTH 0x18), success (CONNACK or
  AUTH 0x00 with the method and its data) or failure. A successful step's `ConnectInfo`
  is authoritative for `Username`/`ClientID` and skips the `Auth` hook (ACLs still run);
  AUTH 0x19 re-authenticates a live connection without touching its session. A CONNECT
  naming an unsupported method is refused with CONNACK 0x8C where it used to be
  accepted, Authentication Data without a method is 0x82, and `MaxAuthSteps` (8),
  `ConnectTimeout` and `AuthTimeout` bound the exchange.

- **Shared subscriptions and subscription identifiers work, and the CONNACK advertises
  both.** A malformed `$share/{name}/{filter}` is refused per filter with SUBACK 0x8F;
  a publication matching a group goes to exactly one live member, round robin, with no
  retained replay and nothing queued for an offline member. Across a cluster the
  producing node elects one node per group (`fnv64(groupKey)` plus a per-group seat over
  the sorted owners) and names it in the new `ClusterMessage.Shared`, so a group is
  delivered once cluster-wide; both backends advertise the group keys in their summary.
  A SUBSCRIBE carrying property 0x0B is accepted, the identifier is stored in
  `SessionSubscription.SubscriptionIdentifiers` and attached to every PUBLISH the
  subscription causes (merged, deduplicated, capped at 8 per delivery), and 0 is a
  protocol error (DISCONNECT 0x82). The two "not supported" reason codes are removed.

- **Kicks carry an epoch, and persistent sessions can live in a shared store.**
  `ClusterKick` now carries `ConnectTime` and `OnRemoteKick` applies a kick only to a
  strictly older connection (equal epochs are broken by node id), so a delayed kick
  cannot close the connection that already replaced it; `NotKickSeconds` is enforced on
  the receiving side too, a kick from an older peer without `ConnectTime` is applied
  unconditionally and counted `cluster_kick_legacy`, and refusals are
  `cluster_kick_stale`/`cluster_kick_rate_limit`. Cross-node ordering assumes
  NTP-synchronised clocks. The Redis session store this entry introduced was later
  folded into the single persistence seam of 2026-11-03.

- **A subscription index replaces the per-publish scan, and a publication can be
  directed at one client id.** Literal filters, wildcard filters and one entry per
  (filter, session) live in a routing table, so a publish no longer walks every
  connection and session, takes no global lock and does not touch the session store on
  the hot path; the semantics (QoS cap, no-local, retain-as-published, `$` exclusion,
  offline targets) are unchanged and `BenchmarkLocalTargets` is flat from 10 to 10 000
  subscriptions. `Broker.PublishToClient(ctx, clientID, Publication)` delivers to a live
  local connection, a local offline session (queued) or the node holding the id, through
  the new `ClusterTarget` event; Redis keeps a presence record with `PresenceTTL`, the
  TCP mesh broadcasts the event and applies it only where the id lives, and the
  directed publish is at-most-once.

- **CONNECT and authorization deadlines, capability refusals, delayed wills, retained
  expiry and cross-node takeover.** `Config.ConnectTimeout` (10 s) bounds the CONNECT
  phase and `Config.AuthTimeout` (5 s) bounds `Authenticate`/`ACL.Check`, where an expiry
  is a refusal (`auth_timeout`/`acl_timeout`) rather than a success. Capabilities the
  CONNACK advertises as unavailable are refused instead of stored: a `$share/` subscribe
  answers SUBACK 0x9E and a subscription identifier answers DISCONNECT 0x9A. A will delay
  is `min(delay, MaxWillDelayInterval, remaining Session Expiry)` and a resume cancels
  it; a retained message keeps its publish time and Message Expiry Interval and is
  replayed with what is left of it. Every CONNECT now spreads a kick in both connect
  modes and a displaced connection gets DISCONNECT 0x8E, while a takeover inside
  `NotKickSeconds` (30 s) is refused with CONNACK 0x9F.

- **No QoS 1/2 delivery is lost to a full inflight window, and no stalled socket blocks
  the broker.** A delivery that finds the window full queues for that connection
  (`Config.MaxInflightQueueSize`, 256) and is sent when an acknowledgement frees a slot;
  only a delivery that finds the queue full is dropped (`inflight_overflow` plus the
  `inflight` overflow counter), replacing the silent `inflight_full` skip. Every packet
  is now written by one writer goroutine from a queue bounded by `MaxOutboundQueueSize`
  (256) under a per-packet `WriteTimeout` (5 s), so a peer that stops reading cannot
  hold the publisher's read loop or the node's single cluster receive goroutine: a full
  queue drops a QoS 0 PUBLISH (`outbound_queue_full`) and disconnects a slow consumer
  for QoS 1/2 (`slow_consumer`).

- **Persistent-session storage, a bounded offline queue and broker-side QoS 1/2
  retransmission.** `SessionStore` (`Load`/`Save`/`Delete`/`Keys`) holds a session with
  no live connection, a bounded sweeper (`SessionSweepInterval`, 30 s) enforces Session
  Expiry and an expired session is not resumed. A persistent session queues what its
  subscriptions match in FIFO order (`MaxOfflineQueueSize` 1000 drops the oldest,
  `MaxTotalOfflineMessages` 100000 rejects the newest) and drains it after the next
  CONNACK; unconfirmed QoS 1/2 deliveries live in a per-connection inflight table,
  retried by `QoSRetryInterval` (5 s, doubling) up to `QoSMaxRetries` (5) with DUP and
  bounded by `MaxInflightQoS1`/`MaxInflightQoS2` (64 each). The stores this entry and the
  kick entry introduced were later folded into the single persistence seam of 2026-11-03.

- **Dispatcher SPI, connection context and namespace isolation.** `Config.Dispatcher` is
  the single decision point for accepted publications (nil selects `DefaultDispatcher`),
  and a custom dispatcher routes through the read-only `LocalTargets`/`SendLocal`/
  `SpreadPublication` seam instead of re-implementing topic matching. Every hook runs
  with the connection-scoped context, which carries `ConnInfo`, is enriched once by
  `Config.ConnContext` and is cancelled with the connection. `Config.Namespace`
  partitions the session table, subscription matching, the retained store and the
  advertised summaries, and both backends drop events of another namespace.

- **Protocol conformance fixes (MQTT 3.1.1 / 5).** A wire-level suite against a real
  broker exposed and closed these: a persistent CONNECT restores its subscriptions and
  answers Session Present; a will keeps its QoS, retain flag and properties and an
  invalid will topic rejects the CONNECT; a delivery uses `min(publication QoS,
  subscription QoS)`; a retransmitted QoS 2 PUBLISH is delivered once; a PUBLISH topic
  must be non-empty and wildcard-free and a leading `#`/`+` no longer matches `$` topics;
  and a protocol error ends an MQTT 5 connection with the matching DISCONNECT reason code
  (0x81, 0x82, 0x90, 0x94, 0x95).

- **Cluster: retained replication and node id ownership.** A retained publish received
  from a peer is now applied to the receiving node's retained store (empty payload
  deletes, `MaxRetainMessages` bounds it) instead of being delivered as a live message
  only, so a subscriber that connects to another node later gets the retained value.
  The Redis backend enforces one live owner per node id: `Start` takes
  `<ChannelPrefix>:owner:<nodeID>` with `SET NX` and `NodeTTL`, renews the lease and
  releases it on `Stop`, and a second process with the same id fails with the new
  `ErrNodeIDInUse` instead of sharing the channel.

- **Redis cluster backend.** `NewRedisCluster` replaces the TCP mesh with Redis: one
  private event channel per node (`<ChannelPrefix>:node:<nodeID>`), membership by a
  member key's TTL (`NodeTTL`), a subscription summary per node (`SummaryTTL`) and one
  `PUBLISH` per node whose summary matches, so N nodes cost O(N) operations instead of
  O(N²) links. It reuses the TCP backend's Origin/Seq dedup, loop guard and drop
  reasons, and `Members()` returns the peers sorted by node id. When Redis is
  unreachable a locally produced spread fails with the new `ErrClusterUnavailable` and
  the receive loop re-subscribes with a bounded backoff; `Stop` is idempotent. It adds
  `github.com/redis/go-redis/v9` (tests use `github.com/alicebob/miniredis/v2`).

## 2026-10-30

### Added

- **Built-in TCP cluster: membership, deduplication and directed routing.** Every link
  starts with a versioned handshake (node id, advertised address, incarnation, member
  view), so one node id has exactly one logical member and a version mismatch is refused
  at the handshake; heartbeats evict a silent member after `HeartbeatMissBudget`
  intervals, and `Members()` (new optional `MemberView`) exposes the view.
  `ClusterMessage` gained `Origin`/`Seq`, a bounded TTL dedup table, no forwarding of
  received events and an origin loop guard. Peers advertise the filters their sessions
  subscribe to (every `SummaryInterval`, pushed on change, capped by `MaxSummaryFilters`)
  and a publish goes only to the peers whose summary matches, while retained publishes
  and kicks still broadcast. The wire format is versioned and incompatible with the
  previous bare JSON frames.

- **Packets go through the typed codec.** `mqtt.Decoder` and `mqtt.Encode` read and write
  every packet and the connection loop switches on the message type, so `packet.go` and
  `properties.go` are gone (a new `props.go` only converts MQTT 5 properties for the sink
  and cluster frames). Wire-visible differences are all spec-correct: the CONNACK no
  longer advertises the invalid `Maximum QoS 2`, a v5 UNSUBACK carries one reason code
  per filter, and absent v5 defaults are omitted. MQTT 3.1 keeps working through a
  CONNECT rewrite.

- **H1: result-bearing publish.** `EventSink.OnPublish` receives a `PublishEvent` and
  returns `PublishAccepted`/`PublishRetryable`/`PublishRejected`; PUBACK/PUBREC is sent
  only after an accepted result, so a rejected publish is never acknowledged
  optimistically (MQTT 3.1.1 has no negative ack), `Config.PublishCallbackTimeout`
  bounds the sink wait, and retained state is written only for accepted publishes.

- Publisher identity: `ConnectionID` is a process-unique physical connection id,
  distinct from the client id that survives reconnects.

- MQTT 3.1 (`MQIsdp`, protocol level 3) CONNECT compatibility; v3.1 client-id rules.

## 2026-10-29

### Added

- **A single-node MQTT 3.1.1 broker.** A TCP listener,
  CONNECT/SUBSCRIBE/PUBLISH/PING/DISCONNECT, QoS 0/1/2, retained messages, wildcard
  subscriptions, will and keepalive.

- **Extension SPIs and connection governance.** `AuthManager`, `AclManager`,
  `EventSink`, `MetricsManager` and `Interceptor` are the seams; `ConnectMode`
  (`UNIQUE`/`KICK`, both covered by tests), the connection/subscription/retained
  limits and the connection rate limit are the bounds; metrics counters and the
  lifecycle callbacks (connect/disconnect/lost/ping) report them.

- **Four listeners: TCP, TLS (server certificate, optional client certificate),
  WebSocket and WSS.** The WebSocket listener validates the `mqtt` subprotocol and
  takes its path from `Config.WebsocketPath`, `Addrs()` reports each listener's
  resolved address, and TLS/WebSocket integration tests cover the transports.

- **MQTT 5.** CONNECT with properties, the v5 CONNACK/SUBACK/UNSUBACK formats, the
  PUBLISH property block (topic alias inbound and per-session, plus forwarded user
  properties, content type, response topic and correlation data), CONNACK capability
  properties, the subscription options (no-local, retain-as-published,
  retain-handling) and the negative ACK reason codes (Not authorized, Quota exceeded,
  Client Identifier not valid), with tests for v5 connect/pub-sub/auth.

- **The cluster seam.** `Cluster`/`ClusterMessage` and the broker hooks that publish
  and kick across nodes, with an in-memory two-node test.

### Changed

- The library README/docs are product-neutral and self-contained.
- **Transports come from gnety.** The TCP/TLS/WS/WSS listeners are gnety's
  `RawHandler`; the MQTT codec and the broker state machine stay in gmqx.
