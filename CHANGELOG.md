# Changelog

> Development snapshots are untagged; entries below are chronological.


## [Unreleased]

- **Persistent-session storage, bounded offline queue and broker-side QoS 1/2
  retransmission.** Two parity gaps of the reference broker of this family are
  closed together, because a queued offline message is pointless if delivery after
  the reconnect is never confirmed or retried. Defaults are bounded and chosen
  here; every earlier behavior that a deployment relied on is reachable.

  *Session store seam.* `SessionStore` (`Load`, `Save`, `Delete`, `Keys`) now
  holds the state of a session with no live connection: subscriptions, queued
  messages and the expiry deadline. `Config.SessionStore` defaults to
  `MemorySessionStore`, which is the in-memory behavior of earlier versions, and
  no network or disk backend ships here. `Broker.sessions` is gone; a connected
  session owns its state, so a publication reaches a session exactly once.

  *Session expiry.* MQTT 5's Session Expiry Interval is now enforced:
  `ConnectInfo.SessionExpiry` is stamped as a deadline on disconnect, a bounded
  sweeper (`SessionSweepInterval`, default 30 s, at most 512 sessions per tick)
  deletes expired sessions with their subscriptions and queued messages, and the
  CONNECT path refuses to resume a session whose deadline passed. A zero interval
  on a non-clean session keeps the previous behavior and never expires (MQTT 3.1.1
  has no interval); on MQTT 5 a zero interval still ends the session with the
  connection.

  *Offline queue.* A persistent session with no live connection keeps the
  messages its subscriptions match, per session in FIFO order, and receives them
  right after the next CONNACK with packet ids assigned at drain time. A message
  that cannot get a free packet id or an inflight slot is delivered at QoS 0
  rather than dropped or reordered. `MaxOfflineQueueSize` (default 1000) drops the
  oldest message of a full session; `MaxTotalOfflineMessages` (default 100000)
  rejects the newest one at the global cap. Clean sessions never queue (the
  reference queues for them too — that is not copied). Both drops are visible
  through `MetricsManager` (`offline_queue_full`, `offline_queue_total`), and
  `RecordQueueDepth("offline", n)` reports the current depth.

  *Outbound QoS 1/2.* Every unconfirmed QoS 1/2 delivery is kept in a
  per-connection inflight table keyed by packet id, driven by one timer per
  connection and not one goroutine per message. `QoSRetryInterval` (default 5 s)
  is the base interval, doubled per attempt, and `QoSMaxRetries` (default 5) caps
  the attempts. Retransmitted PUBLISHes carry DUP (the reference never sets it —
  not copied); QoS 2 runs PUBLISH → PUBREC → PUBREL → PUBCOMP with the PUBREL
  retried until PUBCOMP. `MaxInflightQoS1` (default 64) bounds the unconfirmed
  QoS 1 deliveries of a connection and `MaxInflightQoS2` (default 64) its QoS 2
  exchanges in each direction; a delivery over the window is skipped and counted
  (`inflight_full`) instead of blocking the publisher, and an inbound QoS 2
  PUBLISH over the window is answered with PUBREC 0x97 Quota exceeded on MQTT 5.
  `MaxInflightQoS2` was declared but read by nothing before this change.

  Behavior notes: a clean session no longer accumulates anything; the session
  sweeper runs one goroutine per broker and `Stop` waits for it; the inflight
  exchange and the inbound QoS 2 state are per connection, so a reconnect resumes
  the offline queue but not the unconfirmed deliveries of the previous connection;
  `LocalTargets` may return an offline target (`LocalTarget.Offline`), and a
  custom `Dispatcher` that loops over all targets now reaches offline sessions as
  well. A deployment that wants the previous "retry nothing, drop nothing" flow
  sets `MaxInflightQoS1` and `QoSMaxRetries` to negative values.

- **Dispatcher SPI, connection context and namespace isolation.** Three
  portability gaps from the reference broker of this family are closed with Go
  equivalents; every existing deployment keeps its behavior because the zero
  values select the previous path.

  *Dispatcher.* `Config.Dispatcher` (`Dispatcher`, `Publication`) is now the
  single decision point for accepted publications. The previous hardcoded path
  is `DefaultDispatcher` (local matching sessions plus the cluster spread), so a
  nil dispatcher changes nothing; a custom dispatcher can suppress delivery or
  route through the new read-only broker seam (`LocalTargets`, `SendLocal`,
  `SpreadPublication`) instead of re-implementing topic matching. Retained state
  is still written by the broker before routing.

  *Connection context.* Every hook now receives the connection-scoped context
  instead of `context.Background()`: it carries `ConnInfo`
  (`ContextWithConn`/`ConnFromContext`), is enriched once at CONNECT by the new
  `Config.ConnContext`, is reused for the whole connection and is cancelled when
  the connection closes. The disconnect hooks observe the cancelled context,
  which still carries the ConnInfo of the connection that ended.

  *Namespaces.* `Config.Namespace` (already present) now partitions the session
  table, subscription matching, the retained store and the advertised
  subscription summaries. `ClusterMessage` carries the namespace, and both
  built-in backends drop a remote event or summary of another namespace. The
  empty namespace is the default and keeps the previous behavior, including the
  Redis key layout.

  Behavior notes: a cluster spread failure is now logged as
  `gmqx dispatch failed` (the error is returned by the dispatcher), and a remote
  publication is deliberately not handed to the dispatcher again — it was
  already routed by its origin, and re-dispatching it would put it back on the
  cluster.

- **Protocol conformance fixes (MQTT 3.1.1 / 5).** A wire-level conformance suite
  (`TestConformance`, `mqtt_conformance_test.go` and
  `mqtt_conformance_v5_test.go`) walks the MQTT 3.1.1 and MQTT 5 feature matrix
  against a real in-process broker. It exposed the following defects, fixed
  here; the cases that hit a documented limitation skip with a reason instead.

  *Persistent sessions.* A CONNECT with Clean Session (MQTT 5: Clean Start)
  cleared now restores the stored subscriptions and answers Session Present; a
  clean CONNECT discards the stored session. MQTT 5 sessions are stored when the
  CONNECT carries a non-zero Session Expiry Interval. Timed expiry is still open
  (see ROADMAP.md).

  *Will fidelity.* A will is published with its configured QoS, RETAIN flag and
  its PUBLISH-valid properties instead of QoS 0 without either flag. An invalid
  will topic rejects the CONNECT (CONNACK 0x90 on MQTT 5, close on 3.1.1), and
  an MQTT 5 DISCONNECT with reason 0x04 publishes the will.

  *Subscription QoS is a maximum.* A delivery uses
  `min(publication QoS, highest matching subscription QoS)`, so a QoS 1
  publication no longer reaches a QoS 0 subscription as QoS 1.

  *QoS 2 duplicate suppression.* A retransmitted QoS 2 PUBLISH (same packet id,
  DUP set) is answered with PUBREC again but delivered once; PUBREL frees the
  packet id for reuse.

  *Topic rules.* A PUBLISH topic name must be non-empty and free of wildcards;
  invalid topic filters are rejected (SUBACK 0x8F on MQTT 5, connection close on
  3.1.1); and a leading `#`/`+` no longer matches reserved `$` topics.

  *MQTT 5 error reporting.* Protocol errors now end the connection with a
  DISCONNECT carrying the matching reason code: 0x81 malformed packet, 0x82
  protocol error (empty topic without an alias), 0x90 topic name invalid, 0x94
  topic alias invalid and 0x95 packet too large. MQTT 3.1.1 closes instead, as
  it has no server DISCONNECT.


- **Cluster: retained replication and node id ownership.** Two defects in the
  shared cluster path are fixed.

  *Retained replication.* A retained publish received from a peer is now applied
  to the receiving node's retained store with the rules of a local retained
  publish (empty payload deletes, `MaxRetainMessages` bounds the store, QoS and
  properties are kept) instead of being delivered as a live message only. The
  broadcast both backends already performed is therefore useful: a subscriber
  that connects to another node later receives the retained value from that
  node's store. The guarantee is eventual presence in the store; a subscriber
  already admitted when the remote retained publish arrives is not replayed the
  value. Both backends are affected; the store update is shared by the broker,
  and a duplicate `(Origin, Seq)` is still dropped by the backend before it
  reaches the store.

  *Node id ownership.* The Redis backend now enforces one live owner per node id.
  `Start` takes `<ChannelPrefix>:owner:<nodeID>` with `SET NX` and `NodeTTL`
  before it subscribes to the node channel, renews the lease with the member key,
  and releases it on `Stop` (compare-and-delete, so a process that lost the lease
  cannot release a newer owner's id). A second live process with the same id
  fails `Start` with the new `ErrNodeIDInUse`, naming the holding incarnation,
  instead of sharing the channel, applying every event twice and collapsing into
  one `Members()` entry. Because the lease is a TTL, a process that died without
  releasing it blocks a restart only until `NodeTTL` expires. The TCP backend is
  unchanged: its handshake already keeps exactly one member per node id and it
  has no shared store in which to arbitrate a lease.

  *Behavior changes.* `RedisClusterConfig.NodeTTL` now also governs the ownership
  key. A Redis cluster whose node id is already held by a live process no longer
  starts; callers can match the refusal with `errors.Is(err, ErrNodeIDInUse)`.
  A process that loses its lease (for example after a pause longer than
  `NodeTTL`) stops refreshing its member key and logs the loss instead of
  overwriting the new owner's membership.

- **Redis cluster backend.** `NewRedisCluster` adds a centralized alternative to
  the O(N²) TCP mesh: nodes share Redis instead of dialing each other, and
  membership, routing and directed delivery live in Redis.

  *Layout.* `<ChannelPrefix>:node:<nodeID>` is a node's private event channel —
  the only channel it subscribes to; `<ChannelPrefix>:member:<nodeID>` holds
  liveness and identity with `NodeTTL`; `<ChannelPrefix>:subs:<nodeID>` holds the
  subscription summary with `SummaryTTL`. A publish is one `PUBLISH` per node
  whose summary matches its topic, so N nodes cost O(N) operations instead of
  O(N²) links, and no shared broadcast channel exists for publishes.

  *Same semantics as the TCP backend.* Origin/Seq stamping, the shared bounded TTL
  dedup table, the origin-before-dedup loop guard, "a received event is never
  forwarded", the drop reasons (`cluster_duplicate`, `cluster_loop`,
  `cluster_forward`), summary matching with the oldest-filters cap and the
  retained/kick broadcast rules are reused rather than reimplemented; the summary
  computation moved to `cluster_summary.go` and is now shared by both backends.

  *Membership.* The member key TTL is the whole membership protocol: a node that
  stops refreshing leaves every view when its key expires, with no eviction pass
  and no clean shutdown required. `Members()` (optional `MemberView`) reads the
  member keys, returns `[]Member` sorted by node id (peers only, `Addr` empty
  because Redis has no peer address) and reports no peers when Redis cannot be
  read. A restart is a new incarnation; `Stop` deletes the node's keys so peers
  drop it immediately.

  *Failure behaviour.* `Start` fails when Redis is unreachable or the config
  cannot be honoured (empty `Addr`, negative TTL/size, TTL that cannot outlive
  `SummaryInterval`). At runtime a locally produced `Spread` returns the new
  `ErrClusterUnavailable` (wrapping the cause) instead of retrying in place, and
  the receive loop keeps re-subscribing with a bounded backoff (50ms→1s) until
  `Stop`; only transitions into and out of the degraded state are logged. `Stop`
  is idempotent and leaves no goroutine behind.

  *Dependencies.* `github.com/redis/go-redis/v9` (client) and, for tests only,
  `github.com/alicebob/miniredis/v2`.

- **Built-in TCP cluster: membership, deduplication, directed routing.** The
  cluster is no longer a bare flood of `ClusterMessage` payloads.

  *Membership.* Every link now starts with a versioned handshake frame (node id,
  advertised listen address, random per-`Start` process incarnation and the
  sender's member view); a peer speaking another wire version is refused, so a
  version mismatch cannot create a second logical member. Exactly one logical
  member exists per node id: a duplicate link to the same incarnation collapses
  to the link dialed by the lower advertised address and is logged, a new
  incarnation (peer restart) replaces the old link and resets `Since`. Heartbeats
  run every `HeartbeatInterval` and a member that misses `HeartbeatMissBudget`
  intervals is evicted and its link closed; a link that never handshakes within
  the same window is closed. `Members()` (new optional `MemberView` interface,
  `Member{NodeID, Addr, Incarnation, Since}`) exposes the view without changing
  the `Cluster` interface. Events from an unknown or evicted node are still
  handled but never resurrect it.

  *Deduplication.* `ClusterMessage` gains additive `Origin` and `Seq` fields: the
  cluster stamps every locally produced event with its node id and a
  monotonically increasing per-node sequence. Receivers keep a bounded, TTL'd
  table of applied `(Origin, Seq)` identities (`DedupSize`, `DedupTTL`, lazy
  sweep, oldest evicted first) and apply a duplicate once. A received event is
  never forwarded and an event whose origin is the receiving node is dropped, so
  a re-broadcasting topology cannot loop. Drops are counted and reported through
  the optional `TCPClusterConfig.Metrics`.

  *Directed routing.* Peers advertise the topic filters their local sessions are
  subscribed to, refreshed every `SummaryInterval` and immediately when a
  subscription changes (the broker gained the push path). Publishes go only to
  peers whose summary matches the topic; a peer whose summary is unknown or stale
  still receives the publish — that peer alone, never a flood. Retained publishes
  and kicks keep going to every peer (retained state must exist everywhere; a
  kick names a client id, not a topic). The summary is capped at
  `MaxSummaryFilters` per node, oldest filters evicted first.

  *Behavior changes.* The wire format is versioned and incompatible with the
  previous bare-JSON frames: mixed-version nodes refuse each other at the
  handshake instead of half-working. `Spread` returns an error when the cluster
  is not running, refuses an event that carries another node's `Origin`, and
  re-stamps `NodeID`/`Origin` with the local node id on locally produced events.
  Routing is summary-based, so a subscriber that appears after the publisher's
  last summary update can miss a message for at most one `SummaryInterval`;
  delivery remains best-effort.

- **Packet layer on the typed codec.** gmqx no longer parses or builds MQTT
  packets byte by byte: `mqtt.Decoder` reads every packet, `mqtt.Encode` writes
  every packet, and the connection loop switches on the concrete message type
  (`*mqtt.Publish`, `*mqtt.Subscribe`, ...) instead of the fixed-header byte.
  `packet.go` and `properties.go` are deleted; the new `props.go` holds only the
  seam that converts the typed MQTT 5 property set to and from the property
  bytes `PublishEvent.Properties` and `ClusterMessage.Props` carry. Public API is
  unchanged. Wire-visible differences, all spec-correct: the CONNACK no longer
  advertises the invalid `Maximum QoS 2` (an absent value already means QoS 2),
  MQTT 5 UNSUBACK carries one reason code per filter, and MQTT 5 unacknowledged
  defaults (reason code 0, empty property block) are omitted as the
  specification allows. MQTT 3.1 keeps working: its CONNECT is rewritten to the
  3.1.1 spelling before the codec sees it.

- **H1: result-bearing publish.** `EventSink.OnPublish` now receives a
  `PublishEvent` (connection id, client id, topic, payload, QoS, packet id,
  retain, properties) and returns a `PublishResult` (`PublishAccepted` /
  `PublishRetryable` / `PublishRejected` + MQTT 5 reason code). PUBACK/PUBREC is
  sent **only** after an accepted result: MQTT 3.1.1 has no negative ack, so a
  retryable or rejected publish is left unacknowledged rather than acknowledged
  optimistically. `Config.PublishCallbackTimeout` bounds the sink wait and makes
  an expired wait retryable. Retained state is now written only for accepted
  publishes, so a rejected message cannot become the retained value.

- Publisher identity: `ConnectionID` is a process-unique physical connection id,
  distinct from the client id that survives reconnects.

- MQTT 3.1 (`MQIsdp`, protocol level 3) CONNECT compatibility; v3.1 client-id rules.


- Cluster interface (`Cluster`/`ClusterMessage`) and broker hooks for cross-node
  publish and kick; in-memory two-node test.


## [0.6.0-dev.1] - 2026-10-29

- Transport migrated to gnety (`RawHandler`) for TCP/TLS/WS/WSS; MQTT codec and
  broker state machine remain in gmqx.


- Transport migration: TCP/TLS/WebSocket/WSS listeners now come from the gnety
  transport framework (RawHandler); MQTT codec and broker state machine stay in gmqx.


## [0.5.0] - 2026-10-29

- MQTT 5 CONNACK capability properties (retain/wildcard/subscription-id/shared/max-qos/max-packet/topic-alias).
- Topic Alias handling (inbound and per-session resolution).
- Publish property forwarding (User Property, content type, response topic, correlation data).
- Subscription options: no-local, retain-as-published, retain-handling.
- Tests for v5 CONNACK properties, topic alias and user property forwarding.


## [0.4.0] - 2026-10-29

- Basic MQTT 5 support: CONNECT with properties (skipped), v5 CONNACK/SUBACK/UNSUBACK formats,
  v5 PUBLISH property block, v5 negative ACK reason codes (Not authorized, Quota exceeded,
  Client Identifier not valid), tests for v5 connect/pub-sub/auth.


## [0.3.0] - 2026-10-29

- Multi-listener support: TCP, TLS (server cert/optional client cert), WebSocket and WSS.
- WebSocket subprotocol validation and path configuration.
- TLS/WS integration tests; listener address accessors.


## [0.2.3] - 2026-10-29

- Documentation cleanup: the library README/docs are product-neutral and self-contained.


## [0.2.2] - 2026-10-29

- Bump version constant; include connect mode tests.

## [0.2.1] - 2026-10-29

- Add tests for UNIQUE/KICK connect modes.

## [0.2.0] - 2026-10-29

- Broker SPIs: AuthManager, AclManager, EventSink, MetricsManager, Interceptor.
- ConnectMode UNIQUE/KICK, connection/subscription/retained limits, connection rate limiting.
- Metrics counters and lifecycle callbacks (connect/disconnect/lost/ping).

## [0.1.0] - 2026-10-29

- Initial single-node MQTT 3.1.1 broker: TCP listener, CONNECT/SUBSCRIBE/PUBLISH/PING/DISCONNECT,
  QoS 0/1/2 basic flows, retained messages, wildcard subscriptions, will and keepalive.
