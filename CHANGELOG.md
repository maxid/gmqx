# Changelog

> Development snapshots are untagged; entries below are chronological.


## [Unreleased]

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
