# Changelog

> Development snapshots are untagged; entries below are chronological.


## [Unreleased]

- **A gossip cluster backend is the default; `Config.Cluster.Enabled` now does
  something without an explicit backend.**

  *The backend.* `NewGossipCluster` (and `GossipClusterConfig`) implements the
  existing `Cluster`/`ClusterHandler`/`MemberView` contracts with a SWIM-style
  membership service and epidemic event dissemination over gnety's length-prefixed
  TCP transport. Membership: a random incarnation per process, one random direct
  probe per `PingInterval`, up to three indirect probes after `PingTimeout`, a
  suspect window of `SuspicionMultiplier × PingInterval`, incarnation-number
  refutation of false suspicions, bounded member deltas piggybacked on every
  ping/ack, and removal of dead members after `DeadTimeout`. Seeding needs one
  reachable address: the rest of the view arrives through the deltas.

  *Dissemination.* Publishes, kicks, retained updates and directed publishes are
  written to at most `Fanout` peers (`min(len(members), 3)` by default) and forwarded
  by every receiver to peers it did not receive them from; the existing
  `(Origin, Seq)` dedup table terminates the epidemic, so a cluster of N nodes
  costs O(N·Fanout) frames per event instead of N². At N ≤ Fanout the origin writes
  every peer and nothing is left to forward, which is the full broadcast a small
  cluster wants. Subscription summaries (filters and shared-group keys, the same
  `summarySet`) travel as membership metadata and still direct ordinary publishes;
  an unknown or stale summary is included in the epidemic instead of being skipped.
  Client-id presence is gossiped the same way (Lamport version, process
  incarnation, TTL, namespace-scoped), so `PublishToClient` is written to the one
  node that holds the client id and falls back to the epidemic only when that
  claim is absent, expired or dead.

  *Bounds.* One frame carries at most `MaxGossipDelta` member records and
  `MaxPresenceDelta` presence claims; the presence table, dedup table, probe tables,
  dial queue and per-link frame queue are bounded; fan-out is bounded; each link
  has one writer goroutine, never one per message. `Members()` reports the new
  `Member.State` (`alive`/`suspect`/`dead`) sorted by node id.

  *Default selection and configuration.* `Config.Cluster.Enabled` with no
  `Config.ClusterBackend` creates the gossip backend from `ClusterConfig.Addr`,
  `Seeds`, `PingInterval`, `PingTimeout`, `SuspicionMultiplier` and `Fanout`; an
  explicit backend still wins, so `NewTCPCluster` and `NewRedisCluster` keep
  working. `Cluster.Node`/`Cluster.Namespace` now seed `Config.NodeID`/`Config.Namespace`
  when those are empty. A node with no seeds starts alone and logs it.

  *Removed configuration* (none of it was ever read): `Config.Cluster.URL`,
  `Config.Cluster.Port` and `Config.Cluster.ClusterMessageBufferSize`.
  `Config.Cluster.PingTimeout` and `Config.Cluster.SuspicionMultiplier` were dead
  too and are now wired to the gossip backend, and `Cluster.Addr`, `Seeds`,
  `PingInterval` and `Fanout` were added to it.

  *Behaviour changes for existing users.* A deployment that set
  `Cluster.Enabled` without a backend now runs a cluster backend instead of none
  (and must therefore set `Cluster.Addr`); a deployment that set `Cluster.Node` or
  `Cluster.Namespace` now gets them applied instead of ignored. Nothing changes for
  a deployment that installs `NewTCPCluster`/`NewRedisCluster` explicitly. Code
  that set `Cluster.URL`, `Cluster.Port` or `Cluster.ClusterMessageBufferSize`
  no longer compiles, because those fields were removed.

- **MQTT 5 enhanced authentication (AUTH) works; the broker no longer answers every
  AUTH with DISCONNECT 0x82.**

  *The SPI.* A new `AuthExchange` hook (`Method`, `Begin`, `Continue`) is built per
  connection by `Config.Authenticator func(method string) AuthExchange`, so one
  broker can support several methods; `Config.AuthMethods` is the advertised set a
  CONNECT is checked against, and a `nil` factory keeps the previous behavior (no
  method, AUTH is a Protocol Error). `AuthStep` reports `AuthContinue` (the broker
  sends AUTH 0x18 with the step's data and calls `Continue` with the client's
  answer), `AuthSucceeded` (CONNACK 0x00 — or AUTH 0x00 on a live connection —
  carrying the method and the final data, plus an optional `ConnectInfo` with the
  authenticated identity) or `AuthFailed` (CONNACK/DISCONNECT with the step's reason
  code, 0x87 by default). `ConnectInfo` grew `AuthMethod` and `AuthData`, which is
  how the CONNECT's authentication properties reach `Begin`.

  *The handshake.* A CONNECT naming a method the broker has no exchange for is
  refused with CONNACK 0x8C (Bad authentication method) and closed; an exchange's
  first step may succeed immediately (CONNACK 0x00 with the method and its data) or
  continue (AUTH 0x18, one per challenge, each answered by the client's AUTH 0x18 and
  fed to `Continue`). The CONNACK's Authentication Method is exactly the method the
  client used, as §3.2.2.3.8 requires; Authentication Data on a CONNECT without a
  method is a Protocol Error (CONNACK 0x82).

  *Identity.* A successful step's `ConnectInfo` is authoritative: its non-empty
  `Username` and `ClientID` replace the CONNECT's, the session is registered under
  them and the hooks see them. Because the exchange is the authentication, the
  `Auth` (`AuthManager`) hook is skipped for such a connection — `DenyAll` there does
  not block an enhanced-authenticated client — while `AclManager` still authorizes
  every publish and subscribe of it.

  *Re-authentication.* An AUTH 0x19 on a live connection starts a fresh exchange
  over the connection (a new exchange from the factory, `Begin` again), answered with
  AUTH 0x00 on success and a DISCONNECT with the step's reason code on failure; the
  session — subscriptions, offline queue, inflight exchanges — is not touched and the
  `Username` may be updated. A re-authentication naming another method, or an AUTH
  that does not ask to re-authenticate, is a Protocol Error (DISCONNECT 0x82).

  *Bounds.* One exchange runs at most `MaxAuthSteps` (8) challenges and is then
  refused with 0x87 (`RecordDroppedMessage("auth_steps")`). The whole CONNECT
  exchange is inside `Config.ConnectTimeout` (a client that never answers a challenge
  is disconnected at that deadline), each `Begin`/`Continue` call is bounded by
  `Config.AuthTimeout`, and on a live connection the same timeout is the read
  deadline of the whole re-authentication. The exchange runs on the connection's own
  goroutine, so no AUTH is ever handled concurrently with another packet of the
  client.

  Behavior changes for existing deployments:

  - A CONNECT that names an Authentication Method when the broker has no exchange for
    it is now refused with CONNACK 0x8C. It used to be accepted (the CONNACK carried
    no method) and the AUTH that followed was answered with 0x82.
  - Authentication Data without an Authentication Method is now a CONNACK 0x82
    instead of an ignored property.
  - A connection that negotiates no method behaves exactly as before: no
    Authentication Method in the CONNACK and DISCONNECT 0x82 for an AUTH.
  - New `RecordDroppedMessage` reasons: `auth_method` (a CONNECT named an unsupported
    method) and `auth_steps` (the exchange exceeded `MaxAuthSteps`); the new exported
    sentinel `ErrBadAuthMethod` and the constant `ReasonBadAuthMethod` (0x8C) name
    the refusal.

- **Shared subscriptions and subscription identifiers work; the CONNACK advertises
  both as available.**

  *Shared subscriptions.* A `$share/{ShareName}/{filter}` filter is validated per
  MQTT 5 §4.8.2 (non-empty ShareName without `/`, `+` or `#`, then a valid topic
  filter) and refused **per filter** with SUBACK 0x8F when malformed; a filter that
  merely starts with `$share` (`$share` alone) stays an ordinary literal filter, as
  before. A publication matching a group goes to exactly one live member of each
  group, chosen round robin; normal subscribers on the same filter still get their
  own copy; a shared subscription with No Local is refused with 0x8F
  (MQTT-3.8.3-4); no retained message is replayed on a shared subscription (§4.8.2);
  an offline persistent member receives nothing because shared state is never queued
  (the subscription itself survives in the stored session and rejoins the group on a
  resume); the group's round-robin state is dropped with its last member.

  Across a cluster the node that produces the publication elects exactly one node
  per group — `fnv64(groupKey) + a per-group seat` over the sorted nodes that
  advertise a live member — and names it in the new `ClusterMessage.Shared`
  (`SharedDelivery`). The elected node picks the member locally. Both built-in
  backends advertise the group keys in their subscription summary (a new `Shared`
  field beside `Filters` in the TCP summary frame and in
  `redisSummaryRecord`/`redisPeer`), and route a publish to a peer named in
  `Shared` regardless of its topic summary. A publication is delivered at most once
  per group cluster-wide; if the elected node has lost its last member it drops the
  publication and counts `RecordDroppedMessage("shared_no_member")` instead of
  handing it to another node's member. A `Cluster` backend outside this package
  does not advertise groups, so shared subscriptions there stay local to the
  publishing node; a peer from an older build ignores `Shared` and delivers no
  shared copy across nodes — never a duplicate.

  *Subscription identifiers.* A SUBSCRIBE carrying property 0x0B is accepted (the
  DISCONNECT 0x9A is gone). The identifier is stored with the subscription,
  written through the `SessionStore` in the new
  `SessionSubscription.SubscriptionIdentifiers` (additive on both the in-memory and
  the Redis record), restored on a resume, and attached to every PUBLISH that
  subscription causes: live, retained replay, offline-queue drain and shared-group
  delivery. All identifiers of the subscriptions that matched one session are
  merged into a single PUBLISH, deduplicated, and capped at
  `maxSubscriptionIdentifiersPerDelivery` (8, plus 8 stored per subscription);
  identifiers a delivery had to leave out are counted with
  `RecordDroppedMessage("subscription_identifier_limit")`. A value of 0 is a
  Protocol Error: the connection is closed with DISCONNECT 0x82 (the gnety codec
  refuses the value while parsing, and the broker maps that one refusal; every other
  unparsable packet remains 0x81). The property stays hop-local: a client PUBLISH
  that carries it is still stripped, and delivery identifiers are attached by the
  node that writes the PUBLISH.

  Behavior changes for existing deployments:

  - The CONNACK now advertises `SubscriptionIdentifierAvailable = true` and
    `SharedSubscriptionAvailable = true`, and a `$share/...` SUBSCRIBE or a
    Subscription Identifier no longer gets SUBACK 0x9E / DISCONNECT 0x9A. The two
    exported reason-code constants that named those refusals
    (`ReasonSharedSubscriptionsNotSupported`, `ReasonSubscriptionIdentifiersNotSupported`)
    are removed.
  - `SessionSubscription` grew a field. A store written by an older build decodes it
    as "no identifier", which is what those subscriptions had, so a mixed-version
    cluster keeps working in both directions.
  - `Broker.SpreadPublication` now also performs the shared-subscription routing of
    the publication (the local deliveries of the groups this node was elected for),
    so a custom `Dispatcher` that spreads after its own local fan-out keeps getting
    correct shared behaviour without a new call.
  - `ClusterMessage` grew the additive `Shared` field; the TCP summary frame and the
    Redis summary record grew a `shared` list. Older peers ignore both.

- **Kicks carry an epoch, and a Redis-backed store lets the nodes of a cluster share
  persistent sessions.**

  *Kick epochs.* `ClusterMessage` carries `ConnectTime` — the connect time of the
  connection that produced a kick — on `ClusterKick` events. Every successful CONNECT
  sets it, and `Broker.OnRemoteKick` applies a kick only to a connection that is
  strictly older, so a delayed or reordered kick can no longer close a connection that
  is newer than the one it was meant to replace. Equal epochs are broken
  deterministically by node id (the connection on the greater node id wins), so both
  ends of one client id reach the same decision instead of closing both. Refused kicks
  are counted with the new `RecordDroppedMessage` reasons `cluster_kick_stale` (lost
  the epoch comparison) and `cluster_kick_rate_limit` (the incumbent is still inside
  the `Config.NotKickSeconds` anti-flap window), and logged at Debug; a kick that does
  displace still sends DISCONNECT 0x8E and cancels a pending delayed will.

  Behavior notes for existing deployments:

  - `Config.NotKickSeconds` is now enforced on the receiving side as well: a peer's
    kick that arrives while the connection it names is younger than the window is
    refused instead of closing it. A cluster whose clients move between nodes faster
    than the window (which is what the window is for) therefore keeps the old
    connection until the window elapses; set `NotKickSeconds` to a negative value to
    keep the previous, window-less cross-node behavior.
  - A kick received without `ConnectTime` — from a peer built before this change — is
    applied unconditionally, which is that peer's behavior, and counted with
    `cluster_kick_legacy`. A mixed cluster degrades to the previous semantics rather
    than refusing every kick, and the wire stays compatible in both directions
    (the field is additive; an older peer ignores it).
  - Cross-node ordering assumes NTP-synchronised clocks within the anti-flap window;
    the epoch is a wall-clock reading because a monotonic reading is meaningless in
    another process. Within one node nothing crosses clocks (`time.Since`).

  *Shared sessions.* `NewRedisSessionStore` is the second `SessionStore` this package
  ships: it keeps each stored session in the cluster's Redis under
  `<prefix>:session:<namespaceKey(ns, clientID)>`, with the session's own deadline as
  the key's TTL (no TTL for a session without one, and no write at all for a state
  whose deadline already passed). `NewRedisClusterWithSessionStore(RedisClusterConfig)`
  builds the Redis cluster backend and the store from one configuration. A value that
  does not decode is removed and counted (`session_store_corrupt`); a failed Redis
  command is counted as `session_store_error` and degrades to "no session".

  The store satisfies the new optional `SharedSessionStore` interface and the optional
  `Take` method the broker uses to hand a stored session out atomically, so two nodes
  reconnecting the same client id cannot both resume it. A connection that a peer's
  kick displaces now leaves a shared store alone (it neither saves its own copy back
  nor deletes the key), so the node that took the session over owns it; with the
  default in-memory store nothing changes. `MemorySessionStore` stays the default and
  the store seam is otherwise unchanged (`Keys`, `Load`, `Save`, `Delete`), so existing
  deployments behave exactly as before. As with the node-local store, the store moves
  state but not routing: a publication on a node that does not have a stored session in
  its routing index does not queue for it.

- **A subscription index replaces the per-publish scan, and a publication can be
  directed at one client id (locally or across the cluster).**

  *Routing index.* `LocalTargets` no longer walks every live connection and every
  stored session per publication. The broker keeps a routing table
  (`routing_index.go`): literal filters in a map keyed by the filter (one lookup per
  publication), wildcard filters in a map keyed by the filter (one `matchTopic` per
  distinct wildcard filter, not per subscription), and one entry per (filter,
  session) so overlapping filters of a session merge into a single target with the
  highest matching QoS and the OR of retain-as-published. The broker's global lock is
  not taken at all on this path, and `SessionStore` is not called: a session is
  indexed when it is saved and removed when it is taken over, discarded by a clean
  CONNECT or expired. `New` indexes the sessions a store already held, so a
  deployment that restores session state before the broker starts keeps routing to
  them. `matchTopic` no longer splits the filter and the topic into slices, which
  removes the last per-wildcard allocation from the path. Semantics are unchanged:
  the `min(publication, subscription)` QoS cap, no-local, retain-as-published, the
  `$`-topic exclusion of wildcard filters, duplicate-client-id takeover and the
  presence of offline targets (`LocalTarget.Offline()`). Guarantee: a subscription
  is routable before its SUBACK is written, an unsubscribe is unroutable before its
  UNSUBACK, a resumed session's filters are routable before its CONNACK, and a
  session is never both a live and a stored target, so a publication reaches one
  session once. `BenchmarkLocalTargets` shows a flat cost from 10 to 10 000
  subscriptions.

  Behavior note for existing deployments: `SessionStore.Keys` is no longer called by
  routing (it remains the startup index source and the sweeper's enumeration), and a
  store that an external process writes while the broker runs is not re-read — such a
  writer has to preload the store before `New`, as before.

  *Directed publish.* The new `Broker.PublishToClient(ctx, clientID, Publication)`
  delivers one publication to one client id: a live local connection (through
  `SendLocal`, at the publication's QoS and with the connection's inflight window
  respected), a local persistent session with no connection (queued, drained after
  its next CONNACK, under the existing offline-queue bounds), or the cluster. The new
  `ClusterTarget` event carries the client id and the publication, is namespace
  scoped and deduplicated by `(Origin, Seq)` like every other event, and is applied
  by a `ClusterHandler` that implements the optional `targetHandler` interface — the
  broker does. The Redis backend keeps a `clientId -> node` presence record under
  `<prefix>:client:<key>` with the new `RedisClusterConfig.PresenceTTL` (default
  3×`SummaryInterval`), written when the broker reports a CONNECT, kept while a
  persistent session stays stored on the node, and deleted with a compare-and-delete
  when the session really ends or moves to another node (a peer's connect-time kick
  releases the claim even when the persistent session stays stored, because the queue
  is node-local), so a disconnect of an old connection cannot erase the record of a
  client that already moved. A target event is written to that node's
  channel only; a missing record means no node claims the client id and the event is
  counted (`RecordDroppedMessage("target_unknown")`) and dropped, never broadcast —
  the directed publish is at-most-once, and a publish that races a CONNECT by less
  than one presence refresh is lost. The TCP mesh has no presence: it broadcasts a
  target event to every peer exactly like a kick, each peer applies it only if it
  holds that client id (or queues it for its offline persistent session) and counts
  it otherwise. Kicks keep their broadcast on purpose: a stale presence record must
  not leave a zombie connection alive. `PublishToClient` returns an error only for a
  caller mistake (`ErrClientIDRequired`, `ErrNamespaceMismatch`, an invalid topic or
  QoS) or when the cluster backend refused the event; a directed publish does not run
  through the ACL, interceptors, `EventSink` or the retained store.

- **CONNECT-phase and authorization deadlines, capability refusals, delayed wills,
  retained expiry and cross-node takeover.** A batch of parity and security fixes.

  *CONNECT and authorization deadlines (security).* A connection now runs its
  CONNECT phase under a read deadline: `Config.ConnectTimeout` (default 10 s) is
  armed before the first packet is read and cleared after the CONNACK, and the
  read loop then arms the keep-alive deadline it owns. A client that opens a
  socket and sends nothing — or sends its CONNECT too slowly — is disconnected,
  so it can no longer hold a connection slot, a goroutine and a session forever.
  `Config.AuthTimeout` (default 5 s) bounds `AuthManager.Authenticate` and
  `AclManager.Check`: the hook runs in its own goroutine with a context that
  carries the deadline, and an expired call is a refusal — CONNACK 0x87 for
  authentication, SUBACK 0x87 or no success PUBACK for authorization — never a
  success (`RecordDroppedMessage("auth_timeout")`, `"acl_timeout"`). The built-in
  `AllowAll` hooks keep the synchronous path, so the default publish path spawns
  no goroutine; a negative `AuthTimeout` disables the bound.

  *Capabilities the CONNACK advertises as unavailable are refused.* A SUBSCRIBE
  filter starting with `$share/` is no longer stored as a literal filter that can
  never match: it is answered with SUBACK 0x9E (Shared Subscriptions not
  supported) per filter on MQTT 5, as §3.9 requires, and with 0x80 on MQTT 3.1.1
  (`RecordDroppedMessage("shared_subscription")`). A SUBSCRIBE that carries a
  Subscription Identifier is answered with DISCONNECT 0x9A (Subscription
  Identifiers not supported) on MQTT 5 and closes the connection on 3.1.1
  (`RecordDroppedMessage("subscription_identifier")`). An audit of `props.go`
  found these two to be the only capabilities it advertises as unavailable; Retain
  Available, Wildcard Subscription Available and Topic Alias Maximum (16) are
  advertised as available and are implemented. The reason codes are now exported
  as `ReasonSessionTakenOver`, `ReasonSubscriptionIdentifiersNotSupported`,
  `ReasonSharedSubscriptionsNotSupported` and `ReasonConnectionRateExceeded`.

  *Will Delay Interval (MQTT 5).* A will with a delay is published when the delay
  elapses or when the session ends, whichever comes first: a session that ends
  with its connection (Clean Start, zero Session Expiry) publishes its will
  immediately; otherwise the wait is `min(delay, Config.MaxWillDelayInterval,
  remaining Session Expiry)`; a CONNECT that resumes the session cancels the
  pending will, locally or through the connect-time cluster kick, while a CONNECT
  with Clean Start set discards the session and publishes that will right away;
  and `Stop` drops what is still pending. The new `Config.MaxWillDelayInterval` (default
  5 min, negative: unbounded) clamps a longer request and counts it with
  `RecordOverflow("will_delay")`. Scheduling is one timer per pending will.

  *Retained message expiry (MQTT 5).* A retained publish now stores its publish
  time and Message Expiry Interval. A replay advertises what is left of the
  interval (rounded up, because a zero would mean "never expires" on the wire);
  an elapsed message is not replayed and is deleted from the store
  (`RecordDroppedMessage("retained_expired")`). A retained message without an
  interval still never expires.

  *Cross-node takeover.* Every successful CONNECT now spreads a kick, in both
  connect modes, instead of only in `KICK`: a client that reconnects to another
  node would otherwise leave a zombie connection that duplicates every delivery
  until its keep-alive expires. A displaced connection — locally or by a peer's
  kick — receives a server DISCONNECT with reason code 0x8E (Session taken over)
  on MQTT 5 before it is closed, so a client can distinguish a takeover from a
  network drop, and a delayed will pending for that client id is cancelled.

  *`NotKickSeconds` is now enforced.* The field (default 30 s) is the anti-flap
  window of a takeover: in `KICK` mode a CONNECT that would displace a connection
  younger than the window is refused with CONNACK 0x9F (Connection rate exceeded,
  `RecordDroppedMessage("takeover_rate_limit")`) on MQTT 5 and return code 3
  (Server unavailable) on 3.1.1, leaving the incumbent session in place. 0x9F is
  the reason code MQTT 5 reserves for a rate-limited connection; 0x8A would claim
  an administrative ban and 0x89 a whole-server overload.

  Behavior notes for existing deployments: `KICK` mode no longer displaces a
  connection that began less than `NotKickSeconds` ago — set `NotKickSeconds` to a
  negative value to get the previous immediate takeover; `UNIQUE` mode keeps
  deciding locally (a node cannot see its peers' sessions), but every CONNECT now
  also closes a duplicate connection of the same client id on other nodes, so a
  client that roams between nodes no longer receives duplicate deliveries;
  `AuthTimeout` applies to every subscribe and publish of a custom `AclManager`;
  and a client whose `Authenticator` does not answer in `AuthTimeout` is refused.

- **No QoS 1/2 delivery is lost to a full inflight window, and no stalled socket
  blocks the broker.** Two related delivery defects are fixed.

  *Inflight queue.* A QoS 1/2 delivery that finds the connection's inflight window
  full is now queued for that connection and sent when an acknowledgement frees a
  slot, in arrival order. The queue is bounded by the new
  `Config.MaxInflightQueueSize` (default 256); only a delivery that finds it full is
  dropped, and that drop is counted twice
  (`RecordDroppedMessage("inflight_overflow")`, `RecordOverflow("inflight")`) and
  logged at Warn level with the client id. Behavior change: the previous
  `RecordDroppedMessage("inflight_full")` skip is gone — an acknowledged publish is
  no longer silently discarded because the subscriber is slow. The offline-queue
  drain keeps its documented QoS 0 degradation when the window is full.

  *Writer goroutine, bounded outbound queue and write deadline.* Every packet of a
  connection is now written by one writer goroutine, in enqueue order, from a
  queue bounded by the new `Config.MaxOutboundQueueSize` (default 256 packets),
  with a per-packet write deadline from the new `Config.WriteTimeout` (default
  5 s). `SendLocal`, the will publish inside `close`, the cluster receive path and
  every acknowledgement no longer write to the socket on the caller's goroutine,
  so a peer that stops reading cannot hold the publisher's read loop, a closing
  connection or the node's single cluster receive goroutine. A full queue drops a
  QoS 0 PUBLISH (`RecordDroppedMessage("outbound_queue_full")`,
  `RecordOverflow("outbound_queue")`); for a QoS 1/2 PUBLISH or an acknowledgement
  it disconnects the slow consumer instead (`RecordDroppedMessage("slow_consumer")`
  plus a Warn log). The queue is bounded in packets, not bytes. The CONNACK of the
  handshake and a server DISCONNECT are still written synchronously, so both are on
  the wire before the connection closes. The writer goroutine ends with the
  connection, is joined by `close` and `serve`, and `Stop` still waits for every
  broker goroutine.

  Behavior notes for existing deployments: `SendLocal` now returns before the
  packet is on the wire (an error means the packet was not accepted — the
  connection is closed, or the message was dropped as QoS 0 at a full queue); a
  client that does not read its socket is eventually disconnected instead of
  stalling delivery; and a write that exceeds `WriteTimeout` closes the
  connection. `MaxInflightQueueSize`, `MaxOutboundQueueSize` and `WriteTimeout`
  have no unlimited setting: a non-positive value selects the default.

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
  exchanges in each direction; a delivery over the window is queued for the
  connection instead of blocking the publisher (see the entry above, which replaces
  the `inflight_full` skip this entry introduced), and an inbound QoS 2 PUBLISH
  over the window is answered with PUBREC 0x97 Quota exceeded on MQTT 5.
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
