English | [简体中文](PERSISTENCE.zh-CN.md)

# Persistence and availability

This document is the operator's answer to two questions that a broker's
acknowledgement raises the moment a publisher trusts it:

1. **What does a QoS 1/2 PUBACK promise?** The broker accepted the message and
   took responsibility for delivering it to every matching session. This document
   says exactly which part of that responsibility survives a process restart, a
   node loss and a store failure, and which part does not.
2. **How does a deployment stay available while that is true?** With session state
   in a shared store and a gossip cluster, losing a node costs reconnects, not
   sessions. The sections below say why, and what the trade-offs are.

The seam is `SessionStore`, the shipped implementation is the Redis one, and the
rest of this document is the contract, the deployment rules, two further blueprints
(SQL, and the boundary between a cache and a session store), the availability model,
and a sizing worksheet.

## What must survive a restart

A **persistent** session is the only thing a broker keeps beyond a connection.
MQTT 3.1.1 says so with Clean Session cleared; MQTT 5 with Clean Start cleared plus
a non-zero Session Expiry Interval. Its state lives behind `SessionStore` while no
connection owns it, and `SessionState` is the complete list:

| State | Field | Must survive a restart? | Why |
|---|---|---|---|
| Subscriptions, with QoS, No Local, Retain As Published, Retain Handling | `SessionState.Subscriptions` | **Yes** | A resumed session that forgot its filters would silently deliver nothing. `Session Present` in the CONNACK is a promise about them. |
| Subscription Identifiers | `SessionState.Subscriptions[*].SubscriptionIdentifiers` | **Yes** | They are state of the subscription, not of the connection (MQTT 5 §3.8.3.1): a resumed session must keep tagging deliveries with them. |
| Messages queued while the session was offline | `SessionState.Queue` | **Yes** | They were acknowledged to their publisher. This is the offline queue, drained in order right after the next CONNACK. |
| Inbound QoS 2 packet ids whose PUBREL never arrived | `SessionState.InboundQoS2` | **Yes** | A publisher that reconnects and retransmits one of them must get the same PUBREC and no second delivery (MQTT §4.4). |
| Unconfirmed outbound QoS 1/2 exchanges, with their packet id and stage | `SessionState.Inflight` | **Yes** | They are re-sent on resume with DUP (PUBLISH) or as PUBREL, under their original packet ids (MQTT §4.4). |
| Session expiry deadline | `SessionState.ExpiresAt` | **Yes** | It is what ends the state. A store that loses it keeps sessions alive forever. |
| The connection: socket, reader/writer goroutines, keep-alive, topic aliases | — | **No** | A connection is not state a broker can move; the client reconnects. |
| The inflight window, the pending queue *as such*, packet-id allocations | — | **No** | The window is per connection and is rebuilt. The **messages** in it are not lost: a persistent session's pending deliveries move into `SessionState.Queue` before the session is stored (see [the inflight queue](#the-inflight-queue-is-not-a-loss-point)). |
| Negotiated limits (Receive Maximum, Maximum Packet Size), authentication session | — | **No** | They are renegotiated by the next CONNECT. |
| Retained messages | `Broker.retained` | **No, by design until expiry** | The retained store is node-local and in-process; retained messages are held until their MQTT 5 Message Expiry Interval elapses or they are cleared. A restart loses them. |
| A delayed will waiting on its Will Delay Interval | `Broker.wills` | **No** | `Stop` drops what is still pending: there is nowhere left to publish it and no timer may outlive the broker. |
| Cluster membership and routing summaries | cluster backend | **No** | They are rebuilt by the backend (gossip handshake, or a TTL'd record in Redis). |

The rule behind the table: **state that a client asked to be kept and that a
publisher was told was accepted must be in the store; state that belongs to a
connection or that the broker owns by node must not be.** Nothing that the broker
acked to a publisher may live only in the memory of the process that could restart.

### The inflight queue is not a loss point

A QoS 1/2 delivery that finds a connection's inflight window full waits in that
connection's `pending` queue (`MaxInflightQueueSize`). When a **persistent**
session's pending queue overflows, the broker does not drop the delivery: it takes
the session offline — the connection is closed the way a lost one is, the normal
unregister path stores the session, and everything the connection still owed (the
unconfirmed exchanges, the remaining offline queue and the pending deliveries
behind it) is stored with it — and the next CONNECT drains it. The event is counted
as `RecordOverflow("inflight_overflow")` and logged at Warn level with the client id
and the reason `inflight_overflow`; no drop counter moves.

The same holds for a persistent connection that simply goes away (a lost socket, a
slow-consumer disconnect): its pending deliveries move into the session queue before
it is stored.

A **clean** session has no store entry and no queue — its state ends with the
connection, which is what declaring a session clean means — so the overflow drops
the delivery there, on one explicit path, counted as
`RecordDroppedMessage("inflight_overflow_clean")`. Clean sessions trade durability
for statelessness; that is the trade, not a defect.

The only loss points a persistent session keeps are the offline queue's own caps,
`MaxOfflineQueueSize` (per session, drop-oldest) and `MaxTotalOfflineMessages`
(broker-wide, reject-newest), counted as `offline_queue_full` and
`offline_queue_total`. They are a capacity limit, and they are the numbers the
[sizing worksheet](#sizing-worksheet) is about.

## The seam

`SessionStore` is four methods and one type, and it is deliberately small:

```go
type SessionStore interface {
	Load(key string) (SessionState, bool)
	Save(key string, state SessionState)
	Delete(key string)
	Keys() []string
}
```

* `Load` returns the state stored under `key`, or `false`. A corrupt value is
  reported as "no session" — never as a panic, and never as a partially decoded
  state.
* `Save` replaces the whole value. It is called with the broker's session lock
  held, so the store never sees two of its own `Save` calls for one key from one
  node concurrently.
* `Delete` removes the key; deleting a missing key is a no-op.
* `Keys` enumerates the stored sessions. Routing never uses it: the broker indexes
  a session's subscriptions when it saves it, so a publish never calls the store.
  `Keys` is used once by `New` (to index sessions a deployment restored before the
  broker existed) and by the expiry sweeper (which has to enumerate the deadlines it
  enforces). A store that cannot enumerate its sessions cannot satisfy the seam.

The broker never calls back into itself from inside the store, and the store must
never call back into the broker. `SessionState` is handed over, not shared: a store
must return state the caller may keep and must not hand out a map or a slice it
still writes to. The payload and property **bytes** of a queued message are
immutable, so sharing those is fine.

### Atomic take

A store that several nodes share needs one more operation, expressed as the
optional interface

```go
type sessionTaker interface {
	Take(key string) (SessionState, bool)
}
```

(`sessionTaker` is unexported because only the broker decides when a session is
taken; the method name is exported so an implementation outside the package can
provide it.) `Take` must load **and** remove in one atomic step. `Load` followed by
`Delete` is two round trips, so two nodes reconnecting the same client id at the
same instant can both see the session and both serve it — two live connections with
one client id, duplicate deliveries, and both halves of the QoS 2 state moving in
different directions. A shared store must implement `Take`; the broker falls back to
`Load`+`Delete` only for node-local stores, where its own session lock already
serialises the race.

`Take` and `Load` must together observe the session-expiry deadline: a session whose
`ExpiresAt` has passed is never resumed, whether or not the sweeper has deleted it
yet. The broker checks the deadline itself, and a store that can enforce it (a TTL,
see below) saves the sweeper a pass and makes an expired session unreachable even
when no broker is running.

### SharedSessionStore

```go
type SharedSessionStore interface {
	SharedAcrossNodes() bool
}
```

The broker uses this answer in exactly one decision: what a connection displaced by
a peer's kick may do to the store. With a shared store the peer that now holds the
client id also owns the stored session, so the displaced node must neither save its
own copy back (that would resurrect a session the peer is serving and let two nodes
serve one session) nor delete the key (that would erase the state the peer saved).
With a node-local store the previous behavior stays: the state of a kicked session
is kept where it is, because only that node can ever reach it.

### Rules a backing implementation must satisfy

1. **Atomic take.** `Take` loads and removes in one step (a Lua script, a
   transaction, a compare-and-delete). Without it, two nodes can own one session.
2. **Per-key TTL from the session expiry.** `SessionState.ExpiresAt` is the
   deadline, not a hint: write it as the key's TTL (or as a column the sweeper can
   enforce). A state whose deadline already passed is deleted instead of written. A
   session without a deadline (MQTT 3.1.1, which has no Session Expiry Interval) is
   written without a TTL and lives until the broker deletes it.
3. **Namespace-scoped keys.** The broker passes `namespaceKey(Config.Namespace,
   ClientID)`, already qualified, and expects the store to use it verbatim behind
   its own prefix. Two namespaces must never collide, and a store shared by two
   brokers of different namespaces must keep their sessions apart. `Keys` returns
   keys in the form the broker passed in (without the store's own prefix), and the
   broker skips keys of another namespace.
4. **Exact round-trip.** A value that comes back must be the value that went in:
   every subscription option, every Subscription Identifier, the full payload and
   MQTT 5 property block of every queued message, the inbound QoS 2 packet ids, the
   outbound exchanges with their packet ids and stages, the client id and the
   deadline. Version the stored form so a future field can be told apart from a
   value written by an older build.
5. **A corrupt value is dropped and counted, never fatal.** A value that does not
   decode is removed and counted (`RecordDroppedMessage("session_store_corrupt")`),
   and the broker keeps serving the other sessions. A failed command is counted
   (`"session_store_error"`) and degrades to "no session" — an at-most-once
   behavior, never a panic and never a CONNACK failure caused by the store.
6. **Bounded calls.** The broker calls the store with its session lock held, so
   every operation must be a bounded round trip. Configure a dial and an I/O
   timeout; an unreachable store must cost a bounded wait, not an unbounded one.
7. **No callbacks into the broker.** The store is a leaf.

### A custom store

The seam is small enough that a custom store is a page of code. This one is the
in-memory reference shape — the four methods, the optional atomic take, and the
copying rules; swap the map for the client of your choosing:

```go
package mypolicy

import (
	"sync"

	gmqx "github.com/maxid/gmqx"
)

// The store satisfies the seam, and provides the atomic take a shared store needs.
var _ interface {
	gmqx.SessionStore
	Take(string) (gmqx.SessionState, bool)
} = (*mapStore)(nil)

// mapStore keeps stored sessions in this process. It is node-local, so it must
// NOT report itself shared (SharedSessionStore): a peer's kick then leaves the
// stored session where it is.
type mapStore struct {
	mu       sync.Mutex
	sessions map[string]gmqx.SessionState
}

func (s *mapStore) Load(key string) (gmqx.SessionState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[key]
	if !ok {
		return gmqx.SessionState{}, false
	}
	return copyState(state), true
}

func (s *mapStore) Save(key string, state gmqx.SessionState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[string]gmqx.SessionState)
	}
	s.sessions[key] = copyState(state)
}

func (s *mapStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, key)
}

func (s *mapStore) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.sessions))
	for key := range s.sessions {
		keys = append(keys, key)
	}
	return keys
}

// Take hands one session to exactly one caller: the load and the delete are one
// critical section, which is the property a shared store has to provide across
// processes (a Lua script, a transaction or a compare-and-delete).
func (s *mapStore) Take(key string) (gmqx.SessionState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[key]
	if !ok {
		return gmqx.SessionState{}, false
	}
	delete(s.sessions, key)
	return copyState(state), true
}

// copyState detaches a state from the caller: the mutable maps and slices are new,
// the payload and property bytes are shared because nothing mutates them once a
// message is queued.
func copyState(state gmqx.SessionState) gmqx.SessionState {
	if len(state.Subscriptions) > 0 {
		subs := make(map[string]gmqx.SessionSubscription, len(state.Subscriptions))
		for filter, sub := range state.Subscriptions {
			sub.SubscriptionIdentifiers = append([]uint32(nil), sub.SubscriptionIdentifiers...)
			subs[filter] = sub
		}
		state.Subscriptions = subs
	}
	state.Queue = append([]gmqx.QueuedMessage(nil), state.Queue...)
	state.InboundQoS2 = append([]uint16(nil), state.InboundQoS2...)
	state.Inflight = append([]gmqx.SessionInflight(nil), state.Inflight...)
	return state
}
```

Then `Config.SessionStore: &mapStore{}`. A store that several processes really
share additionally implements `SharedAcrossNodes() bool { return true }`, and its
`Take` must be atomic in the backing system rather than in a mutex.

## Blueprint A — Redis (shipped)

`NewRedisSessionStore(RedisSessionStoreConfig)` is the implementation this package
ships, and `NewRedisClusterWithSessionStore(RedisClusterConfig)` builds it together
with the Redis cluster backend from one configuration, so both use the same address,
database and `ChannelPrefix`:

```go
redisCluster, sessions, err := gmqx.NewRedisClusterWithSessionStore(gmqx.RedisClusterConfig{
	Addr:          "redis.internal:6379",
	Password:      os.Getenv("REDIS_PASSWORD"),
	ChannelPrefix: "gmqx:prod",
})
if err != nil {
	return err
}
defer sessions.Close()

broker, err := gmqx.New(gmqx.Config{
	NodeID:         "n1",
	Namespace:      "tenant-a",
	ClusterBackend: redisCluster,
	SessionStore:   sessions,
	SessionSweepInterval: 30 * time.Second,
	MaxOfflineQueueSize:  1000,
	MaxTotalOfflineMessages: 100000,
})
```

The store satisfies the optional interfaces: `Take` is one Lua script that reads and
deletes the key in one round trip, and `SharedAcrossNodes` reports true. Its stored
value is an explicit versioned JSON record (`{v, client, expires, subs, queue,
in_qos2, inflight}`, keys `<prefix>:session:<namespaceKey(ns, clientID)>`), so a
change of a Go field name cannot change the stored layout.

### Recommended deployment

| Concern | Recommendation | Why |
|---|---|---|
| Durability | `appendonly yes`, `appendfsync everysec` | A restart of Redis replays the AOF, so acknowledged sessions and queued messages come back. `everysec` bounds the loss to about a second of writes; `always` costs latency on every `Save`, and `no` loses the tail of the AOF on a crash. |
| Replication | at least one replica per shard | The session state is only as available as the store. A single Redis is a single point of failure for *durability*, not for *delivery* (see [the availability model](#availability-model)). |
| Failover | sentinel or cluster mode in front of the address you configure | The store is one address per node; it does not do sentinel discovery itself. Put a sentinel-aware proxy or a cluster-aware client endpoint there and let it move the primary. |
| Key prefix | one `ChannelPrefix` per environment and cluster, identical on every node | It namespaces both the cluster's keys (`node`, `owner`, `member`, `subs`, `client`) and the session keys (`session`), so two deployments can share one Redis without ever addressing each other's state. |
| Namespace | `Config.Namespace` per tenant, or one broker set per tenant | The store's key already contains the broker's namespace key, so tenants cannot collide inside one prefix. Isolation is still a policy decision: one namespace per deployment is simplest, several are supported. |
| TTL | leave it to the store; it uses `ExpiresAt` as the key's TTL | An expired session disappears from Redis at its deadline even if no broker runs. Do not put a shorter, cache-like TTL on the key: that is data loss (see [blueprint C](#blueprint-c--a-cache-library-as-the-store)). |
| Memory | `sessions × (queue length × payload + subscriptions + overhead)`, plus headroom | The worksheet below turns messages-per-session and payload into a number. `maxmemory-policy noeviction` for the database the store lives in: eviction of a session key is silent data loss, and the store cannot tell it happened. |
| Timeouts | `DialTimeout`/`IOTimeout` (default 2 s each) | The broker calls the store with its session lock held; an unreachable Redis must cost a bounded wait. |
| Observability | alert on `session_store_error` and `session_store_corrupt` | `session_store_error` rising means sessions are being silently degraded to "no session"; `session_store_corrupt` means the stored format and the reading build disagree. |

### What it buys

* **Cross-node resume.** A client that reconnects to any node of the cluster takes
  its session from Redis: subscriptions, Subscription Identifiers, the offline
  queue, the inbound QoS 2 packet ids and the unconfirmed outbound exchanges. The
  CONNACK reports `Session Present`, and the resume re-sends the unconfirmed
  exchanges under their original packet ids (MQTT §4.4).
* **Survives a broker restart.** A process that restarts (deploy, panic, OOM kill)
  finds its sessions where the previous process left them — including the messages
  it had acknowledged and queued, and the messages whose inflight queue overflowed
  and took the session offline.
* **Turns "node loss" into "reconnect".** With replication and failover in front of
  it, losing a broker node costs its clients one reconnect each, not their sessions.

### Failure modes

| Failure | What happens | What to watch |
|---|---|---|
| Redis unreachable when a node starts | `Start` fails when the Redis cluster backend is configured: it pings Redis before claiming membership. Without the cluster backend the broker starts, and every store call fails from the first CONNECT on. | `Start`'s error; then `session_store_error` |
| Redis unreachable at runtime | Every `Load`/`Save`/`Delete`/`Take`/`Keys` fails, is counted as `session_store_error`, and degrades to "no session": a reconnecting client gets a fresh session (`Session Present` false) and the queued messages of that session stay in Redis until their TTL. The cluster backend keeps retrying; routing degrades to what the node's own index knows. | `session_store_error`, the cluster's own `redis unreachable` warning |
| Redis loses its data (no persistence, a wiped volume, a failed replica promoted with a lag) | The sessions of the lost window are gone: those clients get fresh sessions and whatever the store had not persisted is lost. Messages the broker acked after the last durable write of Redis are lost with it — that is why the AOF setting is the durability boundary. | Redis' own metrics; a spike of `Session Present` false |
| A corrupt or incompatible value | That one key is deleted and counted as `session_store_corrupt`; the client gets a fresh session, the broker keeps serving. | `session_store_corrupt` |
| Two nodes appending to one stored session at the same time | `Save` replaces the whole record and the broker's lock only serialises one node, so one of the two queued publications can be overwritten (last writer wins). The queue of an offline session is effectively owned by the node that stored it. | not observable directly — the mitigation is the directed routing described below |
| Expiry | The key's TTL removes the session at its deadline; each node's sweeper also enforces deadlines for its namespace, and a CONNECT checks the deadline. | `session_expired` |

**Limits of what Part 1 needed from the store.** The inflight-overflow hand-off is a
single `Save` of the complete session state under the broker's session lock, so it
needs no new atomic operation: the pending deliveries are appended to the state in
memory (and the offline caps are applied there) *before* the one `Save` that stores
the session. The Redis store therefore keeps exactly one Lua script — the take — and
no counter is maintained in Redis. The other append path (`enqueueOffline`, a
publication arriving for an already-stored session) remains `Load` → append →
`Save`, with the single-writer assumption the store documents: if two nodes must be
able to append to one offline session concurrently, the store needs an atomic append
(a Lua script that appends one message and trims to the cap) and the broker needs to
route that session's publications to one node — that is a change for a deployment
that requires it, not something this build claims.

## Blueprint B — a SQL store

A relational database is the natural choice when the sessions must live next to the
rest of an operator's data, and when "what did the broker lose" must be a query
rather than a Redis dump. This is a **design sketch**, not an implementation: no
dependency is added, and `SessionState` already contains everything the rows need.

### Tables

```sql
-- One row per stored session. The primary key is the key the broker passes in
-- (namespaceKey(Config.Namespace, clientID)), used verbatim.
CREATE TABLE mqtt_session (
    session_key   VARCHAR(255) NOT NULL PRIMARY KEY,
    client_id     VARCHAR(255) NOT NULL,
    namespace     VARCHAR(64)  NOT NULL,
    expires_at    TIMESTAMP(6) NULL,          -- NULL = MQTT 3.1.1: never expires
    updated_at    TIMESTAMP(6) NOT NULL,
    KEY idx_mqtt_session_expiry (expires_at)  -- the sweeper's scan
);

-- The subscriptions of a session, one row per filter.
CREATE TABLE mqtt_subscription (
    session_key             VARCHAR(255) NOT NULL,
    filter                  VARCHAR(512) NOT NULL,
    qos                     TINYINT      NOT NULL,
    no_local                BOOLEAN      NOT NULL,
    retain_as_published     BOOLEAN      NOT NULL,
    retain_handling         TINYINT      NOT NULL,
    subscription_ids        JSON         NULL, -- []uint32
    PRIMARY KEY (session_key, filter),
    FOREIGN KEY (session_key) REFERENCES mqtt_session (session_key) ON DELETE CASCADE
);

-- The queue: one row per message, in publication order. The auto-increment column
-- IS the FIFO order; nothing else may be used for it.
CREATE TABLE mqtt_queued_message (
    id           BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    session_key  VARCHAR(255) NOT NULL,
    topic        VARCHAR(512) NOT NULL,
    payload      BLOB         NOT NULL,
    qos          TINYINT      NOT NULL,
    retain       BOOLEAN      NOT NULL,
    props        BLOB         NULL,           -- MQTT 5 property block, no length prefix
    enqueued_at  TIMESTAMP(6) NOT NULL,
    KEY idx_mqtt_queue_session (session_key, id),
    FOREIGN KEY (session_key) REFERENCES mqtt_session (session_key) ON DELETE CASCADE
);

-- The unconfirmed outbound exchanges: they carry a packet id, so rows are the only
-- shape that can round-trip them.
CREATE TABLE mqtt_inflight (
    session_key  VARCHAR(255) NOT NULL,
    packet_id    SMALLINT UNSIGNED NOT NULL,
    topic        VARCHAR(512) NOT NULL,
    payload      BLOB         NOT NULL,
    qos          TINYINT      NOT NULL,
    retain       BOOLEAN      NOT NULL,
    props        BLOB         NULL,
    stage        TINYINT      NOT NULL,       -- 0 PUBACK, 1 PUBREC, 2 PUBCOMP
    PRIMARY KEY (session_key, packet_id),
    FOREIGN KEY (session_key) REFERENCES mqtt_session (session_key) ON DELETE CASCADE
);

-- Inbound QoS 2 packet ids waiting for their PUBREL.
CREATE TABLE mqtt_inbound_qos2 (
    session_key  VARCHAR(255) NOT NULL,
    packet_id    SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (session_key, packet_id),
    FOREIGN KEY (session_key) REFERENCES mqtt_session (session_key) ON DELETE CASCADE
);
```

### The atomic take

`Take` must load the state and remove it in one transaction, or two nodes will serve
one session. Both idioms below are correct; the second needs no interactive
transaction.

```sql
-- (a) Lock the row, read the children, delete the parent (children cascade).
BEGIN;
SELECT session_key, client_id, expires_at
  FROM mqtt_session
 WHERE session_key = ? AND (expires_at IS NULL OR expires_at > NOW(6))
   FOR UPDATE;
SELECT filter, qos, no_local, retain_as_published, retain_handling, subscription_ids
  FROM mqtt_subscription WHERE session_key = ?;
SELECT id, topic, payload, qos, retain, props
  FROM mqtt_queued_message WHERE session_key = ? ORDER BY id;
SELECT packet_id, topic, payload, qos, retain, props, stage
  FROM mqtt_inflight WHERE session_key = ?;
SELECT packet_id FROM mqtt_inbound_qos2 WHERE session_key = ?;
DELETE FROM mqtt_session WHERE session_key = ?;
COMMIT;
```

```sql
-- (b) Compare-and-delete: claim the row by moving a generation counter, then read
-- and delete. A loser sees rowcount 0 and reports "no session" without reading a
-- byte of state.
UPDATE mqtt_session
   SET generation = generation + 1, updated_at = NOW(6)
 WHERE session_key = ? AND generation = ?;      -- rowcount 0 => someone else took it
-- ... read the children of the claimed row, then delete it ...
```

`Save` is the mirror image: upsert the session row, then replace its children
(`DELETE` + `INSERT`, or a diff) in one transaction. A store that wants to keep the
write cheap can write only the rows that changed, but the whole state must be
committed atomically — a session that exists with half its queue is worse than a
session that does not exist.

### Why the queue should be rows, not a blob

The offline queue is the largest part of a session and the only part that is
appended to while the session is stored. As rows it is:

* **appendable without a read-modify-write.** One `INSERT` per queued publication
  (plus the trim below), instead of reading a possibly megabyte-sized blob,
  appending to it and writing it back — a race with any other writer, and a write
  amplification of the whole queue per message;
* **trimmable with an index.** The per-session cap is
  `DELETE FROM mqtt_queued_message WHERE session_key = ? ORDER BY id LIMIT ?`,
  which is where the drop-oldest policy of `MaxOfflineQueueSize` lands;
* **drainable in order.** `SELECT ... ORDER BY id LIMIT n` is the FIFO the drain
  needs, and a partial drain deletes exactly the rows it sent;
* **inspectable.** "Which sessions hold the most messages" is
  `SELECT session_key, COUNT(*) ... GROUP BY session_key`, which is what an operator
  wants when sizing the caps;
* **bounded per row.** A single payload cannot push the session's row over the
  database's maximum row size.

A blob per session is defensible only for a store that keeps the whole session in
memory anyway (the Redis record, which is written as one value under the
single-writer assumption above).

### Bounding the queue

Three bounds, all enforced in the store, not only in the broker:

1. **Per session** — after each insert, delete the oldest rows beyond
   `MaxOfflineQueueSize` (the `LIMIT` delete above). Count it as
   `offline_queue_full`.
2. **Broker-wide** — the broker's own `MaxTotalOfflineMessages` gate is the fast
   path; a store that several brokers share should also enforce a total, e.g.
   `SELECT COUNT(*) FROM mqtt_queued_message` (cheap with an index-only scan) or a
   counter table updated in the same transaction, and reject the newest insertion
   past the cap (`offline_queue_total`).
3. **Expiry** — the queue of a session is deleted with its session (`ON DELETE
   CASCADE`), and the sweeper's `DELETE FROM mqtt_session WHERE expires_at <
   NOW(6)` is a single statement on the expiry index. A store that also wants the
   deadline enforced without a broker can run the same statement from a scheduled
   job.

## Blueprint C — a cache library as the store

`gcache` is no longer a scaffold: it is implemented and tested, and two of its
ports are exactly what a stored session needs. `RemoteStore` is a key-addressed
store with a per-key TTL (`Get`/`Set`/`Delete`, implemented on Redis and on an
embedded Olric cluster), and `Locker` is a lease with a compare-the-token release
and renew. The ready-made adapter is
[`contrib/sessionstore-gcache`](../contrib/sessionstore-gcache): it implements
`SessionStore` over a `RemoteStore`, enumerates with gcache's optional `Scanner`
port for `Keys`, makes `Take` atomic with `Locker.Acquire` plus a
compare-and-delete release (three round trips and a lock TTL), and reports itself
shared. Nothing in `gmqx` imports gcache: the adapter is a separate module, so a
deployment that does not use it does not build gcache at all.

What is **not** a session store is `gcache.Cache`, the two-level cache built on
top of those ports, and a deployment must not put it in front of the stored
session: its local tier can answer with an older copy than the one another node
wrote, it evicts under memory pressure, it clamps TTLs to its own maximum, it can
serve an expired entry after a store failure (stale-if-error) and it caches
absences. Each of those is data loss by design — acceptable for cached data, fatal
for a session. The adapter therefore reads and writes the `RemoteStore` directly.
The seam's contract is the opposite of a cache's: a value that was saved is there
until it is taken, deleted or expires.

### What any other KV store would have to offer

A store like Ignite (or any other key-value system) fits the seam only if it has all
of the following; check them explicitly before choosing one:

1. **Per-key TTL or expiry** that a query can see and enforce, so `ExpiresAt` is a
   deadline rather than a suggestion.
2. **An atomic compare-and-delete** (or a transaction with the same effect) for
   `Take`, so one session has one owner.
3. **A scan by prefix** for `Keys`, without loading the values, and bounded enough
   that a sweep of a few hundred sessions per tick is cheap. (In gcache this is the
   optional `Scanner` port; `redis.Store` implements it with `SCAN`.)
4. **Low and predictable latency with the broker's session lock held** — the store
   is called on the CONNECT and DISCONNECT path, so a store with a 50 ms p99 makes a
   reconnect slow and a store with an unbounded tail makes it hang.
5. **No eviction policy that can drop a key** (or a size that makes it impossible),
   and a durability story for a node loss: replication with a defined consistency
   model, or an append-only log.
6. **Values that round-trip byte-exactly** (payloads and property blocks), without
   encoding surprises.
7. **A backup/restore story** that includes those keys, because "the sessions are in
   the KV store" moves the backup obligation to the KV store.

If a candidate cannot answer 1–3, it is a cache, and what it needs is blueprint A or
B in front of it.

## Availability model

Put the sessions in a shared store (blueprint A or B) and run the cluster backend
(gossip is the default; Redis or the TCP mesh are the alternatives). Then:

* **A node loss costs reconnects, not sessions.** Every client of the lost node
  reconnects, and each one takes its session from the shared store — subscriptions,
  queue, QoS 2 state — on whichever node it reaches. What the clients see is the
  reconnect; what they keep is the session.
* **Throughput scales by adding nodes.** Delivery is per-node: a publication is
  matched locally against the node's own subscription index and written by that
  node's connections, and the routing that decides *which* node has a matching
  session is a summary the peers exchange (gossip disseminates it; Redis keeps a
  TTL'd summary per node). A publication therefore costs one local match and one
  cluster spread, not a fan-out through every node, so adding a node adds delivery
  capacity instead of adding work to every peer.
* **A partitioned cluster still delivers locally.** A node that cannot reach its
  peers keeps serving the sessions it holds: local matching, local delivery, local
  queues. What it loses is cross-node delivery and the freshness of the summaries
  until the partition heals; a directed publish to a client id on the other side
  fails into the counted, fire-and-forget path rather than into a hot loop.

The trade-offs, stated plainly:

| Property | What it costs |
|---|---|
| Resume latency | The store is consulted on CONNECT (a take), on DISCONNECT (a save) and by the sweeper — **not on the publish path**. A publish never touches the store, so shared-store latency shows up as a slower reconnect and a slower disconnect, not as lower throughput. |
| Availability of *durability* | The store is a dependency: while it is unreachable, a reconnect gets a fresh session and a disconnect cannot save its queue. Delivery itself keeps working. |
| A partitioned cluster | Local delivery continues, but a session whose queue lives on a node the client cannot reach is not resumed on the other side until the partition heals and the session is taken from the store. |
| Consistency | Two nodes appending to one *stored* session can lose one append (last writer wins). One client id is one live connection at a time (the connect-time kick enforces it), so this only affects the narrow window of a displaced session. |
| Memory | The queue caps are the memory bound. Raising them buys durability window and costs RAM (and store bytes) on every node that holds sessions. |

## Sizing worksheet

Two configuration numbers decide how much a disconnect may cost:
`MaxOfflineQueueSize` (per session) and `MaxTotalOfflineMessages` (broker-wide).
Both are capacities, not tuning knobs for latency: a message that arrives when they
are full is dropped (`offline_queue_full` / `offline_queue_total`).

**Step 1 — the durability window.** How long may a client be disconnected before the
broker starts dropping its oldest messages?

```
W = peak disconnect duration (seconds)
```

Sources: your reconnect policy and backoff ceiling, the client's keep-alive, the
deployment's rollout/failover time, and how long a mobile or field device may stay
off the network. Take the worst case you are willing to survive, not the average.

**Step 2 — the rate.** How many messages per second does one session's
subscriptions carry at peak?

```
R = messages per second for one session at peak
N = W × R          # messages the session will accumulate
```

**Step 3 — the per-session cap.**

```
MaxOfflineQueueSize ≈ N × safety (1.5–2×)
```

Example: a client that may be away for 120 s while its subscriptions carry 5
msg/s → `N = 600` → `MaxOfflineQueueSize ≈ 1000` (the default). A client away for
10 minutes at 50 msg/s → `N = 30000`: either raise the cap (and pay the memory) or
accept that its oldest messages are dropped, and alert on `offline_queue_full`.

**Step 4 — the broker-wide cap.** How many sessions can be offline *at the same
time*? A node loss makes every client of that node offline at once, so size for
that, not for the average:

```
S = sessions per node × fraction offline at peak (0.1–1.0 for a node loss)
MaxTotalOfflineMessages ≈ S × MaxOfflineQueueSize × sharing (0.1–0.5)
```

The sharing factor is how much of each session's cap is actually used: queues are
full only for the traffic-heavy sessions, so a total equal to
`S × MaxOfflineQueueSize` is a worst case that is rarely reached. The default
100000 is about 100 full sessions of 1000 messages.

**Step 5 — memory and store bytes.**

```
payload ≈ message size (bytes) + ~64 B of framing (topic, QoS, retain, props, headers)
broker RAM ≈ MaxTotalOfflineMessages × payload × 1.3
store bytes ≈ the same, plus replication factor and AOF size
```

Example: 64 B payloads, `MaxTotalOfflineMessages = 100000` → about
100000 × 128 B × 1.3 ≈ 17 MB in the broker and about the same in Redis (× the
number of replicas, plus the AOF). 1 KiB payloads at the same cap → about 140 MB
each.

**Step 6 — the store's own numbers.**

| Question | What it decides |
|---|---|
| `sessions × (queue × payload + subscriptions + overhead)` | Redis `maxmemory` (with `noeviction`) or the database's table size |
| `Save` rate = disconnect rate + `enqueueOffline` rate | Redis ops/s or the database's write IOPS. A node loss produces one `Save` per disconnected session, in a burst. |
| `Take` rate = reconnect rate | The store's read latency on the CONNECT path |
| The sweeper's `Keys`/expiry scan per `SessionSweepInterval` | Redis `SCAN` cost, or the database's expiry-index scan. Keep the stored session count in the range one sweep can walk (the broker bounds a tick to 512 stored sessions per pass; a larger store needs more ticks, not a bigger tick). |

Finally, watch the counters this document names: `offline_queue_full`,
`offline_queue_total`, `inflight_overflow` (a session went offline because its
inflight queue was full — normal under a stalled consumer, a problem if it climbs),
`inflight_overflow_clean` (a clean session lost a delivery), `session_store_error`
and `session_store_corrupt`. The first two are capacity; the last two are the store.
