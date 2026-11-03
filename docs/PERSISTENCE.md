English | [简体中文](PERSISTENCE.zh-CN.md)

# Persistence and availability

This document answers three questions about the state a broker keeps beyond one
connection: **what must survive a restart**, **where it lives**, and **what a
deployment gives up when it makes it shared**. The design is one persistence seam
for both kinds of retained state — persistent sessions and retained messages — so
a deployment picks a backend, not a "session store" and a "retained store".

## What must survive a restart

A persistent session is the only thing a broker keeps beyond a connection, and a
retained message is the only publication it keeps after delivering it. Both are
written through to the configured store.

| State | In the store? | Why |
|---|---|---|
| Subscriptions, with QoS, No Local, Retain As Published, Retain Handling | **Yes** | A resumed session that forgot its filters would silently deliver nothing. `Session Present` in the CONNACK is a promise about them. |
| Subscription Identifiers | **Yes** | They are state of the subscription, not of the connection (MQTT 5 §3.8.3.1): a resumed session must keep tagging deliveries with them. |
| Messages queued while the session was offline | **Yes** | They were acknowledged to their publisher. This is the offline queue, drained in order right after the next CONNACK. |
| Inbound QoS 2 packet ids whose PUBREL never arrived | **Yes** | A publisher that reconnects and retransmits one of them must get the same PUBREC and no second delivery (MQTT §4.4). |
| Unconfirmed outbound QoS 1/2 exchanges, with packet id and stage | **Yes** | They are re-sent on resume with DUP (PUBLISH) or as PUBREL, under their original packet ids (MQTT §4.4). |
| Session expiry deadline | **Yes** | It is what ends the state. A store that loses it keeps sessions alive forever. |
| Retained messages, with QoS, properties and Message Expiry | **Yes** | They are the value a late subscriber receives. Without them a restart silently forgets the last known state of every topic. |
| The connection: socket, reader/writer goroutines, keep-alive, topic aliases | — | A connection is not state a broker can move; the client reconnects. |
| The inflight window, the pending queue *as such*, packet-id allocations | — | The window is per connection and is rebuilt. The **messages** in it are not lost: a persistent session's pending deliveries move into its queue before the session is stored (see [the inflight queue](#the-inflight-queue-is-not-a-loss-point)). |
| Negotiated limits (Receive Maximum, Maximum Packet Size), authentication session | — | They are renegotiated by the next CONNECT. |
| A delayed will waiting on its Will Delay Interval | — | `Stop` drops what is still pending: there is nowhere left to publish it and no timer may outlive the broker. |
| Cluster membership and routing summaries | — | They are rebuilt by the backend (gossip handshake, or a TTL'd record in Redis). |

The rule behind the table: **state that a client asked to be kept and that a
publisher was told was accepted must be in the store; state that belongs to a
connection or that the broker owns by node must not be.**

## The seam

One field: `Config.Store` is a [`gcache.RemoteStore`](https://github.com/maxid/gcache)
(`Get`, `Set(ctx, key, value, ttl)`, `Delete`, `Close`), plus `Config.Locker` for a
store several nodes write. `Config.KeyPrefix` (default `gmqx:`) namespaces the
keys of one broker; two brokers in one process can share a backend as long as their
prefixes (or their stores) differ.

```go
import (
    gmqx "github.com/maxid/gmqx"
    "github.com/maxid/gcache/memory"
)

broker, err := gmqx.New(gmqx.Config{
    Store: memory.NewStore(),   // the default: in-process, lost on restart
})
```

`Config.Store` nil installs `gcache/memory`, so a broker without configuration
behaves exactly like the in-memory broker of earlier versions: state is node-local
and a restart loses it. That default is also the store with no external
dependency — it is `gcache`'s own in-process implementation.

### Keys, TTL and encoding

| Key | Value | TTL |
|---|---|---|
| `<prefix>session:<broker session key>` | JSON record v1: client id, subscriptions and their identifiers, the queue, the inbound QoS 2 ids, the outbound exchanges (packet id, payload, QoS, retain, property block, stage), the deadline | the session's remaining lifetime; none for a session that never expires |
| `<prefix>retained:<broker retained key>` | JSON record v1: namespace, topic, payload, QoS, property block, deadline | the message's remaining Message Expiry Interval; none without one |

The broker session key is `namespaceKey(Config.Namespace, ClientID)`, so two
namespaces sharing a store cannot collide — that is the same isolation the routing
table and the cluster use.

The TTL is the state's own deadline, not a cache lifetime: the backend drops a key
at the moment the broker would stop using it. A state whose deadline has already
passed is deleted instead of written. Packet-id sets are written in ascending
order, so a session's stored form is reproducible.

The encoding is versioned. A value of another version, a value that does not
decode, and an exchange that cannot have been a QoS 1/2 PUBLISH are dropped and
counted (`session_store_corrupt`, `retained_store_corrupt`, `offline_queue_corrupt`)
instead of failing a CONNECT. A failed command is counted (`session_store_error`,
`retained_store_error`) and reported as a miss — the at-most-once behavior the
cluster itself has. The broker keeps serving either way.

### The atomic take

Reconnecting to a session removes it from the store, and two nodes must not both
take it. With `Config.Locker` set, the take is a `TryAcquire` of a per-key lock
(gcache's `Locker`; the lock TTL bounds the get-and-delete critical section)
followed by the get and the delete. A node that loses the lock treats the session
as not stored and starts a clean one. Without a Locker the take is Load followed by
Delete, which is correct for a store only one process writes — the default
in-process store, or one dedicated to a node.

`Config.Locker` is also the signal that the store is **shared across nodes**: a
connection displaced by a peer's kick then neither writes its copy of the session
back (which would resurrect a session the peer serves) nor deletes the key (which
would erase the state the peer saved). A store without a Locker is single-writer,
and the pre-existing behavior stays.

### Reading back what another process stored

`gcache.Scanner` is optional. When the store implements `Scan`, the broker loads
what a previous process stored: sessions are indexed as offline targets, retained
messages fill the hot table. Without a Scanner the broker skips that load, which is
correct for a store no other process writes to.

Enumeration is **not** on the expiry path: the broker's routing index carries each
stored session's deadline and its queued-message count, so the sweeper walks the
index, and the queue counter stays right even after the backend evicted an expired
value. A publication never consults the store: the index answers routing, and the
retained hot table answers a replay.

### The retained hot table

The broker keeps retained messages in memory and writes through to the store. A
SUBSCRIBE scans the hot table (not the store), so the replay path does not pay a
round trip; the store is the copy that survives a restart and that another node
can read. Expired entries are removed from both when a replay encounters them, and
the backend's TTL removes them even if nobody subscribes.

## Choosing a backend

| Backend | Restart-safe | Shared across nodes | Enumerable | Notes |
|---|---|---|---|---|
| `gcache/memory` (default) | no | no | no | Zero configuration; the same behavior as the in-memory broker. |
| `gcache/redis` | yes | yes | yes (`Scanner`) | One Redis for the deployment; keys under `Config.KeyPrefix`. Use `Config.Locker` for multi-node takes. |
| `gcache/olric` | yes | yes | **no** (no `Scanner` yet) | Reads/writes and TTL work; without enumeration a restarted node cannot index sessions or retained messages stored by peers. Fine for a single long-lived node, not for shared-cluster semantics until gcache implements `Scan`. |
| Your own `gcache.RemoteStore` | as you make it | as you make it | if you implement `Scanner` | Anything that satisfies the port works; a SQL table, a file, another cache. |

Two brokers in one process each get the store they are given — there is no global
instance. A deployment that embeds gmqx next to another application (or runs two
brokers with different isolation scopes) gives each its own gcache instance or its
own `KeyPrefix`.

## The inflight queue is not a loss point

When a session's pending queue overflows, the broker does not drop the delivery: it
takes the session offline — the connection is closed the way a lost one is, the
normal unregister path stores the session, and everything the connection still owed
(the inflight window's messages, the pending queue) joins the session's queue
before it is stored. The next CONNECT drains it.

A **clean** session has no store entry and no queue — its state ends with the
connection — so the overflow drops the delivery with its own reason
(`inflight_overflow_clean`). Clean sessions trade durability for that.

The only loss points a persistent session keeps are the offline queue's own caps,
`MaxOfflineQueueSize` (per session, drop-oldest) and `MaxTotalOfflineMessages`
(global, reject-newest), both counted (`offline_queue_full`, `offline_queue_total`).

## Availability model

- **One node, in-process store (default).** A restart loses sessions, queued
  messages and retained messages, exactly as an in-memory broker always has. This
  is the "embedded, no external dependency" deployment.
- **One node, shared backend.** A restart keeps everything: sessions resume from
  the store, retained messages replay. The node is still a single point of
  availability while it is down.
- **Several nodes, shared backend.** A client that reconnects to another node
  resumes its session (atomic take), and a retained message published anywhere is
  replayed everywhere. The nodes need the backend and — for takes and kicks — a
  `Locker`; the cluster backend still carries live routing, presence and directed
  delivery.
- **Several nodes, no shared backend.** Sessions and retained messages are
  node-local: a reconnect to another node starts from nothing, and retained state
  is replicated only by the cluster's retained events (which are not persistence).
  This is the "highest throughput, weakest durability" deployment.

## Sizing worksheet

| Quantity | Formula | Default |
|---|---|---|
| Worst-case queued messages | sessions × `MaxOfflineQueueSize` | 1000 per session |
| Worst-case queued bytes | messages × (payload + topic + property block) | payload-dependent |
| Global message cap | `MaxTotalOfflineMessages` | 100 000 |
| Retained messages | `MaxRetainMessages` | 0 (unbounded) |
| Retained bytes | messages × (payload + topic + properties) | payload-dependent |
| Store round trips per CONNECT (persistent) | 1 take (lock + get + delete) or 1 load | — |
| Store round trips per disconnect (persistent) | 1 save | — |
| Store round trips per publication | 0 for routing; 1 save when it is retained or queued | — |
| Store round trips per SUBSCRIBE | 0 (the hot table answers) | — |

A deployment that cannot bound these numbers should bound the queues, not expect
the store to absorb an unbounded one: the store holds what the broker's limits
allow it to hold.

## What this replaced

The `SessionStore` SPI, `MemorySessionStore`, `RedisSessionStore` and the
`contrib/sessionstore-gcache` module are gone: they were three implementations of
one idea, split by feature instead of by backend. A deployment that used
`RedisSessionStore` now passes `gcache/redis`'s store as `Config.Store` and its
locker as `Config.Locker`; one that used the contrib module passes the same gcache
store it already built.
