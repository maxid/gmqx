English | [简体中文](README.zh-CN.md)

# sessionstore-gcache

A ready-made `gmqx.SessionStore` backed by
[`github.com/maxid/gcache`](https://github.com/maxid/gcache), so the nodes of a
gmqx deployment can keep persistent MQTT sessions in one shared Redis: a client
that reconnects to another broker resumes its subscriptions, its offline queue
and its QoS 2 state instead of starting from nothing.

```go
import (
	gmqx "github.com/maxid/gmqx"
	"github.com/maxid/gcache/redis"
	gcachestore "github.com/maxid/gmqx/contrib/sessionstore-gcache"
)

remote, _ := redis.NewStore(client)          // gcache.RemoteStore on go-redis
locker, _ := redis.NewLocker(client, "gmqx:fencing")
sessions, err := gcachestore.New(gcachestore.Config{
	Store:  remote,
	Locker: locker,
	Prefix: "gmqx:session:",
})
if err != nil {
	return err // a store that cannot enumerate, or no locker: refused here
}
defer client.Close()

broker, err := gmqx.New(gmqx.Config{
	SessionStore: sessions,
	// ...
})
```

## Why this is its own module

`contrib/sessionstore-gcache` is a separate Go module
(`github.com/maxid/gmqx/contrib/sessionstore-gcache`) that replaces
`github.com/maxid/gmqx` with `../..` and `github.com/maxid/gcache` with
`../../../gcache`. The core broker therefore never depends on gcache or on
go-redis: a deployment that does not use this store builds neither, and the
library's `go.mod` stays what it was. Nothing in `github.com/maxid/gmqx` imports
this package.

## What it implements

| gmqx seam | How |
|---|---|
| `Load(key)` | `RemoteStore.Get` + versioned JSON decode. A value past its deadline is deleted and counted. |
| `Save(key, state)` | Encode and `RemoteStore.Set` with the session's own deadline as the key's TTL. A state whose deadline already passed is deleted instead of written. |
| `Delete(key)` | `RemoteStore.Delete`. |
| `Keys()` | `gcache.Scanner.Scan(prefix, ...)` with this store's key prefix, removed from the returned keys. |
| `Take(key)` | The atomic take below. |
| `SharedAcrossNodes()` | Always `true`: the store is shared, so a connection displaced by a peer must not write its copy back. |

`New` refuses three configurations instead of failing later, on a reconnect:

| Error | When |
|---|---|
| `ErrStoreRequired` | no `RemoteStore` was configured |
| `ErrEnumerationUnsupported` | the store does not implement `gcache.Scanner`. `Keys` is required twice: `gmqx.New` indexes the stored sessions with it, so a publication for an offline session is queued rather than dropped, and the expiry sweeper enumerates the deadlines with it. |
| `ErrLockerRequired` | no `Locker` was configured. The store reports itself shared, and a shared store without `Take` can hand one session to two nodes. |
| `ErrInvalidPrefix` | the prefix contains a SCAN glob character, which would make `Keys` scan keys this store does not own. |

## The take: three round trips and a lock TTL

A take must remove the stored session and hand it to exactly one caller. It is:

1. **Acquire** the lock for the session key with `Locker.Acquire`
   (`SET key token NX PX ttl`, retried under `LockWait`);
2. **Read** the value with `RemoteStore.Get` while holding the lock;
3. **Delete** it with `RemoteStore.Delete` while still holding the lock.

Then the lock is released with `Lock.Unlock`, which is the locker's
compare-and-delete of the lock token (a lock that expired and was taken over by
another node is never released by the wrong owner). That is a fourth round trip;
the three above are the take's critical path and what bounds its cost.

Holding the lock across steps 2 and 3 is what makes the read and the delete one
critical section: a second node cannot read the key between them and then delete
it again, so two nodes cannot both resume the session.

**The lock TTL.** `LockTTL` (default 5 s) bounds the critical section and is not
renewed, so it must be larger than the store's p99 for a `Get` followed by a
`Delete`. If a pause outlives it, the guarantee degrades from exactly-once to
at-most-once: the release is still a compare-and-delete, so the expired lock is
not released by the wrong owner, but a second node may have entered the same
critical section and both may hand the state out. A Redis that pauses for
seconds is a Redis problem; size the TTL so the lease cannot expire in normal
operation.

**If the lock cannot be acquired**, another node (or another CONNECT of the same
client id) is taking that session. `Take` waits at most `LockWait` (default
500 ms), then returns `(SessionState{}, false)` — the caller starts a fresh
session instead of blocking the CONNECT or failing it. Nothing is deleted, the
session stays in the store, and the event is counted as
`session_take_contended`. A failed store or lock command is counted as
`session_store_error` and degrades the same way (at-most-once, never a panic).

`Take` handles the rest of the rules without extra round trips: a missing key is
"no session", an undecodable value is deleted and counted
(`session_store_corrupt`), and a value past its deadline is deleted and counted
(`session_expired`).

## The L1 caveat: do not wrap it in `gcache.Cache`

The adapter uses `gcache.RemoteStore` directly and never puts the two-level
`gcache.Cache` in front of it. That is deliberate, and a deployment must not do
it either:

- a `gcache.Cache` read may be answered by its **local tier (L1)**, which can be
  an older copy than the one another node just wrote — invalidation is best
  effort pub/sub, so a lost message leaves a stale local value;
- L1 **evicts** under memory pressure and is bounded; an evicted session is a
  session that silently ceased to exist;
- a `Cache` **clamps TTLs** to its `MaxTTL` and applies its default TTL, while a
  session's `ExpiresAt` is a deadline, not a freshness hint;
- `stale-if-error` may serve an expired entry after a store failure, which for a
  session means two nodes with two histories;
- negative caching remembers "this key is absent", the opposite of what a store's
  `Load` returning `false` must mean.

`RemoteStore` is the part of gcache that behaves like a store: a value that was
written is there until it is overwritten, deleted or expires. That is the
contract a session needs.

## Stored layout

Session keys are written as `<Prefix><key>`, where `<key>` is exactly what the
broker passes in (already namespace-qualified). Take locks live under
`take-lock:<Prefix><key>`, outside the data prefix, so `Keys` never returns a lock
and `"<Prefix>*"` never matches one.

The value is a versioned JSON record (`{"v":1,...}`) with the same field names
and version as gmqx's own `RedisSessionStore`, so a value written by one can be
read by the other when a deployment moves between them (only the key prefix
differs). Payload and MQTT 5 property bytes are base64 in JSON and round-trip byte
for byte, NUL bytes included; nil and empty collections stay distinct. A record
of another version is treated like an undecodable one: dropped and counted.

`Keys` scans `<Prefix>*` in pages through `gcache.Scanner`, so a large store does
not come back in one reply. gmqx filters the keys of other namespaces itself; the
scan is already limited to this store's prefix.

## Configuration

| Field | Default | Meaning |
|---|---|---|
| `Store` | — | the `gcache.RemoteStore`; required, must implement `gcache.Scanner` |
| `Locker` | — | the `gcache.Locker`; required |
| `Prefix` | `gmqx:session:` | this store's key namespace; no SCAN glob characters |
| `LockTTL` | 5 s | the lock lease, and with it the take's critical-section bound |
| `LockWait` | 500 ms timeout | how long a take retries the lock before reporting "taken elsewhere" |
| `Timeout` | 2 s | one store operation, and the whole `Keys` walk |
| `Metrics` | nil | receives `session_store_corrupt`, `session_store_error`, `session_expired`, `session_take_contended` through `RecordDroppedMessage` |
| `Logger` | `slog.Default()` | command, decode and lock failures |

The broker calls the store with its own session lock held, so every operation is
bounded by `Timeout`; an unreachable Redis costs a bounded wait, not a hang.

## Deployment

The store is as durable as the Redis behind it. For a real deployment, at
minimum:

```sh
docker run --name gmqx-redis -p 6379:6379 -v gmqx-redis:/data redis:7 \
  redis-server --appendonly yes --appendfsync everysec \
               --maxmemory-policy noeviction
```

`noeviction` is not optional: an evicted session key is a session that silently
disappeared, and the broker cannot even see that it happened. `appendonly yes`
makes an acknowledged session survive a Redis restart; `everysec` bounds the
loss to about a second of writes. Add a replica per shard (or a Sentinel/cluster
endpoint in front of the address) for availability, and size
`maxmemory` from the session count × queue size.

The full guidance — AOF and replication trade-offs, sentinel/cluster failover,
key prefixes, namespaces, memory sizing, and the failure-mode table — is in
gmqx's [docs/PERSISTENCE.md](../../docs/PERSISTENCE.md). The adapter's own
failure modes are the Redis row of that table: `session_store_error` on the
metrics, at-most-once degradation, and no panic.

## Testing

The tests run against miniredis and a real broker, and cover the cross-node
resume (a persistent client subscribes on broker A, disconnects, messages are
published while it is away, and it reconnects on broker B with the same store and
receives everything in order with `sessionPresent`), the clean-connect discard,
two brokers racing for one session, the session expiry as the key's TTL, a corrupt
value, and the namespace-limited `Keys`:

```sh
cd libs/gmqx/contrib/sessionstore-gcache
go test ./...
go test -race ./...
go vet ./...
```
