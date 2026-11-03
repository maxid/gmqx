English | [简体中文](DEPLOYMENT.zh-CN.md)

# Deployment

How to run a cluster of brokers and what to configure. The contract behind each
knob is in [PERSISTENCE.md](PERSISTENCE.md); this page is the recipe.

## A three-node cluster

Every node runs the same binary with the same seed list; the list includes the node's
own cluster address, and nodes dial only the seeds that sort after their own address,
so a symmetric list yields exactly one connection per pair.

```sh
# node a                                                                    # node b
gmqx -node a -addr 0.0.0.0:1883  -cluster-addr 10.0.0.1:7901 \
     -seeds 10.0.0.1:7901,10.0.0.2:7901,10.0.0.3:7901
```

`examples/cluster-node` is the same recipe as a runnable program, and
`examples/README.md` shows two nodes started side by side.

Three things bite:

- **The seed list must be symmetric and must contain every node**, including the
  local one. A node that lists only itself never joins.
- **`cluster-addr` must be explicit**, never `:0`: peers need a stable address, so an
  ephemeral port is not an option.
- **Cluster membership is not session state.** Enabling a cluster shares routing and
  replicates retained publishes; it does not make a session resumable on another
  node. That is `Config.Store` (see below), and `docs/PERSISTENCE.md` explains why the
  broker never picks one.

## Persistence

| Backend | When | What to set |
|---|---|---|
| `gcache/memory` (default) | One node, or a cluster that can lose a session when a device reconnects elsewhere | nothing |
| `gcache/redis` | You already run Redis and want sessions to move between nodes | `Store` + `Locker` from `gcache/redis` |
| `gcache/olric` | No Redis; the persistence tier follows the broker | `Store` + `Locker` from `gcache/olric` |

A shared store without a `Locker` takes a session by load-then-delete, which is only
safe for a store a single node writes to. Both shipped backends have a locker;
`gcache/olric`'s is a single-key lock on the partition owner, not a Redlock quorum —
see its README.

Olric has two shapes and the code does not change between them: an **embedded** node
runs inside the broker process (no extra deployment unit), a **client** talks to a
cluster you operate (independent scaling and upgrades). Pick one per deployment; do
not mix, or two tiers will hold different sessions.

## Connection takeover

`NotKickSeconds` (default 30) refuses a CONNECT that would displace a connection
younger than that: two ends of one client id cannot displace each other in a loop.
A device that roams between nodes normally never hits it — the old connection is gone
before the reconnect — but a load balancer that migrates sessions on purpose does.
That deployment sets `ConnectMode: ConnectModeKick` and `NotKickSeconds: -1`, or
accepts the refusal (MQTT 3.1.1 return code 3, MQTT 5 reason 0x9F) and retries.

## Listeners and limits

- TCP `Addr`, TLS `SecureAddr` + `TLS`, WebSocket `WebsocketAddr` +
  `WebsocketPath`, WSS `WebsocketSecureAddr`. `PortFallback` (default 16) makes a
  bind probe the next port when the configured one is taken; `Addrs()` reports what was
  really bound, so read it instead of assuming.
- `MaxConnections`, `ConnectionRateLimit`, `NotKickSeconds`, `ConnectTimeout`,
  `AuthTimeout`, `MaxPacketSize` and the byte-rate shapers are the knobs that bound a
  hostile peer. An unbounded queue is the one setting that turns a slow consumer into
  an outage: keep `MaxOfflineQueueSize` and `MaxTotalOfflineMessages` set.
- `MaxTotalOfflineMessages` is counted per node, not across the cluster (see
  PERSISTENCE.md).

## Observability

`Metrics` is a hook, not an endpoint: the broker calls it, the deployment decides
where the numbers go (Prometheus, OTLP, logs). `Sink` is the same for publications,
and `Logger` (a `*slog.Logger`) is where membership, kick and store decisions land.
There is no `$SYS` tree and no built-in health endpoint — both are one hook away, and
a library that opened a port nobody asked for would be worse.

## Upgrades

A rolling upgrade is safe in either direction: a peer that does not know a cluster
event type drops the frame and keeps going (that is why `ClusterSessionReleased` and
directed publishes degrade instead of breaking). What is *not* versioned is the store
encoding: two nodes on different builds may disagree on a record's version, and a
record whose version the build does not know is dropped, not misread. Upgrade the
store-writing nodes together, or empty the store between major changes.
