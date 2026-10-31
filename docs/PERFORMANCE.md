# Performance baseline

This document records the benchmark and soak baselines of this repository's
broker and client. It answers one narrow question — "how does the current code
behave on this machine?" — and deliberately not "how many connections will it
serve in production?". See [Limits of these numbers](#limits-of-these-numbers).

The numbers below were produced by the committed benchmarks in
[`bench_test.go`](../bench_test.go) and the soak test in
[`soak_test.go`](../soak_test.go). Re-measure before drawing conclusions from a
different version, machine, or Go toolchain: they are indicative, not a capacity
guarantee.

## Environment

| Item | Value |
|---|---|
| Date | 2026-11-01 |
| Go | `go1.27.1` |
| `GOOS` / `GOARCH` | `darwin` / `arm64` |
| CPU | Apple M1 Max, 10 cores (10 logical, `GOMAXPROCS=10`) |
| Memory | 64 GiB |
| OS | macOS 14.8.9 (Darwin 23J631) |
| Transport | loopback TCP, one process holds both client and broker |

## Methodology

Every benchmark in `bench_test.go` runs the real `gmqx.Broker` over real TCP:
the benchmark starts its own broker on `127.0.0.1:0`, dials it with a small MQTT
3.1.1 wire client written on `gnety/codec/mqtt`, and measures publications end to
end. Nothing is mocked, no benchmark shares a broker with another, and every
benchmark stops its broker when its function returns.

The shape is the same in all cases:

- setup (broker start, connections, subscriptions, retained seeding) runs before
  `b.ResetTimer()`, so it is never part of `ns/op`;
- a dedicated drain goroutine reads and acknowledges every delivery on the
  subscriber side, so the broker is never stalled by a full write-behind queue
  or a full inflight window — a benchmark that does not drain mostly measures
  its own backpressure;
- the publishing loop is windowed: every few publications it waits until the
  subscriber received them, with the timer still running. `ns/op` therefore
  covers the publish→deliver path, not just "the broker read the bytes". Only
  the tail (less than one window) is drained after `b.StopTimer()`;
- `b.ReportAllocs()` reports `B/op` and `allocs/op` for the whole benchmark
  process, drain goroutine included;
- payloads are 16 bytes unless stated otherwise; there are no fixed sleeps in a
  measured path, and waits block on a channel rather than on a poll.

| Benchmark | Topology | What `ns/op` means |
|---|---|---|
| `BenchmarkPublishQoS0` | 1 publisher → 1 subscriber, subscription QoS 0 | one QoS 0 publication, write to delivery |
| `BenchmarkPublishQoS1` | 1 publisher → 1 subscriber, subscription QoS 1 | one QoS 1 publication: PUBLISH, broker route, delivery, PUBACK |
| `BenchmarkFanout/subscribers=N` | 1 publisher → N subscribers (N = 10, 100) | one publication fanned out to all N; per-delivery cost is `ns/op ÷ N` |
| `BenchmarkManySubscriptions` | 1 publisher, N live subscriptions on one connection, one matching | one publication matching exactly one of N subscriptions |
| `BenchmarkRetainedReplay` | broker holding 1000 retained topics, one fresh subscriber per op | connect + SUBSCRIBE + replay of 1000 retained messages + disconnect |
| `BenchmarkClientPublishQoS1` | `client` package → broker → 1 wire subscriber | one `client.Publish` call at QoS 1, i.e. a full round trip to PUBACK |

`BenchmarkManySubscriptions` publishes to a topic that is itself one of the
subscriptions, so there is exactly one target in every case. The `exact/subs=100`
case exists as the baseline for `exact/subs=10000`: if the routing index degraded
with the number of subscriptions, the two would diverge. The `wildcard=100` case
adds 100 wildcard filters that do *not* match the published topic; a wildcard
filter anywhere in the index forces the general routing path, which runs
`matchTopic` against every distinct wildcard filter and merges the matches per
session, so this case exposes the documented cost of that scan.

## Measured baseline

One run of:

```sh
go test -run '^$' -bench . -benchtime=2s ./...
```

Results (real output, reformatted into a table):

| Benchmark | ns/op | B/op | allocs/op | Derived rate |
|---|---:|---:|---:|---:|
| `BenchmarkPublishQoS0` | 5,931 | 1,024 | 10 | ≈169k publications/s |
| `BenchmarkPublishQoS1` | 31,925 | 2,344 | 19 | ≈31k publications/s |
| `BenchmarkFanout/subscribers=10` | 29,492 | 6,128 | 46 | ≈34k publications/s, 2.95 µs per delivery |
| `BenchmarkFanout/subscribers=100` | 235,682 | 57,912 | 406 | ≈4.2k publications/s, 2.36 µs per delivery |
| `BenchmarkManySubscriptions/exact/subs=100` | 6,759 | 1,026 | 9 | ≈148k publications/s |
| `BenchmarkManySubscriptions/exact/subs=10000` | 5,133 | 1,026 | 9 | ≈195k publications/s |
| `BenchmarkManySubscriptions/wildcard=100/subs=10000` | 7,301 | 1,073 | 10 | ≈137k publications/s |
| `BenchmarkRetainedReplay` | 7,609,712 | 3,840,692 | 7,880 | ≈131 joins/s, 1000 retained messages each |
| `BenchmarkClientPublishQoS1` | 41,642 | 3,801 | 29 | ≈24k `client.Publish` round trips/s |

The same run also executed the routing-index benchmarks already present in the
package. They isolate `Broker.LocalTargets` from the socket path and are the
clearest evidence that the index does not degrade with subscription count:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkLocalTargets/subs=10/wildcards=0` | 150.0 | 96 | 1 |
| `BenchmarkLocalTargets/subs=1000/wildcards=0` | 145.4 | 96 | 1 |
| `BenchmarkLocalTargets/subs=10000/wildcards=0` | 138.3 | 96 | 1 |
| `BenchmarkLocalTargets/subs=10/wildcards=64` | 1,384 | 144 | 2 |
| `BenchmarkLocalTargets/subs=1000/wildcards=64` | 1,413 | 144 | 2 |
| `BenchmarkLocalTargets/subs=10000/wildcards=64` | 2,211 | 144 | 2 |
| `BenchmarkSharedTargetSelection/members=1` | 79.21 | 0 | 0 |
| `BenchmarkSharedTargetSelection/members=10` | 83.39 | 0 | 0 |
| `BenchmarkSharedTargetSelection/members=1000` | 93.19 | 0 | 0 |

Run-to-run variance on this machine is real: repeated 2 s runs of the benchmarks
above moved `BenchmarkPublishQoS0` between 4.7 µs and 7.3 µs, `BenchmarkPublishQoS1`
between 31.9 µs and 53.3 µs, `BenchmarkFanout/subscribers=10` between 29.3 µs and
64.0 µs, and `BenchmarkClientPublishQoS1` between 40.8 µs and 53.4 µs, while
`BenchmarkRetainedReplay` stayed within 7.5–7.9 ms. The allocations moved by at
most one object per op. A single run is enough to compare orders of magnitude
and to catch the kind of regression this baseline exists for; it is not enough
to claim a 10 % improvement, and the fanout cases need a repeated run before any
difference is called real.

## What the measurements show

- **The routing index is flat.** `BenchmarkManySubscriptions/exact` measures the
  same cost at 100 and at 10,000 live subscriptions (5.1–6.8 µs, inside the
  noise band), and `BenchmarkLocalTargets` — which measures only the lookup —
  stays at ~140–150 ns/op from 10 to 10,000 subscriptions. The end-to-end number
  is dominated by the two loopback socket hops, not by routing.
- **Wildcard filters are the cost multiplier of the index, not subscription
  count.** The general routing path runs `matchTopic` against every distinct
  wildcard filter. At the index level, 64 non-matching wildcard filters turn a
  ~145 ns lookup into ~1.4–2.2 µs; 100 non-matching wildcard filters in the
  end-to-end benchmark add roughly 1–2 µs per publication. This is the documented
  design trade-off of `subscriptionIndex` (see `routing_index.go`), and it is
  why deployments with many wildcard filters should be sized by wildcard-filter
  count, not by subscription count.
- **The per-publication allocation is ~1 KB at QoS 0 and ~2.3 KB at QoS 1.**
  Profiles (`go test -bench ... -memprofile`) attribute the QoS 0 allocations to
  the wire codec's decode (`gnety/codec/mqtt.decodePublish` allocates the payload
  and topic strings per packet), the broker's outbound publish construction
  (`(*client).writePublishLocked`), and one `LocalTarget` slice per publish
  (`subscriptionIndex.targets`). None of them grows with the subscription count.
- **QoS 1/2 deliveries allocate one timer each.** The broker connection's
  `(*client).retransmitLoop` creates a new `time.Timer` every time it is woken
  for an inflight message, and every live QoS 1/2 delivery wakes it. In the QoS 1 benchmark that shows up as roughly
  10 % of allocated bytes and one object per delivery. It is worth a dedicated
  look if QoS 1 delivery volume becomes the bottleneck, because a reused or
  deadline-queue timer would remove it.
- **QoS 1 costs ~5× a QoS 0 publication.** A QoS 1 publish waits for the PUBACK,
  which the broker hands to the publishing connection's writer goroutine (a
  channel handoff and a goroutine wakeup), not a direct socket write. That
  handoff, not the routing, is the difference between ~5.9 µs one-way and
  ~32 µs round trip.
- **Fanout scales with the delivery count, not with the publisher.** Per-delivery
  cost stays at ~2.4–3.0 µs from 10 to 100 subscribers; the publisher's own cost
  is negligible next to it.
- **A retained replay is linear in the retained-store size.** Each join costs
  ~7.6 ms for 1000 retained topics (~7.6 µs per replayed message, including the
  connect, the SUBSCRIBE and the QoS 1 acknowledgements). The broker scans its
  whole retained map on every subscription (`sendRetained`), so both the scan and
  the deliveries grow with the number of retained topics.

## Soak test

`TestSoak` is off by default and turns on with `GMQX_SOAK=1`; its run length is
`GMQX_SOAK_DURATION` (Go duration, default `10s`). Four workers each own a
publisher connection and a subscriber connection and repeat:
publish a batch of 16 messages whose QoS cycles 0, 1 and 2; wait until every
acknowledged QoS 1/2 message of that batch was delivered; reconnect every 64
batches — with DISCONNECT, or every third cycle by aborting the sockets, which
is the unexpected connection loss the broker must clean up after. Publishing a
batch only after the previous one was fully drained is what makes the delivery
assertion exact: no message can be acknowledged and then lost to a teardown the
test performed itself.

Run on the machine above:

```sh
GMQX_SOAK=1 GMQX_SOAK_DURATION=10s go test -run TestSoak -v ./...
```

Result:

```
soak teardown: clients=0 stored_sessions=0 indexed_sessions=0 retained=0 pending_wills=0 offline_queued=0
soak delivery ledger: acknowledged=261249 delivered_qos12=261249 duplicates=0
soak resources: goroutines=2 (baseline 2, bound 10) heap_before=428928 heap_after=651640 heap_bound=67966720 total_alloc_delta=1046284304
soak result: 380 generations, 768 connections, 24492 batches, 391872 messages (130623 QoS 0, 130626 QoS 1, 130623 QoS 2), 261249 acknowledged, 261249 delivered
```

A second run of the same command, on the same machine, produced 358 generations,
724 connections, 368,576 messages and 245,719 acknowledged / 245,719 delivered,
with the same zero tables and the same two-goroutine baseline. The counters move
with scheduling; the bounds did not.

| Soak parameter / result | Value |
|---|---|
| Duration | 10 s |
| Workers | 4 (one publisher + one subscriber each) |
| Batch | 16 messages, QoS 0/1/2 mixed |
| Batches per connection | 64 |
| Connection generations | 380 |
| Connections opened | 768 |
| Batches | 24,492 |
| Messages published | 391,872 (130,623 QoS 0, 130,626 QoS 1, 130,623 QoS 2) |
| QoS 1/2 messages acknowledged | 261,249 |
| QoS 1/2 messages delivered | 261,249 (0 missing, 0 duplicates) |
| Broker client / session / index tables after the run | 0 / 0 / 0 |
| Retained messages / pending wills after the run | 0 / 0 |
| Goroutines after the run | 2, baseline 2, bound 10 |
| Live heap after the run (after two GCs) | 651,640 B, before 428,928 B, bound 67,966,720 B |

The bounds are deliberately conservative, and this is the rationale:

- **Goroutines: baseline + 8.** A leak of a broker connection goroutine, a
  writer, a retransmission timer or a drain goroutine adds at least one goroutine
  per connection, and the run opens hundreds of them; a per-worker leak would add
  at least four. `Broker.Stop` joins every goroutine the broker started (the
  dedicated leak test asserts `baseline+2` for that), so 8 covers runtime and
  testing-package noise without hiding a real leak.
- **Heap: 2× the pre-test live heap + 64 MiB**, measured with
  `runtime.ReadMemStats` after two forced GCs. A factor alone is too tight on a
  small baseline, and a floor alone would ignore real growth on a large one; the
  pair bounds both. The 10 s run above allocated ~1.05 GB in total (all garbage,
  collected) while the live heap stayed under 1 MiB.
- **Tables empty, not bounded.** Every soak session is a clean session, so a
  session or client entry left behind after `Stop` is a bug, not a capacity
  effect. The assertions check the broker's client table, its session store, its
  routing index, its retained store and its pending wills.
- **Delivery ledger.** Per batch, every QoS 1/2 message whose acknowledgement the
  worker received must appear in that batch's delivered set; at the end the
  totals must satisfy `delivered_qos12 >= acknowledged`. Duplicates are reported
  but not failed, because at-least-once semantics permit them; every run above
  had zero.

## Reproduce

From the repository root (`libs/gmqx`):

```sh
# Fast smoke: one iteration of every benchmark, seconds in total.
go test -run '^$' -bench . -benchtime=1x ./...

# The measured baseline above.
go test -run '^$' -bench . -benchtime=2s ./...

# A single case, with a CPU profile.
go test -run '^$' -bench 'BenchmarkPublishQoS1$' -benchtime=2s \
    -cpuprofile /tmp/cpu.out -memprofile /tmp/mem.out .
go tool pprof -top /tmp/cpu.out

# The soak test (10 s by default).
GMQX_SOAK=1 GMQX_SOAK_DURATION=10s go test -run TestSoak -v ./...

# Shorter soak, useful in a pre-merge check.
GMQX_SOAK=1 GMQX_SOAK_DURATION=3s go test -run TestSoak -v ./...
```

`scripts/bench.sh` and `scripts/soak.sh` wrap the two commands
(`BENCHTIME=2s ./scripts/bench.sh`, `GMQX_SOAK_DURATION=30s ./scripts/soak.sh`).

The full `-benchtime=2s` run takes about a minute on this machine: roughly half
of it goes to the routing-index benchmarks that were already in the package, and
roughly half to the benchmark functions that produced this baseline.

## Limits of these numbers

These results are **indicative, not a capacity guarantee**. They come from one
machine, one Go version, one process, and loopback TCP:

- both ends share a CPU and a kernel; there is no network round-trip time, no
  packet loss, no TLS handshake or record overhead, and no proxy or load
  balancer in front of the broker;
- the broker runs with its default limits (inflight windows of 64, 256-packet
  write-behind and inflight queues, 8/16 MiB/s per-connection and 256 MiB/s
  broker-wide byte-rate shaping) and with no authentication, ACL, sink,
  interceptor, session store or cluster backend configured; the retained-replay
  case raises only `MaxInflightQueueSize` to 4096 so a 1000-message replay is
  not dropped by a full inflight queue;
- the benchmarks use one 16-byte payload, one topic shape and one publisher per
  case; real traffic mixes payload sizes, topic cardinalities, QoS levels and
  connection lifetimes, and has slow consumers;
- `ns/op` is wall time for a single-threaded publisher, not the throughput of a
  tuned multi-publisher deployment; the derived rates in the result table are
  simple reciprocals, useful for comparison only;
- run-to-run variance on this machine is typically 30 % for the socket-bound
  cases and reached 2× for the fanout case, so only a clearly larger change is
  visible in a single run.

## What a production sizing exercise would additionally need

- **Representative hardware and network.** The target CPU class, the real NIC and
  its interrupt distribution, the client→broker RTT and bandwidth, and the
  connection-establishment rate (including TLS), measured across the topology the
  deployment actually uses.
- **The production configuration.** TLS (including session resumption), the
  configured inflight windows, queue sizes and byte-rate limits, authentication
  and ACL hooks, the event sink, interceptors, and whichever `SessionStore`
  backs persistent sessions (including its latency and its disk).
- **The real traffic mix.** Payload-size and topic-cardinality distributions, the
  QoS mix, persistent versus clean sessions, the offline-queue depth, shared
  subscriptions, retained-message counts, and the number and behaviour of slow
  consumers. The wildcard-filter count deserves its own line in the plan, since
  it, not the subscription count, drives routing cost.
- **The cluster.** Multi-node fanout and cluster backends (gossip, TCP mesh,
  Redis), partition and failover behaviour, and the extra hops a remote publish
  takes.
- **Sustained-load evidence, not a benchmark.** Hours-long runs with CPU, heap,
  goroutine, file-descriptor and connection-count dashboards, plus CPU and heap
  profiles taken under load, so leaks and slow drifts are visible — the 10 s soak
  here is a correctness and gross-regression gate, not a leak proof.
- **OS and host tuning.** File-descriptor and ephemeral-port limits, TCP accept
  and TIME_WAIT behaviour, and CPU pinning or cgroup limits, measured at the
  connection counts the deployment expects.
- **A capacity model with headroom.** Sizing from measured per-message and
  per-connection costs plus the expected peak and burst traffic, with explicit
  headroom for failover and for the degradation of slow consumers, instead of
  reading a benchmark's `ns/op` as a serving limit.
