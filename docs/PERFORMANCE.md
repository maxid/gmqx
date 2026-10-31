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

The sequential benchmarks below measure one publish→deliver round trip at a
time; a broker's sustained rate under many concurrent connections is a different
measurement and lives in its own harness. See
[Concurrent throughput](#concurrent-throughput-200-connections) for the
200-connection number, its design and its before/after results.

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

Results (real output, reformatted into a table). These are the baseline this
document started from; the throughput work below re-measured four of them (see
the sequential before/after table in
[What changed](#what-changed-and-what-each-change-was-worth)):

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
- **The per-publication allocation is under 1 KB at QoS 0 and ~1.7 KB at QoS 1.**
  Profiles (`go test -bench ... -memprofile`) attribute the QoS 0 allocations to
  the wire codec's decode (`gnety/codec/mqtt.decodePublish` allocates the payload
  and topic strings per packet) and the broker's outbound publish construction
  (`(*client).writePublishLocked`). The per-publish `LocalTarget` slice of the
  built-in dispatcher was removed by the throughput work (the public
  `Broker.LocalTargets` still returns a slice). None of the remaining ones grows
  with the subscription count.
- **QoS 1/2 deliveries no longer allocate a timer each.** The connection's
  `(*client).retransmitLoop` now owns one timer for the life of the connection
  and is woken only when the earliest retransmission deadline moves; before the
  throughput work it created a `time.Timer`, rescanned the whole inflight table
  and woke the loop for every delivery. The sequential QoS 1 benchmark dropped
  from 2,344 B/op and 19 allocs/op to 1,667 B/op and 13 allocs/op.
- **QoS 1 costs ~5× a QoS 0 publication.** A QoS 1 publish waits for the PUBACK,
  which the broker hands to the publishing connection's writer goroutine (a
  channel handoff and a goroutine wakeup), not a direct socket write. That
  handoff, not the routing, is the difference between one-way and round-trip
  time; coalescing the writer's queue (see below) lowered the round trip but did
  not remove the handoff.
- **Fanout scales with the delivery count, not with the publisher.** Per-delivery
  cost is ~1.6–1.8 µs from 10 to 100 subscribers after the writer coalescing
  (it was ~2.4–3.0 µs); the publisher's own cost is negligible next to it.
- **A retained replay is linear in the retained-store size.** Each join costs
  ~7.6 ms for 1000 retained topics (~7.6 µs per replayed message, including the
  connect, the SUBSCRIBE and the QoS 1 acknowledgements). The broker scans its
  whole retained map on every subscription (`sendRetained`), so both the scan and
  the deliveries grow with the number of retained topics.

## Concurrent throughput (200 connections)

`BenchmarkPublishQoS1` is one publish→deliver round trip at a time, so it reports
a latency — not the message rate a broker sustains when hundreds of connections
publish at once. `throughput_test.go` measures that rate. It reproduces the shape
the reference implementation of this broker family reports (200 publisher
connections, a fixed 5 s window, a per-second interval throughput in msg/s), so
the two numbers can be compared, and it adds what that figure does not report:
the delivered rate and the p50/p99 delivery latency.

### The harness

`TestThroughput` is gated by `GMQX_THROUGHPUT=1`; without it the test skips and
the normal suite is untouched.

- **Topology.** One broker per case on `127.0.0.1:0`. 200 publisher connections
  (`GMQX_THROUGHPUT_CLIENTS`), each its own TCP connection, publishing to its own
  topic `bench/throughput/p<shard>/<publisher>`. 4 subscriber connections
  (`GMQX_THROUGHPUT_SUBS`); subscriber *s* subscribes to
  `bench/throughput/p<s>/#`, so the publishers are sharded across the subscribers
  and every publication is delivered to exactly one of them — no fanout
  amplification between the published and delivered counts.
- **Load.** The publishers run flat out for the window (5 s,
  `GMQX_THROUGHPUT_DURATION`) with no per-message round trip in the loop. At
  QoS 1/2 each publisher also keeps a bounded pipeline of unconfirmed
  publications (256, `GMQX_THROUGHPUT_UNACKED`), released per PUBACK/PUBCOMP, so
  the send rate is not request/response bound.
- **Closed loop.** A publication enters the harness only when an earlier one was
  delivered or dropped (a shared pacer, sized to each subscriber's inflight
  window plus 4096 publications of slack; `GMQX_THROUGHPUT_INFLIGHT` overrides
  it). Without it, a QoS 0 case fills the kernel's socket buffers —
  on this machine the write eventually fails with "no buffer space available" —
  and a QoS 1/2 case buries the broker's bounded inflight queue and measures how
  fast it drops messages. The closed loop keeps the publisher queue in the
  publisher, and the per-second samples show the warmup instead of hiding it in
  an average.
- **Payload and latency.** Payloads are 64 B and 1 KiB
  (`GMQX_THROUGHPUT_PAYLOAD`). The first 8 bytes are a per-publisher sequence
  number, the next 8 the sender's wall clock in Unix nanoseconds; the subscriber
  computes the delivery latency from that stamp, so p50/p99 cover the whole
  publish→deliver path inside one process (the same clock).
- **What is counted.** `published/s` is what the publishers finished writing,
  `acked/s` the PUBACKs (QoS 1) or PUBCOMPs (QoS 2) they received, `accepted/s`
  what the broker routed, and `delivered/s` what the subscribers received. The
  headline deltas are taken between the first and the last sample of the window,
  so nothing a publisher still holds when the window closes is counted.
- **Client.** The harness reuses the wire client of `bench_test.go` rather than
  the `client` package: the client package's delivery goroutine and reconnect
  machinery would sit in front of the broker at 200 connections. The publisher
  hot loop writes pre-framed PUBLISHes in batches straight to the socket.
- **Harness configuration.** Byte-rate shaping is disabled, the outbound inflight
  window and its queue are widened (256 / 8192) and the write-behind queue is
  4096 packets, so the number is the broker's throughput and not one of its
  documented flow-control defaults. Drops are still counted and printed, and
  every other limit keeps its default.

Run it (from `libs/gmqx`):

```sh
# Full matrix: QoS 0/1/2 × 64 B / 1 KiB, 200 connections, 5 s each (~35 s).
GMQX_THROUGHPUT=1 go test -run TestThroughput -v .

# One case (the one profiled below).
GMQX_THROUGHPUT=1 GMQX_THROUGHPUT_QOS=1 GMQX_THROUGHPUT_PAYLOAD=64 \
    go test -run TestThroughput -cpuprofile /tmp/cpu.out \
    -memprofile /tmp/mem.out .
go tool pprof -top /tmp/cpu.out
```

### Result

Same machine as the rest of this document, same command, 200 publisher
connections, 4 subscriber connections, 5 s window. "Before" is the broker at the
start of this measurement (the code the sequential baseline above was taken on);
"after" is the same broker with the changes listed below.

| QoS | payload | | published/s | acked/s | delivered/s | p50 | p99 | drops |
|---:|---:|---|---:|---:|---:|---:|---:|---|
| 0 | 64 B | before | 545,441 | — | 350,296 | 40.9 ms | 102.1 ms | 1,040,017 |
| 0 | 64 B | after | 1,391,076 | — | 1,382,662 | 6.8 ms | 59.1 ms | 45,767 |
| 0 | 1 KiB | before | 486,631 | — | 343,195 | 46.0 ms | 85.6 ms | 739,026 |
| 0 | 1 KiB | after | 986,969 | — | 983,664 | 9.9 ms | 87.6 ms | none |
| 1 | 64 B | before | 145,798 | 145,700 | 140,650 | 115.5 ms | 280.6 ms | 9,067 |
| 1 | 64 B | after | 627,855 | 627,728 | 617,757 | 24.2 ms | 73.8 ms | 40,358 |
| 1 | 1 KiB | before | 137,999 | 137,780 | 132,678 | 122.3 ms | 305.6 ms | 10,284 |
| 1 | 1 KiB | after | 477,730 | 477,476 | 471,028 | 35.0 ms | 94.5 ms | 17,520 |
| 2 | 64 B | before | 74,792 | 74,376 | 71,126 | 234.3 ms | 479.2 ms | 1,130 |
| 2 | 64 B | after | 175,380 | 175,126 | 168,885 | 87.4 ms | 242.0 ms | 15,687 |
| 2 | 1 KiB | before | 71,278 | 71,175 | 67,796 | 230.9 ms | 444.6 ms | none |
| 2 | 1 KiB | after | 136,881 | 136,707 | 133,032 | 109.7 ms | 329.0 ms | 1,995 |

Run-to-run variance on this machine is real at this load: repeated QoS 0 / 64 B
runs delivered 350k, 358k and 392k msg/s before, and 1.38M, 1.57M, 1.61M, 1.63M,
1.70M and 1.76M after; QoS 1 / 64 B delivered 139k, 141k and 143k before, and
582k, 597k, 618k, 685k, 733k and 749k after. The medians of those runs are
~358k → ~1.63M msg/s at QoS 0 (≈4.5×) and ~141k → ~617k msg/s at QoS 1 (≈4.4×);
QoS 2 improved ≈2×. The delivered rate at QoS 1/2 is the *sustained end-to-end*
rate: the publisher's PUBACK bounds the pipeline, the subscriber's inflight
window bounds the delivery, and the small `inflight_overflow` count is the
broker's documented overflow policy for a delivery that finds the bounded queue
full.

The per-second curve of the profiled case (QoS 1, 64 B, after), which is what the
harness prints, shows the window is steady rather than a burst:

```
case qos=1 payload=64B clients=200 subscribers=4 window=5s
  t         published/s      acked/s   accepted/s  delivered/s
  t+1.0s         738171       737119       737429       718531
  t+2.0s         679143       678907       679377       666380
  t+3.0s         684331       685498       684760       678784
  t+4.0s         684430       683944       684185       682466
  t+5.0s         684369       683964       684381       680042
  total          694090       693887       694027       685241   (p50=21740µs p99=75108µs n=3444572)
```

### Profile: what the 200-connection load costs

CPU profile of the QoS 1 / 64 B case, same load. Percentages are of the profile's
total samples (5.7–6.6 cores busy on a 10-core machine — the harness shares the
process, so its share is included).

Before (the code the sequential baseline was taken on):

| Cost | Share |
|---|---:|
| one `write(2)` per packet in the per-connection writer goroutine | 52 % |
| read + codec decode of the inbound PUBLISH (payload/topic allocation) | 27 % |
| routing: the per-publish target slice and the wildcard scan | 4.3 % |
| `windowFullLocked`: a full scan of the inflight map on every delivery | 4.3 % |
| allocation and GC (`mallocgc`, `scanObject`, `sweep`) | ~8 % |

Allocations were 2.97 GB over the window (594 MB/s), attributed to the codec's
`decodePublish` (33 %), the typed PUBACK (27 %), the routing target slice and
merge map (12 %), the pending-delivery slice (10 %) and the codec's body buffer
(9 %).

After:

| Cost | Share |
|---|---:|
| read syscalls and the codec decode | 45 % read, 34 % of it inside the broker's `readMessage`/`Decode` |
| write syscalls | 37 % total, 17 % in the broker's writer (`flushBatch`), the rest in the harness |
| the broker's publish → route → deliver path | ~10 % |
| allocation and GC | ~6 % |

The writer now carries **12.4 packets per write** (measured over the window), so
the write syscall count is an order of magnitude below one per packet.
Allocations are still 13.3 GB per 5 s window, but the window delivered 5× more
messages: per delivered message they are roughly unchanged (~4 KB), dominated by
the codec's ~400-byte `mqtt.Properties` value embedded in every packet struct and
by the pending/inflight tables copying it.

**Remaining top costs** (in order):

1. **The inbound read path.** Every publisher frame costs a `read(2)`-backed
   `bufio` fill plus the codec's decode, which allocates the payload and the
   topic per packet. This is now the largest single cost and the next lever: a
   broker-side decoder that reads a PUBLISH body into a reusable buffer would
   remove the codec's per-packet allocations.
2. **Allocation of the delivery tables.** A pending or inflight delivery copies
   the codec's `mqtt.Properties` value (~400 B) even though MQTT 3.1.1 traffic
   never carries one. A pointer-sized property block cut the bytes by ~45 % but
   did not move the rate outside the noise band, so it was reverted (see below);
   pooling `mqtt.Publish`/`inflightMessage` is the remaining option.
3. **The subscriber connection's mutex.** At 200 publishers over 4 subscribers,
   `sendPublish` spends most of its wall time waiting on one connection's lock:
   the mutex profile attributes 116 s of delay over a 3 s window to it. More
   subscriber shards or fewer publishers per shard scale better; a lock-free
   per-connection delivery queue is the structural fix.
4. **QoS 1/2 delivery window.** Per subscriber connection the delivered rate is
   window / acknowledgement round trip. The harness configures a 256-deep window;
   at 200 publishers this is the bound, and the ~1 % `inflight_overflow` drops
   are its documented overflow.

### What changed, and what each change was worth

Each change kept `go test -race ./...` green and the documented semantics
(bounded queues, no dropped QoS 1/2 message, FIFO ordering, one writer goroutine
per connection).

| Change | Measured effect |
|---|---|
| **Coalesced writes.** The writer goroutine drains the packets already queued and writes them with one `write(2)`, up to 64 packets / 64 KiB, under the same write deadline and in the same queue order. | QoS 0 delivered 247k → 1.37M msg/s (5.5×); QoS 1 acknowledged 486k → 639k (+31 %), measured with the harness of that step. |
| **O(1) inflight window.** `windowFullLocked`/`receiveMaximumFullLocked` read counters maintained with the inflight map instead of scanning the map on every delivery — the scan ran under the connection lock 200 publishers share. | QoS 1 acknowledged 639k → 1.59M msg/s (+148 %); delivered 22k → 80k (3.6×), same harness. |
| **Compact acknowledgements.** A PUBACK/PUBREC/PUBREL/PUBCOMP with a zero reason code and no properties travels as its packet id: the writer builds the four-byte frame, no typed packet and no allocation. | The PUBACK path was 2.52 GB (27 %) of the window's allocations before and is ~0 after; no throughput change outside the noise band once the window scan was gone. |
| **Reused retransmission timer.** One timer per connection, reset only when the earliest retransmission deadline moves; a delivery no longer wakes the loop, scans the whole inflight table and allocates a `time.Timer`. | No throughput change outside the noise band (79.7k → 80k delivered in the harness of that step), but it removes one timer allocation and one whole-table scan per delivery, both under the connection lock. |
| **No routing allocation.** The built-in dispatcher collects targets into a stack array instead of a new slice per publish, and a topic that matches exactly one wildcard filter skips the per-session merge map. The targets are still collected under the index read lock and delivered after it is released (a delivery can close a connection and unregister its session, which takes that lock for writing). | Routing's share of CPU fell 4.3 % → 0.7 %; its allocation 1.15 GB → 0.1 % of the profile. `BenchmarkLocalTargets` is unchanged (the public `LocalTargets` still returns a slice, one allocation); `BenchmarkFanout/subscribers=100` went 345 µs → 203 µs (41 %) from the coalesced writes. |

The sequential benchmarks changed in allocations, which are reproducible, while
their `ns/op` stayed inside the run-to-run noise this machine shows (the same
documented 30 % band, up to 2× for fanout). Two paired before/after runs of
`go test -run '^$' -bench 'PublishQoS|Fanout' -benchtime=2s` gave:

| Benchmark | before | after |
|---|---|---|
| `BenchmarkPublishQoS0` | 1,024 B/op, 9–10 allocs/op | 928 B/op, 8–9 allocs/op |
| `BenchmarkPublishQoS1` | 2,344 B/op, 19 allocs/op | 1,667 B/op, 13 allocs/op |
| `BenchmarkClientPublishQoS1` | 3,800 B/op, 28 allocs/op | 3,121 B/op, 22 allocs/op |
| `BenchmarkFanout/subscribers=10` | 59.5 µs and 20.7 µs | 32.3 µs and 18.0 µs |
| `BenchmarkFanout/subscribers=100` | 345.3 µs and 265.2 µs | 203.3 µs and 121.3 µs |

The fanout case improved in every pair (the coalesced writes are one write per
subscriber batch instead of one per delivery); the single-publisher cases moved
by less than the noise, and `BenchmarkClientPublishQoS1` — one request/response
round trip — is flat. The concurrent harness is where the changes are visible:
200 connections publish continuously, so a per-packet write or a per-delivery
map scan is paid millions of times per second instead of once per `ns/op`.

Tried, measured and reverted or rejected:

- **Pointer-sized property block in the pending queue.** The pending table's
  allocations (5.7 GB over the window) are dominated by copying the codec's
  ~400-byte `mqtt.Properties` value into every queued delivery; making it a
  pointer removed that copy. The delivered rate moved only inside the ±10 %
  run-to-run noise, so it was reverted rather than kept as extra complexity.
- **Direct write of tiny control packets from the reader goroutine.** Measured
  packets-per-write after coalescing is 12.4, so a direct PUBACK write would turn
  one write per 12 acknowledgements into one write per acknowledgement (an order
  of magnitude more syscalls) and would break the documented single-writer FIFO
  order. Not adopted.
- **`jumbo` batches / larger write batch.** No additional effect beyond the
  64-packet / 64 KiB bound already in place; the average batch is 12.4 packets.
- **More subscriber shards.** 8 and 50 subscriber connections on the same 200
  publishers do not raise the delivered total (the machine is syscall- and
  lock-bound); 4 is kept as the default and matches the sharded topic space.

One defect was found by the harness while doing this and fixed before the
numbers above: the batch drain loop treated a compact acknowledgement (whose
`msg` field is nil) as "queue empty" and silently dropped it, which showed up as
a QoS 1 publisher pipeline that filled up and stalled.

### Limits of the concurrent numbers

The same limits as the rest of this document apply, plus two specific to this
harness: the load generator and the broker share this process's CPUs (the
reference figure is measured the same way), and the number depends on the
subscriber shard count and on the harness's widened flow-control windows, both
documented above. It is a single-machine, loopback, one-process capacity reading
— comparable to the reference's figure for the same reason (200 concurrent
publisher connections, a fixed sampling window, msg/s) and comparable across
versions of this broker for the same harness — not a deployment guarantee.

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

# The concurrent throughput matrix (200 connections, 5 s per case).
GMQX_THROUGHPUT=1 go test -run TestThroughput -v .

# One throughput case with CPU and heap profiles.
GMQX_THROUGHPUT=1 GMQX_THROUGHPUT_QOS=1 GMQX_THROUGHPUT_PAYLOAD=64 \
    go test -run TestThroughput -cpuprofile /tmp/cpu.out -memprofile /tmp/mem.out .
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
