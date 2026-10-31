# Interoperability matrix

This directory is the interoperability matrix of the gmqx MQTT broker and the
gmqx client package. It answers one question with evidence instead of opinion:
**do the broker and the client talk to implementations they did not write?**

The matrix runs in three directions:

| Direction | Under test | Reference implementation | Cases |
|---|---|---|---|
| paho → broker | the gmqx broker, in process on an ephemeral port | [`github.com/eclipse/paho.mqtt.golang`](https://github.com/eclipse/paho.mqtt.golang), a third-party MQTT client | QoS 0/1/2 publish/subscribe round trips (QoS 2 asserted exactly once), retained message replayed to a subscriber that connects later, will published when the client's socket dies and *not* published on a clean `DISCONNECT`, persistent session that receives messages queued while it was away, 256 KiB payload round trip, a publish above the broker's Maximum Packet Size rejected with the client seeing the disconnect |
| client → mosquitto | the gmqx `client` package | eclipse-mosquitto 2, running in a container | QoS 0/1/2 round trips over MQTT 3.1.1 **and** MQTT 5, retained message replayed to a late subscriber, will on an abrupt socket kill and none on a clean close |
| reference clients → broker | the gmqx broker on the host | `mosquitto_pub` / `mosquitto_sub` from the eclipse-mosquitto image, run with `docker exec` | a reference publication reaches a host subscriber at QoS 2, a host publication reaches a reference subscriber, a retained reference publication is replayed to a late host subscriber |

## Why this is its own module

`interop/` is a separate Go module (`github.com/maxid/gmqx/interop`) that
replaces `github.com/maxid/gmqx` with the source tree directly above it
(`replace github.com/maxid/gmqx => ..`). The third-party client and the docker
tooling therefore never enter the library's `go.mod`: the library stays
dependency-free, and this matrix can add an implementation without changing the
library's own contract. Nothing here is imported by the library.

## How to run it

```sh
cd libs/gmqx/interop

go test ./...                  # the whole matrix
go test -v -count=1 ./...      # per-case output, shows what ran and what skipped
go test -run TestPaho -v ./... # only the third-party client cases, no docker
go test -run 'Mosquitto|Reference' -v ./...  # only the container cases
```

Requirements:

- Go 1.27.
- Docker, for the mosquitto cases only. The image `eclipse-mosquitto:2` is
  pulled on demand (the pull is bounded to three minutes and happens once per
  image), every container is removed again at the end of its test, and the host
  port is picked from a free loopback port at run time. Nothing else has to be
  installed: the reference *broker* and the reference *clients* both come from
  the image.
- No docker is needed for the paho cases; they start an in-process broker on an
  ephemeral port.

The whole matrix finishes in a few seconds once the image is present; a cold
image pull is the only slow step. Every wait is bounded, so a run cannot hang:
readiness polls have deadlines (30 s for the container, 10 s for a handshake),
message waits time out after 10 s, docker commands are killed by their context,
and the reference subscribers stop themselves after 20 s.

## What is skipped, and why

The suite is safe to run anywhere: a missing prerequisite skips a case with the
reason printed, it never fails the run.

| Situation | Result |
|---|---|
| docker is not installed or its daemon is not running | `TestClientAgainstMosquitto` and `TestBrokerAgainstReferenceClients` skip: "docker is unavailable, so the reference broker and its clients are skipped: ..." |
| the `eclipse-mosquitto:2` image cannot be pulled (no network) | the same two tests skip: "the eclipse-mosquitto image cannot be pulled ..." |
| the container starts but does not accept a TCP connection, or does not answer an MQTT `CONNECT`, within 30 s | the test skips and prints the last lines of the container log |
| the third-party client module cannot be built (offline machine with a cold module cache) | `go test -tags interop_without_paho ./...` builds without it and reports the six paho cases as skipped with a reason. The default build compiles paho in, so it needs the module; `go.sum` is committed for that |

The paho cases are always reported individually, so `-v` output says exactly
which of them ran and which were skipped.

## Why the cases are deterministic

- The broker is in-process on an ephemeral port; a container broker is probed
  with a real MQTT handshake (a TCP accept from a forwarded port is not proof
  that the broker is listening).
- The persistent-session case does not sleep: the test passes its own
  `SessionStore` to the broker and polls it until the session is stored and the
  offline queue holds the expected number of messages, then reconnects.
- The direction-B subscriber is confirmed through the broker's own
  `MetricsManager` (`IncrementSubscribeEvents`), not by reading the reference
  client's stdout, which is block-buffered on a pipe until the process exits.
- A will is provoked by resetting the client's socket (`SO_LINGER` 0) and
  closing it behind the client's back, so no `DISCONNECT` is ever sent; the
  clean case closes the client normally. The two cases differ only in that.
- "Nothing must arrive" assertions listen for a fixed quiet window; the two
  clean-disconnect will cases follow theirs with a positive control message, so a
  dead subscriber cannot make the negative assertion pass.

## Files

| File | Contents |
|---|---|
| `go.mod`, `go.sum` | the matrix module: the library (replaced with `..`), paho, and their sums |
| `helpers_test.go` | broker launcher, bounded waits, delivery recorder, socket kill, gmqx client helpers |
| `paho_test.go` | the paho → gmqx broker cases |
| `paho_skip_test.go` | the same cases reported as skipped under the `interop_without_paho` build tag |
| `mosquitto_test.go` | the container harness plus the gmqx client → mosquitto and reference clients → gmqx broker cases |

Container names start with `gmqx-interop-`; `docker ps -a` should never show one
after a run.

## What this matrix does not cover

TLS, WebSocket and WSS listeners, authentication and ACLs, clustering, MQTT 3.1
(`MQIsdp`), shared subscriptions and subscription identifiers, session expiry
across a broker restart, and throughput or latency. Those belong to the library's
own test suite or to a benchmark baseline, not to an interoperability gate.
