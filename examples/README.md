English | [简体中文](README.zh-CN.md)

# Examples

Four runnable programs, each answering one question. Every one is a self-contained
`main` package, so `go run ./examples/<name>` from the repository root is enough —
no third-party MQTT server, no configuration file.

- [broker-demo](#broker-demo) — how to embed a broker in your own process, and what a
  retained message does for a subscriber that arrives later.
- [client-demo](#client-demo) — how to use `github.com/maxid/gmqx/client`.
- [cluster-node](#cluster-node) — how to run a node that joins a cluster.
- [plugin-codec](#plugin-codec) — how an application-side plugin contributes codecs
  and transcodes payloads.

## Common conventions

- Each demo prints one line per step — `[1/5] action … result` — and `[done] …` when
  it finishes. `cluster-node` is the exception: it is a long-running node and logs
  through `slog`.
- Failures go to stderr as `<example>: <action>: <error>` with a non-zero exit code.
- Ctrl-C stops any example. A demo prints `[stop] interrupted`, `cluster-node`
  prints `[stop] node <id> stopped`, and both exit 0 — an example is not a service.
- `-h` prints what the example answers plus commands you can copy verbatim.
- Noisy broker internals stay quiet: the demos set a `warn`-level logger and print
  their own progress. `cluster-node -v` is the way to see the broker's debug log.

## broker-demo

**Answers:** how little code embeds a broker, and why a retained message survives the
publisher. The demo starts a broker in this process, publishes one retained message
with the library's own client, then connects a *second* client that subscribes
afterwards. Everything it prints on the last step is the retained replay.

```sh
go run ./examples/broker-demo
go run ./examples/broker-demo -topic demo/retained -message "hello from gmqx"
go run ./examples/broker-demo -addr 127.0.0.1:1883 -qos 0 -version 4
```

```
[1/5] broker listening on 127.0.0.1:60653
[2/6] publisher "broker-demo-pub" connected (MQTT level 5)
[3/6] published retained "hello from the gmqx broker demo" to demo/retained (QoS 1)
[4/6] subscriber "broker-demo-sub" subscribed to demo/retained and demo/retained_reply (granted [1 0])
[5/6] replayed demo/retained to a subscriber that connected later (QoS 1, retain true): hello from the gmqx broker demo
[6/6] the broker's own publish reached demo/retained_reply: hi!
[done] retained message replayed to a later subscriber; broker stopped
```

Steps 2 and 4 also show the other half: the broker has an `EventSink` that sees every
accepted connection and answers with a greeting of its own — a **retained** `hi!` on
`<topic>_reply`, published through `Broker.PublishMessage` and routed by the
`Dispatcher` like any other publication. It is retained on purpose: `OnConnect` runs
before the client has subscribed to anything, so a plain publish would have nobody to
reach.

`-addr 127.0.0.1:0` (the default) picks an ephemeral port, so the demo never collides
with a broker you already run. Client details — QoS 2, reconnects, MQTT 3.1.1 versus
5 — are `client-demo`'s subject.

## client-demo

**Answers:** how to subscribe, publish and receive with `github.com/maxid/gmqx/client`.
It starts an in-process broker unless `-addr` names an existing one, then runs one
round trip and prints the delivery.

```sh
go run ./examples/client-demo
go run ./examples/client-demo -topic demo/hello -message "hello" -qos 2 -version 5
go run ./examples/client-demo -version 3      # MQTT 3.1
go run ./examples/client-demo -addr 127.0.0.1:1883 -topic demo/hello
```

```
[1/5] in-process broker listening on 127.0.0.1:61027
[2/5] connected as "client-demo" (MQTT level 4)
[3/5] subscribed to demo/hello (granted [1])
[4/5] published "hello from the gmqx MQTT client" to demo/hello (QoS 1)
[5/5] received "hello from the gmqx MQTT client" on demo/hello (QoS 1, retain false)
[done] publish/subscribe round trip complete; client closed
```

The granted QoS in step 3 is the server's answer, not the requested one: subscribe
with `-qos 2` and a server that only grants 1 shows up as `granted [1]`.
`-version 3` runs the same round trip over MQTT 3.1.

The handler is passed **on the subscription** (`client.Subscription.Handler`), which
is the client's dispatch model: a publication is matched against the registered
filters, every matching one runs, and `Config.OnMessage` only answers what no filter
claimed. `Client.AddRoute` registers the same thing without a subscription. This is
the paho model — [`docs/CLIENT.md`](../docs/CLIENT.md) has the mapping table.

## cluster-node

**Answers:** how to run a broker node that joins a cluster, so a publish on one node
reaches a subscriber on another. This one is a long-running node rather than a demo:
start two processes with the same seed list and point an MQTT client at each.

```sh
go run ./examples/cluster-node -node a -addr 127.0.0.1:1883 -cluster-addr 127.0.0.1:7901 -seeds 127.0.0.1:7901,127.0.0.1:7902
go run ./examples/cluster-node -node b -addr 127.0.0.1:1884 -cluster-addr 127.0.0.1:7902 -seeds 127.0.0.1:7901,127.0.0.1:7902
```

```
node a
  mqtt    127.0.0.1:1883
  cluster 127.0.0.1:7901
  seeds   [127.0.0.1:7901 127.0.0.1:7902]
[stop] node a stopped
```

Nodes dial only the seeds whose address sorts after their own cluster address, so a
symmetric seed list yields exactly one connection per pair. Authentication is
allow-all: this is a local verification harness, never a production deployment. Stop
a node with Ctrl-C; it drains and exits 0. Add `-v` to see the broker's debug log.

Every accepted connection makes the node publish a retained `hi!` on
`<greet>_reply` (`-greet`, default `cluster/hello`; `-greet ""` turns it off), and the
cluster replicates it like any retained publication. So a client that subscribes on
**another** node still finds it — the shortest way to see the cluster work with one
MQTT client and no second connection:

```sh
# terminal 1 and 2: the two nodes above, unchanged
# then, against node b (the greeting came from node a):
go run ./examples/client-demo -addr 127.0.0.1:1884 -topic cluster/hello_reply
```

## plugin-codec

**Answers:** how an application-side plugin extends the broker. The broker has no idea
what JSON, CSV or a binary frame is; the plugin registers the codecs and the
transcoder, the subscriber receives the transcoded bytes. The last step unloads the
plugin and publishes again, so passthrough is visible too.

```sh
go run ./examples/plugin-codec
go run ./examples/plugin-codec -format csv
go run ./examples/plugin-codec -topic demo/telemetry -format json
```

```
[1/6] broker listening on 127.0.0.1:62260
[2/6] plugin codec-demo is enabled (codecs [application/json application/octet-stream text/csv])
[3/6] client "plugin-codec" subscribed to demo/telemetry
[4/6] published json, received as application/octet-stream: id 7 state on
[5/6] decoded with the same codec set: map[id:7 state:on]
[6/6] plugin unloaded; the payload passes through unchanged (21 bytes)
[done] codecs registered, transcoded and removed by an application-side plugin
```

Step 5 decodes the received bytes through the same `hook.CodecSet` the broker used,
which is how the demo proves the payload really is the expected media type and not
just opaque bytes. With `-format csv` the same round trip runs the other direction:
CSV in, JSON out.
