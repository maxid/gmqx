# gmqx

A lightweight, embeddable **MQTT 3.1.1 broker** written in Go.

> Status: **development snapshot (untagged)** — MQTT 3.1.1/5 broker; TCP/TLS/WebSocket/WSS transport via gnety,
> pluggable authentication/ACL/event/metrics hooks, connect modes and limits.
> API may change before v1.0.

## Features

- MQTT 3.1.1 and basic MQTT 5 over TCP, TLS, WebSocket and WSS
- CONNECT/CONNACK, SUBSCRIBE/SUBACK, PUBLISH, PING, DISCONNECT
- MQTT 5: CONNACK capability properties, Topic Alias, User Property forwarding,
  subscription options (no-local / retain-as-published / retain-handling), reason codes
- QoS 0/1 basic flows and QoS 2 handshake
- Retained messages, wildcard subscriptions (`+`/`#`), will messages, keepalive
- Pluggable hooks: `AuthManager`, `AclManager`, `EventSink` (lifecycle/messages),
  `MetricsManager`, `Interceptor`
- Connect modes: `UNIQUE` / `KICK` (client id takeover)
- Limits: max connections, max subscriptions, max retained messages,
  connection rate limiting
- Graceful shutdown and client-id session takeover

## Install

```bash
go get github.com/maxid/gmqx
```

## Quick start

```go
broker, _ := gmqx.New(gmqx.Config{Addr: ":1883"})
_ = broker.Start(context.Background())
defer broker.Stop(context.Background())
```

With hooks:

```go
broker, _ := gmqx.New(gmqx.Config{
    Addr: ":1883",
    Auth: myAuth{},          // implements gmqx.AuthManager
    ACL:  myACL{},           // implements gmqx.AclManager
    Sink: myDispatcher{},    // implements gmqx.EventSink
    Metrics: gmqx.NoopMetrics{},
    ConnectMode: gmqx.ConnectModeKick,
})
```

## Configuration (selected)

| Field | Default | Description |
|---|---|---|
| `Addr` / `Port` | `:1883` / `1883` | TCP listen address |
| `SecureAddr` / `SecurePort` | — / 8883 | TLS listener (requires `TLS`) |
| `WebsocketAddr` / `WebsocketPort` | — / 1884 | WebSocket listener |
| `WebsocketSecureAddr` / `WebsocketSecurePort` | — / 8884 | WSS listener |
| `WebsocketPath` | `/mqtt` | WebSocket path |
| `TLS` | disabled | `Enabled`, `Cert`, `Key`, `CA`, `RequireClientCert` |
| `MaxPacketSize` | 4 MiB | inbound packet limit |
| `ConnectMode` | `UNIQUE` | reject or kick an existing client id |
| `MaxConnections` | 0 (unlimited) | concurrent connection limit |
| `MaxTopicSubscriptions` | 0 | per-session subscription limit |
| `MaxRetainMessages` | 0 | retained message count limit |
| `ConnectionRateLimit` | 0 | connections/second token bucket |
| `Auth` / `ACL` / `Sink` / `Metrics` | allow-all / no-op | pluggable SPI |

Full MQTT 5 properties/semantics, full QoS 2, clustering and the client library are on the roadmap (see [ROADMAP.md](ROADMAP.md)).

## Transport layering

TCP/TLS/WebSocket/WSS listeners are provided by the
[`gnety`](https://github.com/maxid/gnety) transport framework (`RawHandler`
mode). gmqx owns the MQTT packet codec and broker state machine; the wire codec
may move to `gnety/codec/mqtt` later.

## Versioning

See [VERSIONING.md](VERSIONING.md).

## License

MIT. See [LICENSE](LICENSE).
