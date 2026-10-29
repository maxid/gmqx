# gmqx

A lightweight, embeddable **MQTT 3.1.1 broker** written in Go.

> Status: **v0.3.0** — MQTT 3.1.1 broker with TCP/TLS/WebSocket/WSS listeners,
> pluggable authentication/ACL/event/metrics hooks, connect modes and limits.
> API may change before v1.0.

## Features

- MQTT 3.1.1 over TCP, TLS, WebSocket and WSS
- CONNECT, SUBSCRIBE, PUBLISH, PING, DISCONNECT
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

MQTT 5, full QoS 2, clustering and the client library are on the roadmap (see [ROADMAP.md](ROADMAP.md)).

## License

MIT. See [LICENSE](LICENSE).
