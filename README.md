# gmqx

Go implementation of [jmqx](https://github.com/maxid/jmqx) (MQTT broker/client/cluster) with feature parity, written in Go.

> Status: **v0.2.2** — single-node MQTT 3.1.1 TCP broker with jmqx-aligned SPIs, connect modes, limits and metrics. API may change before v1.0. See [docs/PARITY.md](docs/PARITY.md) for the Java jmqx parity matrix.

## Features (v0.1.0)

- MQTT 3.1.1 over TCP (CONNECT/SUBSCRIBE/PUBLISH/PING/DISCONNECT, QoS 0/1 basic, QoS 2 handshake)
- Retained messages, wildcard subscriptions (`+`/`#`), will messages, keepalive timeout
- Authentication / ACL / event hooks (`Authenticator`, `ACL`, `EventSink`)
- Client-id session takeover and graceful shutdown
- No XDMP business semantics: device auth/TSL/telemetry belong to the caller (xdmp-hub adapter)

## Install

```bash
go get github.com/maxid/gmqx
```

## Compatibility

Behavioral parity with jmqx; see [ROADMAP.md](ROADMAP.md) and `docs/compatibility.md` (to be added).

## License

MIT. See [LICENSE](LICENSE).
