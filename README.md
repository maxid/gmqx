# gmqx

Go implementation of [jmqx](https://github.com/maxid/jmqx) (MQTT broker/client/cluster) with feature parity, written in Go.

> Status: **experimental scaffold** (v0.0.0-dev). API is not stable.

## Planned features

- MQTT 3.1.1 / 5.0 broker and client
- TCP / TLS / WebSocket / WSS listeners
- Authentication and ACL hooks (same semantics as jmqx)
- Cluster routing: `clientId → node`, `topicFilter → node`, cross-node publish forwarding, kick broadcast
- Metrics, health and configuration

## Install

```bash
go get github.com/maxid/gmqx
```

## Compatibility

Behavioral parity with jmqx; see [ROADMAP.md](ROADMAP.md) and `docs/compatibility.md` (to be added).

## License

MIT. See [LICENSE](LICENSE).
