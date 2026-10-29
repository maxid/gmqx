# Changelog

## [Unreleased]

## [0.4.0] - 2026-10-29

- Basic MQTT 5 support: CONNECT with properties (skipped), v5 CONNACK/SUBACK/UNSUBACK formats,
  v5 PUBLISH property block, v5 negative ACK reason codes (Not authorized, Quota exceeded,
  Client Identifier not valid), tests for v5 connect/pub-sub/auth.


## [0.3.0] - 2026-10-29

- Multi-listener support: TCP, TLS (server cert/optional client cert), WebSocket and WSS.
- WebSocket subprotocol validation and path configuration.
- TLS/WS integration tests; listener address accessors.


## [0.2.3] - 2026-10-29

- Documentation cleanup: the library README/docs are product-neutral and self-contained.


## [0.2.2] - 2026-10-29

- Bump version constant; include connect mode tests.

## [0.2.1] - 2026-10-29

- Add tests for UNIQUE/KICK connect modes.

## [0.2.0] - 2026-10-29

- Broker SPIs: AuthManager, AclManager, EventSink, MetricsManager, Interceptor.
- ConnectMode UNIQUE/KICK, connection/subscription/retained limits, connection rate limiting.
- Metrics counters and lifecycle callbacks (connect/disconnect/lost/ping).

## [0.1.0] - 2026-10-29

- Initial single-node MQTT 3.1.1 broker: TCP listener, CONNECT/SUBSCRIBE/PUBLISH/PING/DISCONNECT,
  QoS 0/1/2 basic flows, retained messages, wildcard subscriptions, will and keepalive.
