# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.2.0] - 2026-10-29

- Align broker SPI with jmqx: AuthManager/AclManager/EventSink/MetricsManager/Interceptor.
- ConnectMode UNIQUE/KICK, connection/subscription/retained limits, connection rate limiting.
- jmqx parity matrix (docs/PARITY.md) and phased roadmap.


## [0.1.0] - 2026-10-29

- Initial single-node MQTT 3.1.1 broker: TCP listener, CONNECT/SUBSCRIBE/PUBLISH/PING/DISCONNECT,
  QoS 0/1/2 basic flows, retained messages, wildcard subscriptions, will and keepalive.
- Hooks: Authenticator, ACL, EventSink; client-id takeover; graceful shutdown.


- Initial scaffold.
