# Roadmap

- [x] MQTT 3.1.1 TCP packet codec
- [x] Session/subscriptions/wildcards/QoS 0–2 basic/retained/will/keepalive
- [x] Auth & ACL hook interfaces
- [x] Connect modes (UNIQUE/KICK), limits, connection rate limiting, metrics hooks
- [x] TLS / WebSocket / WSS listeners
- [x] MQTT 5 handshake, property skipping and basic reason codes
- [x] MQTT 5: CONNACK capabilities, Topic Alias, User Property forwarding, subscription options
- [ ] Full MQTT 5 properties/semantics (session expiry, receive maximum, packet limits)
- [ ] Full QoS 2 state machine and inflight limits
- [ ] Cluster membership + session/topic routing + kick broadcast
- [ ] Client library
- [ ] Core conformance tests (mosquitto/paho clients)
- [ ] Benchmarks and soak tests
- [ ] v1.0 API stabilization
