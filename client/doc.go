// Package client is the MQTT client of the gmqx family.
//
// It speaks MQTT 3.1.1 (mqtt.Version311) and MQTT 5 (mqtt.Version5) over TCP or
// TLS, and it implements the client half of the protocol: the QoS 1 and QoS 2
// state machines in both directions, reconnection with exponential backoff and
// jitter, keepalive, MQTT 5 properties, and the flow-control limits a server
// advertises. It depends only on the standard library and the MQTT wire codec
// (gnety/codec/mqtt); there is no third-party dependency and no reference to the
// broker's internals, so the same package can talk to any MQTT 3.1.1/5 server.
//
// # Connections and goroutines
//
// Dial opens one connection, completes CONNECT/CONNACK and returns a Client.
// While a connection lives the client runs exactly one reader goroutine, one
// writer goroutine and one delivery goroutine, plus one supervisor goroutine
// that owns the connection lifetime (it is what performs a reconnect). Nothing
// runs per message. Every one of those goroutines is stopped and joined by
// Close, so a closed client leaves no goroutine behind.
//
// # Ordering, backpressure and memory bounds
//
// Every outgoing packet goes through one bounded FIFO, Config.OutboundQueueSize
// (default 256 packets), drained by the single writer goroutine. Packets
// therefore leave in the order they were queued, and one client can never grow
// its writer backlog beyond that many packets. When the queue is full the
// caller blocks until the writer catches up, the caller's context is done or the
// client is closed; the client does not drop the packet. Dropping would break
// the promise a QoS 1/2 publish makes, and returning "queue full" would make a
// caller lose a message it believed was accepted; blocking is the same
// backpressure TCP applies and every API entry point takes a context to bound
// the wait. QoS 0 publishes are therefore also bounded, not silently discarded.
//
// The delivery side is a second bounded FIFO (same size) consumed by the
// delivery goroutine, which invokes Config.OnMessage. A slow callback
// backpressures the connection through that queue and through TCP; it does not
// block the writer, the reader's control-packet processing beyond the queue
// limit, or a client method. Callbacks never run while the client holds a lock.
//
// # QoS 1 and QoS 2
//
// Outbound, PublishMessage with QoS 1 returns after PUBACK and with QoS 2 after
// PUBCOMP; the inflight entry keeps the original packet so a retransmission is
// the same packet id with DUP set (a PUBLISH) or a repeated PUBREL. The inflight
// window is Config.MaxInflight, lowered by the Receive Maximum the server sends
// in its CONNACK; a publish that would exceed the window waits for a free slot
// instead of being dropped or reordered. Inbound, a QoS 1 PUBLISH is
// acknowledged with PUBACK and a QoS 2 PUBLISH with PUBREC, is delivered exactly
// once (the packet id is remembered until PUBREL) and is completed with PUBCOMP.
//
// # Reconnection
//
// A connection that ends unexpectedly (a socket error, a keepalive timeout) is
// reported through Config.OnDisconnect and, when Config.Reconnect.Enabled is
// set, re-established with exponential backoff plus jitter, bounded by
// ReconnectPolicy.MaxAttempts (zero means unlimited). After a successful
// reconnect the client reports Config.OnConnect(sessionPresent), resends every
// unconfirmed QoS 1/2 exchange (DUP for a PUBLISH, PUBREL for an exchange
// awaiting PUBCOMP) and, when the server did not keep a session, restores its
// subscriptions. A user-initiated Close never reconnects.
//
// # Keepalive
//
// With a non-zero Config.KeepAlive the writer sends PINGREQ once per interval
// and treats a PINGRESP that does not arrive within one further interval as a
// dead connection, which is then handled exactly like a transport failure (so a
// reconnect happens when it is enabled). KeepAlive == 0 disables both the
// PINGREQ and the watchdog.
//
// # MQTT 5 properties
//
// The client sends the properties a client legitimately sets — Session Expiry
// Interval, Receive Maximum and Maximum Packet Size in CONNECT; user properties,
// content type, response topic, correlation data, payload format and message
// expiry on PUBLISH; user properties and a subscription identifier on SUBSCRIBE
// — and surfaces the ones a server sends: the assigned client id, reason codes,
// the server's Receive Maximum and Maximum Packet Size and its capability flags
// (see ServerInfo). Every packet the client composes is measured against the
// server's Maximum Packet Size (ErrPacketTooLarge) and the outbound window is
// capped by the server's Receive Maximum. Outbound topic aliases are not used;
// inbound aliases sent by a server are resolved before delivery.
//
// # Errors
//
// ErrClosed, ErrNotConnected and ErrDisconnected report where a call failed, and
// ErrPacketTooLarge is the answer to a payload the server's Maximum Packet Size
// forbids (the packet is refused, never silently dropped). A negative MQTT 5
// acknowledgement (CONNACK, PUBACK, PUBREC, UNSUBACK) is a *ReasonError carrying
// the reason code the server sent.
package client
