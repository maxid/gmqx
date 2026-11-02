package hook

// MetricsManager is the metrics SPI.
//
// RecordDroppedMessage observes something the broker refused to deliver or keep.
// The reasons this package records are "acl_publish", "acl_subscribe",
// "acl_timeout" (an AclManager.Check call exceeded Config.AuthTimeout and counts
// as a denial), "auth_timeout" (an AuthManager.Authenticate or AuthExchange call
// exceeded it and counts as a failed authentication), "auth_method" (a CONNECT
// named an MQTT 5 Authentication Method this broker has no exchange for, so the
// connection was refused with 0x8C), "auth_steps" (an AuthExchange asked for more
// than MaxAuthSteps challenges and was cut off), "interceptor", "sink_rejected",
// "sink_retryable", "max_retain", "retained_expired" (a retained message was not
// replayed because its Message Expiry Interval elapsed, and it was deleted),
// "max_connections", "max_subscriptions", "connection_rate_limit",
// "takeover_rate_limit" (a CONNECT inside the Config.NotKickSeconds window would
// have displaced a live connection), "shared_subscription" (a malformed
// "$share/..." filter, or one that sets No Local, was refused), "shared_no_member"
// (a shared publication reached a node with no live member of the group, so it was
// dropped instead of being handed to another node's member),
// "subscription_identifier" (a SUBSCRIBE carried an out-of-range Subscription
// Identifier, which is a Protocol Error), "subscription_identifier_limit" (a
// delivery carried more Subscription Identifiers than the documented cap, or a
// subscription stored more), "cluster_namespace", "inflight_overflow_clean" (a
// QoS 1/2 delivery found the inflight window and the queue behind it of a *clean*
// session both full, and a clean session has no offline queue to keep it in, so it
// was dropped — a persistent session is taken offline instead and keeps the
// delivery, see RecordOverflow), "receive_maximum_full" (an inbound QoS 1/2 PUBLISH over the broker's own
// receive maximum: it was neither delivered nor acknowledged),
// "retry_exhausted" (an outbound exchange used up its retransmissions),
// "outbound_queue_full" (a QoS 0 PUBLISH was dropped because the connection's
// writer queue was full), "slow_consumer" (a packet that must not be dropped — a
// QoS 1/2 PUBLISH, an acknowledgement — hit a full writer queue, so the
// connection is disconnected), "offline_queue_full" (the per-session cap dropped
// the oldest message), "offline_queue_total" (the global cap rejected the newest
// one), "offline_queue_corrupt", "offline_session_missing", "session_expired",
// "session_store_corrupt" (a stored session in a SessionStore did not decode, so
// it was dropped), "session_store_error" (a SessionStore command failed, so the
// operation degraded to "no session"), "cluster_kick_stale" (a peer's kick is not
// newer than the connection it would close, so it was refused and the connection
// kept), "cluster_kick_rate_limit" (a peer's kick arrived inside the
// Config.NotKickSeconds window of the connection it would close, so it was refused
// and the connection kept), "cluster_kick_legacy" (a peer's kick carried no epoch
// because its sender predates kick epochs, so it was applied with the pre-epoch
// behavior; it is counted although it is not dropped, so an operator can see a
// mixed-version cluster)
// and "target_unknown" (a directed publish for a client id nobody holds: no
// cluster backend is configured, the Redis presence view has no record for it, or
// a TCP peer received the broadcast without holding the id — a directed publish is
// fire-and-forget, so this is counted instead of returned as an error).
// RecordQueueDepth("offline", n) reports the messages currently queued for
// offline sessions across the broker; RecordOverflow reports a limit that was hit
// without losing a message ("connections", "inflight" — a clean session's delivery
// was dropped as well, see "inflight_overflow_clean" —, "inflight_overflow" — a
// persistent session's inflight queue was full, so the session was taken offline
// and what it still owed was stored with it, nothing was dropped —,
// "receive_maximum", "outbound_queue", "will_delay" — a Will Delay Interval was
// clamped to Config.MaxWillDelayInterval —, "packet_ids" — a delivery degraded to
// QoS 0 because no packet id was free — and "byte_rate_read"/"byte_rate_write" — a
// byte-rate limit delayed a transfer; the throttled byte count itself is reported
// through the optional ByteRateMetrics interface).
type MetricsManager interface {
	IncrementConnections()
	DecrementConnections()
	IncrementPublishedMessages()
	IncrementSubscribeEvents()
	DecrementSubscribeEvents()
	IncrementDisconnectEvents()
	RecordDroppedMessage(reason string)
	RecordOverflow(sinkName string)
	RecordQueueDepth(queueName string, depth int)
	RecordConnections(count int)
}

// NoopMetrics implements MetricsManager by discarding everything.
//
// It is the base a consumer embeds to implement only the counters it cares about: embed
// NoopMetrics, override the methods that matter, and the interface is satisfied without
// nine empty bodies.
type NoopMetrics struct{}

func (NoopMetrics) IncrementConnections()        {}
func (NoopMetrics) DecrementConnections()        {}
func (NoopMetrics) IncrementPublishedMessages()  {}
func (NoopMetrics) IncrementSubscribeEvents()    {}
func (NoopMetrics) DecrementSubscribeEvents()    {}
func (NoopMetrics) IncrementDisconnectEvents()   {}
func (NoopMetrics) RecordDroppedMessage(string)  {}
func (NoopMetrics) RecordOverflow(string)        {}
func (NoopMetrics) RecordQueueDepth(string, int) {}
func (NoopMetrics) RecordConnections(int)        {}
