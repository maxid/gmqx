package gmqx

// Namespace isolation
//
// A Broker serves exactly one namespace (Config.Namespace). The reference broker
// in this family exposes namespaces as a first-class isolation scope; in gmqx
// the scope is a partition key, because a process may run several brokers, each
// with its own namespace:
//
//   - the session and client tables are keyed by (namespace, client id), so a
//     client id never collides across scopes;
//   - a connection inherits its broker's namespace, and local matching only
//     considers sessions of the publication's namespace;
//   - the retained store is keyed by (namespace, topic), and a SUBSCRIBE only
//     replays the retained state of its own namespace;
//   - the subscription summary a cluster peer advertises carries the namespace,
//     a received summary for another namespace is ignored, and ClusterMessage
//     events carry the namespace their publication belongs to: a remote event
//     for another namespace is dropped before it can reach a session or the
//     retained store.
//
// The empty string is the default namespace, and it is the zero value of
// Config.Namespace. namespaceKey keeps the bare name for it, so a deployment
// that does not use namespaces stores, advertises and routes exactly the keys it
// did before this scope existed — including the Redis key layout operators read.

// namespaceKey qualifies a per-namespace name with its namespace. The default
// namespace keeps the bare name. A non-empty namespace is followed by a NUL
// separator, which no MQTT UTF-8 encoded string may contain, so a key cannot
// collide with a name of another namespace.
func namespaceKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "\x00" + name
}
