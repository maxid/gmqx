// Package namespace holds the key-space rules of one broker namespace.
//
// A namespace isolates sessions, retained messages and cluster traffic: the same client
// id in two namespaces is two clients, and a key built for one namespace can never collide
// with a key of another. The rules are a package of their own because the session store,
// the retained store and the cluster backends all build keys and must agree on them.
package namespace

import "strings"

// Key qualifies a per-namespace name with its namespace. The default namespace keeps the
// bare name, so a deployment that uses no namespaces stores, advertises and routes exactly
// the keys it did before the scope existed — including the Redis key layout operators read.
//
// A non-empty namespace is followed by a NUL separator, which no MQTT UTF-8 encoded string
// may contain, so a key can never collide with a name of another namespace.
func Key(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "\x00" + name
}

// InNamespace reports whether a session key belongs to namespace. A shared store can be
// shared by brokers of different namespaces (a store is not owned by a namespace), so the
// broker only routes to, and only expires, the keys of its own scope.
func InNamespace(namespace, key string) bool {
	if namespace == "" {
		return !strings.Contains(key, "\x00")
	}
	return strings.HasPrefix(key, namespace+"\x00")
}
