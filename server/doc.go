// Package server is the broker engine of gmqx: one process's listeners, the
// broker lifecycle, the clientId table, the session store usage and the retained
// store usage — the assembly layer the role packages hang on.
//
// The roles live in their own packages: gmqx/conn is one physical connection (the
// CONNECT/AUTH handshake, the read loop, the write-behind queue, the outbound
// QoS 1/2 window and the will it leaves behind); gmqx/routing is the routing table
// and the shared-subscription node election; gmqx/session is the session state and
// the SessionStore seam; gmqx/cluster is the gossip / TCP mesh / Redis backends;
// gmqx/hook is the extension contract and gmqx/retained the retained store.
//
// This package implements conn.Host, owns the broker lifecycle (New, Start, Stop,
// Close), the clientId registration and takeover rules, the session bookkeeping
// (store take/save/sweep, the offline queue and its caps), the pending-will
// scheduler, the plugin host and the public programming surface (Config,
// Dispatcher, LocalTarget, the hook aliases).
//
// The root package gmqx is the facade: it re-exports this package's public types as
// aliases (rule L3), so a consumer imports gmqx and holds server types.
package server
