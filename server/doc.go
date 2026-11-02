// Package server is the broker engine of gmqx: one process's listeners, the
// connection and session state machine, the routing index, the session store
// usage and the retained store usage.
//
// It is one package on purpose. The pieces are a single state machine rather than
// separable roles: the outbound QoS bookkeeping (inflight.go), the wills (will.go)
// and the authentication exchange (auth.go) are methods of the unexported client
// type, the routing index holds *client, and the persistent-session read/write
// path is methods of Broker. Splitting them into role packages means changing type
// ownership across the whole engine, not moving files; the staged plan for that is
// recorded in docs/design/source-layout.md §4.3.
//
// The root package gmqx is the facade: it re-exports this package's public types as
// aliases (rule L3), so a consumer imports gmqx and holds server types.
package server
