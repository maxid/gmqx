// Package gcachestore is a ready-made gmqx.SessionStore backed by
// github.com/maxid/gcache.
//
// It lives in its own module (contrib/sessionstore-gcache) on purpose: the gmqx
// module does not depend on gcache, and gcache's dependencies do not enter the
// broker's go.mod. No file of github.com/maxid/gmqx imports this package.
//
// # Ports it uses
//
//   - gcache.RemoteStore — Get/Set/Delete with a per-key TTL. Save writes the
//     encoded SessionState under the session key with the session's own deadline
//     as the key's TTL, and a state whose deadline has already passed is deleted
//     instead of written.
//   - gcache.Scanner — Keys. The port is optional in gcache, so the constructor
//     refuses a store that does not implement it: gmqx calls Keys when a broker
//     starts (to index the sessions a deployment restored into the store) and from
//     the expiry sweeper. Without enumeration those two cannot work, and the
//     failure would show up later, on a reconnect or a sweep, instead of at
//     startup.
//   - gcache.Locker — the atomic Take. The adapter acquires a lock on the session
//     key, reads the state under it and deletes the key before releasing the lock,
//     so two nodes reconnecting the same client id cannot both resume the session.
//
// # Why the two-level cache is not used
//
// The adapter deliberately reads the RemoteStore directly and never puts a
// gcache.Cache in front of it. A two-level Cache has a local tier that can return
// an older copy than the one another node just wrote, evicts under memory
// pressure, clamps a TTL to its configured maximum, can serve an expired entry
// after a store failure (stale-if-error) and caches absences — each of those is
// data loss by design, which is acceptable for cached data and fatal for a
// session. The RemoteStore is the part of gcache that behaves like a store: a
// value that was written is there until it is overwritten, deleted or expires.
//
// # What it does not do
//
// The store is a protocol-neutral SessionStore, not a durability guarantee. Redis
// must be configured for it (append-only persistence, noeviction, replication for
// availability); see the module's README and gmqx's docs/PERSISTENCE.md.
package gcachestore
