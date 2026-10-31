// A separate module on purpose: the interoperability matrix needs a
// third-party MQTT client (paho) and a container runtime, and the library module
// itself must keep zero third-party dependencies. Nothing in this directory is
// imported by github.com/maxid/gmqx.
module github.com/maxid/gmqx/interop

go 1.27

require (
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/maxid/gmqx v0.0.0
	github.com/maxid/gnety v0.0.0-20261031140716-cf189e47d917
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/pion/dtls/v2 v2.2.12 // indirect
	github.com/pion/logging v0.2.2 // indirect
	github.com/pion/transport/v2 v2.2.4 // indirect
	github.com/redis/go-redis/v9 v9.23.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)

// The library is the module one level up (libs/gmqx), so the replacement is
// "..": the matrix always runs against the source tree it lives in, never
// against a published version.
replace github.com/maxid/gmqx => ..
