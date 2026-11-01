// A separate module on purpose: gcache and its dependencies must stay out of the
// gmqx module, so a deployment that does not use this session store never builds
// them. Nothing in github.com/maxid/gmqx imports this package.
module github.com/maxid/gmqx/contrib/sessionstore-gcache

go 1.27

require (
	github.com/alicebob/miniredis/v2 v2.40.0
	github.com/maxid/gcache v0.0.0-20261101071036-f52cfa5736a3
	github.com/maxid/gmqx v0.0.0
	github.com/redis/go-redis/v9 v9.23.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/maxid/gnety v0.0.0-20261101055410-7fc85d35bd1c // indirect
	github.com/pion/dtls/v2 v2.2.12 // indirect
	github.com/pion/logging v0.2.2 // indirect
	github.com/pion/transport/v2 v2.2.4 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)

// The libraries are the source trees this module lives in, so it always tests
// against them, never against a published version.
replace github.com/maxid/gmqx => ../..
