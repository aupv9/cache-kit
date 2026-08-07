module cachekit-testapp

go 1.25.0

require (
	github.com/aupv9/cache-kit v0.0.0
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/valkey-io/valkey-go v1.0.76 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
)

// Build against the library in this repo (context is the repo root).
replace github.com/aupv9/cache-kit => ../../..
