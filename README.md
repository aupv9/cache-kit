# cache-kit

Small Go caching toolkit for Redis/Valkey: a byte-oriented `Cache`
interface, a go-redis v9 backend (RESP — works with both Redis and Valkey),
pluggable codecs (JSON by default), and a generic cache-aside helper with
single-flight deduplication.

## Install

```bash
go get github.com/aupv9/cache-kit
```

## Usage

```go
package main

import (
	"context"
	"time"

	cachekit "github.com/aupv9/cache-kit"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func main() {
	cache := cachekit.New(
		cachekit.WithAddr("localhost:6379"),
		cachekit.WithPrefix("users-svc:"),
		cachekit.WithDefaultTTL(5*time.Minute),
	)
	defer cache.Close()

	ctx := context.Background()

	// Cache-aside: one loader call even under concurrent misses.
	u, err := cachekit.GetOrSet(ctx, cache, "user:42", time.Minute,
		func(ctx context.Context) (User, error) {
			return loadUserFromDB(ctx, 42) // hits the DB only on miss
		})
	_ = u
	_ = err

	// Low-level API when you need it.
	if _, err := cache.Get(ctx, "user:42"); cachekit.IsMiss(err) {
		// absent — distinct from connection errors
	}
	_ = cache.Delete(ctx, "user:42")
}
```

Semantics worth knowing:

- A miss is `ErrCacheMiss` (check with `cachekit.IsMiss`), never `nil, nil`;
  connection errors are separate so you know when to fall back to the DB.
- `GetOrSet` degrades gracefully: if the cache is down it still calls your
  loader and returns the result.
- `Set` with `ttl <= 0` uses the configured `DefaultTTL` (or no expiry).

## Testing

Tests run against [miniredis](https://github.com/alicebob/miniredis) — no
real server required:

```bash
go test -race ./...
```

## Backends

Everything depends on the `cachekit.Cache` interface; pick the backend that
fits and the rest of your code doesn't change:

- **`cachekit.New`** — go-redis v9. The safe default: plain RESP, works
  with both Redis and Valkey, biggest ecosystem.
- **`cachekit.NewValkey`** — [valkey-go](https://github.com/valkey-io/valkey-go):
  auto-pipelining, and opt-in server-assisted client-side caching. Pass
  `cachekit.WithClientSideCacheTTL(time.Minute)` and hot reads are served
  from a local LRU that the server invalidates on writes (requires a server
  with `CLIENT TRACKING`, i.e. Redis 6+/Valkey):

  ```go
  cache, err := cachekit.NewValkey(
      cachekit.WithAddr("localhost:6379"),
      cachekit.WithPrefix("users-svc:"),
      cachekit.WithClientSideCacheTTL(time.Minute),
  )
  ```

- **`cachekit.NewMemory`** — process-local map for tests, local dev, or as
  an L1. TTL honored via lazy expiry; no eviction, so don't feed it
  unbounded key sets.

All backends pass the same conformance suite (`conformance_test.go`). To
add another one, see `.claude/skills/add-backend/`.
