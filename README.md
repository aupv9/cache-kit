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
- A cached entry that fails to decode (corruption, schema change between
  deploys) is treated as a miss: deleted and reloaded, never a stuck error.
- Canceling your context while waiting on a shared load returns
  immediately; the load itself finishes detached and populates the cache.
- `Set` with `ttl <= 0` uses the configured `DefaultTTL` (or no expiry).

## Production knobs

```go
cache := cachekit.New(
    cachekit.WithAddr("localhost:6379"),
    cachekit.WithDefaultTTL(5*time.Minute),

    // Avalanche: spread TTLs ±10% so co-written keys don't expire together.
    cachekit.WithTTLJitter(0.1),

    // Penetration: cache "does not exist" for 30s. Loaders signal it by
    // returning an error wrapping cachekit.ErrNotFound; GetOrSet then
    // serves ErrNotFound without touching the DB until the entry expires.
    cachekit.WithNegativeTTL(30*time.Second),

    // A slow cache is worse than a down one: bound every cache operation
    // (never the loader) with its own deadline.
    cachekit.WithOpTimeout(50*time.Millisecond),
)
```

## Batch reads

`GetOrSetMany` loads N keys with one cache round trip (MGET / pipelined)
and one loader call for whichever keys are missing:

```go
users, err := cachekit.GetOrSetMany(ctx, cache,
    []string{"user:1", "user:2", "user:3"}, time.Minute,
    func(ctx context.Context, missing []string) (map[string]User, error) {
        return loadUsersFromDB(ctx, missing) // called only for cache misses
    })
```

Keys the loader omits are absent from the result (and negative-cached when
`WithNegativeTTL` is set). Note: batch loads are not single-flighted —
that protection currently applies to per-key `GetOrSet` only.

## Observability

Wire `Hooks` to your metrics — especially `OnError`, which also fires for
errors cachekit swallows by design (the best-effort `Set` after a load).
Without it, a degraded cache is invisible while your DB absorbs the traffic:

```go
cache := cachekit.New(
    cachekit.WithAddr("localhost:6379"),
    cachekit.WithHooks(cachekit.Hooks{
        OnHit:   func(key string) { metrics.Hits.Inc() },
        OnMiss:  func(key string) { metrics.Misses.Inc() },
        OnError: func(op cachekit.Op, key string, err error) {
            metrics.Errors.WithLabelValues(string(op)).Inc()
        },
        OnLoad: func(key string, dur time.Duration, err error) {
            metrics.LoaderDuration.Observe(dur.Seconds())
        },
    }),
)
```

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
