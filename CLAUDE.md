# cache-kit

Go caching toolkit for services using Redis or Valkey as a shared cache.
Single package `cachekit`, module `github.com/aupv9/cache-kit`.

## Commands

```bash
go build ./...          # compile
go test ./...           # unit tests (miniredis in-memory, no real server needed)
go test -race ./...     # always run before pushing — the package is concurrency-heavy
go vet ./...
```

Tests never require a running Redis/Valkey: they use `miniredis.RunT(t)`.
Do not add tests that dial a real server.

## Architecture

| File         | Role |
|--------------|------|
| `cache.go`   | `Cache` interface, `ErrCacheMiss`, generic `GetOrSet` (cache-aside) |
| `redis.go`   | `RedisCache` — go-redis v9 backend, works with both Redis and Valkey (RESP) |
| `codec.go`   | `Codec` interface, `JSONCodec` default |
| `options.go` | `Options` + functional `With*` options |
| `group.go`   | single-flight dedup for concurrent loads of the same key |

Dependency direction: everything depends on the `Cache` interface in
`cache.go`; only `redis.go` imports a client library. Callers should hold a
`cachekit.Cache`, not `*RedisCache`.

## Invariants — do not break

- **Miss vs failure**: `Get` on an absent key returns an error wrapping
  `ErrCacheMiss`; connection/protocol errors are returned as-is (wrapped with
  context). Callers rely on `cachekit.IsMiss(err)` to decide DB fallback.
  Never return `nil, nil` for a miss, and never let backend sentinel errors
  (e.g. `redis.Nil`) leak through the interface.
- **TTL semantics**: `Set` with `ttl <= 0` uses the cache's `DefaultTTL`;
  if that is also zero, the key never expires.
- **Cache failures degrade, not fail**: in `GetOrSet`, a broken cache falls
  through to the loader; a failed `Set` after a successful load is swallowed.
  Loader errors always propagate and never poison the cache.
- **Single-flight**: concurrent `GetOrSet` misses on the same (cache, key)
  share one loader call. Flight keys are namespaced per cache instance.
- **Prefixing** happens inside the backend (`RedisCache.key`); public API
  keys are always logical (unprefixed).

## Adding a new backend (e.g. valkey-go, in-memory)

Use the `add-backend` skill (`.claude/skills/add-backend/`). In short: one
new file implementing `Cache`, a `var _ Cache = (*T)(nil)` compile-time
check, miss mapped to `ErrCacheMiss`, and the same test suite run against it.
valkey-go note: it has built-in client-side caching + single-flight, so its
`GetOrSet` path may bypass `group.go`.

## Conventions

- Error messages: prefix `cachekit:`, include the logical key, wrap the cause
  with `%w`.
- Go ≥ 1.25, generics used freely; no reflection outside codecs.
- New knobs go through functional options in `options.go`, never new
  constructor parameters.
