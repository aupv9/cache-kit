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
Do not add tests that dial a real server — except `integration_test.go`
(build tag `integration`), which exercises the client-side-caching paths
miniredis cannot (`DoCache`/`DoMultiCache`). Run those with:

```bash
CACHEKIT_SERVER_ADDR=localhost:6379 go test -race -tags integration -run Integration ./...
```

CI runs them against a valkey service container.

## Architecture

| File         | Role |
|--------------|------|
| `cache.go`   | `Cache` interface, `ErrCacheMiss`, generic `GetOrSet` (cache-aside) |
| `batch.go`   | `BatchCache` optional interface, generic `GetOrSetMany` |
| `negative.go`| `ErrNotFound` + negative-entry marker (cache penetration defense) |
| `redis.go`   | `RedisCache` — go-redis v9 backend, works with both Redis and Valkey (RESP) |
| `valkey.go`  | `ValkeyCache` — valkey-go backend: auto-pipelining, optional server-assisted client-side caching (`WithClientSideCacheTTL`) |
| `memory.go`  | `MemoryCache` — process-local map, TTL via lazy expiry; tests/dev/L1 only |
| `envelope.go`| metadata envelope (freshness + loader delta) for SWR/early refresh, XFetch |
| `codec.go`   | `Codec` interface, `JSONCodec` default |
| `options.go` | `Options` + functional `With*` options |
| `hooks.go`   | `Hooks` (OnHit/OnMiss/OnError/OnLoad) — the observability surface |
| `group.go`   | single-flight dedup for concurrent loads of the same key |

Dependency direction: everything depends on the `Cache` interface in
`cache.go`; only `redis.go` and `valkey.go` import client libraries.
Callers should hold a `cachekit.Cache`, not a concrete backend type.

The behavioral contract is executable: `conformance_test.go` runs the same
suite against every backend via the `backends()` table — a new backend must
be added there.

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
- **Swallowed errors are observable**: every backend failure — including the
  best-effort `Set` that `GetOrSet` swallows — must fire `Hooks.OnError`.
  Any new degradation path needs a hook call, or outages become invisible.
- **Decode failures self-heal**: a cached entry that fails to decode
  (corruption, incompatible schema after a deploy) is treated as a miss —
  `GetOrSet` deletes it and reloads. A key must never error until TTL expiry.
- **Single-flight**: concurrent `GetOrSet` misses on the same (cache, key)
  share one loader call. Flight keys are namespaced per cache instance.
  Waiters honor their own context (a canceled waiter returns `ctx.Err()`
  without aborting the flight); the loader runs on a context detached from
  the initiator's cancellation but keeps its deadline and values.
- **Prefixing** happens inside the backend (`RedisCache.key`); public API
  keys are always logical (unprefixed).
- **Negative caching is opt-in and marker-based**: with `WithNegativeTTL`,
  a loader error wrapping `ErrNotFound` (or a key omitted by a batch
  loader) stores `negativeMarker`; the GetOrSet helpers translate it back
  to `ErrNotFound`. The marker is checked *before* any decode so it never
  triggers the decode self-heal. `ErrNotFound` ≠ `ErrCacheMiss` — never
  conflate them.
- **TTL resolution order** in every backend: explicit ttl → `DefaultTTL` →
  jitter (`WithTTLJitter`) → backend clamps (valkey PX ≥ 1ms). Batch and
  single-key writes must share this path (`ttlFor`/`newItem`).
- **OpTimeout bounds cache ops only** (`opContext` in each backend op) —
  never the GetOrSet loader.
- **Batch loads are not single-flighted** (documented in `GetOrSetMany`);
  batch reads degrade to "all missing" on infra failure, batch writes are
  best-effort.
- **Envelope order**: negative marker check → envelope parse → decode, on
  every read path. The envelope is written only when `WithStaleTTL` or
  `WithEarlyRefresh` is set, and *parsed* only then too
  (`cacheConfig.parseStored`) — with the features off, values are opaque
  bytes even if they start with the magic. Bare bytes always parse as
  "no metadata, fresh until backend TTL" (back-compat); disabling the
  features later makes old enveloped entries self-heal once.
- **Jitter lands on the freshness window, never the total**: enveloped
  writes are jittered inside `packForStore` and backends skip jitter for
  them (`hasEnvelope`), so `freshUntil + staleTTL == backend expiry`
  holds exactly. Re-jittering the total could cut it below `freshUntil`
  and silently delete the stale window.
- **Stale serves never block**: a stale hit returns immediately and
  triggers `refreshAsync` (deduped through the same single-flight group
  as misses; failures keep the stale entry). Background refreshes drop
  the initiator's deadline but are bounded by freshness + stale window
  (hung loaders must not pin goroutines forever), and their failures —
  including panics — fire `OnError(OpRefresh)`, their only visibility.
  A refresh flight's re-check only accepts an entry *fresher than the
  one that triggered it* (`flightOpts.refreshOf`), otherwise an early
  refresh would short-circuit on its own fresh entry and never reload.
  Synchronous flights keep the initiator's deadline. `GetOrSetMany`
  serves stale without refreshing (hard TTL bounds staleness).

## Adding a new backend (e.g. valkey-go, in-memory)

Use the `add-backend` skill (`.claude/skills/add-backend/`). In short: one
new file implementing `Cache`, a `var _ Cache = (*T)(nil)` compile-time
check, miss mapped to `ErrCacheMiss`, and an entry in `backends()` in
`conformance_test.go` so the shared suite covers it.

valkey-go notes: its built-in single-flight dedupes concurrent GETs to the
*server*; `group.go` still applies on top because it dedupes *loader* (DB)
calls, which no client library can. In tests, create the client with
`DisableCache: true` — miniredis has no `CLIENT TRACKING` support, so the
client-side-caching path (`WithClientSideCacheTTL` → `DoCache`) only works
against a real server and stays untested by design.

## Conventions

- Error messages: prefix `cachekit:`, include the logical key, wrap the cause
  with `%w`.
- Go ≥ 1.25, generics used freely; no reflection outside codecs.
- New knobs go through functional options in `options.go`, never new
  constructor parameters.
