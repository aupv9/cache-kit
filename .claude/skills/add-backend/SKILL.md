---
name: add-backend
description: Add a new cache backend (e.g. valkey-go, in-memory, memcached) behind the cachekit.Cache interface. Use when asked to support a new client library or storage engine.
---

# Add a cache backend to cache-kit

Follow these steps in order. The contract lives in `cache.go` (`Cache`
interface + `ErrCacheMiss`); read it and `redis.go` (the reference
implementation) first.

## 1. Create the implementation file

One new file at the repo root, named after the backend (`valkey.go`,
`memory.go`, …), package `cachekit`. It must contain:

- A struct holding the client, `prefix`, `defaultTTL`, `codec`, and
  `ownsClient` if it can create its own client.
- `var _ Cache = (*YourType)(nil)` compile-time check.
- `NewYourBackend(opts ...Option)` and, when wrapping an injected client,
  `NewYourBackendWithClient(client, opts ...Option)` (Close is a no-op for
  injected clients).
- `Get`/`Set`/`Delete` honoring the invariants in CLAUDE.md:
  - absent key → `fmt.Errorf("%w: %s", ErrCacheMiss, key)` — never the
    client's own sentinel error, never `nil, nil`;
  - `ttl <= 0` → `defaultTTL` → no expiry;
  - prefix applied internally via a `key(k string)` helper;
  - errors wrapped as `cachekit: <op> %q: %w` with the logical key.

## 2. Backend-specific notes

- **valkey-go** (`github.com/valkey-io/valkey-go`): prefer
  `client.DoCache(...)` with a client-side cache TTL for reads — it gives
  server-assisted invalidation and built-in single-flight, so a
  valkey-go-specific GetOrSet may bypass `group.go`. Map
  `valkey.IsValkeyNil(err)` to `ErrCacheMiss`.
- **In-memory**: guard the map with a mutex, honor TTL via stored deadlines
  checked on Get (no background reaper needed for a first version).

## 3. Tests

Extend the existing suite rather than inventing a new one: refactor tests in
`cache_test.go` to run against any `Cache` via a constructor table if needed.
Minimum coverage for the new backend: roundtrip, miss → `IsMiss`, idempotent
delete, prefix isolation, default TTL, GetOrSet dedupe. Never require a real
server in tests (miniredis for RESP backends; valkey-go has its own mock in
`mock/`).

## 4. Verify and document

```bash
go vet ./... && go test -race ./...
```

Then add the file to the architecture table in CLAUDE.md and note any
invariant the backend handles differently (e.g. built-in single-flight).
Run the `cache-reviewer` agent on the diff before pushing.
