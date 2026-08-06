# Roadmap

Where cache-kit is going, in order. Each milestone is shippable on its
own; nothing below breaks the public API without a major version bump.

Done so far (v0.1, PR #1–#2): `Cache` interface with go-redis / valkey-go /
in-memory backends and a conformance suite; cache-aside `GetOrSet` +
`GetOrSetMany` with per-process single-flight (cancelable, panic-safe);
observability hooks (including swallowed errors and background-refresh
failures); decode self-heal; TTL jitter; negative caching; per-op
timeouts; stale-while-revalidate + XFetch early refresh over an opt-in
metadata envelope; CI with race tests, govulncheck, and integration tests
against a real Valkey (client-side caching paths).

## v0.2 — Release discipline & polish

Goal: people outside this repo can adopt the library and trust upgrades.

- [ ] Tag `v0.1.0` once PR #2 merges; adopt semver + a CHANGELOG.
      Document the compatibility promise (what `v0.x` may break).
- [ ] `golangci-lint` in CI (errcheck, staticcheck, revive) + dependabot
      for module updates.
- [ ] Benchmarks (`bench_test.go`): GetOrSet hit/miss paths per backend,
      batch vs loop, envelope overhead, codec comparison. Run in CI
      (informational, not gating).
- [ ] Cluster / Sentinel / TLS first-class options for `New` and
      `NewValkey` (`WithTLS`, `WithInitAddresses`, `WithSentinel`) —
      today's escape hatch is `NewWithClient`/`NewValkeyWithClient`.
- [ ] `codec/msgpack` sub-module (keeps the root module dependency-free)
      + codec versioning guidance: how to roll a schema change safely
      given decode self-heal.
- [ ] Example wiring for Prometheus and OpenTelemetry on top of `Hooks`
      (examples or a tiny `contrib/` sub-module — hooks stay the only
      contract).

## v0.3 — Tiered caching (L1 + L2)

Goal: microsecond hot reads without giving up shared-cache coherence.
This is the biggest remaining feature gap.

- [ ] Bounded `MemoryCache`: max-entries with LRU eviction (today it
      documents "TTL-only, don't feed it unbounded key sets"). Eviction
      metrics via hooks.
- [ ] `Tiered(l1, l2 Cache)` composing two caches behind the same
      interface: read-through (L1 → L2 → loader), write-through, L1 TTL
      capped by L2's remaining TTL so tiers can't disagree longer than
      the L1 TTL.
- [ ] Cross-instance L1 invalidation, two options documented and tested:
      valkey-go client-side caching as the L1 (server-pushed, already
      supported) vs pub/sub invalidation channel for the go-redis
      backend (`Delete` publishes; subscribers evict).
- [ ] Invalidation helpers: `DeleteByPrefix` (SCAN-based, rate-limited)
      and versioned-namespace invalidation (bump a prefix epoch key —
      O(1) invalidation for a whole namespace).

## v0.4 — Distributed loading semantics

Goal: strictly-once loading per cluster where XFetch's probabilistic
spreading isn't enough (very expensive loaders, strict quotas).

- [ ] Optional distributed lock for `GetOrSet` (`WithDistributedLock`):
      `SET NX PX` lease around the loader, waiters poll the cache with
      backoff instead of hammering the DB; lock loss mid-load documented
      (last-writer-wins, bounded by lease). Fencing token in the
      envelope if we find a real need.
- [ ] Single-flight for batch loads: per-key flights with partial joins
      so overlapping `GetOrSetMany` calls share loader work (documented
      gap since P1).
- [ ] Background refresh for stale batch entries (today `GetOrSetMany`
      serves stale without refreshing; hard TTL bounds staleness).

## v1.0 — API freeze

Goal: commit to the interface.

- [ ] API review pass: anything exported by accident, naming
      consistency, options audit. Freeze `Cache`, `BatchCache`, `Hooks`,
      option set.
- [ ] Fuzz tests for the envelope parser and codecs (malformed inputs
      must fail closed into the self-heal path, never panic).
- [ ] Fault-injection suite (toxiproxy or equivalent) for the degradation
      matrix: latency, partitions, half-open connections — asserting the
      documented behavior (degrade to loader, hooks fire, no goroutine
      leaks) under real network failure, not just `mr.Close()`.
- [ ] Docs: godoc examples for every option, a "which knob for which
      failure mode" guide (avalanche → jitter, penetration → negative
      TTL, hot-key latency → SWR, cross-instance stampede → XFetch/lock).

## Explicit non-goals

- Reimplementing client-side caching, connection pooling, or cluster
  topology — that's the client libraries' job; we compose them.
- Generic multi-store abstraction over Memcached/DynamoDB/etc. — the
  toolkit targets Redis-protocol caches plus a local L1.
- Cache warming / preloading orchestration — belongs in application
  code; `SetMany` is the primitive it needs.

## Process

Every feature lands with: conformance/behavior tests (real-server
integration tests where miniredis can't cover it), a CLAUDE.md invariant
entry if it adds one, a `cache-reviewer` pass before push, and a
CHANGELOG entry. Concurrency-touching changes need `-race` plus a test
that fails without the fix.
