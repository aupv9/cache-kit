// Package cachekit provides a small caching toolkit for services that use
// Redis or Valkey as a shared cache. It exposes a byte-oriented Cache
// interface, a pluggable codec layer, and a generic cache-aside helper
// (GetOrSet) with single-flight deduplication.
package cachekit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrCacheMiss is returned by Cache.Get when the key does not exist.
// It is deliberately distinct from connection/infrastructure errors so
// callers can decide when to fall back to the source of truth.
var ErrCacheMiss = errors.New("cachekit: cache miss")

// Cache is the minimal contract every backend must satisfy.
// Implementations must return ErrCacheMiss (possibly wrapped) from Get
// when the key is absent, and reserve other errors for real failures.
type Cache interface {
	// Get returns the raw value stored at key, or ErrCacheMiss.
	Get(ctx context.Context, key string) ([]byte, error)
	// Set stores val at key. ttl <= 0 means "use the backend's default
	// TTL"; if the backend has no default, the key does not expire.
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	// Delete removes key. Deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
}

// IsMiss reports whether err represents a cache miss.
func IsMiss(err error) bool {
	return errors.Is(err, ErrCacheMiss)
}

// GetOrSet implements the cache-aside pattern: it returns the cached value
// for key, or invokes loader, stores the result, and returns it.
//
// Concurrent callers that miss on the same key share a single loader call
// (see group.go). Waiters honor their own context: a canceled waiter
// returns ctx.Err() immediately without aborting the shared flight. The
// loader itself runs detached from the initiating caller's cancellation
// (values and any deadline are kept) so one canceled request cannot fail
// every waiter or waste a completed load.
//
// Infrastructure errors from Get are treated as a miss so the service
// keeps working when the cache is down; in that degraded mode the loader
// result is still returned and Set is attempted best-effort. A cached
// entry that fails to decode (corrupted, or written by an incompatible
// schema version) is also treated as a miss: the entry is deleted and the
// loader repopulates it, instead of the key erroring until TTL expiry.
//
// If the loader returns an error wrapping ErrNotFound and the cache is
// configured with WithNegativeTTL, the absence itself is cached: further
// calls return ErrNotFound without invoking the loader until the negative
// entry expires.
//
// With WithStaleTTL, an entry past its freshness TTL is still served —
// immediately, with no loader latency — while a deduplicated background
// refresh replaces it (stale-while-revalidate). With WithEarlyRefresh,
// entries approaching their freshness deadline are probabilistically
// refreshed in the background before they expire, spreading reloads
// across time and across instances (XFetch). Both features store entries
// in a small metadata envelope; entries written without one (older
// deploys, raw Set calls) are treated as fresh until their backend TTL
// expires. Values are encoded with codec (nil means the package default,
// JSON).
func GetOrSet[T any](ctx context.Context, c Cache, key string, ttl time.Duration, loader func(context.Context) (T, error)) (T, error) {
	return GetOrSetWithCodec(ctx, c, key, ttl, defaultCodec, loader)
}

// GetOrSetWithCodec is GetOrSet with an explicit Codec.
func GetOrSetWithCodec[T any](ctx context.Context, c Cache, key string, ttl time.Duration, codec Codec, loader func(context.Context) (T, error)) (T, error) {
	var zero T
	if codec == nil {
		codec = defaultCodec
	}
	cfg := configOf(c)
	hooks := cfg.hooks

	if data, err := c.Get(ctx, key); err == nil {
		if isNegativeEntry(data) {
			return zero, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		env, payload := parseEnvelope(data)
		var v T
		uerr := codec.Unmarshal(payload, &v)
		if uerr == nil {
			now := time.Now()
			switch {
			case env.stale(now):
				// Stale-while-revalidate: serve the stale value with no
				// loader latency on the request path; a deduplicated
				// background refresh replaces the entry.
				refreshAsync(ctx, c, key, ttl, codec, loader, cfg)
			case env.shouldEarlyRefresh(now, cfg.earlyRefreshBeta):
				// Probabilistic early refresh near the freshness
				// deadline: spreads reloads across time and instances.
				refreshAsync(ctx, c, key, ttl, codec, loader, cfg)
			}
			return v, nil
		}
		// Self-heal: drop the undecodable entry and reload, rather than
		// returning an error for this key until its TTL expires. The
		// Delete can race a concurrent valid write and remove it — the
		// cost is one wasted load absorbed by the single-flight group,
		// which is not worth an atomic compare-and-delete.
		hooks.error(OpDecode, key, uerr)
		_ = c.Delete(ctx, key)
	}

	// Miss (or degraded cache): load through the single-flight group so
	// only one goroutine per (cache, key) hits the source of truth.
	// preserveDeadline: the flight is detached from the initiator's
	// cancellation but keeps its deadline.
	data, err := loadGroup.do(ctx, flightKey(c, key),
		loadAndStore(ctx, true, c, key, ttl, codec, loader, cfg))
	if err != nil {
		return zero, err
	}
	if isNegativeEntry(data) {
		return zero, fmt.Errorf("%w: %s", ErrNotFound, key)
	}

	_, payload := parseEnvelope(data)
	var v T
	if uerr := codec.Unmarshal(payload, &v); uerr != nil {
		// The flight validated or produced these bytes, so this is either
		// a codec Marshal/Unmarshal asymmetry or a waiter that joined the
		// flight with a different T/codec than the initiator's — not
		// cache corruption.
		return zero, fmt.Errorf("cachekit: decode %q: %w", key, uerr)
	}
	return v, nil
}

// loadAndStore builds the single-flight function shared by the miss path
// and background refreshes: re-check the cache, run the loader, store the
// result (enveloped when stale-while-revalidate / early refresh is on),
// and return the stored bytes.
//
// The returned function derives its own context from base when it runs:
// detached from cancellation, keeping values, and — only when
// preserveDeadline is set (the synchronous miss path) — the base deadline.
// Background refreshes pass false so they aren't cut short by the request
// that happened to trigger them.
func loadAndStore[T any](base context.Context, preserveDeadline bool, c Cache, key string, ttl time.Duration, codec Codec, loader func(context.Context) (T, error), cfg cacheConfig) func() ([]byte, error) {
	return func() ([]byte, error) {
		fctx := context.WithoutCancel(base)
		if preserveDeadline {
			if deadline, ok := base.Deadline(); ok {
				var cancel context.CancelFunc
				fctx, cancel = context.WithDeadline(fctx, deadline)
				defer cancel()
			}
		}

		// Another flight may have populated the key while we waited. A
		// stale entry does not satisfy the flight — refreshing it is
		// exactly this flight's job.
		if data, err := c.Get(fctx, key); err == nil {
			if isNegativeEntry(data) {
				return data, nil // resolved after the flight
			}
			env, payload := parseEnvelope(data)
			if !env.stale(time.Now()) {
				var probe T
				uerr := codec.Unmarshal(payload, &probe)
				if uerr == nil {
					return data, nil
				}
				// Concurrently written undecodable entry: same self-heal.
				cfg.hooks.error(OpDecode, key, uerr)
				_ = c.Delete(fctx, key)
			}
		}

		start := time.Now()
		v, err := loader(fctx)
		loadDur := time.Since(start)
		cfg.hooks.load(key, loadDur, err)
		if err != nil {
			// Negative caching: "does not exist" is a valid, cacheable
			// answer — store it so hot lookups of nonexistent entities
			// stop reaching the database. Other loader errors propagate
			// and are never cached.
			if cfg.negativeTTL > 0 && errors.Is(err, ErrNotFound) {
				_ = c.Set(fctx, key, negativeMarker, cfg.negativeTTL)
				return negativeMarker, nil
			}
			return nil, err
		}
		payload, merr := codec.Marshal(v)
		if merr != nil {
			return nil, fmt.Errorf("cachekit: encode %q: %w", key, merr)
		}
		data, storeTTL := packForStore(payload, ttl, loadDur, cfg)
		// Best effort: a failed Set must not fail the request. The
		// backend reports the failure through Hooks.OnError.
		_ = c.Set(fctx, key, data, storeTTL)
		return data, nil
	}
}

// refreshAsync starts (or joins) a background refresh flight for key.
// It never blocks: concurrent triggers share one loader call through the
// same single-flight group as the miss path, and its failures leave the
// existing (stale) entry in place until hard expiry.
func refreshAsync[T any](ctx context.Context, c Cache, key string, ttl time.Duration, codec Codec, loader func(context.Context) (T, error), cfg cacheConfig) {
	loadGroup.doAsync(flightKey(c, key),
		loadAndStore(ctx, false, c, key, ttl, codec, loader, cfg))
}

// packForStore decides what bytes and TTL actually hit the backend. With
// stale-while-revalidate or early refresh enabled, the payload is wrapped
// in the metadata envelope and the backend TTL is extended to freshness +
// stale window; otherwise both pass through untouched.
func packForStore(payload []byte, ttl, loadDur time.Duration, cfg cacheConfig) ([]byte, time.Duration) {
	if !cfg.envelopeEnabled() {
		return payload, ttl
	}
	fresh := ttl
	if fresh <= 0 {
		fresh = cfg.defaultTTL
	}
	var freshUntil time.Time
	var total time.Duration
	if fresh > 0 {
		freshUntil = time.Now().Add(fresh)
		total = fresh + cfg.staleTTL
	}
	return wrapEnvelope(payload, freshUntil, loadDur), total
}

// flightKey namespaces single-flight keys per cache instance so two caches
// with different prefixes never share a flight for the same logical key.
func flightKey(c Cache, key string) string {
	return fmt.Sprintf("%p\x00%s", c, key)
}
