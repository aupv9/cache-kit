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
// Values are encoded with codec (nil means the package default, JSON).
func GetOrSet[T any](ctx context.Context, c Cache, key string, ttl time.Duration, loader func(context.Context) (T, error)) (T, error) {
	return GetOrSetWithCodec(ctx, c, key, ttl, defaultCodec, loader)
}

// GetOrSetWithCodec is GetOrSet with an explicit Codec.
func GetOrSetWithCodec[T any](ctx context.Context, c Cache, key string, ttl time.Duration, codec Codec, loader func(context.Context) (T, error)) (T, error) {
	var zero T
	if codec == nil {
		codec = defaultCodec
	}
	hooks := hooksOf(c)

	if data, err := c.Get(ctx, key); err == nil {
		var v T
		uerr := codec.Unmarshal(data, &v)
		if uerr == nil {
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
	data, err := loadGroup.do(ctx, flightKey(c, key), func() ([]byte, error) {
		// The flight outlives any single waiter, so run it on a context
		// detached from the initiating caller's cancellation. Its values
		// (tracing metadata) and deadline, if any, are preserved.
		fctx := context.WithoutCancel(ctx)
		if deadline, ok := ctx.Deadline(); ok {
			var cancel context.CancelFunc
			fctx, cancel = context.WithDeadline(fctx, deadline)
			defer cancel()
		}

		// Another flight may have populated the key while we waited.
		if data, err := c.Get(fctx, key); err == nil {
			var probe T
			uerr := codec.Unmarshal(data, &probe)
			if uerr == nil {
				return data, nil
			}
			// Concurrently written undecodable entry: same self-heal.
			hooks.error(OpDecode, key, uerr)
			_ = c.Delete(fctx, key)
		}

		start := time.Now()
		v, err := loader(fctx)
		hooks.load(key, time.Since(start), err)
		if err != nil {
			return nil, err
		}
		data, err := codec.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("cachekit: encode %q: %w", key, err)
		}
		// Best effort: a failed Set must not fail the request. The
		// backend reports the failure through Hooks.OnError.
		_ = c.Set(fctx, key, data, ttl)
		return data, nil
	})
	if err != nil {
		return zero, err
	}

	var v T
	if uerr := codec.Unmarshal(data, &v); uerr != nil {
		// The flight validated or produced these bytes, so this is either
		// a codec Marshal/Unmarshal asymmetry or a waiter that joined the
		// flight with a different T/codec than the initiator's — not
		// cache corruption.
		return zero, fmt.Errorf("cachekit: decode %q: %w", key, uerr)
	}
	return v, nil
}

// flightKey namespaces single-flight keys per cache instance so two caches
// with different prefixes never share a flight for the same logical key.
func flightKey(c Cache, key string) string {
	return fmt.Sprintf("%p\x00%s", c, key)
}
