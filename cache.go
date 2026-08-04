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
// (see group.go). Infrastructure errors from Get are treated as a miss so
// the service keeps working when the cache is down; in that degraded mode
// the loader result is still returned and Set is attempted best-effort.
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

	if data, err := c.Get(ctx, key); err == nil {
		var v T
		if uerr := codec.Unmarshal(data, &v); uerr != nil {
			return zero, fmt.Errorf("cachekit: decode %q: %w", key, uerr)
		}
		return v, nil
	}

	// Miss (or degraded cache): load through the single-flight group so
	// only one goroutine per (cache, key) hits the source of truth.
	data, err := loadGroup.do(flightKey(c, key), func() ([]byte, error) {
		// Another flight may have populated the key while we waited.
		if data, err := c.Get(ctx, key); err == nil {
			return data, nil
		}
		v, err := loader(ctx)
		if err != nil {
			return nil, err
		}
		data, err := codec.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("cachekit: encode %q: %w", key, err)
		}
		// Best effort: a failed Set must not fail the request.
		_ = c.Set(ctx, key, data, ttl)
		return data, nil
	})
	if err != nil {
		return zero, err
	}

	var v T
	if uerr := codec.Unmarshal(data, &v); uerr != nil {
		return zero, fmt.Errorf("cachekit: decode %q: %w", key, uerr)
	}
	return v, nil
}

// flightKey namespaces single-flight keys per cache instance so two caches
// with different prefixes never share a flight for the same logical key.
func flightKey(c Cache, key string) string {
	return fmt.Sprintf("%p\x00%s", c, key)
}
