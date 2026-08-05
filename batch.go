package cachekit

import (
	"context"
	"fmt"
	"time"
)

// BatchCache is implemented by backends with efficient multi-key
// operations (all built-in backends implement it). GetOrSetMany uses it
// when available and falls back to per-key Get/Set otherwise.
type BatchCache interface {
	Cache
	// GetMany returns the values stored at keys; missing keys are absent
	// from the map. A non-nil error means the whole batch failed.
	GetMany(ctx context.Context, keys []string) (map[string][]byte, error)
	// SetMany stores all items with the same ttl (resolved per key:
	// default fallback, jitter). Best-effort per key where the backend
	// allows it; the returned error reflects the first failure.
	SetMany(ctx context.Context, items map[string][]byte, ttl time.Duration) error
}

// GetOrSetMany is the batched cache-aside helper: it returns cached values
// for all keys it can, calls loader once with the keys that are missing,
// stores the loaded values, and returns the merged map.
//
// The result contains an entry per key that exists; keys the loader omits
// are simply absent (and, with WithNegativeTTL, that absence is cached so
// they are not re-loaded until the negative entry expires). A loader error
// fails the whole call. A broken cache degrades: read failures make every
// key load, write failures are best-effort, and undecodable entries
// self-heal exactly like GetOrSet.
//
// Unlike GetOrSet, batch loads are not single-flighted: deduplicating
// overlapping batches would require per-key flights with partial joins.
// Duplicate keys in the input are deduplicated.
func GetOrSetMany[T any](ctx context.Context, c Cache, keys []string, ttl time.Duration, loader func(ctx context.Context, missing []string) (map[string]T, error)) (map[string]T, error) {
	return GetOrSetManyWithCodec(ctx, c, keys, ttl, defaultCodec, loader)
}

// GetOrSetManyWithCodec is GetOrSetMany with an explicit Codec.
func GetOrSetManyWithCodec[T any](ctx context.Context, c Cache, keys []string, ttl time.Duration, codec Codec, loader func(ctx context.Context, missing []string) (map[string]T, error)) (map[string]T, error) {
	if codec == nil {
		codec = defaultCodec
	}
	cfg := configOf(c)

	uniq := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		uniq = append(uniq, k)
	}

	// Read phase; on infra failure every key degrades to the loader.
	var raw map[string][]byte
	bc, batched := c.(BatchCache)
	if batched {
		if r, err := bc.GetMany(ctx, uniq); err == nil {
			raw = r
		}
	} else {
		raw = make(map[string][]byte, len(uniq))
		for _, k := range uniq {
			if data, err := c.Get(ctx, k); err == nil {
				raw[k] = data
			}
		}
	}

	result := make(map[string]T, len(uniq))
	var missing []string
	for _, k := range uniq {
		data, ok := raw[k]
		if !ok {
			missing = append(missing, k)
			continue
		}
		if isNegativeEntry(data) {
			continue // known absent: excluded from the result, not reloaded
		}
		var v T
		if uerr := codec.Unmarshal(data, &v); uerr != nil {
			cfg.hooks.error(OpDecode, k, uerr)
			_ = c.Delete(ctx, k)
			missing = append(missing, k)
			continue
		}
		result[k] = v
	}
	if len(missing) == 0 {
		return result, nil
	}

	start := time.Now()
	loaded, err := loader(ctx, missing)
	dur := time.Since(start)
	for _, k := range missing {
		cfg.hooks.load(k, dur, err)
	}
	if err != nil {
		return nil, err
	}

	vals := make(map[string][]byte, len(loaded))
	var negs map[string][]byte
	for _, k := range missing {
		v, ok := loaded[k]
		if !ok {
			if cfg.negativeTTL > 0 {
				if negs == nil {
					negs = make(map[string][]byte)
				}
				negs[k] = negativeMarker
			}
			continue
		}
		data, merr := codec.Marshal(v)
		if merr != nil {
			return nil, fmt.Errorf("cachekit: encode %q: %w", k, merr)
		}
		vals[k] = data
		result[k] = v
	}

	// Best effort: failed writes must not fail the request. Backends
	// report failures through Hooks.OnError.
	if batched {
		_ = bc.SetMany(ctx, vals, ttl)
		_ = bc.SetMany(ctx, negs, cfg.negativeTTL)
	} else {
		for k, data := range vals {
			_ = c.Set(ctx, k, data, ttl)
		}
		for k, data := range negs {
			_ = c.Set(ctx, k, data, cfg.negativeTTL)
		}
	}
	return result, nil
}
