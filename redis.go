package cachekit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache implements Cache on top of go-redis v9. Because go-redis
// speaks plain RESP it works unchanged against both Redis and Valkey.
//
// To switch to a different client (e.g. valkey-go for auto-pipelining and
// server-assisted client-side caching), add a sibling file implementing
// the same Cache interface — callers depend only on cachekit.Cache.
type RedisCache struct {
	client      redis.UniversalClient
	prefix      string
	defaultTTL  time.Duration
	codec       Codec
	hooks       Hooks
	ttlJitter   float64
	negativeTTL time.Duration
	opTimeout   time.Duration
	staleTTL    time.Duration
	earlyBeta   float64
	ownsClient  bool
}

// compile-time checks
var (
	_ Cache      = (*RedisCache)(nil)
	_ BatchCache = (*RedisCache)(nil)
)

// New dials a Redis/Valkey server from Options and returns a cache.
// The returned cache owns the connection; call Close when done.
func New(opts ...Option) *RedisCache {
	o := buildOptions(opts)
	client := redis.NewClient(&redis.Options{
		Addr:     o.Addr,
		Username: o.Username,
		Password: o.Password,
		DB:       o.DB,
	})
	c := newFromClient(client, o)
	c.ownsClient = true
	return c
}

// NewWithClient wraps an existing go-redis client (single node, cluster,
// or sentinel). The caller keeps ownership of the client's lifecycle;
// Close on the returned cache is a no-op. Addr/Auth/DB options are
// ignored — only Prefix, DefaultTTL and Codec apply.
func NewWithClient(client redis.UniversalClient, opts ...Option) *RedisCache {
	return newFromClient(client, buildOptions(opts))
}

func newFromClient(client redis.UniversalClient, o Options) *RedisCache {
	return &RedisCache{
		client:      client,
		prefix:      o.Prefix,
		defaultTTL:  o.DefaultTTL,
		codec:       o.Codec,
		hooks:       o.Hooks,
		ttlJitter:   o.TTLJitter,
		negativeTTL: o.NegativeTTL,
		opTimeout:   o.OpTimeout,
		staleTTL:    o.StaleTTL,
		earlyBeta:   o.EarlyRefreshBeta,
	}
}

func (c *RedisCache) cachekitConfig() cacheConfig {
	return cacheConfig{
		hooks:            c.hooks,
		negativeTTL:      c.negativeTTL,
		defaultTTL:       c.defaultTTL,
		staleTTL:         c.staleTTL,
		earlyRefreshBeta: c.earlyBeta,
	}
}

// ttlFor resolves the effective TTL for a write: default fallback, then
// jitter so co-written keys don't expire together.
func (c *RedisCache) ttlFor(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	return applyJitter(ttl, c.ttlJitter)
}

func (c *RedisCache) key(k string) string { return c.prefix + k }

// Get returns the value at key, or ErrCacheMiss if absent.
func (c *RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()
	data, err := c.client.Get(ctx, c.key(key)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			c.hooks.miss(key)
			return nil, fmt.Errorf("%w: %s", ErrCacheMiss, key)
		}
		c.hooks.error(OpGet, key, err)
		return nil, fmt.Errorf("cachekit: get %q: %w", key, err)
	}
	c.hooks.hit(key)
	return data, nil
}

// Set stores val at key. ttl <= 0 falls back to the configured DefaultTTL;
// if that is also zero the key never expires.
func (c *RedisCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()
	if err := c.client.Set(ctx, c.key(key), val, c.ttlFor(ttl)).Err(); err != nil {
		c.hooks.error(OpSet, key, err)
		return fmt.Errorf("cachekit: set %q: %w", key, err)
	}
	return nil
}

// Delete removes key. Deleting a missing key succeeds.
func (c *RedisCache) Delete(ctx context.Context, key string) error {
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()
	if err := c.client.Del(ctx, c.key(key)).Err(); err != nil {
		c.hooks.error(OpDelete, key, err)
		return fmt.Errorf("cachekit: delete %q: %w", key, err)
	}
	return nil
}

// Codec returns the codec configured for this cache (default JSON),
// for use with GetOrSetWithCodec.
func (c *RedisCache) Codec() Codec { return c.codec }

// Ping verifies connectivity, for health checks.
func (c *RedisCache) Ping(ctx context.Context) error {
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("cachekit: ping: %w", err)
	}
	return nil
}

// GetMany returns the values stored at keys in one MGET round trip.
// Missing keys are simply absent from the result map.
func (c *RedisCache) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()

	pkeys := make([]string, len(keys))
	for i, k := range keys {
		pkeys[i] = c.key(k)
	}
	vals, err := c.client.MGet(ctx, pkeys...).Result()
	if err != nil {
		for _, k := range keys {
			c.hooks.error(OpGet, k, err)
		}
		return nil, fmt.Errorf("cachekit: mget (%d keys): %w", len(keys), err)
	}
	for i, v := range vals {
		s, ok := v.(string)
		if !ok { // nil for missing keys
			c.hooks.miss(keys[i])
			continue
		}
		c.hooks.hit(keys[i])
		out[keys[i]] = []byte(s)
	}
	return out, nil
}

// SetMany stores all items in one pipelined round trip, applying the same
// TTL (after default fallback and jitter, per key) to each.
func (c *RedisCache) SetMany(ctx context.Context, items map[string][]byte, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()

	pipe := c.client.Pipeline()
	for k, v := range items {
		pipe.Set(ctx, c.key(k), v, c.ttlFor(ttl))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		for k := range items {
			c.hooks.error(OpSet, k, err)
		}
		return fmt.Errorf("cachekit: mset (%d keys): %w", len(items), err)
	}
	return nil
}

// Close releases the underlying client if this cache created it (New).
// For NewWithClient it is a no-op — the caller owns the client.
func (c *RedisCache) Close() error {
	if !c.ownsClient {
		return nil
	}
	return c.client.Close()
}
