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
	client     redis.UniversalClient
	prefix     string
	defaultTTL time.Duration
	codec      Codec
	hooks      Hooks
	ownsClient bool
}

// compile-time check
var _ Cache = (*RedisCache)(nil)

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
		client:     client,
		prefix:     o.Prefix,
		defaultTTL: o.DefaultTTL,
		codec:      o.Codec,
		hooks:      o.Hooks,
	}
}

func (c *RedisCache) cacheHooks() Hooks { return c.hooks }

func (c *RedisCache) key(k string) string { return c.prefix + k }

// Get returns the value at key, or ErrCacheMiss if absent.
func (c *RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
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
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	if err := c.client.Set(ctx, c.key(key), val, ttl).Err(); err != nil {
		c.hooks.error(OpSet, key, err)
		return fmt.Errorf("cachekit: set %q: %w", key, err)
	}
	return nil
}

// Delete removes key. Deleting a missing key succeeds.
func (c *RedisCache) Delete(ctx context.Context, key string) error {
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
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("cachekit: ping: %w", err)
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
