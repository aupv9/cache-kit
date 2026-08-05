package cachekit

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
)

// ValkeyCache implements Cache on top of valkey-go, which adds
// auto-pipelining and optional server-assisted client-side caching on top
// of plain RESP. It works against both Valkey and Redis servers.
//
// When a client-side cache TTL is configured (WithClientSideCacheTTL),
// reads go through DoCache: hot keys are served from the client's local
// LRU, the server invalidates them on writes from any client, and
// valkey-go deduplicates concurrent cache-filling GETs on its own.
// Note that cachekit's GetOrSet single-flight (group.go) still applies —
// it deduplicates *loader* (database) calls, which no client can do.
//
// Degradation caveat: unlike go-redis (which gives up after a few
// retries), valkey-go retries network errors until the context is done.
// When the server is unreachable, Get/Set block for the full context
// deadline before GetOrSet can fall through to the loader — so always
// pass request-scoped contexts with deadlines, or inject a client built
// with DisableRetry via NewValkeyWithClient if you prefer fail-fast.
type ValkeyCache struct {
	client         valkey.Client
	prefix         string
	defaultTTL     time.Duration
	codec          Codec
	clientCacheTTL time.Duration
	hooks          Hooks
	ttlJitter      float64
	negativeTTL    time.Duration
	opTimeout      time.Duration
	staleTTL       time.Duration
	earlyBeta      float64
	ownsClient     bool
}

// compile-time checks
var (
	_ Cache      = (*ValkeyCache)(nil)
	_ BatchCache = (*ValkeyCache)(nil)
)

// NewValkey dials a Valkey/Redis server from Options and returns a cache.
// The returned cache owns the connection; call Close when done.
//
// Server-assisted client-side caching (CLIENT TRACKING) is enabled only
// when WithClientSideCacheTTL is set; otherwise the client is created with
// tracking disabled so it also works against servers without RESP3
// invalidation support.
func NewValkey(opts ...Option) (*ValkeyCache, error) {
	o := buildOptions(opts)
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:  []string{o.Addr},
		Username:     o.Username,
		Password:     o.Password,
		SelectDB:     o.DB,
		DisableCache: o.ClientSideCacheTTL <= 0,
	})
	if err != nil {
		return nil, fmt.Errorf("cachekit: connect %q: %w", o.Addr, err)
	}
	c := newValkeyFromClient(client, o)
	c.ownsClient = true
	return c, nil
}

// NewValkeyWithClient wraps an existing valkey-go client. The caller keeps
// ownership of the client's lifecycle; Close on the returned cache is a
// no-op. Addr/Auth/DB options are ignored. If WithClientSideCacheTTL is
// used with a client created with DisableCache, valkey-go silently falls
// back to plain reads — no error, just no client-side caching.
func NewValkeyWithClient(client valkey.Client, opts ...Option) *ValkeyCache {
	return newValkeyFromClient(client, buildOptions(opts))
}

func newValkeyFromClient(client valkey.Client, o Options) *ValkeyCache {
	return &ValkeyCache{
		client:         client,
		prefix:         o.Prefix,
		defaultTTL:     o.DefaultTTL,
		codec:          o.Codec,
		clientCacheTTL: o.ClientSideCacheTTL,
		hooks:          o.Hooks,
		ttlJitter:      o.TTLJitter,
		negativeTTL:    o.NegativeTTL,
		opTimeout:      o.OpTimeout,
		staleTTL:       o.StaleTTL,
		earlyBeta:      o.EarlyRefreshBeta,
	}
}

func (c *ValkeyCache) cachekitConfig() cacheConfig {
	return cacheConfig{
		hooks:            c.hooks,
		negativeTTL:      c.negativeTTL,
		defaultTTL:       c.defaultTTL,
		staleTTL:         c.staleTTL,
		earlyRefreshBeta: c.earlyBeta,
	}
}

// ttlFor resolves the effective TTL for a write: default fallback, jitter,
// then the PX floor — PX truncates to milliseconds and servers reject
// "PX 0", so sub-millisecond TTLs round up like go-redis does.
func (c *ValkeyCache) ttlFor(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	ttl = applyJitter(ttl, c.ttlJitter)
	if ttl > 0 && ttl < time.Millisecond {
		ttl = time.Millisecond
	}
	return ttl
}

// setCmd builds a SET with the resolved TTL.
func (c *ValkeyCache) setCmd(key string, val []byte, ttl time.Duration) valkey.Completed {
	b := c.client.B().Set().Key(c.key(key)).Value(valkey.BinaryString(val))
	if ttl = c.ttlFor(ttl); ttl > 0 {
		return b.Px(ttl).Build()
	}
	return b.Build()
}

func (c *ValkeyCache) key(k string) string { return c.prefix + k }

// Get returns the value at key, or ErrCacheMiss if absent. With a
// client-side cache TTL configured, the read is served from the local
// cache when possible.
func (c *ValkeyCache) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()
	var res valkey.ValkeyResult
	if c.clientCacheTTL > 0 {
		res = c.client.DoCache(ctx, c.client.B().Get().Key(c.key(key)).Cache(), c.clientCacheTTL)
	} else {
		res = c.client.Do(ctx, c.client.B().Get().Key(c.key(key)).Build())
	}
	data, err := res.AsBytes()
	if err != nil {
		if valkey.IsValkeyNil(err) {
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
func (c *ValkeyCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()
	if err := c.client.Do(ctx, c.setCmd(key, val, ttl)).Error(); err != nil {
		c.hooks.error(OpSet, key, err)
		return fmt.Errorf("cachekit: set %q: %w", key, err)
	}
	return nil
}

// Delete removes key. Deleting a missing key succeeds.
func (c *ValkeyCache) Delete(ctx context.Context, key string) error {
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()
	if err := c.client.Do(ctx, c.client.B().Del().Key(c.key(key)).Build()).Error(); err != nil {
		c.hooks.error(OpDelete, key, err)
		return fmt.Errorf("cachekit: delete %q: %w", key, err)
	}
	return nil
}

// Codec returns the codec configured for this cache (default JSON).
func (c *ValkeyCache) Codec() Codec { return c.codec }

// Ping verifies connectivity, for health checks.
func (c *ValkeyCache) Ping(ctx context.Context) error {
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()
	if err := c.client.Do(ctx, c.client.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("cachekit: ping: %w", err)
	}
	return nil
}

// GetMany returns the values stored at keys in one auto-pipelined round
// trip (through the client-side cache when WithClientSideCacheTTL is
// set, like Get). Missing keys are simply absent from the result map.
func (c *ValkeyCache) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()

	var results []valkey.ValkeyResult
	if c.clientCacheTTL > 0 {
		cmds := make([]valkey.CacheableTTL, len(keys))
		for i, k := range keys {
			cmds[i] = valkey.CT(c.client.B().Get().Key(c.key(k)).Cache(), c.clientCacheTTL)
		}
		results = c.client.DoMultiCache(ctx, cmds...)
	} else {
		cmds := make([]valkey.Completed, len(keys))
		for i, k := range keys {
			cmds[i] = c.client.B().Get().Key(c.key(k)).Build()
		}
		results = c.client.DoMulti(ctx, cmds...)
	}

	// Consume every result before failing so hit/miss hooks fire per key
	// (matching RedisCache.GetMany's per-key error accounting) — DoMulti
	// has already paid for all responses.
	var firstErr error
	for i, res := range results {
		data, err := res.AsBytes()
		if err != nil {
			if valkey.IsValkeyNil(err) {
				c.hooks.miss(keys[i])
				continue
			}
			c.hooks.error(OpGet, keys[i], err)
			if firstErr == nil {
				firstErr = fmt.Errorf("cachekit: get %q: %w", keys[i], err)
			}
			continue
		}
		c.hooks.hit(keys[i])
		out[keys[i]] = data
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

// SetMany stores all items in one auto-pipelined round trip, applying the
// same TTL (after default fallback and jitter, per key) to each.
func (c *ValkeyCache) SetMany(ctx context.Context, items map[string][]byte, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	ctx, cancel := opContext(ctx, c.opTimeout)
	defer cancel()

	ks := make([]string, 0, len(items))
	cmds := make([]valkey.Completed, 0, len(items))
	for k, v := range items {
		ks = append(ks, k)
		cmds = append(cmds, c.setCmd(k, v, ttl))
	}
	var firstErr error
	for i, res := range c.client.DoMulti(ctx, cmds...) {
		if err := res.Error(); err != nil {
			c.hooks.error(OpSet, ks[i], err)
			if firstErr == nil {
				firstErr = fmt.Errorf("cachekit: set %q: %w", ks[i], err)
			}
		}
	}
	return firstErr
}

// Close releases the underlying client if this cache created it
// (NewValkey). For NewValkeyWithClient it is a no-op.
func (c *ValkeyCache) Close() error {
	if c.ownsClient {
		c.client.Close()
	}
	return nil
}
