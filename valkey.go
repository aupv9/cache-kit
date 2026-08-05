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
	ownsClient     bool
}

// compile-time check
var _ Cache = (*ValkeyCache)(nil)

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
	}
}

func (c *ValkeyCache) key(k string) string { return c.prefix + k }

// Get returns the value at key, or ErrCacheMiss if absent. With a
// client-side cache TTL configured, the read is served from the local
// cache when possible.
func (c *ValkeyCache) Get(ctx context.Context, key string) ([]byte, error) {
	var res valkey.ValkeyResult
	if c.clientCacheTTL > 0 {
		res = c.client.DoCache(ctx, c.client.B().Get().Key(c.key(key)).Cache(), c.clientCacheTTL)
	} else {
		res = c.client.Do(ctx, c.client.B().Get().Key(c.key(key)).Build())
	}
	data, err := res.AsBytes()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return nil, fmt.Errorf("%w: %s", ErrCacheMiss, key)
		}
		return nil, fmt.Errorf("cachekit: get %q: %w", key, err)
	}
	return data, nil
}

// Set stores val at key. ttl <= 0 falls back to the configured DefaultTTL;
// if that is also zero the key never expires.
func (c *ValkeyCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	// PX truncates to milliseconds; a sub-millisecond TTL would become
	// "PX 0", which servers reject. Round up like go-redis does.
	if ttl > 0 && ttl < time.Millisecond {
		ttl = time.Millisecond
	}
	b := c.client.B().Set().Key(c.key(key)).Value(valkey.BinaryString(val))
	var cmd valkey.Completed
	if ttl > 0 {
		cmd = b.Px(ttl).Build()
	} else {
		cmd = b.Build()
	}
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("cachekit: set %q: %w", key, err)
	}
	return nil
}

// Delete removes key. Deleting a missing key succeeds.
func (c *ValkeyCache) Delete(ctx context.Context, key string) error {
	if err := c.client.Do(ctx, c.client.B().Del().Key(c.key(key)).Build()).Error(); err != nil {
		return fmt.Errorf("cachekit: delete %q: %w", key, err)
	}
	return nil
}

// Codec returns the codec configured for this cache (default JSON).
func (c *ValkeyCache) Codec() Codec { return c.codec }

// Ping verifies connectivity, for health checks.
func (c *ValkeyCache) Ping(ctx context.Context) error {
	if err := c.client.Do(ctx, c.client.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("cachekit: ping: %w", err)
	}
	return nil
}

// Close releases the underlying client if this cache created it
// (NewValkey). For NewValkeyWithClient it is a no-op.
func (c *ValkeyCache) Close() error {
	if c.ownsClient {
		c.client.Close()
	}
	return nil
}
