package cachekit

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryCache is a process-local Cache backed by a mutex-guarded map.
// It is meant for tests, local development, and as an L1 in front of a
// shared backend — it does no eviction beyond TTL expiry, so do not use
// it for unbounded key sets.
//
// Expired entries are dropped lazily on Get; there is no background
// reaper. Values are copied on Set and Get so callers can't alias the
// stored bytes.
type MemoryCache struct {
	mu          sync.Mutex
	items       map[string]memoryItem
	prefix      string
	defaultTTL  time.Duration
	codec       Codec
	hooks       Hooks
	ttlJitter   float64
	negativeTTL time.Duration
	staleTTL    time.Duration
	earlyBeta   float64
	now         func() time.Time // overridable in tests
}

type memoryItem struct {
	val      []byte
	deadline time.Time // zero means no expiry
}

// compile-time checks
var (
	_ Cache      = (*MemoryCache)(nil)
	_ BatchCache = (*MemoryCache)(nil)
)

// NewMemory returns an empty in-memory cache. Addr/Auth/DB options are
// ignored; Prefix, DefaultTTL and Codec apply as usual.
func NewMemory(opts ...Option) *MemoryCache {
	o := buildOptions(opts)
	return &MemoryCache{
		items:       make(map[string]memoryItem),
		prefix:      o.Prefix,
		defaultTTL:  o.DefaultTTL,
		codec:       o.Codec,
		hooks:       o.Hooks,
		ttlJitter:   o.TTLJitter,
		negativeTTL: o.NegativeTTL,
		staleTTL:    o.StaleTTL,
		earlyBeta:   o.EarlyRefreshBeta,
		now:         time.Now,
	}
}

func (c *MemoryCache) cachekitConfig() cacheConfig {
	return cacheConfig{
		hooks:            c.hooks,
		negativeTTL:      c.negativeTTL,
		defaultTTL:       c.defaultTTL,
		staleTTL:         c.staleTTL,
		earlyRefreshBeta: c.earlyBeta,
		ttlJitter:        c.ttlJitter,
	}
}

func (c *MemoryCache) key(k string) string { return c.prefix + k }

// Get returns the value at key, or ErrCacheMiss if absent or expired.
func (c *MemoryCache) Get(_ context.Context, key string) ([]byte, error) {
	// Hooks fire after unlocking: they run arbitrary user code, which
	// must not serialize other operations or deadlock by re-entering
	// this cache (e.g. when it serves as an L1).
	c.mu.Lock()
	it, ok := c.items[c.key(key)]
	if ok && !it.deadline.IsZero() && !c.now().Before(it.deadline) {
		delete(c.items, c.key(key))
		ok = false
	}
	var val []byte
	if ok {
		val = bytes.Clone(it.val)
	}
	c.mu.Unlock()

	if !ok {
		c.hooks.miss(key)
		return nil, fmt.Errorf("%w: %s", ErrCacheMiss, key)
	}
	c.hooks.hit(key)
	return val, nil
}

// Set stores val at key. ttl <= 0 falls back to the configured DefaultTTL;
// if that is also zero the key never expires.
func (c *MemoryCache) Set(_ context.Context, key string, val []byte, ttl time.Duration) error {
	it := c.newItem(val, ttl)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[c.key(key)] = it
	return nil
}

func (c *MemoryCache) newItem(val []byte, ttl time.Duration) memoryItem {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	// Enveloped values skip jitter: packForStore already jittered their
	// freshness window, and jittering the total again could shrink it
	// below freshUntil.
	if !hasEnvelope(val) {
		ttl = applyJitter(ttl, c.ttlJitter)
	}
	it := memoryItem{val: bytes.Clone(val)}
	if ttl > 0 {
		it.deadline = c.now().Add(ttl)
	}
	return it
}

// GetMany returns the values stored at keys under a single lock
// acquisition. Missing keys are simply absent from the result map.
func (c *MemoryCache) GetMany(_ context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	var hits, misses []string

	c.mu.Lock()
	for _, k := range keys {
		it, ok := c.items[c.key(k)]
		if ok && !it.deadline.IsZero() && !c.now().Before(it.deadline) {
			delete(c.items, c.key(k))
			ok = false
		}
		if !ok {
			misses = append(misses, k)
			continue
		}
		out[k] = bytes.Clone(it.val)
		hits = append(hits, k)
	}
	c.mu.Unlock()

	for _, k := range hits {
		c.hooks.hit(k)
	}
	for _, k := range misses {
		c.hooks.miss(k)
	}
	return out, nil
}

// SetMany stores all items under a single lock acquisition, applying the
// same TTL (after default fallback and jitter, per key) to each.
func (c *MemoryCache) SetMany(_ context.Context, items map[string][]byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range items {
		c.items[c.key(k)] = c.newItem(v, ttl)
	}
	return nil
}

// Delete removes key. Deleting a missing key succeeds.
func (c *MemoryCache) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, c.key(key))
	return nil
}

// Codec returns the codec configured for this cache (default JSON).
func (c *MemoryCache) Codec() Codec { return c.codec }

// Len reports the number of stored entries, including not-yet-reaped
// expired ones. Intended for tests and metrics.
func (c *MemoryCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
