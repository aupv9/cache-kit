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
	mu         sync.Mutex
	items      map[string]memoryItem
	prefix     string
	defaultTTL time.Duration
	codec      Codec
	now        func() time.Time // overridable in tests
}

type memoryItem struct {
	val      []byte
	deadline time.Time // zero means no expiry
}

// compile-time check
var _ Cache = (*MemoryCache)(nil)

// NewMemory returns an empty in-memory cache. Addr/Auth/DB options are
// ignored; Prefix, DefaultTTL and Codec apply as usual.
func NewMemory(opts ...Option) *MemoryCache {
	o := buildOptions(opts)
	return &MemoryCache{
		items:      make(map[string]memoryItem),
		prefix:     o.Prefix,
		defaultTTL: o.DefaultTTL,
		codec:      o.Codec,
		now:        time.Now,
	}
}

func (c *MemoryCache) key(k string) string { return c.prefix + k }

// Get returns the value at key, or ErrCacheMiss if absent or expired.
func (c *MemoryCache) Get(_ context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	it, ok := c.items[c.key(key)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCacheMiss, key)
	}
	if !it.deadline.IsZero() && !c.now().Before(it.deadline) {
		delete(c.items, c.key(key))
		return nil, fmt.Errorf("%w: %s", ErrCacheMiss, key)
	}
	return bytes.Clone(it.val), nil
}

// Set stores val at key. ttl <= 0 falls back to the configured DefaultTTL;
// if that is also zero the key never expires.
func (c *MemoryCache) Set(_ context.Context, key string, val []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	it := memoryItem{val: bytes.Clone(val)}
	if ttl > 0 {
		it.deadline = c.now().Add(ttl)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[c.key(key)] = it
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
