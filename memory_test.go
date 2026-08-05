package cachekit

import (
	"context"
	"testing"
	"time"
)

// Backend-agnostic behavior lives in conformance_test.go; this file keeps
// the MemoryCache-specific tests (clock-driven expiry, value copying).

func TestMemoryTTLExpiry(t *testing.T) {
	c := NewMemory(WithDefaultTTL(time.Minute))
	now := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return now }
	ctx := context.Background()

	if err := c.Set(ctx, "k", []byte("v"), 0); err != nil { // uses DefaultTTL
		t.Fatalf("Set: %v", err)
	}
	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatalf("Get before expiry: %v", err)
	}

	now = now.Add(2 * time.Minute)
	if _, err := c.Get(ctx, "k"); !IsMiss(err) {
		t.Fatalf("Get after expiry err = %v, want ErrCacheMiss", err)
	}
	if c.Len() != 0 {
		t.Fatalf("expired entry not reaped, Len = %d", c.Len())
	}
}

func TestMemoryNoTTLMeansNoExpiry(t *testing.T) {
	c := NewMemory() // no DefaultTTL
	now := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return now }
	ctx := context.Background()

	if err := c.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	now = now.Add(1000 * time.Hour)
	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatalf("key expired without a TTL: %v", err)
	}
}

func TestMemoryValuesAreCopied(t *testing.T) {
	c := NewMemory()
	ctx := context.Background()

	in := []byte("original")
	if err := c.Set(ctx, "k", in, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	in[0] = 'X' // mutating the caller's slice must not affect the store

	out, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(out) != "original" {
		t.Fatalf("stored value aliased caller's slice: %q", out)
	}

	out[0] = 'Y' // mutating the returned slice must not affect the store
	again, _ := c.Get(ctx, "k")
	if string(again) != "original" {
		t.Fatalf("returned value aliased the store: %q", again)
	}
}
