package cachekit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/valkey-io/valkey-go"
)

// Backend-agnostic behavior lives in conformance_test.go; this file keeps
// the ValkeyCache-specific tests.

func newTestValkey(t *testing.T, opts ...Option) (*ValkeyCache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:  []string{mr.Addr()},
		DisableCache: true, // miniredis has no CLIENT TRACKING support
	})
	if err != nil {
		t.Fatalf("valkey.NewClient: %v", err)
	}
	t.Cleanup(client.Close)
	return NewValkeyWithClient(client, opts...), mr
}

func TestValkeyMissDoesNotLeakSentinel(t *testing.T) {
	c, _ := newTestValkey(t)

	_, err := c.Get(context.Background(), "absent")
	if !IsMiss(err) {
		t.Fatalf("Get(absent) err = %v, want ErrCacheMiss", err)
	}
	if valkey.IsValkeyNil(err) {
		t.Fatalf("backend error leaked through the Cache interface: %v", err)
	}
}

func TestValkeyDefaultTTLApplied(t *testing.T) {
	c, mr := newTestValkey(t, WithDefaultTTL(time.Minute))
	ctx := context.Background()

	if err := c.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if ttl := mr.TTL("k"); ttl != time.Minute {
		t.Fatalf("TTL = %v, want %v", ttl, time.Minute)
	}

	mr.FastForward(2 * time.Minute)
	if _, err := c.Get(ctx, "k"); !IsMiss(err) {
		t.Fatalf("expected expiry after FastForward, err = %v", err)
	}
}

func TestValkeyNoTTLMeansNoExpiry(t *testing.T) {
	c, mr := newTestValkey(t) // no DefaultTTL

	if err := c.Set(context.Background(), "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if ttl := mr.TTL("k"); ttl != 0 {
		t.Fatalf("TTL = %v, want none", ttl)
	}
}
