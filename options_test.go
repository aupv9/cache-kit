package cachekit

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestTTLJitterSpreadsExpiry(t *testing.T) {
	const base = time.Minute
	c, mr := newTestCache(t, WithDefaultTTL(base), WithTTLJitter(0.5))
	ctx := context.Background()

	lo, hi := base/2, base+base/2
	distinct := map[time.Duration]struct{}{}
	for i := range 20 {
		key := fmt.Sprintf("k%d", i)
		if err := c.Set(ctx, key, []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
		ttl := mr.TTL(key)
		if ttl < lo || ttl > hi {
			t.Fatalf("TTL(%s) = %v, want within [%v, %v]", key, ttl, lo, hi)
		}
		distinct[ttl] = struct{}{}
	}
	if len(distinct) < 2 {
		t.Fatalf("all 20 jittered TTLs identical (%v) — jitter not applied", mr.TTL("k0"))
	}
}

func TestTTLJitterZeroKeepsExactTTL(t *testing.T) {
	c, mr := newTestCache(t, WithDefaultTTL(time.Minute))
	if err := c.Set(context.Background(), "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if ttl := mr.TTL("k"); ttl != time.Minute {
		t.Fatalf("TTL = %v, want exactly %v with jitter off", ttl, time.Minute)
	}
}

func TestOpTimeoutBoundsCacheOps(t *testing.T) {
	// A nanosecond budget expires before any I/O: every op must fail
	// fast with a deadline error instead of waiting on the server.
	c, _ := newTestCache(t, WithOpTimeout(time.Nanosecond))
	ctx := context.Background()

	if _, err := c.Get(ctx, "k"); err == nil || IsMiss(err) {
		t.Fatalf("Get err = %v, want deadline failure", err)
	}
	if err := c.Set(ctx, "k", []byte("v"), 0); err == nil {
		t.Fatal("Set succeeded despite expired op budget")
	}
	if err := c.Delete(ctx, "k"); err == nil {
		t.Fatal("Delete succeeded despite expired op budget")
	}
}

func TestOpTimeoutDoesNotBoundLoader(t *testing.T) {
	c, _ := newTestCache(t, WithOpTimeout(20*time.Millisecond))

	// The loader takes far longer than the op timeout; only cache ops
	// are bounded, so GetOrSet must still succeed.
	u, err := GetOrSet(context.Background(), c, "slow", time.Minute, func(ctx context.Context) (user, error) {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return user{}, ctx.Err()
		}
		return user{ID: 1, Name: "slow"}, nil
	})
	if err != nil || u.ID != 1 {
		t.Fatalf("GetOrSet = %+v, %v", u, err)
	}
}
