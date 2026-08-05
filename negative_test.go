package cachekit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestGetOrSetNegativeCaching(t *testing.T) {
	c, mr := newTestCache(t, WithNegativeTTL(time.Minute))
	ctx := context.Background()

	calls := 0
	load := func(context.Context) (user, error) {
		calls++
		return user{}, fmt.Errorf("user 42: %w", ErrNotFound)
	}

	// First call reaches the loader and caches the absence.
	if _, err := GetOrSet(ctx, c, "user:42", time.Minute, load); !errors.Is(err, ErrNotFound) {
		t.Fatalf("first call err = %v, want ErrNotFound", err)
	}
	// Second call is served from the negative entry.
	if _, err := GetOrSet(ctx, c, "user:42", time.Minute, load); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second call err = %v, want ErrNotFound", err)
	}
	if calls != 1 {
		t.Fatalf("loader called %d times, want 1 (absence must be cached)", calls)
	}

	// After the negative TTL expires the loader is consulted again —
	// and the entity may exist now.
	mr.FastForward(2 * time.Minute)
	u, err := GetOrSet(ctx, c, "user:42", time.Minute, func(context.Context) (user, error) {
		calls++
		return user{ID: 42, Name: "created"}, nil
	})
	if err != nil || u.ID != 42 {
		t.Fatalf("after expiry: %+v, %v", u, err)
	}
	if calls != 2 {
		t.Fatalf("loader called %d times total, want 2", calls)
	}
}

func TestGetOrSetNotFoundWithoutNegativeTTLIsNotCached(t *testing.T) {
	c, _ := newTestCache(t) // negative caching off
	ctx := context.Background()

	calls := 0
	load := func(context.Context) (user, error) {
		calls++
		return user{}, ErrNotFound
	}
	for i := 0; i < 2; i++ {
		if _, err := GetOrSet(ctx, c, "user:42", time.Minute, load); !errors.Is(err, ErrNotFound) {
			t.Fatalf("call %d err = %v, want ErrNotFound", i, err)
		}
	}
	if calls != 2 {
		t.Fatalf("loader called %d times, want 2 (nothing must be cached)", calls)
	}
	if _, err := c.Get(ctx, "user:42"); !IsMiss(err) {
		t.Fatalf("negative entry cached with negative caching off: %v", err)
	}
}

func TestNotFoundIsDistinctFromCacheMiss(t *testing.T) {
	if errors.Is(ErrNotFound, ErrCacheMiss) || errors.Is(ErrCacheMiss, ErrNotFound) {
		t.Fatal("ErrNotFound and ErrCacheMiss must be distinct sentinels")
	}
}

// A raw Get on a negative entry returns the marker bytes; only the
// GetOrSet helpers interpret it. This documents that boundary.
func TestNegativeMarkerNeverDecodesAsJSON(t *testing.T) {
	var v user
	if err := (JSONCodec{}).Unmarshal(negativeMarker, &v); err == nil {
		t.Fatal("negative marker decoded as JSON — marker must be unambiguous")
	}
}
