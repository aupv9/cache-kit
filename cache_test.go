package cachekit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Backend-agnostic behavior lives in conformance_test.go; this file keeps
// the RedisCache-specific tests (raw server state, outage fallback).

// newTestCache spins up an in-memory miniredis server so tests need no
// real Redis/Valkey instance.
func newTestCache(t *testing.T, opts ...Option) (*RedisCache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return NewWithClient(client, opts...), mr
}

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestRedisMissDoesNotLeakSentinel(t *testing.T) {
	c, _ := newTestCache(t)

	_, err := c.Get(context.Background(), "absent")
	if !IsMiss(err) {
		t.Fatalf("Get(absent) err = %v, want ErrCacheMiss", err)
	}
	if errors.Is(err, redis.Nil) {
		t.Fatalf("backend error leaked through the Cache interface: %v", err)
	}
}

func TestRedisPrefixAppliedOnServer(t *testing.T) {
	c, mr := newTestCache(t, WithPrefix("svc-a:"))

	if err := c.Set(context.Background(), "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !mr.Exists("svc-a:k") {
		t.Fatal("expected raw key svc-a:k on the server")
	}
}

func TestRedisDefaultTTLApplied(t *testing.T) {
	c, mr := newTestCache(t, WithDefaultTTL(time.Minute))
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

func TestGetOrSetFallsBackWhenCacheDown(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	c := NewWithClient(client)
	mr.Close() // simulate an outage: every cache op now errors

	u, err := GetOrSet(context.Background(), c, "user:3", time.Minute, func(context.Context) (user, error) {
		return user{ID: 3, Name: "resilient"}, nil
	})
	if err != nil {
		t.Fatalf("GetOrSet with cache down: %v", err)
	}
	if u.ID != 3 {
		t.Fatalf("got %+v", u)
	}
}
