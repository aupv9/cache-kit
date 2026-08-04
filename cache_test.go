package cachekit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestCache spins up an in-memory miniredis server so tests need no
// real Redis/Valkey instance.
func newTestCache(t *testing.T, opts ...Option) (*RedisCache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return NewWithClient(client, opts...), mr
}

func TestSetGetRoundTrip(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	if err := c.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("Get = %q, want %q", got, "v")
	}
}

func TestGetMissReturnsErrCacheMiss(t *testing.T) {
	c, _ := newTestCache(t)

	_, err := c.Get(context.Background(), "absent")
	if !IsMiss(err) {
		t.Fatalf("Get(absent) err = %v, want ErrCacheMiss", err)
	}
	if errors.Is(err, redis.Nil) {
		t.Fatalf("backend error leaked through the Cache interface: %v", err)
	}
}

func TestDeleteIsIdempotent(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	if err := c.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete(missing): %v", err)
	}
	if _, err := c.Get(ctx, "k"); !IsMiss(err) {
		t.Fatalf("Get after Delete err = %v, want ErrCacheMiss", err)
	}
}

func TestPrefixIsolatesKeys(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	a := NewWithClient(client, WithPrefix("svc-a:"))
	b := NewWithClient(client, WithPrefix("svc-b:"))
	ctx := context.Background()

	if err := a.Set(ctx, "k", []byte("from-a"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := b.Get(ctx, "k"); !IsMiss(err) {
		t.Fatalf("prefix b saw prefix a's key: err = %v", err)
	}
	if !mr.Exists("svc-a:k") {
		t.Fatal("expected raw key svc-a:k on the server")
	}
}

func TestDefaultTTLApplied(t *testing.T) {
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

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestGetOrSetCachesLoaderResult(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	calls := 0
	load := func(context.Context) (user, error) {
		calls++
		return user{ID: 1, Name: "an"}, nil
	}

	u1, err := GetOrSet(ctx, c, "user:1", time.Minute, load)
	if err != nil {
		t.Fatalf("GetOrSet #1: %v", err)
	}
	u2, err := GetOrSet(ctx, c, "user:1", time.Minute, load)
	if err != nil {
		t.Fatalf("GetOrSet #2: %v", err)
	}
	if u1 != u2 || u1.Name != "an" {
		t.Fatalf("values differ: %+v vs %+v", u1, u2)
	}
	if calls != 1 {
		t.Fatalf("loader called %d times, want 1 (second call must hit cache)", calls)
	}
}

func TestGetOrSetPropagatesLoaderError(t *testing.T) {
	c, _ := newTestCache(t)
	sentinel := errors.New("db down")

	_, err := GetOrSet(context.Background(), c, "user:2", time.Minute, func(context.Context) (user, error) {
		return user{}, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
	// A failed load must not poison the cache.
	if _, err := c.Get(context.Background(), "user:2"); !IsMiss(err) {
		t.Fatalf("key cached after loader error: %v", err)
	}
}

func TestGetOrSetSingleFlight(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	var calls atomic.Int32
	release := make(chan struct{})
	load := func(context.Context) (user, error) {
		calls.Add(1)
		<-release // hold the flight open until all goroutines have joined
		return user{ID: 7, Name: "shared"}, nil
	}

	const n = 20
	var wg sync.WaitGroup
	results := make([]user, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = GetOrSet(ctx, c, "hot", time.Minute, load)
		}()
	}

	// Wait until at least one goroutine is inside the loader, give the
	// rest a moment to pile onto the same flight, then release.
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	for i := range n {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if results[i].ID != 7 {
			t.Fatalf("goroutine %d got %+v", i, results[i])
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader ran %d times under concurrency, want 1", got)
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
