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
	"github.com/valkey-io/valkey-go"
)

// backendFactory builds two caches for a backend: `a` and `b` share the
// same underlying store where the backend has one, with prefixes "a:" and
// "b:", so the suite can assert prefix isolation.
type backendFactory struct {
	name string
	make func(t *testing.T) (a, b Cache)
}

func backends() []backendFactory {
	return []backendFactory{
		{name: "redis", make: func(t *testing.T) (Cache, Cache) {
			t.Helper()
			mr := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { client.Close() })
			return NewWithClient(client, WithPrefix("a:")), NewWithClient(client, WithPrefix("b:"))
		}},
		{name: "valkey", make: func(t *testing.T) (Cache, Cache) {
			t.Helper()
			mr := miniredis.RunT(t)
			// DisableCache: miniredis has no CLIENT TRACKING support;
			// the client-side-caching path needs a real server.
			client, err := valkey.NewClient(valkey.ClientOption{
				InitAddress:  []string{mr.Addr()},
				DisableCache: true,
			})
			if err != nil {
				t.Fatalf("valkey.NewClient: %v", err)
			}
			t.Cleanup(client.Close)
			return NewValkeyWithClient(client, WithPrefix("a:")), NewValkeyWithClient(client, WithPrefix("b:"))
		}},
		{name: "memory", make: func(t *testing.T) (Cache, Cache) {
			t.Helper()
			return NewMemory(WithPrefix("a:")), NewMemory(WithPrefix("b:"))
		}},
	}
}

// TestConformance runs the behavioral contract from CLAUDE.md against
// every backend. New backends must be added to backends() above.
func TestConformance(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("RoundTrip", func(t *testing.T) {
				c, _ := b.make(t)
				ctx := context.Background()
				val := []byte("v\x00binary\xff") // values must be binary-safe
				if err := c.Set(ctx, "k", val, time.Minute); err != nil {
					t.Fatalf("Set: %v", err)
				}
				got, err := c.Get(ctx, "k")
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if string(got) != string(val) {
					t.Fatalf("Get = %q, want %q", got, val)
				}
			})

			t.Run("MissIsErrCacheMiss", func(t *testing.T) {
				c, _ := b.make(t)
				_, err := c.Get(context.Background(), "absent")
				if !IsMiss(err) {
					t.Fatalf("Get(absent) err = %v, want ErrCacheMiss", err)
				}
			})

			t.Run("DeleteIsIdempotent", func(t *testing.T) {
				c, _ := b.make(t)
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
			})

			t.Run("PrefixIsolation", func(t *testing.T) {
				a, bc := b.make(t)
				ctx := context.Background()
				if err := a.Set(ctx, "k", []byte("from-a"), 0); err != nil {
					t.Fatalf("Set: %v", err)
				}
				if _, err := bc.Get(ctx, "k"); !IsMiss(err) {
					t.Fatalf("prefix b saw prefix a's key: err = %v", err)
				}
			})

			t.Run("GetOrSetCachesLoaderResult", func(t *testing.T) {
				c, _ := b.make(t)
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
					t.Fatalf("loader called %d times, want 1", calls)
				}
			})

			t.Run("GetOrSetPropagatesLoaderError", func(t *testing.T) {
				c, _ := b.make(t)
				ctx := context.Background()
				sentinel := errors.New("db down")
				_, err := GetOrSet(ctx, c, "user:2", time.Minute, func(context.Context) (user, error) {
					return user{}, sentinel
				})
				if !errors.Is(err, sentinel) {
					t.Fatalf("err = %v, want %v", err, sentinel)
				}
				if _, err := c.Get(ctx, "user:2"); !IsMiss(err) {
					t.Fatalf("key cached after loader error: %v", err)
				}
			})

			t.Run("GetOrSetSingleFlight", func(t *testing.T) {
				c, _ := b.make(t)
				ctx := context.Background()

				var calls atomic.Int32
				release := make(chan struct{})
				load := func(context.Context) (user, error) {
					calls.Add(1)
					<-release // hold the flight open until all goroutines join
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
			})
		})
	}
}
