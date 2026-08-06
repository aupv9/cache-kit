//go:build integration

package cachekit

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/valkey-io/valkey-go"
)

// Integration tests exercise the paths miniredis cannot: valkey-go's
// server-assisted client-side caching (CLIENT TRACKING → DoCache /
// DoMultiCache). They need a real Redis 6+/Valkey server:
//
//	CACHEKIT_SERVER_ADDR=localhost:6379 go test -race -tags integration -run Integration ./...
//
// CI runs them against a valkey service container.

func integrationAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("CACHEKIT_SERVER_ADDR")
	if addr == "" {
		t.Skip("set CACHEKIT_SERVER_ADDR to run integration tests")
	}
	return addr
}

// uniquePrefix keeps concurrent/repeated runs from seeing each other's keys.
func uniquePrefix(t *testing.T) string {
	return fmt.Sprintf("it-%s-%d:", t.Name(), time.Now().UnixNano())
}

func TestIntegrationValkeyClientSideCacheInvalidation(t *testing.T) {
	addr := integrationAddr(t)
	prefix := uniquePrefix(t)

	cache, err := NewValkey(
		WithAddr(addr),
		WithPrefix(prefix),
		WithClientSideCacheTTL(time.Minute),
	)
	if err != nil {
		t.Fatalf("NewValkey: %v", err)
	}
	t.Cleanup(func() { cache.Close() })
	ctx := context.Background()

	if err := cache.Set(ctx, "k", []byte("v1"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// Prime the client-side cache.
	if data, err := cache.Get(ctx, "k"); err != nil || string(data) != "v1" {
		t.Fatalf("Get = %q, %v", data, err)
	}

	// Write through a completely separate connection: the server must
	// push an invalidation to our tracking client.
	other, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:  []string{addr},
		DisableCache: true,
	})
	if err != nil {
		t.Fatalf("second client: %v", err)
	}
	t.Cleanup(other.Close)
	if err := other.Do(ctx, other.B().Set().Key(prefix+"k").Value("v2").Build()).Error(); err != nil {
		t.Fatalf("external SET: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		data, err := cache.Get(ctx, "k")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(data) == "v2" {
			return // invalidation received, fresh value served
		}
		select {
		case <-deadline:
			t.Fatalf("client-side cache never invalidated, still %q", data)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestIntegrationValkeyGetManyThroughClientSideCache(t *testing.T) {
	addr := integrationAddr(t)
	prefix := uniquePrefix(t)

	cache, err := NewValkey(
		WithAddr(addr),
		WithPrefix(prefix),
		WithClientSideCacheTTL(time.Minute),
	)
	if err != nil {
		t.Fatalf("NewValkey: %v", err)
	}
	t.Cleanup(func() { cache.Close() })
	ctx := context.Background()

	items := map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2\x00bin")}
	if err := cache.SetMany(ctx, items, time.Minute); err != nil {
		t.Fatalf("SetMany: %v", err)
	}
	for i := 0; i < 2; i++ { // second pass reads from the local cache
		got, err := cache.GetMany(ctx, []string{"k1", "k2", "absent"})
		if err != nil {
			t.Fatalf("GetMany #%d: %v", i, err)
		}
		if len(got) != 2 || string(got["k1"]) != "v1" || string(got["k2"]) != "v2\x00bin" {
			t.Fatalf("GetMany #%d = %q", i, got)
		}
	}
}

// TestIntegrationConformanceRealServer runs a quick end-to-end sanity of
// both connection-owning constructors against the real server.
func TestIntegrationConformanceRealServer(t *testing.T) {
	addr := integrationAddr(t)

	t.Run("redis", func(t *testing.T) {
		client := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { client.Close() })
		c := NewWithClient(client, WithPrefix(uniquePrefix(t)))
		integrationSanity(t, c)
	})
	t.Run("valkey", func(t *testing.T) {
		c, err := NewValkey(WithAddr(addr), WithPrefix(uniquePrefix(t)), WithClientSideCacheTTL(time.Minute))
		if err != nil {
			t.Fatalf("NewValkey: %v", err)
		}
		t.Cleanup(func() { c.Close() })
		integrationSanity(t, c)
	})
}

func integrationSanity(t *testing.T, c Cache) {
	t.Helper()
	ctx := context.Background()

	if _, err := c.Get(ctx, "absent"); !IsMiss(err) {
		t.Fatalf("Get(absent) = %v, want miss", err)
	}
	u, err := GetOrSet(ctx, c, "user:1", time.Minute, func(context.Context) (user, error) {
		return user{ID: 1, Name: "an"}, nil
	})
	if err != nil || u.ID != 1 {
		t.Fatalf("GetOrSet = %+v, %v", u, err)
	}
	u, err = GetOrSet(ctx, c, "user:1", time.Minute, func(context.Context) (user, error) {
		t.Error("loader must not run on a hit")
		return user{}, nil
	})
	if err != nil || u.ID != 1 {
		t.Fatalf("GetOrSet hit = %+v, %v", u, err)
	}
	if err := c.Delete(ctx, "user:1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}
