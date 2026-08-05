package cachekit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// SWR tests use MemoryCache with the real clock: envelope freshness is
// wall-clock based, so short real durations are the honest way to test it.

func TestEnvelopeRoundTrip(t *testing.T) {
	fu := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	data := wrapEnvelope([]byte("payload\x00bin"), fu, 250*time.Millisecond)

	env, payload := parseEnvelope(data)
	if string(payload) != "payload\x00bin" {
		t.Fatalf("payload = %q", payload)
	}
	if !env.freshUntil.Equal(fu) {
		t.Fatalf("freshUntil = %v, want %v", env.freshUntil, fu)
	}
	if env.delta != 250*time.Millisecond {
		t.Fatalf("delta = %v", env.delta)
	}
}

func TestEnvelopeLegacyBytesParseAsBarePayload(t *testing.T) {
	for _, raw := range [][]byte{[]byte(`{"id":1}`), []byte(""), []byte("\x00short")} {
		env, payload := parseEnvelope(raw)
		if !env.freshUntil.IsZero() || env.delta != 0 {
			t.Fatalf("legacy bytes %q produced metadata %+v", raw, env)
		}
		if string(payload) != string(raw) {
			t.Fatalf("payload = %q, want %q", payload, raw)
		}
	}
}

func TestEnvelopeMarkerAndMagicDisjoint(t *testing.T) {
	if env, _ := parseEnvelope(negativeMarker); !env.freshUntil.IsZero() {
		t.Fatal("negative marker parsed as an envelope")
	}
	if isNegativeEntry(wrapEnvelope(nil, time.Now(), 0)) {
		t.Fatal("envelope recognized as negative marker")
	}
}

func TestXFetchBounds(t *testing.T) {
	now := time.Now()

	if (envelope{}).shouldEarlyRefresh(now, 1) {
		t.Fatal("entry without freshness metadata must never early-refresh")
	}
	e := envelope{freshUntil: now.Add(time.Minute), delta: time.Millisecond}
	if e.shouldEarlyRefresh(now, 0) {
		t.Fatal("beta = 0 must disable early refresh")
	}

	// Far from expiry with a cheap loader: P(refresh) = e^(-60000) ≈ 0.
	far := envelope{freshUntil: now.Add(time.Minute), delta: time.Millisecond}
	for i := 0; i < 1000; i++ {
		if far.shouldEarlyRefresh(now, 1) {
			t.Fatal("refreshed a full minute early with a 1ms loader")
		}
	}

	// Moments from expiry with an expensive loader: P(no refresh over
	// 200 trials) < 1e-80.
	near := envelope{freshUntil: now.Add(time.Millisecond), delta: time.Second}
	hits := 0
	for i := 0; i < 200; i++ {
		if near.shouldEarlyRefresh(now, 1) {
			hits++
		}
	}
	if hits == 0 {
		t.Fatal("never refreshed just before expiry despite expensive loader")
	}
}

func TestStaleWhileRevalidateServesStaleAndRefreshes(t *testing.T) {
	c := NewMemory(WithStaleTTL(5 * time.Second))
	ctx := context.Background()

	var calls atomic.Int32
	release := make(chan struct{})
	version := func(n int) func(context.Context) (user, error) {
		return func(context.Context) (user, error) {
			calls.Add(1)
			if n == 2 {
				<-release // make the refresh observable
			}
			return user{ID: n, Name: "v"}, nil
		}
	}

	// Populate with a short freshness TTL, then let it go stale.
	if _, err := GetOrSet(ctx, c, "k", 30*time.Millisecond, version(1)); err != nil {
		t.Fatalf("initial load: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	// Stale hit: must return v1 immediately even though the refresh
	// loader is blocked, and must start a background refresh.
	start := time.Now()
	u, err := GetOrSet(ctx, c, "k", 30*time.Millisecond, version(2))
	if err != nil || u.ID != 1 {
		t.Fatalf("stale serve = %+v, %v (want cached v1)", u, err)
	}
	if since := time.Since(start); since > time.Second {
		t.Fatalf("stale serve blocked on the loader (%v)", since)
	}
	for deadline := time.After(2 * time.Second); calls.Load() < 2; { // refresh runs async
		select {
		case <-deadline:
			t.Fatal("background refresh never started")
		case <-time.After(time.Millisecond):
		}
	}

	// More stale hits pile onto the same (still blocked) refresh flight.
	for i := 0; i < 5; i++ {
		if u, err := GetOrSet(ctx, c, "k", 30*time.Millisecond, version(2)); err != nil || u.ID != 1 {
			t.Fatalf("stale serve #%d = %+v, %v", i, u, err)
		}
	}
	time.Sleep(20 * time.Millisecond) // give stray refreshes time to surface
	if got := calls.Load(); got != 2 { // initial load + one refresh
		t.Fatalf("loader calls = %d, want 2 (refresh must dedupe)", got)
	}

	// Let the refresh finish and land.
	close(release)
	deadline := time.After(2 * time.Second)
	for {
		u, err := GetOrSet(ctx, c, "k", 30*time.Millisecond, version(3))
		if err != nil {
			t.Fatalf("GetOrSet: %v", err)
		}
		if u.ID == 2 {
			return // refreshed value visible
		}
		select {
		case <-deadline:
			t.Fatalf("refresh never landed, still seeing %+v", u)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestStaleWhileRevalidateRefreshFailureKeepsStale(t *testing.T) {
	c := NewMemory(WithStaleTTL(5 * time.Second))
	ctx := context.Background()

	if _, err := GetOrSet(ctx, c, "k", 30*time.Millisecond, func(context.Context) (user, error) {
		return user{ID: 1, Name: "v1"}, nil
	}); err != nil {
		t.Fatalf("initial load: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	failed := make(chan struct{})
	var once atomic.Bool
	u, err := GetOrSet(ctx, c, "k", 30*time.Millisecond, func(context.Context) (user, error) {
		if once.CompareAndSwap(false, true) {
			close(failed)
		}
		return user{}, errors.New("db down")
	})
	if err != nil || u.ID != 1 {
		t.Fatalf("stale serve during refresh failure = %+v, %v", u, err)
	}

	<-failed // refresh ran and failed...
	time.Sleep(20 * time.Millisecond)
	u, err = GetOrSet(ctx, c, "k", 30*time.Millisecond, func(context.Context) (user, error) {
		return user{}, errors.New("db still down")
	})
	if err != nil || u.ID != 1 {
		t.Fatalf("stale value lost after failed refresh: %+v, %v", u, err)
	}
}

func TestStaleWindowEndsAtHardExpiry(t *testing.T) {
	c := NewMemory(WithStaleTTL(40 * time.Millisecond))
	ctx := context.Background()

	calls := 0
	load := func(context.Context) (user, error) {
		calls++
		return user{ID: calls, Name: "v"}, nil
	}
	if _, err := GetOrSet(ctx, c, "k", 30*time.Millisecond, load); err != nil {
		t.Fatalf("initial load: %v", err)
	}

	// Past freshness + stale window: the entry is gone, so this is a
	// plain synchronous miss-load, not a stale serve.
	time.Sleep(120 * time.Millisecond)
	u, err := GetOrSet(ctx, c, "k", 30*time.Millisecond, load)
	if err != nil || u.ID != 2 {
		t.Fatalf("after hard expiry = %+v, %v (want fresh v2)", u, err)
	}
}

func TestSWRComposesWithNegativeCaching(t *testing.T) {
	c := NewMemory(WithStaleTTL(time.Second), WithNegativeTTL(time.Minute))
	ctx := context.Background()

	calls := 0
	if _, err := GetOrSet(ctx, c, "ghost", time.Minute, func(context.Context) (user, error) {
		calls++
		return user{}, ErrNotFound
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if _, err := GetOrSet(ctx, c, "ghost", time.Minute, func(context.Context) (user, error) {
		calls++
		return user{}, ErrNotFound
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if calls != 1 {
		t.Fatalf("loader calls = %d, want 1 — negative entry must bypass envelope handling", calls)
	}
}

func TestSWREnabledCacheReadsLegacyPlainEntries(t *testing.T) {
	c := NewMemory(WithStaleTTL(time.Second))
	ctx := context.Background()

	// Entry written before SWR was enabled (bare codec bytes).
	if err := c.Set(ctx, "k", []byte(`{"id":7,"name":"legacy"}`), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	u, err := GetOrSet(ctx, c, "k", time.Minute, func(context.Context) (user, error) {
		t.Error("loader must not run for a readable legacy entry")
		return user{}, nil
	})
	if err != nil || u.ID != 7 {
		t.Fatalf("legacy read = %+v, %v", u, err)
	}
}

func TestGetOrSetManyReadsEnvelopedEntries(t *testing.T) {
	c := NewMemory(WithStaleTTL(time.Second))
	ctx := context.Background()

	// Written (enveloped) by GetOrSet, read back by GetOrSetMany.
	if _, err := GetOrSet(ctx, c, "user:1", time.Minute, func(context.Context) (user, error) {
		return user{ID: 1, Name: "one"}, nil
	}); err != nil {
		t.Fatalf("GetOrSet: %v", err)
	}
	got, err := GetOrSetMany(ctx, c, []string{"user:1", "user:2"}, time.Minute,
		func(_ context.Context, missing []string) (map[string]user, error) {
			if len(missing) != 1 || missing[0] != "user:2" {
				t.Errorf("missing = %v, want only user:2", missing)
			}
			return map[string]user{"user:2": {ID: 2, Name: "two"}}, nil
		})
	if err != nil || len(got) != 2 || got["user:1"].ID != 1 || got["user:2"].ID != 2 {
		t.Fatalf("GetOrSetMany = %+v, %v", got, err)
	}

	// And the reverse: written (enveloped) by GetOrSetMany, read by GetOrSet.
	u, err := GetOrSet(ctx, c, "user:2", time.Minute, func(context.Context) (user, error) {
		t.Error("loader must not run — GetOrSetMany already cached user:2")
		return user{}, nil
	})
	if err != nil || u.ID != 2 {
		t.Fatalf("cross-read = %+v, %v", u, err)
	}
}
