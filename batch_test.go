package cachekit

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestBatchConformance(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("GetManySetManyRoundTrip", func(t *testing.T) {
				c, _ := b.make(t)
				bc, ok := c.(BatchCache)
				if !ok {
					t.Fatalf("%T must implement BatchCache", c)
				}
				ctx := context.Background()

				items := map[string][]byte{
					"k1": []byte("v1"),
					"k2": []byte("v2\x00bin"),
				}
				if err := bc.SetMany(ctx, items, time.Minute); err != nil {
					t.Fatalf("SetMany: %v", err)
				}
				got, err := bc.GetMany(ctx, []string{"k1", "k2", "absent"})
				if err != nil {
					t.Fatalf("GetMany: %v", err)
				}
				if len(got) != 2 || string(got["k1"]) != "v1" || string(got["k2"]) != "v2\x00bin" {
					t.Fatalf("GetMany = %q", got)
				}
				if _, ok := got["absent"]; ok {
					t.Fatal("missing key present in GetMany result")
				}
			})

			t.Run("GetManyEmpty", func(t *testing.T) {
				c, _ := b.make(t)
				got, err := c.(BatchCache).GetMany(context.Background(), nil)
				if err != nil || len(got) != 0 {
					t.Fatalf("GetMany(nil) = %v, %v", got, err)
				}
			})
		})
	}
}

func TestGetOrSetManyLoadsOnlyMissing(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	// Preload user:1 through the same helper so encodings match.
	if _, err := GetOrSetMany(ctx, c, []string{"user:1"}, time.Minute,
		func(_ context.Context, missing []string) (map[string]user, error) {
			return map[string]user{"user:1": {ID: 1, Name: "one"}}, nil
		}); err != nil {
		t.Fatalf("preload: %v", err)
	}

	var loaderKeys []string
	got, err := GetOrSetMany(ctx, c, []string{"user:1", "user:2", "user:3", "user:2"}, time.Minute,
		func(_ context.Context, missing []string) (map[string]user, error) {
			loaderKeys = slices.Clone(missing)
			return map[string]user{
				"user:2": {ID: 2, Name: "two"},
				"user:3": {ID: 3, Name: "three"},
			}, nil
		})
	if err != nil {
		t.Fatalf("GetOrSetMany: %v", err)
	}
	slices.Sort(loaderKeys)
	if !slices.Equal(loaderKeys, []string{"user:2", "user:3"}) {
		t.Fatalf("loader got %v, want only the missing (deduplicated) keys", loaderKeys)
	}
	if len(got) != 3 || got["user:1"].Name != "one" || got["user:2"].Name != "two" || got["user:3"].Name != "three" {
		t.Fatalf("result = %+v", got)
	}

	// Everything is cached now: a second call must not hit the loader.
	got2, err := GetOrSetMany(ctx, c, []string{"user:1", "user:2", "user:3"}, time.Minute,
		func(_ context.Context, missing []string) (map[string]user, error) {
			t.Errorf("loader called for %v after full cache", missing)
			return nil, nil
		})
	if err != nil || len(got2) != 3 {
		t.Fatalf("second call: %+v, %v", got2, err)
	}
}

func TestGetOrSetManyLoaderErrorPropagates(t *testing.T) {
	c, _ := newTestCache(t)
	sentinel := errors.New("db down")

	_, err := GetOrSetMany(context.Background(), c, []string{"a", "b"}, time.Minute,
		func(context.Context, []string) (map[string]user, error) {
			return nil, sentinel
		})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
	if _, err := c.Get(context.Background(), "a"); !IsMiss(err) {
		t.Fatalf("key cached after loader error: %v", err)
	}
}

func TestGetOrSetManyNegativeCaching(t *testing.T) {
	c, _ := newTestCache(t, WithNegativeTTL(time.Minute))
	ctx := context.Background()

	calls := 0
	loader := func(_ context.Context, missing []string) (map[string]user, error) {
		calls++
		out := map[string]user{}
		for _, k := range missing {
			if k == "user:1" { // only user:1 exists
				out[k] = user{ID: 1, Name: "one"}
			}
		}
		return out, nil
	}

	got, err := GetOrSetMany(ctx, c, []string{"user:1", "ghost"}, time.Minute, loader)
	if err != nil || len(got) != 1 {
		t.Fatalf("first call: %+v, %v", got, err)
	}

	// ghost's absence is now cached: second call must not load anything.
	got, err = GetOrSetMany(ctx, c, []string{"user:1", "ghost"}, time.Minute, loader)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if _, ok := got["ghost"]; ok {
		t.Fatalf("ghost appeared: %+v", got)
	}
	if calls != 1 {
		t.Fatalf("loader called %d times, want 1 (absence must be cached)", calls)
	}
}

// failingCodec wraps JSONCodec but refuses to marshal users named "bad".
type failingCodec struct{ JSONCodec }

func (failingCodec) Marshal(v any) ([]byte, error) {
	if u, ok := v.(user); ok && u.Name == "bad" {
		return nil, errors.New("unmarshalable value")
	}
	return JSONCodec{}.Marshal(v)
}

func TestGetOrSetManyEncodeFailureSkipsCachingOnly(t *testing.T) {
	rec := &hookCounters{}
	c, _ := newTestCache(t, WithHooks(rec.hooks()))
	ctx := context.Background()

	got, err := GetOrSetManyWithCodec(ctx, c, []string{"good", "bad"}, time.Minute, failingCodec{},
		func(_ context.Context, missing []string) (map[string]user, error) {
			return map[string]user{
				"good": {ID: 1, Name: "good"},
				"bad":  {ID: 2, Name: "bad"},
			}, nil
		})
	if err != nil {
		t.Fatalf("GetOrSetMany: %v", err)
	}
	// The caller still gets both values; only caching is skipped for "bad".
	if len(got) != 2 || got["bad"].ID != 2 {
		t.Fatalf("result = %+v", got)
	}
	if !hasOp(rec.errOps(), OpEncode) {
		t.Fatalf("OnError(OpEncode) not fired: %v", rec.errOps())
	}
	if _, err := c.Get(ctx, "good"); err != nil {
		t.Fatalf("good not cached: %v", err)
	}
	if _, err := c.Get(ctx, "bad"); !IsMiss(err) {
		t.Fatalf("unencodable value ended up cached: %v", err)
	}
}

func TestGetOrSetManyWritesSurviveCallerCancel(t *testing.T) {
	c, _ := newTestCache(t)
	ctx, cancel := context.WithCancel(context.Background())

	got, err := GetOrSetMany(ctx, c, []string{"user:1"}, time.Minute,
		func(_ context.Context, missing []string) (map[string]user, error) {
			cancel() // caller gives up right as the load completes
			return map[string]user{"user:1": {ID: 1, Name: "kept"}}, nil
		})
	if err != nil || got["user:1"].ID != 1 {
		t.Fatalf("GetOrSetMany: %+v, %v", got, err)
	}
	// The write phase runs detached from the canceled context.
	if _, err := c.Get(context.Background(), "user:1"); err != nil {
		t.Fatalf("loaded value not cached after caller cancel: %v", err)
	}
}

func TestGetOrSetManyDecodeFailureSelfHeals(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()

	if err := c.Set(ctx, "user:1", []byte("{corrupt"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := GetOrSetMany(ctx, c, []string{"user:1"}, time.Minute,
		func(_ context.Context, missing []string) (map[string]user, error) {
			if !slices.Equal(missing, []string{"user:1"}) {
				t.Errorf("corrupt key not passed to loader: %v", missing)
			}
			return map[string]user{"user:1": {ID: 1, Name: "healed"}}, nil
		})
	if err != nil || got["user:1"].Name != "healed" {
		t.Fatalf("GetOrSetMany over corrupt entry: %+v, %v", got, err)
	}
}
