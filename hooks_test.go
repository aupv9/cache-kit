package cachekit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// hookCounters is a thread-safe Hooks recorder for tests.
type hookCounters struct {
	hits, misses, loads atomic.Int32
	mu                  sync.Mutex
	errs                []Op
}

func (r *hookCounters) hooks() Hooks {
	return Hooks{
		OnHit:  func(string) { r.hits.Add(1) },
		OnMiss: func(string) { r.misses.Add(1) },
		OnError: func(op Op, _ string, _ error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.errs = append(r.errs, op)
		},
		OnLoad: func(_ string, _ time.Duration, _ error) { r.loads.Add(1) },
	}
}

func (r *hookCounters) errOps() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Op(nil), r.errs...)
}

func hasOp(ops []Op, want Op) bool {
	for _, op := range ops {
		if op == want {
			return true
		}
	}
	return false
}

func TestHooksCountHitsAndMisses(t *testing.T) {
	rec := &hookCounters{}
	c, _ := newTestCache(t, WithHooks(rec.hooks()))
	ctx := context.Background()

	if _, err := c.Get(ctx, "absent"); !IsMiss(err) {
		t.Fatalf("Get(absent): %v", err)
	}
	if err := c.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := c.Get(ctx, "k"); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got := rec.misses.Load(); got != 1 {
		t.Fatalf("misses = %d, want 1", got)
	}
	if got := rec.hits.Load(); got != 1 {
		t.Fatalf("hits = %d, want 1", got)
	}
	if len(rec.errOps()) != 0 {
		t.Fatalf("unexpected errors: %v", rec.errOps())
	}
}

// The best-effort Set in GetOrSet is swallowed by design; Hooks.OnError is
// the only place a degraded cache becomes visible. This test guards that.
func TestHooksSurfaceSwallowedSetError(t *testing.T) {
	rec := &hookCounters{}
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	c := NewWithClient(client, WithHooks(rec.hooks()))
	mr.Close() // outage: Get and Set both fail

	u, err := GetOrSet(context.Background(), c, "user:1", time.Minute, func(context.Context) (user, error) {
		return user{ID: 1, Name: "an"}, nil
	})
	if err != nil || u.ID != 1 {
		t.Fatalf("GetOrSet = %+v, %v", u, err)
	}

	ops := rec.errOps()
	if !hasOp(ops, OpGet) {
		t.Fatalf("OnError not fired for failed Get: %v", ops)
	}
	if !hasOp(ops, OpSet) {
		t.Fatalf("swallowed Set error not surfaced via OnError: %v", ops)
	}
	if got := rec.loads.Load(); got != 1 {
		t.Fatalf("OnLoad fired %d times, want 1", got)
	}
}

func TestGetOrSetDecodeFailureSelfHeals(t *testing.T) {
	rec := &hookCounters{}
	c, _ := newTestCache(t, WithHooks(rec.hooks()))
	ctx := context.Background()

	// Simulate a corrupted / incompatible-schema entry.
	if err := c.Set(ctx, "user:1", []byte("{not json"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	calls := 0
	u, err := GetOrSet(ctx, c, "user:1", time.Minute, func(context.Context) (user, error) {
		calls++
		return user{ID: 1, Name: "healed"}, nil
	})
	if err != nil {
		t.Fatalf("GetOrSet over corrupted entry: %v", err)
	}
	if calls != 1 || u.Name != "healed" {
		t.Fatalf("loader calls = %d, value = %+v", calls, u)
	}
	if !hasOp(rec.errOps(), OpDecode) {
		t.Fatalf("OnError(OpDecode) not fired: %v", rec.errOps())
	}

	// The corrupted entry must be gone: a second GetOrSet hits the fresh one.
	u2, err := GetOrSet(ctx, c, "user:1", time.Minute, func(context.Context) (user, error) {
		calls++
		return user{}, errors.New("must not be called")
	})
	if err != nil || u2 != u || calls != 1 {
		t.Fatalf("second GetOrSet: %+v, %v, calls = %d", u2, err, calls)
	}
}

// singleflight's DoChan re-panics a panicking fn on a fresh goroutine,
// which no caller can recover — group.do must convert loader panics to
// errors or one bad loader takes down the whole process.
func TestGetOrSetLoaderPanicBecomesError(t *testing.T) {
	c, _ := newTestCache(t)

	_, err := GetOrSet(context.Background(), c, "boom", time.Minute, func(context.Context) (user, error) {
		panic("loader exploded")
	})
	if err == nil || !strings.Contains(err.Error(), "loader panic") {
		t.Fatalf("err = %v, want loader panic converted to error", err)
	}

	// The failed flight must not be cached and must not wedge the key.
	u, err := GetOrSet(context.Background(), c, "boom", time.Minute, func(context.Context) (user, error) {
		return user{ID: 5, Name: "recovered"}, nil
	})
	if err != nil || u.ID != 5 {
		t.Fatalf("key wedged after panic: %+v, %v", u, err)
	}
}

func TestGetOrSetWaiterCancelDoesNotAbortFlight(t *testing.T) {
	c, _ := newTestCache(t)
	bg := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	load := func(context.Context) (user, error) {
		calls.Add(1)
		close(started)
		<-release
		return user{ID: 7, Name: "shared"}, nil
	}

	// First caller initiates the flight.
	firstDone := make(chan error, 1)
	go func() {
		_, err := GetOrSet(bg, c, "hot", time.Minute, load)
		firstDone <- err
	}()
	<-started

	// Second caller joins the flight, then cancels: it must return
	// promptly with ctx.Err() while the flight keeps running.
	ctx2, cancel2 := context.WithCancel(bg)
	waiterDone := make(chan error, 1)
	go func() {
		_, err := GetOrSet(ctx2, c, "hot", time.Minute, func(context.Context) (user, error) {
			t.Error("waiter must join the existing flight, not load")
			return user{}, nil
		})
		waiterDone <- err
	}()
	time.Sleep(20 * time.Millisecond) // let the waiter join the flight
	cancel2()

	select {
	case err := <-waiterDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled waiter did not return while flight was in progress")
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first caller: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader ran %d times, want 1", got)
	}
}

func TestGetOrSetLoaderSurvivesInitiatorCancel(t *testing.T) {
	c, _ := newTestCache(t)

	started := make(chan struct{})
	release := make(chan struct{})
	load := func(lctx context.Context) (user, error) {
		close(started)
		<-release
		if err := lctx.Err(); err != nil {
			return user{}, err // would prove cancellation leaked into the flight
		}
		return user{ID: 9, Name: "detached"}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := GetOrSet(ctx, c, "hot", time.Minute, load)
		done <- err
	}()
	<-started
	cancel() // initiator gives up mid-flight

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("initiator err = %v, want context.Canceled", err)
	}

	// The detached flight must still complete and populate the cache.
	close(release)
	deadline := time.After(2 * time.Second)
	for {
		if data, err := c.Get(context.Background(), "hot"); err == nil {
			if string(data) == "" {
				t.Fatal("cached empty value")
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("flight result never reached the cache after initiator cancel")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
