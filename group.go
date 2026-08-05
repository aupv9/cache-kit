package cachekit

import (
	"context"
	"fmt"

	"golang.org/x/sync/singleflight"
)

// loadGroup deduplicates concurrent loader calls in GetOrSet: when many
// goroutines miss on the same (cache, key) at once, only one executes the
// loader and the rest share its result. This prevents a thundering herd
// against the database on hot-key expiry.
//
// Note: valkey-go ships an equivalent mechanism built into its
// client-side cache, but it dedupes GETs to the *server*; this group
// dedupes loader (database) calls, so it applies to every backend.
var loadGroup group

type group struct {
	sf singleflight.Group
}

// do runs fn once per in-flight key, returning the shared result to all
// concurrent callers. Waiters select on their own context: a canceled
// waiter returns ctx.Err() immediately, while the flight keeps running
// for the others (and to populate the cache). The result is forgotten
// once the call completes so later misses trigger a fresh load.
func (g *group) do(ctx context.Context, key string, fn func() ([]byte, error)) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-g.start(key, fn):
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.([]byte), nil
	}
}

// doAsync starts (or joins) a flight without waiting for its result —
// used for background refreshes. The result channel is buffered, so
// nothing leaks when nobody reads it.
func (g *group) doAsync(key string, fn func() ([]byte, error)) {
	g.start(key, fn)
}

func (g *group) start(key string, fn func() ([]byte, error)) <-chan singleflight.Result {
	return g.sf.DoChan(key, func() (v any, err error) {
		defer g.sf.Forget(key)
		// A panicking fn under DoChan is re-panicked by singleflight on a
		// fresh goroutine, where nothing can recover it — a loader panic
		// would kill the process. Convert it to an error instead.
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("cachekit: loader panic: %v", r)
			}
		}()
		return fn()
	})
}
