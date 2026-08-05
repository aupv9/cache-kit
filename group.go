package cachekit

import (
	"context"

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
	ch := g.sf.DoChan(key, func() (any, error) {
		defer g.sf.Forget(key)
		return fn()
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.([]byte), nil
	}
}
