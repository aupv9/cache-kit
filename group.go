package cachekit

import "golang.org/x/sync/singleflight"

// loadGroup deduplicates concurrent loader calls in GetOrSet: when many
// goroutines miss on the same (cache, key) at once, only one executes the
// loader and the rest share its result. This prevents a thundering herd
// against the database on hot-key expiry.
//
// Note: valkey-go ships an equivalent mechanism built into its
// client-side cache; if a valkey-go backend is added, its GetOrSet path
// can bypass this group.
var loadGroup group

type group struct {
	sf singleflight.Group
}

// do runs fn once per in-flight key, returning the shared result to all
// concurrent callers. The result is forgotten immediately after the call
// completes so later misses trigger a fresh load.
func (g *group) do(key string, fn func() ([]byte, error)) ([]byte, error) {
	v, err, _ := g.sf.Do(key, func() (any, error) {
		defer g.sf.Forget(key)
		return fn()
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}
