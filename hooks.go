package cachekit

import "time"

// Op identifies which cache operation an OnError hook fires for.
type Op string

const (
	OpGet    Op = "get"
	OpSet    Op = "set"
	OpDelete Op = "delete"
	// OpDecode fires when a cached entry fails to decode (corrupted or
	// written by an incompatible schema version). GetOrSet self-heals by
	// deleting the entry and falling back to the loader.
	OpDecode Op = "decode"
)

// Hooks receives cache events, for wiring metrics (hit ratio, error
// counters, loader latency) and logging. The zero value is valid and
// disables everything; individual callbacks may be nil.
//
// Callbacks run synchronously on the request path and may be invoked
// concurrently — keep them fast and thread-safe (atomic counters, not
// blocking I/O).
type Hooks struct {
	// OnHit / OnMiss fire on every Get, keyed by the logical key.
	OnHit  func(key string)
	OnMiss func(key string)
	// OnError fires for every backend failure — including errors cachekit
	// swallows by design, such as the best-effort Set after a load in
	// GetOrSet. Wire this to a counter or a degraded cache stays invisible.
	OnError func(op Op, key string, err error)
	// OnLoad fires after every GetOrSet loader invocation with its
	// duration and outcome. Loader calls are the cache's whole reason to
	// exist — this is the number to alert on.
	OnLoad func(key string, dur time.Duration, err error)
}

func (h Hooks) hit(key string) {
	if h.OnHit != nil {
		h.OnHit(key)
	}
}

func (h Hooks) miss(key string) {
	if h.OnMiss != nil {
		h.OnMiss(key)
	}
}

func (h Hooks) error(op Op, key string, err error) {
	if h.OnError != nil {
		h.OnError(op, key, err)
	}
}

func (h Hooks) load(key string, dur time.Duration, err error) {
	if h.OnLoad != nil {
		h.OnLoad(key, dur, err)
	}
}

// hooked is implemented by backends so GetOrSet can report loader and
// decode events through the same Hooks the backend was configured with.
type hooked interface {
	cacheHooks() Hooks
}

// hooksOf returns c's configured Hooks, or a no-op zero value for
// third-party Cache implementations.
func hooksOf(c Cache) Hooks {
	if h, ok := c.(hooked); ok {
		return h.cacheHooks()
	}
	return Hooks{}
}
