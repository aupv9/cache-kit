package cachekit

import (
	"context"
	"math/rand/v2"
	"time"
)

// Options configures a cache backend. Zero values are usable defaults:
// localhost:6379, no prefix, no default TTL, JSON codec.
type Options struct {
	// Addr is the host:port of the Redis/Valkey server.
	Addr string
	// Username and Password authenticate the connection (ACL or requirepass).
	Username string
	Password string
	// DB selects the logical database (ignored in cluster mode).
	DB int
	// Prefix is prepended to every key, typically "service-name:".
	// It namespaces keys so services can share one server safely.
	Prefix string
	// DefaultTTL applies when Set is called with ttl <= 0.
	// Zero means keys without an explicit TTL never expire.
	DefaultTTL time.Duration
	// Codec overrides the default JSON codec for GetOrSet helpers
	// bound to this cache (see RedisCache.Codec).
	Codec Codec
	// ClientSideCacheTTL enables server-assisted client-side caching on
	// backends that support it (currently ValkeyCache): reads are served
	// from a local LRU for at most this duration, with the server pushing
	// invalidations on writes. Zero disables it. Ignored by backends
	// without client-side caching.
	ClientSideCacheTTL time.Duration
	// Hooks receives hit/miss/error/loader events for metrics and logging.
	// See the Hooks type; the zero value disables everything.
	Hooks Hooks
	// TTLJitter spreads every stored TTL uniformly within ±(fraction ×
	// TTL), so keys written together don't all expire together (cache
	// avalanche). 0.1 means ±10%. Zero disables; values are capped at 0.9.
	TTLJitter float64
	// NegativeTTL enables negative caching in GetOrSet/GetOrSetMany: when
	// the loader reports ErrNotFound (or, for batch loads, omits a key),
	// that absence is cached for this duration and served as ErrNotFound
	// without re-invoking the loader. Zero disables negative caching.
	NegativeTTL time.Duration
	// OpTimeout bounds each individual cache operation (Get/Set/Delete/
	// batch/Ping) with its own deadline, so a slow cache can't hold
	// requests hostage — a slow cache is worse than a down one. It does
	// not bound GetOrSet loaders. Zero means no per-op deadline. Ignored
	// by MemoryCache.
	OpTimeout time.Duration
	// StaleTTL enables stale-while-revalidate in GetOrSet: after an
	// entry's freshness TTL passes, it stays servable for this long while
	// a deduplicated background refresh reloads it — hot keys never pay
	// loader latency on the request path. Entries are stored in a small
	// metadata envelope (see envelope.go). GetOrSetMany serves stale
	// entries but does not background-refresh them; they reload at hard
	// expiry (freshness TTL + StaleTTL). Zero disables.
	StaleTTL time.Duration
	// EarlyRefreshBeta enables probabilistic early refresh (XFetch): as
	// an entry approaches its freshness deadline, each GetOrSet hit
	// refreshes it in the background with a probability that rises with
	// proximity to expiry, scaled by the loader's observed duration.
	// Because every process rolls independently, a fleet spreads its
	// reloads instead of stampeding the database at expiry — this is the
	// cross-instance complement to per-process single-flight. 1.0 is a
	// sensible default; larger refreshes earlier. Zero disables.
	EarlyRefreshBeta float64
}

// Option mutates Options in the functional-options style.
type Option func(*Options)

func WithAddr(addr string) Option { return func(o *Options) { o.Addr = addr } }
func WithAuth(user, pass string) Option {
	return func(o *Options) { o.Username, o.Password = user, pass }
}
func WithDB(db int) Option                  { return func(o *Options) { o.DB = db } }
func WithPrefix(prefix string) Option       { return func(o *Options) { o.Prefix = prefix } }
func WithDefaultTTL(d time.Duration) Option { return func(o *Options) { o.DefaultTTL = d } }
func WithCodec(c Codec) Option              { return func(o *Options) { o.Codec = c } }
func WithClientSideCacheTTL(d time.Duration) Option {
	return func(o *Options) { o.ClientSideCacheTTL = d }
}
func WithHooks(h Hooks) Option { return func(o *Options) { o.Hooks = h } }
func WithTTLJitter(fraction float64) Option {
	return func(o *Options) { o.TTLJitter = fraction }
}
func WithNegativeTTL(d time.Duration) Option { return func(o *Options) { o.NegativeTTL = d } }
func WithOpTimeout(d time.Duration) Option   { return func(o *Options) { o.OpTimeout = d } }
func WithStaleTTL(d time.Duration) Option    { return func(o *Options) { o.StaleTTL = d } }
func WithEarlyRefresh(beta float64) Option {
	return func(o *Options) { o.EarlyRefreshBeta = beta }
}

func buildOptions(opts []Option) Options {
	o := Options{Addr: "localhost:6379"}
	for _, opt := range opts {
		opt(&o)
	}
	if o.Codec == nil {
		o.Codec = defaultCodec
	}
	if o.TTLJitter < 0 {
		o.TTLJitter = 0
	}
	if o.TTLJitter > 0.9 {
		o.TTLJitter = 0.9
	}
	if o.EarlyRefreshBeta < 0 {
		o.EarlyRefreshBeta = 0
	}
	return o
}

// applyJitter spreads ttl uniformly within ±(frac × ttl). No-op for
// non-positive ttl (no expiry / default already resolved) or frac.
func applyJitter(ttl time.Duration, frac float64) time.Duration {
	if ttl <= 0 || frac <= 0 {
		return ttl
	}
	delta := (rand.Float64()*2 - 1) * frac // uniform in [-frac, +frac)
	return time.Duration(float64(ttl) * (1 + delta))
}

// opContext bounds a single cache operation with its own deadline when
// the backend is configured with WithOpTimeout.
func opContext(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}
