package cachekit

import "time"

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
}

// Option mutates Options in the functional-options style.
type Option func(*Options)

func WithAddr(addr string) Option        { return func(o *Options) { o.Addr = addr } }
func WithAuth(user, pass string) Option  { return func(o *Options) { o.Username, o.Password = user, pass } }
func WithDB(db int) Option               { return func(o *Options) { o.DB = db } }
func WithPrefix(prefix string) Option    { return func(o *Options) { o.Prefix = prefix } }
func WithDefaultTTL(d time.Duration) Option { return func(o *Options) { o.DefaultTTL = d } }
func WithCodec(c Codec) Option           { return func(o *Options) { o.Codec = c } }

func buildOptions(opts []Option) Options {
	o := Options{Addr: "localhost:6379"}
	for _, opt := range opts {
		opt(&o)
	}
	if o.Codec == nil {
		o.Codec = defaultCodec
	}
	return o
}
