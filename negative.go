package cachekit

import (
	"bytes"
	"errors"
)

// ErrNotFound signals "the entity does not exist at the source of truth" —
// distinct from ErrCacheMiss ("the cache has no entry"). Loaders return an
// error wrapping ErrNotFound to mean the lookup succeeded but found
// nothing; when the cache is configured with WithNegativeTTL, GetOrSet
// stores that fact so hot lookups of nonexistent entities stop hammering
// the database (cache penetration). Subsequent GetOrSet calls return
// ErrNotFound without invoking the loader until the negative entry expires.
var ErrNotFound = errors.New("cachekit: not found")

// negativeMarker is the stored representation of a negative entry. It can
// never collide with a JSON value (JSON never contains NUL) and is
// vanishingly unlikely to collide with other codecs' output; it is checked
// before any decode so it never triggers the decode self-heal path.
var negativeMarker = []byte("\x00cachekit:negative\x00")

func isNegativeEntry(data []byte) bool { return bytes.Equal(data, negativeMarker) }
