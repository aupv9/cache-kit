package cachekit

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"time"
)

// The stale-while-revalidate and early-refresh features need metadata next
// to the cached value: when the entry stops being fresh, and how long the
// loader took (for probabilistic early refresh). When either feature is
// enabled, the GetOrSet helpers wrap payloads in a small binary envelope:
//
//	magic (5 bytes) | freshUntil unix-ms, int64 BE | loader delta ms, int64 BE | payload
//
// The magic starts with a NUL so an envelope can never be mistaken for a
// JSON value, and entries written without an envelope (older deploys, raw
// Set calls, other codecs) parse as "no metadata": always fresh until the
// backend TTL expires. The negative marker is checked before envelope
// parsing everywhere, so the two never interact.
var envelopeMagic = []byte{0x00, 'c', 'k', 'e', '1'}

const envelopeHeaderLen = 5 + 8 + 8

type envelope struct {
	freshUntil time.Time     // zero = fresh until backend TTL expiry
	delta      time.Duration // duration of the load that produced the entry
}

func wrapEnvelope(payload []byte, freshUntil time.Time, delta time.Duration) []byte {
	buf := make([]byte, envelopeHeaderLen, envelopeHeaderLen+len(payload))
	copy(buf, envelopeMagic)
	var fu int64
	if !freshUntil.IsZero() {
		fu = freshUntil.UnixMilli()
	}
	binary.BigEndian.PutUint64(buf[5:], uint64(fu))
	binary.BigEndian.PutUint64(buf[13:], uint64(delta.Milliseconds()))
	return append(buf, payload...)
}

// parseEnvelope splits stored bytes into metadata and payload. Bytes
// without the magic are a bare payload with no metadata.
func parseEnvelope(data []byte) (envelope, []byte) {
	if len(data) < envelopeHeaderLen || !bytes.HasPrefix(data, envelopeMagic) {
		return envelope{}, data
	}
	var e envelope
	if fu := int64(binary.BigEndian.Uint64(data[5:13])); fu != 0 {
		e.freshUntil = time.UnixMilli(fu)
	}
	e.delta = time.Duration(int64(binary.BigEndian.Uint64(data[13:21]))) * time.Millisecond
	return e, data[envelopeHeaderLen:]
}

// stale reports whether the entry is past its freshness window (but still
// within the backend TTL, or it wouldn't have been returned at all).
func (e envelope) stale(now time.Time) bool {
	return !e.freshUntil.IsZero() && now.After(e.freshUntil)
}

// shouldEarlyRefresh implements probabilistic early expiration ("XFetch",
// Vattani et al.): refresh when now + delta·beta·(-ln U) crosses the
// freshness deadline, U uniform in (0,1]. Refresh probability rises
// smoothly as expiry approaches, so a fleet of instances spreads its
// reloads instead of stampeding the database the moment a hot key
// expires — per-process single-flight can't help across instances, this
// can. Larger beta refreshes earlier; 1.0 is a sensible default.
func (e envelope) shouldEarlyRefresh(now time.Time, beta float64) bool {
	if beta <= 0 || e.freshUntil.IsZero() {
		return false
	}
	delta := e.delta
	if delta <= 0 {
		delta = time.Millisecond // unknown load cost: assume cheap
	}
	spread := time.Duration(float64(delta) * beta * -math.Log(rand.Float64()))
	return now.Add(spread).After(e.freshUntil)
}
