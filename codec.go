package cachekit

import "encoding/json"

// Codec converts values to and from the raw bytes stored in the cache.
// Implementations must be safe for concurrent use.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONCodec encodes values as JSON. It is the package default: readable,
// schema-free, and good enough until profiling says otherwise (swap in
// msgpack/protobuf via WithCodec or GetOrSetWithCodec when it does).
type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

var defaultCodec Codec = JSONCodec{}
