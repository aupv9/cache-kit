// Command testapp is a minimal service that uses cache-kit against the
// in-cluster Valkey cluster. It proves an application pod can connect via
// a go-redis cluster client, and that cache-aside (GetOrSet) works across
// hash slots. It exercises the cluster on startup and also exposes an HTTP
// surface (/healthz, /stats, /demo) for on-demand checks.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	cachekit "github.com/aupv9/cache-kit"
	"github.com/redis/go-redis/v9"
)

// User is the toy value we cache; JSON is cache-kit's default codec.
type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// demoKeys deliberately span multiple hash slots (and therefore shards).
var demoKeys = []string{
	"user:1", "user:2", "user:42",
	"session:abc", "session:xyz",
	"product:100", "cart:7", "flag:beta",
}

const prefix = "testapp:"

func seeds() []string {
	addr := os.Getenv("CACHEKIT_SERVER_ADDR")
	if addr == "" {
		addr = "valkey-cluster-headless.cache-kit.svc.cluster.local:6379"
	}
	return strings.Split(addr, ",")
}

var (
	hits, misses, loads atomic.Int64
)

func main() {
	addrs := seeds()
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})

	hooks := cachekit.Hooks{
		OnHit:  func(string) { hits.Add(1) },
		OnMiss: func(string) { misses.Add(1) },
		OnLoad: func(k string, d time.Duration, err error) {
			loads.Add(1)
			log.Printf("LOAD  key=%s dur=%s err=%v", k, d.Round(time.Millisecond), err)
		},
		OnError: func(op cachekit.Op, k string, err error) {
			log.Printf("ERROR op=%s key=%s err=%v", op, k, err)
		},
	}

	cache := cachekit.NewWithClient(rdb,
		cachekit.WithPrefix(prefix),
		cachekit.WithDefaultTTL(5*time.Minute),
		cachekit.WithHooks(hooks),
	)

	ctx := context.Background()
	waitReady(ctx, cache)
	log.Printf("connected to Valkey cluster via seeds=%v", addrs)
	logTopology(ctx, rdb)
	runDemo(ctx, cache, rdb) // startup proof; logs LOAD on first pass, hits on second

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := cache.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]int64{
			"hits": hits.Load(), "misses": misses.Load(), "loads": loads.Load(),
		})
	})
	mux.HandleFunc("/demo", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, runDemo(r.Context(), cache, rdb))
	})

	log.Println("listening on :8080 (/healthz /stats /demo)")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

// waitReady blocks until the cluster answers PING — the app may start before
// the bootstrap Job has finished forming the cluster.
func waitReady(ctx context.Context, cache *cachekit.RedisCache) {
	for i := 1; ; i++ {
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := cache.Ping(cctx)
		cancel()
		if err == nil {
			return
		}
		log.Printf("waiting for cluster (attempt %d): %v", i, err)
		time.Sleep(2 * time.Second)
	}
}

func logTopology(ctx context.Context, rdb *redis.ClusterClient) {
	s, err := rdb.ClusterNodes(ctx).Result()
	if err != nil {
		log.Printf("CLUSTER NODES failed: %v", err)
		return
	}
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		f := strings.Fields(line)
		if len(f) >= 3 {
			log.Printf("topology: %-55s %s", f[1], f[2]) // addr@bus,hostname  role
		}
	}
}

type keyResult struct {
	Key   string `json:"key"`
	Slot  int64  `json:"slot"`
	Value User   `json:"value"`
}

// runDemo caches every demoKey through GetOrSet (miss -> loader -> store) and
// returns which slot each landed on. A second pass must be all hits: its
// loader errors if ever called, which would surface via OnError.
func runDemo(ctx context.Context, cache *cachekit.RedisCache, rdb *redis.ClusterClient) map[string]any {
	results := make([]keyResult, 0, len(demoKeys))
	for i, k := range demoKeys {
		id := i + 1
		u, err := cachekit.GetOrSet(ctx, cache, k, time.Minute, func(context.Context) (User, error) {
			return User{ID: id, Name: "name-of-" + k}, nil
		})
		if err != nil {
			log.Printf("GetOrSet %s error: %v", k, err)
			continue
		}
		slot, _ := rdb.ClusterKeySlot(ctx, prefix+k).Result()
		results = append(results, keyResult{Key: k, Slot: slot, Value: u})
	}
	for _, k := range demoKeys {
		if _, err := cachekit.GetOrSet(ctx, cache, k, time.Minute, func(context.Context) (User, error) {
			return User{}, fmt.Errorf("loader must not run on a hit for %s", k)
		}); err != nil {
			log.Printf("second-pass %s error: %v", k, err)
		}
	}
	log.Printf("demo: cached %d keys across slots; totals hits=%d misses=%d loads=%d",
		len(results), hits.Load(), misses.Load(), loads.Load())
	return map[string]any{"count": len(results), "keys": results}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
