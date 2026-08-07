# Local Valkey cluster on k3d

A sharded **Valkey 8** cluster (3 masters + 3 replicas, cluster mode) for
developing/running a service that uses `cache-kit`. Both the Valkey cluster and
the consuming app run **inside** the k3d cluster and talk over in-cluster DNS.

Storage is **ephemeral** (`emptyDir`, `save ""`, `appendonly no`): a full
StatefulSet restart wipes all data and the cluster must be re-formed. That's the
intended trade-off for a local cache.

## Layout

| File                  | What it is |
|-----------------------|------------|
| `k3d.yaml`            | k3d cluster `cache-kit`: 1 server + 2 agents |
| `valkey-cluster.yaml` | Namespace, ConfigMap, headless Service, 6-node StatefulSet |
| `bootstrap-job.yaml`  | Idempotent Job that runs `valkey-cli --cluster create` |
| `testapp/`            | A tiny Go service using cache-kit against the cluster (+ Dockerfile) |
| `testapp.yaml`        | Deployment + Service for the test app |
| `Makefile`            | `make up` / `down` / `verify` / `testapp` / `status` / `psh` |

## Quick start

```bash
cd deploy/k3d
make up        # create k3d cluster + Valkey cluster + bootstrap + verify
make status    # kubectl get pods
make psh       # open a cluster-mode valkey-cli shell
make down      # delete the whole k3d cluster
```

`make up` is safe to re-run; the bootstrap Job exits early if the cluster is
already `cluster_state:ok`.

## How the app connects (inside k3d)

Nodes announce their **stable pod hostnames** (`cluster-announce-hostname` +
`cluster-preferred-endpoint-type hostname`), so `CLUSTER SLOTS` returns DNS
names, not pod IPs. A single seed is enough — go-redis discovers the rest and
dials everything by hostname:

```go
import (
    "github.com/redis/go-redis/v9"
    cachekit "github.com/aupv9/cache-kit"
)

rdb := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs: []string{"valkey-cluster-headless.cache-kit.svc.cluster.local:6379"},
    // no password; in-cluster only
})
cache := cachekit.NewWithClient(rdb, cachekit.WithPrefix("app:"))
```

`redis.ClusterClient` implements `redis.UniversalClient`, which is exactly what
`cachekit.NewWithClient` expects — the cache-kit code is unchanged whether the
backend is single-node, sentinel, or cluster.

**Env-var convention** — inject the seed so it matches the repo's
`CACHEKIT_SERVER_ADDR` habit:

```yaml
env:
  - name: CACHEKIT_SERVER_ADDR
    value: valkey-cluster-headless.cache-kit.svc.cluster.local:6379
```

Deploy your app into the `cache-kit` namespace (or any namespace — just use the
fully-qualified `...cache-kit.svc.cluster.local` name shown above).

## Test app

`testapp/` is a real (tiny) service that imports cache-kit via a local
`replace`, connects with a go-redis cluster client, and exercises cache-aside
(`GetOrSet`) over keys spanning all three shards. It's the end-to-end proof that
an application pod can use the cluster.

```bash
make testapp        # docker build -> k3d image import -> deploy -> print logs
make testapp-demo   # run the cross-slot demo over HTTP and print slot per key
make testapp-logs   # follow logs
```

The image is built locally and side-loaded with `k3d image import` (no
registry), so the Deployment uses `imagePullPolicy: Never`. After editing the
app, re-run `make testapp` to rebuild, reload, and roll the Deployment.

HTTP surface (in-cluster, port 8080; Service `cachekit-testapp` on port 80):

| Path       | What |
|------------|------|
| `/healthz` | `cache.Ping` — used by the readiness/liveness probes |
| `/demo`    | Runs the cache-aside demo, returns each key's hash slot as JSON |
| `/stats`   | Cumulative hit / miss / loader counters (from cache-kit `Hooks`) |

To reach it from your Mac: `kubectl port-forward -n cache-kit deploy/cachekit-testapp 8080:8080` then `curl localhost:8080/demo`.

## Topology

```
StatefulSet valkey-cluster (6 pods, Parallel, emptyDir)
  valkey-cluster-0  master  slots 0-5460       <- replica: valkey-cluster-4
  valkey-cluster-1  master  slots 5461-10922   <- replica: valkey-cluster-5
  valkey-cluster-2  master  slots 10923-16383  <- replica: valkey-cluster-3
Service valkey-cluster-headless (clusterIP: None, publishNotReadyAddresses)
  -> valkey-cluster-<n>.valkey-cluster-headless.cache-kit.svc.cluster.local:6379
```

(Master↔replica pairing is assigned at bootstrap and may differ across runs.)

## Notes & gotchas

- **Do not expose to the host.** `protected-mode no` + no auth is fine for
  in-cluster only. Reaching a sharded cluster from your Mac needs
  `cluster-announce-ip`/per-node port mapping and is deliberately not set up
  here (you chose in-cluster access).
- **Restart = re-bootstrap.** Deleting/recreating pods loses `nodes.conf` (it's
  on `emptyDir`). Run `make bootstrap` again to reform.
- **Scaling shards** means re-sharding slots (`valkey-cli --cluster reshard`);
  just bumping StatefulSet `replicas` adds empty nodes that own no slots.
- **Redis instead of Valkey**: swap `image: valkey/valkey:8` for `redis:7` and
  `valkey-server`/`valkey-cli` for `redis-server`/`redis-cli` — the manifests
  are otherwise identical (both speak RESP).
```
