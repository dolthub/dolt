# Fetch Performance Investigation — Notes

Working notes from a session investigating why `dolt fetch` ingresses
roughly 2× the source database size, after the new `StreamChunkLocations`
RPC landed. All instrumentation and tunables described here live behind
env vars on the `aaron/temp-fetch-test` branch and are not intended to be
shipped as-is.

The test workload was a clone-style fetch of a ~526 MiB database against
a CloudFront-fronted S3 bucket using HTTP/2 (`USE_HTTP2=1`). All numbers
below are from that single workload; results will differ on other
workloads (one-shot clones, GC compaction, sparse cross-commit reuse,
etc.), and "wasted effort and memory" tradeoffs depend on the workload.

## Instrumentation Built

A new `netstats` package
(`go/libraries/doltcore/remotestorage/netstats/`) records counters at
four layers, gated on `DOLT_NETSTATS=1`. Dumps to stderr on process
exit, on `SIGUSR1` (unix), and periodically if
`DOLT_NETSTATS_INTERVAL=<duration>` is set.

| Layer | What it measures | How |
|---|---|---|
| TCP | per-addr conns, in/out raw bytes | wraps `net.Dialer.DialContext`; `net.Conn` Read/Write counted |
| HTTP | per-host reqs, req/resp header & body bytes, status codes | `http.RoundTripper` wrapper around the dial provider's HTTPFetcher |
| gRPC (client) | per-method calls, payload bytes (length and wire), header & trailer wire bytes | `grpc.WithStatsHandler` |
| Fetch | dispatched regions, dispatched dark bytes, shadow-cache hit rate | `RecordDispatchedDarkBytes` / `RecordDispatchedRegion` / `CheckDarkCacheHit` from `chunk_fetcher.go` |

Wired in
`go/libraries/doltcore/env/grpc_dial_provider.go:GetGRPCDialParams`,
which composes a `newHTTPClient()` factory used either once (normal
fetcher) or repeatedly (PerURLFetcher).

## Tunables Added

All read once at package init from environment variables. Invalid values
warn on stderr and fall back to defaults.

| Env var | Default | Effect |
|---|---|---|
| `DOLT_NETSTATS` | unset | Enables the counters above. |
| `DOLT_NETSTATS_INTERVAL` | unset | If set to a Go duration, periodic stderr dump. |
| `DOLT_CHUNK_AGG_DISTANCE` | 8192 | Bytes; max gap to bridge when coalescing chunk byte ranges into a single HTTP Range. |
| `DOLT_MAX_CONCURRENT_DOWNLOADS` | 64 | Ceiling for the AIMD concurrency controller. |
| `DOLT_STARTING_CONCURRENT_DOWNLOADS` | 64 | Initial pool size. Clamped to max with a warning. |
| `DOLT_PER_URL_FETCHER` | unset | Enables `PerURLFetcher` — one `*http.Client` / connection pool per URL-path, keyed by `scheme+host+path` so presigned-URL refreshes share an entry. |
| `DOLT_PER_URL_FETCHER_CACHE` | 1024 | LRU cap on PerURLFetcher; on eviction, the Transport's `CloseIdleConnections` runs. |
| `DOLT_HEAP_STRATEGY` | `largest` | Region-dispatch order. Values: `largest`, `smallest`, `random`, `goodput` (= `largest_goodput`). |

For investigation, **always pin concurrency** with
`DOLT_STARTING_CONCURRENT_DOWNLOADS == DOLT_MAX_CONCURRENT_DOWNLOADS`
to remove AIMD as a confounding variable.

## Major Findings

### 1. The 2× ingress was range coalescing overhead.

Default config (`chunk_agg=8K`, `concurrency=64`) downloaded 1.88 GiB of
HTTP body for a 526 MiB DB — ~3.66× amplification. The math: at 8 KiB
gap-bridging with typical NBS chunks of ~4 KiB, each coalesced range
carries roughly equal parts wanted and dark bytes. Lowering
`chunk_agg_distance` to 512 B reduced body to ~857 MiB at the cost of
~3× more HTTP requests.

Below ~512 B, returns diminish — chunks are already packed tightly
enough in NBS table files that further reductions don't merge fewer
neighbors, they just stop bridging gaps that aren't there.

### 2. HPACK was not compressing the presigned URLs at all.

CloudFront advertises the RFC default `SETTINGS_HEADER_TABLE_SIZE`
(4 KiB). Each presigned URL has ~700–900 bytes of `:path` (signature,
date, expiry params). With ~250 unique URLs in rotation and 4 KiB of
table holding ~5 URL entries, every request was sending its `:path` as
a Huffman-coded literal — about 600 bytes per request on the wire.

At 243k requests, that's ~145 MiB of header overhead alone, and the
shared-pool `http.Transport` made it worse: under high concurrency, many
simultaneous connections each paid HPACK priming separately.

### 3. PerURLFetcher fixes HPACK by isolation.

`PerURLFetcher` (`go/libraries/doltcore/remotestorage/per_url_fetcher.go`)
keeps a separate `*http.Client` / `*http.Transport` per URL-path.
Connections in each pool see exactly one `:path` value, which HPACK
indexes after the first request and serves from the dynamic table as a
1-byte reference thereafter.

The result: per-request wire egress dropped from ~1130 B to ~117 B
(~10×). Total CDN egress dropped from 314 MiB to 39 MiB on the
representative `1024/256` config. As a side effect, **5xx errors at high
concurrency disappeared** — at `1024` concurrency, the no-PerURL run
saw 3× 503s; PerURLFetcher saw 0. The "CloudFront is throttling at high
concurrency" theory was partly wrong: the cause was cold-HPACK churn
making each conn expensive to serve, not raw conn count. Once HPACK
warms and stays warm, CloudFront is happy.

PerURLFetcher does increase total connection count (~250 vs. ~33 at
moderate concurrency) since pools no longer share. Memory cost is tiny.

### 4. Goodput dispatch order modestly improves coalescing.

`HeapStrategy_largestGoodput` orders the region heap by
`Region.MatchedBytes` (sum of wanted chunk bytes inside the region)
instead of total span. The effect is not on bytes-actually-downloaded
in any single dispatch — it changes which regions sit in the tree
longer. Sparser regions with high dark-to-matched ratio stay queued, so
later-arriving chunks have more time to coalesce into them.

On the test workload at concurrency=256, `goodput` reduced
dispatched-dark-bytes by ~9% vs. `largest` (504 → 458 MiB). At
concurrency=64 the absolute reductions were larger because pre-dispatch
coalescing was already much more effective: 504 → 266 MiB.

### 5. Lower concurrency dramatically improves coalescing.

With `goodput @ 64` vs. `largest @ 256` on the test workload:
- Dispatched regions: 264k → 105k (60% fewer)
- Dispatched dark bytes: 504 → 266 MiB (47% less)
- Total body bytes: ~1030 → ~792 MiB

The mechanism: a slower coordinator loop spends more time accumulating
ranges between dispatches, so the tree grows and coalesces before
regions get pulled out. The cost is wall time and worse pipe utilization
at the fetch tail (see open questions).

### 6. Dark-cache hit-rate measurement gives a realistic savings ceiling.

A shadow LRU of dispatched `(url, start, end)` spans + a containment
check on every chunk arrival in the coalescer measures how many bytes
would be served by a real dark-range cache.

On `largest @ 256`: 220 MiB hit-bytes (44.5% hit rate) — chunks that
later re-enter the coalescer for byte ranges already covered by a prior
dispatch. That's the realistic caching ceiling: ~20–25% reduction in
body bytes if a real cache held every dispatched response.

On `goodput @ 64`: 136 MiB hit-bytes (27.6% hit rate). The cache
opportunity *itself* shrinks under aggressive coalescing — the chunks
that would have been re-fetched were already absorbed into earlier
dispatches.

The shadow cache also reports total span coverage (~700 MiB across 14
URLs/table files for this workload) which sizes a real cache: keeping
all hits requires holding ~700 MiB resident, which is a lot for a
one-shot fetch.

## Canonical Run Comparison

All runs against the same database (HTTP/2 to CloudFront, PerURLFetcher
unless noted, pinned concurrency).

| config | time | TCP in | resp body | reqs | conns | reqs/conn | dispatched_regions | dispatched_dark | hit_bytes |
|---|---|---|---|---|---|---|---|---|---|
| `8K/64` baseline (no PerURL) | 3m39 | 1.98 GiB | 1.88 GiB | 77k | 124 | 620 | — | — | — |
| `512/1024` no PerURL | 3m18 | 1.36 GiB | 982 MiB | 617k | 1,914 | 322 | — | — | — |
| `512/128` no PerURL | 5m49 | 1.02 GiB | 857 MiB | 243k | 33 | 7,359 | — | — | — |
| `1024/256` PerURL | 3m43 | 1.43 GiB | 1.19 GiB | 350k | 217 | — | — | — | — |
| `1024/1024` PerURL | 3m22 | 1.57 GiB | 970 MiB | 992k | 2,678 | — | — | — | — |
| `1024/256` PerURL + dark stats | — | — | — | — | — | — | 264,354 | 504.5 MiB | 220.0 MiB |
| `1024/256` goodput | — | — | — | — | — | — | 245,431 | 457.7 MiB | 205.9 MiB |
| `1024/64` goodput | — | — | ~792 MiB | — | — | — | 105,252 | 266.1 MiB | 136.2 MiB |

Run-to-run variance is large at fixed config: same `chunk_agg`,
`concurrency`, and PerURLFetcher setting can produce ±25% on
request count and body bytes because chunk arrival order drives
coalescing decisions. The `checks=3,359,050` count (graph-walk-deterministic)
is identical across same-state runs.

## Open Questions and Future Work

### A. Byte-budget concurrency controller.

**Motivation:** Request-count concurrency is a poor proxy for BDP
saturation when range sizes vary by orders of magnitude. 256 in-flight
4 KiB requests = 1 MiB outstanding ≈ link-idle; 256 in-flight 16 MiB
requests = 4 GiB outstanding ≈ link-saturated for many seconds. With
HTTP/2 multiplexing, the "right" concurrency is "outstanding bytes
≈ bandwidth × RTT", not stream count.

**Sketch:** Replace the AIMD count-based controller with a byte budget.
Issue the next request when `sum(pending request Length) < budget`.
Budget can be static (env-var) or auto-tuned from observed throughput.
Naturally composes with all heap strategies. Sidesteps the AIMD-on-
small-tail-requests pathology where count-concurrency drops without
the controller noticing.

This is the single biggest design change worth considering and would
also obviate most reasons to tune concurrency manually.

### B. DAG-position-aware scheduling.

**Motivation:** A small uncoalesced range that holds a high-DAG-position
chunk (an internal Merkle node) is much more valuable to fetch early
than a leaf chunk of the same size — its bytes reveal references to
many more chunks, expanding the fetcher's work queue and keeping
concurrency saturated. Current scheduling has no such notion.

**Sketch:** Tag chunks by depth or known-internal status when locations
arrive (server may already know this; if not, we can usually infer for
prolly tree internals via chunk size or content peek). Add a tiebreaker
or weight to the heap that prefers high-position chunks. Likely
interacts well with the byte-budget controller — the system can spend
small-budget cycles eagerly on internal-node fetches at low cost.

### C. Real dark-range cache.

**Motivation:** The shadow cache sized the realistic ceiling at ~220
MiB savings on the test workload (~20–25% body reduction). Different
workloads may show wildly different hit rates — GC compaction or
cold-clone fetches are likely to see ~0%; warm working-set fetches like
this one could see more.

**Sketch:** Bytes from each successful HTTP response stashed in an LRU,
keyed by `(url, offset, length)`. On chunk arrival into the coalescer,
serve from cache if fully contained. Subtle decisions:

- **Cache size budget.** Resident span coverage was 702 MiB on the
  test workload. Realistic budget would need to be a fraction of that
  (say 50–100 MiB); eviction will reduce realized savings.
- **In-flight reuse.** If a chunk arrives whose bytes are inside a
  *currently-dispatching* region, the right move is to wait for the
  in-flight response rather than re-fetch. That's where the user's
  "subtle" caveat applies — needs careful synchronization to avoid
  deadlocks.
- **Should be per-workload tunable**, off by default for workloads
  where it would be pure overhead.

### D. Tail-aware dispatch.

**Motivation:** Throughput on the test workload stayed >10 MB/s for
most of the fetch and dropped to ~500 KB/s near the end. Mechanism is
debatable — could be a long tail of tiny leaf-chunk fetches that
fixed-count concurrency under-serves (byte-budget would help), or
graph-walk depth at the tail with serial chunk-scan dependencies.

If the former, byte-budget (A) addresses it directly. If the latter,
nothing client-side helps; needs server-side speculation ("here are
chunks you'll likely want next, pre-located") to break the depth chain.

Worth measuring more carefully: track outstanding-bytes over time vs.
wall clock during a fetch; if outstanding-bytes drops to near-zero
while throughput is low, it's the tail-fetch problem; if outstanding
bytes is healthy but throughput is still low, it's something else
(server, link).

### E. Server-side multi-chunk responses.

**Motivation:** Even after PerURLFetcher, ~100 B/req of header
overhead × hundreds of thousands of requests is significant baseline
cost. The remaining bytes are the presigned URL plus per-stream HTTP/2
framing; neither has a client-side fix.

**Sketch:** A new RPC that takes a batch of chunk hashes and streams
back the chunk bytes directly over gRPC, bypassing the
presigned-URL/CDN path entirely for some requests. Tradeoff is server
egress costs and CDN cache miss; appropriate for small/sparse fetches
where the per-request overhead dominates, less so for bulk. Could be
chosen heuristically per-request based on coalesced-region size.

### F. Heap strategy tweaks beyond goodput.

- Density (`MatchedBytes / span`) as a tiebreaker — penalize regions
  with more dark bytes per useful byte.
- Defer-dispatch window — hold small regions briefly waiting for
  coalescing partners, rather than dispatching the heap top
  unconditionally.
- URL-coherent batching — dispatch all currently-queued ranges for one
  URL before moving to the next, to maximize HPACK and conn-warm
  benefits even further.

These are smaller bets than (A)–(E); worth probing if they start
showing up as outliers in the data.

## Files Touched (this branch)

```
go/libraries/doltcore/remotestorage/netstats/netstats.go        (new)
go/libraries/doltcore/remotestorage/netstats/dark_cache.go      (new)
go/libraries/doltcore/remotestorage/netstats/signal_unix.go     (new)
go/libraries/doltcore/remotestorage/netstats/signal_other.go    (new)
go/libraries/doltcore/remotestorage/per_url_fetcher.go          (new)
go/libraries/doltcore/remotestorage/chunk_store.go              (env-var knobs)
go/libraries/doltcore/remotestorage/chunk_fetcher.go            (dark-bytes/region recording, hit checks)
go/libraries/doltcore/remotestorage/internal/ranges/ranges.go   (DeleteMaxRegion returns region+dark, goodput strategy, env selector)
go/libraries/doltcore/env/grpc_dial_provider.go                 (factory refactor, netstats wiring, PerURLFetcher gate)
go/cmd/dolt/dolt.go                                             (defer netstats dump in runMain)
```
