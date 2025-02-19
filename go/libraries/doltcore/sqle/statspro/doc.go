// Copyright 2025 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statspro

// Package statspro provides a queue that manages table statistics
// management and access.
//
// At any given time there is one issuer thread, one scheduler thread,
// and one worker thread.
//
// The issuer executes cycles of fetching the most recent session root,
// reading all of its databases/tables/ indexes, collecting statistics
// for those objects, and updating the shared statistics state. Every
// cycle replaces the shared state.
//
// Cycle work is delegated to the scheduler thread, which serializes
// stats work with concurrent async requests, and rate limits sending
// work to the worker thread. The worker thread simply executes a function
// callback.
//
// GC occurs within an update cycle. Through a cycle GC populates an
// in-memory cache with the complete and exclusive set of values of
// the new shared statistics object. Both are atomically swapped using
// a generation counter (which may or may not be necessary, but is one
// of several guards against surprising concurrent changes).
//
// Concurrent issuer threads are further restrained with a context list
// that at most one thread owns. There are two contexts, one for the
// thread and another for the specific update cycle. Listeners (like wait)
// use the second context to follow update cycles. Concurrent restarts
// cancel and replace the previous owner's contexts with their own. Atomic
// shared state swaps are likewise guarded on the issuer's context
// integrity.
//
// All stats are persisted within a single database. If there are
// multiple databases, one is selected by random as the storage target.
// If during initialization multiple databases have stats, one will be
// chosen by random as the target. If a database changes between server
// restarts, the storage stats will be useless but not impair regular
// operations because storage is only ever a best-effort
// content-addressed persistence layer; buckets will be regenerated if
// they are missing. If the database acting as a storage target is
// deleted, we swap the cache and write to a new storage target.
//
// The main data structures:
//  - Table statistics map, that returns a list of table index statistics
//    for a specific branch, database, and table name.
//  - Object caches:
//    - Bucket cache: Chunk addressed hash map. All provider histogram
//      references point to objects in the bucket cache. Backed by a
//      best-effort on-disk prolly.Map to make restarts faster.
//    - Template cache: Table-schema/index addressed stats.Statistics object
//      for a specific index.
//    - Bound cache: Chunk addressed first row for an index histogram.
//
// The stats lifecycle can be controlled with:
//  - dolt_stats_stop: clear queue and disable thread
//  - dolt_stats_restart: clear queue, refresh queue, start thread
//  - dolt_stats_purge: clear queue, refresh queue, clear cache,
//    disable thread
//  - dolt_stats_validate: return report of cache misses for current
//    root value.
//
// `dolt_stats_wait` is additionally useful for blocking on a full
// queue cycle and then validating whether the session head is caught up.
//
