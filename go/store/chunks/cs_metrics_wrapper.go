// Copyright 2020 Dolthub, Inc.
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

package chunks

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/dolthub/dolt/go/store/hash"
)

// CSMetrics contains the metrics aggregated by a CSMetricsWrapper
type CSMetrics struct {
	TotalChunkGets      int32
	TotalChunkHasChecks int32
	TotalChunkPuts      int32
	Delegate            interface{}
	DelegateSummary     string
}

// NewCSMetrics creates a CSMetrics instance
func NewCSMetrics(csMW *CSMetricWrapper) CSMetrics {
	return CSMetrics{
		TotalChunkGets:      atomic.LoadInt32(&csMW.TotalChunkGets),
		TotalChunkHasChecks: atomic.LoadInt32(&csMW.TotalChunkHasChecks),
		TotalChunkPuts:      atomic.LoadInt32(&csMW.TotalChunkPuts),
		Delegate:            csMW.cs.Stats(),
		DelegateSummary:     csMW.cs.StatsSummary(),
	}
}

// String prints CSMetrics as JSON with indenting
func (csm CSMetrics) String() string {
	return fmt.Sprintf(`{
	"TotalChunkGets":       %d,
	"TotalChunkHasChecks":  %d,
	"TotalChunkPuts":       %d,
	"DelegateSummary":      "%s",
}`, csm.TotalChunkGets, csm.TotalChunkHasChecks, csm.TotalChunkPuts, csm.DelegateSummary)
}

// CSMetricWrapper is a ChunkStore implementation that wraps a ChunkStore, and collects metrics on the calls.
type CSMetricWrapper struct {
	TotalChunkGets      int32
	TotalChunkHasChecks int32
	TotalChunkPuts      int32
	cs                  ChunkStore
}

var _ ChunkStore = &CSMetricWrapper{}

// NewCSMetricWrapper returns a new CSMetricWrapper
func NewCSMetricWrapper(cs ChunkStore) *CSMetricWrapper {
	return &CSMetricWrapper{
		cs: cs,
	}
}

// Get the Chunk for the value of the hash in the store. If the hash is
// absent from the store EmptyChunk is returned.
func (csMW *CSMetricWrapper) Get(ctx context.Context, h hash.Hash) (Chunk, error) {
	atomic.AddInt32(&csMW.TotalChunkGets, 1)
	return csMW.cs.Get(ctx, h)
}

// GetMany gets the Chunks with |hashes| from the store. On return,
// |foundChunks| will have been fully sent all chunks which have been
// found. Any non-present chunks will silently be ignored.
func (csMW *CSMetricWrapper) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *Chunk)) error {
	atomic.AddInt32(&csMW.TotalChunkGets, int32(len(hashes)))
	return csMW.cs.GetMany(ctx, hashes, found)
}

// Returns true iff the value at the address |h| is contained in the
// store
func (csMW *CSMetricWrapper) Has(ctx context.Context, h hash.Hash) (bool, error) {
	atomic.AddInt32(&csMW.TotalChunkHasChecks, 1)
	return csMW.cs.Has(ctx, h)
}

// Returns a new HashSet containing any members of |hashes| that are
// absent from the store.
func (csMW *CSMetricWrapper) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	atomic.AddInt32(&csMW.TotalChunkHasChecks, int32(len(hashes)))
	return csMW.cs.HasMany(ctx, hashes)
}

// Put caches c in the ChunkSource. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (csMW *CSMetricWrapper) Put(ctx context.Context, c Chunk, getAddrs GetAddrsCurry) error {
	atomic.AddInt32(&csMW.TotalChunkPuts, 1)
	return csMW.cs.Put(ctx, c, getAddrs)
}

// Returns the NomsBinFormat with which this ChunkSource is compatible.
func (csMW *CSMetricWrapper) Version() string {
	return csMW.cs.Version()
}

func (csMW *CSMetricWrapper) AccessMode() ExclusiveAccessMode {
	return csMW.cs.AccessMode()
}

// Rebase brings this ChunkStore into sync with the persistent storage's
// current root.
func (csMW *CSMetricWrapper) Rebase(ctx context.Context) error {
	return csMW.cs.Rebase(ctx)
}

// Root returns the root of the database as of the time the ChunkStore
// was opened or the most recent call to Rebase.
func (csMW *CSMetricWrapper) Root(ctx context.Context) (hash.Hash, error) {
	return csMW.cs.Root(ctx)
}

// Commit atomically attempts to persist all novel Chunks and update the
// persisted root hash from last to current (or keeps it the same).
// If last doesn't match the root in persistent storage, returns false.
func (csMW *CSMetricWrapper) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	return csMW.cs.Commit(ctx, current, last)
}

// Stats may return some kind of struct that reports statistics about the
// ChunkStore instance. The type is implementation-dependent, and impls
// may return nil
func (csMW *CSMetricWrapper) Stats() interface{} {
	return NewCSMetrics(csMW)
}

// StatsSummary may return a string containing summarized statistics for
// this ChunkStore. It must return "Unsupported" if this operation is not
// supported.
func (csMW *CSMetricWrapper) StatsSummary() string {
	return NewCSMetrics(csMW).String()
}

// Close tears down any resources in use by the implementation. After
// Close(), the ChunkStore may not be used again. It is NOT SAFE to call
// Close() concurrently with any other ChunkStore method; behavior is
// undefined and probably crashy.
func (csMW *CSMetricWrapper) Close() error {
	return csMW.cs.Close()
}

func (csMW *CSMetricWrapper) PersistGhostHashes(ctx context.Context, refs hash.HashSet) error {
	return csMW.cs.PersistGhostHashes(ctx, refs)
}
