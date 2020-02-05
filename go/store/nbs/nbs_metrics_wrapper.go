// Copyright 2020 Liquidata, Inc.
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

package nbs

import (
	"context"
	"fmt"
	"io"

	"github.com/liquidata-inc/dolt/go/store/chunks"

	"github.com/liquidata-inc/dolt/go/store/hash"
)

// NBSMetrics contains the metrics aggregated by a CSMetricsWrapper
type NBSMetrics struct {
	TotalChunkGets       int
	UniqueGets           int
	TotalChunkHasChecks  int
	UniqueChunkHasChecks int
	UniquePuts           int
	Delegate             interface{}
	DelegateSummary      string
}

// NewCSMetrics creates a NBSMetrics instance
func NewNBSMetrics(csMW *NBSMetricWrapper) NBSMetrics {
	return NBSMetrics{
		TotalChunkGets:       csMW.totalChunkGets,
		UniqueGets:           len(csMW.uniqueGets),
		TotalChunkHasChecks:  csMW.totalChunkHasChecks,
		UniqueChunkHasChecks: len(csMW.uniqueChunkHasChecks),
		UniquePuts:           len(csMW.uniquePuts),
		Delegate:             csMW.nbs.Stats(),
		DelegateSummary:      csMW.nbs.StatsSummary(),
	}
}

// String prints NBSMetrics as JSON with indenting
func (nbsm NBSMetrics) String() string {
	return fmt.Sprintf(`{
	"totalChunkGets":       %d,
	"uniqueGets":           %d,
	"totalChunkHasChecks":  %d,
	"uniqueChunkHasChecks": %d,
	"uniquePuts":           %d,
	"delegate":             "%s",
}`, nbsm.TotalChunkGets, nbsm.UniqueGets, nbsm.TotalChunkHasChecks, nbsm.UniqueChunkHasChecks, nbsm.UniquePuts, nbsm.DelegateSummary)
}

// NBSMetricWrapper is a ChunkStore implementation that wraps a ChunkStore, and collects metrics on the calls.
type NBSMetricWrapper struct {
	totalChunkGets       int
	uniqueGets           hash.HashSet
	totalChunkHasChecks  int
	uniqueChunkHasChecks hash.HashSet
	uniquePuts           hash.HashSet
	nbs                  *NomsBlockStore
}

// NewCSMetricWrapper returns a new NBSMetricWrapper
func NewNBSMetricWrapper(nbs *NomsBlockStore) *NBSMetricWrapper {
	return &NBSMetricWrapper{
		uniqueGets:           make(hash.HashSet),
		uniqueChunkHasChecks: make(hash.HashSet),
		uniquePuts:           make(hash.HashSet),
		nbs:                  nbs,
	}
}

// Get the Chunk for the value of the hash in the store. If the hash is
// absent from the store EmptyChunk is returned.
func (nbsMW *NBSMetricWrapper) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	nbsMW.totalChunkGets++
	nbsMW.uniqueGets.Insert(h)
	return nbsMW.nbs.Get(ctx, h)
}

// GetMany gets the Chunks with |hashes| from the store. On return,
// |foundChunks| will have been fully sent all chunks which have been
// found. Any non-present chunks will silently be ignored.
func (nbsMW *NBSMetricWrapper) GetMany(ctx context.Context, hashes hash.HashSet, foundChunks chan<- *chunks.Chunk) error {
	nbsMW.totalChunkGets += len(hashes)
	for h := range hashes {
		nbsMW.uniqueGets.Insert(h)
	}
	return nbsMW.nbs.GetMany(ctx, hashes, foundChunks)
}

// Returns true iff the value at the address |h| is contained in the
// store
func (nbsMW *NBSMetricWrapper) Has(ctx context.Context, h hash.Hash) (bool, error) {
	nbsMW.totalChunkHasChecks++
	nbsMW.uniqueChunkHasChecks.Insert(h)
	return nbsMW.nbs.Has(ctx, h)
}

// Returns a new HashSet containing any members of |hashes| that are
// absent from the store.
func (nbsMW *NBSMetricWrapper) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	nbsMW.totalChunkHasChecks += len(hashes)
	for h := range hashes {
		nbsMW.uniqueChunkHasChecks.Insert(h)
	}
	return nbsMW.nbs.HasMany(ctx, hashes)
}

// Put caches c in the ChunkSource. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (nbsMW *NBSMetricWrapper) Put(ctx context.Context, c chunks.Chunk) error {
	nbsMW.uniquePuts.Insert(c.Hash())
	return nbsMW.nbs.Put(ctx, c)
}

// Returns the NomsVersion with which this ChunkSource is compatible.
func (nbsMW *NBSMetricWrapper) Version() string {
	return nbsMW.nbs.Version()
}

// Rebase brings this ChunkStore into sync with the persistent storage's
// current root.
func (nbsMW *NBSMetricWrapper) Rebase(ctx context.Context) error {
	return nbsMW.nbs.Rebase(ctx)
}

// Root returns the root of the database as of the time the ChunkStore
// was opened or the most recent call to Rebase.
func (nbsMW *NBSMetricWrapper) Root(ctx context.Context) (hash.Hash, error) {
	return nbsMW.nbs.Root(ctx)
}

// Commit atomically attempts to persist all novel Chunks and update the
// persisted root hash from last to current (or keeps it the same).
// If last doesn't match the root in persistent storage, returns false.
func (nbsMW *NBSMetricWrapper) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	return nbsMW.nbs.Commit(ctx, current, last)
}

// Stats may return some kind of struct that reports statistics about the
// ChunkStore instance. The type is implementation-dependent, and impls
// may return nil
func (nbsMW *NBSMetricWrapper) Stats() interface{} {
	return NewNBSMetrics(nbsMW)
}

// StatsSummary may return a string containing summarized statistics for
// this ChunkStore. It must return "Unsupported" if this operation is not
// supported.
func (nbsMW *NBSMetricWrapper) StatsSummary() string {
	return NewNBSMetrics(nbsMW).String()
}

// Close tears down any resources in use by the implementation. After
// Close(), the ChunkStore may not be used again. It is NOT SAFE to call
// Close() concurrently with any other ChunkStore method; behavior is
// undefined and probably crashy.
func (nbsMW *NBSMetricWrapper) Close() error {
	return nbsMW.nbs.Close()
}

// Sources retrieves the current root hash, and a list of all the table files
func (nbsMW *NBSMetricWrapper) Sources(ctx context.Context) (hash.Hash, []TableFile, error) {
	return nbsMW.nbs.Sources(ctx)
}

// WriteTableFile will read a table file from the provided reader and write it to the TableFileStore
func (nbsMW *NBSMetricWrapper) WriteTableFile(ctx context.Context, fileId string, numChunks int, rd io.Reader, contentLength uint64, contentHash []byte) error {
	return nbsMW.nbs.WriteTableFile(ctx, fileId, numChunks, rd, contentLength, contentHash)
}

// SetRootChunk changes the root chunk hash from the previous value to the new root.
func (nbsMW *NBSMetricWrapper) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	return nbsMW.nbs.SetRootChunk(ctx, root, previous)
}

// GetManyCompressed gets the compressed Chunks with |hashes| from the store. On return,
// |foundChunks| will have been fully sent all chunks which have been
// found. Any non-present chunks will silently be ignored.
func (nbsMW *NBSMetricWrapper) GetManyCompressed(ctx context.Context, hashes hash.HashSet, cmpChChan chan<- CompressedChunk) error {
	return nbsMW.nbs.GetManyCompressed(ctx, hashes, cmpChChan)
}
