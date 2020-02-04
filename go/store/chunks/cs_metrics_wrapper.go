package chunks

import (
	"context"
	"fmt"

	"github.com/liquidata-inc/dolt/go/store/hash"
)

// CSMetrics contains the metrics aggregated by a CSMetricsWrapper
type CSMetrics struct {
	totalChunkGets       int
	uniqueGets           int
	totalChunkHasChecks  int
	uniqueChunkHasChecks int
	uniquePuts           int
}

// NewCSMetrics creates a CSMetrics instance
func NewCSMetrics(csMW *CSMetricWrapper) CSMetrics {
	return CSMetrics{
		totalChunkGets:       csMW.totalChunkGets,
		uniqueGets:           len(csMW.uniqueGets),
		totalChunkHasChecks:  csMW.totalChunkHasChecks,
		uniqueChunkHasChecks: len(csMW.uniqueChunkHasChecks),
		uniquePuts:           len(csMW.uniquePuts),
	}
}

// String prints CSMetrics as JSON with indenting
func (csm CSMetrics) String() string {
	return fmt.Sprintf(`{
		"totalChunkGets":       %d,
		"uniqueGets":           %d,
		"totalChunkHasChecks":  %d,
		"uniqueChunkHasChecks": %d,
		"uniquePuts":           %d,
	}`, csm.totalChunkGets, csm.uniqueGets, csm.totalChunkHasChecks, csm.uniqueChunkHasChecks, csm.uniquePuts)
}

// CSMetricWrapper is a ChunkStore implementation that wraps a ChunkStore, and collects metrics on the calls.
type CSMetricWrapper struct {
	totalChunkGets       int
	uniqueGets           hash.HashSet
	totalChunkHasChecks  int
	uniqueChunkHasChecks hash.HashSet
	uniquePuts           hash.HashSet
	cs                   ChunkStore
}

// NewCSMetricWrapper returns a new CSMetricWrapper
func NewCSMetricWrapper(cs ChunkStore) *CSMetricWrapper {
	return &CSMetricWrapper{
		uniqueGets:           make(hash.HashSet),
		uniqueChunkHasChecks: make(hash.HashSet),
		uniquePuts:           make(hash.HashSet),
		cs:                   cs,
	}
}

// Get the Chunk for the value of the hash in the store. If the hash is
// absent from the store EmptyChunk is returned.
func (csMW *CSMetricWrapper) Get(ctx context.Context, h hash.Hash) (Chunk, error) {
	csMW.totalChunkGets++
	csMW.uniqueGets.Insert(h)
	return csMW.cs.Get(ctx, h)
}

// GetMany gets the Chunks with |hashes| from the store. On return,
// |foundChunks| will have been fully sent all chunks which have been
// found. Any non-present chunks will silently be ignored.
func (csMW *CSMetricWrapper) GetMany(ctx context.Context, hashes hash.HashSet, foundChunks chan<- *Chunk) error {
	csMW.totalChunkGets += len(hashes)
	for h := range hashes {
		csMW.uniqueGets.Insert(h)
	}
	return csMW.cs.GetMany(ctx, hashes, foundChunks)
}

// Returns true iff the value at the address |h| is contained in the
// store
func (csMW *CSMetricWrapper) Has(ctx context.Context, h hash.Hash) (bool, error) {
	csMW.totalChunkHasChecks++
	csMW.uniqueChunkHasChecks.Insert(h)
	return csMW.cs.Has(ctx, h)
}

// Returns a new HashSet containing any members of |hashes| that are
// absent from the store.
func (csMW *CSMetricWrapper) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	csMW.totalChunkHasChecks += len(hashes)
	for h := range hashes {
		csMW.uniqueChunkHasChecks.Insert(h)
	}
	return csMW.cs.HasMany(ctx, hashes)
}

// Put caches c in the ChunkSource. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (csMW *CSMetricWrapper) Put(ctx context.Context, c Chunk) error {
	csMW.uniquePuts.Insert(c.Hash())
	return csMW.cs.Put(ctx, c)
}

// Returns the NomsVersion with which this ChunkSource is compatible.
func (csMW *CSMetricWrapper) Version() string {
	return csMW.cs.Version()
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
	return
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
	return csMW.Close()
}
