package chunks

import (
	"context"
	"github.com/dolthub/dolt/go/store/hash"
)

var _ ChunkStore = (*GenerationalCS)(nil)

type GenerationalCS struct {
	oldGen ChunkStore
	newGen ChunkStore
}

func NewGenerationalCS(oldGen, newGen ChunkStore) *GenerationalCS {
	if oldGen.Version() != newGen.Version() {
		panic("oldgen and newgen chunkstore versions vary")
	}

	return &GenerationalCS{
		oldGen: oldGen,
		newGen: newGen,
	}
}

// Get the Chunk for the value of the hash in the store. If the hash is absent from the store EmptyChunk is returned.
func (gcs *GenerationalCS) Get(ctx context.Context, h hash.Hash) (Chunk, error) {
	c, err := gcs.oldGen.Get(ctx, h)

	if err != nil {
		return EmptyChunk, err
	}

	if c.IsEmpty() {
		return gcs.newGen.Get(ctx, h)
	}

	return c, nil
}

// GetMany gets the Chunks with |hashes| from the store. On return, |foundChunks| will have been fully sent all chunks
// which have been found. Any non-present chunks will silently be ignored.
func (gcs *GenerationalCS) GetMany(ctx context.Context, hashes hash.HashSet, found func(*Chunk)) error {
	notInOldGen := hashes.Copy()
	err := gcs.oldGen.GetMany(ctx, hashes, func(chunk *Chunk) {
		delete(notInOldGen, chunk.Hash())
		found(chunk)
	})

	if err != nil {
		return err
	}

	if len(notInOldGen) == 0 {
		return nil
	}

	return gcs.newGen.GetMany(ctx, notInOldGen, found)
}

// Returns true iff the value at the address |h| is contained in the
// store
func (gcs *GenerationalCS) Has(ctx context.Context, h hash.Hash) (bool, error) {
	has, err := gcs.oldGen.Has(ctx, h)

	if err != nil {
		return false, err
	}

	if has {
		return true, nil
	}

	return gcs.newGen.Has(ctx, h)
}

// Returns a new HashSet containing any members of |hashes| that are
// absent from the store.
func (gcs *GenerationalCS) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	notInOldGen, err := gcs.oldGen.HasMany(ctx, hashes)

	if err != nil {
		return nil, err
	}

	if len(notInOldGen) == 0 {
		return notInOldGen, nil
	}

	return gcs.newGen.HasMany(ctx, notInOldGen)
}

// Put caches c in the ChunkSource. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (gcs *GenerationalCS) Put(ctx context.Context, c Chunk) error {
	return gcs.newGen.Put(ctx, c)
}

// Returns the NomsVersion with which this ChunkSource is compatible.
func (gcs *GenerationalCS) Version() string {
	return gcs.newGen.Version()
}

// Rebase brings this ChunkStore into sync with the persistent storage's
// current root.
func (gcs *GenerationalCS) Rebase(ctx context.Context) error {
	oErr := gcs.oldGen.Rebase(ctx)
	nErr := gcs.newGen.Rebase(ctx)

	if oErr != nil {
		return oErr
	}

	return nErr
}

// Root returns the root of the database as of the time the ChunkStore
// was opened or the most recent call to Rebase.
func (gcs *GenerationalCS) Root(ctx context.Context) (hash.Hash, error) {
	return gcs.newGen.Root(ctx)
}

// Commit atomically attempts to persist all novel Chunks and update the
// persisted root hash from last to current (or keeps it the same).
// If last doesn't match the root in persistent storage, returns false.
func (gcs *GenerationalCS) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	return gcs.newGen.Commit(ctx, current, last)
}

// Stats may return some kind of struct that reports statistics about the
// ChunkStore instance. The type is implementation-dependent, and impls
// may return nil
func (gcs *GenerationalCS) Stats() interface{} {
	return nil
}

// StatsSummary may return a string containing summarized statistics for
// this ChunkStore. It must return "Unsupported" if this operation is not
// supported.
func (gcs *GenerationalCS) StatsSummary() string {
	return ""
}

// Close tears down any resources in use by the implementation. After // Close(), the ChunkStore may not be used again. It is NOT SAFE to call
// Close() concurrently with any other ChunkStore method; behavior is
// undefined and probably crashy.
func (gcs *GenerationalCS) Close() error {
	oErr := gcs.oldGen.Close()
	nErr := gcs.newGen.Close()

	if oErr != nil {
		return oErr
	}

	return nErr
}

func (gcs *GenerationalCS) copyToOldGen(ctx context.Context, hashes hash.HashSet) error {
	notInOldGen, err := gcs.oldGen.HasMany(ctx, hashes)

	if err != nil {
		return err
	}

	var putErr error
	err = gcs.newGen.GetMany(ctx, notInOldGen, func(chunk *Chunk) {
		if putErr == nil {
			putErr = gcs.oldGen.Put(ctx, *chunk)
		}
	})

	if putErr != nil {
		return putErr
	}

	return err
}

func (gcs *GenerationalCS) MarkAndSweepChunks(ctx context.Context, last hash.Hash, keepChunks <-chan []hash.Hash, dest ChunkStore) error {
	if csgc, ok := gcs.newGen.(ChunkStoreGarbageCollector); ok {
		return csgc.MarkAndSweepChunks(ctx, last, keepChunks, dest)
	}

	return nil
}
