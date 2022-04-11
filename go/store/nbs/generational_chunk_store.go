// Copyright 2021 Dolthub, Inc.
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
	"io"
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

var _ chunks.ChunkStore = (*GenerationalNBS)(nil)
var _ chunks.GenerationalCS = (*GenerationalNBS)(nil)
var _ TableFileStore = (*GenerationalNBS)(nil)

type GenerationalNBS struct {
	oldGen *NomsBlockStore
	newGen *NomsBlockStore
}

func NewGenerationalCS(oldGen, newGen *NomsBlockStore) *GenerationalNBS {
	if oldGen.Version() != "" && oldGen.Version() != newGen.Version() {
		panic("oldgen and newgen chunkstore versions vary")
	}

	return &GenerationalNBS{
		oldGen: oldGen,
		newGen: newGen,
	}
}

func (gcs *GenerationalNBS) NewGen() chunks.ChunkStoreGarbageCollector {
	return gcs.newGen
}

func (gcs *GenerationalNBS) OldGen() chunks.ChunkStoreGarbageCollector {
	return gcs.oldGen
}

// Get the Chunk for the value of the hash in the store. If the hash is absent from the store EmptyChunk is returned.
func (gcs *GenerationalNBS) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	c, err := gcs.oldGen.Get(ctx, h)

	if err != nil {
		return chunks.EmptyChunk, err
	}

	if c.IsEmpty() {
		return gcs.newGen.Get(ctx, h)
	}

	return c, nil
}

// GetMany gets the Chunks with |hashes| from the store. On return, |foundChunks| will have been fully sent all chunks
// which have been found. Any non-present chunks will silently be ignored.
func (gcs *GenerationalNBS) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *chunks.Chunk)) error {
	mu := &sync.Mutex{}
	notInOldGen := hashes.Copy()
	err := gcs.oldGen.GetMany(ctx, hashes, func(ctx context.Context, chunk *chunks.Chunk) {
		func() {
			mu.Lock()
			defer mu.Unlock()
			delete(notInOldGen, chunk.Hash())
		}()

		found(ctx, chunk)
	})

	if err != nil {
		return err
	}

	if len(notInOldGen) == 0 {
		return nil
	}

	return gcs.newGen.GetMany(ctx, notInOldGen, found)
}

func (gcs *GenerationalNBS) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, CompressedChunk)) error {
	mu := &sync.Mutex{}
	notInOldGen := hashes.Copy()
	err := gcs.oldGen.GetManyCompressed(ctx, hashes, func(ctx context.Context, chunk CompressedChunk) {
		func() {
			mu.Lock()
			defer mu.Unlock()
			delete(notInOldGen, chunk.Hash())
		}()

		found(ctx, chunk)
	})

	if err != nil {
		return err
	}

	if len(notInOldGen) == 0 {
		return nil
	}

	return gcs.newGen.GetManyCompressed(ctx, notInOldGen, found)
}

// Returns true iff the value at the address |h| is contained in the
// store
func (gcs *GenerationalNBS) Has(ctx context.Context, h hash.Hash) (bool, error) {
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
func (gcs *GenerationalNBS) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
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
func (gcs *GenerationalNBS) Put(ctx context.Context, c chunks.Chunk) error {
	return gcs.newGen.Put(ctx, c)
}

// Returns the NomsVersion with which this ChunkSource is compatible.
func (gcs *GenerationalNBS) Version() string {
	return gcs.newGen.Version()
}

// Rebase brings this ChunkStore into sync with the persistent storage's
// current root.
func (gcs *GenerationalNBS) Rebase(ctx context.Context) error {
	oErr := gcs.oldGen.Rebase(ctx)
	nErr := gcs.newGen.Rebase(ctx)

	if oErr != nil {
		return oErr
	}

	return nErr
}

// Root returns the root of the database as of the time the ChunkStore
// was opened or the most recent call to Rebase.
func (gcs *GenerationalNBS) Root(ctx context.Context) (hash.Hash, error) {
	return gcs.newGen.Root(ctx)
}

// Commit atomically attempts to persist all novel Chunks and update the
// persisted root hash from last to current (or keeps it the same).
// If last doesn't match the root in persistent storage, returns false.
func (gcs *GenerationalNBS) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	return gcs.newGen.Commit(ctx, current, last)
}

// Stats may return some kind of struct that reports statistics about the
// ChunkStore instance. The type is implementation-dependent, and impls
// may return nil
func (gcs *GenerationalNBS) Stats() interface{} {
	return nil
}

// StatsSummary may return a string containing summarized statistics for
// this ChunkStore. It must return "Unsupported" if this operation is not
// supported.
func (gcs *GenerationalNBS) StatsSummary() string {
	return ""
}

// Close tears down any resources in use by the implementation. After // Close(), the ChunkStore may not be used again. It is NOT SAFE to call
// Close() concurrently with any other ChunkStore method; behavior is
// undefined and probably crashy.
func (gcs *GenerationalNBS) Close() error {
	oErr := gcs.oldGen.Close()
	nErr := gcs.newGen.Close()

	if oErr != nil {
		return oErr
	}

	return nErr
}

func (gcs *GenerationalNBS) copyToOldGen(ctx context.Context, hashes hash.HashSet) error {
	notInOldGen, err := gcs.oldGen.HasMany(ctx, hashes)

	if err != nil {
		return err
	}

	var putErr error
	err = gcs.newGen.GetMany(ctx, notInOldGen, func(ctx context.Context, chunk *chunks.Chunk) {
		if putErr == nil {
			putErr = gcs.oldGen.Put(ctx, *chunk)
		}
	})

	if putErr != nil {
		return putErr
	}

	return err
}

// Sources retrieves the current root hash, a list of all the table files (which may include appendix table files),
// and a second list containing only appendix table files for both the old gen and new gen stores.
func (gcs *GenerationalNBS) Sources(ctx context.Context) (hash.Hash, []TableFile, []TableFile, error) {
	_, tFiles, appFiles, err := gcs.oldGen.Sources(ctx)

	if err != nil {
		return hash.Hash{}, nil, nil, err
	}

	newRoot, newTFiles, newAppFiles, err := gcs.newGen.Sources(ctx)

	if err != nil {
		return hash.Hash{}, nil, nil, err
	}

	for _, tf := range newTFiles {
		tFiles = append(tFiles, tf)
	}
	for _, tf := range newAppFiles {
		appFiles = append(appFiles, tf)
	}

	return newRoot, tFiles, appFiles, nil
}

// Size  returns the total size, in bytes, of the table files in the new and old gen stores combined
func (gcs *GenerationalNBS) Size(ctx context.Context) (uint64, error) {
	oldSize, err := gcs.oldGen.Size(ctx)

	if err != nil {
		return 0, err
	}

	newSize, err := gcs.newGen.Size(ctx)

	if err != nil {
		return 0, err
	}

	return oldSize + newSize, nil
}

// WriteTableFile will read a table file from the provided reader and write it to the new gen TableFileStore
func (gcs *GenerationalNBS) WriteTableFile(ctx context.Context, fileId string, numChunks int, contentHash []byte, getRd func() (io.ReadCloser, uint64, error)) error {
	return gcs.newGen.WriteTableFile(ctx, fileId, numChunks, contentHash, getRd)
}

// AddTableFilesToManifest adds table files to the manifest of the newgen cs
func (gcs *GenerationalNBS) AddTableFilesToManifest(ctx context.Context, fileIdToNumChunks map[string]int) error {
	return gcs.newGen.AddTableFilesToManifest(ctx, fileIdToNumChunks)
}

// PruneTableFiles deletes old table files that are no longer referenced in the manifest of the new or old gen chunkstores
func (gcs *GenerationalNBS) PruneTableFiles(ctx context.Context) error {
	err := gcs.oldGen.PruneTableFiles(ctx)

	if err != nil {
		return err
	}

	return gcs.newGen.PruneTableFiles(ctx)
}

// SetRootChunk changes the root chunk hash from the previous value to the new root for the newgen cs
func (gcs *GenerationalNBS) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	return gcs.newGen.SetRootChunk(ctx, root, previous)
}

// SupportedOperations returns a description of the support TableFile operations. Some stores only support reading table files, not writing.
func (gcs *GenerationalNBS) SupportedOperations() TableFileStoreOps {
	return gcs.newGen.SupportedOperations()
}
