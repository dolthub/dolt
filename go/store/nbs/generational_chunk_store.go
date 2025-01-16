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
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

var _ chunks.ChunkStore = (*GenerationalNBS)(nil)
var _ chunks.TableFileStore = (*GenerationalNBS)(nil)
var _ chunks.GenerationalCS = (*GenerationalNBS)(nil)
var _ chunks.ChunkStoreGarbageCollector = (*GenerationalNBS)(nil)
var _ NBSCompressedChunkStore = (*GenerationalNBS)(nil)

type GenerationalNBS struct {
	oldGen   *NomsBlockStore
	newGen   *NomsBlockStore
	ghostGen *GhostBlockStore
}

var ErrGhostChunkRequested = errors.New("requested chunk which is expected to be a ghost chunk")

func (gcs *GenerationalNBS) PersistGhostHashes(ctx context.Context, refs hash.HashSet) error {
	if gcs.ghostGen == nil {
		return gcs.ghostGen.PersistGhostHashes(ctx, refs)
	}
	return fmt.Errorf("runtime error. ghostGen is nil but an attempt to persist ghost hashes was made")
}

func (gcs *GenerationalNBS) GhostGen() chunks.ChunkStore {
	return gcs.ghostGen
}

func NewGenerationalCS(oldGen, newGen *NomsBlockStore, ghostGen *GhostBlockStore) *GenerationalNBS {
	if oldGen.Version() != "" && oldGen.Version() != newGen.Version() {
		panic("oldgen and newgen chunkstore versions vary")
	}

	return &GenerationalNBS{
		oldGen:   oldGen,
		newGen:   newGen,
		ghostGen: ghostGen,
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
		c, err = gcs.newGen.Get(ctx, h)
	}
	if err != nil {
		return chunks.EmptyChunk, err
	}

	if c.IsEmpty() && gcs.ghostGen != nil {
		c, err = gcs.ghostGen.Get(ctx, h)
		if err != nil {
			return chunks.EmptyChunk, err
		}
	}

	return c, nil
}

// GetMany gets the Chunks with |hashes| from the store. On return, |foundChunks| will have been fully sent all chunks
// which have been found. Any non-present chunks will silently be ignored.
func (gcs *GenerationalNBS) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *chunks.Chunk)) error {
	mu := &sync.Mutex{}
	notFound := hashes.Copy()
	err := gcs.oldGen.GetMany(ctx, hashes, func(ctx context.Context, chunk *chunks.Chunk) {
		func() {
			mu.Lock()
			defer mu.Unlock()
			delete(notFound, chunk.Hash())
		}()

		found(ctx, chunk)
	})
	if err != nil {
		return err
	}
	if len(notFound) == 0 {
		return nil
	}

	hashes = notFound
	notFound = hashes.Copy()
	err = gcs.newGen.GetMany(ctx, hashes, func(ctx context.Context, chunk *chunks.Chunk) {
		func() {
			mu.Lock()
			defer mu.Unlock()
			delete(notFound, chunk.Hash())
		}()

		found(ctx, chunk)
	})
	if err != nil {
		return err
	}
	if len(notFound) == 0 {
		return nil
	}

	// Last ditch effort to see if the requested objects are commits we've decided to ignore. Note the function spec
	// considers non-present chunks to be silently ignored, so we don't need to return an error here
	if gcs.ghostGen == nil {
		return nil
	}
	return gcs.ghostGen.GetMany(ctx, notFound, found)
}

func (gcs *GenerationalNBS) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, CompressedChunk)) error {
	var mu sync.Mutex
	notInOldGen := hashes.Copy()
	err := gcs.oldGen.GetManyCompressed(ctx, hashes, func(ctx context.Context, chunk CompressedChunk) {
		mu.Lock()
		delete(notInOldGen, chunk.Hash())
		mu.Unlock()
		found(ctx, chunk)
	})
	if err != nil {
		return err
	}
	if len(notInOldGen) == 0 {
		return nil
	}

	notFound := notInOldGen.Copy()
	err = gcs.newGen.GetManyCompressed(ctx, notInOldGen, func(ctx context.Context, chunk CompressedChunk) {
		mu.Lock()
		delete(notFound, chunk.Hash())
		mu.Unlock()
		found(ctx, chunk)
	})
	if err != nil {
		return err
	}
	if len(notFound) == 0 {
		return nil
	}

	// The missing chunks may be ghost chunks.
	if gcs.ghostGen != nil {
		return gcs.ghostGen.GetManyCompressed(ctx, notFound, found)
	}
	return nil
}

// Has returns true iff the value at the address |h| is contained in the store
func (gcs *GenerationalNBS) Has(ctx context.Context, h hash.Hash) (bool, error) {
	has, err := gcs.oldGen.Has(ctx, h)
	if err != nil || has {
		return has, err
	}

	has, err = gcs.newGen.Has(ctx, h)
	if err != nil || has {
		return has, err
	}

	// Possibly a truncated commit.
	if gcs.ghostGen != nil {
		has, err = gcs.ghostGen.Has(ctx, h)
		if err != nil {
			return has, err
		}
	}
	return has, nil
}

// HasMany returns a new HashSet containing any members of |hashes| that are absent from the store.
func (gcs *GenerationalNBS) HasMany(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
	absent, err := gcs.newGen.HasMany(ctx, hashes)
	if err != nil {
		return nil, err
	}
	if len(absent) == 0 {
		return nil, err
	}

	absent, err = gcs.oldGen.HasMany(ctx, absent)
	if err != nil {
		return nil, err
	}
	if len(absent) == 0 || gcs.ghostGen == nil {
		return nil, err
	}

	return gcs.ghostGen.HasMany(ctx, absent)
}

// |refCheck| is called from write processes in newGen, so it is called with
// newGen.mu held. oldGen.mu is not held however.
func (gcs *GenerationalNBS) refCheck(recs []hasRecord) (hash.HashSet, error) {
	absent, err := gcs.newGen.refCheck(recs)
	if err != nil {
		return nil, err
	} else if len(absent) == 0 {
		return absent, nil
	}

	absent, err = func() (hash.HashSet, error) {
		gcs.oldGen.mu.RLock()
		defer gcs.oldGen.mu.RUnlock()
		return gcs.oldGen.refCheck(recs)
	}()
	if err != nil {
		return nil, err
	}
	if len(absent) == 0 || gcs.ghostGen == nil {
		return absent, nil
	}

	return gcs.ghostGen.hasMany(absent)
}

// Put caches c in the ChunkSource. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (gcs *GenerationalNBS) Put(ctx context.Context, c chunks.Chunk, getAddrs chunks.GetAddrsCurry) error {
	return gcs.newGen.putChunk(ctx, c, getAddrs, gcs.refCheck)
}

// Returns the NomsBinFormat with which this ChunkSource is compatible.
func (gcs *GenerationalNBS) Version() string {
	return gcs.newGen.Version()
}

func (gcs *GenerationalNBS) AccessMode() chunks.ExclusiveAccessMode {
	newGenMode := gcs.newGen.AccessMode()
	oldGenMode := gcs.oldGen.AccessMode()
	if oldGenMode > newGenMode {
		return oldGenMode
	}
	return newGenMode
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
	return gcs.newGen.commit(ctx, current, last, gcs.refCheck)
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
	var sb strings.Builder
	sb.WriteString("New Gen: \n\t")
	sb.WriteString(gcs.newGen.StatsSummary())
	sb.WriteString("\nOld Gen: \n\t")
	sb.WriteString(gcs.oldGen.StatsSummary())
	return sb.String()
}

// Close tears down any resources in use by the implementation. After
// Close(), the ChunkStore may not be used again. It is NOT SAFE to call
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
			putErr = gcs.oldGen.Put(ctx, *chunk, func(c chunks.Chunk) chunks.GetAddrsCb {
				return func(ctx context.Context, addrs hash.HashSet, _ chunks.PendingRefExists) error { return nil }
			})
		}
	})

	if putErr != nil {
		return putErr
	}

	return err
}

type prefixedTableFile struct {
	chunks.TableFile
	prefix string
}

func (p prefixedTableFile) LocationPrefix() string {
	return p.prefix + "/"
}

// Sources retrieves the current root hash, a list of all the table files (which may include appendix table files),
// and a second list containing only appendix table files for both the old gen and new gen stores.
func (gcs *GenerationalNBS) Sources(ctx context.Context) (hash.Hash, []chunks.TableFile, []chunks.TableFile, error) {
	root, tFiles, appFiles, err := gcs.newGen.Sources(ctx)
	if err != nil {
		return hash.Hash{}, nil, nil, err
	}

	_, oldTFiles, oldAppFiles, err := gcs.oldGen.Sources(ctx)
	if err != nil {
		return hash.Hash{}, nil, nil, err
	}

	prefix := gcs.RelativeOldGenPath()

	for _, tf := range oldTFiles {
		tFiles = append(tFiles, prefixedTableFile{tf, prefix})
	}
	for _, tf := range oldAppFiles {
		appFiles = append(appFiles, prefixedTableFile{tf, prefix})
	}

	return root, tFiles, appFiles, nil
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
	err := gcs.oldGen.pruneTableFiles(ctx)

	if err != nil {
		return err
	}

	return gcs.newGen.pruneTableFiles(ctx)
}

// SetRootChunk changes the root chunk hash from the previous value to the new root for the newgen cs
func (gcs *GenerationalNBS) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	return gcs.newGen.setRootChunk(ctx, root, previous, gcs.refCheck)
}

// SupportedOperations returns a description of the support TableFile operations. Some stores only support reading table files, not writing.
func (gcs *GenerationalNBS) SupportedOperations() chunks.TableFileStoreOps {
	return gcs.newGen.SupportedOperations()
}

func (gcs *GenerationalNBS) GetChunkLocationsWithPaths(ctx context.Context, hashes hash.HashSet) (map[string]map[hash.Hash]Range, error) {
	res, err := gcs.newGen.GetChunkLocationsWithPaths(ctx, hashes)
	if err != nil {
		return nil, err
	}
	if len(hashes) > 0 {
		prefix := gcs.RelativeOldGenPath()
		toadd, err := gcs.oldGen.GetChunkLocationsWithPaths(ctx, hashes)
		if err != nil {
			return nil, err
		}
		for k, v := range toadd {
			res[filepath.ToSlash(filepath.Join(prefix, k))] = v
		}
	}
	return res, nil
}

func (gcs *GenerationalNBS) GetChunkLocations(ctx context.Context, hashes hash.HashSet) (map[hash.Hash]map[hash.Hash]Range, error) {
	res, err := gcs.newGen.GetChunkLocations(ctx, hashes)
	if err != nil {
		return nil, err
	}
	if len(hashes) > 0 {
		toadd, err := gcs.oldGen.GetChunkLocations(ctx, hashes)
		if err != nil {
			return nil, err
		}
		for k, v := range toadd {
			res[k] = v
		}
	}
	return res, nil
}

func (gcs *GenerationalNBS) RelativeOldGenPath() string {
	newgenpath, ngpok := gcs.newGen.Path()
	oldgenpath, ogpok := gcs.oldGen.Path()
	if ngpok && ogpok {
		if p, err := filepath.Rel(newgenpath, oldgenpath); err == nil {
			return p
		}
	}
	return "oldgen"
}

func (gcs *GenerationalNBS) Path() (string, bool) {
	return gcs.newGen.Path()
}

func (gcs *GenerationalNBS) UpdateManifest(ctx context.Context, updates map[hash.Hash]uint32) (mi ManifestInfo, err error) {
	return gcs.newGen.UpdateManifest(ctx, updates)
}

func (gcs *GenerationalNBS) BeginGC(keeper func(hash.Hash) bool) error {
	return gcs.newGen.BeginGC(keeper)
}

func (gcs *GenerationalNBS) EndGC() {
	gcs.newGen.EndGC()
}

func (gcs *GenerationalNBS) MarkAndSweepChunks(ctx context.Context, getAddrs chunks.GetAddrsCurry, filter chunks.HasManyFunc, dest chunks.ChunkStore, mode chunks.GCMode) (chunks.MarkAndSweeper, error) {
	return markAndSweepChunks(ctx, gcs.newGen, gcs, dest, getAddrs, filter, mode)
}

func (gcs *GenerationalNBS) IterateAllChunks(ctx context.Context, cb func(chunk chunks.Chunk)) error {
	err := gcs.newGen.IterateAllChunks(ctx, cb)
	if err != nil {
		return err
	}
	err = gcs.oldGen.IterateAllChunks(ctx, cb)
	if err != nil {
		return err
	}
	return nil
}

func (gcs *GenerationalNBS) Count() (uint32, error) {
	newGenCnt, err := gcs.newGen.Count()
	if err != nil {
		return 0, err
	}
	oldGenCnt, err := gcs.oldGen.Count()
	if err != nil {
		return 0, err
	}
	return newGenCnt + oldGenCnt, nil
}
