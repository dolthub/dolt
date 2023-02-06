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
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

var _ chunks.ChunkStore = (*GenerationalNBS)(nil)
var _ chunks.GenerationalCS = (*GenerationalNBS)(nil)
var _ chunks.TableFileStore = (*GenerationalNBS)(nil)

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

// Has returns true iff the value at the address |h| is contained in the store
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

// HasMany returns a new HashSet containing any members of |hashes| that are absent from the store.
func (gcs *GenerationalNBS) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	gcs.newGen.mu.RLock()
	defer gcs.newGen.mu.RUnlock()
	return gcs.hasMany(toHasRecords(hashes))
}

func (gcs *GenerationalNBS) hasMany(recs []hasRecord) (absent hash.HashSet, err error) {
	absent, err = gcs.newGen.hasMany(recs)
	if err != nil {
		return nil, err
	} else if len(absent) == 0 {
		return absent, nil
	}

	gcs.oldGen.mu.RLock()
	defer gcs.oldGen.mu.RUnlock()
	return gcs.oldGen.hasMany(recs)
}

func (gcs *GenerationalNBS) errorIfDangling(ctx context.Context, addrs hash.HashSet) error {
	absent, err := gcs.HasMany(ctx, addrs)
	if err != nil {
		return err
	}
	if len(absent) != 0 {
		s := absent.String()
		return fmt.Errorf("Found dangling references to %s", s)
	}
	return nil
}

// Put caches c in the ChunkSource. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (gcs *GenerationalNBS) Put(ctx context.Context, c chunks.Chunk, getAddrs chunks.GetAddrsCb) error {
	return gcs.newGen.putChunk(ctx, c, getAddrs, gcs.hasMany)
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
	return gcs.newGen.commit(ctx, current, last, gcs.hasMany)
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
			putErr = gcs.oldGen.Put(ctx, *chunk, func(ctx context.Context, c chunks.Chunk) (hash.HashSet, error) {
				return nil, nil
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

func (p prefixedTableFile) FileID() string {
	return filepath.ToSlash(filepath.Join(p.prefix, p.TableFile.FileID()))
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
	err := gcs.oldGen.pruneTableFiles(ctx, gcs.hasMany)

	if err != nil {
		return err
	}

	return gcs.newGen.pruneTableFiles(ctx, gcs.hasMany)
}

// SetRootChunk changes the root chunk hash from the previous value to the new root for the newgen cs
func (gcs *GenerationalNBS) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	return gcs.newGen.setRootChunk(ctx, root, previous, gcs.hasMany)
}

// SupportedOperations returns a description of the support TableFile operations. Some stores only support reading table files, not writing.
func (gcs *GenerationalNBS) SupportedOperations() chunks.TableFileStoreOps {
	return gcs.newGen.SupportedOperations()
}

func (gcs *GenerationalNBS) GetChunkLocationsWithPaths(hashes hash.HashSet) (map[string]map[hash.Hash]Range, error) {
	res, err := gcs.newGen.GetChunkLocationsWithPaths(hashes)
	if err != nil {
		return nil, err
	}
	if len(hashes) > 0 {
		prefix := gcs.RelativeOldGenPath()
		toadd, err := gcs.oldGen.GetChunkLocationsWithPaths(hashes)
		if err != nil {
			return nil, err
		}
		for k, v := range toadd {
			res[filepath.ToSlash(filepath.Join(prefix, k))] = v
		}
	}
	return res, nil
}

func (gcs *GenerationalNBS) GetChunkLocations(hashes hash.HashSet) (map[hash.Hash]map[hash.Hash]Range, error) {
	res, err := gcs.newGen.GetChunkLocations(hashes)
	if err != nil {
		return nil, err
	}
	if len(hashes) > 0 {
		toadd, err := gcs.oldGen.GetChunkLocations(hashes)
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
