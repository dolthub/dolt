// Copyright 2024 Dolthub, Inc.
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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

type GhostBlockStore struct {
	skippedRefs      *hash.HashSet
	ghostObjectsFile string
}

// We use the Has, HasMany, Get, GetMany, GetManyCompressed, and PersistGhostHashes methods from the ChunkStore interface. All other methods are not supported.
var _ chunks.ChunkStore = (*GhostBlockStore)(nil)
var _ NBSCompressedChunkStore = (*GenerationalNBS)(nil)

// NewGhostBlockStore returns a new GhostBlockStore instance. Currently the only parameter is the path to the directory
// where we will create a text file called ghostObjects.txt. This file will contain the hashes of the ghost objects. Creation
// and use of this file is constrained to this instance. If there is no ghostObjects.txt file, then the GhostBlockStore will
// be empty - never returning any values from the Has, HasMany, Get, or GetMany methods.
func NewGhostBlockStore(nomsPath string) (*GhostBlockStore, error) {
	ghostPath := filepath.Join(nomsPath, "ghostObjects.txt")
	f, err := os.Open(ghostPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return &GhostBlockStore{
				skippedRefs:      &hash.HashSet{},
				ghostObjectsFile: ghostPath,
			}, nil
		}
		// Other error, permission denied, etc, we want to hear about.
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	skiplist := &hash.HashSet{}
	for scanner.Scan() {
		h := scanner.Text()
		if hash.IsValid(h) {
			skiplist.Insert(hash.Parse(h))
		} else {
			return nil, fmt.Errorf("invalid hash %s in ghostObjects.txt", h)
		}
	}

	return &GhostBlockStore{
		skippedRefs:      skiplist,
		ghostObjectsFile: ghostPath,
	}, nil
}

// Get returns a ghost chunk if the hash is in the ghostObjectsFile. Otherwise, it returns an empty chunk. Chunks returned
// by this code will always be ghost chunks, ie chunk.IsGhost() will always return true.
func (g GhostBlockStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	if g.skippedRefs.Has(h) {
		return *chunks.NewGhostChunk(h), nil
	}
	return chunks.EmptyChunk, nil
}

func (g GhostBlockStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *chunks.Chunk)) error {
	for h := range hashes {
		if g.skippedRefs.Has(h) {
			found(ctx, chunks.NewGhostChunk(h))
		}
	}
	return nil
}

func (g GhostBlockStore) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(context.Context, CompressedChunk)) error {
	for h := range hashes {
		if g.skippedRefs.Has(h) {
			found(ctx, NewGhostCompressedChunk(h))
		}
	}
	return nil
}

func (g *GhostBlockStore) PersistGhostHashes(ctx context.Context, hashes hash.HashSet) error {
	if hashes.Size() == 0 {
		return fmt.Errorf("runtime error. PersistGhostHashes called with empty hash set")
	}

	f, err := os.OpenFile(g.ghostObjectsFile, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	for h := range hashes {
		if _, err := f.WriteString(h.String() + "\n"); err != nil {
			return err
		}
	}

	g.skippedRefs = &hash.HashSet{}
	for h := range hashes {
		g.skippedRefs.Insert(h)
	}

	return nil
}

func (g GhostBlockStore) Has(ctx context.Context, h hash.Hash) (bool, error) {
	if g.skippedRefs.Has(h) {
		return true, nil
	}
	return false, nil
}

func (g GhostBlockStore) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	return g.hasMany(hashes)
}

func (g GhostBlockStore) hasMany(hashes hash.HashSet) (absent hash.HashSet, err error) {
	absent = hash.HashSet{}
	for h := range hashes {
		if !g.skippedRefs.Has(h) {
			absent.Insert(h)
		}
	}
	return absent, nil
}

func (g GhostBlockStore) Put(ctx context.Context, c chunks.Chunk, getAddrs chunks.GetAddrsCurry) error {
	panic("GhostBlockStore does not support Put")
}

func (g GhostBlockStore) Version() string {
	panic("GhostBlockStore does not support Version")
}

func (g GhostBlockStore) AccessMode() chunks.ExclusiveAccessMode {
	panic("GhostBlockStore does not support AccessMode")
}

func (g GhostBlockStore) Rebase(ctx context.Context) error {
	panic("GhostBlockStore does not support Rebase")
}

func (g GhostBlockStore) Root(ctx context.Context) (hash.Hash, error) {
	panic("GhostBlockStore does not support Root")
}

func (g GhostBlockStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	panic("GhostBlockStore does not support Commit")
}

func (g GhostBlockStore) Stats() interface{} {
	panic("GhostBlockStore does not support Stats")
}

func (g GhostBlockStore) StatsSummary() string {
	panic("GhostBlockStore does not support StatsSummary")
}

func (g GhostBlockStore) Close() error {
	panic("GhostBlockStore does not support Close")
}
