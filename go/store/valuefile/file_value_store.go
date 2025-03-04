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

package valuefile

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

var _ chunks.ChunkStore = (*FileValueStore)(nil)
var _ types.ValueReadWriter = (*FileValueStore)(nil)

// FileValueStore implements a trivial in memory chunks.ChunkStore and types.ValueReadWriter in order to allow easy
// serialization / deserialization of noms data to and from a file
type FileValueStore struct {
	nbf *types.NomsBinFormat

	valLock *sync.Mutex
	values  map[hash.Hash]types.Value

	rootHash  hash.Hash
	chunkLock *sync.Mutex
	chunks    map[hash.Hash][]byte
}

// NewFileValueStore creates a new FileValueStore
func NewFileValueStore(nbf *types.NomsBinFormat) (*FileValueStore, error) {
	return &FileValueStore{
		nbf:       nbf,
		valLock:   &sync.Mutex{},
		values:    make(map[hash.Hash]types.Value),
		chunkLock: &sync.Mutex{},
		chunks:    make(map[hash.Hash][]byte),
	}, nil
}

// Gets the NomsBinaryFormat for the Store
func (f *FileValueStore) Format() *types.NomsBinFormat {
	return f.nbf
}

// ReadValue reads a value from the store
func (f *FileValueStore) ReadValue(ctx context.Context, h hash.Hash) (types.Value, error) {
	f.valLock.Lock()
	defer f.valLock.Unlock()

	v := f.values[h]
	return v, nil
}

// ReadManyValues reads and decodes Values indicated by |hashes| from lvs and returns the found Values in the same order.
// Any non-present Values will be represented by nil.
func (f *FileValueStore) ReadManyValues(ctx context.Context, hashes hash.HashSlice) (types.ValueSlice, error) {
	f.valLock.Lock()
	defer f.valLock.Unlock()

	vals := make(types.ValueSlice, len(hashes))
	for i, h := range hashes {
		vals[i] = f.values[h]
	}

	return vals, nil
}

// WriteValue adds a value to the store
func (f *FileValueStore) WriteValue(ctx context.Context, v types.Value) (types.Ref, error) {
	f.valLock.Lock()
	defer f.valLock.Unlock()

	h, err := v.Hash(f.nbf)

	if err != nil {
		return types.Ref{}, err
	}

	_, ok := f.values[h]

	if !ok {
		f.values[h] = v

		c, err := types.EncodeValue(v, f.nbf)

		if err != nil {
			return types.Ref{}, err
		}

		err = f.Put(ctx, c, func(c chunks.Chunk) chunks.GetAddrsCb {
			return func(ctx context.Context, addrs hash.HashSet, _ chunks.PendingRefExists) error {
				return types.AddrsFromNomsValue(c, f.nbf, addrs)
			}
		})

		if err != nil {
			return types.Ref{}, err
		}
	}

	return types.NewRef(v, f.nbf)
}

// Get gets a chunk by it's hash
func (f *FileValueStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	data, ok := f.chunks[h]

	if !ok {
		return chunks.EmptyChunk, nil
	} else {
		return chunks.NewChunkWithHash(h, data), nil
	}
}

// GetMany gets chunks by their hashes. Chunks that are found are written to the channel.
func (f *FileValueStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *chunks.Chunk)) error {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	for h := range hashes {
		data, ok := f.chunks[h]

		if ok {
			ch := chunks.NewChunkWithHash(h, data)
			found(ctx, &ch)
		}
	}

	return nil
}

// Has returns true if a chunk is present in the store, false if not
func (f *FileValueStore) Has(ctx context.Context, h hash.Hash) (bool, error) {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	_, ok := f.chunks[h]
	return ok, nil
}

func (f *FileValueStore) CacheHas(h hash.Hash) bool {
	_, ok := f.chunks[h]
	return ok
}

func (f *FileValueStore) PurgeCaches() {
}

// HasMany returns the set of hashes that are absent from the store
func (f *FileValueStore) HasMany(ctx context.Context, hashes hash.HashSet) (absent hash.HashSet, err error) {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	absent = make(hash.HashSet, len(hashes))
	for h := range hashes {
		_, ok := f.chunks[h]

		if !ok {
			absent[h] = struct{}{}
		}
	}

	return absent, nil
}

func (f *FileValueStore) errorIfDangling(ctx context.Context, addrs hash.HashSet) error {
	absent, err := f.HasMany(ctx, addrs)
	if err != nil {
		return err
	}
	if len(absent) != 0 {
		s := absent.String()
		return fmt.Errorf("Found dangling references to %s", s)
	}
	return nil
}

// Put puts a chunk into the store
func (f *FileValueStore) Put(ctx context.Context, c chunks.Chunk, getAddrs chunks.GetAddrsCurry) error {
	addrs := hash.NewHashSet()
	err := getAddrs(c)(ctx, addrs, f.CacheHas)
	if err != nil {
		return err
	}

	err = f.errorIfDangling(ctx, addrs)
	if err != nil {
		return err
	}

	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	f.chunks[c.Hash()] = c.Data()
	return nil
}

// Version returns the nbf version string
func (f *FileValueStore) Version() string {
	return f.nbf.VersionString()
}

func (f *FileValueStore) AccessMode() chunks.ExclusiveAccessMode {
	return chunks.ExclusiveAccessMode_Shared
}

// Rebase brings this ChunkStore into sync with the persistent storage's current root.  Has no impact here
func (f *FileValueStore) Rebase(ctx context.Context) error {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()
	return nil
}

// Root returns the root hash
func (f *FileValueStore) Root(ctx context.Context) (hash.Hash, error) {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()
	return f.rootHash, nil
}

// Commit sets the root hash
func (f *FileValueStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	if f.rootHash == last {
		f.rootHash = current
		return true, nil
	}

	return false, nil
}

// Stats doesn't do anything
func (f *FileValueStore) Stats() interface{} {
	return nil
}

// StatsSummary doesn't do anything
func (f *FileValueStore) StatsSummary() string {
	return ""
}

// Close doesn't do anything
func (f *FileValueStore) Close() error {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	return nil
}

func (f *FileValueStore) numChunks() int {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	return len(f.chunks)
}

func (f *FileValueStore) iterChunks(cb func(ch chunks.Chunk) error) error {
	f.chunkLock.Lock()
	defer f.chunkLock.Unlock()

	hashes := make(hash.HashSlice, 0, len(f.chunks))
	for h := range f.chunks {
		hashes = append(hashes, h)
	}

	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i].Less(hashes[j])
	})

	for _, h := range hashes {
		data := f.chunks[h]
		err := cb(chunks.NewChunkWithHash(h, data))

		if err != nil {
			return err
		}
	}

	return nil
}

func (f *FileValueStore) PersistGhostHashes(ctx context.Context, refs hash.HashSet) error {
	// Current unimplemented, but may be useful for testing someday.
	panic("not implemented")
}
