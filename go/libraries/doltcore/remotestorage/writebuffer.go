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

package remotestorage

import (
	"errors"
	"sync"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

type WriteBuffer interface {
	// Add a compressed chunk to the write buffer. It will be
	// returned from future calls to |GetAllForWrite| until a
	// write is successful.
	Put(nbs.CompressedChunk) error

	// Returns the current set of written chunks.  After this
	// returns, concurrent calls to other methods may block until
	// |WriteCompleted| is called.  Calls to |GetAllForWrite| must
	// be followed by a call to |WriteCompleted| once the write
	// attempt is finished.
	GetAllForWrite() map[hash.Hash]nbs.CompressedChunk

	// Called after a call to |GetAllForWrite|, this records
	// success or failure of the write operation.  If the write
	// operation was successful, then the written chunks are now
	// in the upstream and they can be cleared from the write
	// buffer. Otherwise, the written chunks are retained in the
	// write buffer so that the write can be retried.
	WriteCompleted(success bool)

	// ChunkStore clients expect to read their own writes before a
	// commit.  On the get path, remotestorage should add buffered
	// chunks matching a given |query| to its |result|. On the
	// HasMany path, remotestorage should remove present chunks
	// from its absent set on the HasMany response.
	AddBufferedChunks(query hash.HashSet, result map[hash.Hash]nbs.ToChunker)
	// Removes the addresses of any buffered chunks from |hashes|.
	// Used to filter the |absent| response of a HasMany call so
	// that buffered chunks are not considered absent.
	RemovePresentChunks(hashes hash.HashSet)
}

type noopWriteBuffer struct {
}

var _ WriteBuffer = noopWriteBuffer{}

func (noopWriteBuffer) Put(nbs.CompressedChunk) error {
	return errors.New("unsupported operation: write on a read-only remotestorage chunk store")
}

func (noopWriteBuffer) GetAllForWrite() map[hash.Hash]nbs.CompressedChunk {
	panic("attempt to upload chunks on a read-only remotestorage chunk store")
}

func (noopWriteBuffer) WriteCompleted(success bool) {
	panic("call to WriteCompleted on a noopWriteBuffer")
}

func (noopWriteBuffer) AddBufferedChunks(hash.HashSet, map[hash.Hash]nbs.ToChunker) {
}

func (noopWriteBuffer) RemovePresentChunks(hash.HashSet) {
}

// A simple WriteBuffer which buffers unlimited data in memory and
// waits to flush it.
type mapWriteBuffer struct {
	mu   sync.Mutex
	cond sync.Cond
	// Set when an outstanding write is in progress, |Put| will
	// block against this. Reset by |WriteCompleted| after the
	// appropriate updates to |chunks| have been made.
	writing bool
	chunks  map[hash.Hash]nbs.CompressedChunk
}

func newMapWriteBuffer() *mapWriteBuffer {
	ret := &mapWriteBuffer{
		chunks: make(map[hash.Hash]nbs.CompressedChunk),
	}
	ret.cond.L = &ret.mu
	return ret
}

func (b *mapWriteBuffer) Put(cc nbs.CompressedChunk) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.writing {
		b.cond.Wait()
	}
	b.chunks[cc.H] = cc
	return nil
}

func (b *mapWriteBuffer) GetAllForWrite() map[hash.Hash]nbs.CompressedChunk {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.writing {
		b.cond.Wait()
	}
	b.writing = true
	return b.chunks
}

func (b *mapWriteBuffer) WriteCompleted(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.writing {
		panic("mapWriteBuffer got WriteCompleted while no write was in progress")
	}
	b.writing = false
	if success {
		b.chunks = make(map[hash.Hash]nbs.CompressedChunk)
	}
	b.cond.Broadcast()
}

func (b *mapWriteBuffer) AddBufferedChunks(hs hash.HashSet, res map[hash.Hash]nbs.ToChunker) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for h := range hs {
		cc, ok := b.chunks[h]
		if ok {
			res[h] = cc
		}
	}
}

func (b *mapWriteBuffer) RemovePresentChunks(hashes hash.HashSet) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.chunks) < len(hashes) {
		for h := range b.chunks {
			hashes.Remove(h)
		}
	} else {
		var toRemove []hash.Hash
		for h := range hashes {
			if _, ok := b.chunks[h]; ok {
				toRemove = append(toRemove, h)
			}
		}
		for _, h := range toRemove {
			hashes.Remove(h)
		}
	}
}
