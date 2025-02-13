package remotestorage

import (
	"errors"
	"sync"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

type WriteBuffer interface {
	Put(nbs.CompressedChunk) error
	GetAllAndClear() map[hash.Hash]nbs.CompressedChunk

	// ChunkStore clients expect to read their own writes before a commit.
	// On the get path, remotestorage should add pending chunks to its result
	// set. On the HasMany path, remotestorage should remove present chunks
	// from its absent set on the HasMany response.
	AddPendingChunks(h hash.HashSet, res map[hash.Hash]nbs.CompressedChunk)
	RemovePresentChunks(h hash.HashSet)
}

type noopWriteBuffer struct {
}
var _ WriteBuffer = noopWriteBuffer{}

func (noopWriteBuffer) Put(nbs.CompressedChunk) error {
	return errors.New("unsupported operation: write on a read-only remotestorage chunk store")
}

func (noopWriteBuffer) GetAllAndClear() map[hash.Hash]nbs.CompressedChunk {
	panic("attempt to upload chunks on a read-only remotestorage chunk store") 
}

func (noopWriteBuffer) AddPendingChunks(hash.HashSet, map[hash.Hash]nbs.CompressedChunk) {
}

func (noopWriteBuffer) RemovePresentChunks(hash.HashSet) {
}

// A simple WriteBuffer which buffers unlimited data in memory and
// waits to flush it.
type mapWriteBuffer struct {
	mu sync.Mutex
	chunks map[hash.Hash]nbs.CompressedChunk
}

func newMapWriteBuffer() *mapWriteBuffer {
	var ret mapWriteBuffer
	ret.chunks = make(map[hash.Hash]nbs.CompressedChunk)
	return &ret
}

func (b *mapWriteBuffer) Put(cc nbs.CompressedChunk) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.chunks[cc.H] = cc
	return nil
}

func (b *mapWriteBuffer) GetAllAndClear() map[hash.Hash]nbs.CompressedChunk {
	b.mu.Lock()
	defer b.mu.Unlock()
	ret := b.chunks
	b.chunks = make(map[hash.Hash]nbs.CompressedChunk)
	return ret
}

func (b *mapWriteBuffer) AddPendingChunks(hs hash.HashSet, res map[hash.Hash]nbs.CompressedChunk) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for h := range hs {
		cc, ok := b.chunks[h]
		if ok {
			res[h] = cc
		}
	}
}

func (b *mapWriteBuffer) RemovePresentChunks(absent hash.HashSet) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.chunks) < len(absent) {
		for h := range b.chunks {
			absent.Remove(h)
		}
	} else {
		var toRemove []hash.Hash
		for h := range absent {
			if _, ok := b.chunks[h]; ok {
				toRemove = append(toRemove, h)
			}
		}
		for _, h := range toRemove {
			absent.Remove(h)
		}
	}
}
