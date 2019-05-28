package remotestorage

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"sync"
)

type mapChunkCache struct {
	mu          *sync.Mutex
	hashToChunk map[hash.Hash]chunks.Chunk
	toFlush     map[hash.Hash]chunks.Chunk
}

func newMapChunkCache() *mapChunkCache {
	return &mapChunkCache{
		&sync.Mutex{},
		make(map[hash.Hash]chunks.Chunk),
		make(map[hash.Hash]chunks.Chunk),
	}
}

func (mcc *mapChunkCache) Put(chnks []chunks.Chunk) {
	mcc.mu.Lock()
	defer mcc.mu.Unlock()

	for i := 0; i < len(chnks); i++ {
		c := chnks[i]
		h := c.Hash()

		if curr, ok := mcc.hashToChunk[h]; ok {
			if !curr.IsEmpty() {
				continue
			}
		}

		mcc.hashToChunk[h] = c

		if !c.IsEmpty() {
			mcc.toFlush[h] = c
		}
	}
}

func (mcc *mapChunkCache) Get(hashes hash.HashSet) map[hash.Hash]chunks.Chunk {
	hashToChunk := make(map[hash.Hash]chunks.Chunk)

	mcc.mu.Lock()
	defer mcc.mu.Unlock()

	for h := range hashes {
		if c, ok := mcc.hashToChunk[h]; ok {
			hashToChunk[h] = c
		} else {
			hashToChunk[h] = chunks.EmptyChunk
		}
	}

	return hashToChunk
}

func (mcc *mapChunkCache) Has(hashes hash.HashSet) (absent hash.HashSet) {
	absent = make(hash.HashSet)

	mcc.mu.Lock()
	defer mcc.mu.Unlock()

	for h := range hashes {
		if _, ok := mcc.hashToChunk[h]; !ok {
			absent[h] = struct{}{}
		}
	}

	return absent
}

func (mcc *mapChunkCache) PutChunk(ch *chunks.Chunk) bool {
	mcc.mu.Lock()
	defer mcc.mu.Unlock()

	h := ch.Hash()
	if _, ok := mcc.hashToChunk[h]; !ok {
		mcc.hashToChunk[h] = *ch
		return true
	}

	return false
}

func (mcc *mapChunkCache) GetAndClearChunksToFlush() map[hash.Hash]chunks.Chunk {
	newToFlush := make(map[hash.Hash]chunks.Chunk)

	mcc.mu.Lock()
	defer mcc.mu.Unlock()

	toFlush := mcc.toFlush
	mcc.toFlush = newToFlush

	return toFlush
}
