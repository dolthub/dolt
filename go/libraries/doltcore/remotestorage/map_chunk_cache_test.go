package remotestorage

import (
	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/hash"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

func genRandomChunks(n int) (hash.HashSet, []chunks.Chunk) {
	chks := make([]chunks.Chunk, n)
	hashes := make(hash.HashSet)
	for i := 0; i < n; i++ {
		size := int(rng.Int31n(100))
		bytes := make([]byte, size)
		for j := 0; j < size; j++ {
			bytes[j] = byte(rng.Int31n(255))
		}

		chk := chunks.NewChunk(bytes)
		chks[i] = chk

		hashes[chk.Hash()] = struct{}{}
	}

	return hashes, chks
}

func TestMapChunkCache(t *testing.T) {
	const chunkBatchSize = 10

	hashes, chks := genRandomChunks(chunkBatchSize)

	mapChunkCache := newMapChunkCache()
	mapChunkCache.Put(chks)
	hashToChunk := mapChunkCache.Get(hashes)

	if len(hashToChunk) != chunkBatchSize {
		t.Error("Did not read back all chunks")
	}

	absent := mapChunkCache.Has(hashes)

	if len(absent) != 0 {
		t.Error("Missing chunks that were added")
	}

	toFlush := mapChunkCache.GetAndClearChunksToFlush()

	if !reflect.DeepEqual(toFlush, hashToChunk) {
		t.Error("unexpected or missing chunks to flush")
	}

	moreHashes, moreChks := genRandomChunks(chunkBatchSize)

	joinedHashes := make(hash.HashSet)

	for h := range hashes {
		joinedHashes[h] = struct{}{}
	}

	for h := range moreHashes {
		joinedHashes[h] = struct{}{}
	}

	absent = mapChunkCache.Has(joinedHashes)

	if !reflect.DeepEqual(absent, moreHashes) {
		t.Error("unexpected absent hashset")
	}

	if mapChunkCache.PutChunk(&chks[0]) {
		t.Error("existing chunk should return false")
	}

	if !mapChunkCache.PutChunk(&moreChks[0]) {
		t.Error("new chunk should return true")
	}

	toFlush = mapChunkCache.GetAndClearChunksToFlush()

	if !reflect.DeepEqual(toFlush, map[hash.Hash]chunks.Chunk{moreChks[0].Hash(): moreChks[0]}) {
		t.Error("Missing or unexpected chunks to flush")
	}
}
