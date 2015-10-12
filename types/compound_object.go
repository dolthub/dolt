package types

import (
	"crypto/sha1"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/kch42/buzhash"
	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

const (
	objectWindowSize = 8 * sha1.Size
	objectPattern    = uint32(1<<6 - 1) // Average size of 64 elements
)

type compoundObject struct {
	offsets []uint64
	futures []Future
	ref     *ref.Ref
	cs      chunks.ChunkSource
}

func (co compoundObject) Len() uint64 {
	return co.offsets[len(co.offsets)-1]
}

func (co compoundObject) Chunks() (futures []Future) {
	for _, f := range co.futures {
		futures = appendChunks(futures, f)
	}
	return
}

type compoundObjectToFuture func(co compoundObject) Future

func compoundObjectToBlobFuture(co compoundObject) Future {
	return futureFromValue(compoundBlob{co})
}

// splitCompoundObject chunks a compound list/blob into smaller compound
// lists/blobs. If no split was made the same compoundObject is returned.
func splitCompoundObject(co compoundObject, toFuture compoundObjectToFuture) compoundObject {
	offsets := []uint64{}
	futures := []Future{}

	startIndex := uint64(0)
	h := buzhash.NewBuzHash(objectWindowSize)

	for i := 0; i < len(co.offsets); i++ {
		future := co.futures[i]
		digest := future.Ref().Digest()
		_, err := h.Write(digest[:])
		d.Chk.NoError(err)
		if h.Sum32()&objectPattern == objectPattern {
			h = buzhash.NewBuzHash(objectWindowSize)
			future := makeSubObject(co, startIndex, uint64(i)+1, toFuture)
			startIndex = uint64(i) + 1
			offsets = append(offsets, co.offsets[i])
			futures = append(futures, future)
		}
	}

	// No split, use original.
	if startIndex == 0 {
		return co
	}

	// Add remaining.
	if startIndex != uint64(len(co.offsets)) {
		future := makeSubObject(co, startIndex, uint64(len(co.offsets)), toFuture)
		offsets = append(offsets, co.offsets[len(co.offsets)-1])
		futures = append(futures, future)
	}

	// Single chunk, use original.
	if len(offsets) == 1 {
		return co
	}

	// It is possible that the splitting the object produces the exact same
	// compound object.
	if len(offsets) == len(co.offsets) {
		return co
	}

	// Split again.
	return splitCompoundObject(compoundObject{offsets, futures, &ref.Ref{}, co.cs}, toFuture)
}

func makeSubObject(co compoundObject, startIndex, endIndex uint64, toFuture compoundObjectToFuture) Future {
	d.Chk.True(endIndex-startIndex > 0)
	if endIndex-startIndex == 1 {
		return co.futures[startIndex]
	}

	futures := make([]Future, endIndex-startIndex)
	copy(futures, co.futures[startIndex:endIndex])
	offsets := make([]uint64, endIndex-startIndex)
	startOffset := uint64(0)
	if startIndex > 0 {
		startOffset = co.offsets[startIndex-1]
	}
	for i := startIndex; i < endIndex; i++ {
		offsets[i-startIndex] = co.offsets[i] - startOffset
	}
	return toFuture(compoundObject{offsets, futures, &ref.Ref{}, co.cs})
}
