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
	chunks  []ref.Ref
	ref     *ref.Ref
	cs      chunks.ChunkSource
}

func (co compoundObject) Len() uint64 {
	return co.offsets[len(co.offsets)-1]
}

func (co compoundObject) Chunks() []ref.Ref {
	return co.chunks
}

func (co compoundObject) ChildValues() []Value {
	res := make([]Value, len(co.chunks))
	for i, r := range co.chunks {
		res[i] = NewRefOfBlob(r)
	}
	return res
}

// splitCompoundObject chunks a compound list/blob into smaller compound
// lists/blobs. If no split was made the same compoundObject is returned.
func splitCompoundObject(co compoundObject, cs chunks.ChunkSink) compoundObject {
	offsets := []uint64{}
	chunks := []ref.Ref{}

	startIndex := uint64(0)
	h := buzhash.NewBuzHash(objectWindowSize)

	for i := 0; i < len(co.offsets); i++ {
		c := co.chunks[i]
		digest := c.Digest()
		_, err := h.Write(digest[:])
		d.Chk.NoError(err)
		if h.Sum32()&objectPattern == objectPattern {
			h = buzhash.NewBuzHash(objectWindowSize)
			c := makeSubObject(co, startIndex, uint64(i)+1, cs)
			startIndex = uint64(i) + 1
			offsets = append(offsets, co.offsets[i])
			chunks = append(chunks, c)
		}
	}

	// No split, use original.
	if startIndex == 0 {
		return co
	}

	// Add remaining.
	if startIndex != uint64(len(co.offsets)) {
		c := makeSubObject(co, startIndex, uint64(len(co.offsets)), cs)
		offsets = append(offsets, co.offsets[len(co.offsets)-1])
		chunks = append(chunks, c)
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
	return splitCompoundObject(compoundObject{offsets, chunks, &ref.Ref{}, co.cs}, cs)
}

func makeSubObject(co compoundObject, startIndex, endIndex uint64, cs chunks.ChunkSink) ref.Ref {
	d.Chk.True(endIndex-startIndex > 0)
	if endIndex-startIndex == 1 {
		return co.chunks[startIndex]
	}

	chunks := make([]ref.Ref, endIndex-startIndex)
	copy(chunks, co.chunks[startIndex:endIndex])
	offsets := make([]uint64, endIndex-startIndex)
	startOffset := uint64(0)
	if startIndex > 0 {
		startOffset = co.offsets[startIndex-1]
	}
	for i := startIndex; i < endIndex; i++ {
		offsets[i-startIndex] = co.offsets[i] - startOffset
	}
	return WriteValue(compoundBlob{compoundObject{offsets, chunks, &ref.Ref{}, co.cs}}, cs)
}
