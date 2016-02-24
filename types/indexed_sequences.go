package types

import (
	"crypto/sha1"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/ref"
)

func newIndexedMetaSequenceBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(objectWindowSize, sha1.Size, objectPattern, func(item sequenceItem) []byte {
		digest := item.(metaTuple).ChildRef().Digest()
		return digest[:]
	})
}

// If |sink| is not nil, chunks will be eagerly written as they're created. Otherwise they are
// written when the root is written.
func newIndexedMetaSequenceChunkFn(t Type, cs chunks.ChunkSource, sink chunks.ChunkSink) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		tuples := make(metaSequenceData, len(items))

		for i, v := range items {
			tuples[i] = v.(metaTuple)
		}

		meta := newMetaSequenceFromData(tuples, t, cs)
		if sink != nil {
			return metaTuple{nil, WriteValue(meta, sink), Uint64(tuples.uint64ValuesSum())}, meta
		}
		return metaTuple{meta, ref.Ref{}, Uint64(tuples.uint64ValuesSum())}, meta
	}
}
