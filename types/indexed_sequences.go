package types

import (
	"crypto/sha1"

	"github.com/attic-labs/noms/ref"
)

func newIndexedMetaSequenceBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(objectWindowSize, sha1.Size, objectPattern, func(item sequenceItem) []byte {
		digest := item.(metaTuple).ChildRef().Digest()
		return digest[:]
	})
}

func newIndexedMetaSequenceChunkFn(t Type) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		tuples := make(metaSequenceData, len(items))

		for i, v := range items {
			tuples[i] = v.(metaTuple) // chunk is written when the root sequence is written
		}

		meta := newMetaSequenceFromData(tuples, t, nil)
		return metaTuple{meta, ref.Ref{}, Uint64(tuples.uint64ValuesSum())}, meta
	}
}
