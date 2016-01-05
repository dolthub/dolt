package types

import (
	"crypto/sha1"

	"github.com/attic-labs/noms/chunks"
)

func newIndexedMetaSequenceBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(objectWindowSize, sha1.Size, objectPattern, func(item sequenceItem) []byte {
		digest := item.(metaTuple).childRef.Digest()
		return digest[:]
	})
}

func newIndexedMetaSequenceChunkFn(t Type, cs chunks.ChunkStore) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		tuples := make(metaSequenceData, len(items))
		for i, v := range items {
			mt := v.(metaTuple)
			tuples[i] = mt
			// Immediately write intermediate chunks. It would be better to defer writing any chunks until commit, see https://github.com/attic-labs/noms/issues/710.
			WriteValue(mt.child, cs)
		}

		meta := newMetaSequenceFromData(tuples, t, cs)
		return metaTuple{meta, meta.Ref(), Uint64(tuples.uint64ValuesSum())}, meta
	}
}
