package types

import "crypto/sha1"

func newIndexedMetaSequenceBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(objectWindowSize, sha1.Size, objectPattern, func(item sequenceItem) []byte {
		digest := item.(metaTuple).ChildRef().TargetRef().Digest()
		return digest[:]
	})
}

// If |sink| is not nil, chunks will be eagerly written as they're created. Otherwise they are
// written when the root is written.
func newIndexedMetaSequenceChunkFn(t Type, source ValueReader, sink ValueWriter) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		tuples := make(metaSequenceData, len(items))
		numLeaves := uint64(0)

		for i, v := range items {
			mt := v.(metaTuple)
			tuples[i] = mt
			numLeaves += mt.numLeaves
		}

		meta := newMetaSequenceFromData(tuples, t, source)
		if sink != nil {
			r := sink.WriteValue(meta)
			return newMetaTuple(Uint64(tuples.uint64ValuesSum()), nil, r, numLeaves), meta
		}
		return newMetaTuple(Uint64(tuples.uint64ValuesSum()), meta, Ref{}, numLeaves), meta
	}
}
