// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import "github.com/attic-labs/noms/go/d"

func newListMetaSequence(tuples []metaTuple, vr ValueReader) metaSequence {
	ts := make([]*Type, len(tuples))
	for i, mt := range tuples {
		// Ref<List<T>>
		ts[i] = mt.ref.Type().Desc.(CompoundDesc).ElemTypes[0].Desc.(CompoundDesc).ElemTypes[0]
	}
	t := MakeListType(MakeUnionType(ts...))
	return newMetaSequence(tuples, t, vr)
}

func newBlobMetaSequence(tuples []metaTuple, vr ValueReader) metaSequence {
	return newMetaSequence(tuples, BlobType, vr)
}

// advanceCursorToOffset advances the cursor as close as possible to idx
//
// If the cursor references a leaf sequence,
// 	advance to idx,
// 	and return the number of values preceding the idx
// If it references a meta-sequence,
// 	advance to the tuple containing idx,
// 	and return the number of leaf values preceding this tuple
func advanceCursorToOffset(cur *sequenceCursor, idx uint64) uint64 {
	seq := cur.seq

	if ms, ok := seq.(metaSequence); ok {
		// For a meta sequence, advance the cursor to the smallest position where idx < seq.cumulativeNumLeaves()
		cur.idx = 0
		cum := uint64(0)

		// Advance the cursor to the meta-sequence tuple containing idx
		for cur.idx < ms.seqLen()-1 && uint64(idx) >= cum+ms.tuples[cur.idx].numLeaves {
			cum += ms.tuples[cur.idx].numLeaves
			cur.idx++
		}

		return cum // number of leaves sequences BEFORE cur.idx in meta sequence
	}

	cur.idx = int(idx)
	if cur.idx > seq.seqLen() {
		cur.idx = seq.seqLen()
	}
	return uint64(cur.idx)
}

// If |sink| is not nil, chunks will be eagerly written as they're created. Otherwise they are
// written when the root is written.
func newIndexedMetaSequenceChunkFn(kind NomsKind, source ValueReader) makeChunkFn {
	return func(items []sequenceItem) (Collection, orderedKey, uint64) {
		tuples := make([]metaTuple, len(items))
		numLeaves := uint64(0)

		for i, v := range items {
			mt := v.(metaTuple)
			tuples[i] = mt
			numLeaves += mt.numLeaves
		}

		var col Collection
		if kind == ListKind {
			col = newList(newListMetaSequence(tuples, source))
		} else {
			d.PanicIfFalse(BlobKind == kind)
			col = newBlob(newBlobMetaSequence(tuples, source))
		}
		return col, orderedKeyFromSum(tuples), numLeaves
	}
}

func orderedKeyFromSum(msd []metaTuple) orderedKey {
	sum := uint64(0)
	for _, mt := range msd {
		sum += mt.numLeaves
	}
	return orderedKeyFromUint64(sum)
}
