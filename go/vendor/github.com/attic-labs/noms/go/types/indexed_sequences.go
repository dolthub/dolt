// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/hash"
)

func newListMetaSequence(level uint64, tuples []metaTuple, vrw ValueReadWriter) metaSequence {
	return newMetaSequenceFromTuples(ListKind, level, tuples, vrw)
}

func newBlobMetaSequence(level uint64, tuples []metaTuple, vrw ValueReadWriter) metaSequence {
	return newMetaSequenceFromTuples(BlobKind, level, tuples, vrw)
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

		seqLen := ms.seqLen()
		// Advance the cursor to the meta-sequence tuple containing idx
		for cur.idx < seqLen-1 {
			numLeaves := ms.getNumLeavesAt(cur.idx)
			if uint64(idx) >= cum+numLeaves {
				cum += numLeaves
				cur.idx++
			} else {
				break
			}
		}

		return cum // number of leaves sequences BEFORE cur.idx in meta sequence
	}

	seqLen := seq.seqLen()
	cur.idx = int(idx)
	if cur.idx > seqLen {
		cur.idx = seqLen
	}
	return uint64(cur.idx)
}

func newIndexedMetaSequenceChunkFn(kind NomsKind, vrw ValueReadWriter) makeChunkFn {
	return func(level uint64, items []sequenceItem) (Collection, orderedKey, uint64) {
		tuples := make([]metaTuple, len(items))
		numLeaves := uint64(0)

		for i, v := range items {
			mt := v.(metaTuple)
			tuples[i] = mt
			numLeaves += mt.numLeaves()
		}

		var col Collection
		if kind == ListKind {
			col = newList(newListMetaSequence(level, tuples, vrw))
		} else {
			d.PanicIfFalse(BlobKind == kind)
			col = newBlob(newBlobMetaSequence(level, tuples, vrw))
		}
		return col, orderedKeyFromSum(tuples), numLeaves
	}
}

func orderedKeyFromSum(msd []metaTuple) orderedKey {
	sum := uint64(0)
	for _, mt := range msd {
		sum += mt.numLeaves()
	}
	return orderedKeyFromUint64(sum)
}

// LoadLeafNodes loads the set of leaf nodes which contain the items
// [startIdx -> endIdx).  Returns the set of nodes and the offset within
// the first sequence which corresponds to |startIdx|.
func LoadLeafNodes(cols []Collection, startIdx, endIdx uint64) ([]Collection, uint64) {
	vrw := cols[0].asSequence().valueReadWriter()
	d.PanicIfTrue(vrw == nil)

	if cols[0].asSequence().isLeaf() {
		for _, c := range cols {
			d.PanicIfFalse(c.asSequence().isLeaf())
		}

		return cols, startIdx
	}

	level := cols[0].asSequence().treeLevel()
	childTuples := []metaTuple{}

	cum := uint64(0)
	for _, c := range cols {
		s := c.asSequence()
		d.PanicIfFalse(s.treeLevel() == level)
		ms := s.(metaSequence)

		for _, mt := range ms.tuples() {
			numLeaves := mt.numLeaves()
			if cum == 0 && numLeaves <= startIdx {
				// skip tuples whose items are < startIdx
				startIdx -= numLeaves
				endIdx -= numLeaves
				continue
			}

			childTuples = append(childTuples, mt)
			cum += numLeaves
			if cum >= endIdx {
				break
			}
		}
	}

	hs := make(hash.HashSlice, len(childTuples))
	for i, mt := range childTuples {
		hs[i] = mt.ref().TargetHash()
	}

	// Fetch committed child sequences in a single batch
	readValues := vrw.ReadManyValues(hs)

	childCols := make([]Collection, len(readValues))
	for i, v := range readValues {
		childCols[i] = v.(Collection)
	}

	return LoadLeafNodes(childCols, startIdx, endIdx)
}
