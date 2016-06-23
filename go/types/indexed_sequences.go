// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"crypto/sha1"
	"sort"

	"github.com/attic-labs/noms/go/d"
)

type indexedSequence interface {
	sequence
	getOffset(idx int) uint64
}

type indexedMetaSequence struct {
	metaSequenceObject
	offsets []uint64
}

func newListMetaSequence(tuples metaSequenceData, vr ValueReader) indexedMetaSequence {
	ts := make([]*Type, len(tuples))
	for i, mt := range tuples {
		// Ref<List<T>>
		ts[i] = mt.ref.Type().Desc.(CompoundDesc).ElemTypes[0].Desc.(CompoundDesc).ElemTypes[0]
	}
	t := MakeListType(MakeUnionType(ts...))
	return newIndexedMetaSequence(tuples, t, vr)
}

func newBlobMetaSequence(tuples metaSequenceData, vr ValueReader) indexedMetaSequence {
	return newIndexedMetaSequence(tuples, BlobType, vr)
}

func newIndexedMetaSequence(tuples metaSequenceData, t *Type, vr ValueReader) indexedMetaSequence {
	var offsets []uint64
	cum := uint64(0)
	for _, mt := range tuples {
		cum += mt.key.uint64Value()
		offsets = append(offsets, cum)
	}
	leafCount := offsets[len(offsets)-1]
	return indexedMetaSequence{
		metaSequenceObject{tuples, t, vr, leafCount},
		offsets,
	}
}

func (ims indexedMetaSequence) getOffset(idx int) uint64 {
	// TODO: precompute these on the construction
	offsets := []uint64{}
	cum := uint64(0)
	for _, mt := range ims.tuples {
		cum += mt.key.uint64Value()
		offsets = append(offsets, cum)
	}

	return ims.offsets[idx] - 1
}

func (ims indexedMetaSequence) getCompareFn(other sequence) compareFn {
	oms := other.(indexedMetaSequence)
	return func(idx, otherIdx int) bool {
		return ims.tuples[idx].ref.TargetHash() == oms.tuples[otherIdx].ref.TargetHash()
	}
}

func newCursorAtIndex(seq indexedSequence, idx uint64) *sequenceCursor {
	var cur *sequenceCursor
	for {
		cur = newSequenceCursor(cur, seq, 0)
		idx = idx - advanceCursorToOffset(cur, idx)
		cs := cur.getChildSequence()
		if cs == nil {
			break
		}
		seq = cs.(indexedSequence)
	}

	d.Chk.True(cur != nil)
	return cur
}

func advanceCursorToOffset(cur *sequenceCursor, idx uint64) uint64 {
	seq := cur.seq.(indexedSequence)
	cur.idx = sort.Search(seq.seqLen(), func(i int) bool {
		return uint64(idx) <= seq.getOffset(i)
	})
	if _, ok := seq.(metaSequence); ok {
		if cur.idx == seq.seqLen() {
			cur.idx = seq.seqLen() - 1
		}
	}
	if cur.idx == 0 {
		return 0
	}
	return seq.getOffset(cur.idx-1) + 1
}

func newIndexedMetaSequenceBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(objectWindowSize, sha1.Size, objectPattern, func(item sequenceItem) []byte {
		digest := item.(metaTuple).ref.TargetHash().Digest()
		return digest[:]
	})
}

// If |sink| is not nil, chunks will be eagerly written as they're created. Otherwise they are
// written when the root is written.
func newIndexedMetaSequenceChunkFn(kind NomsKind, source ValueReader, sink ValueWriter) makeChunkFn {
	return func(items []sequenceItem) (metaTuple, sequence) {
		tuples := make(metaSequenceData, len(items))
		numLeaves := uint64(0)

		for i, v := range items {
			mt := v.(metaTuple)
			tuples[i] = mt
			numLeaves += mt.numLeaves
		}

		var col Collection
		var metaSeq indexedMetaSequence
		if kind == ListKind {
			metaSeq = newListMetaSequence(tuples, source)
			col = newList(metaSeq)
		} else {
			d.Chk.True(BlobKind == kind)
			metaSeq = newBlobMetaSequence(tuples, source)
			col = newBlob(metaSeq)
		}
		if sink != nil {
			return newMetaTuple(sink.WriteValue(col), orderedKeyFromSum(tuples), numLeaves, nil), metaSeq
		}
		return newMetaTuple(NewRef(col), orderedKeyFromSum(tuples), numLeaves, col), metaSeq
	}
}

func orderedKeyFromSum(msd metaSequenceData) orderedKey {
	sum := uint64(0)
	for _, mt := range msd {
		sum += mt.key.uint64Value()
	}
	return orderedKeyFromUint64(sum)
}
