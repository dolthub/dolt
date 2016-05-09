package types

import (
	"crypto/sha1"
	"sort"

	"github.com/attic-labs/noms/d"
)

type indexedSequence interface {
	sequence
	getOffset(idx int) uint64
}

type indexedMetaSequence struct {
	metaSequenceObject
	offsets []uint64
}

func computeIndexedSequenceOffsets(tuples metaSequenceData) (offsets []uint64) {
	cum := uint64(0)
	for _, mt := range tuples {
		cum += mt.uint64Value()
		offsets = append(offsets, cum)
	}
	return
}

func (ims indexedMetaSequence) getOffset(idx int) uint64 {
	// TODO: precompute these on the construction
	offsets := []uint64{}
	cum := uint64(0)
	for _, mt := range ims.tuples {
		cum += mt.uint64Value()
		offsets = append(offsets, cum)
	}

	return ims.offsets[idx] - 1
}

func newCursorAtIndex(seq indexedSequence, idx uint64) *sequenceCursor {
	var cur *sequenceCursor = nil
	for {
		cur = newSequenceCursor(cur, seq, 0)
		idx = idx - advanceCursorToOffset(cur, idx)
		cs := cur.getChildSequence()
		if cs == nil {
			break
		}
		seq = cs.(indexedSequence)
	}

	d.Chk.NotNil(cur)
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
		digest := item.(metaTuple).ChildRef().TargetRef().Digest()
		return digest[:]
	})
}

// If |sink| is not nil, chunks will be eagerly written as they're created. Otherwise they are
// written when the root is written.
func newIndexedMetaSequenceChunkFn(t *Type, source ValueReader, sink ValueWriter) makeChunkFn {
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
			return newMetaTuple(Number(tuples.uint64ValuesSum()), nil, r, numLeaves), meta
		}
		return newMetaTuple(Number(tuples.uint64ValuesSum()), meta, NewTypedRefFromValue(meta), numLeaves), meta
	}
}
