package types

import (
	"crypto/sha1"
	"sort"

	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/ref"
)

type orderedSequence interface {
	sequence
	getKey(idx int) Value
}

type orderedMetaSequence struct {
	metaSequenceObject
	leafCount uint64
}

func newOrderedMetaSequence(tuples metaSequenceData, t *Type, vr ValueReader) orderedMetaSequence {
	leafCount := uint64(0)
	for _, mt := range tuples {
		leafCount += mt.numLeaves
	}

	return orderedMetaSequence{
		metaSequenceObject{tuples, t, vr},
		leafCount,
	}
}

func (oms orderedMetaSequence) numLeaves() uint64 {
	return oms.leafCount
}

func (oms orderedMetaSequence) getKey(idx int) Value {
	return oms.tuples[idx].value
}

func newCursorAtKey(seq orderedSequence, key Value, forInsertion bool, last bool) *sequenceCursor {
	var cur *sequenceCursor
	for {
		idx := 0
		if last {
			idx = -1
		}
		_, seqIsMeta := seq.(metaSequence)
		cur = newSequenceCursor(cur, seq, idx)
		if key != nil {
			if !seekTo(cur, key, forInsertion && seqIsMeta) {
				return cur
			}
		}

		cs := cur.getChildSequence()
		if cs == nil {
			break
		}
		seq = cs.(orderedSequence)
	}

	d.Chk.NotNil(cur)
	return cur
}

func seekTo(cur *sequenceCursor, key Value, lastPositionIfNotFound bool) bool {
	seq := cur.seq.(orderedSequence)
	keyIsOrderedByValue := isKindOrderedByValue(key.Type().Kind())
	_, seqIsMeta := seq.(metaSequence)
	var keyRef ref.Ref

	var searchFn func(i int) bool

	if seqIsMeta {
		if !keyIsOrderedByValue {
			keyRef = key.Ref()
		}
		// For non-native values, meta sequences will hold types.Ref rather than the value
		searchFn = func(i int) bool {
			sk := seq.getKey(i)
			if sr, ok := sk.(Ref); ok {
				if keyIsOrderedByValue {
					return true // Values > ordered
				}
				return !sr.TargetRef().Less(keyRef)
			}
			return !sk.Less(key)
		}
	} else {
		searchFn = func(i int) bool {
			return !seq.getKey(i).Less(key)
		}
	}

	cur.idx = sort.Search(seq.seqLen(), searchFn)

	if cur.idx == seq.seqLen() && lastPositionIfNotFound {
		d.Chk.True(cur.idx > 0)
		cur.idx--
	}

	return cur.idx < seq.seqLen()
}

func newOrderedMetaSequenceBoundaryChecker() boundaryChecker {
	return newBuzHashBoundaryChecker(orderedSequenceWindowSize, sha1.Size, objectPattern, func(item sequenceItem) []byte {
		digest := item.(metaTuple).ChildRef().TargetRef().Digest()
		return digest[:]
	})
}

func newOrderedMetaSequenceChunkFn(t *Type, vr ValueReader) makeChunkFn {
	return func(items []sequenceItem) (sequenceItem, Value) {
		tuples := make(metaSequenceData, len(items))
		numLeaves := uint64(0)

		for i, v := range items {
			mt := v.(metaTuple)
			tuples[i] = mt // chunk is written when the root sequence is written
			numLeaves += mt.numLeaves
		}

		metaSeq := newOrderedMetaSequence(tuples, t, vr)
		var col Collection
		if t.Kind() == SetKind {
			col = newSet(metaSeq)
		} else {
			d.Chk.Equal(MapKind, t.Kind())
			col = newMap(metaSeq)
		}

		return newMetaTuple(tuples.last().value, col, NewRef(col), numLeaves), col
	}
}
