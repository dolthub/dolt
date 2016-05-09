package types

import (
	"crypto/sha1"
	"sort"

	"github.com/attic-labs/noms/d"
)

type orderedSequence interface {
	sequence
	getKey(idx int) Value
}

type orderedMetaSequence struct {
	metaSequenceObject
}

func (oms orderedMetaSequence) getKey(idx int) Value {
	return oms.tuples[idx].value
}

func newCursorAtKey(seq orderedSequence, key Value, forInsertion bool, last bool) *sequenceCursor {
	var cur *sequenceCursor = nil
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
	keyElemIsOrdered := seq.Type().Desc.(CompoundDesc).ElemTypes[0].IsOrdered()
	_, seqIsMeta := seq.(metaSequence)
	keyRef := key.Ref()

	// Default order by value ref
	searchFn := func(i int) bool {
		return !seq.getKey(i).Ref().Less(keyRef)
	}

	if keyElemIsOrdered {
		// Order by native value for scalars
		orderedKey := key.(OrderedValue)
		searchFn = func(i int) bool {
			return !seq.getKey(i).(OrderedValue).Less(orderedKey)
		}
	} else if seqIsMeta {
		// For non-native values, meta sequences will hold types.Ref rather than the value
		searchFn = func(i int) bool {
			return !seq.getKey(i).(Ref).TargetRef().Less(keyRef)
		}
	}

	cur.idx = sort.Search(seq.seqLen(), searchFn)

	if cur.idx == seq.seqLen() && lastPositionIfNotFound {
		d.Chk.True(cur.idx > 0)
		cur.idx--
	}

	return cur.idx < seq.seqLen()
}

func isSequenceOrderedByIndexedType(t *Type) bool {
	return t.Desc.(CompoundDesc).ElemTypes[0].IsOrdered()
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

		meta := newMetaSequenceFromData(tuples, t, vr)
		return newMetaTuple(tuples.last().value, meta, NewTypedRefFromValue(meta), numLeaves), meta
	}
}
