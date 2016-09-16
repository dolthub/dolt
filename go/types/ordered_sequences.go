// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"sort"

	"github.com/attic-labs/noms/go/d"
)

type orderedSequence interface {
	sequence
	getKey(idx int) orderedKey
}

func newSetMetaSequence(tuples []metaTuple, vr ValueReader) metaSequence {
	ts := make([]*Type, len(tuples))
	for i, mt := range tuples {
		// Ref<Set<T>>
		ts[i] = mt.ref.Type().Desc.(CompoundDesc).ElemTypes[0].Desc.(CompoundDesc).ElemTypes[0]
	}
	t := MakeSetType(MakeUnionType(ts...))
	return newMetaSequence(tuples, t, vr)
}

func newMapMetaSequence(tuples []metaTuple, vr ValueReader) metaSequence {
	kts := make([]*Type, len(tuples))
	vts := make([]*Type, len(tuples))
	for i, mt := range tuples {
		// Ref<Map<K, V>>
		ets := mt.ref.Type().Desc.(CompoundDesc).ElemTypes[0].Desc.(CompoundDesc).ElemTypes
		kts[i] = ets[0]
		vts[i] = ets[1]
	}
	t := MakeMapType(MakeUnionType(kts...), MakeUnionType(vts...))
	return newMetaSequence(tuples, t, vr)
}

func newCursorAtValue(seq orderedSequence, val Value, forInsertion bool, last bool) *sequenceCursor {
	var key orderedKey
	if val != nil {
		key = newOrderedKey(val)
	}
	return newCursorAt(seq, key, forInsertion, last)
}

func newCursorAt(seq orderedSequence, key orderedKey, forInsertion bool, last bool) *sequenceCursor {
	var cur *sequenceCursor
	for {
		idx := 0
		if last {
			idx = -1
		}
		seqIsMeta := isMetaSequence(seq)
		cur = newSequenceCursor(cur, seq, idx)
		if key != emptyKey {
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

	d.PanicIfFalse(cur != nil)
	return cur
}

func seekTo(cur *sequenceCursor, key orderedKey, lastPositionIfNotFound bool) bool {
	seq := cur.seq.(orderedSequence)

	// Find smallest idx in seq where key(idx) >= key
	cur.idx = sort.Search(seq.seqLen(), func(i int) bool {
		return !seq.getKey(i).Less(key)
	})

	if cur.idx == seq.seqLen() && lastPositionIfNotFound {
		d.PanicIfFalse(cur.idx > 0)
		cur.idx--
	}

	return cur.idx < seq.seqLen()
}

// Gets the key used for ordering the sequence at current index.
func getCurrentKey(cur *sequenceCursor) orderedKey {
	seq, ok := cur.seq.(orderedSequence)
	d.PanicIfFalse(ok, "need an ordered sequence here")
	return seq.getKey(cur.idx)
}

// If |vw| is not nil, chunks will be eagerly written as they're created. Otherwise they are
// written when the root is written.
func newOrderedMetaSequenceChunkFn(kind NomsKind, vr ValueReader) makeChunkFn {
	return func(items []sequenceItem) (Collection, orderedKey, uint64) {
		tuples := make([]metaTuple, len(items))
		numLeaves := uint64(0)

		for i, v := range items {
			mt := v.(metaTuple)
			tuples[i] = mt // chunk is written when the root sequence is written
			numLeaves += mt.numLeaves
		}

		var col Collection
		if kind == SetKind {
			col = newSet(newSetMetaSequence(tuples, vr))
		} else {
			d.PanicIfFalse(MapKind == kind)
			col = newMap(newMapMetaSequence(tuples, vr))
		}

		return col, tuples[len(tuples)-1].key, numLeaves
	}
}
