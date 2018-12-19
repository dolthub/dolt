// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/d"
)

type orderedSequence interface {
	sequence
	getKey(idx int) orderedKey
	search(key orderedKey) int
}

func newSetMetaSequence(level uint64, tuples []metaTuple, vrw ValueReadWriter) metaSequence {
	return newMetaSequenceFromTuples(SetKind, level, tuples, vrw)
}

func newMapMetaSequence(level uint64, tuples []metaTuple, vrw ValueReadWriter) metaSequence {
	return newMetaSequenceFromTuples(MapKind, level, tuples, vrw)
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
		cur = newSequenceCursor(cur, seq, idx)
		if key != emptyKey {
			if !seekTo(cur, key, forInsertion && !seq.isLeaf()) {
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
	cur.idx = seq.search(key)
	seqLen := seq.seqLen()
	if cur.idx == seqLen && lastPositionIfNotFound {
		d.PanicIfFalse(cur.idx > 0)
		cur.idx--
	}

	return cur.idx < seqLen
}

// Gets the key used for ordering the sequence at current index.
func getCurrentKey(cur *sequenceCursor) orderedKey {
	seq, ok := cur.seq.(orderedSequence)
	if !ok {
		d.Panic("need an ordered sequence here")
	}
	return seq.getKey(cur.idx)
}

func getMapValue(cur *sequenceCursor) Value {
	if ml, ok := cur.seq.(mapLeafSequence); ok {
		return ml.getValue(cur.idx)
	}

	return nil
}

// If |vw| is not nil, chunks will be eagerly written as they're created. Otherwise they are
// written when the root is written.
func newOrderedMetaSequenceChunkFn(kind NomsKind, vrw ValueReadWriter) makeChunkFn {
	return func(level uint64, items []sequenceItem) (Collection, orderedKey, uint64) {
		tuples := make([]metaTuple, len(items))
		numLeaves := uint64(0)

		var lastKey orderedKey
		for i, v := range items {
			mt := v.(metaTuple)
			key := mt.key()
			d.PanicIfFalse(lastKey == emptyKey || lastKey.Less(key))
			lastKey = key
			tuples[i] = mt // chunk is written when the root sequence is written
			numLeaves += mt.numLeaves()
		}

		var col Collection
		if kind == SetKind {
			col = newSet(newSetMetaSequence(level, tuples, vrw))
		} else {
			d.PanicIfFalse(MapKind == kind)
			col = newMap(newMapMetaSequence(level, tuples, vrw))
		}

		return col, tuples[len(tuples)-1].key(), numLeaves
	}
}
