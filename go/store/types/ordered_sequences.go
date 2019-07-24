// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"

	"github.com/liquidata-inc/dolt/go/store/d"
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

func newCursorAtValue(ctx context.Context, seq orderedSequence, val Value, forInsertion bool, last bool) *sequenceCursor {
	var key orderedKey
	if val != nil {
		key = newOrderedKey(val, seq.format())
	}
	return newCursorAt(ctx, seq, key, forInsertion, last)
}

func newCursorAt(ctx context.Context, seq orderedSequence, key orderedKey, forInsertion bool, last bool) *sequenceCursor {
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

		cs := cur.getChildSequence(ctx)
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
			d.PanicIfFalse(lastKey == emptyKey || lastKey.Less(vrw.Format(), key))
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
