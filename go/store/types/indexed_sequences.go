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
	"github.com/liquidata-inc/dolt/go/store/hash"
)

func newListMetaSequence(level uint64, tuples []metaTuple, vrw ValueReadWriter) (metaSequence, error) {
	return newMetaSequenceFromTuples(ListKind, level, tuples, vrw)
}

func newBlobMetaSequence(level uint64, tuples []metaTuple, vrw ValueReadWriter) (metaSequence, error) {
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
func advanceCursorToOffset(cur *sequenceCursor, idx uint64) (uint64, error) {
	seq := cur.seq

	if ms, ok := seq.(metaSequence); ok {
		// For a meta sequence, advance the cursor to the smallest position where idx < seq.cumulativeNumLeaves()
		cur.idx = 0
		cum := uint64(0)

		seqLen := ms.seqLen()
		// Advance the cursor to the meta-sequence tuple containing idx
		for cur.idx < seqLen-1 {
			numLeaves, err := ms.getNumLeavesAt(cur.idx)

			if err != nil {
				return 0, err
			}

			if uint64(idx) >= cum+numLeaves {
				cum += numLeaves
				cur.idx++
			} else {
				break
			}
		}

		return cum, nil // number of leaves sequences BEFORE cur.idx in meta sequence
	}

	seqLen := seq.seqLen()
	cur.idx = int(idx)
	if cur.idx > seqLen {
		cur.idx = seqLen
	}
	return uint64(cur.idx), nil
}

func newIndexedMetaSequenceChunkFn(kind NomsKind, vrw ValueReadWriter) makeChunkFn {
	return func(level uint64, items []sequenceItem) (Collection, orderedKey, uint64, error) {
		tuples := make([]metaTuple, len(items))
		numLeaves := uint64(0)

		for i, v := range items {
			mt := v.(metaTuple)
			tuples[i] = mt
			numLeaves += mt.numLeaves()
		}

		var col Collection
		if kind == ListKind {
			mseq, err := newListMetaSequence(level, tuples, vrw)

			if err != nil {
				return nil, orderedKey{}, 0, err
			}

			col = newList(mseq)
		} else {
			d.PanicIfFalse(BlobKind == kind)
			mseq, err := newBlobMetaSequence(level, tuples, vrw)

			if err != nil {
				return nil, orderedKey{}, 0, err
			}

			col = newBlob(mseq)
		}

		ordKey, err := orderedKeyFromSum(tuples, vrw.Format())

		if err != nil {
			return nil, orderedKey{}, 0, err
		}

		return col, ordKey, numLeaves, nil
	}
}

func orderedKeyFromSum(msd []metaTuple, nbf *NomsBinFormat) (orderedKey, error) {
	sum := uint64(0)
	for _, mt := range msd {
		sum += mt.numLeaves()
	}
	return orderedKeyFromUint64(sum, nbf)
}

// LoadLeafNodes loads the set of leaf nodes which contain the items
// [startIdx -> endIdx).  Returns the set of nodes and the offset within
// the first sequence which corresponds to |startIdx|.
func LoadLeafNodes(ctx context.Context, cols []Collection, startIdx, endIdx uint64) ([]Collection, uint64, error) {
	vrw := cols[0].asSequence().valueReadWriter()
	d.PanicIfTrue(vrw == nil)

	if cols[0].asSequence().isLeaf() {
		for _, c := range cols {
			d.PanicIfFalse(c.asSequence().isLeaf())
		}

		return cols, startIdx, nil
	}

	level := cols[0].asSequence().treeLevel()
	childTuples := []metaTuple{}

	cum := uint64(0)
	for _, c := range cols {
		s := c.asSequence()
		d.PanicIfFalse(s.treeLevel() == level)
		ms := s.(metaSequence)

		tups, err := ms.tuples()

		if err != nil {
			return nil, 0, err
		}

		for _, mt := range tups {
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
	readValues, err := vrw.ReadManyValues(ctx, hs)

	if err != nil {
		return nil, 0, err
	}

	childCols := make([]Collection, len(readValues))
	for i, v := range readValues {
		childCols[i] = v.(Collection)
	}

	return LoadLeafNodes(ctx, childCols, startIdx, endIdx)
}
