// Copyright 2020 Liquidata, Inc.
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

package types

import (
	"context"

	"github.com/liquidata-inc/dolt/go/store/d"
)

type bufferedSequenceIterator struct {
	parent    *bufferedSequenceIterator
	seq       sequence
	idx       int
	seqLen    int
	batchSize uint64
}

// newbufferedSequenceIterator creates a bufCursor on seq positioned at idx.
// If idx < 0, count backward from the end of seq.
func newbufferedSequenceIterator(parent *bufferedSequenceIterator, seq sequence, idx int, batchSize uint64) *bufferedSequenceIterator {
	d.PanicIfTrue(seq == nil)
	seqLen := seq.seqLen()
	if idx < 0 {
		idx += seqLen
		d.PanicIfFalse(idx >= 0)
	}
	return &bufferedSequenceIterator{parent, seq, idx, seqLen, batchSize}
}

func (cur *bufferedSequenceIterator) length() int {
	return cur.seqLen
}

// sync loads the sequence that the bufCursor index points to.
// It's called whenever the bufCursor advances/retreats to a different chunk.
func (cur *bufferedSequenceIterator) sync(ctx context.Context) error {
	d.PanicIfFalse(cur.parent != nil)
	var err error

	if cur.batchSize > 0 {
		batch := cur.batchSize
		if batch > uint64(cur.parent.seqLen-cur.parent.idx) {
			batch = uint64(cur.parent.seqLen - cur.parent.idx)
		}
		cur.seq, err = cur.parent.seq.getCompositeChildSequence(ctx, uint64(cur.parent.idx), batch)
		if err != nil {
			return err
		}

		cur.parent.idx += int(batch) - 1
	} else {
		// no buffering
		cur.seq, err = cur.parent.seq.getChildSequence(ctx, cur.parent.idx)
		if err != nil {
			return err
		}
	}

	cur.seqLen = cur.seq.seqLen()

	return nil
}

func (cur *bufferedSequenceIterator) current() (sequenceItem, error) {
	d.PanicIfFalse(cur.valid())
	return cur.seq.getItem(cur.idx)
}

func (cur *bufferedSequenceIterator) valid() bool {
	return cur.idx >= 0 && cur.idx < cur.length()
}

func (cur *bufferedSequenceIterator) advance(ctx context.Context) (bool, error) {
	return cur.advanceMaybeAllowPastEnd(ctx, true)
}

func (cur *bufferedSequenceIterator) advanceMaybeAllowPastEnd(ctx context.Context, allowPastEnd bool) (bool, error) {
	if cur.idx < cur.length()-1 {
		cur.idx++
		return true, nil
	}

	if cur.idx == cur.length() {
		return false, nil
	}

	if cur.parent != nil {
		ok, err := cur.parent.advanceMaybeAllowPastEnd(ctx, false)

		if err != nil {
			return false, err
		}

		if ok {
			// at end of current leaf chunk and there are more
			err := cur.sync(ctx)

			if err != nil {
				return false, err
			}

			cur.idx = 0
			return true, nil
		}
	}

	if allowPastEnd {
		cur.idx++
	}

	return false, nil
}

func (cur *bufferedSequenceIterator) iter(ctx context.Context, cb cursorIterCallback) error {
	for cur.valid() {
		item, err := cur.current()

		if err != nil {
			return err
		}

		stop, err := cb(item)

		if err != nil {
			return err
		}

		if stop {
			return nil
		}

		_, err = cur.advance(ctx)

		if err != nil {
			return err
		}
	}

	return nil
}

// newIteratorAtIndex creates a new buffered iterator over seq positioned at idx.
//
// Implemented by searching down the tree to the leaf sequence containing idx. Each
// sequence bufCursor includes a back pointer to its parent so that it can follow the path
// to the next leaf chunk when the bufCursor exhausts the entries in the current chunk.
func newBufferedIteratorAtIndex(ctx context.Context, seq sequence, idx uint64) (*bufferedSequenceIterator, error) {
	var cur *bufferedSequenceIterator
	batchSizes := []uint64{1024, 256, 64, 16, 4}
	for {
		tl := int(seq.treeLevel())
		if tl <= len(batchSizes) && tl > 0 {
			// for sequences at tree level i, buffer batchSizes[i-1]
			// chunks for child sequences at tree level i-1
			cur = newbufferedSequenceIterator(cur, seq, 0, batchSizes[tl-1])
		} else {
			cur = newbufferedSequenceIterator(cur, seq, 0, 0)
		}

		delta, err := advanceBufferedIteratorToOffset(cur, idx)
		if err != nil {
			return nil, err
		}

		idx = idx - delta

		var cs sequence
		if cur.batchSize > 0 {
			batch := cur.batchSize
			if batch > uint64(cur.seqLen-cur.idx) {
				batch = uint64(cur.seqLen - cur.idx)
			}
			cs, err = cur.seq.getCompositeChildSequence(ctx, uint64(cur.idx), batch)
			cur.idx += int(batch - 1)
		} else {
			// don't buffer
			cs, err = cur.seq.getChildSequence(ctx, cur.idx)
		}

		if err != nil {
			return nil, err
		}

		if cs == nil {
			break
		}
		seq = cs
	}
	d.PanicIfTrue(cur == nil)
	return cur, nil
}

func advanceBufferedIteratorToOffset(cur *bufferedSequenceIterator, idx uint64) (uint64, error) {
	seq := cur.seq

	if ms, ok := seq.(metaSequence); ok {
		// For a meta sequence, advance the buffered iterator
		// to the smallest position where idx < seq.cumulativeNumLeaves()
		cur.idx = 0
		cum := uint64(0)

		seqLen := ms.seqLen()
		// advance the buffered iterator to the meta-sequence tuple containing idx
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
