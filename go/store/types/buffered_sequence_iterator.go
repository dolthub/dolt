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

type bufferedSequenceIterator struct {
	parent 	   *bufferedSequenceIterator
	seq    	   sequence
	idx    	   int
	seqLen 	   int
	// determines the buffering behavior by tree level
	batchSizes []uint64
}

// newbufferedSequenceIterator creates a bufCursor on seq positioned at idx.
// If idx < 0, count backward from the end of seq.
func newbufferedSequenceIterator(parent *bufferedSequenceIterator, seq sequence, idx int) *bufferedSequenceIterator {
	d.PanicIfTrue(seq == nil)
	seqLen := seq.seqLen()
	if idx < 0 {
		idx += seqLen
		d.PanicIfFalse(idx >= 0)
	}
	batchSizes := []uint64{1024, 256, 64, 16, 4}
	return &bufferedSequenceIterator{parent, seq, idx, seqLen, batchSizes}
}

func (cur *bufferedSequenceIterator) length() int {
	return cur.seqLen
}

// syncAdvance loads the sequence that the bufCursor index points to.
// It's called whenever the bufCursor advances/retreats to a different chunk.
func (cur *bufferedSequenceIterator) sync(ctx context.Context) error {
	d.PanicIfFalse(cur.parent != nil)

	var err error

	tl := int(cur.parent.seq.treeLevel())
	if tl > len(cur.batchSizes) {
		// no buffering
		cur.seq, err = cur.parent.getChildSequence(ctx)
		if err != nil {
			return err
		}
		cur.seqLen = cur.seq.seqLen()
		return nil
	}

	batch := cur.batchSizes[tl-1]
	if batch > uint64(cur.parent.seqLen - cur.parent.idx) {
		batch = uint64(cur.parent.seqLen - cur.parent.idx)
	}
	cur.seq, err = cur.parent.seq.getCompositeChildSequence(ctx, uint64(cur.parent.idx), batch)

	if err != nil {
		return err
	}

	cur.parent.idx += int(batch) - 1
	cur.seqLen = cur.seq.seqLen()

	return nil
}

func (cur *bufferedSequenceIterator) getChildSequence(ctx context.Context) (sequence, error) {
	return cur.seq.getChildSequence(ctx, cur.idx)
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

func (cur *bufferedSequenceIterator) retreat(ctx context.Context) (bool, error) {
	return cur.retreatMaybeAllowBeforeStart(ctx, true)
}

func (cur *bufferedSequenceIterator) retreatMaybeAllowBeforeStart(ctx context.Context, allowBeforeStart bool) (bool, error) {
	if cur.idx > 0 {
		cur.idx--
		return true, nil
	}

	if cur.idx == -1 {
		return false, nil
	}

	d.PanicIfFalse(0 == cur.idx)

	if cur.parent != nil {
		ok, err := cur.parent.retreatMaybeAllowBeforeStart(ctx, false)

		if err != nil {
			return false, err
		}

		if ok {
			err := cur.sync(ctx)

			if err != nil {
				return false, err
			}

			cur.idx = cur.length() - 1
			return true, nil
		}
	}

	if allowBeforeStart {
		cur.idx--
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

// newIteratorAtIndex creates a new bufCursor over seq positioned at idx.
//
// Implemented by searching down the tree to the leaf sequence containing idx. Each
// sequence bufCursor includes a back pointer to its parent so that it can follow the path
// to the next leaf chunk when the bufCursor exhausts the entries in the current chunk.
func newBufferedIteratorAtIndex(ctx context.Context, seq sequence, idx uint64) (*bufferedSequenceIterator, error) {
	var cur *bufferedSequenceIterator
	for {
		cur = newbufferedSequenceIterator(cur, seq, 0)
		delta, err := advanceBufferedCursorToOffset(cur, idx)

		if err != nil {
			return nil, err
		}

		idx = idx - delta

		var cs sequence
		tl := int(cur.seq.treeLevel())
		if tl <= len(cur.batchSizes) && cur.seq.treeLevel() > 0 {
			batch := cur.batchSizes[tl-1]
			if batch > uint64(cur.seqLen - cur.idx) {
				batch = uint64(cur.seqLen - cur.idx)
			}
			cs, err = cur.seq.getCompositeChildSequence(ctx, uint64(cur.idx), batch)
			cur.idx += cur.seqLen - cur.idx
		} else {
			// don't buffer
			cs, err = cur.getChildSequence(ctx)
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

func advanceBufferedCursorToOffset(cur *bufferedSequenceIterator, idx uint64) (uint64, error) {
	seq := cur.seq

	if ms, ok := seq.(metaSequence); ok {
		// For a meta sequence, advance the bufCursor to the smallest position where idx < seq.cumulativeNumLeaves()
		cur.idx = 0
		cum := uint64(0)

		seqLen := ms.seqLen()
		// Advance the bufCursor to the meta-sequence tuple containing idx
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
