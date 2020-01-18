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
	"fmt"

	"github.com/liquidata-inc/dolt/go/store/d"
)

const batchLevel = 2
const batchSize = uint64(1024)

// bufferedSequenceCursor explores a tree of sequence items.
type bufferedSequenceCursor struct {
	parent *bufferedSequenceCursor
	seq    sequence
	idx    int
	seqLen int
}

// newbufferedSequenceCursor creates a bufCursor on seq positioned at idx.
// If idx < 0, count backward from the end of seq.
func newbufferedSequenceCursor(parent *bufferedSequenceCursor, seq sequence, idx int) *bufferedSequenceCursor {
	d.PanicIfTrue(seq == nil)
	seqLen := seq.seqLen()
	if idx < 0 {
		idx += seqLen
		d.PanicIfFalse(idx >= 0)
	}

	return &bufferedSequenceCursor{parent, seq, idx, seqLen}
}

func (cur *bufferedSequenceCursor) length() int {
	return cur.seqLen
}

func (cur *bufferedSequenceCursor) getItem(idx int) (sequenceItem, error) {
	return cur.seq.getItem(idx)
}

// sync loads the sequence that the bufCursor index points to.
// It's called whenever the bufCursor advances/retreats to a different chunk.
func (cur *bufferedSequenceCursor) sync(ctx context.Context) error {
	d.PanicIfFalse(cur.parent != nil)

	var err error

	if cur.parent.seq.treeLevel() > batchLevel {
		cur.seq, err = cur.parent.getChildSequence(ctx)
		if err != nil {
			return err
		}
		cur.seqLen = cur.seq.seqLen()
		return nil
	}
	batch := batchSize
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

// getChildSequence retrieves the child at the current bufCursor position.
func (cur *bufferedSequenceCursor) getChildSequence(ctx context.Context) (sequence, error) {
	return cur.seq.getChildSequence(ctx, cur.idx)
}

// current returns the value at the current bufCursor position
func (cur *bufferedSequenceCursor) current() (sequenceItem, error) {
	d.PanicIfFalse(cur.valid())
	return cur.getItem(cur.idx)
}

func (cur *bufferedSequenceCursor) valid() bool {
	return cur.idx >= 0 && cur.idx < cur.length()
}

func (cur *bufferedSequenceCursor) indexInChunk() int {
	return cur.idx
}

func (cur *bufferedSequenceCursor) atLastItem() bool {
	return cur.idx == cur.length()-1
}

func (cur *bufferedSequenceCursor) advance(ctx context.Context) (bool, error) {
	return cur.advanceMaybeAllowPastEnd(ctx, true)
}

func (cur *bufferedSequenceCursor) advanceMaybeAllowPastEnd(ctx context.Context, allowPastEnd bool) (bool, error) {
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

func (cur *bufferedSequenceCursor) retreat(ctx context.Context) (bool, error) {
	return cur.retreatMaybeAllowBeforeStart(ctx, true)
}

func (cur *bufferedSequenceCursor) retreatMaybeAllowBeforeStart(ctx context.Context, allowBeforeStart bool) (bool, error) {
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

func (cur *bufferedSequenceCursor) String() string {
	m, err := newMap(cur.seq.(orderedSequence)).Hash(cur.seq.format())

	if err != nil {
		return fmt.Sprintf("error: %s", err.Error())
	}

	if cur.parent == nil {
		return fmt.Sprintf("%s (%d): %d", m.String(), cur.seq.seqLen(), cur.idx)
	}

	return fmt.Sprintf("%s (%d): %d -- %s", m.String(), cur.seq.seqLen(), cur.idx, cur.parent.String())
}

func (cur *bufferedSequenceCursor) compare(other *bufferedSequenceCursor) int {
	if cur.parent != nil {
		d.PanicIfFalse(other.parent != nil)
		p := cur.parent.compare(other.parent)
		if p != 0 {
			return p
		}

	}

	// TODO: It'd be nice here to assert that the two sequences are the same
	// but there isn't a good way to that at this point because the containing
	// collection of the sequence isn't available.
	d.PanicIfFalse(cur.seq.seqLen() == other.seq.seqLen())
	return cur.idx - other.idx
}

// iter iterates forward from the current position
func (cur *bufferedSequenceCursor) iter(ctx context.Context, cb cursorIterCallback) error {
	for cur.valid() {
		item, err := cur.getItem(cur.idx)

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

// newCursorAtIndex creates a new bufCursor over seq positioned at idx.
//
// Implemented by searching down the tree to the leaf sequence containing idx. Each
// sequence bufCursor includes a back pointer to its parent so that it can follow the path
// to the next leaf chunk when the bufCursor exhausts the entries in the current chunk.
func newBufferedCursorAtIndex(ctx context.Context, seq sequence, idx uint64) (*bufferedSequenceCursor, error) {
	var cur *bufferedSequenceCursor
	for {
		cur = newbufferedSequenceCursor(cur, seq, 0)
		delta, err := advanceBufferedCursorToOffset(cur, idx)

		if err != nil {
			return nil, err
		}

		idx = idx - delta

		cs, err := cur.getChildSequence(ctx)

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


func advanceBufferedCursorToOffset(cur *bufferedSequenceCursor, idx uint64) (uint64, error) {
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