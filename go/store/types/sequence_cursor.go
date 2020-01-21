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

type sequenceIterator interface {
	valid() bool
	current() (sequenceItem, error)
	advance(ctx context.Context) (bool, error)
	retreat(ctx context.Context) (bool, error)
	iter(ctx context.Context, cb cursorIterCallback) error
}

// sequenceCursor explores a tree of sequence items.
type sequenceCursor struct {
	parent *sequenceCursor
	seq    sequence
	idx    int
	seqLen int
}

// newSequenceCursor creates a cursor on seq positioned at idx.
// If idx < 0, count backward from the end of seq.
func newSequenceCursor(parent *sequenceCursor, seq sequence, idx int) *sequenceCursor {
	d.PanicIfTrue(seq == nil)
	seqLen := seq.seqLen()
	if idx < 0 {
		idx += seqLen
		d.PanicIfFalse(idx >= 0)
	}

	return &sequenceCursor{parent, seq, idx, seqLen}
}

func (cur *sequenceCursor) length() int {
	return cur.seqLen
}

func (cur *sequenceCursor) getItem(idx int) (sequenceItem, error) {
	return cur.seq.getItem(idx)
}

// syncAdvance loads the sequence that the cursor index points to.
// It's called whenever the cursor advances/retreats to a different chunk.
func (cur *sequenceCursor) sync(ctx context.Context) error {
	d.PanicIfFalse(cur.parent != nil)

	var err error
	cur.seq, err = cur.parent.getChildSequence(ctx)

	if err != nil {
		return err
	}

	cur.seqLen = cur.seq.seqLen()

	return nil
}

// getChildSequence retrieves the child at the current cursor position.
func (cur *sequenceCursor) getChildSequence(ctx context.Context) (sequence, error) {
	return cur.seq.getChildSequence(ctx, cur.idx)
}

// current returns the value at the current cursor position
func (cur *sequenceCursor) current() (sequenceItem, error) {
	d.PanicIfFalse(cur.valid())
	return cur.getItem(cur.idx)
}

func (cur *sequenceCursor) valid() bool {
	return cur.idx >= 0 && cur.idx < cur.length()
}

func (cur *sequenceCursor) indexInChunk() int {
	return cur.idx
}

func (cur *sequenceCursor) atLastItem() bool {
	return cur.idx == cur.length()-1
}

func (cur *sequenceCursor) advance(ctx context.Context) (bool, error) {
	return cur.advanceMaybeAllowPastEnd(ctx, true)
}

func (cur *sequenceCursor) advanceMaybeAllowPastEnd(ctx context.Context, allowPastEnd bool) (bool, error) {
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

func (cur *sequenceCursor) retreat(ctx context.Context) (bool, error) {
	return cur.retreatMaybeAllowBeforeStart(ctx, true)
}

func (cur *sequenceCursor) retreatMaybeAllowBeforeStart(ctx context.Context, allowBeforeStart bool) (bool, error) {
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

type cursorIterCallback func(item interface{}) (bool, error)

func (cur *sequenceCursor) String() string {
	m, err := newMap(cur.seq.(orderedSequence)).Hash(cur.seq.format())

	if err != nil {
		return fmt.Sprintf("error: %s", err.Error())
	}

	if cur.parent == nil {
		return fmt.Sprintf("%s (%d): %d", m.String(), cur.seq.seqLen(), cur.idx)
	}

	return fmt.Sprintf("%s (%d): %d -- %s", m.String(), cur.seq.seqLen(), cur.idx, cur.parent.String())
}

func (cur *sequenceCursor) compare(other *sequenceCursor) int {
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
func (cur *sequenceCursor) iter(ctx context.Context, cb cursorIterCallback) error {
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

func newIteratorAtIndex(ctx context.Context, seq sequence, idx uint64) (sequenceIterator, error) {
	return newCursorAtIndex(ctx, seq, idx)
}

// newIteratorAtIndex creates a new cursor over seq positioned at idx.
//
// Implemented by searching down the tree to the leaf sequence containing idx. Each
// sequence cursor includes a back pointer to its parent so that it can follow the path
// to the next leaf chunk when the cursor exhausts the entries in the current chunk.
func newCursorAtIndex(ctx context.Context, seq sequence, idx uint64) (*sequenceCursor, error) {
	var cur *sequenceCursor
	for {
		cur = newSequenceCursor(cur, seq, 0)
		delta, err := advanceCursorToOffset(cur, idx)

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
