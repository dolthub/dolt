// Copyright 2022 Dolthub, Inc.
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

package tree

import (
	"bytes"
	"context"
	"io"
)

type DiffType byte

const (
	AddedDiff    DiffType = 0
	ModifiedDiff DiffType = 1
	RemovedDiff  DiffType = 2
)

type Diff struct {
	Key      Item
	From, To Item
	Type     DiffType
}

type DiffFn func(context.Context, Diff) error

type Differ[K ~[]byte, O Ordering[K]] struct {
	from, to         *Cursor
	fromStop, toStop *Cursor
	order            O
}

func DifferFromRoots[K ~[]byte, O Ordering[K]](
	ctx context.Context,
	fromNs NodeStore, toNs NodeStore,
	from, to Node,
	order O,
) (Differ[K, O], error) {
	var fc, tc *Cursor
	var err error

	if !from.empty() {
		fc, err = newCursorAtStart(ctx, fromNs, from)
		if err != nil {
			return Differ[K, O]{}, err
		}
	} else {
		fc = &Cursor{}
	}

	if !to.empty() {
		tc, err = newCursorAtStart(ctx, toNs, to)
		if err != nil {
			return Differ[K, O]{}, err
		}
	} else {
		tc = &Cursor{}
	}

	fs, err := newCursorPastEnd(ctx, fromNs, from)
	if err != nil {
		return Differ[K, O]{}, err
	}

	ts, err := newCursorPastEnd(ctx, toNs, to)
	if err != nil {
		return Differ[K, O]{}, err
	}

	return Differ[K, O]{
		from:     fc,
		to:       tc,
		fromStop: fs,
		toStop:   ts,
		order:    order,
	}, nil
}

func DifferFromCursors[K ~[]byte, O Ordering[K]](
	ctx context.Context,
	fromRoot, toRoot Node,
	findStart, findStop SearchFn,
	fromStore, toStore NodeStore,
	order O,
) (Differ[K, O], error) {
	fromStart, err := newCursorFromSearchFn(ctx, fromStore, fromRoot, findStart)
	if err != nil {
		return Differ[K, O]{}, err
	}
	toStart, err := newCursorFromSearchFn(ctx, toStore, toRoot, findStart)
	if err != nil {
		return Differ[K, O]{}, err
	}
	fromStop, err := newCursorFromSearchFn(ctx, fromStore, fromRoot, findStop)
	if err != nil {
		return Differ[K, O]{}, err
	}
	toStop, err := newCursorFromSearchFn(ctx, toStore, toRoot, findStop)
	if err != nil {
		return Differ[K, O]{}, err
	}
	return Differ[K, O]{
		from:     fromStart,
		to:       toStart,
		fromStop: fromStop,
		toStop:   toStop,
		order:    order,
	}, nil
}

func (td Differ[K, O]) Next(ctx context.Context) (diff Diff, err error) {
	for td.from.Valid() && td.from.compare(td.fromStop) < 0 && td.to.Valid() && td.to.compare(td.toStop) < 0 {

		f := td.from.CurrentKey()
		t := td.to.CurrentKey()
		cmp := td.order.Compare(K(f), K(t))

		switch {
		case cmp < 0:
			return sendRemoved(ctx, td.from)

		case cmp > 0:
			return sendAdded(ctx, td.to)

		case cmp == 0:
			if !equalcursorValues(td.from, td.to) {
				return sendModified(ctx, td.from, td.to)
			}

			// seek ahead to the next diff and loop again
			if err = skipCommon(ctx, td.from, td.to); err != nil {
				return Diff{}, err
			}
		}
	}

	if td.from.Valid() && td.from.compare(td.fromStop) < 0 {
		return sendRemoved(ctx, td.from)
	}
	if td.to.Valid() && td.to.compare(td.toStop) < 0 {
		return sendAdded(ctx, td.to)
	}

	return Diff{}, io.EOF
}

func sendRemoved(ctx context.Context, from *Cursor) (diff Diff, err error) {
	diff = Diff{
		Type: RemovedDiff,
		Key:  from.CurrentKey(),
		From: from.currentValue(),
	}

	if err = from.advance(ctx); err != nil {
		return Diff{}, err
	}
	return
}

func sendAdded(ctx context.Context, to *Cursor) (diff Diff, err error) {
	diff = Diff{
		Type: AddedDiff,
		Key:  to.CurrentKey(),
		To:   to.currentValue(),
	}

	if err = to.advance(ctx); err != nil {
		return Diff{}, err
	}
	return
}

func sendModified(ctx context.Context, from, to *Cursor) (diff Diff, err error) {
	diff = Diff{
		Type: ModifiedDiff,
		Key:  from.CurrentKey(),
		From: from.currentValue(),
		To:   to.currentValue(),
	}

	if err = from.advance(ctx); err != nil {
		return Diff{}, err
	}
	if err = to.advance(ctx); err != nil {
		return Diff{}, err
	}
	return
}

func skipCommon(ctx context.Context, from, to *Cursor) (err error) {
	// track when |from.parent| and |to.parent| change
	// to avoid unnecessary comparisons.
	parentsAreNew := true

	for from.Valid() && to.Valid() {
		if !equalItems(from, to) {
			// found the next difference
			return nil
		}

		if parentsAreNew {
			if equalParents(from, to) {
				// if our parents are equal, we can search for differences
				// faster at the next highest tree Level.
				if err = skipCommonParents(ctx, from, to); err != nil {
					return err
				}
				continue
			}
			parentsAreNew = false
		}

		// if one of the cursors is at the end of its node, it will
		// need to Advance its parent and fetch a new node. In this
		// case we need to Compare parents again.
		parentsAreNew = from.atNodeEnd() || to.atNodeEnd()

		if err = from.advance(ctx); err != nil {
			return err
		}
		if err = to.advance(ctx); err != nil {
			return err
		}
	}

	return err
}

func skipCommonParents(ctx context.Context, from, to *Cursor) (err error) {
	err = skipCommon(ctx, from.parent, to.parent)
	if err != nil {
		return err
	}

	if from.parent.Valid() {
		if err = from.fetchNode(ctx); err != nil {
			return err
		}
		from.skipToNodeStart()
	} else {
		from.invalidateAtEnd()
	}

	if to.parent.Valid() {
		if err = to.fetchNode(ctx); err != nil {
			return err
		}
		to.skipToNodeStart()
	} else {
		to.invalidateAtEnd()
	}

	return
}

// todo(andy): assumes equal byte representations
func equalItems(from, to *Cursor) bool {
	return bytes.Equal(from.CurrentKey(), to.CurrentKey()) &&
		bytes.Equal(from.currentValue(), to.currentValue())
}

func equalParents(from, to *Cursor) (eq bool) {
	if from.parent != nil && to.parent != nil {
		eq = equalItems(from.parent, to.parent)
	}
	return
}

func equalcursorValues(from, to *Cursor) bool {
	return bytes.Equal(from.currentValue(), to.currentValue())
}
