// Copyright 2021 Dolthub, Inc.
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

package prolly

import (
	"bytes"
	"context"
	"io"

	"github.com/dolthub/dolt/go/store/val"
)

type DiffType byte

const (
	AddedDiff    DiffType = 0
	ModifiedDiff DiffType = 1
	RemovedDiff  DiffType = 2
)

type Diff struct {
	Type     DiffType
	Key      val.Tuple
	From, To val.Tuple
}

type DiffFn func(context.Context, Diff) error

type treeDiffer struct {
	from, to *Cursor
	cmp      CompareFn
}

func treeDifferFromMaps(ctx context.Context, from, to Map) (treeDiffer, error) {
	fc, err := NewCursorAtStart(ctx, from.ns, from.root)
	if err != nil {
		return treeDiffer{}, err
	}

	tc, err := NewCursorAtStart(ctx, to.ns, to.root)
	if err != nil {
		return treeDiffer{}, err
	}

	return treeDiffer{from: fc, to: tc, cmp: from.compareItems}, nil
}

func (td treeDiffer) Next(ctx context.Context) (diff Diff, err error) {
	for td.from.Valid() && td.to.Valid() {

		f := td.from.CurrentKey()
		t := td.to.CurrentKey()
		cmp := td.cmp(f, t)

		switch {
		case cmp < 0:
			return sendRemoved(ctx, td.from)

		case cmp > 0:
			return sendAdded(ctx, td.to)

		case cmp == 0:
			if !equalValues(td.from, td.to) {
				return sendModified(ctx, td.from, td.to)
			}

			// seek ahead to the next diff and loop again
			if err = skipCommon(ctx, td.from, td.to); err != nil {
				return Diff{}, err
			}
		}
	}

	if td.from.Valid() {
		return sendRemoved(ctx, td.from)
	}
	if td.to.Valid() {
		return sendAdded(ctx, td.to)
	}

	return Diff{}, io.EOF
}

func sendRemoved(ctx context.Context, from *Cursor) (diff Diff, err error) {
	diff = Diff{
		Type: RemovedDiff,
		Key:  val.Tuple(from.CurrentKey()),
		From: val.Tuple(from.CurrentValue()),
	}

	if _, err = from.Advance(ctx); err != nil {
		return Diff{}, err
	}
	return
}

func sendAdded(ctx context.Context, to *Cursor) (diff Diff, err error) {
	diff = Diff{
		Type: AddedDiff,
		Key:  val.Tuple(to.CurrentKey()),
		To:   val.Tuple(to.CurrentValue()),
	}

	if _, err = to.Advance(ctx); err != nil {
		return Diff{}, err
	}
	return
}

func sendModified(ctx context.Context, from, to *Cursor) (diff Diff, err error) {
	diff = Diff{
		Type: ModifiedDiff,
		Key:  val.Tuple(from.CurrentKey()),
		From: val.Tuple(from.CurrentValue()),
		To:   val.Tuple(to.CurrentValue()),
	}

	if _, err = from.Advance(ctx); err != nil {
		return Diff{}, err
	}
	if _, err = to.Advance(ctx); err != nil {
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
				// faster at the next highest tree level.
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

		if _, err = from.Advance(ctx); err != nil {
			return err
		}
		if _, err = to.Advance(ctx); err != nil {
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
		from.Invalidate()
	}

	if to.parent.Valid() {
		if err = to.fetchNode(ctx); err != nil {
			return err
		}
		to.skipToNodeStart()
	} else {
		to.Invalidate()
	}

	return
}

// todo(andy): assumes equal byte representations
func equalItems(from, to *Cursor) bool {
	return bytes.Equal(from.CurrentKey(), to.CurrentKey()) &&
		bytes.Equal(from.CurrentValue(), to.CurrentValue())
}

func equalParents(from, to *Cursor) (eq bool) {
	if from.parent != nil && to.parent != nil {
		eq = equalItems(from.parent, to.parent)
	}
	return
}

func equalValues(from, to *Cursor) bool {
	return bytes.Equal(from.CurrentValue(), to.CurrentValue())
}
