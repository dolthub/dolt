// Copyright 2023 Dolthub, Inc.
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
	"errors"
	"fmt"
	// "github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/val"
)

// ThreeWayDiffer is an iterator that gives an increased level of granularity
// of diffs between three root values. See diffOp for the classes of diffs.
type ThreeWayDiffer[K ~[]byte, O Ordering[K]] struct {
	lIter, rIter              Differ[K, O]
	resolveCb                 resolveCb
	lDiff                     Diff
	rDiff                     Diff
	lDone                     bool
	rDone                     bool
	keyless                   bool
	leftAndRightSchemasDiffer bool
}

//var _ DiffIter = (*threeWayDiffer[Item, val.TupleDesc])(nil)

type resolveCb func(*sql.Context, val.Tuple, val.Tuple, val.Tuple) (val.Tuple, bool, error)

// ThreeWayDiffInfo stores contextual data that can influence the diff.
// If |LeftSchemaChange| is true, then the left side's bytes have a different interpretation from the base,
// so every row in both Left and Base should be considered a modification, even if they have the same bytes.
// If |RightSchemaChange| is true, then the right side's bytes have a different interpretation from the base,
// so every row in both Right and Base should be considered a modification, even if they have the same bytes.
// Note that these values aren't set for schema changes that have no effect on the meaning of the bytes,
// such as collation.
// If |LeftAndRightSchemasDiffer| is true, then the left and right sides of the diff have a different interpretation
// of their bytes, so there cannot be any convergent edits, even if two rows in Left and Right have the same bytes.
type ThreeWayDiffInfo struct {
	LeftSchemaChange          bool
	RightSchemaChange         bool
	LeftAndRightSchemasDiffer bool
}

func NewThreeWayDiffer[K, V ~[]byte, O Ordering[K]](
	ctx context.Context,
	ns NodeStore,
	left StaticMap[K, V, O],
	right StaticMap[K, V, O],
	base StaticMap[K, V, O],
	resolveCb resolveCb,
	keyless bool,
	diffInfo ThreeWayDiffInfo,
	order O,
) (*ThreeWayDiffer[K, O], error) {
	// probably compute each of these separately
	ld, err := DifferFromRoots[K](ctx, ns, ns, base.Root, left.Root, order, diffInfo.LeftSchemaChange)
	if err != nil {
		return nil, err
	}

	rd, err := DifferFromRoots[K](ctx, ns, ns, base.Root, right.Root, order, diffInfo.RightSchemaChange)
	if err != nil {
		return nil, err
	}

	return &ThreeWayDiffer[K, O]{
		lIter:                     ld,
		rIter:                     rd,
		resolveCb:                 resolveCb,
		keyless:                   keyless,
		leftAndRightSchemasDiffer: diffInfo.LeftAndRightSchemasDiffer,
	}, nil
}

type threeWayDiffState uint8

const (
	dsUnknown threeWayDiffState = iota
	dsInit
	dsDiffFinalize
	dsCompare
	dsNewLeft
	dsNewRight
	dsMatch
	dsMatchFinalize
)

// Three way differ combines two two-way differs, causing each of them to recurse when necessary.
// For each example consider the logic but only implement the parts we need.
// Example 1: Differs each have 1 non-overlapping blocks
//
//	Get block from each, see that they don't overlap, return each one.
//
// Example 2: Differs each have 1 (overlapping) block
func (d *ThreeWayDiffer[K, O]) Next(ctx *sql.Context) (ThreeWayDiff, error) {
	var err error
	var res ThreeWayDiff
	nextState := dsInit
	for {
		// The regular flow will be:
		// - dsInit: get the first diff in each iterator if this is the first Next
		// - dsDiffFinalize: short-circuit comparing if one iterator is exhausted
		// - dsCompare: compare keys for the leading diffs, to determine whether
		//   the diffs are independent, or require further disambiguation.
		// - dsNewLeft: an edit was made to the left root value for a key not edited
		//   on the right.
		// - dsNewRight: ditto above, edit to key only on right.
		// - dsMatch: edits made to the same key in left and right roots, either
		//   resolve non-overlapping field changes or indicate schema/value conflict.
		// - dsMatchFinalize: increment both iters after performing match disambiguation.
		switch nextState {
		case dsInit:
			if !d.lDone {
				if d.lDiff.Key == nil {
					d.lDiff, err = d.lIter.Next(ctx)
					if errors.Is(err, io.EOF) {
						d.lDone = true
					} else if err != nil {
						return ThreeWayDiff{}, err
					}
				}
			}
			if !d.rDone {
				if d.rDiff.Key == nil {
					d.rDiff, err = d.rIter.Next(ctx)
					if errors.Is(err, io.EOF) {
						d.rDone = true
					} else if err != nil {
						return ThreeWayDiff{}, err
					}
				}
			}
			nextState = dsDiffFinalize
		case dsDiffFinalize:
			if d.lDone && d.rDone {
				return ThreeWayDiff{}, io.EOF
			} else if d.lDone {
				nextState = dsNewRight
			} else if d.rDone {
				nextState = dsNewLeft
			} else {
				nextState = dsCompare
			}
		case dsCompare:
			// We want to compare which side has the next edited row.
			// But this is made harder by the different semantics between row diffs and range diffs
			// Row diffs have the next row modified in Key. Range diffs use Key for the largest modified value,
			//   and PreviousKey for the key before the next key to be edited.
			// But if both iterators are on the same level this gets easier.
			// TODO: Add a test where the two trees have different heights.
			var cmp int
			if d.lDiff.toCur.isLeaf() {
				cmp = d.lIter.order.Compare(ctx, K(d.lDiff.Key), K(d.rDiff.Key))
			} else {
				if d.lDiff.PreviousKey == nil && d.rDiff.PreviousKey == nil {
					cmp = 0
				} else if d.lDiff.PreviousKey == nil {
					cmp = -1
				} else if d.rDiff.PreviousKey == nil {
					cmp = 1
				} else {
					cmp = d.lIter.order.Compare(ctx, K(d.lDiff.PreviousKey), K(d.rDiff.PreviousKey))
				}
			}

			switch {
			case cmp < 0:
				nextState = dsNewLeft
			case cmp == 0:
				nextState = dsMatch
			case cmp > 0:
				nextState = dsNewRight
			default:
			}
		case dsNewLeft:
			// The next edited row appears on the left.
			// Determine whether it overlaps with the next edited row on the right.
			cmp := d.lIter.order.Compare(ctx, K(d.lDiff.Key), K(d.rDiff.PreviousKey))
			if cmp <= 0 {
				// left node ends before right block begins. Emit the entire block.
				// If both, make this its own state?
				res = d.newLeftEdit(d.lDiff)
				d.lDiff, err = d.lIter.Next(ctx)
				if errors.Is(err, io.EOF) {
					d.lDone = true
				} else if err != nil {
					return ThreeWayDiff{}, err
				}
				return res, nil
			} else {
				// nodes overlap. Split both differs (or just the left one?)
				d.lDiff, err = d.lIter.split(ctx)
				if err != nil {
					return ThreeWayDiff{}, err
				}
				// It's no longer guaranteed that the next modified row is on the left.
				nextState = dsDiffFinalize
				continue
			}
		case dsNewRight:
			// The next edited row appears on the right.
			// Determine whether it overlaps with the next edited row on the left.
			cmp := d.lIter.order.Compare(ctx, K(d.rDiff.Key), K(d.lDiff.PreviousKey))
			if cmp <= 0 {
				res = d.newRightEdit(d.rDiff)
				d.rDiff, err = d.rIter.Next(ctx)
				if errors.Is(err, io.EOF) {
					d.rDone = true
				} else if err != nil {
					return ThreeWayDiff{}, err
				}
				return res, nil
			} else {
				// nodes overlap. Split both differs (or just the right one?)
				// If both, make this its own state?
				d.rDiff, err = d.rIter.split(ctx)
				if err != nil {
					return ThreeWayDiff{}, err
				}
				// It's no longer guaranteed that the next modified row is on the right.
				nextState = dsDiffFinalize
				continue
			}
		case dsMatch:
			if d.lDiff.To == nil && d.rDiff.To == nil {
				res = d.newConvergentEdit(d.lDiff.Key, d.lDiff.To(), d.lDiff.Type)
			} else if d.lDiff.To == nil || d.rDiff.To == nil {
				// Divergent delete. Attempt to resolve.
				_, ok, err := d.resolveCb(ctx, val.Tuple(d.lDiff.To()), val.Tuple(d.rDiff.To()), val.Tuple(d.lDiff.From))
				if err != nil {
					return ThreeWayDiff{}, err
				}
				if !ok {
					res = d.newDivergentDeleteConflict(d.lDiff.Key, d.lDiff.From, d.lDiff.To(), d.rDiff.To())
				} else {
					res = d.newDivergentDeleteResolved(d.lDiff.Key, d.lDiff.From, d.lDiff.To(), d.rDiff.To())
				}
			} else if d.lDiff.Type == d.rDiff.Type && bytes.Equal(d.lDiff.To(), d.rDiff.To()) {
				res = d.newConvergentEdit(d.lDiff.Key, d.lDiff.To(), d.lDiff.Type)
			} else {
				resolved, ok, err := d.resolveCb(ctx, val.Tuple(d.lDiff.To()), val.Tuple(d.rDiff.To()), val.Tuple(d.lDiff.From))
				if err != nil {
					return ThreeWayDiff{}, err
				}
				if !ok {
					res = d.newDivergentClashConflict(d.lDiff.Key, d.lDiff.From, d.lDiff.To(), d.rDiff.To())
				} else {
					res = d.newDivergentResolved(d.lDiff.Key, d.lDiff.To(), d.rDiff.To(), Item(resolved))
				}
			}
			nextState = dsMatchFinalize
		case dsMatchFinalize:
			d.lDiff, err = d.lIter.Next(ctx)
			if errors.Is(err, io.EOF) {
				d.lDone = true
			} else if err != nil {
				return ThreeWayDiff{}, err
			}

			d.rDiff, err = d.rIter.Next(ctx)
			if errors.Is(err, io.EOF) {
				d.rDone = true
			} else if err != nil {
				return ThreeWayDiff{}, err
			}

			return res, nil
		default:
			panic(fmt.Sprintf("unknown threeWayDiffState: %d", nextState))
		}
	}
}

func (d *ThreeWayDiffer[K, O]) Close() error {
	return nil
}

//go:generate stringer -type=diffOp -linecomment

type DiffOp uint16

const (
	DiffOpLeftAdd                 DiffOp = iota // leftAdd
	DiffOpRightAdd                              // rightAdd
	DiffOpLeftDelete                            //leftDelete
	DiffOpRightDelete                           //rightDelete
	DiffOpLeftModify                            //leftModify
	DiffOpRightModify                           //rightModify
	DiffOpConvergentAdd                         //convergentAdd
	DiffOpConvergentDelete                      //convergentDelete
	DiffOpConvergentModify                      //convergentModify
	DiffOpDivergentModifyResolved               //divergentModifyResolved
	DiffOpDivergentDeleteConflict               //divergentDeleteConflict
	DiffOpDivergentModifyConflict               //divergentModifyConflict
	DiffOpDivergentDeleteResolved               //divergentDeleteConflict
)

// ThreeWayDiff is a generic object for encoding a three way diff.
type ThreeWayDiff struct {
	// Op indicates the type of diff
	Op DiffOp
	// a partial set of tuple values are set
	// depending on the diffOp
	Key, Base, Left, Right, Merged val.Tuple
	// The node is set for a range diff
	Node Node
}

func (d *ThreeWayDiffer[K, O]) newLeftEdit(diff Diff) ThreeWayDiff {
	var op DiffOp
	switch diff.Type {
	case AddedDiff:
		op = DiffOpLeftAdd
	case ModifiedDiff:
		op = DiffOpLeftModify
	case RemovedDiff:
		op = DiffOpLeftDelete
	default:
		panic("unknown diff type")
	}
	return ThreeWayDiff{
		Op:   op,
		Key:  val.Tuple(diff.Key),
		Base: val.Tuple(diff.From),
		Left: val.Tuple(diff.To()),
		Node: diff.toCur.nd,
	}
}

func (d *ThreeWayDiffer[K, O]) newRightEdit(diff Diff) ThreeWayDiff {
	var op DiffOp
	switch diff.Type {
	case AddedDiff:
		op = DiffOpRightAdd
	case ModifiedDiff:
		op = DiffOpRightModify
	case RemovedDiff:
		op = DiffOpRightDelete
	default:
		panic("unknown diff type")
	}
	return ThreeWayDiff{
		Op:    op,
		Key:   val.Tuple(diff.Key),
		Base:  val.Tuple(diff.From),
		Right: val.Tuple(diff.To()),
		Node:  diff.toCur.nd,
	}
}

func (d *ThreeWayDiffer[K, O]) newConvergentEdit(key, left Item, typ DiffType) ThreeWayDiff {
	var op DiffOp
	switch typ {
	case AddedDiff:
		op = DiffOpConvergentAdd
	case ModifiedDiff:
		op = DiffOpConvergentModify
	case RemovedDiff:
		op = DiffOpConvergentDelete
	default:
		panic("unknown diff type")
	}
	return ThreeWayDiff{
		Op:   op,
		Key:  val.Tuple(key),
		Left: val.Tuple(left),
	}
}

func (d *ThreeWayDiffer[K, O]) newDivergentResolved(key, left, right, merged Item) ThreeWayDiff {
	return ThreeWayDiff{
		Op:     DiffOpDivergentModifyResolved,
		Key:    val.Tuple(key),
		Left:   val.Tuple(left),
		Right:  val.Tuple(right),
		Merged: val.Tuple(merged),
	}
}

func (d *ThreeWayDiffer[K, O]) newDivergentDeleteConflict(key, base, left, right Item) ThreeWayDiff {
	return ThreeWayDiff{
		Op:    DiffOpDivergentDeleteConflict,
		Key:   val.Tuple(key),
		Base:  val.Tuple(base),
		Left:  val.Tuple(left),
		Right: val.Tuple(right),
	}
}

func (d *ThreeWayDiffer[K, O]) newDivergentDeleteResolved(key, base, left, right Item) ThreeWayDiff {
	return ThreeWayDiff{
		Op:    DiffOpDivergentDeleteResolved,
		Key:   val.Tuple(key),
		Base:  val.Tuple(base),
		Left:  val.Tuple(left),
		Right: val.Tuple(right),
	}
}

func (d *ThreeWayDiffer[K, O]) newDivergentClashConflict(key, base, left, right Item) ThreeWayDiff {
	return ThreeWayDiff{
		Op:    DiffOpDivergentModifyConflict,
		Key:   val.Tuple(key),
		Base:  val.Tuple(base),
		Left:  val.Tuple(left),
		Right: val.Tuple(right),
	}
}
