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
	"io"

	"github.com/dolthub/dolt/go/store/val"
)

// threeWayDiffer is an iterator that gives an increased level of granularity
// of diffs between three root values. See diffOp for the classes of diffs.
type threeWayDiffer[K ~[]byte, O Ordering[K]] struct {
	lIter, rIter Differ[K, O]
	resolveCb    resolveCb
	lDiff        Diff
	rDiff        Diff
	lDone        bool
	rDone        bool
}

type resolveCb func(Diff, Diff) (Item, bool)

func NewThreeWayDiffer[K, V ~[]byte, O Ordering[K]](
	ctx context.Context,
	ns NodeStore,
	left, right, base StaticMap[K, V, O],
	resolveCb resolveCb,
	order O,
) (*threeWayDiffer[K, O], error) {
	ld, err := DifferFromRoots[K](ctx, ns, ns, base.Root, left.Root, order)
	if err != nil {
		return nil, err
	}

	rd, err := DifferFromRoots[K](ctx, ns, ns, base.Root, right.Root, order)
	if err != nil {
		return nil, err
	}

	return &threeWayDiffer[K, O]{
		lIter:     ld,
		rIter:     rd,
		resolveCb: resolveCb,
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

func (d *threeWayDiffer[K, O]) Next(ctx context.Context) (threeWayDiff, error) {
	var err error
	var res threeWayDiff
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
					if err != nil {
						return threeWayDiff{}, err
					}
				}
			}
			if !d.rDone {
				if d.rDiff.Key == nil {
					d.rDiff, err = d.rIter.Next(ctx)
					if err != nil {
						return threeWayDiff{}, err
					}
				}
			}
			nextState = dsDiffFinalize
		case dsDiffFinalize:
			if d.lDone && d.rDone {
				return threeWayDiff{}, io.EOF
			} else if d.lDone {
				nextState = dsNewRight
			} else if d.rDone {
				nextState = dsNewLeft
			} else {
				nextState = dsCompare
			}
		case dsCompare:
			cmp := d.lIter.order.Compare(K(d.lDiff.Key), K(d.rDiff.Key))
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
			res = d.newLeftEdit(d.lDiff.Key, d.lDiff.To)
			d.lDiff, err = d.lIter.Next(ctx)
			if errors.Is(err, io.EOF) {
				d.lDone = true
			} else if err != nil {
				return threeWayDiff{}, err
			}
			return res, nil
		case dsNewRight:
			res = d.newRightEdit(d.rDiff.Key, d.rDiff.To)
			d.rDiff, err = d.rIter.Next(ctx)
			if errors.Is(err, io.EOF) {
				d.rDone = true
			} else if err != nil {
				return threeWayDiff{}, err
			}
			return res, nil
		case dsMatch:
			if d.lDiff.To == nil && d.rDiff.To == nil {
				res = d.newConvergentEdit(d.lDiff.Key, d.lDiff.To)
			} else if d.lDiff.To == nil || d.rDiff.To == nil {
				res = d.newDivergentDeleteConflict(d.lDiff.Key, d.lDiff.To, d.rDiff.To)
			} else if d.lDiff.Type == d.rDiff.Type && bytes.Equal(d.lDiff.To, d.rDiff.To) {
				res = d.newConvergentEdit(d.lDiff.Key, d.lDiff.To)
			} else {
				resolved, ok := d.resolveCb(d.lDiff, d.rDiff)
				if !ok {
					res = d.newDivergentClashConflict(d.lDiff.Key, d.lDiff.To, d.rDiff.To)
				} else {
					res = d.newDivergentResolved(d.lDiff.Key, d.lDiff.To, d.rDiff.To, resolved)
				}
			}
			nextState = dsMatchFinalize
		case dsMatchFinalize:
			d.lDiff, err = d.lIter.Next(ctx)
			if errors.Is(err, io.EOF) {
				d.lDone = true
			} else if err != nil {
				return threeWayDiff{}, err
			}

			d.rDiff, err = d.rIter.Next(ctx)
			if errors.Is(err, io.EOF) {
				d.rDone = true
			} else if err != nil {
				return threeWayDiff{}, err
			}

			return res, nil
		default:
			panic(fmt.Sprintf("unknown threeWayDiffState: %d", nextState))
		}
	}
}

type diffOp uint8

const (
	dtLeftEdit diffOp = iota
	dtRightEdit
	dtLeftDelete
	dtRightDelete
	dtConvergentEdit
	dtDivergentResolved
	dtDivergentDeleteConflict
	dtDivergentClashConflict
)

func (o diffOp) String() string {
	switch o {
	case dtLeftEdit:
		return "leftEdit"
	case dtRightEdit:
		return "rightEdit"
	case dtConvergentEdit:
		return "convergentEdit"
	case dtDivergentResolved:
		return "divergentResolved"
	case dtDivergentDeleteConflict:
		return "divergentDeleteConflict"
	case dtDivergentClashConflict:
		return "divergentClashConflict"
	default:
		panic("unknown diff type")
	}
}

// threeWayDiff is a generic object for encoding a three way diff.
type threeWayDiff struct {
	// op indicates the type of diff
	op diffOp
	// a partial set of tuple values are set
	// depending on the diffOp
	k, l, r, m val.Tuple
}

func (d *threeWayDiffer[K, O]) newLeftEdit(key, left Item) threeWayDiff {
	op := dtLeftEdit
	if left == nil {
		op = dtLeftDelete
	}
	return threeWayDiff{
		op: op,
		k:  val.Tuple(key),
		l:  val.Tuple(left),
	}
}

func (d *threeWayDiffer[K, O]) newRightEdit(key, right Item) threeWayDiff {
	op := dtRightEdit
	if right == nil {
		op = dtRightDelete
	}
	return threeWayDiff{
		op: op,
		k:  val.Tuple(key),
		r:  val.Tuple(right),
	}
}

func (d *threeWayDiffer[K, O]) newConvergentEdit(key, left Item) threeWayDiff {
	return threeWayDiff{
		op: dtConvergentEdit,
		k:  val.Tuple(key),
		l:  val.Tuple(left),
	}
}

func (d *threeWayDiffer[K, O]) newDivergentResolved(key, left, right, merged Item) threeWayDiff {
	return threeWayDiff{
		op: dtDivergentResolved,
		k:  val.Tuple(key),
		l:  val.Tuple(left),
		r:  val.Tuple(right),
		m:  val.Tuple(merged),
	}
}

func (d *threeWayDiffer[K, O]) newDivergentDeleteConflict(key, left, right Item) threeWayDiff {
	return threeWayDiff{
		op: dtDivergentDeleteConflict,
		k:  val.Tuple(key),
		l:  val.Tuple(left),
		r:  val.Tuple(right),
	}
}

func (d *threeWayDiffer[K, O]) newDivergentClashConflict(key, left, right Item) threeWayDiff {
	return threeWayDiff{
		op: dtDivergentClashConflict,
		k:  val.Tuple(key),
		l:  val.Tuple(left),
		r:  val.Tuple(right),
	}
}
