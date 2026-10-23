// Copyright 2026 Dolthub, Inc.
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
	"context"
	"errors"
	"io"
	"sort"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/store/val"
)

// FlaggedKeyCheck decides a diff of a flagged key, returning it or a diff to take its place.
type FlaggedKeyCheck func(ctx *sql.Context, diff ThreeWayDiff) (ThreeWayDiff, error)

// FlaggedKeyDiffer is a ThreeWayDiffer for a merge whose base has flagged keys, whose base values the merge cannot
// trust, such as the rows of a virtual merge base that conflicted between its merge bases. It passes each diff of a
// flagged key to a check, and so also each flagged key that neither side changed, as a DiffOpUnchanged diff whose
// Base, Left and Right hold the base value, nil when the key is absent. It drops a DiffOpUnchanged diff unless the
// check replaces it. Diffs come in key order.
type FlaggedKeyDiffer[K ~[]byte, O Ordering[K]] struct {
	inner     *ThreeWayDiffer[K, O]
	order     O
	baseValue func(ctx context.Context, key K) (val.Tuple, error)
	flagged   []K
	check     FlaggedKeyCheck
	pending   *ThreeWayDiff
	innerDone bool
}

// NewFlaggedKeyDiffer returns a FlaggedKeyDiffer over |inner|, whose base is |base|. |flagged| need not be sorted.
func NewFlaggedKeyDiffer[K, V ~[]byte, O Ordering[K]](ctx context.Context, inner *ThreeWayDiffer[K, O], base StaticMap[K, V, O], flagged []K, check FlaggedKeyCheck) (*FlaggedKeyDiffer[K, O], error) {
	sorted := append([]K(nil), flagged...)
	var sortErr error
	sort.Slice(sorted, func(i, j int) bool {
		cmp, err := base.Order.Compare(ctx, sorted[i], sorted[j])
		if err != nil && sortErr == nil {
			sortErr = err
		}
		return cmp < 0
	})
	if sortErr != nil {
		return nil, sortErr
	}
	baseValue := func(ctx context.Context, key K) (val.Tuple, error) {
		var value val.Tuple
		err := base.Get(ctx, key, func(_ K, v V) error {
			value = val.Tuple(v)
			return nil
		})
		return value, err
	}
	return &FlaggedKeyDiffer[K, O]{inner: inner, order: base.Order, baseValue: baseValue, flagged: sorted, check: check}, nil
}

func (d *FlaggedKeyDiffer[K, O]) Next(ctx *sql.Context) (ThreeWayDiff, error) {
	for {
		if d.pending == nil && !d.innerDone {
			diff, err := d.inner.Next(ctx)
			if errors.Is(err, io.EOF) {
				d.innerDone = true
			} else if err != nil {
				return ThreeWayDiff{}, err
			} else {
				d.pending = &diff
			}
		}
		if len(d.flagged) == 0 {
			if d.pending == nil {
				return ThreeWayDiff{}, io.EOF
			}
			return d.takePending(), nil
		}

		var diff ThreeWayDiff
		cmp := 1
		if d.pending != nil {
			var err error
			if cmp, err = d.order.Compare(ctx, K(d.pending.Key), d.flagged[0]); err != nil {
				return ThreeWayDiff{}, err
			}
		}
		switch {
		case cmp < 0:
			return d.takePending(), nil
		case cmp == 0:
			diff = d.takePending()
		default:
			base, err := d.baseValue(ctx, d.flagged[0])
			if err != nil {
				return ThreeWayDiff{}, err
			}
			diff = ThreeWayDiff{Op: DiffOpUnchanged, Key: val.Tuple(d.flagged[0]), Base: base, Left: base, Right: base}
		}
		d.flagged = d.flagged[1:]

		diff, err := d.check(ctx, diff)
		if err != nil {
			return ThreeWayDiff{}, err
		}
		if diff.Op != DiffOpUnchanged {
			return diff, nil
		}
	}
}

func (d *FlaggedKeyDiffer[K, O]) takePending() ThreeWayDiff {
	diff := *d.pending
	d.pending = nil
	return diff
}

func (d *FlaggedKeyDiffer[K, O]) Close() error {
	return d.inner.Close()
}
