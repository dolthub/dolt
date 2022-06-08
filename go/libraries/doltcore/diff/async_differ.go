// Copyright 2019 Dolthub, Inc.
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

package diff

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

type RowDiffer interface {
	// Start starts the RowDiffer.
	Start(ctx context.Context, from, to types.Map)

	// StartWithRange starts the RowDiffer with the specified range
	StartWithRange(ctx context.Context, from, to types.Map, start types.Value, inRange types.ValueInRange)

	// GetDiffs returns the requested number of diff.Differences, or times out.
	GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, bool, error)

	// GetDiffsWithFilter returns the requested number of filtered diff.Differences, or times out.
	GetDiffsWithFilter(numDiffs int, timeout time.Duration, filterByChangeType types.DiffChangeType) ([]*diff.Difference, bool, error)

	// Close closes the RowDiffer.
	Close() error
}

func NewRowDiffer(ctx context.Context, fromSch, toSch schema.Schema, buf int) RowDiffer {
	ad := NewAsyncDiffer(buf)

	// Returns an EmptyRowDiffer if the two schemas are not diffable.
	if !schema.ArePrimaryKeySetsDiffable(fromSch, toSch) {
		return &EmptyRowDiffer{}
	}

	if schema.IsKeyless(fromSch) || schema.IsKeyless(toSch) {
		return &keylessDiffer{AsyncDiffer: ad}
	}

	return ad
}

// todo: make package private
type AsyncDiffer struct {
	diffChan   chan diff.Difference
	bufferSize int

	eg       *errgroup.Group
	egCtx    context.Context
	egCancel func()

	diffStats map[types.DiffChangeType]uint64
}

var _ RowDiffer = &AsyncDiffer{}

// todo: make package private once dolthub is migrated
func NewAsyncDiffer(bufferedDiffs int) *AsyncDiffer {
	return &AsyncDiffer{
		diffChan:   make(chan diff.Difference, bufferedDiffs),
		bufferSize: bufferedDiffs,
		egCtx:      context.Background(),
		egCancel:   func() {},
		diffStats:  make(map[types.DiffChangeType]uint64),
	}
}

func tableDontDescendLists(v1, v2 types.Value) bool {
	kind := v1.Kind()
	return !types.IsPrimitiveKind(kind) && kind != types.TupleKind && kind == v2.Kind() && kind != types.RefKind
}

func (ad *AsyncDiffer) Start(ctx context.Context, from, to types.Map) {
	ad.start(ctx, func(ctx context.Context) error {
		return diff.Diff(ctx, from, to, ad.diffChan, true, tableDontDescendLists)
	})
}

func (ad *AsyncDiffer) StartWithRange(ctx context.Context, from, to types.Map, start types.Value, inRange types.ValueInRange) {
	ad.start(ctx, func(ctx context.Context) error {
		return diff.DiffMapRange(ctx, from, to, start, inRange, ad.diffChan, true, tableDontDescendLists)
	})
}

func (ad *AsyncDiffer) start(ctx context.Context, diffFunc func(ctx context.Context) error) {
	ad.eg, ad.egCtx = errgroup.WithContext(ctx)
	ad.egCancel = async.GoWithCancel(ad.egCtx, ad.eg, func(ctx context.Context) (err error) {
		defer close(ad.diffChan)
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in diff.Diff: %v", r)
			}
		}()
		return diffFunc(ctx)
	})
}

func (ad *AsyncDiffer) Close() error {
	ad.egCancel()
	return ad.eg.Wait()
}

func (ad *AsyncDiffer) getDiffs(numDiffs int, timeoutChan <-chan time.Time, pred diffPredicate) ([]*diff.Difference, bool, error) {
	diffs := make([]*diff.Difference, 0, numDiffs)
	for {
		select {
		case d, more := <-ad.diffChan:
			if more {
				if pred(&d) {
					ad.diffStats[d.ChangeType]++
					diffs = append(diffs, &d)
				}
				if numDiffs != 0 && numDiffs == len(diffs) {
					return diffs, true, nil
				}
			} else {
				return diffs, false, ad.eg.Wait()
			}
		case <-timeoutChan:
			return diffs, true, nil
		case <-ad.egCtx.Done():
			return nil, false, ad.eg.Wait()
		}
	}
}

var forever <-chan time.Time = make(chan time.Time)

type diffPredicate func(*diff.Difference) bool

var alwaysTruePredicate diffPredicate = func(*diff.Difference) bool {
	return true
}

func hasChangeTypePredicate(changeType types.DiffChangeType) diffPredicate {
	return func(d *diff.Difference) bool {
		return d.ChangeType == changeType
	}
}

func (ad *AsyncDiffer) GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, bool, error) {
	if timeout < 0 {
		return ad.GetDiffsWithoutTimeout(numDiffs)
	}
	return ad.getDiffs(numDiffs, time.After(timeout), alwaysTruePredicate)
}

func (ad *AsyncDiffer) GetDiffsWithFilter(numDiffs int, timeout time.Duration, filterByChangeType types.DiffChangeType) ([]*diff.Difference, bool, error) {
	if timeout < 0 {
		return ad.GetDiffsWithoutTimeoutWithFilter(numDiffs, filterByChangeType)
	}
	return ad.getDiffs(numDiffs, time.After(timeout), hasChangeTypePredicate(filterByChangeType))
}

func (ad *AsyncDiffer) GetDiffsWithoutTimeoutWithFilter(numDiffs int, filterByChangeType types.DiffChangeType) ([]*diff.Difference, bool, error) {
	return ad.getDiffs(numDiffs, forever, hasChangeTypePredicate(filterByChangeType))
}

func (ad *AsyncDiffer) GetDiffsWithoutTimeout(numDiffs int) ([]*diff.Difference, bool, error) {
	return ad.getDiffs(numDiffs, forever, alwaysTruePredicate)
}

type keylessDiffer struct {
	*AsyncDiffer

	df         diff.Difference
	copiesLeft uint64
}

var _ RowDiffer = &keylessDiffer{}

func (kd *keylessDiffer) getDiffs(numDiffs int, timeoutChan <-chan time.Time, pred diffPredicate) ([]*diff.Difference, bool, error) {
	diffs := make([]*diff.Difference, numDiffs)
	idx := 0

	for {
		// first populate |diffs| with copies of |kd.df|

		cpy := kd.df // save a copy of kd.df to reference
		for (idx < numDiffs) && (kd.copiesLeft > 0) {
			diffs[idx] = &cpy
			idx++
			kd.copiesLeft--
		}
		if idx == numDiffs {
			return diffs, true, nil
		}

		// then find the next Difference the satisfies |pred|
		match := false
		for !match {
			select {
			case <-timeoutChan:
				return diffs, true, nil

			case <-kd.egCtx.Done():
				return nil, false, kd.eg.Wait()

			case d, more := <-kd.diffChan:
				if !more {
					return diffs[:idx], more, nil
				}

				var err error
				kd.df, kd.copiesLeft, err = convertDiff(d)
				if err != nil {
					return nil, false, err
				}

				match = pred(&kd.df)
			}
		}
	}
}

func (kd *keylessDiffer) GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, bool, error) {
	if timeout < 0 {
		return kd.getDiffs(numDiffs, forever, alwaysTruePredicate)
	}
	return kd.getDiffs(numDiffs, time.After(timeout), alwaysTruePredicate)
}

func (kd *keylessDiffer) GetDiffsWithFilter(numDiffs int, timeout time.Duration, filterByChangeType types.DiffChangeType) ([]*diff.Difference, bool, error) {
	if timeout < 0 {
		return kd.getDiffs(numDiffs, forever, hasChangeTypePredicate(filterByChangeType))
	}
	return kd.getDiffs(numDiffs, time.After(timeout), hasChangeTypePredicate(filterByChangeType))
}

// convertDiff reports the cardinality of a change,
// and converts updates to adds or deletes
func convertDiff(df diff.Difference) (diff.Difference, uint64, error) {
	var oldCard uint64
	if df.OldValue != nil {
		v, err := df.OldValue.(types.Tuple).Get(row.KeylessCardinalityValIdx)
		if err != nil {
			return df, 0, err
		}
		oldCard = uint64(v.(types.Uint))
	}

	var newCard uint64
	if df.NewValue != nil {
		v, err := df.NewValue.(types.Tuple).Get(row.KeylessCardinalityValIdx)
		if err != nil {
			return df, 0, err
		}
		newCard = uint64(v.(types.Uint))
	}

	switch df.ChangeType {
	case types.DiffChangeRemoved:
		return df, oldCard, nil

	case types.DiffChangeAdded:
		return df, newCard, nil

	case types.DiffChangeModified:
		delta := int64(newCard) - int64(oldCard)
		if delta > 0 {
			df.ChangeType = types.DiffChangeAdded
			df.OldValue = nil
			return df, uint64(delta), nil
		} else if delta < 0 {
			df.ChangeType = types.DiffChangeRemoved
			df.NewValue = nil
			return df, uint64(-delta), nil
		} else {
			panic(fmt.Sprintf("diff with delta = 0 for key: %s", df.KeyValue.HumanReadableString()))
		}
	default:
		return df, 0, fmt.Errorf("unexpected DiffChange type %d", df.ChangeType)
	}
}

type EmptyRowDiffer struct {
}

var _ RowDiffer = &EmptyRowDiffer{}

func (e EmptyRowDiffer) Start(ctx context.Context, from, to types.Map) {
}

func (e EmptyRowDiffer) StartWithRange(ctx context.Context, from, to types.Map, start types.Value, inRange types.ValueInRange) {

}

func (e EmptyRowDiffer) GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, bool, error) {
	return nil, false, nil
}

func (e EmptyRowDiffer) GetDiffsWithFilter(numDiffs int, timeout time.Duration, filterByChangeType types.DiffChangeType) ([]*diff.Difference, bool, error) {
	return nil, false, nil
}

func (e EmptyRowDiffer) Close() error {
	return nil
}
