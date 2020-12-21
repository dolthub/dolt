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
	"errors"
	"fmt"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"

	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

type DiffSummaryProgress struct {
	Adds, Removes, Changes, CellChanges, NewSize, OldSize uint64
}

type reporter func(ctx context.Context, change *diff.Difference, ch chan<- DiffSummaryProgress) error

// todo: make package private once dolthub is migrated
// Summary reports a summary of diff changes between two values
// Summary reports a summary of diff changes between two values
func Summary(ctx context.Context, ch chan DiffSummaryProgress, from, to types.Map) (err error) {
	ad := NewAsyncDiffer(1024)
	ad.Start(ctx, from, to)
	defer func() {
		if cerr := ad.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	ch <- DiffSummaryProgress{OldSize: from.Len(), NewSize: to.Len()}

	hasMore := true
	var diffs []*diff.Difference
	for hasMore {
		diffs, hasMore, err = ad.GetDiffs(100, time.Millisecond)
		if err != nil {
			return err
		}

		for i := range diffs {
			curr := diffs[i]
			err := reportPkChanges(ctx, curr, ch)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func SummaryForTableDelta(ctx context.Context, ch chan DiffSummaryProgress, td TableDelta) error {
	keyless, err := td.IsKeyless(ctx)
	if err != nil {
		return err
	}

	fromRows, toRows, err := td.GetMaps(ctx)
	if err != nil {
		return err
	}

	var rpr reporter
	if keyless {
		rpr = reportKeylessChanges
	} else {
		rpr = reportPkChanges
		ch <- DiffSummaryProgress{
			OldSize: fromRows.Len(),
			NewSize: toRows.Len(),
		}
	}

	return summaryWithReporter(ctx, ch, fromRows, toRows, rpr)
}

func summaryWithReporter(ctx context.Context, ch chan DiffSummaryProgress, from, to types.Map, rpr reporter) (err error) {
	ad := NewAsyncDiffer(1024)
	ad.Start(ctx, from, to)
	defer func() {
		if cerr := ad.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	var more bool
	var diffs []*diff.Difference
	for {
		diffs, more, err = ad.GetDiffs(100, time.Millisecond)
		if err != nil {
			return err
		}

		for _, df := range diffs {
			err = rpr(ctx, df, ch)
			if err != nil {
				return err
			}
		}

		if !more {
			break
		}
	}

	return nil
}

func reportPkChanges(ctx context.Context, change *diff.Difference, ch chan<- DiffSummaryProgress) error {
	var summary DiffSummaryProgress
	switch change.ChangeType {
	case types.DiffChangeAdded:
		summary = DiffSummaryProgress{Adds: 1}
	case types.DiffChangeRemoved:
		summary = DiffSummaryProgress{Removes: 1}
	case types.DiffChangeModified:
		oldTuple := change.OldValue.(types.Tuple)
		newTuple := change.NewValue.(types.Tuple)
		cellChanges, err := row.CountCellDiffs(oldTuple, newTuple)
		if err != nil {
			return err
		}
		summary = DiffSummaryProgress{Changes: 1, CellChanges: cellChanges}
	default:
		return errors.New("unknown change type")
	}
	select {
	case ch <- summary:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func reportKeylessChanges(ctx context.Context, change *diff.Difference, ch chan<- DiffSummaryProgress) error {
	var oldCard uint64
	if change.OldValue != nil {
		v, err := change.OldValue.(types.Tuple).Get(row.KeylessCardinalityValIdx)
		if err != nil {
			return err
		}
		oldCard = uint64(v.(types.Uint))
	}

	var newCard uint64
	if change.NewValue != nil {
		v, err := change.NewValue.(types.Tuple).Get(row.KeylessCardinalityValIdx)
		if err != nil {
			return err
		}
		newCard = uint64(v.(types.Uint))
	}

	var summary DiffSummaryProgress
	delta := int64(newCard) - int64(oldCard)
	if delta > 0 {
		summary = DiffSummaryProgress{Adds: uint64(delta)}
	} else if delta < 0 {
		summary = DiffSummaryProgress{Removes: uint64(-delta)}
	} else {
		return fmt.Errorf("diff with delta = 0 for key: %s", change.KeyValue.HumanReadableString())
	}

	select {
	case ch <- summary:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
