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

package diff

import (
	"context"
	"errors"
	"time"

	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/dolt/go/store/diff"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type DiffSummaryProgress struct {
	Adds, Removes, Changes, CellChanges, NewSize, OldSize uint64
}

type DifferenceSummary struct {
	RowsAdded, RowsDeleted, RowsUnmodified, RowsModified, CellsModified, NewTableCount, OldTableCount uint64
}

// Summary reports a summary of diff changes between two values
func Summary(ctx context.Context, ch chan DiffSummaryProgress, v1, v2 types.Map) error {
	ad := NewAsyncDiffer(1024)
	ad.Start(ctx, v1, v2)
	defer ad.Close()

	ch <- DiffSummaryProgress{OldSize: v2.Len(), NewSize: v1.Len()}

	for !ad.IsDone() {
		diffs, err := ad.GetDiffs(100, time.Millisecond)

		if err != nil {
			return err
		}

		for i := range diffs {
			curr := diffs[i]
			err := reportChanges(curr, ch)

			if err != nil {
				return err
			}
		}
	}

	return nil
}

// DiffSummary returns a summary of diff changes between two values
func DiffSummary(ctx context.Context, v1, v2 types.Map) (DifferenceSummary, error) {
	ae := atomicerr.New()
	ch := make(chan DiffSummaryProgress)
	go func() {
		defer close(ch)
		err := Summary(ctx, ch, v1, v2)

		ae.SetIfError(err)
	}()

	acc := DiffSummaryProgress{}
	for p := range ch {
		if ae.IsSet() {
			break
		}

		acc.Adds += p.Adds
		acc.Removes += p.Removes
		acc.Changes += p.Changes
		acc.CellChanges += p.CellChanges
		acc.NewSize += p.NewSize
		acc.OldSize += p.OldSize
	}

	summary := DifferenceSummary{}
	if err := ae.Get(); err != nil {
		return summary, err
	}
	if acc.NewSize == 0 && acc.OldSize == 0 {
		return summary, nil
	}

	summary.RowsAdded = acc.Adds
	summary.RowsDeleted = acc.Removes
	summary.RowsModified = acc.Changes
	summary.RowsUnmodified = acc.OldSize - acc.Changes - acc.Removes
	summary.CellsModified = acc.CellChanges
	summary.OldTableCount = acc.OldSize
	summary.NewTableCount = acc.NewSize

	return summary, nil
}

func reportChanges(change *diff.Difference, ch chan<- DiffSummaryProgress) error {
	switch change.ChangeType {
	case types.DiffChangeAdded:
		ch <- DiffSummaryProgress{Adds: 1}
	case types.DiffChangeRemoved:
		ch <- DiffSummaryProgress{Removes: 1}
	case types.DiffChangeModified:
		oldTuple := change.OldValue.(types.Tuple)
		newTuple := change.NewValue.(types.Tuple)
		cellChanges, err := oldTuple.CountDifferencesBetweenTupleFields(newTuple)
		if err != nil {
			return err
		}
		ch <- DiffSummaryProgress{Changes: 1, CellChanges: cellChanges}
	default:
		return errors.New("unknown change type")
	}

	return nil
}
