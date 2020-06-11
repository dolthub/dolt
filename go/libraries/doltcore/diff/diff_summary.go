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

	"github.com/liquidata-inc/dolt/go/store/diff"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type DiffSummaryProgress struct {
	Adds, Removes, Changes, CellChanges, NewSize, OldSize uint64
}

// Summary reports a summary of diff changes between two values
func Summary(ctx context.Context, ch chan DiffSummaryProgress, from, to types.Map) error {
	ad := NewAsyncDiffer(1024)
	ad.Start(ctx, from, to)
	defer ad.Close()

	ch <- DiffSummaryProgress{OldSize: from.Len(), NewSize: to.Len()}

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
