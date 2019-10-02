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

	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/dolt/go/store/diff"
	"github.com/liquidata-inc/dolt/go/store/types"
)

type diffFunc func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{})

type DiffSummaryProgress struct {
	Adds, Removes, Changes, CellChanges, NewSize, OldSize uint64
}

// Summary reports a summary of diff changes between two values
func Summary(ctx context.Context, ae *atomicerr.AtomicError, ch chan DiffSummaryProgress, v1, v2 types.Map) {
	if !v1.Equals(v2) {
		if diff.ShouldDescend(v1, v2) {
			diffSummaryValueChanged(ae, ch, v1.Len(), v2.Len(), func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
				v2.Diff(ctx, v1, ae, changeChan, stopChan)
			})
		} else {
			ch <- DiffSummaryProgress{Adds: 1, Removes: 1, NewSize: 1, OldSize: 1}
		}
	}
}

func diffSummaryValueChanged(ae *atomicerr.AtomicError, ch chan<- DiffSummaryProgress, oldSize, newSize uint64, f diffFunc) {
	ch <- DiffSummaryProgress{OldSize: oldSize, NewSize: newSize}

	changeChan := make(chan types.ValueChanged)
	stopChan := make(chan struct{}, 1) // buffer size of 1, so this won't block if diff already finished

	go func() {
		defer close(changeChan)
		f(changeChan, stopChan)
	}()

	reportChanges(ae, ch, changeChan)
}

func reportChanges(ae *atomicerr.AtomicError, ch chan<- DiffSummaryProgress, changeChan chan types.ValueChanged) {
	var err error
	for change := range changeChan {
		switch change.ChangeType {
		case types.DiffChangeAdded:
			ch <- DiffSummaryProgress{Adds: 1}
		case types.DiffChangeRemoved:
			ch <- DiffSummaryProgress{Removes: 1}
		case types.DiffChangeModified:
			var cellChanges uint64
			cellChanges, err = getCellChanges(change.NewValue, change.OldValue, change.Key)
			ch <- DiffSummaryProgress{Changes: 1, CellChanges: cellChanges}
		default:
			err = errors.New("unknown change type")
		}
	}
	ae.SetIfError(err)
}

func getCellChanges(oldVal, newVal, key types.Value) (uint64, error) {
	oldTuple := oldVal.(types.Tuple)
	newTuple := newVal.(types.Tuple)

	if oldTuple.Len() > newTuple.Len() {
		return oldTuple.CountDifferencesBetweenTupleFields(newTuple)
	}
	return newTuple.CountDifferencesBetweenTupleFields(oldTuple)
}
