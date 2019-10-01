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
	"fmt"
	"sync/atomic"

	humanize "github.com/dustin/go-humanize"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/store/atomicerr"
	"github.com/liquidata-inc/dolt/go/store/datas"
	"github.com/liquidata-inc/dolt/go/store/diff"
	"github.com/liquidata-inc/dolt/go/store/types"
	"github.com/liquidata-inc/dolt/go/store/util/status"
)

type diffFunc func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{})

// Summary prints a summary of the diff between two values
func Summary(ctx context.Context, v1, v2 types.Value, colLen int) errhand.VerboseError {
	if is1, err := datas.IsCommit(v1); err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	} else if is1 {
		if is2, err := datas.IsCommit(v2); err != nil {
			return errhand.BuildDError("").AddCause(err).Build()
		} else if is2 {
			cli.Println("Comparing commit values")

			var err error
			v1, _, err = v1.(types.Struct).MaybeGet(datas.ValueField)
			if err != nil {
				return errhand.BuildDError("").AddCause(err).Build()
			}

			v2, _, err = v2.(types.Struct).MaybeGet(datas.ValueField)
			if err != nil {
				return errhand.BuildDError("").AddCause(err).Build()
			}
		}
	}

	var singular, plural string
	if v1.Kind() == v2.Kind() {
		switch v1.Kind() {
		case types.StructKind:
			singular = "Field"
			plural = "Fields"
		case types.MapKind:
			singular = "Entry"
			plural = "Entries"
		default:
			singular = "Value"
			plural = "Values"
		}
	}

	var rp atomic.Value
	var verr errhand.VerboseError
	ae := atomicerr.New()
	ch := make(chan diffSummaryProgress)
	go func() {
		defer close(ch)
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
			}
		}()
		verr = diffSummary(ctx, ae, ch, v2, v1)
	}()

	if verr != nil {
		return verr
	}

	acc := diffSummaryProgress{}
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

	if err := ae.Get(); err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	if r := rp.Load(); r != nil {
		err := fmt.Errorf("panic occured during closing: %v", r)
		return errhand.BuildDError("").AddCause(err).Build()
	}
	if acc.NewSize > 0 || acc.OldSize > 0 {
		formatStatus(acc, singular, plural, colLen)
	} else {
		cli.Println("No data changes. See schema changes by using -s or --schema.")
	}
	status.Done()

	return nil
}

type diffSummaryProgress struct {
	Adds, Removes, Changes, CellChanges, NewSize, OldSize uint64
}

func diffSummary(ctx context.Context, ae *atomicerr.AtomicError, ch chan diffSummaryProgress, v1, v2 types.Value) errhand.VerboseError {
	var verr errhand.VerboseError
	if !v1.Equals(v2) {
		if diff.ShouldDescend(v1, v2) {
			switch v1.Kind() {
			case types.ListKind:
				verr = diffSummaryList(ctx, ae, ch, v1.(types.List), v2.(types.List))
			case types.MapKind:
				verr = diffSummaryMap(ctx, ae, ch, v1.(types.Map), v2.(types.Map))
			case types.SetKind:
				verr = diffSummarySet(ctx, ae, ch, v1.(types.Set), v2.(types.Set))
			case types.StructKind:
				verr = diffSummaryStructs(ae, ch, v1.(types.Struct), v2.(types.Struct))
			default:
				return errhand.BuildDError("Unrecognized type in diff function").Build()
			}
		} else {
			ch <- diffSummaryProgress{Adds: 1, Removes: 1, NewSize: 1, OldSize: 1}
		}
	}
	if verr != nil {
		return verr
	}
	return nil
}

func diffSummaryList(ctx context.Context, ae *atomicerr.AtomicError, ch chan<- diffSummaryProgress, v1, v2 types.List) errhand.VerboseError {
	ch <- diffSummaryProgress{OldSize: v1.Len(), NewSize: v2.Len()}

	spliceChan := make(chan types.Splice)
	stopChan := make(chan struct{}, 1) // buffer size of 1, so this won't block if diff already finished

	var rp atomic.Value
	var err error
	go func() {
		defer close(spliceChan)
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
			}
		}()
		err = v2.Diff(ctx, v1, spliceChan, stopChan)
	}()

	if err != nil {
		return errhand.BuildDError("").AddCause(err).Build()
	}

	for splice := range spliceChan {
		if splice.SpRemoved == splice.SpAdded {
			ch <- diffSummaryProgress{Changes: splice.SpRemoved}
		} else {
			ch <- diffSummaryProgress{Adds: splice.SpAdded, Removes: splice.SpRemoved}
		}
	}

	if r := rp.Load(); r != nil {
		err := fmt.Errorf("panic occured during closing: %v", r)
		return errhand.BuildDError("").AddCause(err).Build()
	}

	return nil
}

func diffSummaryMap(ctx context.Context, ae *atomicerr.AtomicError, ch chan<- diffSummaryProgress, v1, v2 types.Map) errhand.VerboseError {
	verr := diffSummaryValueChanged(ae, ch, v1.Len(), v2.Len(), func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		v2.Diff(ctx, v1, ae, changeChan, stopChan)
	})
	if verr != nil {
		return verr
	}
	return nil
}

func diffSummarySet(ctx context.Context, ae *atomicerr.AtomicError, ch chan<- diffSummaryProgress, v1, v2 types.Set) errhand.VerboseError {
	verr := diffSummaryValueChanged(ae, ch, v1.Len(), v2.Len(), func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		v2.Diff(ctx, v1, ae, changeChan, stopChan)
	})
	if verr != nil {
		return verr
	}
	return nil
}

func diffSummaryStructs(ae *atomicerr.AtomicError, ch chan<- diffSummaryProgress, v1, v2 types.Struct) errhand.VerboseError {
	// TODO: Operate on values directly
	t1, err := types.TypeOf(v1)

	if ae.SetIfError(err) {
		return nil
	}

	t2, err := types.TypeOf(v2)

	if ae.SetIfError(err) {
		return nil
	}

	size1 := uint64(t1.Desc.(types.StructDesc).Len())
	size2 := uint64(t2.Desc.(types.StructDesc).Len())
	verr := diffSummaryValueChanged(ae, ch, size1, size2, func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		err := v2.Diff(v1, changeChan, stopChan)
		ae.SetIfError(err)
	})
	if verr != nil {
		return verr
	}
	return nil
}

func diffSummaryValueChanged(ae *atomicerr.AtomicError, ch chan<- diffSummaryProgress, oldSize, newSize uint64, f diffFunc) errhand.VerboseError {
	ch <- diffSummaryProgress{OldSize: oldSize, NewSize: newSize}

	changeChan := make(chan types.ValueChanged)
	stopChan := make(chan struct{}, 1) // buffer size of 1, so this won't block if diff already finished

	var rp atomic.Value
	go func() {
		defer close(changeChan)
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
			}
		}()
		f(changeChan, stopChan)
	}()
	verr := reportChanges(ch, changeChan)
	if verr != nil {
		return verr
	}
	if r := rp.Load(); r != nil {
		err := fmt.Errorf("panic occured during closing: %v", r)
		return errhand.BuildDError("").AddCause(err).Build()
	}
	return nil
}

func reportChanges(ch chan<- diffSummaryProgress, changeChan chan types.ValueChanged) errhand.VerboseError {
	var verr errhand.VerboseError
	for change := range changeChan {
		switch change.ChangeType {
		case types.DiffChangeAdded:
			ch <- diffSummaryProgress{Adds: 1}
		case types.DiffChangeRemoved:
			ch <- diffSummaryProgress{Removes: 1}
		case types.DiffChangeModified:
			ch <- diffSummaryProgress{Changes: 1}
			cellChanges, err := getCellChanges(change.NewValue, change.OldValue, change.Key)
			if err != nil {
				verr = errhand.BuildDError("").AddCause(err).Build()
			}
			ch <- diffSummaryProgress{CellChanges: cellChanges}
		default:
			return errhand.BuildDError("unknown change type").Build()
		}
	}
	if verr != nil {
		return verr
	}
	return nil
}

func getCellChanges(oldVal, newVal, key types.Value) (uint64, error) {
	var cellsChanged uint64
	var err error

	oldTuple := oldVal.(types.Tuple)
	newTuple := newVal.(types.Tuple)

	if oldTuple.Len() > newTuple.Len() {
		cellsChanged, err = oldTuple.CountDifferencesBetweenTupleFields(newTuple)
	} else {
		cellsChanged, err = newTuple.CountDifferencesBetweenTupleFields(oldTuple)
	}

	if err != nil {
		return 0, err
	}

	return cellsChanged, nil
}

func formatStatus(acc diffSummaryProgress, singular, plural string, colLen int) {
	pluralize := func(singular, plural string, n uint64) string {
		var noun string
		if n != 1 {
			noun = plural
		} else {
			noun = singular
		}
		return fmt.Sprintf("%s %s", humanize.Comma(int64(n)), noun)
	}

	rowsUnmodified := uint64(acc.OldSize - acc.Changes - acc.Removes)
	unmodified := pluralize("Row Unmodified", "Rows Unmodified", rowsUnmodified)
	insertions := pluralize("Row Added", "Rows Added", acc.Adds)
	deletions := pluralize("Row Deleted", "Rows Deleted", acc.Removes)
	changes := pluralize("Row Modified", "Rows Modified", acc.Changes)
	cellChanges := pluralize("Cell Modified", "Cells Modified", acc.CellChanges)

	oldValues := pluralize(singular, plural, acc.OldSize)
	newValues := pluralize(singular, plural, acc.NewSize)

	percentCellsChanged := float64(100*acc.CellChanges) / (float64(acc.OldSize) * float64(colLen))

	cli.Printf("%s (%.2f%%)\n", unmodified, (float64(100*rowsUnmodified) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", insertions, (float64(100*acc.Adds) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", deletions, (float64(100*acc.Removes) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", changes, (float64(100*acc.Changes) / float64(acc.OldSize)))
	cli.Printf("%s (%.2f%%)\n", cellChanges, percentCellsChanged)
	cli.Printf("(%s vs %s)\n", oldValues, newValues)
}
