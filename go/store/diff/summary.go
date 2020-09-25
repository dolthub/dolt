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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"context"
	"fmt"
	"sync/atomic"

	humanize "github.com/dustin/go-humanize"

	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/status"
)

// Summary prints a summary of the diff between two values to stdout.
func Summary(ctx context.Context, value1, value2 types.Value) {
	if is1, err := datas.IsCommit(value1); err != nil {
		panic(err)
	} else if is1 {
		if is2, err := datas.IsCommit(value2); err != nil {
			panic(err)
		} else if is2 {
			fmt.Println("Comparing commit values")

			var err error
			value1, _, err = value1.(types.Struct).MaybeGet(datas.ValueField)
			d.PanicIfError(err)

			value2, _, err = value2.(types.Struct).MaybeGet(datas.ValueField)
			d.PanicIfError(err)
		}
	}

	var singular, plural string
	if value1.Kind() == value2.Kind() {
		switch value1.Kind() {
		case types.StructKind:
			singular = "field"
			plural = "fields"
		case types.MapKind:
			singular = "entry"
			plural = "entries"
		default:
			singular = "value"
			plural = "values"
		}
	}

	var rp atomic.Value
	ae := atomicerr.New()
	ch := make(chan diffSummaryProgress)
	go func() {
		defer close(ch)
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
			}
		}()
		diffSummary(ctx, ae, ch, value1, value2)
	}()

	acc := diffSummaryProgress{}
	for p := range ch {
		if ae.IsSet() {
			break
		}

		acc.Adds += p.Adds
		acc.Removes += p.Removes
		acc.Changes += p.Changes
		acc.NewSize += p.NewSize
		acc.OldSize += p.OldSize
		if status.WillPrint() {
			formatStatus(acc, singular, plural)
		}
	}

	if err := ae.Get(); err != nil {
		panic(err)
	}

	if r := rp.Load(); r != nil {
		panic(r)
	}

	formatStatus(acc, singular, plural)
	status.Done()
}

type diffSummaryProgress struct {
	Adds, Removes, Changes, NewSize, OldSize uint64
}

func diffSummary(ctx context.Context, ae *atomicerr.AtomicError, ch chan diffSummaryProgress, v1, v2 types.Value) {
	if !v1.Equals(v2) {
		if ShouldDescend(v1, v2) {
			switch v1.Kind() {
			case types.ListKind:
				diffSummaryList(ctx, ae, ch, v1.(types.List), v2.(types.List))
			case types.MapKind:
				diffSummaryMap(ctx, ae, ch, v1.(types.Map), v2.(types.Map))
			case types.SetKind:
				diffSummarySet(ctx, ae, ch, v1.(types.Set), v2.(types.Set))
			case types.StructKind:
				diffSummaryStructs(ae, ch, v1.(types.Struct), v2.(types.Struct))
			default:
				panic("Unrecognized type in diff function")
			}
		} else {
			ch <- diffSummaryProgress{Adds: 1, Removes: 1, NewSize: 1, OldSize: 1}
		}
	}
}

func diffSummaryList(ctx context.Context, ae *atomicerr.AtomicError, ch chan<- diffSummaryProgress, v1, v2 types.List) {
	ch <- diffSummaryProgress{OldSize: v1.Len(), NewSize: v2.Len()}

	spliceChan := make(chan types.Splice)
	stopChan := make(chan struct{}, 1) // buffer size of 1, so this won't block if diff already finished

	var rp atomic.Value
	go func() {
		defer close(spliceChan)
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
			}
		}()
		err := v2.Diff(ctx, v1, spliceChan, stopChan)
		d.PanicIfError(err)
	}()

	for splice := range spliceChan {
		if splice.SpRemoved == splice.SpAdded {
			ch <- diffSummaryProgress{Changes: splice.SpRemoved}
		} else {
			ch <- diffSummaryProgress{Adds: splice.SpAdded, Removes: splice.SpRemoved}
		}
	}

	if r := rp.Load(); r != nil {
		panic(r)
	}
}

func diffSummaryMap(ctx context.Context, ae *atomicerr.AtomicError, ch chan<- diffSummaryProgress, v1, v2 types.Map) {
	diffSummaryValueChanged(ae, ch, v1.Len(), v2.Len(), func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		v2.Diff(ctx, v1, ae, changeChan, stopChan)
	})
}

func diffSummarySet(ctx context.Context, ae *atomicerr.AtomicError, ch chan<- diffSummaryProgress, v1, v2 types.Set) {
	diffSummaryValueChanged(ae, ch, v1.Len(), v2.Len(), func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		v2.Diff(ctx, v1, ae, changeChan, stopChan)
	})
}

func diffSummaryStructs(ae *atomicerr.AtomicError, ch chan<- diffSummaryProgress, v1, v2 types.Struct) {
	// TODO: Operate on values directly
	t1, err := types.TypeOf(v1)

	if ae.SetIfError(err) {
		return
	}

	t2, err := types.TypeOf(v2)

	if ae.SetIfError(err) {
		return
	}

	size1 := uint64(t1.Desc.(types.StructDesc).Len())
	size2 := uint64(t2.Desc.(types.StructDesc).Len())
	diffSummaryValueChanged(ae, ch, size1, size2, func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		err := v2.Diff(v1, changeChan, stopChan)
		ae.SetIfError(err)
	})
}

func diffSummaryValueChanged(ae *atomicerr.AtomicError, ch chan<- diffSummaryProgress, oldSize, newSize uint64, f diffFunc) {
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
	reportChanges(ch, changeChan)
	if r := rp.Load(); r != nil {
		panic(r)
	}
}

func reportChanges(ch chan<- diffSummaryProgress, changeChan chan types.ValueChanged) {
	for change := range changeChan {
		switch change.ChangeType {
		case types.DiffChangeAdded:
			ch <- diffSummaryProgress{Adds: 1}
		case types.DiffChangeRemoved:
			ch <- diffSummaryProgress{Removes: 1}
		case types.DiffChangeModified:
			ch <- diffSummaryProgress{Changes: 1}
		default:
			panic("unknown change type")
		}
	}
}

func formatStatus(acc diffSummaryProgress, singular, plural string) {
	pluralize := func(singular, plural string, n uint64) string {
		var noun string
		if n != 1 {
			noun = plural
		} else {
			noun = singular
		}
		return fmt.Sprintf("%s %s", humanize.Comma(int64(n)), noun)
	}

	insertions := pluralize("insertion", "insertions", acc.Adds)
	deletions := pluralize("deletion", "deletions", acc.Removes)
	changes := pluralize("change", "changes", acc.Changes)

	oldValues := pluralize(singular, plural, acc.OldSize)
	newValues := pluralize(singular, plural, acc.NewSize)

	status.Printf("%s (%.2f%%), %s (%.2f%%), %s (%.2f%%), (%s vs %s)", insertions, (float64(100*acc.Adds) / float64(acc.OldSize)), deletions, (float64(100*acc.Removes) / float64(acc.OldSize)), changes, (float64(100*acc.Changes) / float64(acc.OldSize)), oldValues, newValues)
}
