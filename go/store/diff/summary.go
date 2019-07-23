// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"context"
	"fmt"
	"sync/atomic"

	humanize "github.com/dustin/go-humanize"

	"github.com/liquidata-inc/ld/dolt/go/store/datas"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/liquidata-inc/ld/dolt/go/store/util/status"
)

// Summary prints a summary of the diff between two values to stdout.
func Summary(ctx context.Context, value1, value2 types.Value) {
	if datas.IsCommit(value1) && datas.IsCommit(value2) {
		fmt.Println("Comparing commit values")
		value1 = value1.(types.Struct).Get(datas.ValueField)
		value2 = value2.(types.Struct).Get(datas.ValueField)
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
	ch := make(chan diffSummaryProgress)
	go func() {
		defer close(ch)
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
			}
		}()
		diffSummary(ctx, ch, value1, value2)
	}()

	acc := diffSummaryProgress{}
	for p := range ch {
		acc.Adds += p.Adds
		acc.Removes += p.Removes
		acc.Changes += p.Changes
		acc.NewSize += p.NewSize
		acc.OldSize += p.OldSize
		if status.WillPrint() {
			formatStatus(acc, singular, plural)
		}
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

func diffSummary(ctx context.Context, ch chan diffSummaryProgress, v1, v2 types.Value) {
	if !v1.Equals(v2) {
		if ShouldDescend(v1, v2) {
			switch v1.Kind() {
			case types.ListKind:
				diffSummaryList(ctx, ch, v1.(types.List), v2.(types.List))
			case types.MapKind:
				diffSummaryMap(ctx, ch, v1.(types.Map), v2.(types.Map))
			case types.SetKind:
				diffSummarySet(ctx, ch, v1.(types.Set), v2.(types.Set))
			case types.StructKind:
				diffSummaryStructs(ch, v1.(types.Struct), v2.(types.Struct))
			default:
				panic("Unrecognized type in diff function: " + types.TypeOf(v1).Describe(ctx) + " and " + types.TypeOf(v2).Describe(ctx))
			}
		} else {
			ch <- diffSummaryProgress{Adds: 1, Removes: 1, NewSize: 1, OldSize: 1}
		}
	}
}

func diffSummaryList(ctx context.Context, ch chan<- diffSummaryProgress, v1, v2 types.List) {
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
		v2.Diff(ctx, v1, spliceChan, stopChan)
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

func diffSummaryMap(ctx context.Context, ch chan<- diffSummaryProgress, v1, v2 types.Map) {
	diffSummaryValueChanged(ch, v1.Len(), v2.Len(), func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		v2.Diff(ctx, v1, changeChan, stopChan)
	})
}

func diffSummarySet(ctx context.Context, ch chan<- diffSummaryProgress, v1, v2 types.Set) {
	diffSummaryValueChanged(ch, v1.Len(), v2.Len(), func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		v2.Diff(ctx, v1, changeChan, stopChan)
	})
}

func diffSummaryStructs(ch chan<- diffSummaryProgress, v1, v2 types.Struct) {
	// TODO: Operate on values directly
	size1 := uint64(types.TypeOf(v1).Desc.(types.StructDesc).Len())
	size2 := uint64(types.TypeOf(v2).Desc.(types.StructDesc).Len())
	diffSummaryValueChanged(ch, size1, size2, func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		v2.Diff(v1, changeChan, stopChan)
	})
}

func diffSummaryValueChanged(ch chan<- diffSummaryProgress, oldSize, newSize uint64, f diffFunc) {
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
