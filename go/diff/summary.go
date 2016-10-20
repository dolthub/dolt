// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package diff

import (
	"fmt"

	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/status"
	humanize "github.com/dustin/go-humanize"
)

// Summary prints a summary of the diff between two values to stdout.
func Summary(value1, value2 types.Value) {
	if datas.IsCommitType(value1.Type()) && datas.IsCommitType(value2.Type()) {
		fmt.Println("Comparing commit values")
		value1 = value1.(types.Struct).Get(datas.ValueField)
		value2 = value2.(types.Struct).Get(datas.ValueField)
	}

	var singular, plural string
	if value1.Type().Kind() == value2.Type().Kind() {
		switch value1.Type().Kind() {
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

	ch := make(chan diffSummaryProgress)
	go func() {
		diffSummary(ch, value1, value2)
		close(ch)
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
	formatStatus(acc, singular, plural)
	status.Done()
}

type diffSummaryProgress struct {
	Adds, Removes, Changes, NewSize, OldSize uint64
}

func diffSummary(ch chan diffSummaryProgress, v1, v2 types.Value) {
	if !v1.Equals(v2) {
		if shouldDescend(v1, v2) {
			switch v1.Type().Kind() {
			case types.ListKind:
				diffSummaryList(ch, v1.(types.List), v2.(types.List))
			case types.MapKind:
				diffSummaryMap(ch, v1.(types.Map), v2.(types.Map))
			case types.SetKind:
				diffSummarySet(ch, v1.(types.Set), v2.(types.Set))
			case types.StructKind:
				diffSummaryStructs(ch, v1.(types.Struct), v2.(types.Struct))
			default:
				panic("Unrecognized type in diff function: " + v1.Type().Describe() + " and " + v2.Type().Describe())
			}
		} else {
			ch <- diffSummaryProgress{Adds: 1, Removes: 1, NewSize: 1, OldSize: 1}
		}
	}
}

func diffSummaryList(ch chan<- diffSummaryProgress, v1, v2 types.List) {
	ch <- diffSummaryProgress{OldSize: v1.Len(), NewSize: v2.Len()}

	spliceChan := make(chan types.Splice)
	stopChan := make(chan struct{}, 1) // buffer size of 1, so this won't block if diff already finished

	go func() {
		v2.Diff(v1, spliceChan, stopChan)
		close(spliceChan)
	}()

	for splice := range spliceChan {
		if splice.SpRemoved == splice.SpAdded {
			ch <- diffSummaryProgress{Changes: splice.SpRemoved}
		} else {
			ch <- diffSummaryProgress{Adds: splice.SpAdded, Removes: splice.SpRemoved}
		}
	}
}

func diffSummaryMap(ch chan<- diffSummaryProgress, v1, v2 types.Map) {
	diffSummaryValueChanged(ch, v1.Len(), v2.Len(), func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		v2.Diff(v1, changeChan, stopChan)
	})
}

func diffSummarySet(ch chan<- diffSummaryProgress, v1, v2 types.Set) {
	diffSummaryValueChanged(ch, v1.Len(), v2.Len(), func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		v2.Diff(v1, changeChan, stopChan)
	})
}

func diffSummaryStructs(ch chan<- diffSummaryProgress, v1, v2 types.Struct) {
	size1 := uint64(v1.Type().Desc.(types.StructDesc).Len())
	size2 := uint64(v2.Type().Desc.(types.StructDesc).Len())
	diffSummaryValueChanged(ch, size1, size2, func(changeChan chan<- types.ValueChanged, stopChan <-chan struct{}) {
		v2.Diff(v1, changeChan, stopChan)
	})
}

func diffSummaryValueChanged(ch chan<- diffSummaryProgress, oldSize, newSize uint64, f diffFunc) {
	ch <- diffSummaryProgress{OldSize: oldSize, NewSize: newSize}

	changeChan := make(chan types.ValueChanged)
	stopChan := make(chan struct{}, 1) // buffer size of 1, so this won't block if diff already finished

	go func() {
		f(changeChan, stopChan)
		close(changeChan)
	}()
	reportChanges(ch, changeChan)
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
