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
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/util/status"
)

// Summary prints a summary of the diff between two values to stdout.
func Summary(ctx context.Context, vr1 types.ValueReader, vr2 types.ValueReader, value1, value2 types.Value) {
	if is1, err := datas.IsCommit(value1); err != nil {
		panic(err)
	} else if is1 {
		if is2, err := datas.IsCommit(value2); err != nil {
			panic(err)
		} else if is2 {
			fmt.Println("Comparing commit values")

			var err error
			value1, err = datas.GetCommittedValue(ctx, vr1, value1)
			d.PanicIfError(err)

			value2, err = datas.GetCommittedValue(ctx, vr2, value2)
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

	eg, ctx := errgroup.WithContext(ctx)
	var rp atomic.Value
	ch := make(chan diffSummaryProgress)

	eg.Go(func() (err error) {
		defer close(ch)
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
				err = fmt.Errorf("panic")
			}
		}()
		err = diffSummary(ctx, ch, value1, value2)
		return
	})
	eg.Go(func() error {
		acc := diffSummaryProgress{}
	LOOP:
		for {
			select {
			case p, ok := <-ch:
				if !ok {
					break LOOP
				}
				acc.Adds += p.Adds
				acc.Removes += p.Removes
				acc.Changes += p.Changes
				acc.NewSize += p.NewSize
				acc.OldSize += p.OldSize
				if status.WillPrint() {
					formatStatus(acc, singular, plural)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		formatStatus(acc, singular, plural)
		status.Done()
		return nil
	})

	if err := eg.Wait(); err != nil {
		if r := rp.Load(); r != nil {
			panic(r)
		}
		panic(err)
	}

}

type diffSummaryProgress struct {
	Adds, Removes, Changes, NewSize, OldSize uint64
}

func diffSummary(ctx context.Context, ch chan diffSummaryProgress, v1, v2 types.Value) error {
	if !v1.Equals(v2) {
		if ShouldDescend(v1, v2) {
			var err error
			switch v1.Kind() {
			case types.ListKind:
				err = diffSummaryList(ctx, ch, v1.(types.List), v2.(types.List))
			case types.MapKind:
				err = diffSummaryMap(ctx, ch, v1.(types.Map), v2.(types.Map))
			case types.SetKind:
				err = diffSummarySet(ctx, ch, v1.(types.Set), v2.(types.Set))
			case types.StructKind:
				err = diffSummaryStructs(ctx, ch, v1.(types.Struct), v2.(types.Struct))
			default:
				panic("Unrecognized type in diff function")
			}
			if err != nil {
				return err
			}
		} else {
			ch <- diffSummaryProgress{Adds: 1, Removes: 1, NewSize: 1, OldSize: 1}
		}
	}
	return nil
}

func diffSummaryList(ctx context.Context, ch chan<- diffSummaryProgress, v1, v2 types.List) error {
	select {
	case ch <- diffSummaryProgress{OldSize: v1.Len(), NewSize: v2.Len()}:
	case <-ctx.Done():
		return ctx.Err()
	}

	spliceChan := make(chan types.Splice)
	eg, ctx := errgroup.WithContext(ctx)

	var rp atomic.Value
	eg.Go(func() (err error) {
		defer close(spliceChan)
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
				err = fmt.Errorf("panic")
			}
		}()
		return v2.Diff(ctx, v1, spliceChan)
	})

	eg.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
				err = fmt.Errorf("panic")
			}
		}()
	LOOP:
		for {
			select {
			case splice, ok := <-spliceChan:
				if !ok {
					break LOOP
				}
				var summary diffSummaryProgress
				if splice.SpRemoved == splice.SpAdded {
					summary = diffSummaryProgress{Changes: splice.SpRemoved}
				} else {
					summary = diffSummaryProgress{Adds: splice.SpAdded, Removes: splice.SpRemoved}
				}
				select {
				case ch <- summary:
				case <-ctx.Done():
					return ctx.Err()
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		if r := rp.Load(); r != nil {
			panic(r)
		}
		return err
	}
	return nil
}

func diffSummaryMap(ctx context.Context, ch chan<- diffSummaryProgress, v1, v2 types.Map) error {
	return diffSummaryValueChanged(ctx, ch, v1.Len(), v2.Len(), func(ctx context.Context, changeChan chan<- types.ValueChanged) error {
		return v2.Diff(ctx, v1, changeChan)
	})
}

func diffSummarySet(ctx context.Context, ch chan<- diffSummaryProgress, v1, v2 types.Set) error {
	return diffSummaryValueChanged(ctx, ch, v1.Len(), v2.Len(), func(ctx context.Context, changeChan chan<- types.ValueChanged) error {
		return v2.Diff(ctx, v1, changeChan)
	})
}

func diffSummaryStructs(ctx context.Context, ch chan<- diffSummaryProgress, v1, v2 types.Struct) error {
	// TODO: Operate on values directly
	t1, err := types.TypeOf(v1)
	if err != nil {
		return err
	}

	t2, err := types.TypeOf(v2)
	if err != nil {
		return err
	}

	size1 := uint64(t1.Desc.(types.StructDesc).Len())
	size2 := uint64(t2.Desc.(types.StructDesc).Len())
	return diffSummaryValueChanged(ctx, ch, size1, size2, func(ctx context.Context, changeChan chan<- types.ValueChanged) error {
		return v2.Diff(ctx, v1, changeChan)
	})
}

func diffSummaryValueChanged(ctx context.Context, ch chan<- diffSummaryProgress, oldSize, newSize uint64, f diffFunc) error {
	select {
	case ch <- diffSummaryProgress{OldSize: oldSize, NewSize: newSize}:
	case <-ctx.Done():
		return ctx.Err()
	}

	changeChan := make(chan types.ValueChanged)

	eg, ctx := errgroup.WithContext(ctx)

	var rp atomic.Value
	eg.Go(func() (err error) {
		defer close(changeChan)
		defer func() {
			if r := recover(); r != nil {
				rp.Store(r)
				err = fmt.Errorf("panic")
			}
		}()
		return f(ctx, changeChan)
	})
	eg.Go(func() error {
		return reportChanges(ctx, ch, changeChan)
	})
	if err := eg.Wait(); err != nil {
		if r := rp.Load(); r != nil {
			panic(r)
		}
		return err
	}
	return nil
}

func reportChanges(ctx context.Context, ch chan<- diffSummaryProgress, changeChan chan types.ValueChanged) error {
LOOP:
	for {
		select {
		case change, ok := <-changeChan:
			if !ok {
				break LOOP
			}
			var summary diffSummaryProgress
			switch change.ChangeType {
			case types.DiffChangeAdded:
				summary = diffSummaryProgress{Adds: 1}
			case types.DiffChangeRemoved:
				summary = diffSummaryProgress{Removes: 1}
			case types.DiffChangeModified:
				summary = diffSummaryProgress{Changes: 1}
			default:
				panic("unknown change type")
			}
			select {
			case ch <- summary:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
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
