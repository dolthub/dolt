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
	"io"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrPrimaryKeySetChanged = errors.New("primary key set changed")

type DiffSummaryProgress struct {
	Adds, Removes, Changes, CellChanges, NewRowSize, OldRowSize, NewCellSize, OldCellSize uint64
}

type prollyReporter func(ctx context.Context, vMapping val.OrdinalMapping, fromD, toD val.TupleDesc, change tree.Diff, ch chan<- DiffSummaryProgress) error
type nomsReporter func(ctx context.Context, change *diff.Difference, fromSch, toSch schema.Schema, ch chan<- DiffSummaryProgress) error

// Summary reports a summary of diff changes between two values
// todo: make package private once dolthub is migrated
func Summary(ctx context.Context, ch chan DiffSummaryProgress, from, to durable.Index, fromSch, toSch schema.Schema) (err error) {
	fc, err := from.Count()
	if err != nil {
		return err
	}
	tc, err := to.Count()
	if err != nil {
		return err
	}
	ch <- DiffSummaryProgress{OldRowSize: fc, NewRowSize: tc}

	fk, tk := schema.IsKeyless(fromSch), schema.IsKeyless(toSch)
	var keyless bool
	if fk && tk {
		keyless = true
	} else if fk != tk {
		return fmt.Errorf("cannot perform a diff between keyless and keyed schema")
	}

	if types.IsFormat_DOLT(from.Format()) {
		return diffProllyTrees(ctx, ch, keyless, from, to, fromSch, toSch)
	}

	return diffNomsMaps(ctx, ch, keyless, from, to, fromSch, toSch)
}

// SummaryForTableDelta pushes diff summary progress messages for the table delta given to the channel given
func SummaryForTableDelta(ctx context.Context, ch chan DiffSummaryProgress, td TableDelta) error {
	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("cannot retrieve schema for table %s", td.ToName).AddCause(err).Build()
	}

	if !schema.ArePrimaryKeySetsDiffable(td.Format(), fromSch, toSch) {
		return fmt.Errorf("failed to compute diff summary for table %s: %w", td.CurName(), ErrPrimaryKeySetChanged)
	}

	keyless, err := td.IsKeyless(ctx)
	if err != nil {
		return err
	}

	fromRows, toRows, err := td.GetRowData(ctx)
	if err != nil {
		return err
	}

	if types.IsFormat_DOLT(td.Format()) {
		return diffProllyTrees(ctx, ch, keyless, fromRows, toRows, fromSch, toSch)
	} else {
		return diffNomsMaps(ctx, ch, keyless, fromRows, toRows, fromSch, toSch)
	}
}

func diffProllyTrees(ctx context.Context, ch chan DiffSummaryProgress, keyless bool, from, to durable.Index, fromSch, toSch schema.Schema) error {
	_, vMapping, err := schema.MapSchemaBasedOnTagAndName(fromSch, toSch)
	if err != nil {
		return err
	}

	f := durable.ProllyMapFromIndex(from)
	t := durable.ProllyMapFromIndex(to)
	_, fVD := f.Descriptors()
	_, tVD := t.Descriptors()

	var rpr prollyReporter
	if keyless {
		rpr = reportKeylessChanges
	} else {
		fc, err := from.Count()
		if err != nil {
			return err
		}
		cfc := uint64(len(fromSch.GetAllCols().GetColumns())) * fc
		tc, err := to.Count()
		if err != nil {
			return err
		}
		ctc := uint64(len(toSch.GetAllCols().GetColumns())) * tc
		rpr = reportPkChanges
		ch <- DiffSummaryProgress{
			OldRowSize:  fc,
			NewRowSize:  tc,
			OldCellSize: cfc,
			NewCellSize: ctc,
		}
	}

	err = prolly.DiffMaps(ctx, f, t, func(ctx context.Context, diff tree.Diff) error {
		return rpr(ctx, vMapping, fVD, tVD, diff, ch)
	})
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

func diffNomsMaps(ctx context.Context, ch chan DiffSummaryProgress, keyless bool, fromRows durable.Index, toRows durable.Index, fromSch, toSch schema.Schema) error {
	var rpr nomsReporter
	if keyless {
		rpr = reportNomsKeylessChanges
	} else {
		fc, err := fromRows.Count()
		if err != nil {
			return err
		}
		cfc := uint64(len(fromSch.GetAllCols().GetColumns())) * fc
		tc, err := toRows.Count()
		if err != nil {
			return err
		}
		ctc := uint64(len(toSch.GetAllCols().GetColumns())) * tc
		rpr = reportNomsPkChanges
		ch <- DiffSummaryProgress{
			OldRowSize:  fc,
			NewRowSize:  tc,
			OldCellSize: cfc,
			NewCellSize: ctc,
		}
	}

	return summaryWithReporter(ctx, ch, durable.NomsMapFromIndex(fromRows), durable.NomsMapFromIndex(toRows), rpr, fromSch, toSch)
}

func summaryWithReporter(ctx context.Context, ch chan DiffSummaryProgress, from, to types.Map, rpr nomsReporter, fromSch, toSch schema.Schema) (err error) {
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
			err = rpr(ctx, df, fromSch, toSch, ch)
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

func reportPkChanges(ctx context.Context, vMapping val.OrdinalMapping, fromD, toD val.TupleDesc, change tree.Diff, ch chan<- DiffSummaryProgress) error {
	var sum DiffSummaryProgress
	switch change.Type {
	case tree.AddedDiff:
		sum.Adds++
	case tree.RemovedDiff:
		sum.Removes++
	case tree.ModifiedDiff:
		sum.CellChanges = prollyCountCellDiff(vMapping, fromD, toD, val.Tuple(change.From), val.Tuple(change.To))
		sum.Changes++
	default:
		return errors.New("unknown change type")
	}
	select {
	case ch <- sum:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func reportKeylessChanges(ctx context.Context, vMapping val.OrdinalMapping, fromD, toD val.TupleDesc, change tree.Diff, ch chan<- DiffSummaryProgress) error {
	var sum DiffSummaryProgress
	var n, n2 uint64
	switch change.Type {
	case tree.AddedDiff:
		n, _ = toD.GetUint64(0, val.Tuple(change.To))
		sum.Adds += n
	case tree.RemovedDiff:
		n, _ = fromD.GetUint64(0, val.Tuple(change.From))
		sum.Removes += n
	case tree.ModifiedDiff:
		n, _ = fromD.GetUint64(0, val.Tuple(change.From))
		n2, _ = toD.GetUint64(0, val.Tuple(change.To))
		if n < n2 {
			sum.Adds += n2 - n
		} else {
			sum.Removes += n - n2
		}
	default:
		return errors.New("unknown change type")
	}
	select {
	case ch <- sum:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// prollyCountCellDiff counts the number of changes columns between two tuples
// |from| and |to|. |mapping| should map columns from |from| to |to|.
func prollyCountCellDiff(mapping val.OrdinalMapping, fromD, toD val.TupleDesc, from val.Tuple, to val.Tuple) uint64 {
	newCols := uint64(toD.Count())
	changed := uint64(0)
	for i, j := range mapping {
		newCols--
		if j == -1 {
			// column was dropped
			changed++
			continue
		}

		if fromD.Types[i].Enc != toD.Types[j].Enc {
			// column type is different
			changed++
			continue
		}

		if fromD.CompareField(toD.GetField(j, to), i, from) != 0 {
			// column was modified
			changed++
			continue
		}
	}

	// some columns were added
	changed += newCols
	return changed
}

func reportNomsPkChanges(ctx context.Context, change *diff.Difference, fromSch, toSch schema.Schema, ch chan<- DiffSummaryProgress) error {
	var summary DiffSummaryProgress
	switch change.ChangeType {
	case types.DiffChangeAdded:
		summary = DiffSummaryProgress{Adds: 1}
	case types.DiffChangeRemoved:
		summary = DiffSummaryProgress{Removes: 1}
	case types.DiffChangeModified:
		oldTuple := change.OldValue.(types.Tuple)
		newTuple := change.NewValue.(types.Tuple)
		cellChanges, err := row.CountCellDiffs(oldTuple, newTuple, fromSch, toSch)
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

func reportNomsKeylessChanges(ctx context.Context, change *diff.Difference, fromSch, toSch schema.Schema, ch chan<- DiffSummaryProgress) error {
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
