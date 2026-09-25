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

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrPrimaryKeySetChanged = errors.New("primary key set changed")

type DiffStatProgress struct {
	Adds, Removes, Changes, CellChanges, NewRowSize, OldRowSize, NewCellSize, OldCellSize uint64
}

type prollyReporter func(ctx context.Context, vMapping val.OrdinalMapping, fromD, toD *val.TupleDesc, change tree.Diff) (DiffStatProgress, error)

// Stat reports a stat of diff changes between two values
// todo: make package private once dolthub is migrated
func Stat(ctx context.Context, ch chan DiffStatProgress, from, to durable.Index, fromSch, toSch schema.Schema) (err error) {
	fc, err := from.Count()
	if err != nil {
		return err
	}
	tc, err := to.Count()
	if err != nil {
		return err
	}
	if err := sendDiffStat(ctx, ch, DiffStatProgress{OldRowSize: fc, NewRowSize: tc}); err != nil {
		return err
	}

	fk, tk := schema.IsKeyless(fromSch), schema.IsKeyless(toSch)
	var keyless bool
	if fk && tk {
		keyless = true
	} else if fk != tk {
		return fmt.Errorf("cannot perform a diff between keyless and keyed schema")
	}

	return diffProllyTrees(ctx, ch, keyless, from, to, fromSch, toSch)
}

// StatForTableDelta pushes diff stat progress messages for the table delta given to the channel given
func StatForTableDelta(ctx context.Context, ch chan DiffStatProgress, td TableDelta) error {
	// Check for root objects first, as they're handled differently
	if td.FromRootObject != nil && td.ToRootObject != nil {
		return sendDiffStat(ctx, ch, DiffStatProgress{Changes: 1})
	} else if td.FromRootObject == nil && td.ToRootObject != nil {
		return sendDiffStat(ctx, ch, DiffStatProgress{Adds: 1})
	} else if td.FromRootObject != nil && td.ToRootObject == nil {
		return sendDiffStat(ctx, ch, DiffStatProgress{Removes: 1})
	}

	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return errhand.BuildDError("cannot retrieve schema for table %s", td.ToName).AddCause(err).Build()
	}

	if !schema.ArePrimaryKeySetsDiffable(fromSch, toSch) {
		return fmt.Errorf("failed to compute diff stat for table %s: %w", td.CurName(), ErrPrimaryKeySetChanged)
	}

	keyless, err := td.IsKeyless(ctx)
	if err != nil {
		return err
	}

	fromRows, toRows, err := td.GetRowData(ctx)
	if err != nil {
		return err
	}

	return diffProllyTrees(ctx, ch, keyless, fromRows, toRows, fromSch, toSch)
}

func diffProllyTrees(ctx context.Context, ch chan DiffStatProgress, keyless bool, from, to durable.Index, fromSch, toSch schema.Schema) error {
	_, vMapping, err := schema.MapSchemaBasedOnTagAndName(fromSch, toSch)
	if err != nil {
		return err
	}

	// Keyed row counts are stored in the tree. When either side is empty,
	// every row on the other side is an addition or deletion; no values need
	// to be read or compared. Keyless tree counts count distinct tuples, not
	// their cardinalities, so they must still be traversed.
	if !keyless {
		var fc, tc uint64
		if from != nil {
			fc, err = from.Count()
			if err != nil {
				return err
			}
		}
		if to != nil {
			tc, err = to.Count()
			if err != nil {
				return err
			}
		}
		sizes := DiffStatProgress{
			OldRowSize: fc, NewRowSize: tc,
			OldCellSize: uint64(fromSch.GetAllCols().Size()) * fc,
			NewCellSize: uint64(toSch.GetAllCols().Size()) * tc,
		}
		if fc == 0 || tc == 0 {
			sizes.Adds, sizes.Removes = tc, fc
			return sendDiffStat(ctx, ch, sizes)
		}
		if err := sendDiffStat(ctx, ch, sizes); err != nil {
			return err
		}
	}

	var f, t prolly.Map
	if from != nil {
		f, err = durable.ProllyMapFromIndex(from)
		if err != nil {
			return err
		}
	}
	if to != nil {
		t, err = durable.ProllyMapFromIndex(to)
		if err != nil {
			return err
		}
	}
	_, fVD := f.Descriptors()
	_, tVD := t.Descriptors()
	var reporter prollyReporter = statPkChange
	if keyless {
		reporter = statKeylessChange
	}

	// Progress consumers only need additive totals. Batch events rather than
	// synchronizing with the consumer for every changed row.
	var pending DiffStatProgress
	changes := 0
	flush := func() error {
		if changes == 0 {
			return nil
		}
		if err := sendDiffStat(ctx, ch, pending); err != nil {
			return err
		}
		pending = DiffStatProgress{}
		changes = 0
		return nil
	}
	// TODO: Use vMapping to account for columns added or removed on otherwise
	// unchanged rows, matching the existing diff-stat schema-change behavior.
	err = prolly.DiffMaps(ctx, f, t, false, func(ctx context.Context, change tree.Diff) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		stat, err := reporter(ctx, vMapping, fVD, tVD, change)
		if err != nil {
			return err
		}
		pending.Adds += stat.Adds
		pending.Removes += stat.Removes
		pending.Changes += stat.Changes
		pending.CellChanges += stat.CellChanges
		changes++
		if changes == diffStatBatchSize {
			return flush()
		}
		return nil
	})
	if err != nil && err != io.EOF {
		return err
	}
	return flush()
}

const diffStatBatchSize = 1024

func sendDiffStat(ctx context.Context, ch chan<- DiffStatProgress, stat DiffStatProgress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case ch <- stat:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func statPkChange(ctx context.Context, vMapping val.OrdinalMapping, fromD, toD *val.TupleDesc, change tree.Diff) (DiffStatProgress, error) {
	var stat DiffStatProgress
	switch change.Type {
	case tree.AddedDiff:
		stat.Adds++
	case tree.RemovedDiff:
		stat.Removes++
	case tree.ModifiedDiff:
		cellChanges, err := prollyCountCellDiff(ctx, vMapping, fromD, toD, val.Tuple(change.From), val.Tuple(change.To))
		if err != nil {
			return stat, err
		}
		stat.CellChanges = cellChanges
		stat.Changes++
	default:
		return stat, errors.New("unknown change type")
	}
	return stat, nil
}

func statKeylessChange(ctx context.Context, vMapping val.OrdinalMapping, fromD, toD *val.TupleDesc, change tree.Diff) (DiffStatProgress, error) {
	var stat DiffStatProgress
	var n, n2 uint64
	switch change.Type {
	case tree.AddedDiff:
		n, _ = toD.GetUint64(0, val.Tuple(change.To))
		stat.Adds += n
	case tree.RemovedDiff:
		n, _ = fromD.GetUint64(0, val.Tuple(change.From))
		stat.Removes += n
	case tree.ModifiedDiff:
		n, _ = fromD.GetUint64(0, val.Tuple(change.From))
		n2, _ = toD.GetUint64(0, val.Tuple(change.To))
		if n < n2 {
			stat.Adds += n2 - n
		} else {
			stat.Removes += n - n2
		}
	default:
		return stat, errors.New("unknown change type")
	}
	return stat, nil
}

// prollyCountCellDiff counts the number of changes columns between two tuples
// |from| and |to|. |mapping| should map columns from |from| to |to|.
func prollyCountCellDiff(ctx context.Context, mapping val.OrdinalMapping, fromD, toD *val.TupleDesc, from, to val.Tuple) (uint64, error) {
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

		cmp, err := fromD.CompareField(ctx, toD.GetField(j, to), i, from)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			// column was modified
			changed++
			continue
		}
	}

	// some columns were added
	changed += newCols
	return changed, nil
}
