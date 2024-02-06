// Copyright 2022 Dolthub, Inc.
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

package merge

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type MergeOpts struct {
	// IsCherryPick is set for cherry-pick operations.
	IsCherryPick bool
	// KeepSchemaConflicts if schema conflicts should be
	// stored, otherwise we end the merge with an error.
	KeepSchemaConflicts bool
}

type TableMerger struct {
	name string

	leftTbl  *doltdb.Table
	rightTbl *doltdb.Table
	ancTbl   *doltdb.Table

	leftSch  schema.Schema
	rightSch schema.Schema
	ancSch   schema.Schema

	rightSrc    doltdb.Rootish
	ancestorSrc doltdb.Rootish

	vrw types.ValueReadWriter
	ns  tree.NodeStore
}

func (tm TableMerger) tableHashes() (left, right, anc hash.Hash, err error) {
	if tm.leftTbl != nil {
		if left, err = tm.leftTbl.HashOf(); err != nil {
			return
		}
	}
	if tm.rightTbl != nil {
		if right, err = tm.rightTbl.HashOf(); err != nil {
			return
		}
	}
	if tm.ancTbl != nil {
		if anc, err = tm.ancTbl.HashOf(); err != nil {
			return
		}
	}
	return
}

type RootMerger struct {
	left  *doltdb.RootValue
	right *doltdb.RootValue
	anc   *doltdb.RootValue

	rightSrc doltdb.Rootish
	ancSrc   doltdb.Rootish

	vrw types.ValueReadWriter
	ns  tree.NodeStore
}

// NewMerger creates a new merger utility object.
func NewMerger(
	left, right, anc *doltdb.RootValue,
	rightSrc, ancestorSrc doltdb.Rootish,
	vrw types.ValueReadWriter,
	ns tree.NodeStore,
) (*RootMerger, error) {
	return &RootMerger{
		left:     left,
		right:    right,
		anc:      anc,
		rightSrc: rightSrc,
		ancSrc:   ancestorSrc,
		vrw:      vrw,
		ns:       ns,
	}, nil
}

type MergedTable struct {
	table    *doltdb.Table
	conflict SchemaConflict
}

// MergeTable merges schema and table data for the table tblName.
// TODO: this code will loop infinitely when merging certain schema changes
func (rm *RootMerger) MergeTable(ctx *sql.Context, tblName string, opts editor.Options, mergeOpts MergeOpts) (*MergedTable, *MergeStats, error) {
	tm, err := rm.makeTableMerger(ctx, tblName)
	if err != nil {
		return nil, nil, err
	}

	// short-circuit here if we can
	finished, stats, err := rm.maybeShortCircuit(ctx, tm, mergeOpts)
	if finished != nil || stats != nil || err != nil {
		return &MergedTable{table: finished}, stats, err
	}

	// Calculate a merge of the schemas, but don't apply it yet
	mergeSch, schConflicts, mergeInfo, diffInfo, err := SchemaMerge(ctx, tm.vrw.Format(), tm.leftSch, tm.rightSch, tm.ancSch, tblName)
	if err != nil {
		return nil, nil, err
	}
	if schConflicts.Count() > 0 {
		if !mergeOpts.KeepSchemaConflicts {
			return nil, nil, schConflicts
		}
		// handle schema conflicts above
		mt := &MergedTable{
			table:    tm.leftTbl,
			conflict: schConflicts,
		}
		stats = &MergeStats{
			Operation:       TableModified,
			SchemaConflicts: schConflicts.Count(),
		}
		return mt, stats, nil
	}

	var tbl *doltdb.Table
	if types.IsFormat_DOLT(tm.vrw.Format()) {
		tbl, stats, err = mergeProllyTable(ctx, tm, mergeSch, mergeInfo, diffInfo)
	} else {
		tbl, stats, err = mergeNomsTable(ctx, tm, mergeSch, rm.vrw, opts)
	}
	if err != nil {
		return nil, nil, err
	}
	return &MergedTable{table: tbl}, stats, nil
}

func (rm *RootMerger) makeTableMerger(ctx context.Context, tblName string) (*TableMerger, error) {
	tm := TableMerger{
		name:        tblName,
		rightSrc:    rm.rightSrc,
		ancestorSrc: rm.ancSrc,
		vrw:         rm.vrw,
		ns:          rm.ns,
	}

	var ok bool
	var err error

	tm.leftTbl, ok, err = rm.left.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if ok {
		if tm.leftSch, err = tm.leftTbl.GetSchema(ctx); err != nil {
			return nil, err
		}
	}

	tm.rightTbl, ok, err = rm.right.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if ok {
		if tm.rightSch, err = tm.rightTbl.GetSchema(ctx); err != nil {
			return nil, err
		}
	}

	tm.ancTbl, ok, err = rm.anc.GetTable(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if ok {
		if tm.ancSch, err = tm.ancTbl.GetSchema(ctx); err != nil {
			return nil, err
		}
	} else if schema.SchemasAreEqual(tm.leftSch, tm.rightSch) && tm.leftTbl != nil {
		// If left & right added the same table, fill tm.anc with an empty table
		tm.ancSch = tm.leftSch
		tm.ancTbl, err = doltdb.NewEmptyTable(ctx, rm.vrw, rm.ns, tm.ancSch)
		if err != nil {
			return nil, err
		}
	}

	return &tm, nil
}

func (rm *RootMerger) maybeShortCircuit(ctx context.Context, tm *TableMerger, opts MergeOpts) (*doltdb.Table, *MergeStats, error) {
	rootHash, mergeHash, ancHash, err := tm.tableHashes()
	if err != nil {
		return nil, nil, err
	}

	leftExists := tm.leftTbl != nil
	rightExists := tm.rightTbl != nil
	ancExists := tm.ancTbl != nil

	// Nothing changed
	if leftExists && rightExists && ancExists && rootHash == mergeHash && rootHash == ancHash {
		return tm.leftTbl, &MergeStats{Operation: TableUnmodified}, nil
	}

	// Both made identical changes
	// For keyless tables, this counts as a conflict
	if leftExists && rightExists && rootHash == mergeHash && !schema.IsKeyless(tm.leftSch) {
		return tm.leftTbl, &MergeStats{Operation: TableUnmodified}, nil
	}

	// One or both added this table
	if !ancExists {
		if rightExists && leftExists {
			if !schema.SchemasAreEqual(tm.leftSch, tm.rightSch) {
				return nil, nil, ErrSameTblAddedTwice.New(tm.name)
			}
		} else if leftExists {
			// fast-forward
			return tm.leftTbl, &MergeStats{Operation: TableUnmodified}, nil
		} else {
			// fast-forward
			return tm.rightTbl, &MergeStats{Operation: TableAdded}, nil
		}
	}

	// Deleted in both, fast-forward
	if ancExists && !leftExists && !rightExists {
		return nil, &MergeStats{Operation: TableRemoved}, nil
	}

	// Deleted in root or in merge, either a conflict (if any changes in other root) or else a fast-forward
	if ancExists && (!leftExists || !rightExists) {
		var childTable *doltdb.Table
		var childHash hash.Hash
		if rightExists {
			childTable = tm.rightTbl
			childHash = mergeHash
		} else {
			childTable = tm.leftTbl
			childHash = rootHash
		}
		if childHash != ancHash {
			schemasEqual, err := doltdb.SchemaHashesEqual(ctx, childTable, tm.ancTbl)
			if err != nil {
				return nil, nil, err
			}
			if schemasEqual {
				return nil, nil, ErrTableDeletedAndModified
			} else {
				return nil, nil, ErrTableDeletedAndSchemaModified
			}
		}
		// fast-forward
		return nil, &MergeStats{Operation: TableRemoved}, nil
	}

	// Changes only in root, table unmodified
	if mergeHash == ancHash {
		return tm.leftTbl, &MergeStats{Operation: TableUnmodified}, nil
	}

	// Changes only in merge root, fast-forward
	// TODO : no fast-forward when cherry-picking for now
	if !opts.IsCherryPick && rootHash == ancHash {
		ms := MergeStats{Operation: TableModified}
		if rootHash != mergeHash {
			ms, err = calcTableMergeStats(ctx, tm.leftTbl, tm.rightTbl)
			if err != nil {
				return nil, nil, err
			}
		}
		return tm.rightTbl, &ms, nil
	}

	// no short-circuit
	return nil, nil, nil
}

func setConflicts(ctx context.Context, cons durable.ConflictIndex, tbl, mergeTbl, ancTbl, tableToUpdate *doltdb.Table) (*doltdb.Table, error) {
	ancSch, err := ancTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	mergeSch, err := mergeTbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	cs := conflict.NewConflictSchema(ancSch, sch, mergeSch)

	tableToUpdate, err = tableToUpdate.SetConflicts(ctx, cs, cons)
	if err != nil {
		return nil, err
	}

	return tableToUpdate, nil
}

func calcTableMergeStats(ctx context.Context, tbl *doltdb.Table, mergeTbl *doltdb.Table) (MergeStats, error) {
	ms := MergeStats{Operation: TableModified}

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	mergeRows, err := mergeTbl.GetRowData(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	mergeSch, err := mergeTbl.GetSchema(ctx)
	if err != nil {
		return MergeStats{}, err
	}

	ae := atomicerr.New()
	ch := make(chan diff.DiffStatProgress)
	go func() {
		defer close(ch)
		err := diff.Stat(ctx, ch, rows, mergeRows, sch, mergeSch)

		ae.SetIfError(err)
	}()

	for p := range ch {
		if ae.IsSet() {
			break
		}

		ms.Adds += int(p.Adds)
		ms.Deletes += int(p.Removes)
		ms.Modifications += int(p.Changes)
	}

	if err := ae.Get(); err != nil {
		return MergeStats{}, err
	}

	return ms, nil
}
