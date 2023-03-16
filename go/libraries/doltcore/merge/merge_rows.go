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
	"errors"
	"fmt"
	"strings"

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
	IsCherryPick bool
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

// MergeTable merges schema and table data for the table tblName.
// TODO: this code will loop infinitely when merging certain schema changes
func (rm *RootMerger) MergeTable(ctx context.Context, tblName string, opts editor.Options, mergeOpts MergeOpts) (*doltdb.Table, *MergeStats, error) {
	tm, err := rm.makeTableMerger(ctx, tblName)
	if err != nil {
		return nil, nil, err
	}

	// short-circuit here if we can
	finished, stats, err := rm.maybeShortCircuit(ctx, tm, mergeOpts)
	if finished != nil || stats != nil || err != nil {
		return finished, stats, err
	}

	if mergeOpts.IsCherryPick && !schema.SchemasAreEqual(tm.leftSch, tm.rightSch) {
		return nil, nil, errors.New(fmt.Sprintf("schema changes not supported: %s table schema does not match in current HEAD and cherry-pick commit.", tblName))
	}

	mergeSch, schConflicts, err := SchemaMerge(ctx, tm.vrw.Format(), tm.leftSch, tm.rightSch, tm.ancSch, tblName)
	if err != nil {
		return nil, nil, err
	}
	if schConflicts.Count() != 0 {
		// error on schema conflicts for now
		return nil, nil, fmt.Errorf("%w.\n%s", ErrSchemaConflict, schConflicts.AsError().Error())
	}

	if types.IsFormat_DOLT(tm.vrw.Format()) {
		err = rm.maybeAbortDueToUnmergeableIndexes(tm.name, tm.leftSch, tm.rightSch, mergeSch)
		if err != nil {
			return nil, nil, err
		}
	}

	mergeTbl, err := tm.leftTbl.UpdateSchema(ctx, mergeSch)
	if err != nil {
		return nil, nil, err
	}

	if types.IsFormat_DOLT(mergeTbl.Format()) {
		mergeTbl, err = mergeTableArtifacts(ctx, tm, mergeTbl)
		if err != nil {
			return nil, nil, err
		}

		var stats *MergeStats
		mergeTbl, stats, err = mergeTableData(ctx, tm, mergeSch, mergeTbl)
		if err != nil {
			return nil, nil, err
		}

		n, err := mergeTbl.NumRowsInConflict(ctx)
		if err != nil {
			return nil, nil, err
		}
		stats.Conflicts = int(n)

		mergeTbl, err = mergeAutoIncrementValues(ctx, tm.leftTbl, tm.rightTbl, mergeTbl)
		if err != nil {
			return nil, nil, err
		}
		return mergeTbl, stats, nil
	}

	// If any indexes were added during the merge, then we need to generate their row data to add to our updated table.
	addedIndexesSet := make(map[string]string)
	for _, index := range mergeSch.Indexes().AllIndexes() {
		addedIndexesSet[strings.ToLower(index.Name())] = index.Name()
	}
	for _, index := range tm.leftSch.Indexes().AllIndexes() {
		delete(addedIndexesSet, strings.ToLower(index.Name()))
	}
	for _, addedIndex := range addedIndexesSet {
		newIndexData, err := editor.RebuildIndex(ctx, mergeTbl, addedIndex, opts)
		if err != nil {
			return nil, nil, err
		}
		mergeTbl, err = mergeTbl.SetNomsIndexRows(ctx, addedIndex, newIndexData)
		if err != nil {
			return nil, nil, err
		}
	}

	updatedTblEditor, err := editor.NewTableEditor(ctx, mergeTbl, mergeSch, tblName, opts)
	if err != nil {
		return nil, nil, err
	}

	rows, err := tm.leftTbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	mergeRows, err := tm.rightTbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	ancRows, err := tm.ancTbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}

	resultTbl, cons, stats, err := mergeNomsTableData(ctx, rm.vrw, tblName, mergeSch, rows, mergeRows, durable.NomsMapFromIndex(ancRows), updatedTblEditor)
	if err != nil {
		return nil, nil, err
	}

	if cons.Len() > 0 {
		resultTbl, err = setConflicts(ctx, durable.ConflictIndexFromNomsMap(cons, rm.vrw), tm.leftTbl, tm.rightTbl, tm.ancTbl, resultTbl)
		if err != nil {
			return nil, nil, err
		}
		stats.Conflicts = int(cons.Len())
	}

	resultTbl, err = mergeAutoIncrementValues(ctx, tm.leftTbl, tm.rightTbl, resultTbl)
	if err != nil {
		return nil, nil, err
	}

	return resultTbl, stats, nil
}

func (rm *RootMerger) makeTableMerger(ctx context.Context, tblName string) (TableMerger, error) {
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
		return TableMerger{}, err
	}
	if ok {
		if tm.leftSch, err = tm.leftTbl.GetSchema(ctx); err != nil {
			return TableMerger{}, err
		}
	}

	tm.rightTbl, ok, err = rm.right.GetTable(ctx, tblName)
	if err != nil {
		return TableMerger{}, err
	}
	if ok {
		if tm.rightSch, err = tm.rightTbl.GetSchema(ctx); err != nil {
			return TableMerger{}, err
		}
	}

	tm.ancTbl, ok, err = rm.anc.GetTable(ctx, tblName)
	if err != nil {
		return TableMerger{}, err
	}
	if ok {
		if tm.ancSch, err = tm.ancTbl.GetSchema(ctx); err != nil {
			return TableMerger{}, err
		}
	} else if schema.SchemasAreEqual(tm.leftSch, tm.rightSch) && tm.leftTbl != nil {
		// If left & right added the same table, fill tm.anc with an empty table
		tm.ancSch = tm.leftSch
		tm.ancTbl, err = doltdb.NewEmptyTable(ctx, rm.vrw, rm.ns, tm.ancSch)
		if err != nil {
			return TableMerger{}, err
		}
	}

	return tm, nil
}

func (rm *RootMerger) maybeShortCircuit(ctx context.Context, tm TableMerger, opts MergeOpts) (*doltdb.Table, *MergeStats, error) {
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
		if opts.IsCherryPick && leftExists && !rightExists {
			// TODO : this is either drop table or rename table case
			// We can delete only if the table in current HEAD and parent commit contents are exact the same (same schema and same data);
			// otherwise, return ErrTableDeletedAndModified
			// We need to track renaming of a table --> the renamed table could be added as new table
			err = fmt.Errorf("schema changes not supported: %s table was renamed or dropped in cherry-pick commit", tm.name)
			return nil, &MergeStats{Operation: TableModified}, err
		}

		if (rightExists && mergeHash != ancHash) ||
			(leftExists && rootHash != ancHash) {
			return nil, nil, ErrTableDeletedAndModified
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

func (rm *RootMerger) maybeAbortDueToUnmergeableIndexes(tableName string, leftSchema, rightSchema, targetSchema schema.Schema) error {
	leftOk, err := validateTupleFields(leftSchema, targetSchema)
	if err != nil {
		return err
	}

	rightOk, err := validateTupleFields(rightSchema, targetSchema)
	if err != nil {
		return err
	}

	if !leftOk || !rightOk {
		return fmt.Errorf("table %s can't be automatically merged.\nTo merge this table, make the schema on the source and target branch equal.", tableName)
	}

	return nil
}

func validateTupleFields(existingSch schema.Schema, targetSch schema.Schema) (bool, error) {
	existingVD := existingSch.GetValueDescriptor()
	targetVD := targetSch.GetValueDescriptor()

	_, valMapping, err := schema.MapSchemaBasedOnTagAndName(existingSch, targetSch)
	if err != nil {
		return false, err
	}

	for i, j := range valMapping {

		// If the field positions have changed between existing and target, bail.
		if i != j {
			return false, nil
		}

		// If the field types have changed between existing and target, bail.
		if existingVD.Types[i].Enc != targetVD.Types[j].Enc {
			return false, nil
		}

		// If a not null constraint was added, bail.
		if existingVD.Types[j].Nullable && !targetVD.Types[j].Nullable {
			return false, nil
		}

		// If the collation was changed, bail.
		// Different collations will affect the ordering of any secondary indexes using this column.
		existingStr, ok1 := existingSch.GetNonPKCols().GetByIndex(i).TypeInfo.ToSqlType().(sql.StringType)
		targetStr, ok2 := targetSch.GetNonPKCols().GetByIndex(i).TypeInfo.ToSqlType().(sql.StringType)

		if ok1 && ok2 && !existingStr.Collation().Equals(targetStr.Collation()) {
			return false, nil
		}
	}

	_, valMapping, err = schema.MapSchemaBasedOnTagAndName(targetSch, existingSch)
	if err != nil {
		return false, err
	}

	for i, j := range valMapping {
		if i == j {
			continue
		}

		// If we haven't bailed so far, then these fields were added at the end.
		// If they are not-null bail.
		if !targetVD.Types[i].Nullable {
			return false, nil
		}
	}

	return true, nil
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
