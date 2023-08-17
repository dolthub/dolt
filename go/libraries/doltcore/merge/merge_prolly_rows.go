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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	errorkinds "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// ErrUnableToMergeColumnDefaultValue is returned when a column's default value cannot be Eval'ed and we are unable to
// correctly fill in a new column's value for existing table rows. This can happen when a column default value uses
// references that need to be resolved by the analyzer (e.g. column references, function references).
var ErrUnableToMergeColumnDefaultValue = errorkinds.NewKind("unable to automatically apply column default value " +
	"in merge: %s for table '%s'; to continue merging, first manually apply the column alteration on this branch")

// mergeProllyTable merges the table specified by |tm| using the specified |mergedSch| and returns the new table
// instance, along with merge stats and any error. This function will merge the table artifacts (e.g. recorded
// conflicts), migrate any existing table data to the specified |mergedSch|, and merge table data from both sides
// of the merge together.
func mergeProllyTable(ctx context.Context, tm *TableMerger, mergedSch schema.Schema) (*doltdb.Table, *MergeStats, error) {
	mergeTbl, err := mergeTableArtifacts(ctx, tm, tm.leftTbl)
	if err != nil {
		return nil, nil, err
	}
	tm.leftTbl = mergeTbl

	// Before we merge the table data we need to fix up the primary index on the left-side of the merge for
	// any ordinal mapping changes (i.e. moving/dropping/adding columns).
	// NOTE: This won't ALWAYS be the left side... eventually we will need to optimize which side we pick
	//       (i.e. the side that needs the least work to modify) and make this logic work for either side.
	lr, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	leftRows := durable.ProllyMapFromIndex(lr)
	valueMerger := newValueMerger(mergedSch, tm.leftSch, tm.rightSch, tm.ancSch, leftRows.Pool())
	leftMapping := valueMerger.leftMapping

	// We need a sql.Context to apply column default values in merges; if we don't have one already,
	// create one, since this code also gets called from the CLI merge code path.
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		sqlCtx = sql.NewContext(ctx)
	}

	// Migrate primary index data to rewrite the values on the left side of the merge if necessary
	schemasDifferentSize := len(tm.leftSch.GetAllCols().GetColumns()) != len(mergedSch.GetAllCols().GetColumns())
	if schemasDifferentSize || leftMapping.IsIdentityMapping() == false {
		if err := migrateDataToMergedSchema(sqlCtx, tm, valueMerger, mergedSch); err != nil {
			return nil, nil, err
		}

		// After we migrate the data on the left-side to the new, merged schema, we reset
		// the left mapping to an identity mapping, since it's a direct mapping now.
		valueMerger.leftMapping = val.NewIdentityOrdinalMapping(len(valueMerger.leftMapping))
	}

	// After we've migrated the existing data to the new schema, it's safe for us to update the schema on the table
	mergeTbl, err = tm.leftTbl.UpdateSchema(sqlCtx, mergedSch)
	if err != nil {
		return nil, nil, err
	}

	var stats *MergeStats
	mergeTbl, stats, err = mergeProllyTableData(sqlCtx, tm, mergedSch, mergeTbl, valueMerger)
	if err != nil {
		return nil, nil, err
	}

	n, err := mergeTbl.NumRowsInConflict(sqlCtx)
	if err != nil {
		return nil, nil, err
	}
	stats.DataConflicts = int(n)

	mergeTbl, err = mergeAutoIncrementValues(sqlCtx, tm.leftTbl, tm.rightTbl, mergeTbl)
	if err != nil {
		return nil, nil, err
	}
	return mergeTbl, stats, nil
}

// mergeProllyTableData three-way merges the data for a given table. We currently take the left
// side of the merge and use that data as the starting point to merge in changes from the right
// side. Eventually, we will need to optimize this to pick the side that needs the least work.
// We iterate over the calculated diffs using a ThreeWayDiffer instance, and for every change
// to the right-side, we apply it to the left-side by merging it into the left-side's primary index
// as well as any secondary indexes, and also checking for unique constraints incrementally. When
// conflicts are detected, this function attempts to resolve them automatically if possible, and
// if not, they are recorded as conflicts in the table's artifacts.
func mergeProllyTableData(ctx *sql.Context, tm *TableMerger, finalSch schema.Schema, mergeTbl *doltdb.Table, valueMerger *valueMerger) (*doltdb.Table, *MergeStats, error) {
	iter, err := threeWayDiffer(ctx, tm, valueMerger)
	if err != nil {
		return nil, nil, err
	}

	lr, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, err
	}
	leftEditor := durable.ProllyMapFromIndex(lr).Rewriter(finalSch.GetKeyDescriptor(), finalSch.GetValueDescriptor())

	ai, err := mergeTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, nil, err
	}
	artEditor := durable.ProllyMapFromArtifactIndex(ai).Editor()

	keyless := schema.IsKeyless(tm.leftSch)

	pri, err := newPrimaryMerger(leftEditor, tm, valueMerger, finalSch)
	if err != nil {
		return nil, nil, err
	}
	sec, err := newSecondaryMerger(ctx, tm, valueMerger, finalSch)
	if err != nil {
		return nil, nil, err
	}
	conflicts, err := newConflictMerger(ctx, tm, artEditor)
	if err != nil {
		return nil, nil, err
	}

	checkValidator, err := newCheckValidator(ctx, tm, valueMerger, finalSch, artEditor)
	if err != nil {
		return nil, nil, err
	}

	// validator shares an artifact editor with conflict merge
	uniq, err := newUniqValidator(ctx, finalSch, tm, valueMerger, artEditor)
	if err != nil {
		return nil, nil, err
	}

	nullChk, err := newNullValidator(ctx, finalSch, tm, valueMerger, artEditor, leftEditor, sec.leftMut)
	if err != nil {
		return nil, nil, err
	}

	s := &MergeStats{
		Operation: TableModified,
	}
	for {
		diff, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, nil, err
		}
		cnt, err := uniq.validateDiff(ctx, diff)
		if err != nil {
			return nil, nil, err
		}
		s.ConstraintViolations += cnt

		cnt, err = nullChk.validateDiff(ctx, diff)
		if err != nil {
			return nil, nil, err
		}
		s.ConstraintViolations += cnt
		if cnt > 0 {
			continue
		}

		cnt, err = checkValidator.validateDiff(ctx, diff)
		if err != nil {
			return nil, nil, err
		}
		s.ConstraintViolations += cnt

		switch diff.Op {
		case tree.DiffOpDivergentModifyConflict, tree.DiffOpDivergentDeleteConflict:
			// In this case, a modification or delete was made to one side, and a conflicting delete or modification
			// was made to the other side, so these cannot be automatically resolved.
			s.DataConflicts++
			err = conflicts.merge(ctx, diff, nil)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpRightAdd:
			s.Adds++
			err = pri.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpRightModify:
			s.Modifications++
			err = pri.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpRightDelete:
			s.Deletes++
			err = pri.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpDivergentModifyResolved:
			// In this case, both sides of the merge have made different changes to a row, but we were able to
			// resolve them automatically.
			s.Modifications++
			err = pri.merge(ctx, diff, nil)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff, nil)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpConvergentAdd, tree.DiffOpConvergentModify, tree.DiffOpConvergentDelete:
			// In this case, both sides of the merge have made the same change, so no additional changes are needed.
			if keyless {
				s.DataConflicts++
				err = conflicts.merge(ctx, diff, nil)
				if err != nil {
					return nil, nil, err
				}
			}
		default:
			// Currently, all changes are applied to the left-side of the merge, so for any left-side diff ops,
			// we can simply ignore them since that data is already in the destination (the left-side).
		}
	}

	finalRows, err := pri.finalize(ctx)
	if err != nil {
		return nil, nil, err
	}

	leftIdxs, rightIdxs, err := sec.finalize(ctx)
	if err != nil {
		return nil, nil, err
	}

	finalIdxs, err := mergeProllySecondaryIndexes(ctx, tm, leftIdxs, rightIdxs, finalSch, finalRows, conflicts.ae)
	if err != nil {
		return nil, nil, err
	}

	finalArtifacts, err := conflicts.finalize(ctx)

	// collect merged data in |finalTbl|
	finalTbl, err := mergeTbl.UpdateRows(ctx, finalRows)
	if err != nil {
		return nil, nil, err
	}

	finalTbl, err = finalTbl.SetIndexSet(ctx, finalIdxs)
	if err != nil {
		return nil, nil, err
	}

	finalTbl, err = finalTbl.SetArtifacts(ctx, finalArtifacts)
	if err != nil {
		return nil, nil, err
	}

	return finalTbl, s, nil
}

func threeWayDiffer(ctx context.Context, tm *TableMerger, valueMerger *valueMerger) (*tree.ThreeWayDiffer[val.Tuple, val.TupleDesc], error) {
	lr, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	leftRows := durable.ProllyMapFromIndex(lr)

	rr, err := tm.rightTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	rightRows := durable.ProllyMapFromIndex(rr)

	ar, err := tm.ancTbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	ancRows := durable.ProllyMapFromIndex(ar)

	return tree.NewThreeWayDiffer(ctx, leftRows.NodeStore(), leftRows.Tuples(), rightRows.Tuples(), ancRows.Tuples(), valueMerger.tryMerge, valueMerger.keyless, leftRows.Tuples().Order)
}

// checkValidator is responsible for inspecting three-way diff events, running any check constraint expressions
// that need to be reevaluated, and reporting any check constraint violations.
type checkValidator struct {
	checkExpressions map[string]sql.Expression
	valueMerger      *valueMerger
	tableMerger      *TableMerger
	sch              schema.Schema
	edits            *prolly.ArtifactsEditor
	srcHash          hash.Hash
}

// newCheckValidator creates a new checkValidator, ready to validate diff events. |tm| provides the overall information
// about the table being merged, |vm| provides the details on how the value tuples are being merged between the ancestor,
// right and left sides of the merge, |sch| provides the final schema of the merge, and |edits| is used to write
// constraint validation artifacts.
func newCheckValidator(ctx *sql.Context, tm *TableMerger, vm *valueMerger, sch schema.Schema, edits *prolly.ArtifactsEditor) (checkValidator, error) {
	checkExpressions := make(map[string]sql.Expression)

	checks := sch.Checks()
	for _, check := range checks.AllChecks() {
		if !check.Enforced() {
			continue
		}

		expr, err := resolveExpression(ctx, check.Expression(), sch, tm.name)
		if err != nil {
			return checkValidator{}, err
		}
		checkExpressions[check.Name()] = expr
	}

	srcHash, err := tm.rightSrc.HashOf()
	if err != nil {
		return checkValidator{}, err
	}

	return checkValidator{
		checkExpressions: checkExpressions,
		valueMerger:      vm,
		tableMerger:      tm,
		sch:              sch,
		edits:            edits,
		srcHash:          srcHash,
	}, nil
}

// validateDiff inspects the three-way diff event |diff| and evaluates any check constraint expressions that need to
// be rechecked after the merge. If any check constraint violations are detected, the violation count is returned as
// the first return parameter and the violations are also written to the artifact editor passed in on creation.
func (cv checkValidator) validateDiff(ctx *sql.Context, diff tree.ThreeWayDiff) (int, error) {
	conflictCount := 0

	var valueTuple val.Tuple
	var valueDesc val.TupleDesc
	switch diff.Op {
	case tree.DiffOpLeftDelete, tree.DiffOpRightDelete, tree.DiffOpConvergentDelete:
		// no need to validate check constraints for deletes
		return 0, nil
	case tree.DiffOpDivergentDeleteConflict, tree.DiffOpDivergentModifyConflict:
		// Don't bother validating divergent conflicts, just let them get reported as conflicts
		return 0, nil
	case tree.DiffOpLeftAdd, tree.DiffOpLeftModify:
		valueTuple = diff.Left
		valueDesc = cv.tableMerger.leftSch.GetValueDescriptor()
	case tree.DiffOpRightAdd, tree.DiffOpRightModify:
		valueTuple = diff.Right
		valueDesc = cv.tableMerger.rightSch.GetValueDescriptor()
	case tree.DiffOpConvergentAdd, tree.DiffOpConvergentModify:
		// both sides made the same change, just take the left
		valueTuple = diff.Left
		valueDesc = cv.tableMerger.leftSch.GetValueDescriptor()
	case tree.DiffOpDivergentModifyResolved:
		valueTuple = diff.Merged
		valueDesc = cv.tableMerger.leftSch.GetValueDescriptor()
	}

	for checkName, checkExpression := range cv.checkExpressions {
		// If the row came from the right side of the merge, then remap it (if necessary) to the final schema.
		// This isn't necessary for left-side changes, because we already migrated the primary index data to
		// the merged schema, and we skip keyless tables, since their value tuples require different mapping
		// logic and we don't currently support merges to keyless tables that contain schema changes anyway.
		newTuple := valueTuple
		if !cv.valueMerger.keyless && (diff.Op == tree.DiffOpRightAdd || diff.Op == tree.DiffOpRightModify) {
			newTupleBytes := remapTuple(valueTuple, valueDesc, cv.valueMerger.rightMapping)
			newTuple = val.NewTuple(cv.valueMerger.syncPool, newTupleBytes...)
		}

		row, err := buildRow(ctx, diff.Key, newTuple, cv.sch, cv.tableMerger)
		if err != nil {
			return 0, err
		}

		result, err := checkExpression.Eval(ctx, row)
		if err != nil {
			return 0, err
		}

		// MySQL treats NULL as TRUE for a check constraint
		if result == nil {
			result = true
		}

		// Coerce into a boolean; technically, this shouldn't be
		// necessary, since check constraint expressions should always
		// be of a boolean type, but Dolt has allowed this previously.
		// https://github.com/dolthub/dolt/issues/6411
		booleanResult, err := types.ConvertToBool(result)
		if err != nil {
			return 0, fmt.Errorf("unable to convert check constraint expression (%s) into boolean value: %v", checkName, err.Error())
		}

		if booleanResult {
			// If a check constraint returns TRUE (or NULL), then the check constraint is fulfilled
			// https://dev.mysql.com/doc/refman/8.0/en/create-table-check-constraints.html
			continue
		} else {
			conflictCount++
			meta, err := newCheckCVMeta(cv.sch, checkName)
			if err != nil {
				return 0, err
			}
			if err = cv.insertArtifact(ctx, diff.Key, newTuple, meta); err != nil {
				return conflictCount, err
			}
		}
	}

	return conflictCount, nil
}

// insertArtifact records a check constraint violation, as described by |meta|, for the row with the specified
// |key| and |value|.
func (cv checkValidator) insertArtifact(ctx context.Context, key, value val.Tuple, meta CheckCVMeta) error {
	vinfo, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	cvm := prolly.ConstraintViolationMeta{VInfo: vinfo, Value: value}
	return cv.edits.ReplaceConstraintViolation(ctx, key, cv.srcHash, prolly.ArtifactTypeChkConsViol, cvm)
}

// buildRow takes the |key| and |value| tuple and returns a new sql.Row, along with any errors encountered.
func buildRow(ctx *sql.Context, key, value val.Tuple, sch schema.Schema, tableMerger *TableMerger) (sql.Row, error) {
	pkCols := sch.GetPKCols()
	valueCols := sch.GetNonPKCols()
	allCols := sch.GetAllCols()

	// When we parse and resolve the check constraint expression with planbuilder, it leaves row position 0
	// for the expression itself, so we add an empty spot in index 0 of our row to account for that to make sure
	// the GetField expressions' indexes match up to the right columns.
	row := make(sql.Row, allCols.Size()+1)

	// Skip adding the key tuple if we're working with a keyless table, since the table row data is
	// always all contained in the value tuple for keyless tables.
	if !schema.IsKeyless(sch) {
		keyDesc := sch.GetKeyDescriptor()
		for i := range keyDesc.Types {
			value, err := index.GetField(ctx, keyDesc, i, key, tableMerger.ns)
			if err != nil {
				return nil, err
			}

			pkCol := pkCols.GetColumns()[i]
			row[allCols.TagToIdx[pkCol.Tag]+1] = value
		}
	}

	valueColIndex := 0
	valueDescriptor := sch.GetValueDescriptor()
	for valueTupleIndex := range valueDescriptor.Types {
		// Skip processing the first value in the value tuple for keyless tables, since that field
		// always holds the cardinality of the row and shouldn't be passed in to an expression.
		if schema.IsKeyless(sch) && valueTupleIndex == 0 {
			continue
		}

		value, err := index.GetField(ctx, valueDescriptor, valueTupleIndex, value, tableMerger.ns)
		if err != nil {
			return nil, err
		}

		col := valueCols.GetColumns()[valueColIndex]
		row[allCols.TagToIdx[col.Tag]+1] = value
		valueColIndex += 1
	}

	return row, nil
}

// uniqValidator checks whether new additions from the merge-right
// duplicate secondary index entries.
type uniqValidator struct {
	src         doltdb.Rootish
	srcHash     hash.Hash
	edits       *prolly.ArtifactsEditor
	indexes     []uniqIndex
	valueMerger *valueMerger
	tm          *TableMerger
}

func newUniqValidator(ctx context.Context, sch schema.Schema, tm *TableMerger, vm *valueMerger, edits *prolly.ArtifactsEditor) (uniqValidator, error) {
	srcHash, err := tm.rightSrc.HashOf()
	if err != nil {
		return uniqValidator{}, err
	}

	uv := uniqValidator{
		src:         tm.rightSrc,
		srcHash:     srcHash,
		edits:       edits,
		valueMerger: vm,
		tm:          tm,
	}

	rows, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return uniqValidator{}, err
	}
	clustered := durable.ProllyMapFromIndex(rows)

	indexes, err := tm.leftTbl.GetIndexSet(ctx)
	if err != nil {
		return uniqValidator{}, err
	}

	for _, def := range sch.Indexes().AllIndexes() {
		if !def.IsUnique() {
			continue
		} else if !tm.leftSch.Indexes().Contains(def.Name()) {
			continue // todo: how do we validate in this case?
		}

		idx, err := indexes.GetIndex(ctx, sch, def.Name())
		if err != nil {
			return uniqValidator{}, err
		}
		secondary := durable.ProllyMapFromIndex(idx)

		u, err := newUniqIndex(sch, def, clustered, secondary)
		if err != nil {
			return uniqValidator{}, err
		}
		uv.indexes = append(uv.indexes, u)
	}
	return uv, nil
}

// validateDiff processes |diff| and checks for any unique constraint violations that need to be updated. The number
// of violations recorded along with any error encountered is returned. Processing |diff| may resolve existing unique
// constraint violations, in which case the violations returned may be a negative number.
func (uv uniqValidator) validateDiff(ctx context.Context, diff tree.ThreeWayDiff) (violations int, err error) {
	var value val.Tuple
	switch diff.Op {
	case tree.DiffOpRightAdd, tree.DiffOpRightModify:
		value = diff.Right
	case tree.DiffOpRightDelete:
		// If we see a row deletion event from the right side, we grab the original/base value so that we can update our
		// local copy of the secondary index.
		value = diff.Base
	case tree.DiffOpDivergentModifyResolved:
		value = diff.Merged
	default:
		return
	}

	// Don't remap the value to the merged schema if the table is keyless or if the mapping is an identity mapping.
	if !uv.valueMerger.keyless && !uv.valueMerger.rightMapping.IsIdentityMapping() {
		modifiedValue := remapTuple(value, uv.tm.rightSch.GetValueDescriptor(), uv.valueMerger.rightMapping)
		value = val.NewTuple(uv.valueMerger.syncPool, modifiedValue...)
	}

	// For a row deletion... we need to remove any unique constraint violations that were previously recorded for
	// this row.
	if diff.Op == tree.DiffOpRightDelete {
		// First update the unique indexes to remove this row.
		for _, idx := range uv.indexes {
			err := idx.removeRow(ctx, diff.Key, value)
			if err != nil {
				return violations, err
			}
		}

		// Then clear any unique constraint violation artifacts for this row. If there is only one unique constraint
		// violation artifact left, it will also be cleared by this function (since unique constraint violations
		// must always occur with at least two rows reported).
		return uv.clearArtifact(ctx, diff.Key, diff.Base)
	}

	for _, idx := range uv.indexes {
		err = idx.findCollisions(ctx, diff.Key, value, func(k, v val.Tuple) error {
			violations++
			return uv.insertArtifact(ctx, k, v, idx.meta)
		})
		if err != nil {
			break
		}
	}

	// After detecting any unique constraint violations, we need to update our indexes with the updated row
	if diff.Op == tree.DiffOpRightAdd || diff.Op == tree.DiffOpRightModify || diff.Op == tree.DiffOpDivergentModifyResolved {
		for _, idx := range uv.indexes {
			err := idx.insertRow(ctx, diff.Key, value)
			if err != nil {
				return violations, err
			}

			err = idx.clustered.Put(ctx, diff.Key, value)
			if err != nil {
				return violations, err
			}
		}
	}

	return violations, err
}

// deleteArtifact deletes the unique constraint violation artifact for the row identified by |key| and returns a
// boolean that indicates if an artifact was deleted, as well as an error that indicates if there were any
// unexpected errors encountered.
func (uv uniqValidator) deleteArtifact(ctx context.Context, key val.Tuple) (bool, error) {
	artifactKey := uv.edits.BuildArtifactKey(ctx, key, uv.srcHash, prolly.ArtifactTypeUniqueKeyViol)

	has, err := uv.edits.Has(ctx, artifactKey)
	if err != nil || !has {
		return false, err
	}

	err = uv.edits.Delete(ctx, artifactKey)
	if err != nil {
		return false, err
	}

	return true, nil
}

// clearArtifactsForValue deletes the unique constraint violation artifact for the row identified by |key| and |value|
// and then checks to see if only one unique constraint violation artifact remains, and if so, deletes it as well,
// since only a single row remaining for a unique constraint violation means that the violation has been fully
// resolved and no other rows conflict with that unique value.
func (uv uniqValidator) clearArtifact(ctx context.Context, key val.Tuple, prevValue val.Tuple) (int, error) {
	deleted, err := uv.deleteArtifact(ctx, key)
	if err != nil || !deleted {
		return 0, err
	}

	// Start the violation count at -1 to represent the artifact above that we just removed
	violationCount := -1

	for _, idx := range uv.indexes {
		// TODO: Test with multiple unique indexes and constraint violations on different values
		//       Multiple unique indexes won't work yet: https://github.com/dolthub/dolt/issues/6329
		err := idx.findCollisions(ctx, key, prevValue, func(k, v val.Tuple) error {
			deleted, err := uv.deleteArtifact(ctx, k)
			if err != nil || !deleted {
				return err
			}
			violationCount = violationCount - 1
			return nil
		})
		if err != nil {
			break
		}
	}

	return violationCount, nil
}

func (uv uniqValidator) insertArtifact(ctx context.Context, key, value val.Tuple, meta UniqCVMeta) error {
	vinfo, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	cvm := prolly.ConstraintViolationMeta{VInfo: vinfo, Value: value}
	return uv.edits.ReplaceConstraintViolation(ctx, key, uv.srcHash, prolly.ArtifactTypeUniqueKeyViol, cvm)
}

type uniqIndex struct {
	def              schema.Index
	secondary        *prolly.MutableMap
	clustered        *prolly.MutableMap
	meta             UniqCVMeta
	prefixDesc       val.TupleDesc
	secondaryBld     index.SecondaryKeyBuilder
	clusteredBld     index.ClusteredKeyBuilder
	clusteredKeyDesc val.TupleDesc
}

func newUniqIndex(sch schema.Schema, def schema.Index, clustered, secondary prolly.Map) (uniqIndex, error) {
	meta, err := makeUniqViolMeta(sch, def)
	if err != nil {
		return uniqIndex{}, err
	}

	if schema.IsKeyless(sch) { // todo(andy): sad panda
		secondary = prolly.ConvertToSecondaryKeylessIndex(secondary)
	}
	p := clustered.Pool()

	prefixDesc := secondary.KeyDesc().PrefixDesc(def.Count())
	secondaryBld := index.NewSecondaryKeyBuilder(sch, def, secondary.KeyDesc(), p)
	clusteredBld := index.NewClusteredKeyBuilder(def, sch, clustered.KeyDesc(), p)

	return uniqIndex{
		def:              def,
		secondary:        secondary.Mutate(),
		clustered:        clustered.Mutate(),
		clusteredKeyDesc: clustered.KeyDesc(),
		meta:             meta,
		prefixDesc:       prefixDesc,
		secondaryBld:     secondaryBld,
		clusteredBld:     clusteredBld,
	}, nil
}

type collisionFn func(key, value val.Tuple) error

func (idx uniqIndex) insertRow(ctx context.Context, key, value val.Tuple) error {
	secondaryIndexKey := idx.secondaryBld.SecondaryKeyFromRow(key, value)
	newValue := val.NewTuple(idx.secondary.NodeStore().Pool(), nil)
	return idx.secondary.Put(ctx, secondaryIndexKey, newValue)
}

func (idx uniqIndex) removeRow(ctx context.Context, key, value val.Tuple) error {
	secondaryIndexKey := idx.secondaryBld.SecondaryKeyFromRow(key, value)
	err := idx.secondary.Delete(ctx, secondaryIndexKey)
	if err != nil {
		return err
	}

	clusteredIndexKey := idx.clusteredBld.ClusteredKeyFromIndexKey(secondaryIndexKey)
	return idx.clustered.Delete(ctx, clusteredIndexKey)
}

// findCollisions searches this unique index to find any rows that have the same values as |value| for the columns
// included in the unique constraint. For any matching row, the specified callback, |cb|, is invoked with the key
// and value for the primary index, representing the conflicting row identified from the unique index.
func (idx uniqIndex) findCollisions(ctx context.Context, key, value val.Tuple, cb collisionFn) error {
	indexKey := idx.secondaryBld.SecondaryKeyFromRow(key, value)
	if idx.prefixDesc.HasNulls(indexKey) {
		return nil // NULLs cannot cause unique violations
	}

	// This code uses the secondary index to iterate over all rows (key/value pairs) that have the same prefix.
	// The prefix here is all the value columns this index is set up to track
	collisions := make([]val.Tuple, 0)
	err := idx.secondary.GetPrefix(ctx, indexKey, idx.prefixDesc, func(k, _ val.Tuple) (err error) {
		if k != nil {
			collisions = append(collisions, k)
		}
		return
	})
	if err != nil || len(collisions) == 0 {
		return err
	}

	collisionDetected := false
	for _, collision := range collisions {
		// Next find the key in the primary (aka clustered) index
		clusteredKey := idx.clusteredBld.ClusteredKeyFromIndexKey(collision)
		if bytes.Equal(key, clusteredKey) {
			continue // collided with ourselves
		}

		// |prefix| was non-unique, find the clustered index row that
		// collided with row(|key|, |value|) and pass both to |cb|
		err = idx.clustered.Get(ctx, clusteredKey, func(k val.Tuple, v val.Tuple) error {
			if k == nil {
				s := idx.clusteredKeyDesc.Format(clusteredKey)
				return errors.New("failed to find key: " + s)
			}
			collisionDetected = true
			return cb(k, v)
		})
		if err != nil {
			return err
		}
	}
	if collisionDetected {
		return cb(key, value)
	} else {
		return nil
	}
}

// nullValidator enforces NOT NULL constraints on merge
type nullValidator struct {
	table string
	// final is the merge result schema
	final schema.Schema
	// leftMap and rightMap map value tuples to |final|
	leftMap, rightMap val.OrdinalMapping
	// edits is the artifacts maps editor
	artEditor *prolly.ArtifactsEditor
	// leftEdits if the left-side row editor
	leftEditor *prolly.MutableMap
	// secEditors are the secondary index editors
	secEditors []MutableSecondaryIdx
	// theirRootish is the hash.Hash of the right-side revision
	theirRootish hash.Hash
	// ourRootish is the hash.Hash of the left-side revision
	ourRootish hash.Hash
}

func newNullValidator(
	ctx context.Context,
	final schema.Schema,
	tm *TableMerger,
	vm *valueMerger,
	artEditor *prolly.ArtifactsEditor,
	leftEditor *prolly.MutableMap,
	secEditors []MutableSecondaryIdx,
) (nullValidator, error) {
	theirRootish, err := tm.rightSrc.HashOf()
	if err != nil {
		return nullValidator{}, err
	}
	ourRootish, err := tm.rightSrc.HashOf()
	if err != nil {
		return nullValidator{}, err
	}
	return nullValidator{
		table:        tm.name,
		final:        final,
		leftMap:      vm.leftMapping,
		rightMap:     vm.rightMapping,
		artEditor:    artEditor,
		leftEditor:   leftEditor,
		secEditors:   secEditors,
		theirRootish: theirRootish,
		ourRootish:   ourRootish,
	}, nil
}

func (nv nullValidator) validateDiff(ctx context.Context, diff tree.ThreeWayDiff) (count int, err error) {
	switch diff.Op {
	case tree.DiffOpRightAdd, tree.DiffOpRightModify:
		var violations []string
		for to, from := range nv.rightMap {
			col := nv.final.GetNonPKCols().GetByIndex(to)
			if col.IsNullable() {
				continue
			}
			if from < 0 {
				// non-nullable column in |nv.final| does not exist
				// on the right side of the merge, check if it will
				// be populated with a default value
				if col.Default == "" {
					violations = append(violations, col.Name)
				}
			} else {
				if diff.Right.FieldIsNull(from) {
					violations = append(violations, col.Name)
				}
			}
		}
		// for right-side NULL violations, we insert a constraint violation and
		// set |count| > 0 to signal to the caller that |diff| should not be applied
		if len(violations) > 0 {
			var meta prolly.ConstraintViolationMeta
			if meta, err = newNotNullViolationMeta(violations, diff.Right); err != nil {
				return 0, err
			}
			err = nv.artEditor.ReplaceConstraintViolation(ctx, diff.Key, nv.theirRootish, prolly.ArtifactTypeNullViol, meta)
			if err != nil {
				return 0, err
			}
		}
		count = len(violations)

	case tree.DiffOpLeftAdd, tree.DiffOpLeftModify:
		var violations []string
		for to, from := range nv.leftMap {
			col := nv.final.GetNonPKCols().GetByIndex(to)
			if col.IsNullable() {
				continue
			}
			if from < 0 {
				// non-nullable column in |nv.final| does not exist
				// on the left side of the merge, check if it will
				// be populated with a default value
				if col.Default == "" {
					violations = append(violations, col.Name)
				}
			} else {
				if diff.Left.FieldIsNull(from) {
					violations = append(violations, col.Name)
				}
			}
		}
		// for left-side NULL violations, we insert a constraint violation and
		// then must explicitly remove this row from all left-side indexes
		if len(violations) > 0 {
			var meta prolly.ConstraintViolationMeta
			if meta, err = newNotNullViolationMeta(violations, diff.Left); err != nil {
				return 0, err
			}
			err = nv.artEditor.ReplaceConstraintViolation(ctx, diff.Key, nv.ourRootish, prolly.ArtifactTypeNullViol, meta)
			if err != nil {
				return 0, err
			}
			if err = nv.leftEditor.Delete(ctx, diff.Key); err != nil {
				return 0, err
			}
			for _, editor := range nv.secEditors {
				if err = editor.DeleteEntry(ctx, diff.Key, diff.Left); err != nil {
					return 0, err
				}
			}
		}
	}
	return
}

// conflictMerger processing primary key diffs
// with conflict types into artifact table writes.
type conflictMerger struct {
	ae           *prolly.ArtifactsEditor
	rightRootish hash.Hash
	meta         []byte
}

func newConflictMerger(ctx context.Context, tm *TableMerger, ae *prolly.ArtifactsEditor) (*conflictMerger, error) {
	has, err := tm.leftTbl.HasConflicts(ctx)
	if err != nil {
		return nil, err
	}
	if has {
		a, l, r, err := tm.leftTbl.GetConflictSchemas(ctx, tm.name)
		if err != nil {
			return nil, err
		}

		equal := schema.ColCollsAreEqual(a.GetAllCols(), tm.ancSch.GetAllCols()) &&
			schema.ColCollsAreEqual(l.GetAllCols(), tm.leftSch.GetAllCols()) &&
			schema.ColCollsAreEqual(r.GetAllCols(), tm.rightSch.GetAllCols())
		if !equal {
			return nil, ErrConflictsIncompatible
		}
	}

	rightHash, err := tm.rightSrc.HashOf()
	if err != nil {
		return nil, err
	}

	baseHash, err := tm.ancestorSrc.HashOf()
	if err != nil {
		return nil, err
	}

	m := prolly.ConflictMetadata{
		BaseRootIsh: baseHash,
	}
	meta, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	return &conflictMerger{
		meta:         meta,
		rightRootish: rightHash,
		ae:           ae,
	}, nil
}

func (m *conflictMerger) merge(ctx context.Context, diff tree.ThreeWayDiff, _ schema.Schema) error {
	switch diff.Op {
	case tree.DiffOpDivergentModifyConflict, tree.DiffOpDivergentDeleteConflict,
		tree.DiffOpConvergentAdd, tree.DiffOpConvergentModify, tree.DiffOpConvergentDelete:
	default:
		return fmt.Errorf("invalid conflict type: %s", diff.Op)
	}
	return m.ae.Add(ctx, diff.Key, m.rightRootish, prolly.ArtifactTypeConflict, m.meta)
}

func (m *conflictMerger) finalize(ctx context.Context) (durable.ArtifactIndex, error) {
	am, err := m.ae.Flush(ctx)
	if err != nil {
		return nil, err
	}
	return durable.ArtifactIndexFromProllyMap(am), nil
}

// primaryMerger translates three-way diffs
// on the primary index into merge-left updates.
type primaryMerger struct {
	mut         *prolly.MutableMap
	valueMerger *valueMerger
	tableMerger *TableMerger
	finalSch    schema.Schema
}

func newPrimaryMerger(leftEditor *prolly.MutableMap, tableMerger *TableMerger, valueMerger *valueMerger, finalSch schema.Schema) (*primaryMerger, error) {
	return &primaryMerger{
		mut:         leftEditor,
		valueMerger: valueMerger,
		tableMerger: tableMerger,
		finalSch:    finalSch,
	}, nil
}

// merge applies the specified |diff| to the primary index of this primaryMerger. The given |sourceSch|
// specifies the schema of the source of the diff, which is used to map the diff to the post-merge
// schema. |sourceSch| may be nil when no mapping from the source schema is needed (i.e. DiffOpRightDelete,
// and DiffOpDivergentModifyResolved).
func (m *primaryMerger) merge(ctx *sql.Context, diff tree.ThreeWayDiff, sourceSch schema.Schema) error {
	switch diff.Op {
	case tree.DiffOpRightAdd, tree.DiffOpRightModify:
		if sourceSch == nil {
			return fmt.Errorf("no source schema specified to map right-side changes to merged schema")
		}

		newTupleValue := diff.Right
		if schema.IsKeyless(sourceSch) {
			if m.valueMerger.rightMapping.IsIdentityMapping() == false {
				return fmt.Errorf("cannot merge keyless tables with reordered columns")
			}
		} else {
			tempTupleValue, err := remapTupleWithColumnDefaults(ctx, diff.Key, diff.Right, sourceSch.GetValueDescriptor(),
				m.valueMerger.rightMapping, m.tableMerger, m.finalSch, m.valueMerger.syncPool)
			if err != nil {
				return err
			}
			newTupleValue = tempTupleValue
		}
		return m.mut.Put(ctx, diff.Key, newTupleValue)
	case tree.DiffOpRightDelete:
		return m.mut.Put(ctx, diff.Key, diff.Right)
	case tree.DiffOpDivergentModifyResolved:
		return m.mut.Put(ctx, diff.Key, diff.Merged)
	default:
		return fmt.Errorf("unexpected diffOp for editing primary index: %s", diff.Op)
	}
}

func (m *primaryMerger) finalize(ctx context.Context) (durable.Index, error) {
	mergedMap, err := m.mut.Map(ctx)
	if err != nil {
		return nil, err
	}
	return durable.IndexFromProllyMap(mergedMap), nil
}

// secondaryMerger translates diffs on the primary index
// into secondary index updates.
type secondaryMerger struct {
	leftSet      durable.IndexSet
	rightSet     durable.IndexSet
	leftMut      []MutableSecondaryIdx
	valueMerger  *valueMerger
	mergedSchema schema.Schema
}

const secondaryMergerPendingSize = 650_000

func newSecondaryMerger(ctx context.Context, tm *TableMerger, valueMerger *valueMerger, mergedSchema schema.Schema) (*secondaryMerger, error) {
	ls, err := tm.leftTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	// Use the mergedSchema to work with the secondary indexes, to pull out row data using the right
	// pri_index -> sec_index mapping.
	lm, err := GetMutableSecondaryIdxsWithPending(ctx, mergedSchema, ls, secondaryMergerPendingSize)
	if err != nil {
		return nil, err
	}

	rs, err := tm.rightTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}

	return &secondaryMerger{
		leftSet:      ls,
		rightSet:     rs,
		leftMut:      lm,
		valueMerger:  valueMerger,
		mergedSchema: mergedSchema,
	}, nil
}

func (m *secondaryMerger) merge(ctx context.Context, diff tree.ThreeWayDiff, sourceSch schema.Schema) error {
	var err error
	for _, idx := range m.leftMut {
		switch diff.Op {
		case tree.DiffOpDivergentModifyResolved:
			err = applyEdit(ctx, idx, diff.Key, diff.Left, diff.Merged)
		case tree.DiffOpRightAdd, tree.DiffOpRightModify:
			// Just as with the primary index, we need to map right-side changes to the final, merged schema.
			if sourceSch == nil {
				return fmt.Errorf("no source schema specified to map right-side changes to merged schema")
			}

			newTupleValue := diff.Right
			if schema.IsKeyless(sourceSch) {
				if m.valueMerger.rightMapping.IsIdentityMapping() == false {
					return fmt.Errorf("cannot merge keyless tables with reordered columns")
				}
			} else {
				valueMappedToMergeSchema := remapTuple(diff.Right, sourceSch.GetValueDescriptor(), m.valueMerger.rightMapping)
				newTupleValue = val.NewTuple(m.valueMerger.syncPool, valueMappedToMergeSchema...)
			}

			err = applyEdit(ctx, idx, diff.Key, diff.Base, newTupleValue)
		case tree.DiffOpRightDelete:
			err = applyEdit(ctx, idx, diff.Key, diff.Base, diff.Right)
		default:
			// Any changes to the left-side of the merge are not needed, since we currently
			// always default to using the left side of the merge as the final result, so all
			// left-side changes are already there. This won't always be the case though! We'll
			// eventually want to optimize the merge side we choose for applying changes and
			// will need to update this code.
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// finalize reifies edits into output index sets
func (m *secondaryMerger) finalize(ctx context.Context) (durable.IndexSet, durable.IndexSet, error) {
	for _, idx := range m.leftMut {
		idxMap, err := idx.Map(ctx)
		if err != nil {
			return nil, nil, err
		}
		m.leftSet, err = m.leftSet.PutIndex(ctx, idx.Name, durable.IndexFromProllyMap(idxMap))
		if err != nil {
			return nil, nil, err
		}
	}
	return m.leftSet, m.rightSet, nil
}

// resolveExpression takes in a string |expression| and does basic resolution on it (e.g. column names and function
// names) so that the returned sql.Expression can be evaluated. The schema of the table is specified in |sch| and the
// name of the table in |tableName|.
func resolveExpression(ctx *sql.Context, expression string, sch schema.Schema, tableName string) (sql.Expression, error) {
	query := fmt.Sprintf("SELECT %s from %s.%s", expression, "mydb", tableName)
	sqlSch, err := sqlutil.FromDoltSchema(tableName, sch)
	if err != nil {
		return nil, err
	}
	mockTable := memory.NewTable(tableName, sqlSch, nil)
	mockDatabase := memory.NewDatabase("mydb")
	mockDatabase.AddTable(tableName, mockTable)
	mockProvider := memory.NewDBProvider(mockDatabase)
	catalog := analyzer.NewCatalog(mockProvider)

	pseudoAnalyzedQuery, err := planbuilder.Parse(ctx, catalog, query)
	if err != nil {
		return nil, err
	}

	var expr sql.Expression
	transform.Inspect(pseudoAnalyzedQuery, func(n sql.Node) bool {
		if projector, ok := n.(sql.Projector); ok {
			expr = projector.ProjectedExprs()[0]
			return false
		}
		return true
	})
	if expr == nil {
		return nil, fmt.Errorf("unable to find expression in analyzed query")
	}

	return expr, nil
}

// remapTuple takes the given |tuple| and the |desc| that describes its data, and uses |mapping| to map the tuple's
// data into a new [][]byte, as indicated by the specified ordinal mapping.
func remapTuple(tuple val.Tuple, desc val.TupleDesc, mapping val.OrdinalMapping) [][]byte {
	result := make([][]byte, len(mapping))
	for to, from := range mapping {
		if from == -1 {
			continue
		}
		result[to] = desc.GetField(from, tuple)
	}

	return result
}

// remapTupleWithColumnDefaults takes the given |tuple| (and the |tupleDesc| that describes how to access its fields)
// and uses |mapping| to map the tuple's data and return a new tuple. |tm| provides high access to the name of the table
// currently being merged and associated node store. |mergedSch| is the new schema of the table and is used to look up
// column default values to apply to any existing rows when a new column is added as part of a merge. |pool| is used
// to allocate memory for the new tuple. A pointer to the new tuple data is returned, along with any error encountered.
func remapTupleWithColumnDefaults(ctx *sql.Context, keyTuple, valueTuple val.Tuple, tupleDesc val.TupleDesc, mapping val.OrdinalMapping, tm *TableMerger, mergedSch schema.Schema, pool pool.BuffPool) (val.Tuple, error) {
	tb := val.NewTupleBuilder(mergedSch.GetValueDescriptor())

	for to, from := range mapping {
		var value interface{}
		if from == -1 {
			// If the column is a new column, then look up any default value
			col := mergedSch.GetNonPKCols().GetByIndex(to)
			if col.Default != "" {
				// TODO: Not great to reparse the expression for every single row... need to cache this
				expression, err := resolveExpression(ctx, col.Default, mergedSch, tm.name)
				if err != nil {
					return nil, err
				}

				if !expression.Resolved() {
					return nil, ErrUnableToMergeColumnDefaultValue.New(col.Default, tm.name)
				}

				row, err := buildRow(ctx, keyTuple, valueTuple, mergedSch, tm)
				if err != nil {
					return nil, err
				}

				value, err = expression.Eval(ctx, row)
				if err != nil {
					return nil, err
				}
				value, _, err = col.TypeInfo.ToSqlType().Convert(value)
				if err != nil {
					return nil, err
				}
				err = index.PutField(ctx, tm.ns, tb, to, value)
				if err != nil {
					return nil, err
				}
			}
		} else {
			tb.PutRaw(to, tupleDesc.GetField(from, valueTuple))
		}
	}
	return tb.Build(pool), nil
}

func mergeTableArtifacts(ctx context.Context, tm *TableMerger, mergeTbl *doltdb.Table) (*doltdb.Table, error) {
	la, err := tm.leftTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	left := durable.ProllyMapFromArtifactIndex(la)

	ra, err := tm.rightTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	right := durable.ProllyMapFromArtifactIndex(ra)

	aa, err := tm.ancTbl.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	anc := durable.ProllyMapFromArtifactIndex(aa)

	var keyCollision bool
	collide := func(l, r tree.Diff) (tree.Diff, bool) {
		if l.Type == r.Type && bytes.Equal(l.To, r.To) {
			return l, true // convergent edit
		}
		keyCollision = true
		return tree.Diff{}, false
	}

	ma, err := prolly.MergeArtifactMaps(ctx, left, right, anc, collide)
	if err != nil {
		return nil, err
	}
	idx := durable.ArtifactIndexFromProllyMap(ma)

	if keyCollision {
		return nil, fmt.Errorf("encountered a key collision when merging the artifacts for table %s", tm.name)
	}

	return mergeTbl.SetArtifacts(ctx, idx)
}

// valueMerger attempts to resolve three-ways diffs on the same
// key but with conflicting values. A successful resolve produces
// a three-way cell edit (tree.DiffOpDivergentModifyResolved).
type valueMerger struct {
	numCols                                int
	vD                                     val.TupleDesc
	leftMapping, rightMapping, baseMapping val.OrdinalMapping
	syncPool                               pool.BuffPool
	keyless                                bool
}

func newValueMerger(merged, leftSch, rightSch, baseSch schema.Schema, syncPool pool.BuffPool) *valueMerger {
	leftMapping, rightMapping, baseMapping := generateSchemaMappings(merged, leftSch, rightSch, baseSch)

	return &valueMerger{
		numCols:      merged.GetNonPKCols().Size(),
		vD:           merged.GetValueDescriptor(),
		leftMapping:  leftMapping,
		rightMapping: rightMapping,
		baseMapping:  baseMapping,
		syncPool:     syncPool,
		keyless:      schema.IsKeyless(merged),
	}
}

// generateSchemaMappings returns three schema mappings: 1) mapping the |leftSch| to |mergedSch|,
// 2) mapping |rightSch| to |mergedSch|, and 3) mapping |baseSch| to |mergedSch|. Columns are
// mapped from the source schema to destination schema by finding an identical tag, or if no
// identical tag is found, then falling back to a match on column name and type.
func generateSchemaMappings(mergedSch, leftSch, rightSch, baseSch schema.Schema) (leftMapping, rightMapping, baseMapping val.OrdinalMapping) {
	n := mergedSch.GetNonPKCols().Size()
	leftMapping = make(val.OrdinalMapping, n)
	rightMapping = make(val.OrdinalMapping, n)
	baseMapping = make(val.OrdinalMapping, n)

	for i, col := range mergedSch.GetNonPKCols().GetColumns() {
		leftMapping[i] = findNonPKColumnMappingByTagOrName(leftSch, col)
		rightMapping[i] = findNonPKColumnMappingByTagOrName(rightSch, col)
		baseMapping[i] = findNonPKColumnMappingByTagOrName(baseSch, col)
	}

	return leftMapping, rightMapping, baseMapping
}

// findNonPKColumnMappingByName returns the index of the column with the given name in the given schema, or -1 if it
// doesn't exist.
func findNonPKColumnMappingByName(sch schema.Schema, name string) int {
	leftNonPKCols := sch.GetNonPKCols()
	if leftNonPKCols.Contains(name) {
		return leftNonPKCols.IndexOf(name)
	} else {
		return -1
	}
}

// findNonPKColumnMappingByTagOrName returns the index of the column with the given tag in the given schema. If a
// matching tag is not found, then this function falls back to looking for a matching column by name. If no
// matching column is found, then this function returns -1.
func findNonPKColumnMappingByTagOrName(sch schema.Schema, col schema.Column) int {
	if idx, ok := sch.GetNonPKCols().TagToIdx[col.Tag]; ok {
		return idx
	} else {
		return findNonPKColumnMappingByName(sch, col.Name)
	}
}

// migrateDataToMergedSchema migrates the data from the left side of the merge of a table to the merged schema. This
// currently only includes updating the primary index. This is necessary when a schema change is
// being applied, so that when the new schema is used to pull out data from the table, it will be in the right order.
func migrateDataToMergedSchema(ctx *sql.Context, tm *TableMerger, vm *valueMerger, mergedSch schema.Schema) error {
	lr, err := tm.leftTbl.GetRowData(ctx)
	if err != nil {
		return err
	}
	leftRows := durable.ProllyMapFromIndex(lr)
	mut := leftRows.Rewriter(mergedSch.GetKeyDescriptor(), mergedSch.GetValueDescriptor())
	mapIter, err := mut.IterAll(ctx)
	if err != nil {
		return err
	}

	leftSch, err := tm.leftTbl.GetSchema(ctx)
	if err != nil {
		return err
	}
	valueDescriptor := leftSch.GetValueDescriptor()

	for {
		keyTuple, valueTuple, err := mapIter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		newValueTuple, err := remapTupleWithColumnDefaults(ctx, keyTuple, valueTuple, valueDescriptor, vm.leftMapping, tm, mergedSch, vm.syncPool)
		if err != nil {
			return err
		}

		err = mut.Put(ctx, keyTuple, newValueTuple)
		if err != nil {
			return err
		}
	}

	m, err := mut.Map(ctx)
	if err != nil {
		return err
	}

	newIndex := durable.IndexFromProllyMap(m)
	newTable, err := tm.leftTbl.UpdateRows(ctx, newIndex)
	if err != nil {
		return err
	}
	tm.leftTbl = newTable

	// TODO: for now... we don't actually need to migrate any of the data held in secondary indexes (yet).
	//       We're currently dealing with column adds/drops/renames/reorders, but none of those directly affect
	//       secondary indexes. Columns drops *should*, but currently Dolt just drops any index referencing the
	//       dropped column, so there's nothing to do currently.
	//       https://github.com/dolthub/dolt/issues/5641
	//       Once we start handling type changes changes or primary key changes, or fix the bug above,
	//       then we will need to start migrating secondary index data, too.

	return nil
}

// tryMerge performs a cell-wise merge given left, right, and base cell value
// tuples. It returns the merged cell value tuple and a bool indicating if a
// conflict occurred. tryMerge should only be called if left and right produce
// non-identical diffs against base.
func (m *valueMerger) tryMerge(left, right, base val.Tuple) (val.Tuple, bool) {
	// If we're merging a keyless table and the keys match, but the values are different,
	// that means that the row data is the same, but the cardinality has changed, and if the
	// cardinality has changed in different ways on each merge side, we can't auto resolve.
	if m.keyless {
		return nil, false
	}

	if base != nil && (left == nil) != (right == nil) {
		// One row deleted, the other modified
		return nil, false
	}

	// Because we have non-identical diffs, left and right are guaranteed to be
	// non-nil at this point.
	if left == nil || right == nil {
		panic("found nil left / right which should never occur")
	}

	mergedValues := make([][]byte, m.numCols)
	for i := 0; i < m.numCols; i++ {
		v, isConflict := m.processColumn(i, left, right, base)
		if isConflict {
			return nil, false
		}
		mergedValues[i] = v
	}

	return val.NewTuple(m.syncPool, mergedValues...), true
}

// processColumn returns the merged value of column |i| of the merged schema,
// based on the |left|, |right|, and |base| schema.
func (m *valueMerger) processColumn(i int, left, right, base val.Tuple) ([]byte, bool) {
	// missing columns are coerced into NULL column values
	var leftCol []byte
	if l := m.leftMapping[i]; l != -1 {
		leftCol = left.GetField(l)
	}
	var rightCol []byte
	if r := m.rightMapping[i]; r != -1 {
		rightCol = right.GetField(r)
	}

	if m.vD.Comparator().CompareValues(i, leftCol, rightCol, m.vD.Types[i]) == 0 {
		return leftCol, false
	}

	if base == nil {
		// Conflicting insert
		return nil, true
	}

	var baseVal []byte
	if b := m.baseMapping[i]; b != -1 {
		baseVal = base.GetField(b)
	}

	leftModified := m.vD.Comparator().CompareValues(i, leftCol, baseVal, m.vD.Types[i]) != 0
	rightModified := m.vD.Comparator().CompareValues(i, rightCol, baseVal, m.vD.Types[i]) != 0

	switch {
	case leftModified && rightModified:
		return nil, true
	case leftModified:
		return leftCol, false
	default:
		return rightCol, false
	}
}
