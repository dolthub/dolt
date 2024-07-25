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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	errorkinds "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/expranalysis"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
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
// instance, along with merge stats and any error. If |diffInfo.RewriteRows| is true, then any existing rows in the
// table's primary index will also be rewritten. This function merges the table's artifacts (e.g. recorded
// conflicts), migrates any existing table data to the specified |mergedSch|, and merges table data from both
// sides of the merge together.
func mergeProllyTable(ctx context.Context, tm *TableMerger, mergedSch schema.Schema, mergeInfo MergeInfo, diffInfo tree.ThreeWayDiffInfo) (*doltdb.Table, *MergeStats, error) {
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
	valueMerger := newValueMerger(mergedSch, tm.leftSch, tm.rightSch, tm.ancSch, leftRows.Pool(), tm.ns)

	if !valueMerger.leftMapping.IsIdentityMapping() {
		mergeInfo.LeftNeedsRewrite = true
	}

	if !valueMerger.rightMapping.IsIdentityMapping() {
		mergeInfo.RightNeedsRewrite = true
	}

	// We need a sql.Context to apply column default values in merges; if we don't have one already,
	// create one, since this code also gets called from the CLI merge code path.
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		sqlCtx = sql.NewContext(ctx)
	}

	var stats *MergeStats
	mergeTbl, stats, err = mergeProllyTableData(sqlCtx, tm, mergedSch, mergeTbl, valueMerger, mergeInfo, diffInfo)
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
func mergeProllyTableData(ctx *sql.Context, tm *TableMerger, finalSch schema.Schema, mergeTbl *doltdb.Table, valueMerger *valueMerger, mergeInfo MergeInfo, diffInfo tree.ThreeWayDiffInfo) (*doltdb.Table, *MergeStats, error) {
	iter, err := threeWayDiffer(ctx, tm, valueMerger, diffInfo)
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

	defaults, err := resolveDefaults(ctx, tm.name, finalSch, tm.leftSch)
	if err != nil {
		return nil, nil, err
	}

	pri, err := newPrimaryMerger(leftEditor, tm, valueMerger, finalSch, mergeInfo, defaults)
	if err != nil {
		return nil, nil, err
	}
	sec, err := newSecondaryMerger(ctx, tm, valueMerger, tm.leftSch, finalSch, mergeInfo)
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

	nullChk, err := newNullValidator(ctx, finalSch, tm, valueMerger, artEditor, leftEditor, sec.leftIdxes)
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
		case tree.DiffOpLeftAdd, tree.DiffOpLeftModify:
			// In the event that the right side introduced a schema change, account for it here.
			// We still have to migrate when the diff is `tree.DiffOpLeftModify` because of the corner case where
			// the right side contains a schema change but the changed column is null, so row bytes don't change.
			err = pri.merge(ctx, diff, tm.leftSch)
			if err != nil {
				return nil, nil, err
			}

		case tree.DiffOpDivergentModifyConflict, tree.DiffOpDivergentDeleteConflict:
			// In this case, a modification or delete was made to one side, and a conflicting delete or modification
			// was made to the other side, so these cannot be automatically resolved.
			s.DataConflicts++
			err = conflicts.merge(ctx, diff, nil)
			if err != nil {
				return nil, nil, err
			}
			err = pri.merge(ctx, diff, tm.leftSch)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpRightAdd:
			s.Adds++
			err = pri.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff, tm.leftSch, tm.rightSch, tm, finalSch)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpRightModify:
			s.Modifications++
			err = pri.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff, tm.leftSch, tm.rightSch, tm, finalSch)
			if err != nil {
				return nil, nil, err
			}
		case tree.DiffOpRightDelete, tree.DiffOpDivergentDeleteResolved:
			s.Deletes++
			err = pri.merge(ctx, diff, tm.rightSch)
			if err != nil {
				return nil, nil, err
			}
			err = sec.merge(ctx, diff, tm.leftSch, tm.rightSch, tm, finalSch)
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
			err = sec.merge(ctx, diff, tm.leftSch, tm.rightSch, tm, finalSch)
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

	// After we've resolved all the diffs, it's safe for us to update the schema on the table
	mergeTbl, err = tm.leftTbl.UpdateSchema(ctx, finalSch)
	if err != nil {
		return nil, nil, err
	}

	finalRows, err := pri.finalize(ctx)
	if err != nil {
		return nil, nil, err
	}

	leftIdxs, rightIdxs, err := sec.finalize(ctx)
	if err != nil {
		return nil, nil, err
	}

	finalIdxs, err := mergeProllySecondaryIndexes(ctx, tm, leftIdxs, rightIdxs, finalSch, finalRows, conflicts.ae, mergeInfo.InvalidateSecondaryIndexes)
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

func threeWayDiffer(ctx context.Context, tm *TableMerger, valueMerger *valueMerger, diffInfo tree.ThreeWayDiffInfo) (*tree.ThreeWayDiffer[val.Tuple, val.TupleDesc], error) {
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

	return tree.NewThreeWayDiffer(
		ctx,
		leftRows.NodeStore(),
		leftRows.Tuples(),
		rightRows.Tuples(),
		ancRows.Tuples(),
		valueMerger.tryMerge,
		valueMerger.keyless,
		diffInfo,
		leftRows.Tuples().Order,
	)
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

		expr, err := expranalysis.ResolveCheckExpression(ctx, tm.name, sch, check.Expression())
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
	case tree.DiffOpLeftDelete, tree.DiffOpRightDelete, tree.DiffOpConvergentDelete, tree.DiffOpDivergentDeleteResolved:
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
		// Remap the value to the final schema before checking.
		// We skip keyless tables, since their value tuples require different mapping
		// logic and we don't currently support merges to keyless tables that contain schema changes anyway.
		newTuple := valueTuple
		if !cv.valueMerger.keyless {
			if diff.Op == tree.DiffOpRightAdd || diff.Op == tree.DiffOpRightModify {
				newTupleBytes := remapTuple(valueTuple, valueDesc, cv.valueMerger.rightMapping)
				newTuple = val.NewTuple(cv.valueMerger.syncPool, newTupleBytes...)
			} else if diff.Op == tree.DiffOpLeftAdd || diff.Op == tree.DiffOpLeftModify {
				newTupleBytes := remapTuple(valueTuple, valueDesc, cv.valueMerger.leftMapping)
				newTuple = val.NewTuple(cv.valueMerger.syncPool, newTupleBytes...)
			}
		}

		row, err := index.BuildRow(ctx, diff.Key, newTuple, cv.sch, cv.valueMerger.ns)
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
		booleanResult, err := sql.ConvertToBool(ctx, result)
		if err != nil {
			return 0, fmt.Errorf("unable to convert check constraint expression (%s) into boolean value: %v", checkName, err.Error())
		}

		if booleanResult {
			// If a check constraint returns TRUE (or NULL), then the check constraint is fulfilled
			// https://dev.mysql.com/doc/refman/8.0/en/create-table-check-constraints.html
			continue
		} else {
			if cv.tableMerger.recordViolations {
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

func newUniqValidator(ctx *sql.Context, sch schema.Schema, tm *TableMerger, vm *valueMerger, edits *prolly.ArtifactsEditor) (uniqValidator, error) {
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

		idx, err := indexes.GetIndex(ctx, sch, nil, def.Name())
		if err != nil {
			return uniqValidator{}, err
		}
		secondary := durable.ProllyMapFromIndex(idx)

		u, err := newUniqIndex(ctx, sch, tm.name, def, clustered, secondary)
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
func (uv uniqValidator) validateDiff(ctx *sql.Context, diff tree.ThreeWayDiff) (violations int, err error) {
	var value val.Tuple
	switch diff.Op {
	case tree.DiffOpRightAdd, tree.DiffOpRightModify:
		value = diff.Right
		// Don't remap the value to the merged schema if the table is keyless or if the mapping is an identity mapping.
		if !uv.valueMerger.keyless && !uv.valueMerger.rightMapping.IsIdentityMapping() {
			modifiedValue := remapTuple(value, uv.tm.rightSch.GetValueDescriptor(), uv.valueMerger.rightMapping)
			value = val.NewTuple(uv.valueMerger.syncPool, modifiedValue...)
		}
	case tree.DiffOpLeftAdd, tree.DiffOpLeftModify:
		value = diff.Left
		// Don't remap the value to the merged schema if the table is keyless or if the mapping is an identity mapping.
		if !uv.valueMerger.keyless && !uv.valueMerger.leftMapping.IsIdentityMapping() {
			modifiedValue := remapTuple(value, uv.tm.leftSch.GetValueDescriptor(), uv.valueMerger.leftMapping)
			value = val.NewTuple(uv.valueMerger.syncPool, modifiedValue...)
		}
	case tree.DiffOpRightDelete:
		// If we see a row deletion event from the right side, we grab the original/base value so that we can update our
		// local copy of the secondary index.
		value = diff.Base
	case tree.DiffOpDivergentModifyResolved:
		value = diff.Merged
	default:
		return
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

	if uv.tm.recordViolations {
		for _, idx := range uv.indexes {
			err = idx.findCollisions(ctx, diff.Key, value, func(k, v val.Tuple) error {
				violations++
				return uv.insertArtifact(ctx, k, v, idx.meta)
			})
			if err != nil {
				break
			}
		}
	}

	// After detecting any unique constraint violations, we need to update our indexes with the updated row
	if diff.Op != tree.DiffOpRightDelete {
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

func newUniqIndex(ctx *sql.Context, sch schema.Schema, tableName string, def schema.Index, clustered, secondary prolly.Map) (uniqIndex, error) {
	meta, err := makeUniqViolMeta(sch, def)
	if err != nil {
		return uniqIndex{}, err
	}

	if schema.IsKeyless(sch) { // todo(andy): sad panda
		secondary = prolly.ConvertToSecondaryKeylessIndex(secondary)
	}
	p := clustered.Pool()

	prefixDesc := secondary.KeyDesc().PrefixDesc(def.Count())
	secondaryBld, err := index.NewSecondaryKeyBuilder(ctx, tableName, sch, def, secondary.KeyDesc(), p, secondary.NodeStore())
	if err != nil {
		return uniqIndex{}, err
	}

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
	secondaryIndexKey, err := idx.secondaryBld.SecondaryKeyFromRow(ctx, key, value)
	if err != nil {
		return err
	}

	// secondary indexes only use their key tuple
	return idx.secondary.Put(ctx, secondaryIndexKey, val.EmptyTuple)
}

func (idx uniqIndex) removeRow(ctx context.Context, key, value val.Tuple) error {
	secondaryIndexKey, err := idx.secondaryBld.SecondaryKeyFromRow(ctx, key, value)
	if err != nil {
		return err
	}

	err = idx.secondary.Delete(ctx, secondaryIndexKey)
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
	indexKey, err := idx.secondaryBld.SecondaryKeyFromRow(ctx, key, value)
	if err != nil {
		return err
	}

	if idx.prefixDesc.HasNulls(indexKey) {
		return nil // NULLs cannot cause unique violations
	}

	// This code uses the secondary index to iterate over all rows (key/value pairs) that have the same prefix.
	// The prefix here is all the value columns this index is set up to track
	collisions := make([]val.Tuple, 0)
	err = idx.secondary.GetPrefix(ctx, indexKey, idx.prefixDesc, func(k, _ val.Tuple) (err error) {
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
		return

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
		count = len(violations)
		return
	case tree.DiffOpDivergentModifyResolved:
		var violations []string
		for to, _ := range nv.leftMap {
			col := nv.final.GetNonPKCols().GetByIndex(to)
			if !col.IsNullable() && diff.Merged.FieldIsNull(to) {
				violations = append(violations, col.Name)
			}
		}
		// for merged NULL violations, we insert a constraint violation and
		// then must explicitly remove this row from all left-side indexes
		if len(violations) > 0 {
			var meta prolly.ConstraintViolationMeta
			if meta, err = newNotNullViolationMeta(violations, diff.Merged); err != nil {
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
		count = len(violations)
		return
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
	mergeInfo   MergeInfo
	defaults    []sql.Expression
}

func newPrimaryMerger(leftEditor *prolly.MutableMap, tableMerger *TableMerger, valueMerger *valueMerger, finalSch schema.Schema, mergeInfo MergeInfo, defaults []sql.Expression) (*primaryMerger, error) {
	return &primaryMerger{
		mut:         leftEditor,
		valueMerger: valueMerger,
		tableMerger: tableMerger,
		finalSch:    finalSch,
		mergeInfo:   mergeInfo,
		defaults:    defaults,
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
			// Remapping when there's no schema change is harmless, but slow.
			if m.mergeInfo.RightNeedsRewrite {
				defaults, err := resolveDefaults(ctx, m.tableMerger.name, m.finalSch, m.tableMerger.rightSch)
				if err != nil {
					return err
				}

				tempTupleValue, err := remapTupleWithColumnDefaults(
					ctx,
					diff.Key,
					diff.Right,
					sourceSch.GetValueDescriptor(),
					m.valueMerger.rightMapping,
					m.tableMerger,
					m.tableMerger.rightSch,
					m.finalSch,
					defaults,
					m.valueMerger.syncPool,
					true,
				)
				if err != nil {
					return err
				}
				newTupleValue = tempTupleValue
			}
		}
		return m.mut.Put(ctx, diff.Key, newTupleValue)
	case tree.DiffOpRightDelete:
		return m.mut.Put(ctx, diff.Key, diff.Right)
	case tree.DiffOpDivergentDeleteResolved:
		// WARNING: In theory, we should only have to call MutableMap::Delete if the key is actually being deleted
		// from the left branch. However, because of https://github.com/dolthub/dolt/issues/7192,
		// if the left side of the merge is an empty table and we don't attempt to modify the map,
		// the table will have an unexpected root hash.
		return m.mut.Delete(ctx, diff.Key)
	case tree.DiffOpDivergentModifyResolved:
		// any generated columns need to be re-resolved because their computed values may have changed as a result of
		// the merge
		merged := diff.Merged
		if hasStoredGeneratedColumns(m.finalSch) {
			defaults, err := resolveDefaults(ctx, m.tableMerger.name, m.finalSch, m.tableMerger.rightSch)
			if err != nil {
				return err
			}

			tempTupleValue, err := remapTupleWithColumnDefaults(
				ctx,
				diff.Key,
				merged,
				m.finalSch.GetValueDescriptor(),
				m.valueMerger.rightMapping,
				m.tableMerger,
				m.tableMerger.rightSch,
				m.finalSch,
				defaults,
				m.valueMerger.syncPool,
				true)
			if err != nil {
				return err
			}
			merged = tempTupleValue
		}

		return m.mut.Put(ctx, diff.Key, merged)
	case tree.DiffOpLeftAdd, tree.DiffOpLeftModify, tree.DiffOpDivergentModifyConflict, tree.DiffOpDivergentDeleteConflict:
		// Remapping when there's no schema change is harmless, but slow.
		if !m.mergeInfo.LeftNeedsRewrite {
			return nil
		}
		// If the right side has a schema change, then newly added rows from the left must be migrated to the new schema.
		// Rows with unresolvable conflicts must also be migrated to the new schema so that they can resolved manually.
		if diff.Left == nil {
			return m.mut.Put(ctx, diff.Key, nil)
		}
		newTupleValue := diff.Left
		if schema.IsKeyless(sourceSch) {
			if m.valueMerger.leftMapping.IsIdentityMapping() == false {
				return fmt.Errorf("cannot merge keyless tables with reordered columns")
			}
		} else {
			tempTupleValue, err := remapTupleWithColumnDefaults(ctx, diff.Key, newTupleValue, sourceSch.GetValueDescriptor(),
				m.valueMerger.leftMapping, m.tableMerger, m.tableMerger.leftSch, m.finalSch, m.defaults, m.valueMerger.syncPool, false)
			if err != nil {
				return err
			}
			newTupleValue = tempTupleValue
		}
		return m.mut.Put(ctx, diff.Key, newTupleValue)
	default:
		return fmt.Errorf("unexpected diffOp for editing primary index: %s", diff.Op)
	}
}

func resolveDefaults(ctx *sql.Context, tableName string, mergedSchema schema.Schema, sourceSchema schema.Schema) ([]sql.Expression, error) {
	var exprs []sql.Expression
	i := 0

	// We want a slice of expressions in the order of the merged schema, but with column indexes from the source schema,
	// against which they will be evaluated
	err := mergedSchema.GetNonPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Virtual {
			return false, nil
		}

		if col.Default != "" || col.Generated != "" || col.OnUpdate != "" {
			expr, err := expranalysis.ResolveDefaultExpression(ctx, tableName, mergedSchema, col)
			if err != nil {
				return true, err
			}
			if len(exprs) == 0 {
				exprs = make([]sql.Expression, mergedSchema.GetNonPKCols().StoredSize())
			}
			exprs[i] = expr
		}

		i++
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	// The default expresions always come in the order of the merged schema, but the fields we need to apply them to
	// might have different column indexes in the case of a schema change
	if len(exprs) > 0 {
		for i := range exprs {
			if exprs[i] == nil {
				continue
			}
			exprs[i], _, _ = transform.Expr(exprs[i], func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				if gf, ok := e.(*expression.GetField); ok {
					newIdx := indexOf(gf.Name(), sourceSchema.GetAllCols().GetColumnNames())
					if newIdx >= 0 {
						return gf.WithIndex(newIdx), transform.NewTree, nil
					}
				}
				return e, transform.SameTree, nil
			})
		}
	}

	return exprs, nil
}

func indexOf(col string, cols []string) int {
	for i, column := range cols {
		if column == col {
			return i
		}
	}
	return -1
}

func hasStoredGeneratedColumns(sch schema.Schema) bool {
	hasGenerated := false
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.Generated != "" && !col.Virtual {
			hasGenerated = true
			return true, nil
		}
		return false, nil
	})
	return hasGenerated
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
	leftIdxes    []MutableSecondaryIdx
	valueMerger  *valueMerger
	mergedSchema schema.Schema
	tableMerger  *TableMerger
	mergeInfo    MergeInfo
}

const secondaryMergerPendingSize = 650_000

func newSecondaryMerger(ctx *sql.Context, tm *TableMerger, valueMerger *valueMerger, leftSchema, mergedSchema schema.Schema, mergeInfo MergeInfo) (*secondaryMerger, error) {
	ls, err := tm.leftTbl.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	// Use the mergedSchema to work with the secondary indexes, to pull out row data using the right
	// pri_index -> sec_index mapping.
	lm, err := GetMutableSecondaryIdxsWithPending(ctx, leftSchema, mergedSchema, tm.name, ls, secondaryMergerPendingSize)
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
		leftIdxes:    lm,
		valueMerger:  valueMerger,
		mergedSchema: mergedSchema,
		tableMerger:  tm,
		mergeInfo:    mergeInfo,
	}, nil
}

func (m *secondaryMerger) merge(ctx *sql.Context, diff tree.ThreeWayDiff, leftSchema, rightSchema schema.Schema, tm *TableMerger, finalSchema schema.Schema) error {
	var err error
	if m.mergeInfo.InvalidateSecondaryIndexes {
		return nil
	}
	for _, idx := range m.leftIdxes {
		switch diff.Op {
		case tree.DiffOpDivergentModifyResolved:
			// TODO: we need to re-resolve values from generated columns here as well
			err = applyEdit(ctx, idx, diff.Key, diff.Left, diff.Merged)
		case tree.DiffOpRightAdd, tree.DiffOpRightModify:
			// Just as with the primary index, we need to map right-side changes to the final, merged schema.
			if rightSchema == nil {
				return fmt.Errorf("no source schema specified to map right-side changes to merged schema")
			}

			newTupleValue := diff.Right
			baseTupleValue := diff.Base
			if m.mergeInfo.RightNeedsRewrite {
				if schema.IsKeyless(rightSchema) {
					if m.valueMerger.rightMapping.IsIdentityMapping() == false {
						return fmt.Errorf("cannot merge keyless tables with reordered columns")
					}
				} else {
					defaults, err := resolveDefaults(ctx, m.tableMerger.name, m.mergedSchema, m.tableMerger.rightSch)
					if err != nil {
						return err
					}

					// Convert right value to result schema
					tempTupleValue, err := remapTupleWithColumnDefaults(
						ctx,
						diff.Key,
						diff.Right,
						m.valueMerger.rightSchema.GetValueDescriptor(),
						m.valueMerger.rightMapping,
						m.tableMerger,
						m.tableMerger.rightSch,
						m.mergedSchema,
						defaults,
						m.valueMerger.syncPool,
						true,
					)
					if err != nil {
						return err
					}
					newTupleValue = tempTupleValue
					if diff.Base != nil {
						defaults, err := resolveDefaults(ctx, m.tableMerger.name, m.mergedSchema, m.tableMerger.ancSch)
						if err != nil {
							return err
						}

						// Convert base value to result schema
						baseTupleValue, err = remapTupleWithColumnDefaults(
							ctx,
							diff.Key,
							diff.Base,
							// Only the right side was modified, so the base schema must be the same as the left schema
							leftSchema.GetValueDescriptor(),
							m.valueMerger.baseMapping,
							tm,
							m.tableMerger.ancSch,
							finalSchema,
							defaults,
							m.valueMerger.syncPool,
							false)
						if err != nil {
							return err
						}
					}
				}
			}

			err = applyEdit(ctx, idx, diff.Key, baseTupleValue, newTupleValue)
		case tree.DiffOpRightDelete:
			err = applyEdit(ctx, idx, diff.Key, diff.Base, diff.Right)
		case tree.DiffOpDivergentDeleteResolved:
			// If the left-side has the delete, the index is already correct and no work needs to be done.
			// If the right-side has the delete, remove the key from the index.
			if diff.Right == nil {
				err = applyEdit(ctx, idx, diff.Key, diff.Base, nil)
			}
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
	for _, idx := range m.leftIdxes {
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
// and uses |mapping| to map the tuple's data and return a new tuple.
// |tm| provides high access to the name of the table currently being merged and associated node store.
// |mergedSch| is the new schema of the table and is used to look up column default values to apply to any existing
// rows when a new column is added as part of a merge.
// |pool| is used to allocate memory for the new tuple.
// |defaultExprs| is a slice of expressions that represent the default or generated values for all columns, with
// indexes in the same order as the tuple provided.
// |rightSide| indicates if the tuple came from the right side of the merge; this is needed to determine if the tuple
// data needs to be converted from the old schema type to a changed schema type.
func remapTupleWithColumnDefaults(
	ctx *sql.Context,
	keyTuple, valueTuple val.Tuple,
	valDesc val.TupleDesc,
	mapping val.OrdinalMapping,
	tm *TableMerger,
	rowSch schema.Schema,
	mergedSch schema.Schema,
	defaultExprs []sql.Expression,
	pool pool.BuffPool,
	rightSide bool,
) (val.Tuple, error) {
	tb := val.NewTupleBuilder(mergedSch.GetValueDescriptor())

	var secondPass []int
	for to, from := range mapping {
		col := mergedSch.GetNonPKCols().GetByStoredIndex(to)
		if from == -1 {
			// If the column is a new column, then look up any default or generated value in a second pass, after the
			// non-default and non-generated fields have been established. Virtual columns have been excluded, so any
			// generated column is stored.
			if col.Default != "" || col.Generated != "" || col.OnUpdate != "" {
				secondPass = append(secondPass, to)
			}
		} else {
			var value any
			var err error
			// Generated column values need to be regenerated after the merge
			if col.Generated != "" {
				secondPass = append(secondPass, to)
			}

			value, err = tree.GetField(ctx, valDesc, from, valueTuple, tm.ns)
			if err != nil {
				return nil, err
			}

			// If the type has changed, then call convert to convert the value to the new type
			value, err = convertValueToNewType(value, col.TypeInfo, tm, from, rightSide)
			if err != nil {
				return nil, err
			}

			err = tree.PutField(ctx, tm.ns, tb, to, value)
			if err != nil {
				return nil, err
			}
		}
	}

	for _, to := range secondPass {
		col := mergedSch.GetNonPKCols().GetByStoredIndex(to)
		err := writeTupleExpression(ctx, keyTuple, valueTuple, defaultExprs[to], col, rowSch, tm, tb, to)
		if err != nil {
			return nil, err
		}
	}

	return tb.Build(pool), nil
}

// writeTupleExpression attempts to evaluate the expression string |exprString| against the row provided and write it
// to the provided index in the tuple builder. This is necessary for column default values and generated columns.
func writeTupleExpression(
	ctx *sql.Context,
	keyTuple val.Tuple,
	valueTuple val.Tuple,
	expr sql.Expression,
	col schema.Column,
	sch schema.Schema,
	tm *TableMerger,
	tb *val.TupleBuilder,
	colIdx int,
) error {
	if !expr.Resolved() {
		return ErrUnableToMergeColumnDefaultValue.New(expr.String(), tm.name)
	}

	row, err := index.BuildRow(ctx, keyTuple, valueTuple, sch, tm.ns)
	if err != nil {
		return err
	}

	value, err := expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	value, _, err = col.TypeInfo.ToSqlType().Convert(value)
	if err != nil {
		return err
	}

	return tree.PutField(ctx, tm.ns, tb, colIdx, value)
}

// convertValueToNewType handles converting a value from a previous type into a new type. |value| is the value from
// the previous schema, |newTypeInfo| is the type info for the value in the new schema, |tm| is the TableMerger
// instance that describes how the table is being merged, |from| is the field position in the value tuple from the
// previous schema, and |rightSide| indicates whether the previous type info can be found on the right side of the merge
// or the left side. If the previous type info is the same as the current type info for the merged schema, then this
// function is a no-op and simply returns |value|. The converted value along with any unexpected error encountered is
// returned.
func convertValueToNewType(value interface{}, newTypeInfo typeinfo.TypeInfo, tm *TableMerger, from int, rightSide bool) (interface{}, error) {
	var previousTypeInfo typeinfo.TypeInfo
	if rightSide {
		previousTypeInfo = tm.rightSch.GetNonPKCols().GetByIndex(from).TypeInfo
	} else {
		previousTypeInfo = tm.leftSch.GetNonPKCols().GetByIndex(from).TypeInfo
	}

	if newTypeInfo.Equals(previousTypeInfo) {
		return value, nil
	}

	// If the type has changed, then call convert to convert the value to the new type
	newValue, inRange, err := newTypeInfo.ToSqlType().Convert(value)
	if err != nil {
		return nil, err
	}
	if !inRange {
		return nil, fmt.Errorf("out of range conversion for value %v to type %s", value, newTypeInfo.String())
	}
	return newValue, nil
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
	baseVD, leftVD, rightVD, resultVD      val.TupleDesc
	leftSchema, rightSchema, resultSchema  schema.Schema
	leftMapping, rightMapping, baseMapping val.OrdinalMapping
	baseToLeftMapping                      val.OrdinalMapping
	baseToRightMapping                     val.OrdinalMapping
	baseToResultMapping                    val.OrdinalMapping
	syncPool                               pool.BuffPool
	keyless                                bool
	ns                                     tree.NodeStore
}

func newValueMerger(merged, leftSch, rightSch, baseSch schema.Schema, syncPool pool.BuffPool, ns tree.NodeStore) *valueMerger {
	leftMapping, rightMapping, baseMapping := generateSchemaMappings(merged, leftSch, rightSch, baseSch)

	baseToLeftMapping, baseToRightMapping, baseToResultMapping := generateSchemaMappings(baseSch, leftSch, rightSch, merged)

	return &valueMerger{
		numCols:             merged.GetNonPKCols().StoredSize(),
		baseVD:              baseSch.GetValueDescriptor(),
		rightVD:             rightSch.GetValueDescriptor(),
		resultVD:            merged.GetValueDescriptor(),
		leftVD:              leftSch.GetValueDescriptor(),
		resultSchema:        merged,
		leftMapping:         leftMapping,
		rightMapping:        rightMapping,
		baseMapping:         baseMapping,
		baseToLeftMapping:   baseToLeftMapping,
		baseToRightMapping:  baseToRightMapping,
		baseToResultMapping: baseToResultMapping,
		leftSchema:          leftSch,
		rightSchema:         rightSch,
		syncPool:            syncPool,
		keyless:             schema.IsKeyless(merged),
		ns:                  ns,
	}
}

// generateSchemaMappings returns three schema mappings: 1) mapping the |leftSch| to |mergedSch|,
// 2) mapping |rightSch| to |mergedSch|, and 3) mapping |baseSch| to |mergedSch|. Columns are
// mapped from the source schema to destination schema by finding an identical tag, or if no
// identical tag is found, then falling back to a match on column name and type.
func generateSchemaMappings(mergedSch, leftSch, rightSch, baseSch schema.Schema) (leftMapping, rightMapping, baseMapping val.OrdinalMapping) {
	n := mergedSch.GetNonPKCols().StoredSize()
	leftMapping = make(val.OrdinalMapping, n)
	rightMapping = make(val.OrdinalMapping, n)
	baseMapping = make(val.OrdinalMapping, n)

	i := 0
	for _, col := range mergedSch.GetNonPKCols().GetColumns() {
		if col.Virtual {
			continue
		}
		leftMapping[i] = findNonPKColumnMappingByTagOrName(leftSch, col)
		rightMapping[i] = findNonPKColumnMappingByTagOrName(rightSch, col)
		baseMapping[i] = findNonPKColumnMappingByTagOrName(baseSch, col)
		i++
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
	if idx, ok := sch.GetNonPKCols().StoredIndexByTag(col.Tag); ok {
		return idx
	} else {
		return findNonPKColumnMappingByName(sch, col.Name)
	}
}

// tryMerge performs a cell-wise merge given left, right, and base cell value
// tuples. It returns the merged cell value tuple and a bool indicating if a
// conflict occurred. tryMerge should only be called if left and right produce
// non-identical diffs against base.
func (m *valueMerger) tryMerge(ctx *sql.Context, left, right, base val.Tuple) (val.Tuple, bool, error) {
	// If we're merging a keyless table and the keys match, but the values are different,
	// that means that the row data is the same, but the cardinality has changed, and if the
	// cardinality has changed in different ways on each merge side, we can't auto resolve.
	if m.keyless {
		return nil, false, nil
	}

	for i := 0; i < len(m.baseToRightMapping); i++ {
		isConflict, err := m.processBaseColumn(ctx, i, left, right, base)
		if err != nil {
			return nil, false, err
		}
		if isConflict {
			return nil, false, nil
		}
	}

	if base != nil && (left == nil) != (right == nil) {
		// One row deleted, the other modified
		// We just validated that this is not a conflict.
		return nil, true, nil
	}

	mergedValues := make([][]byte, m.numCols)
	for i := 0; i < m.numCols; i++ {
		v, isConflict, err := m.processColumn(ctx, i, left, right, base)
		if err != nil {
			return nil, false, err
		}
		if isConflict {
			return nil, false, nil
		}
		mergedValues[i] = v
	}

	return val.NewTuple(m.syncPool, mergedValues...), true, nil
}

// processBaseColumn returns whether column |i| of the base schema,
// if removed on one side, causes a conflict when merged with the other side.
func (m *valueMerger) processBaseColumn(ctx context.Context, i int, left, right, base val.Tuple) (conflict bool, err error) {
	if base == nil {
		// We're resolving an insertion. This can be done entirely in `processColumn`.
		return false, nil
	}
	baseCol := base.GetField(i)

	if left == nil {
		// Left side deleted the row. Thus, right side must have modified the row in order for there to be a conflict to resolve.
		rightCol, rightColIdx, rightColExists := getColumn(&right, &m.baseToRightMapping, i)

		if !rightColExists {
			// Right side deleted the column while left side deleted the row. This is not a conflict.
			return false, nil
		}
		// This is a conflict if the value on the right changed.
		// But if the right side only changed its representation (from ALTER COLUMN) and still has the same value,
		// then this can be resolved.
		baseCol, err = convert(ctx, m.baseVD, m.rightVD, m.rightSchema, i, rightColIdx, base, baseCol, m.ns)
		if err != nil {
			return false, err
		}
		if isEqual(i, baseCol, rightCol, m.rightVD.Types[rightColIdx]) {
			// right column did not change, so there is no conflict.
			return false, nil
		}
		// conflicting modifications
		return true, nil
	}

	if right == nil {
		// Right side deleted the row. Thus, left side must have modified the row in order for there to be a conflict to resolve.
		leftCol, leftColIdx, leftColExists := getColumn(&left, &m.baseToLeftMapping, i)

		if !leftColExists {
			// Left side deleted the column while right side deleted the row. This is not a conflict.
			return false, nil
		}
		// This is a conflict if the value on the left changed.
		// But if the left side only changed its representation (from ALTER COLUMN) and still has the same value,
		// then this can be resolved.
		baseCol, err = convert(ctx, m.baseVD, m.leftVD, m.leftSchema, i, leftColIdx, base, baseCol, m.ns)
		if err != nil {
			return false, err
		}
		if isEqual(i, baseCol, leftCol, m.leftVD.Types[leftColIdx]) {
			// left column did not change, so there is no conflict.
			return false, nil
		}
		// conflicting modifications
		return true, nil
	}

	rightCol, rightColIdx, rightColExists := getColumn(&right, &m.baseToRightMapping, i)

	leftCol, leftColIdx, leftColExists := getColumn(&left, &m.baseToLeftMapping, i)

	if leftColExists && rightColExists {
		// This column also exists in the merged schema, and will be processed there.
		return false, nil
	}

	if !leftColExists && !rightColExists {
		// This column is a convergent deletion. There is no conflict.
		return false, nil
	}

	var modifiedCol []byte
	var modifiedColIdx int
	var modifiedSchema schema.Schema
	var modifiedVD val.TupleDesc
	if !leftColExists {
		modifiedCol, modifiedColIdx = rightCol, rightColIdx
		modifiedSchema = m.rightSchema
		modifiedVD = m.rightVD
	} else {
		modifiedCol, modifiedColIdx = leftCol, leftColIdx
		modifiedSchema = m.leftSchema
		modifiedVD = m.leftVD
	}

	baseCol, err = convert(ctx, m.baseVD, modifiedVD, modifiedSchema, i, modifiedColIdx, base, baseCol, m.ns)
	if err != nil {
		return false, err
	}
	if modifiedVD.Comparator().CompareValues(i, baseCol, modifiedCol, modifiedVD.Types[modifiedColIdx]) == 0 {
		return false, nil
	}
	return true, nil
}

// processColumn returns the merged value of column |i| of the merged schema,
// based on the |left|, |right|, and |base| schema.
func (m *valueMerger) processColumn(ctx *sql.Context, i int, left, right, base val.Tuple) (result []byte, conflict bool, err error) {
	// missing columns are coerced into NULL column values

	var baseCol []byte
	var baseColIdx = -1
	var baseColExists = false
	if base != nil {
		baseCol, baseColIdx, baseColExists = getColumn(&base, &m.baseMapping, i)
	}
	leftCol, leftColIdx, leftColExists := getColumn(&left, &m.leftMapping, i)
	rightCol, rightColIdx, rightColExists := getColumn(&right, &m.rightMapping, i)
	resultType := m.resultVD.Types[i]
	resultColumn := m.resultSchema.GetNonPKCols().GetByIndex(i)
	generatedColumn := resultColumn.Generated != ""

	sqlType := m.resultSchema.GetNonPKCols().GetByIndex(i).TypeInfo.ToSqlType()

	// We previously asserted that left and right are not nil.
	// But base can be nil in the event of convergent inserts.
	if base == nil || !baseColExists {
		// There are two possible cases:
		// - The base row doesn't exist, or
		// - The column doesn't exist in the base row
		// Regardless, both left and right are inserts, or one is an insert and the other doesn't exist.

		if !rightColExists {
			return leftCol, false, nil
		}

		rightCol, err = convert(ctx, m.rightVD, m.resultVD, m.resultSchema, rightColIdx, i, right, rightCol, m.ns)
		if err != nil {
			return nil, false, err
		}

		if !leftColExists {
			return rightCol, false, nil
		}

		leftCol, err = convert(ctx, m.leftVD, m.resultVD, m.resultSchema, leftColIdx, i, left, leftCol, m.ns)
		if err != nil {
			return nil, false, err
		}

		if isEqual(i, leftCol, rightCol, resultType) {
			// Columns are equal, returning either would be correct.
			// However, for certain types the two columns may have different bytes.
			// We need to ensure that merges are deterministic regardless of the merge direction.
			// To achieve this, we sort the two values and return the higher one.
			if bytes.Compare(leftCol, rightCol) > 0 {
				return leftCol, false, nil
			}
			return rightCol, false, nil
		}

		// generated columns will be updated as part of the merge later on, so choose either value for now
		if generatedColumn {
			return leftCol, false, nil
		}

		// conflicting inserts
		return nil, true, nil
	}

	// We can now assume that both left are right contain byte-level changes to an existing column.
	// But we need to know if those byte-level changes represent a modification to the underlying value,
	// and whether those changes represent the *same* modification, otherwise there's a conflict.

	// We can't just look at the bytes to determine this, because if a cell's byte representation changed,
	// but only because of a schema change, we shouldn't consider that a conflict.
	// Conversely, if there was a schema change on only one side, we shouldn't consider the cells equal
	// even if they have the same bytes.

	// Thus, we must convert all cells to the type in the result schema before comparing them.

	if baseCol != nil {
		baseCol, err = convert(ctx, m.baseVD, m.resultVD, m.resultSchema, baseColIdx, i, base, baseCol, m.ns)
		if err != nil {
			return nil, false, err
		}
	}

	var leftModified, rightModified bool

	if leftColIdx == -1 && rightColIdx == -1 {
		// Both branches are implicitly NULL
		return nil, false, err
	}

	if rightColIdx == -1 {
		// The right branch is implicitly NULL
		rightModified = baseCol != nil
	} else {
		// Attempt to convert the right column to match the result schema, then compare it to the base.
		rightCol, err = convert(ctx, m.rightVD, m.resultVD, m.resultSchema, rightColIdx, i, right, rightCol, m.ns)
		if err != nil {
			return nil, true, nil
		}
		rightModified = !isEqual(i, rightCol, baseCol, resultType)
	}

	leftCol, err = convert(ctx, m.leftVD, m.resultVD, m.resultSchema, leftColIdx, i, left, leftCol, m.ns)
	if err != nil {
		return nil, true, nil
	}
	if isEqual(i, leftCol, rightCol, resultType) {
		// Columns are equal, returning either would be correct.
		// However, for certain types the two columns may have different bytes.
		// We need to ensure that merges are deterministic regardless of the merge direction.
		// To achieve this, we sort the two values and return the higher one.
		if bytes.Compare(leftCol, rightCol) > 0 {
			return leftCol, false, nil
		}
		return rightCol, false, nil
	}

	leftModified = !isEqual(i, leftCol, baseCol, resultType)

	switch {
	case leftModified && rightModified:
		// generated columns will be updated as part of the merge later on, so choose either value for now
		if generatedColumn {
			return leftCol, false, nil
		}
		// concurrent modification
		// if the result type is JSON, we can attempt to merge the JSON changes.
		dontMergeJsonVar, err := ctx.Session.GetSessionVariable(ctx, "dolt_dont_merge_json")
		if err != nil {
			return nil, true, err
		}
		disallowJsonMerge, err := sql.ConvertToBool(ctx, dontMergeJsonVar)
		if err != nil {
			return nil, true, err
		}
		if _, ok := sqlType.(types.JsonType); ok && !disallowJsonMerge {
			return m.mergeJSONAddr(ctx, baseCol, leftCol, rightCol)
		}
		// otherwise, this is a conflict.
		return nil, true, nil
	case leftModified:
		return leftCol, false, nil
	default:
		return rightCol, false, nil
	}
}

func (m *valueMerger) mergeJSONAddr(ctx context.Context, baseAddr []byte, leftAddr []byte, rightAddr []byte) (resultAddr []byte, conflict bool, err error) {
	baseDoc, err := tree.NewJSONDoc(hash.New(baseAddr), m.ns).ToIndexedJSONDocument(ctx)
	if err != nil {
		return nil, true, err
	}
	leftDoc, err := tree.NewJSONDoc(hash.New(leftAddr), m.ns).ToIndexedJSONDocument(ctx)
	if err != nil {
		return nil, true, err
	}
	rightDoc, err := tree.NewJSONDoc(hash.New(rightAddr), m.ns).ToIndexedJSONDocument(ctx)
	if err != nil {
		return nil, true, err
	}

	mergedDoc, conflict, err := mergeJSON(ctx, m.ns, baseDoc, leftDoc, rightDoc)
	if err != nil {
		return nil, true, err
	}
	if conflict {
		return nil, true, nil
	}

	root, err := tree.SerializeJsonToAddr(ctx, m.ns, mergedDoc)
	if err != nil {
		return nil, true, err
	}
	mergedAddr := root.HashOf()
	return mergedAddr[:], false, nil
}

func mergeJSON(ctx context.Context, ns tree.NodeStore, base, left, right sql.JSONWrapper) (resultDoc sql.JSONWrapper, conflict bool, err error) {
	// First, deserialize each value into JSON.
	// We can only merge if the value at all three commits is a JSON object.

	baseTypeCategory, err := tree.GetTypeCategory(base)
	if err != nil {
		return nil, true, err
	}
	leftTypeCategory, err := tree.GetTypeCategory(left)
	if err != nil {
		return nil, true, err
	}
	rightTypeCategory, err := tree.GetTypeCategory(right)
	if err != nil {
		return nil, true, err
	}

	baseIsObject := baseTypeCategory == tree.JsonTypeObject
	leftIsObject := leftTypeCategory == tree.JsonTypeObject
	rightIsObject := rightTypeCategory == tree.JsonTypeObject

	if !baseIsObject || !leftIsObject || !rightIsObject {
		// At least one of the commits does not have a JSON object.
		// If both left and right have the same value, use that value.
		// But if they differ, this is an unresolvable merge conflict.
		cmp, err := types.CompareJSON(left, right)
		if err != nil {
			return types.JSONDocument{}, true, err
		}
		if cmp == 0 {
			//convergent operation.
			return left, false, nil
		} else {
			return types.JSONDocument{}, true, nil
		}
	}

	indexedBase, isBaseIndexed := base.(tree.IndexedJsonDocument)
	indexedLeft, isLeftIndexed := left.(tree.IndexedJsonDocument)
	indexedRight, isRightIndexed := right.(tree.IndexedJsonDocument)

	// We only do three way merges on values read from tables right now, which are read in as tree.IndexedJsonDocument.

	var leftDiffer IJsonDiffer
	if isBaseIndexed && isLeftIndexed {
		leftDiffer, err = tree.NewIndexedJsonDiffer(ctx, indexedBase, indexedLeft)
		if err != nil {
			return nil, true, err
		}
	} else {
		baseObject, err := base.ToInterface()
		if err != nil {
			return nil, true, err
		}
		leftObject, err := left.ToInterface()
		if err != nil {
			return nil, true, err
		}
		leftDifferValue := tree.NewJsonDiffer(baseObject.(types.JsonObject), leftObject.(types.JsonObject))
		leftDiffer = &leftDifferValue
	}

	var rightDiffer IJsonDiffer
	if isBaseIndexed && isRightIndexed {
		rightDiffer, err = tree.NewIndexedJsonDiffer(ctx, indexedBase, indexedRight)
		if err != nil {
			return nil, true, err
		}
	} else {
		baseObject, err := base.ToInterface()
		if err != nil {
			return nil, true, err
		}
		rightObject, err := right.ToInterface()
		if err != nil {
			return nil, true, err
		}
		rightDifferValue := tree.NewJsonDiffer(baseObject.(types.JsonObject), rightObject.(types.JsonObject))
		rightDiffer = &rightDifferValue
	}

	threeWayDiffer := ThreeWayJsonDiffer{
		leftDiffer:  leftDiffer,
		rightDiffer: rightDiffer,
		ns:          ns,
	}

	// Compute the merged object by applying diffs to the left object as needed.
	// If the left object isn't an IndexedJsonDocument, we make one.
	var ok bool
	var merged tree.IndexedJsonDocument
	if merged, ok = left.(tree.IndexedJsonDocument); !ok {
		root, err := tree.SerializeJsonToAddr(ctx, ns, left)
		if err != nil {
			return types.JSONDocument{}, true, err
		}
		merged = tree.NewIndexedJsonDocument(ctx, root, ns)
	}

	for {
		threeWayDiff, err := threeWayDiffer.Next(ctx)
		if err == io.EOF {
			return merged, false, nil
		}
		if err != nil {
			return types.JSONDocument{}, true, err
		}

		switch threeWayDiff.Op {
		case tree.DiffOpRightAdd, tree.DiffOpConvergentAdd, tree.DiffOpRightModify, tree.DiffOpConvergentModify, tree.DiffOpDivergentModifyResolved:
			merged, _, err = merged.SetWithKey(ctx, threeWayDiff.Key, threeWayDiff.Right)
			if err != nil {
				return types.JSONDocument{}, true, err
			}
		case tree.DiffOpRightDelete, tree.DiffOpConvergentDelete:
			merged, _, err = merged.RemoveWithKey(ctx, threeWayDiff.Key)
			if err != nil {
				return types.JSONDocument{}, true, err
			}
		case tree.DiffOpLeftAdd, tree.DiffOpLeftModify, tree.DiffOpLeftDelete:
			// these changes already exist on the left, so do nothing.
		case tree.DiffOpDivergentModifyConflict, tree.DiffOpDivergentDeleteConflict:
			return types.JSONDocument{}, true, nil
		default:
			panic("unreachable")
		}
	}
}

func isEqual(i int, left []byte, right []byte, resultType val.Type) bool {
	// We use a default comparator instead of the comparator in the schema.
	// This is necessary to force a binary collation for string comparisons.
	return val.DefaultTupleComparator{}.CompareValues(i, left, right, resultType) == 0
}

func getColumn(tuple *val.Tuple, mapping *val.OrdinalMapping, idx int) (col []byte, colIndex int, exists bool) {
	colIdx := (*mapping)[idx]
	if colIdx == -1 {
		return nil, -1, false
	}
	return tuple.GetField(colIdx), colIdx, true
}

// convert takes the `i`th column in the provided tuple and converts it to the type specified in the provided schema.
// returns the new representation, and a bool indicating success.
func convert(ctx context.Context, fromDesc, toDesc val.TupleDesc, toSchema schema.Schema, fromIndex, toIndex int, tuple val.Tuple, originalValue []byte, ns tree.NodeStore) ([]byte, error) {
	if fromDesc.Types[fromIndex] == toDesc.Types[toIndex] {
		// No conversion is necessary here.
		return originalValue, nil
	}
	parsedCell, err := tree.GetField(ctx, fromDesc, fromIndex, tuple, ns)
	if err != nil {
		return nil, err
	}
	sqlType := toSchema.GetNonPKCols().GetByIndex(toIndex).TypeInfo.ToSqlType()
	convertedCell, _, err := sqlType.Convert(parsedCell)
	if err != nil {
		return nil, err
	}
	typ := toDesc.Types[toIndex]
	// If a merge results in assigning NULL to a non-null column, don't panic.
	// Instead we validate the merged tuple before merging it into the table.
	typ.Nullable = true
	return tree.Serialize(ctx, ns, typ, convertedCell)
}
