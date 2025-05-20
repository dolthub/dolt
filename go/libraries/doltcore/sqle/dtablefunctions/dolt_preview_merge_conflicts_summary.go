// Copyright 2025 Dolthub, Inc.
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

package dtablefunctions

import (
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

const previewMergeConflictsDefaultRowCount = 100

var _ sql.TableFunction = (*PreviewMergeConflictsSummaryTableFunction)(nil)
var _ sql.ExecSourceRel = (*PreviewMergeConflictsSummaryTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*PreviewMergeConflictsSummaryTableFunction)(nil)

type PreviewMergeConflictsSummaryTableFunction struct {
	ctx             *sql.Context
	leftBranchExpr  sql.Expression
	rightBranchExpr sql.Expression
	database        sql.Database
}

var previewMergeConflictsSummarySchema = sql.Schema{
	&sql.Column{Name: "table", Type: types.Text, Nullable: false},
	&sql.Column{Name: "num_data_conflicts", Type: types.Uint64, Nullable: true},
	&sql.Column{Name: "num_schema_conflicts", Type: types.Uint64, Nullable: true},
}

// NewInstance creates a new instance of TableFunction interface
func (ds *PreviewMergeConflictsSummaryTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &PreviewMergeConflictsSummaryTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (ds *PreviewMergeConflictsSummaryTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(ds.Schema())
	numRows, _, err := ds.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (ds *PreviewMergeConflictsSummaryTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return previewMergeConflictsDefaultRowCount, false, nil
}

// Database implements the sql.Databaser interface
func (ds *PreviewMergeConflictsSummaryTableFunction) Database() sql.Database {
	return ds.database
}

// WithDatabase implements the sql.Databaser interface
func (ds *PreviewMergeConflictsSummaryTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	nds := *ds
	nds.database = database
	return &nds, nil
}

// Name implements the sql.TableFunction interface
func (ds *PreviewMergeConflictsSummaryTableFunction) Name() string {
	return "dolt_preview_merge_conflicts_summary"
}

// Resolved implements the sql.Resolvable interface
func (ds *PreviewMergeConflictsSummaryTableFunction) Resolved() bool {
	return ds.leftBranchExpr.Resolved() && ds.rightBranchExpr.Resolved()
}

func (ds *PreviewMergeConflictsSummaryTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (ds *PreviewMergeConflictsSummaryTableFunction) String() string {
	return fmt.Sprintf("DOLT_PREVIEW_MERGE_CONFLICTS_SUMMARY(%s, %s)", ds.leftBranchExpr.String(), ds.rightBranchExpr.String())
}

// Schema implements the sql.Node interface.
func (ds *PreviewMergeConflictsSummaryTableFunction) Schema() sql.Schema {
	return previewMergeConflictsSummarySchema
}

// Children implements the sql.Node interface.
func (ds *PreviewMergeConflictsSummaryTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (ds *PreviewMergeConflictsSummaryTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return ds, nil
}

// CheckAuth implements the interface sql.AuthorizationCheckerNode.
func (pm *PreviewMergeConflictsSummaryTableFunction) CheckAuth(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	tblNames, err := pm.database.GetTableNames(ctx)
	if err != nil {
		return false
	}

	var operations []sql.PrivilegedOperation
	for _, tblName := range tblNames {
		subject := sql.PrivilegeCheckSubject{Database: pm.database.Name(), Table: tblName}
		operations = append(operations, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
	}

	return opChecker.UserHasPrivileges(ctx, operations...)
}

// Expressions implements the sql.Expressioner interface.
func (pm *PreviewMergeConflictsSummaryTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{pm.leftBranchExpr, pm.rightBranchExpr}
}

// WithExpressions implements the sql.Expressioner interface.
func (pm *PreviewMergeConflictsSummaryTableFunction) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(pm.Name(), "2", len(exprs))
	}

	for _, expr := range exprs {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(pm.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(pm.Name(), expr.String())
		}
	}

	newPmcs := *pm
	newPmcs.leftBranchExpr = exprs[0]
	newPmcs.rightBranchExpr = exprs[1]

	// validate the expressions
	if !types.IsText(newPmcs.leftBranchExpr.Type()) && !expression.IsBindVar(newPmcs.leftBranchExpr) {
		return nil, sql.ErrInvalidArgumentDetails.New(newPmcs.Name(), newPmcs.leftBranchExpr.String())
	}
	if !types.IsText(newPmcs.rightBranchExpr.Type()) && !expression.IsBindVar(newPmcs.rightBranchExpr) {
		return nil, sql.ErrInvalidArgumentDetails.New(newPmcs.Name(), newPmcs.rightBranchExpr.String())
	}

	return &newPmcs, nil
}

// RowIter implements the sql.Node interface
func (pm *PreviewMergeConflictsSummaryTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	leftBranchVal, rightBranchVal, err := pm.evaluateArguments()
	if err != nil {
		return nil, err
	}

	leftBranch, err := interfaceToString(leftBranchVal)
	if err != nil {
		return nil, err
	}
	rightBranch, err := interfaceToString(rightBranchVal)
	if err != nil {
		return nil, err
	}

	sqledb, ok := pm.database.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", pm.database)
	}

	tables, err := getTablesWithConflicts(ctx, sqledb, leftBranch, rightBranch)
	if err != nil {
		return nil, err
	}

	return NewPreviewMergeConflictsSummaryTableFunctionRowIter(tables), nil
}

// evaluateArguments returns leftBranchVal amd rightBranchVal.
// It evaluates the argument expressions to turn them into values this PreviewMergeConflictsSummaryTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (pm *PreviewMergeConflictsSummaryTableFunction) evaluateArguments() (interface{}, interface{}, error) {
	leftBranchVal, err := pm.leftBranchExpr.Eval(pm.ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	rightBranchVal, err := pm.rightBranchExpr.Eval(pm.ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	return leftBranchVal, rightBranchVal, nil
}

//--------------------------------------------------
// previewMergeConflictsSummaryTableFunctionRowIter
//--------------------------------------------------

var _ sql.RowIter = &previewMergeConflictsSummaryTableFunctionRowIter{}

type previewMergeConflictsSummaryTableFunctionRowIter struct {
	conflicts []tableConflict
	conIdx    int
}

func (d *previewMergeConflictsSummaryTableFunctionRowIter) incrementIndexes() {
	d.conIdx++
	if d.conIdx >= len(d.conflicts) {
		d.conIdx = 0
		d.conflicts = nil
	}
}

func NewPreviewMergeConflictsSummaryTableFunctionRowIter(pm []tableConflict) sql.RowIter {
	return &previewMergeConflictsSummaryTableFunctionRowIter{
		conflicts: pm,
	}
}

func (d *previewMergeConflictsSummaryTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	defer d.incrementIndexes()
	if d.conIdx >= len(d.conflicts) {
		return nil, io.EOF
	}

	if d.conflicts == nil {
		return nil, io.EOF
	}

	pm := d.conflicts[d.conIdx]
	return getRowFromConflict(pm), nil
}

func (d *previewMergeConflictsSummaryTableFunctionRowIter) Close(context *sql.Context) error {
	return nil
}

func getRowFromConflict(conflict tableConflict) sql.Row {
	row := sql.Row{
		conflict.tableName.String(), // table
	}
	if conflict.numDataConflicts != nil {
		row = append(row, *conflict.numDataConflicts) // num_data_conflicts
	} else {
		row = append(row, nil)
	}
	if conflict.numSchemaConflicts != nil {
		row = append(row, *conflict.numSchemaConflicts) // num_schema_conflicts
	} else {
		row = append(row, nil)
	}
	return row
}

func resolveBranchesToRoots(ctx *sql.Context, db dsess.SqlDatabase, leftBranch, rightBranch string) (doltdb.RootValue, doltdb.RootValue, doltdb.RootValue, error) {
	sess := dsess.DSessFromSess(ctx.Session)

	headRef, err := sess.CWBHeadRef(ctx, db.Name())
	if err != nil {
		return nil, nil, nil, err
	}

	leftCm, err := resolveCommit(ctx, db.DbData().Ddb, headRef, leftBranch)
	if err != nil {
		return nil, nil, nil, err
	}

	rightCm, err := resolveCommit(ctx, db.DbData().Ddb, headRef, rightBranch)
	if err != nil {
		return nil, nil, nil, err
	}

	optCmt, err := doltdb.GetCommitAncestor(ctx, leftCm, rightCm)
	if err != nil {
		return nil, nil, nil, err
	}

	mergeBase, ok := optCmt.ToCommit()
	if !ok {
		return nil, nil, nil, doltdb.ErrGhostCommitEncountered
	}

	rightRoot, err := rightCm.GetRootValue(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	leftRoot, err := leftCm.GetRootValue(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	baseRoot, err := mergeBase.GetRootValue(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	return leftRoot, rightRoot, baseRoot, nil
}

type tableConflict struct {
	tableName          doltdb.TableName
	numDataConflicts   *uint64
	numSchemaConflicts *uint64
}

func getTablesWithConflicts(ctx *sql.Context, db dsess.SqlDatabase, baseBranch, mergeBranch string) ([]tableConflict, error) {
	leftRoot, rightRoot, baseRoot, err := resolveBranchesToRoots(ctx, db, baseBranch, mergeBranch)
	if err != nil {
		return nil, err
	}

	merger, err := merge.NewMerger(leftRoot, rightRoot, baseRoot, rightRoot, baseRoot, leftRoot.VRW(), leftRoot.NodeStore())
	if err != nil {
		return nil, err
	}

	tblNames, err := doltdb.UnionTableNames(ctx, leftRoot, rightRoot)
	if err != nil {
		return nil, err
	}

	mergeOpts := merge.MergeOpts{
		IsCherryPick:           false,
		KeepSchemaConflicts:    true,
		ReverifyAllConstraints: false,
	}

	var conflicted []tableConflict

	for _, tblName := range tblNames {
		tm, err := merger.MakeTableMerger(ctx, tblName, mergeOpts)
		if err != nil {
			return nil, err
		}

		// short-circuit here if we can
		finished, _, stats, err := merger.MaybeShortCircuit(ctx, tm, mergeOpts)
		if err != nil {
			return nil, err
		}
		if finished != nil || stats != nil {
			continue
		}

		// Calculate a merge of the schemas, but don't apply it
		mergeSch, schConflicts, _, diffInfo, err := tm.SchemaMerge(ctx, tblName)
		if err != nil {
			return nil, err
		}
		numSchemaConflicts := uint64(schConflicts.Count())
		if numSchemaConflicts > 0 {
			conflicted = append(conflicted, tableConflict{tableName: tblName, numSchemaConflicts: &numSchemaConflicts})
			// Cannot calculate data conflicts if there are schema conflicts
			continue
		}

		keyless := schema.IsKeyless(mergeSch)

		leftRows, err := tm.LeftRows(ctx)
		if err != nil {
			return nil, err
		}
		rightRows, err := tm.RightRows(ctx)
		if err != nil {
			return nil, err
		}
		ancRows, err := tm.AncRows(ctx)
		if err != nil {
			return nil, err
		}

		valueMerger := tm.GetNewValueMerger(mergeSch, leftRows)

		differ, err := tree.NewThreeWayDiffer(
			ctx,
			leftRows.NodeStore(),
			leftRows.Tuples(),
			rightRows.Tuples(),
			ancRows.Tuples(),
			valueMerger.TryMerge,
			keyless,
			diffInfo,
			leftRows.Tuples().Order,
		)
		if err != nil {
			return nil, err
		}

		var numDataConflicts uint64 = 0
		for {
			diff, err := differ.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, err
			}
			switch diff.Op {
			case tree.DiffOpDivergentModifyConflict, tree.DiffOpDivergentDeleteConflict:
				// In this case, a modification or delete was made to one side, and a conflicting delete or modification
				// was made to the other side, so these cannot be automatically resolved.
				numDataConflicts++
			case tree.DiffOpConvergentAdd, tree.DiffOpConvergentModify, tree.DiffOpConvergentDelete:
				if keyless {
					numDataConflicts++
				}
			}
		}

		if numDataConflicts > 0 {
			conflicted = append(conflicted, tableConflict{tableName: tblName, numSchemaConflicts: &numSchemaConflicts, numDataConflicts: &numDataConflicts})
		}
	}

	return conflicted, nil
}
