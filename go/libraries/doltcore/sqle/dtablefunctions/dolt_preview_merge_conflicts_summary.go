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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/prolly/tree"
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
func (pm *PreviewMergeConflictsSummaryTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
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

func (pm *PreviewMergeConflictsSummaryTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(pm.Schema())
	numRows, _, err := pm.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (pm *PreviewMergeConflictsSummaryTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return previewMergeConflictsDefaultRowCount, false, nil
}

// Database implements the sql.Databaser interface
func (pm *PreviewMergeConflictsSummaryTableFunction) Database() sql.Database {
	return pm.database
}

// WithDatabase implements the sql.Databaser interface
func (pm *PreviewMergeConflictsSummaryTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	npm := *pm
	npm.database = database
	return &npm, nil
}

// Name implements the sql.TableFunction interface
func (pm *PreviewMergeConflictsSummaryTableFunction) Name() string {
	return "dolt_preview_merge_conflicts_summary"
}

// Resolved implements the sql.Resolvable interface
func (pm *PreviewMergeConflictsSummaryTableFunction) Resolved() bool {
	return pm.leftBranchExpr.Resolved() && pm.rightBranchExpr.Resolved()
}

func (pm *PreviewMergeConflictsSummaryTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (pm *PreviewMergeConflictsSummaryTableFunction) String() string {
	return fmt.Sprintf("DOLT_PREVIEW_MERGE_CONFLICTS_SUMMARY(%s, %s)", pm.leftBranchExpr.String(), pm.rightBranchExpr.String())
}

// Schema implements the sql.Node interface.
func (pm *PreviewMergeConflictsSummaryTableFunction) Schema() sql.Schema {
	return previewMergeConflictsSummarySchema
}

// Children implements the sql.Node interface.
func (pm *PreviewMergeConflictsSummaryTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (pm *PreviewMergeConflictsSummaryTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return pm, nil
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
		return nil, fmt.Errorf("invalid left branch parameter: %w", err)
	}
	rightBranch, err := interfaceToString(rightBranchVal)
	if err != nil {
		return nil, fmt.Errorf("invalid right branch parameter: %w", err)
	}

	// Validate branch names are not empty
	if leftBranch == "" {
		return nil, fmt.Errorf("left branch name cannot be empty")
	}
	if rightBranch == "" {
		return nil, fmt.Errorf("right branch name cannot be empty")
	}

	sqledb, ok := pm.database.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", pm.database)
	}

	conflicts, err := getTablesWithConflicts(ctx, sqledb, leftBranch, rightBranch)
	if err != nil {
		return nil, err
	}

	return NewPreviewMergeConflictsSummaryTableFunctionRowIter(conflicts), nil
}

// evaluateArguments returns leftBranchVal and rightBranchVal.
// It evaluates the argument expressions to turn them into values this PreviewMergeConflictsSummaryTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (pm *PreviewMergeConflictsSummaryTableFunction) evaluateArguments() (interface{}, interface{}, error) {
	leftBranchVal, err := pm.leftBranchExpr.Eval(pm.ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to evaluate left branch expression: %w", err)
	}

	rightBranchVal, err := pm.rightBranchExpr.Eval(pm.ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to evaluate right branch expression: %w", err)
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

func (iter *previewMergeConflictsSummaryTableFunctionRowIter) incrementIndexes() {
	iter.conIdx++
	if iter.conIdx >= len(iter.conflicts) {
		iter.conIdx = 0
		iter.conflicts = nil
	}
}

func NewPreviewMergeConflictsSummaryTableFunctionRowIter(pm []tableConflict) sql.RowIter {
	return &previewMergeConflictsSummaryTableFunctionRowIter{
		conflicts: pm,
	}
}

func (iter *previewMergeConflictsSummaryTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	defer iter.incrementIndexes()
	if iter.conIdx >= len(iter.conflicts) || iter.conflicts == nil {
		return nil, io.EOF
	}

	conflict := iter.conflicts[iter.conIdx]
	return getRowFromConflict(conflict), nil
}

func (iter *previewMergeConflictsSummaryTableFunctionRowIter) Close(context *sql.Context) error {
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

type rootInfo struct {
	leftRoot  doltdb.RootValue
	rightRoot doltdb.RootValue
	baseRoot  doltdb.RootValue
	rightCm   doltdb.Rootish
	ancCm     doltdb.Rootish
}

// resolveBranchesToRoots resolves branch names to their corresponding root values
// and finds the common merge base.
func resolveBranchesToRoots(ctx *sql.Context, db dsess.SqlDatabase, leftBranch, rightBranch string) (rootInfo, error) {
	sess := dsess.DSessFromSess(ctx.Session)

	headRef, err := sess.CWBHeadRef(ctx, db.Name())
	if err != nil {
		return rootInfo{}, err
	}

	leftCm, err := resolveCommit(ctx, db.DbData().Ddb, headRef, leftBranch)
	if err != nil {
		return rootInfo{}, err
	}

	rightCm, err := resolveCommit(ctx, db.DbData().Ddb, headRef, rightBranch)
	if err != nil {
		return rootInfo{}, err
	}

	optCm, err := doltdb.GetCommitAncestor(ctx, leftCm, rightCm)
	if err != nil {
		return rootInfo{}, err
	}

	ancCm, ok := optCm.ToCommit()
	if !ok {
		return rootInfo{}, doltdb.ErrGhostCommitEncountered
	}

	rightRoot, err := rightCm.GetRootValue(ctx)
	if err != nil {
		return rootInfo{}, err
	}
	leftRoot, err := leftCm.GetRootValue(ctx)
	if err != nil {
		return rootInfo{}, err
	}
	baseRoot, err := ancCm.GetRootValue(ctx)
	if err != nil {
		return rootInfo{}, err
	}

	return rootInfo{leftRoot, rightRoot, baseRoot, rightCm, ancCm}, nil
}

type tableConflict struct {
	tableName          doltdb.TableName
	numDataConflicts   *uint64 // nil if schema conflicts exist
	numSchemaConflicts *uint64
}

// getTablesWithConflicts analyzes the merge between two branches and returns
// a list of tables that would have conflicts. It performs a dry-run merge
// to identify both schema and data conflicts without modifying the database.
func getTablesWithConflicts(ctx *sql.Context, db dsess.SqlDatabase, baseBranch, mergeBranch string) ([]tableConflict, error) {
	ri, err := resolveBranchesToRoots(ctx, db, baseBranch, mergeBranch)
	if err != nil {
		return nil, err
	}

	merger, err := merge.NewMerger(ri.leftRoot, ri.rightRoot, ri.baseRoot, ri.rightRoot, ri.baseRoot, ri.leftRoot.VRW(), ri.leftRoot.NodeStore())
	if err != nil {
		return nil, err
	}

	tblNames, err := doltdb.UnionTableNames(ctx, ri.leftRoot, ri.rightRoot)
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

		dataConflicts, err := getDataConflictsForTable(ctx, tm, tblName, mergeSch, diffInfo)
		if err != nil {
			return nil, err
		}
		if dataConflicts != nil {
			conflicted = append(conflicted, *dataConflicts)
		}
	}

	return conflicted, nil
}

// getDataConflictsForTable calculates the number of data conflicts for a specific table.
// It performs a three-way diff to identify rows that cannot be automatically merged.
// Returns nil if no data conflicts are found.
func getDataConflictsForTable(ctx *sql.Context, tm *merge.TableMerger, tblName doltdb.TableName, mergeSch schema.Schema, diffInfo tree.ThreeWayDiffInfo) (*tableConflict, error) {
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
		numSchemaConflicts := uint64(0)
		return &tableConflict{tableName: tblName, numSchemaConflicts: &numSchemaConflicts, numDataConflicts: &numDataConflicts}, nil
	}

	return nil, nil
}
