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
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ sql.TableFunction = (*PreviewMergeConflictsTableFunction)(nil)
var _ sql.ExecSourceRel = (*PreviewMergeConflictsTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*PreviewMergeConflictsTableFunction)(nil)

type PreviewMergeConflictsTableFunction struct {
	ctx             *sql.Context
	leftBranchExpr  sql.Expression
	rightBranchExpr sql.Expression
	tableNameExpr   sql.Expression
	sqlSch          sql.Schema
	database        sql.Database

	tblName   doltdb.TableName
	leftRoot  doltdb.RootValue
	rightRoot doltdb.RootValue
	baseRoot  doltdb.RootValue
}

// NewInstance creates a new instance of TableFunction interface
func (ds *PreviewMergeConflictsTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &PreviewMergeConflictsTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (ds *PreviewMergeConflictsTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(ds.Schema())
	numRows, _, err := ds.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (ds *PreviewMergeConflictsTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return previewMergeConflictsDefaultRowCount, false, nil
}

// Database implements the sql.Databaser interface
func (ds *PreviewMergeConflictsTableFunction) Database() sql.Database {
	return ds.database
}

// WithDatabase implements the sql.Databaser interface
func (ds *PreviewMergeConflictsTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	nds := *ds
	nds.database = database
	return &nds, nil
}

// Name implements the sql.TableFunction interface
func (ds *PreviewMergeConflictsTableFunction) Name() string {
	return "dolt_preview_merge_conflicts"
}

// Resolved implements the sql.Resolvable interface
func (ds *PreviewMergeConflictsTableFunction) Resolved() bool {
	return ds.leftBranchExpr.Resolved() && ds.rightBranchExpr.Resolved() && ds.tableNameExpr.Resolved()
}

func (ds *PreviewMergeConflictsTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (ds *PreviewMergeConflictsTableFunction) String() string {
	return fmt.Sprintf("DOLT_PREVIEW_MERGE_CONFLICTS(%s, %s, %s)", ds.leftBranchExpr.String(), ds.rightBranchExpr.String(), ds.tableNameExpr.String())
}

// Schema implements the sql.Node interface.
func (ds *PreviewMergeConflictsTableFunction) Schema() sql.Schema {
	if !ds.Resolved() {
		return nil
	}

	if ds.sqlSch == nil {
		panic("schema hasn't been generated yet")
	}

	return ds.sqlSch
}

// Children implements the sql.Node interface.
func (ds *PreviewMergeConflictsTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (ds *PreviewMergeConflictsTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return ds, nil
}

// CheckAuth implements the interface sql.AuthorizationCheckerNode.
func (pm *PreviewMergeConflictsTableFunction) CheckAuth(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if !types.IsText(pm.tableNameExpr.Type()) {
		return ExpressionIsDeferred(pm.tableNameExpr)
	}

	tableNameVal, err := pm.tableNameExpr.Eval(pm.ctx, nil)
	if err != nil {
		return false
	}
	tableName, ok, err := sql.Unwrap[string](ctx, tableNameVal)
	if err != nil {
		return false
	}
	if !ok {
		return false
	}

	subject := sql.PrivilegeCheckSubject{Database: pm.database.Name(), Table: tableName}
	// TODO: Add tests for privilege checking
	return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
}

// Expressions implements the sql.Expressioner interface.
func (pm *PreviewMergeConflictsTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{pm.leftBranchExpr, pm.rightBranchExpr, pm.tableNameExpr}
}

// WithExpressions implements the sql.Expressioner interface.
func (pm *PreviewMergeConflictsTableFunction) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 3 {
		return nil, sql.ErrInvalidArgumentNumber.New(pm.Name(), "3", len(exprs))
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
	newPmcs.tableNameExpr = exprs[2]

	// validate the expressions
	if !types.IsText(newPmcs.leftBranchExpr.Type()) && !expression.IsBindVar(newPmcs.leftBranchExpr) {
		return nil, sql.ErrInvalidArgumentDetails.New(newPmcs.Name(), newPmcs.leftBranchExpr.String())
	}
	if !types.IsText(newPmcs.rightBranchExpr.Type()) && !expression.IsBindVar(newPmcs.rightBranchExpr) {
		return nil, sql.ErrInvalidArgumentDetails.New(newPmcs.Name(), newPmcs.rightBranchExpr.String())
	}
	if !types.IsText(newPmcs.tableNameExpr.Type()) && !expression.IsBindVar(newPmcs.tableNameExpr) {
		return nil, sql.ErrInvalidArgumentDetails.New(newPmcs.Name(), newPmcs.tableNameExpr.String())
	}

	leftBranchVal, rightBranchVal, tableName, err := newPmcs.evaluateArguments()
	if err != nil {
		return nil, err
	}

	err = newPmcs.generateSchema(pm.ctx, leftBranchVal, rightBranchVal, tableName)
	if err != nil {
		return nil, err
	}

	return &newPmcs, nil
}

func (pm *PreviewMergeConflictsTableFunction) generateSchema(ctx *sql.Context, leftBranchVal, rightBranchVal interface{}, tableName string) error {
	if !pm.Resolved() {
		return nil
	}

	sqledb, ok := pm.database.(dsess.SqlDatabase)
	if !ok {
		return fmt.Errorf("unexpected database type: %T", pm.database)
	}

	leftBranch, err := interfaceToString(leftBranchVal)
	if err != nil {
		return err
	}
	rightBranch, err := interfaceToString(rightBranchVal)
	if err != nil {
		return err
	}

	leftRoot, rightRoot, baseRoot, err := resolveBranchesToRoots(ctx, sqledb, leftBranch, rightBranch)
	if err != nil {
		return err
	}

	tblName := doltdb.TableName{Name: tableName, Schema: doltdb.DefaultSchemaName}
	baseSch, ourSch, theirSch, err := getConflictSchemasFromRoots(ctx, tblName, leftRoot, rightRoot, baseRoot)
	if err != nil {
		return err
	}

	confSch, _, err := dtables.CalculateConflictSchema(baseSch, ourSch, theirSch)
	if err != nil {
		return err
	}

	sqlSch, err := sqlutil.FromDoltSchema(sqledb.Name(), tblName.Name, confSch)
	if err != nil {
		return err
	}

	pm.sqlSch = sqlSch.Schema
	pm.leftRoot = leftRoot
	pm.rightRoot = rightRoot
	pm.baseRoot = baseRoot
	pm.tblName = tblName

	return nil
}

func getConflictSchemasFromRoots(ctx *sql.Context, tblName doltdb.TableName, leftRoot, rightRoot, baseRoot doltdb.RootValue) (base, sch, mergeSch schema.Schema, err error) {
	ourTbl, ourOk, err := leftRoot.GetTable(ctx, tblName)
	if err != nil {
		return nil, nil, nil, err
	}
	if !ourOk {
		return nil, nil, nil, fmt.Errorf("could not find tbl %s in left root value", tblName)
	}

	baseTbl, baseOk, err := baseRoot.GetTable(ctx, tblName)
	if err != nil {
		return nil, nil, nil, err
	}
	theirTbl, theirOK, err := rightRoot.GetTable(ctx, tblName)
	if err != nil {
		return nil, nil, nil, err
	}
	if !theirOK {
		return nil, nil, nil, fmt.Errorf("could not find tbl %s in right root value", tblName)
	}

	ourSch, err := ourTbl.GetSchema(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	theirSch, err := theirTbl.GetSchema(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	// If the table does not exist in the ancestor, pretend it existed and that
	// it was completely empty.
	if !baseOk {
		if schema.SchemasAreEqual(ourSch, theirSch) {
			return ourSch, ourSch, theirSch, nil
		} else {
			return nil, nil, nil, fmt.Errorf("expected our schema to equal their schema since the table did not exist in the ancestor")
		}
	}

	baseSch, err := baseTbl.GetSchema(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	return baseSch, ourSch, theirSch, nil
}

// RowIter implements the sql.Node interface
func (pm *PreviewMergeConflictsTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	sqledb, ok := pm.database.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", pm.database)
	}

	conflicts, err := pm.getConflictsForTable(ctx, sqledb)
	if err != nil {
		return nil, err
	}

	return NewPreviewMergeConflictsTableFunctionRowIter(conflicts), nil
}

func (pm *PreviewMergeConflictsTableFunction) getConflictsForTable(ctx *sql.Context, sqledb dsess.SqlDatabase) ([]tableConflict, error) {
	merger, err := merge.NewMerger(pm.leftRoot, pm.rightRoot, pm.baseRoot, pm.rightRoot, pm.baseRoot, pm.leftRoot.VRW(), pm.leftRoot.NodeStore())
	if err != nil {
		return nil, err
	}

	mergeOpts := merge.MergeOpts{
		IsCherryPick:           false,
		KeepSchemaConflicts:    true,
		ReverifyAllConstraints: false,
	}

	tblName := doltdb.TableName{Name: pm.tableNameExpr.String(), Schema: doltdb.DefaultSchemaName}

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

// evaluateArguments returns leftBranchVal amd rightBranchVal.
// It evaluates the argument expressions to turn them into values this PreviewMergeConflictsTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (pm *PreviewMergeConflictsTableFunction) evaluateArguments() (interface{}, interface{}, string, error) {
	leftBranchVal, err := pm.leftBranchExpr.Eval(pm.ctx, nil)
	if err != nil {
		return nil, nil, "", err
	}

	rightBranchVal, err := pm.rightBranchExpr.Eval(pm.ctx, nil)
	if err != nil {
		return nil, nil, "", err
	}

	tableNameVal, err := pm.tableNameExpr.Eval(pm.ctx, nil)
	if err != nil {
		return nil, nil, "", err
	}

	tableName, ok := tableNameVal.(string)
	if !ok {
		return nil, nil, "", ErrInvalidTableName.New(pm.tableNameExpr.String())
	}

	return leftBranchVal, rightBranchVal, tableName, nil
}

//--------------------------------------------------
// previewMergeConflictsTableFunctionRowIter
//--------------------------------------------------

var _ sql.RowIter = &previewMergeConflictsTableFunctionRowIter{}

type previewMergeConflictsTableFunctionRowIter struct {
	conflicts []tableConflict
	conIdx    int
}

func (d *previewMergeConflictsTableFunctionRowIter) incrementIndexes() {
	d.conIdx++
	if d.conIdx >= len(d.conflicts) {
		d.conIdx = 0
		d.conflicts = nil
	}
}

func NewPreviewMergeConflictsTableFunctionRowIter(pm []tableConflict) sql.RowIter {
	return &previewMergeConflictsTableFunctionRowIter{
		conflicts: pm,
	}
}

func (d *previewMergeConflictsTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
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

func (d *previewMergeConflictsTableFunctionRowIter) Close(context *sql.Context) error {
	return nil
}
