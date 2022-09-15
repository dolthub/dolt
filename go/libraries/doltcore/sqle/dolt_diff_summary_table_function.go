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

package sqle

import (
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"io"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.TableFunction = (*DiffSummaryTableFunction)(nil)

type DiffSummaryTableFunction struct {
	ctx *sql.Context

	fromCommitExpr sql.Expression
	toCommitExpr   sql.Expression
	tableNameExpr  sql.Expression
	database       sql.Database

	tableDelta diff.TableDelta
}

var diffSummaryTableSchema = sql.Schema{
	&sql.Column{Name: "table_name", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "rows_unmodified", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "rows_added", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "rows_deleted", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "row_modified", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "cells_added", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "cells_deleted", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "cells_modified", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "old_row_count", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "new_row_count", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "old_cell_count", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "new_cell_count", Type: sql.Int64, Nullable: true},
}

// NewInstance implements the TableFunction interface
func (ds *DiffSummaryTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &DiffSummaryTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (ds *DiffSummaryTableFunction) Database() sql.Database {
	return ds.database
}

func (ds *DiffSummaryTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	ds.database = database
	return ds, nil
}

func (ds *DiffSummaryTableFunction) FunctionName() string {
	return "dolt_diff_summary"
}

func (ds *DiffSummaryTableFunction) Resolved() bool {
	if ds.tableNameExpr != nil {
		return ds.fromCommitExpr.Resolved() && ds.toCommitExpr.Resolved() && ds.tableNameExpr.Resolved()
	}
	return ds.fromCommitExpr.Resolved() && ds.toCommitExpr.Resolved()
}

func (ds *DiffSummaryTableFunction) String() string {
	if ds.tableNameExpr != nil {
		return fmt.Sprintf("DOLT_DIFF_SUMMARY(%s, %s, %s)", ds.fromCommitExpr.String(), ds.toCommitExpr.String(), ds.tableNameExpr.String())
	}
	return fmt.Sprintf("DOLT_DIFF_SUMMARY(%s, %s)", ds.fromCommitExpr.String(), ds.toCommitExpr.String())
}

func (ds *DiffSummaryTableFunction) Schema() sql.Schema {
	return diffSummaryTableSchema
}

func (ds *DiffSummaryTableFunction) Children() []sql.Node {
	return nil
}

func (ds *DiffSummaryTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		panic("unexpected children")
	}
	return ds, nil
}

func (ds *DiffSummaryTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if ds.tableNameExpr != nil {
		tableNameVal, err := ds.tableNameExpr.Eval(ds.ctx, nil)
		if err != nil {
			return false
		}
		tableName, ok := tableNameVal.(string)
		if !ok {
			return false
		}

		// TODO: Add tests for privilege checking
		return opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(ds.database.Name(), tableName, "", sql.PrivilegeType_Select))
	}

	tblNames, err := ds.database.GetTableNames(ctx)
	if err != nil {
		return false
	}

	var operations []sql.PrivilegedOperation
	for _, tblName := range tblNames {
		operations = append(operations, sql.NewPrivilegedOperation(ds.database.Name(), tblName, "", sql.PrivilegeType_Select))
	}

	return opChecker.UserHasPrivileges(ctx, operations...)
}

func (ds *DiffSummaryTableFunction) Expressions() []sql.Expression {
	exprs := []sql.Expression{ds.fromCommitExpr, ds.toCommitExpr}
	if ds.tableNameExpr != nil {
		exprs = append(exprs, ds.tableNameExpr)
	}
	return exprs
}

func (ds *DiffSummaryTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) < 2 || len(expression) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New(ds.FunctionName(), "2 or 3", len(expression))
	}

	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(ds.FunctionName(), expr.String())
		}
	}

	ds.fromCommitExpr = expression[0]
	ds.toCommitExpr = expression[1]
	if len(expression) == 3 {
		ds.tableNameExpr = expression[2]
	}

	return ds, nil
}

func (ds *DiffSummaryTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	fromCommitVal, toCommitVal, tableName, err := ds.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := ds.database.(Database)
	if !ok {
		panic(fmt.Sprintf("unexpected database type: %T", ds.database))
	}

	sess := dsess.DSessFromSess(ctx.Session)
	fromRoot, _, err := sess.ResolveRootForRef(ctx, sqledb.Name(), fromCommitVal)
	if err != nil {
		return nil, err
	}

	toRoot, _, err := sess.ResolveRootForRef(ctx, sqledb.Name(), toCommitVal)
	if err != nil {
		return nil, err
	}

	deltas, err := diff.GetTableDeltas(ctx, fromRoot, toRoot)
	if err != nil {
		return nil, err
	}

	if tableName != "" {
		delta := findMatchingDelta(deltas, tableName)
		diffSum, err := getDiffSummaryFromDelta(ctx, delta, fromRoot, toRoot, tableName)
		if err != nil {
			return nil, err
		}
		return NewDiffSummaryTableFunctionRowIter([]diffSummaryNode{diffSum}), nil
	}

	var diffSummaries []diffSummaryNode
	for _, delta := range deltas {
		// TODO should it be ToName or FromName??? if they are not the same name?
		tblName := delta.ToName
		diffSum, err := getDiffSummaryFromDelta(ctx, delta, fromRoot, toRoot, tblName)
		if err != nil {
			return nil, err
		}
		diffSummaries = append(diffSummaries, diffSum)
	}

	return NewDiffSummaryTableFunctionRowIter(diffSummaries), nil
}

func getDiffSummaryFromDelta(ctx *sql.Context, delta diff.TableDelta, fromRoot, toRoot *doltdb.RootValue, tableName string) (diffSummaryNode, error) {
	diffSum, err := getDiffSummary(ctx, delta)
	if err != nil {
		return diffSummaryNode{}, err
	}
	oldLen, newLen, err := getColumnLenghts(ctx, fromRoot, toRoot, tableName)
	return diffSummaryNode{tableName, diffSum, oldLen, newLen}, nil
}

// evaluateArguments returns fromCommitValStr, toCommitValStr and tableName. If tableName
// is not defined, it returns empty string.
// It evaluates the argument expressions to turn them into values this DiffTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (dtf *DiffSummaryTableFunction) evaluateArguments() (string, string, string, error) {
	if !dtf.Resolved() {
		return "", "", "", nil
	}

	if !sql.IsText(dtf.fromCommitExpr.Type()) {
		return "", "", "", sql.ErrInvalidArgumentDetails.New(dtf.FunctionName(), dtf.fromCommitExpr.String())
	}

	if !sql.IsText(dtf.toCommitExpr.Type()) {
		return "", "", "", sql.ErrInvalidArgumentDetails.New(dtf.FunctionName(), dtf.toCommitExpr.String())
	}

	var tableName string
	if dtf.tableNameExpr != nil {
		if !sql.IsText(dtf.tableNameExpr.Type()) {
			return "", "", "", sql.ErrInvalidArgumentDetails.New(dtf.FunctionName(), dtf.tableNameExpr.String())
		}
		tableNameVal, err := dtf.tableNameExpr.Eval(dtf.ctx, nil)
		if err != nil {
			return "", "", "", err
		}
		tn, ok := tableNameVal.(string)
		if !ok {
			return "", "", "", ErrInvalidTableName.New(dtf.tableNameExpr.String())
		}
		tableName = tn
	}

	fromCommitVal, err := dtf.fromCommitExpr.Eval(dtf.ctx, nil)
	if err != nil {
		return "", "", "", err
	}
	fromCommitValStr, ok := fromCommitVal.(string)
	if !ok {
		return "", "", "", fmt.Errorf("received '%v' when expecting commit hash string", fromCommitVal)
	}

	toCommitVal, err := dtf.toCommitExpr.Eval(dtf.ctx, nil)
	if err != nil {
		return "", "", "", err
	}
	toCommitValStr, ok := toCommitVal.(string)
	if !ok {
		return "", "", "", fmt.Errorf("received '%v' when expecting commit hash string", toCommitVal)
	}

	return fromCommitValStr, toCommitValStr, tableName, nil
}

func getColumnLenghts(ctx *sql.Context, fromRoot, toRoot *doltdb.RootValue, tableName string) (int, int, error) {
	var oldColLen int
	var newColLen int

	fromTable, _, fromTableExists, err := fromRoot.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return 0, 0, err
	}

	if fromTableExists {
		fromSch, err := fromTable.GetSchema(ctx)
		if err != nil {
			return 0, 0, err
		}
		oldColLen = len(fromSch.GetAllCols().GetColumns())
	}

	toTable, _, toTableExists, err := toRoot.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return 0, 0, err
	}

	if toTableExists {
		toSch, err := toTable.GetSchema(ctx)
		if err != nil {
			return 0, 0, err
		}
		newColLen = len(toSch.GetAllCols().GetColumns())
	}

	return oldColLen, newColLen, nil
}

func getDiffSummary(ctx *sql.Context, td diff.TableDelta) (diff.DiffSummaryProgress, error) {
	// todo: use errgroup.Group
	ae := atomicerr.New()
	ch := make(chan diff.DiffSummaryProgress)
	go func() {
		defer close(ch)
		err := diff.SummaryForTableDelta(ctx, ch, td)

		ae.SetIfError(err)
	}()

	acc := diff.DiffSummaryProgress{}
	var count int64
	var pos int
	for p := range ch {
		if ae.IsSet() {
			break
		}

		acc.Adds += p.Adds
		acc.Removes += p.Removes
		acc.Changes += p.Changes
		acc.CellChanges += p.CellChanges
		acc.NewRowSize += p.NewRowSize
		acc.OldRowSize += p.OldRowSize
		acc.NewCellSize += p.NewCellSize
		acc.OldCellSize += p.OldCellSize

		count++
	}

	pos = cli.DeleteAndPrint(pos, "")

	if err := ae.Get(); err != nil {
		return diff.DiffSummaryProgress{}, err
	}

	_, err := td.IsKeyless(ctx)
	if err != nil {
		return diff.DiffSummaryProgress{}, nil
	}

	if (acc.Adds + acc.Removes + acc.Changes + (acc.OldCellSize - acc.NewCellSize)) == 0 {
		return diff.DiffSummaryProgress{}, fmt.Errorf("no data changes for '%s' table", td.ToName)
	}

	return acc, nil
}

//------------------------------------
// diffSummaryTableFunctionRowIter
//------------------------------------

var _ sql.RowIter = &diffSummaryTableFunctionRowIter{}

type diffSummaryTableFunctionRowIter struct {
	diffSums []diffSummaryNode
	diffIdx  int
}

func (d *diffSummaryTableFunctionRowIter) incrementIndexes() {
	d.diffIdx++
	if d.diffIdx >= len(d.diffSums) {
		d.diffIdx = -1
		d.diffSums = nil
	}
}

type diffSummaryNode struct {
	tblName     string
	diffSummary diff.DiffSummaryProgress
	oldColLen   int
	newColLen   int
}

func NewDiffSummaryTableFunctionRowIter(ds []diffSummaryNode) sql.RowIter {
	return &diffSummaryTableFunctionRowIter{
		diffSums: ds,
	}
}

func (d *diffSummaryTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	defer d.incrementIndexes()
	if d.diffSums == nil {
		return nil, io.EOF
	}

	ds := d.diffSums[d.diffIdx]
	return getRowFromDiffSummary(ds.tblName, ds.diffSummary, ds.oldColLen), nil
}

func (d *diffSummaryTableFunctionRowIter) Close(context *sql.Context) error {
	return nil
}

func getRowFromDiffSummary(tblName string, dsp diff.DiffSummaryProgress, colLen int) sql.Row {
	rowsModified := dsp.OldRowSize - dsp.Changes - dsp.Removes
	numCellInserts := float64(dsp.Adds) * float64(colLen)
	numCellDeletes := float64(dsp.Removes) * float64(colLen)
	if moreInserts := float64(dsp.NewCellSize) - float64(dsp.OldCellSize); moreInserts > 0 {
		numCellInserts = moreInserts + float64(numCellDeletes)
	}
	if moreDeletes := float64(dsp.OldCellSize) - float64(dsp.NewCellSize); moreDeletes > 0 {
		numCellDeletes = moreDeletes + float64(numCellInserts)
	}
	return sql.Row{
		tblName,         // table_name
		rowsModified,    // rows_unmodified
		dsp.Adds,        // rows_added
		dsp.Removes,     // rows_deleted
		dsp.Changes,     // row_modified
		numCellInserts,  // cells_added
		numCellDeletes,  // cells_deleted
		dsp.CellChanges, // cells_modified
		dsp.OldRowSize,  // old_row_count
		dsp.NewRowSize,  // new_row_count
		dsp.OldCellSize, // old_cell_count
		dsp.NewCellSize, // new_cell_count
	}
}
