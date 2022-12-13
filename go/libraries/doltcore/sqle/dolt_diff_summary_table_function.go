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
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
)

var _ sql.TableFunction = (*DiffSummaryTableFunction)(nil)

type DiffSummaryTableFunction struct {
	ctx *sql.Context

	fromCommitExpr sql.Expression
	toCommitExpr   sql.Expression
	dotCommitExpr  sql.Expression
	tableNameExpr  sql.Expression
	database       sql.Database
}

var diffSummaryTableSchema = sql.Schema{
	&sql.Column{Name: "table_name", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "rows_unmodified", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "rows_added", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "rows_deleted", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "rows_modified", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "cells_added", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "cells_deleted", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "cells_modified", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "old_row_count", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "new_row_count", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "old_cell_count", Type: sql.Int64, Nullable: true},
	&sql.Column{Name: "new_cell_count", Type: sql.Int64, Nullable: true},
}

// NewInstance creates a new instance of TableFunction interface
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

// Database implements the sql.Databaser interface
func (ds *DiffSummaryTableFunction) Database() sql.Database {
	return ds.database
}

// WithDatabase implements the sql.Databaser interface
func (ds *DiffSummaryTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	ds.database = database
	return ds, nil
}

// Name implements the sql.TableFunction interface
func (ds *DiffSummaryTableFunction) Name() string {
	return "dolt_diff_summary"
}

func (ds *DiffSummaryTableFunction) commitsResolved() bool {
	if ds.dotCommitExpr != nil {
		return ds.dotCommitExpr.Resolved()
	}
	return ds.fromCommitExpr.Resolved() && ds.toCommitExpr.Resolved()
}

// Resolved implements the sql.Resolvable interface
func (ds *DiffSummaryTableFunction) Resolved() bool {
	if ds.tableNameExpr != nil {
		return ds.commitsResolved() && ds.tableNameExpr.Resolved()
	}
	return ds.commitsResolved()
}

// String implements the Stringer interface
func (ds *DiffSummaryTableFunction) String() string {
	if ds.dotCommitExpr != nil {
		if ds.tableNameExpr != nil {
			return fmt.Sprintf("DOLT_DIFF_SUMMARY(%s, %s)", ds.dotCommitExpr.String(), ds.tableNameExpr.String())
		}
		return fmt.Sprintf("DOLT_DIFF_SUMMARY(%s)", ds.dotCommitExpr.String())
	}
	if ds.tableNameExpr != nil {
		return fmt.Sprintf("DOLT_DIFF_SUMMARY(%s, %s, %s)", ds.fromCommitExpr.String(), ds.toCommitExpr.String(), ds.tableNameExpr.String())
	}
	return fmt.Sprintf("DOLT_DIFF_SUMMARY(%s, %s)", ds.fromCommitExpr.String(), ds.toCommitExpr.String())
}

// Schema implements the sql.Node interface.
func (ds *DiffSummaryTableFunction) Schema() sql.Schema {
	return diffSummaryTableSchema
}

// Children implements the sql.Node interface.
func (ds *DiffSummaryTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (ds *DiffSummaryTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return ds, nil
}

// CheckPrivileges implements the interface sql.Node.
func (ds *DiffSummaryTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if ds.tableNameExpr != nil {
		if !sql.IsText(ds.tableNameExpr.Type()) {
			return false
		}

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

// Expressions implements the sql.Expressioner interface.
func (ds *DiffSummaryTableFunction) Expressions() []sql.Expression {
	exprs := []sql.Expression{}
	if ds.dotCommitExpr != nil {
		exprs = append(exprs, ds.dotCommitExpr)
	} else {
		exprs = append(exprs, ds.fromCommitExpr, ds.toCommitExpr)
	}
	if ds.tableNameExpr != nil {
		exprs = append(exprs, ds.tableNameExpr)
	}
	return exprs
}

// WithExpressions implements the sql.Expressioner interface.
func (ds *DiffSummaryTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) < 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(ds.Name(), "1 to 3", len(expression))
	}

	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(ds.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(ds.Name(), expr.String())
		}
	}

	if strings.Contains(expression[0].String(), "..") {
		if len(expression) < 1 || len(expression) > 2 {
			return nil, sql.ErrInvalidArgumentNumber.New(ds.Name(), "1 or 2", len(expression))
		}
		ds.dotCommitExpr = expression[0]
		if len(expression) == 2 {
			ds.tableNameExpr = expression[1]
		}
	} else {
		if len(expression) < 2 || len(expression) > 3 {
			return nil, sql.ErrInvalidArgumentNumber.New(ds.Name(), "2 or 3", len(expression))
		}
		ds.fromCommitExpr = expression[0]
		ds.toCommitExpr = expression[1]
		if len(expression) == 3 {
			ds.tableNameExpr = expression[2]
		}
	}

	// validate the expressions
	if ds.dotCommitExpr != nil {
		if !sql.IsText(ds.dotCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(ds.Name(), ds.dotCommitExpr.String())
		}
	} else {
		if !sql.IsText(ds.fromCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(ds.Name(), ds.fromCommitExpr.String())
		}
		if !sql.IsText(ds.toCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(ds.Name(), ds.toCommitExpr.String())
		}
	}

	if ds.tableNameExpr != nil {
		if !sql.IsText(ds.tableNameExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(ds.Name(), ds.tableNameExpr.String())
		}
	}

	return ds, nil
}

// RowIter implements the sql.Node interface
func (ds *DiffSummaryTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	fromCommitVal, toCommitVal, dotCommitVal, tableName, err := ds.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := ds.database.(Database)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", ds.database)
	}

	fromCommitStr, toCommitStr, err := loadCommitStrings(ctx, fromCommitVal, toCommitVal, dotCommitVal, sqledb)
	if err != nil {
		return nil, err
	}

	sess := dsess.DSessFromSess(ctx.Session)
	fromRoot, _, err := sess.ResolveRootForRef(ctx, sqledb.Name(), fromCommitStr)
	if err != nil {
		return nil, err
	}

	toRoot, _, err := sess.ResolveRootForRef(ctx, sqledb.Name(), toCommitStr)
	if err != nil {
		return nil, err
	}

	deltas, err := diff.GetTableDeltas(ctx, fromRoot, toRoot)
	if err != nil {
		return nil, err
	}

	// If tableNameExpr defined, return a single table diff summary result
	if ds.tableNameExpr != nil {
		delta := findMatchingDelta(deltas, tableName)
		diffSum, hasDiff, err := getDiffSummaryNodeFromDelta(ctx, delta, fromRoot, toRoot, tableName)
		if err != nil {
			return nil, err
		}
		if !hasDiff {
			return NewDiffSummaryTableFunctionRowIter([]diffSummaryNode{}), nil
		}
		return NewDiffSummaryTableFunctionRowIter([]diffSummaryNode{diffSum}), nil
	}

	var diffSummaries []diffSummaryNode
	for _, delta := range deltas {
		tblName := delta.ToName
		if tblName == "" {
			tblName = delta.FromName
		}
		diffSum, hasDiff, err := getDiffSummaryNodeFromDelta(ctx, delta, fromRoot, toRoot, tblName)
		if err != nil {
			if errors.Is(err, diff.ErrPrimaryKeySetChanged) {
				ctx.Warn(dtables.PrimaryKeyChangeWarningCode, fmt.Sprintf("summary for table %s cannot be determined. Primary key set changed.", tblName))
				// Report an empty diff for tables that have primary key set changes
				diffSummaries = append(diffSummaries, diffSummaryNode{tblName: tblName})
				continue
			}
			return nil, err
		}
		if hasDiff {
			diffSummaries = append(diffSummaries, diffSum)
		}
	}

	return NewDiffSummaryTableFunctionRowIter(diffSummaries), nil
}

// evaluateArguments returns fromCommitVal, toCommitVal, dotCommitVal, and tableName.
// It evaluates the argument expressions to turn them into values this DiffSummaryTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (ds *DiffSummaryTableFunction) evaluateArguments() (interface{}, interface{}, interface{}, string, error) {
	var tableName string
	if ds.tableNameExpr != nil {
		tableNameVal, err := ds.tableNameExpr.Eval(ds.ctx, nil)
		if err != nil {
			return nil, nil, nil, "", err
		}
		tn, ok := tableNameVal.(string)
		if !ok {
			return nil, nil, nil, "", ErrInvalidTableName.New(ds.tableNameExpr.String())
		}
		tableName = tn
	}

	if ds.dotCommitExpr != nil {
		dotCommitVal, err := ds.dotCommitExpr.Eval(ds.ctx, nil)
		if err != nil {
			return nil, nil, nil, "", err
		}

		return nil, nil, dotCommitVal, tableName, nil
	}

	fromCommitVal, err := ds.fromCommitExpr.Eval(ds.ctx, nil)
	if err != nil {
		return nil, nil, nil, "", err
	}

	toCommitVal, err := ds.toCommitExpr.Eval(ds.ctx, nil)
	if err != nil {
		return nil, nil, nil, "", err
	}

	return fromCommitVal, toCommitVal, nil, tableName, nil
}

// getDiffSummaryNodeFromDelta returns diffSummaryNode object and whether there is data diff or not. It gets tables
// from roots and diff summary if there is a valid table exists in both fromRoot and toRoot.
func getDiffSummaryNodeFromDelta(ctx *sql.Context, delta diff.TableDelta, fromRoot, toRoot *doltdb.RootValue, tableName string) (diffSummaryNode, bool, error) {
	var oldColLen int
	var newColLen int
	fromTable, _, fromTableExists, err := fromRoot.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return diffSummaryNode{}, false, err
	}

	if fromTableExists {
		fromSch, err := fromTable.GetSchema(ctx)
		if err != nil {
			return diffSummaryNode{}, false, err
		}
		oldColLen = len(fromSch.GetAllCols().GetColumns())
	}

	toTable, _, toTableExists, err := toRoot.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return diffSummaryNode{}, false, err
	}

	if toTableExists {
		toSch, err := toTable.GetSchema(ctx)
		if err != nil {
			return diffSummaryNode{}, false, err
		}
		newColLen = len(toSch.GetAllCols().GetColumns())
	}

	if !fromTableExists && !toTableExists {
		return diffSummaryNode{}, false, sql.ErrTableNotFound.New(tableName)
	}

	// no diff from tableDelta
	if delta.FromTable == nil && delta.ToTable == nil {
		return diffSummaryNode{}, false, nil
	}

	diffSum, hasDiff, keyless, err := getDiffSummary(ctx, delta)
	if err != nil {
		return diffSummaryNode{}, false, err
	}

	return diffSummaryNode{tableName, diffSum, oldColLen, newColLen, keyless}, hasDiff, nil
}

// getDiffSummary returns diff.DiffSummaryProgress object and whether there is a data diff or not.
func getDiffSummary(ctx *sql.Context, td diff.TableDelta) (diff.DiffSummaryProgress, bool, bool, error) {
	// got this method from diff_output.go

	ch := make(chan diff.DiffSummaryProgress)

	grp, ctx2 := errgroup.WithContext(ctx)
	grp.Go(func() error {
		defer close(ch)
		err := diff.SummaryForTableDelta(ctx2, ch, td)
		return err
	})

	acc := diff.DiffSummaryProgress{}
	var count int64
	grp.Go(func() error {
		for {
			select {
			case p, ok := <-ch:
				if !ok {
					return nil
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
			case <-ctx2.Done():
				return ctx2.Err()
			}
		}
	})

	if err := grp.Wait(); err != nil {
		return diff.DiffSummaryProgress{}, false, false, err
	}

	keyless, err := td.IsKeyless(ctx)
	if err != nil {
		return diff.DiffSummaryProgress{}, false, keyless, err
	}

	if (acc.Adds+acc.Removes+acc.Changes) == 0 && (acc.OldCellSize-acc.NewCellSize) == 0 {
		return diff.DiffSummaryProgress{}, false, keyless, nil
	}

	return acc, true, keyless, nil
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
		d.diffIdx = 0
		d.diffSums = nil
	}
}

type diffSummaryNode struct {
	tblName     string
	diffSummary diff.DiffSummaryProgress
	oldColLen   int
	newColLen   int
	keyless     bool
}

func NewDiffSummaryTableFunctionRowIter(ds []diffSummaryNode) sql.RowIter {
	return &diffSummaryTableFunctionRowIter{
		diffSums: ds,
	}
}

func (d *diffSummaryTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	defer d.incrementIndexes()
	if d.diffIdx >= len(d.diffSums) {
		return nil, io.EOF
	}

	if d.diffSums == nil {
		return nil, io.EOF
	}

	ds := d.diffSums[d.diffIdx]
	return getRowFromDiffSummary(ds.tblName, ds.diffSummary, ds.newColLen, ds.oldColLen, ds.keyless), nil
}

func (d *diffSummaryTableFunctionRowIter) Close(context *sql.Context) error {
	return nil
}

// getRowFromDiffSummary takes diff.DiffSummaryProgress and calculates the row_modified, cell_added, cell_deleted.
// If the number of cell change from old to new cell count does not equal to cell_added and/or cell_deleted, there
// must be schema changes that affects cell_added and cell_deleted value addition to the row count * col length number.
func getRowFromDiffSummary(tblName string, dsp diff.DiffSummaryProgress, newColLen, oldColLen int, keyless bool) sql.Row {
	// if table is keyless table, match current CLI command result
	if keyless {
		return sql.Row{
			tblName,            // table_name
			nil,                // rows_unmodified
			int64(dsp.Adds),    // rows_added
			int64(dsp.Removes), // rows_deleted
			nil,                // rows_modified
			nil,                // cells_added
			nil,                // cells_deleted
			nil,                // cells_modified
			nil,                // old_row_count
			nil,                // new_row_count
			nil,                // old_cell_count
			nil,                // new_cell_count
		}
	}

	numCellInserts, numCellDeletes := GetCellsAddedAndDeleted(dsp, newColLen)
	rowsUnmodified := dsp.OldRowSize - dsp.Changes - dsp.Removes

	return sql.Row{
		tblName,                // table_name
		int64(rowsUnmodified),  // rows_unmodified
		int64(dsp.Adds),        // rows_added
		int64(dsp.Removes),     // rows_deleted
		int64(dsp.Changes),     // rows_modified
		int64(numCellInserts),  // cells_added
		int64(numCellDeletes),  // cells_deleted
		int64(dsp.CellChanges), // cells_modified
		int64(dsp.OldRowSize),  // old_row_count
		int64(dsp.NewRowSize),  // new_row_count
		int64(dsp.OldCellSize), // old_cell_count
		int64(dsp.NewCellSize), // new_cell_count
	}
}

// GetCellsAddedAndDeleted calculates cells added and deleted given diff.DiffSummaryProgress and toCommit table
// column length. We use rows added and deleted to calculate cells added and deleted, but it does not include
// cells added and deleted from schema changes. Here we fill those in using total number of cells in each commit table.
func GetCellsAddedAndDeleted(acc diff.DiffSummaryProgress, newColLen int) (uint64, uint64) {
	var numCellInserts, numCellDeletes float64
	rowToCellInserts := float64(acc.Adds) * float64(newColLen)
	rowToCellDeletes := float64(acc.Removes) * float64(newColLen)
	cellDiff := float64(acc.NewCellSize) - float64(acc.OldCellSize)
	if cellDiff > 0 {
		numCellInserts = cellDiff + rowToCellDeletes
		numCellDeletes = rowToCellDeletes
	} else if cellDiff < 0 {
		numCellInserts = rowToCellInserts
		numCellDeletes = math.Abs(cellDiff) + rowToCellInserts
	} else {
		if rowToCellInserts != rowToCellDeletes {
			numCellDeletes = math.Max(rowToCellDeletes, rowToCellInserts)
			numCellInserts = math.Max(rowToCellDeletes, rowToCellInserts)
		} else {
			numCellDeletes = rowToCellDeletes
			numCellInserts = rowToCellInserts
		}
	}
	return uint64(numCellInserts), uint64(numCellDeletes)
}
