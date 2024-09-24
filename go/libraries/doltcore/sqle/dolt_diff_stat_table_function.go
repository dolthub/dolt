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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
)

const diffStatDefaultRowCount = 10

var _ sql.TableFunction = (*DiffStatTableFunction)(nil)
var _ sql.ExecSourceRel = (*DiffStatTableFunction)(nil)

type DiffStatTableFunction struct {
	ctx *sql.Context

	fromCommitExpr sql.Expression
	toCommitExpr   sql.Expression
	dotCommitExpr  sql.Expression
	tableNameExpr  sql.Expression
	database       sql.Database
}

var diffStatTableSchema = sql.Schema{
	&sql.Column{Name: "table_name", Type: types.LongText, Nullable: false},
	&sql.Column{Name: "rows_unmodified", Type: types.Int64, Nullable: true},
	&sql.Column{Name: "rows_added", Type: types.Int64, Nullable: true},
	&sql.Column{Name: "rows_deleted", Type: types.Int64, Nullable: true},
	&sql.Column{Name: "rows_modified", Type: types.Int64, Nullable: true},
	&sql.Column{Name: "cells_added", Type: types.Int64, Nullable: true},
	&sql.Column{Name: "cells_deleted", Type: types.Int64, Nullable: true},
	&sql.Column{Name: "cells_modified", Type: types.Int64, Nullable: true},
	&sql.Column{Name: "old_row_count", Type: types.Int64, Nullable: true},
	&sql.Column{Name: "new_row_count", Type: types.Int64, Nullable: true},
	&sql.Column{Name: "old_cell_count", Type: types.Int64, Nullable: true},
	&sql.Column{Name: "new_cell_count", Type: types.Int64, Nullable: true},
}

// NewInstance creates a new instance of TableFunction interface
func (ds *DiffStatTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &DiffStatTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (ds *DiffStatTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(ds.Schema())
	numRows, _, err := ds.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (ds *DiffStatTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return diffStatDefaultRowCount, false, nil
}

// Database implements the sql.Databaser interface
func (ds *DiffStatTableFunction) Database() sql.Database {
	return ds.database
}

// WithDatabase implements the sql.Databaser interface
func (ds *DiffStatTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	nds := *ds
	nds.database = database
	return &nds, nil
}

// Name implements the sql.TableFunction interface
func (ds *DiffStatTableFunction) Name() string {
	return "dolt_diff_stat"
}

func (ds *DiffStatTableFunction) commitsResolved() bool {
	if ds.dotCommitExpr != nil {
		return ds.dotCommitExpr.Resolved()
	}
	return ds.fromCommitExpr.Resolved() && ds.toCommitExpr.Resolved()
}

// Resolved implements the sql.Resolvable interface
func (ds *DiffStatTableFunction) Resolved() bool {
	if ds.tableNameExpr != nil {
		return ds.commitsResolved() && ds.tableNameExpr.Resolved()
	}
	return ds.commitsResolved()
}

func (ds *DiffStatTableFunction) IsReadOnly() bool {
	return true
}

// String implements the Stringer interface
func (ds *DiffStatTableFunction) String() string {
	if ds.dotCommitExpr != nil {
		if ds.tableNameExpr != nil {
			return fmt.Sprintf("DOLT_DIFF_STAT(%s, %s)", ds.dotCommitExpr.String(), ds.tableNameExpr.String())
		}
		return fmt.Sprintf("DOLT_DIFF_STAT(%s)", ds.dotCommitExpr.String())
	}
	if ds.tableNameExpr != nil {
		return fmt.Sprintf("DOLT_DIFF_STAT(%s, %s, %s)", ds.fromCommitExpr.String(), ds.toCommitExpr.String(), ds.tableNameExpr.String())
	}
	return fmt.Sprintf("DOLT_DIFF_STAT(%s, %s)", ds.fromCommitExpr.String(), ds.toCommitExpr.String())
}

// Schema implements the sql.Node interface.
func (ds *DiffStatTableFunction) Schema() sql.Schema {
	return diffStatTableSchema
}

// Children implements the sql.Node interface.
func (ds *DiffStatTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (ds *DiffStatTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return ds, nil
}

// CheckPrivileges implements the interface sql.Node.
func (ds *DiffStatTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if ds.tableNameExpr != nil {
		if !types.IsText(ds.tableNameExpr.Type()) {
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

		subject := sql.PrivilegeCheckSubject{Database: ds.database.Name(), Table: tableName}

		// TODO: Add tests for privilege checking
		return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
	}

	tblNames, err := ds.database.GetTableNames(ctx)
	if err != nil {
		return false
	}

	operations := make([]sql.PrivilegedOperation, 0, len(tblNames))
	for _, tblName := range tblNames {
		subject := sql.PrivilegeCheckSubject{Database: ds.database.Name(), Table: tblName}
		operations = append(operations, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
	}

	return opChecker.UserHasPrivileges(ctx, operations...)
}

// Expressions implements the sql.Expressioner interface.
func (ds *DiffStatTableFunction) Expressions() []sql.Expression {
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
func (ds *DiffStatTableFunction) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) < 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(ds.Name(), "1 to 3", len(exprs))
	}

	for _, expr := range exprs {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(ds.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(ds.Name(), expr.String())
		}
	}

	newDstf := *ds
	if strings.Contains(exprs[0].String(), "..") {
		if len(exprs) < 1 || len(exprs) > 2 {
			return nil, sql.ErrInvalidArgumentNumber.New(newDstf.Name(), "1 or 2", len(exprs))
		}
		newDstf.dotCommitExpr = exprs[0]
		if len(exprs) == 2 {
			newDstf.tableNameExpr = exprs[1]
		}
	} else {
		if len(exprs) < 2 || len(exprs) > 3 {
			return nil, sql.ErrInvalidArgumentNumber.New(newDstf.Name(), "2 or 3", len(exprs))
		}
		newDstf.fromCommitExpr = exprs[0]
		newDstf.toCommitExpr = exprs[1]
		if len(exprs) == 3 {
			newDstf.tableNameExpr = exprs[2]
		}
	}

	// validate the expressions
	if newDstf.dotCommitExpr != nil {
		if !types.IsText(newDstf.dotCommitExpr.Type()) && !expression.IsBindVar(newDstf.dotCommitExpr) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.dotCommitExpr.String())
		}
	} else {
		if !types.IsText(newDstf.fromCommitExpr.Type()) && !expression.IsBindVar(newDstf.fromCommitExpr) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.fromCommitExpr.String())
		}
		if !types.IsText(newDstf.toCommitExpr.Type()) && !expression.IsBindVar(newDstf.toCommitExpr) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.toCommitExpr.String())
		}
	}

	if newDstf.tableNameExpr != nil {
		if !types.IsText(newDstf.tableNameExpr.Type()) && !expression.IsBindVar(newDstf.tableNameExpr) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.tableNameExpr.String())
		}
	}

	return &newDstf, nil
}

// RowIter implements the sql.Node interface
func (ds *DiffStatTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	fromCommitVal, toCommitVal, dotCommitVal, tableName, err := ds.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := ds.database.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", ds.database)
	}

	fromRefDetails, toRefDetails, err := loadDetailsForRefs(ctx, fromCommitVal, toCommitVal, dotCommitVal, sqledb)
	if err != nil {
		return nil, err
	}

	deltas, err := diff.GetTableDeltas(ctx, fromRefDetails.root, toRefDetails.root)
	if err != nil {
		return nil, err
	}

	// If tableNameExpr defined, return a single table diff stat result
	if ds.tableNameExpr != nil {
		delta := findMatchingDelta(deltas, tableName)
		schemaName := delta.FromName.Schema
		if schemaName == "" {
			schemaName = delta.ToName.Schema
		}
		diffStat, hasDiff, err := getDiffStatNodeFromDelta(ctx, delta, fromRefDetails.root, toRefDetails.root, doltdb.TableName{Name: tableName, Schema: schemaName})
		if err != nil {
			return nil, err
		}
		if !hasDiff {
			return NewDiffStatTableFunctionRowIter([]diffStatNode{}), nil
		}
		return NewDiffStatTableFunctionRowIter([]diffStatNode{diffStat}), nil
	}

	var diffStats []diffStatNode
	for _, delta := range deltas {
		tblName := delta.ToName
		if tblName.Name == "" {
			tblName = delta.FromName
		}
		diffStat, hasDiff, err := getDiffStatNodeFromDelta(ctx, delta, fromRefDetails.root, toRefDetails.root, tblName)
		if err != nil {
			if errors.Is(err, diff.ErrPrimaryKeySetChanged) {
				ctx.Warn(dtables.PrimaryKeyChangeWarningCode, fmt.Sprintf("stat for table %s cannot be determined. Primary key set changed.", tblName))
				// Report an empty diff for tables that have primary key set changes
				diffStats = append(diffStats, diffStatNode{tblName: tblName.Name})
				continue
			}
			return nil, err
		}
		if hasDiff {
			diffStats = append(diffStats, diffStat)
		}
	}

	return NewDiffStatTableFunctionRowIter(diffStats), nil
}

// evaluateArguments returns fromCommitVal, toCommitVal, dotCommitVal, and tableName.
// It evaluates the argument expressions to turn them into values this DiffStatTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (ds *DiffStatTableFunction) evaluateArguments() (interface{}, interface{}, interface{}, string, error) {
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

// getDiffStatNodeFromDelta returns diffStatNode object and whether there is data diff or not. It gets tables
// from roots and diff stat if there is a valid table exists in both fromRoot and toRoot.
func getDiffStatNodeFromDelta(ctx *sql.Context, delta diff.TableDelta, fromRoot, toRoot doltdb.RootValue, tableName doltdb.TableName) (diffStatNode, bool, error) {
	var oldColLen int
	var newColLen int
	fromTable, _, fromTableExists, err := doltdb.GetTableInsensitive(ctx, fromRoot, tableName)
	if err != nil {
		return diffStatNode{}, false, err
	}

	if fromTableExists {
		fromSch, err := fromTable.GetSchema(ctx)
		if err != nil {
			return diffStatNode{}, false, err
		}
		oldColLen = len(fromSch.GetAllCols().GetColumns())
	}

	toTable, _, toTableExists, err := doltdb.GetTableInsensitive(ctx, toRoot, tableName)
	if err != nil {
		return diffStatNode{}, false, err
	}

	if toTableExists {
		toSch, err := toTable.GetSchema(ctx)
		if err != nil {
			return diffStatNode{}, false, err
		}
		newColLen = len(toSch.GetAllCols().GetColumns())
	}

	if !fromTableExists && !toTableExists {
		return diffStatNode{}, false, sql.ErrTableNotFound.New(tableName)
	}

	// no diff from tableDelta
	if delta.FromTable == nil && delta.ToTable == nil {
		return diffStatNode{}, false, nil
	}

	diffStat, hasDiff, keyless, err := getDiffStat(ctx, delta)
	if err != nil {
		return diffStatNode{}, false, err
	}

	return diffStatNode{tableName.Name, diffStat, oldColLen, newColLen, keyless}, hasDiff, nil
}

// getDiffStat returns diff.DiffStatProgress object and whether there is a data diff or not.
func getDiffStat(ctx *sql.Context, td diff.TableDelta) (diff.DiffStatProgress, bool, bool, error) {
	// got this method from diff_output.go

	ch := make(chan diff.DiffStatProgress)

	grp, ctx2 := errgroup.WithContext(ctx)
	grp.Go(func() error {
		defer close(ch)
		err := diff.StatForTableDelta(ctx2, ch, td)
		return err
	})

	acc := diff.DiffStatProgress{}
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
		return diff.DiffStatProgress{}, false, false, err
	}

	keyless, err := td.IsKeyless(ctx)
	if err != nil {
		return diff.DiffStatProgress{}, false, keyless, err
	}

	if (acc.Adds+acc.Removes+acc.Changes) == 0 && (acc.OldCellSize-acc.NewCellSize) == 0 {
		return diff.DiffStatProgress{}, false, keyless, nil
	}

	return acc, true, keyless, nil
}

//------------------------------------
// diffStatTableFunctionRowIter
//------------------------------------

var _ sql.RowIter = &diffStatTableFunctionRowIter{}

type diffStatTableFunctionRowIter struct {
	diffStats []diffStatNode
	diffIdx   int
}

func (d *diffStatTableFunctionRowIter) incrementIndexes() {
	d.diffIdx++
	if d.diffIdx >= len(d.diffStats) {
		d.diffIdx = 0
		d.diffStats = nil
	}
}

type diffStatNode struct {
	tblName   string
	diffStat  diff.DiffStatProgress
	oldColLen int
	newColLen int
	keyless   bool
}

func NewDiffStatTableFunctionRowIter(ds []diffStatNode) sql.RowIter {
	return &diffStatTableFunctionRowIter{
		diffStats: ds,
	}
}

func (d *diffStatTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	defer d.incrementIndexes()
	if d.diffIdx >= len(d.diffStats) {
		return nil, io.EOF
	}

	if d.diffStats == nil {
		return nil, io.EOF
	}

	ds := d.diffStats[d.diffIdx]
	return getRowFromDiffStat(ds.tblName, ds.diffStat, ds.newColLen, ds.oldColLen, ds.keyless), nil
}

func (d *diffStatTableFunctionRowIter) Close(context *sql.Context) error {
	return nil
}

// getRowFromDiffStat takes diff.DiffStatProgress and calculates the row_modified, cell_added, cell_deleted.
// If the number of cell change from old to new cell count does not equal to cell_added and/or cell_deleted, there
// must be schema changes that affects cell_added and cell_deleted value addition to the row count * col length number.
func getRowFromDiffStat(tblName string, dsp diff.DiffStatProgress, newColLen, oldColLen int, keyless bool) sql.Row {
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

// GetCellsAddedAndDeleted calculates cells added and deleted given diff.DiffStatProgress and toCommit table
// column length. We use rows added and deleted to calculate cells added and deleted, but it does not include
// cells added and deleted from schema changes. Here we fill those in using total number of cells in each commit table.
func GetCellsAddedAndDeleted(acc diff.DiffStatProgress, newColLen int) (uint64, uint64) {
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
