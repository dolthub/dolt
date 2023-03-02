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
	"io"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
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
	&sql.Column{Name: "from_table_name", Type: types.LongText, Nullable: false},
	&sql.Column{Name: "to_table_name", Type: types.LongText, Nullable: false},
	&sql.Column{Name: "diff_type", Type: types.Text, Nullable: false},
	&sql.Column{Name: "data_change", Type: types.Boolean, Nullable: false},
	&sql.Column{Name: "schema_change", Type: types.Boolean, Nullable: false},
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
		if !types.IsText(ds.dotCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(ds.Name(), ds.dotCommitExpr.String())
		}
	} else {
		if !types.IsText(ds.fromCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(ds.Name(), ds.fromCommitExpr.String())
		}
		if !types.IsText(ds.toCommitExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(ds.Name(), ds.toCommitExpr.String())
		}
	}

	if ds.tableNameExpr != nil {
		if !types.IsText(ds.tableNameExpr.Type()) {
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

	sqledb, ok := ds.database.(SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unexpected database type: %T", ds.database)
	}

	fromDetails, toDetails, err := loadDetailsForRefs(ctx, fromCommitVal, toCommitVal, dotCommitVal, sqledb)
	if err != nil {
		return nil, err
	}

	deltas, err := diff.GetTableDeltas(ctx, fromDetails.root, toDetails.root)
	if err != nil {
		return nil, err
	}

	sort.Slice(deltas, func(i, j int) bool {
		return strings.Compare(deltas[i].ToName, deltas[j].ToName) < 0
	})

	// If tableNameExpr defined, return a single table diff summary result
	if ds.tableNameExpr != nil {
		delta := findMatchingDelta(deltas, tableName)

		summ, err := getSummaryForDelta(ctx, delta, sqledb, fromDetails, toDetails, true)
		if err != nil {
			return nil, err
		}

		summs := []*diff.TableDeltaSummary{}
		if summ != nil {
			summs = []*diff.TableDeltaSummary{summ}
		}

		return NewDiffSummaryTableFunctionRowIter(summs), nil
	}

	var diffSummaries []*diff.TableDeltaSummary
	for _, delta := range deltas {
		summ, err := getSummaryForDelta(ctx, delta, sqledb, fromDetails, toDetails, false)
		if err != nil {
			return nil, err
		}
		if summ != nil {
			diffSummaries = append(diffSummaries, summ)
		}
	}

	return NewDiffSummaryTableFunctionRowIter(diffSummaries), nil
}

func getSummaryForDelta(ctx *sql.Context, delta diff.TableDelta, sqledb SqlDatabase, fromDetails, toDetails *refDetails, shouldErrorOnPKChange bool) (*diff.TableDeltaSummary, error) {
	if delta.FromTable == nil && delta.ToTable == nil {
		return nil, nil
	}

	if !schema.ArePrimaryKeySetsDiffable(delta.Format(), delta.FromSch, delta.ToSch) {
		if shouldErrorOnPKChange {
			return nil, fmt.Errorf("failed to compute diff summary for table %s: %w", delta.CurName(), diff.ErrPrimaryKeySetChanged)
		}

		ctx.Warn(dtables.PrimaryKeyChangeWarningCode, fmt.Sprintf(dtables.PrimaryKeyChangeWarning, fromDetails.hashStr, toDetails.hashStr))
		return nil, nil
	}

	summ, err := delta.GetSummary(ctx)
	if err != nil {
		return nil, err
	}

	return summ, nil
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

//------------------------------------
// diffSummaryTableFunctionRowIter
//------------------------------------

var _ sql.RowIter = &diffSummaryTableFunctionRowIter{}

type diffSummaryTableFunctionRowIter struct {
	summaries []*diff.TableDeltaSummary
	diffIdx   int
}

func (d *diffSummaryTableFunctionRowIter) incrementIndexes() {
	d.diffIdx++
	if d.diffIdx >= len(d.summaries) {
		d.diffIdx = 0
		d.summaries = nil
	}
}

func NewDiffSummaryTableFunctionRowIter(ds []*diff.TableDeltaSummary) sql.RowIter {
	return &diffSummaryTableFunctionRowIter{
		summaries: ds,
	}
}

func (d *diffSummaryTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	defer d.incrementIndexes()
	if d.diffIdx >= len(d.summaries) {
		return nil, io.EOF
	}

	if d.summaries == nil {
		return nil, io.EOF
	}

	ds := d.summaries[d.diffIdx]
	return getRowFromSummary(ds), nil
}

func (d *diffSummaryTableFunctionRowIter) Close(context *sql.Context) error {
	return nil
}

func getRowFromSummary(ds *diff.TableDeltaSummary) sql.Row {
	return sql.Row{
		ds.FromTableName, // from_table_name
		ds.ToTableName,   // to_table_name
		ds.DiffType,      // diff_type
		ds.DataChange,    // data_change
		ds.SchemaChange,  // schema_change
	}
}
