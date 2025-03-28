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

package dtablefunctions

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
)

const diffSummaryDefaultRowCount = 10

var _ sql.TableFunction = (*DiffSummaryTableFunction)(nil)
var _ sql.ExecSourceRel = (*DiffSummaryTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*DiffSummaryTableFunction)(nil)

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

func (ds *DiffSummaryTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(ds.Schema())
	numRows, _, err := ds.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (ds *DiffSummaryTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return diffSummaryDefaultRowCount, false, nil
}

// Database implements the sql.Databaser interface
func (ds *DiffSummaryTableFunction) Database() sql.Database {
	return ds.database
}

// WithDatabase implements the sql.Databaser interface
func (ds *DiffSummaryTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	nds := *ds
	nds.database = database
	return &nds, nil
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

func (ds *DiffSummaryTableFunction) IsReadOnly() bool {
	return true
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

// CheckAuth implements the interface sql.AuthorizationCheckerNode.
func (ds *DiffSummaryTableFunction) CheckAuth(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if ds.tableNameExpr != nil {
		if !types.IsText(ds.tableNameExpr.Type()) {
			return ExpressionIsDeferred(ds.tableNameExpr)
		}

		tableNameVal, err := ds.tableNameExpr.Eval(ds.ctx, nil)
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

		subject := sql.PrivilegeCheckSubject{Database: ds.database.Name(), Table: tableName}
		// TODO: Add tests for privilege checking
		return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
	}

	tblNames, err := ds.database.GetTableNames(ctx)
	if err != nil {
		return false
	}

	var operations []sql.PrivilegedOperation
	for _, tblName := range tblNames {
		subject := sql.PrivilegeCheckSubject{Database: ds.database.Name(), Table: tblName}
		operations = append(operations, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
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
func (ds *DiffSummaryTableFunction) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
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
func (ds *DiffSummaryTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	fromCommitVal, toCommitVal, dotCommitVal, tableName, err := ds.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sqledb, ok := ds.database.(dsess.SqlDatabase)
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
		return deltas[i].ToName.Less(deltas[j].ToName)
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

func getSummaryForDelta(ctx *sql.Context, delta diff.TableDelta, sqledb dsess.SqlDatabase, fromDetails, toDetails *refDetails, shouldErrorOnPKChange bool) (*diff.TableDeltaSummary, error) {
	if delta.FromTable == nil && delta.ToTable == nil && delta.FromRootObject == nil && delta.ToRootObject == nil {
		if !strings.HasPrefix(delta.FromName.Name, diff.DBPrefix) && !strings.HasPrefix(delta.ToName.Name, diff.DBPrefix) {
			return nil, nil
		}
		summ, err := delta.GetSummary(ctx)
		if err != nil {
			return nil, err
		}
		return summ, nil
	}

	if !schema.ArePrimaryKeySetsDiffable(delta.Format(), delta.FromSch, delta.ToSch) {
		if shouldErrorOnPKChange {
			return nil, fmt.Errorf("failed to compute diff summary for table %s: %w", delta.CurName(), diff.ErrPrimaryKeySetChanged)
		}

		ctx.Warn(dtables.PrimaryKeyChangeWarningCode, dtables.PrimaryKeyChangeWarning, fromDetails.hashStr, toDetails.hashStr)
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
		ds.FromTableName.String(), // from_table_name
		ds.ToTableName.String(),   // to_table_name
		ds.DiffType,               // diff_type
		ds.DataChange,             // data_change
		ds.SchemaChange,           // schema_change
	}
}
