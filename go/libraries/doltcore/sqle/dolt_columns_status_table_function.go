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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var _ sql.TableFunction = (*ColumnsStatusTableFunction)(nil)
var _ sql.ExecSourceRel = (*ColumnsStatusTableFunction)(nil)

type ColumnsStatusTableFunction struct {
	ctx *sql.Context

	tableNameExpr sql.Expression
	refExpr       sql.Expression
	database      sql.Database
}

var columnStatusTableSchema = sql.Schema{
	&sql.Column{Name: "name", Type: types.LongText, Nullable: false},          // 0
	&sql.Column{Name: "tag", Type: types.Uint64, Nullable: false},             // 1
	&sql.Column{Name: "is_part_of_pk", Type: types.Boolean, Nullable: false},  // 2
	&sql.Column{Name: "default", Type: types.Text, Nullable: false},           // 3
	&sql.Column{Name: "auto_increment", Type: types.Boolean, Nullable: false}, // 4
	&sql.Column{Name: "comment", Type: types.Text, Nullable: false},           // 5
}

// NewInstance creates a new instance of TableFunction interface
func (ds *ColumnsStatusTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &ColumnsStatusTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	ds = node.(*ColumnsStatusTableFunction)

	return ds, nil
}

// Database implements the sql.Databaser interface
func (ds *ColumnsStatusTableFunction) Database() sql.Database {
	return ds.database
}

// WithDatabase implements the sql.Databaser interface
func (ds *ColumnsStatusTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	nds := *ds
	nds.database = database
	return &nds, nil
}

// Name implements the sql.TableFunction interface
func (ds *ColumnsStatusTableFunction) Name() string {
	return "dolt_columns_status"
}

// Resolved implements the sql.Resolvable interface
func (ds *ColumnsStatusTableFunction) Resolved() bool {
	if ds.refExpr == nil {
		return ds.tableNameExpr.Resolved()
	}
	return ds.tableNameExpr.Resolved() && ds.refExpr.Resolved()
}

// String implements the Stringer interface
func (ds *ColumnsStatusTableFunction) String() string {
	if ds.refExpr != nil {
		return fmt.Sprintf("DOLT_COLUMN_STATUS(%s, %s)", ds.tableNameExpr.String(), ds.refExpr.String())
	}
	return fmt.Sprintf("DOLT_COLUMN_STATUS(%s)", ds.tableNameExpr.String())
}

// Schema implements the sql.Node interface.
func (ds *ColumnsStatusTableFunction) Schema() sql.Schema {
	return columnStatusTableSchema
}

// Children implements the sql.Node interface.
func (ds *ColumnsStatusTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (ds *ColumnsStatusTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return ds, nil
}

// CheckPrivileges implements the interface sql.Node.
func (ds *ColumnsStatusTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	if !types.IsText(ds.tableNameExpr.Type()) {
		return false
	}

	tableName, _, err := ds.evaluateArguments()
	if err != nil {
		return false
	}

	// TODO: Add tests for privilege checking
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(ds.database.Name(), tableName, "", sql.PrivilegeType_Select))
}

// Expressions implements the sql.Expressioner interface.
func (ds *ColumnsStatusTableFunction) Expressions() []sql.Expression {
	exprs := []sql.Expression{
		ds.tableNameExpr,
	}
	if ds.refExpr != nil {
		exprs = append(exprs, ds.refExpr)
	}
	return exprs
}

// WithExpressions implements the sql.Expressioner interface.
func (ds *ColumnsStatusTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) < 1 || len(expression) > 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(ds.Name(), "1 or 2", len(expression))
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

	newDstf := *ds
	newDstf.tableNameExpr = expression[0]
	if len(expression) == 2 {
		newDstf.refExpr = expression[1]
	}

	// validate the expressions
	if !types.IsText(newDstf.tableNameExpr.Type()) {
		return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.tableNameExpr.String())
	}
	if newDstf.refExpr != nil {
		if !types.IsText(newDstf.refExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.refExpr.String())
		}
	}

	return &newDstf, nil
}

// RowIter implements the sql.Node interface
func (ds *ColumnsStatusTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	tableName, ref, err := ds.evaluateArguments()
	if err != nil {
		return nil, err
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbName := ds.database.Name()

	var root *doltdb.RootValue
	if ref == "" {
		sqledb, ok := ds.database.(dsess.SqlDatabase)
		if !ok {
			return nil, fmt.Errorf("unexpected database type: %T", ds.database)
		}

		root, err = sqledb.GetRoot(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to get root: %w", err)
		}
	} else {
		root, _, _, err = sess.ResolveRootForRef(ctx, dbName, ref)
		if err != nil {
			return nil, fmt.Errorf("unable to resolve ref: %w", err)
		}
	}

	table, _, err := root.GetTable(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("unable to get table %s: %w", tableName, err)
	}

	if table == nil {
		// TODO: throw error or return empty set?

		//return nil, sql.ErrTableNotFound.New(tableName)

		iter := &columnStatusTableFunctionRowIter{
			index:   0,
			columns: []schema.Column{},
		}
		return iter, nil
	} else {
		sch, err := table.GetSchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to get schema for table %s: %w", tableName, err)
		}

		allCols := sch.GetAllCols()
		columns := []schema.Column{}
		err = allCols.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			columns = append(columns, col)
			return false, nil
		})
		if err != nil {
			return nil, fmt.Errorf("error iterating table %s columns: %w", tableName, err)
		}

		iter := &columnStatusTableFunctionRowIter{
			index:   0,
			columns: columns,
		}

		return iter, nil
	}
}

// evaluateArguments returns tableName and columnName.
// It evaluates the argument expressions to turn them into values this DiffSummaryTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (ds *ColumnsStatusTableFunction) evaluateArguments() (tableName string, ref string, err error) {
	ref = ""

	tableNameVal, err := ds.tableNameExpr.Eval(ds.ctx, nil)
	if err != nil {
		return "", "", err
	}
	tn, ok := tableNameVal.(string)
	if !ok {
		return "", "", ErrInvalidTableName.New(ds.tableNameExpr.String())
	}
	tableName = tn

	if ds.refExpr != nil {
		refVal, err := ds.refExpr.Eval(ds.ctx, nil)
		if err != nil {
			return "", "", err
		}
		r, ok := refVal.(string)
		if !ok {
			return "", "", fmt.Errorf("received '%v' when expecting reference string", refVal)
		}
		ref = r
	}

	return tableName, ref, nil
}

type columnStatusTableFunctionRowIter struct {
	columns []schema.Column
	index   int
}

func (s *columnStatusTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if s.index >= len(s.columns) {
		return nil, io.EOF
	} else {
		col := s.columns[s.index]
		s.index++
		return sql.NewRow(
			col.Name,          // 0
			col.Tag,           // 1
			col.IsPartOfPK,    // 2
			col.Default,       // 3
			col.AutoIncrement, // 4
			col.Comment,       // 5
		), nil
	}
}

func (s *columnStatusTableFunctionRowIter) Close(context *sql.Context) error {
	return nil
}

var _ sql.RowIter = (*columnStatusTableFunctionRowIter)(nil)
