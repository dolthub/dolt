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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var _ sql.TableFunction = (*ForeignKeyStatusTableFunction)(nil)
var _ sql.ExecSourceRel = (*ForeignKeyStatusTableFunction)(nil)

type ForeignKeyStatusTableFunction struct {
	ctx *sql.Context

	refExpr  sql.Expression
	database sql.Database
}

var foreignKeyStatusTableSchema = sql.Schema{
	&sql.Column{Name: "name", Type: types.LongText, Nullable: false},                          // 0
	&sql.Column{Name: "is_resolved", Type: types.Boolean, Nullable: false},                    // 1
	&sql.Column{Name: "table_name", Type: types.Text, Nullable: false},                        // 2
	&sql.Column{Name: "table_index", Type: types.Text, Nullable: false},                       // 3
	&sql.Column{Name: "table_column", Type: types.Uint64, Nullable: false},                    // 4
	&sql.Column{Name: "referenced_table_name", Type: types.Text, Nullable: false},             // 5
	&sql.Column{Name: "referenced_table_index", Type: types.Text, Nullable: false},            // 6
	&sql.Column{Name: "referenced_table_column", Type: types.Uint64, Nullable: false},         // 7
	&sql.Column{Name: "on_update", Type: types.Text, Nullable: false},                         // 8
	&sql.Column{Name: "on_delete", Type: types.Text, Nullable: false},                         // 9
	&sql.Column{Name: "unresolved_table_column", Type: types.Text, Nullable: false},           // 10
	&sql.Column{Name: "unresolved_reference_table_column", Type: types.Text, Nullable: false}, // 11
}

// NewInstance creates a new instance of TableFunction interface
func (ds *ForeignKeyStatusTableFunction) NewInstance(ctx *sql.Context, db sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &ForeignKeyStatusTableFunction{
		ctx:      ctx,
		database: db,
	}

	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}

	ds = node.(*ForeignKeyStatusTableFunction)

	return ds, nil
}

// Database implements the sql.Databaser interface
func (ds *ForeignKeyStatusTableFunction) Database() sql.Database {
	return ds.database
}

// WithDatabase implements the sql.Databaser interface
func (ds *ForeignKeyStatusTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	nds := *ds
	nds.database = database
	return &nds, nil
}

// Name implements the sql.TableFunction interface
func (ds *ForeignKeyStatusTableFunction) Name() string {
	return "dolt_foreign_key_status"
}

// Resolved implements the sql.Resolvable interface
func (ds *ForeignKeyStatusTableFunction) Resolved() bool {
	if ds.refExpr == nil {
		return true
	}
	return ds.refExpr.Resolved()
}

// String implements the Stringer interface
func (ds *ForeignKeyStatusTableFunction) String() string {
	if ds.refExpr == nil {
		return "DOLT_FOREIGN_KEY_STATUS()"
	}
	return fmt.Sprintf("DOLT_FOREIGN_KEY_STATUS(%s)", ds.refExpr.String())
}

// Schema implements the sql.Node interface.
func (ds *ForeignKeyStatusTableFunction) Schema() sql.Schema {
	return foreignKeyStatusTableSchema
}

// Children implements the sql.Node interface.
func (ds *ForeignKeyStatusTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (ds *ForeignKeyStatusTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return ds, nil
}

// CheckPrivileges implements the interface sql.Node.
func (ds *ForeignKeyStatusTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// Expressions implements the sql.Expressioner interface.
func (ds *ForeignKeyStatusTableFunction) Expressions() []sql.Expression {
	exprs := []sql.Expression{}
	if ds.refExpr != nil {
		exprs = append(exprs, ds.refExpr)
	}
	return exprs
}

// WithExpressions implements the sql.Expressioner interface.
func (ds *ForeignKeyStatusTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) > 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(ds.Name(), "0 or 1", len(expression))
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
	if len(expression) == 1 {
		newDstf.refExpr = expression[0]
	}

	// validate the expressions
	if newDstf.refExpr != nil {
		if !types.IsText(newDstf.refExpr.Type()) {
			return nil, sql.ErrInvalidArgumentDetails.New(newDstf.Name(), newDstf.refExpr.String())
		}
	}

	return &newDstf, nil
}

// RowIter implements the sql.Node interface
func (ds *ForeignKeyStatusTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	ref, err := ds.evaluateArguments()
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

	fkColl, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get foreign key collection: %w", err)
	}

	fkRows := []sql.Row{}
	err = fkColl.Iter(func(fk doltdb.ForeignKey) (stop bool, err error) {
		rows := getForeignKeyRows(fk)
		fkRows = append(fkRows, rows...)
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to iterate foreign keys: %w", err)
	}

	iter := &foreignKeyStatusTableFunctionRowIter{
		fkRows: fkRows,
		index:  0,
	}
	return iter, nil
}

func getForeignKeyRows(fk doltdb.ForeignKey) []sql.Row {
	rows := []sql.Row{}

	baseRow := sql.NewRow(
		fk.Name,                 // 0
		fk.IsResolved(),         // 1
		fk.TableName,            // 2
		fk.TableIndex,           // 3
		-1,                      // 4
		fk.ReferencedTableName,  // 5
		fk.ReferencedTableIndex, // 6
		-1,                      // 7
		fk.OnUpdate.String(),    // 8
		fk.OnDelete.String(),    // 9
		"",                      // 10
		"",                      // 11
	)

	for _, colTag := range fk.TableColumns {
		row1 := baseRow.Copy()
		row1[4] = colTag

		for _, refColTag := range fk.ReferencedTableColumns {
			row2 := row1.Copy()
			row2[7] = refColTag

			for _, utc := range fk.UnresolvedFKDetails.TableColumns {
				row3 := row2.Copy()
				row3[10] = utc

				for _, urtc := range fk.UnresolvedFKDetails.ReferencedTableColumns {
					row4 := row3.Copy()
					row4[11] = urtc

					rows = append(rows, row4)
				}
			}
		}
	}

	return rows
}

// evaluateArguments returns ref.
// It evaluates the argument expressions to turn them into values this DiffSummaryTableFunction
// can use. Note that this method only evals the expressions, and doesn't validate the values.
func (ds *ForeignKeyStatusTableFunction) evaluateArguments() (ref string, err error) {
	ref = ""

	if ds.refExpr != nil {
		refVal, err := ds.refExpr.Eval(ds.ctx, nil)
		if err != nil {
			return "", err
		}
		r, ok := refVal.(string)
		if !ok {
			return "", fmt.Errorf("received '%v' when expecting reference string", refVal)
		}
		ref = r
	}

	return ref, nil
}

type foreignKeyStatusTableFunctionRowIter struct {
	fkRows []sql.Row
	index  int
}

func (s *foreignKeyStatusTableFunctionRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if s.index >= len(s.fkRows) {
		return nil, io.EOF
	}
	row := s.fkRows[s.index]
	s.index++
	return row, nil
}

func (s *foreignKeyStatusTableFunctionRowIter) Close(context *sql.Context) error {
	return nil
}

var _ sql.RowIter = (*foreignKeyStatusTableFunctionRowIter)(nil)
