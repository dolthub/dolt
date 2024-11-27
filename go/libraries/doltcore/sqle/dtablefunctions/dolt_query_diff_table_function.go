// Copyright 2020-2021 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
)

const queryDiffDefaultRowCount = 100

var _ sql.TableFunction = (*QueryDiffTableFunction)(nil)
var _ sql.CatalogTableFunction = (*QueryDiffTableFunction)(nil)
var _ sql.ExecSourceRel = (*QueryDiffTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*QueryDiffTableFunction)(nil)

type QueryDiffTableFunction struct {
	ctx      *sql.Context
	database sql.Database
	sqlSch   sql.Schema
	catalog  sql.Catalog
	query1   sql.Expression
	query2   sql.Expression

	engine   *gms.Engine
	rowIter1 sql.RowIter
	rowIter2 sql.RowIter
	schema1  sql.Schema
	schema2  sql.Schema
}

// NewInstance creates a new instance of TableFunction interface
func (tf *QueryDiffTableFunction) NewInstance(ctx *sql.Context, database sql.Database, expressions []sql.Expression) (sql.Node, error) {
	newInstance := &QueryDiffTableFunction{
		ctx:      ctx,
		database: database,
	}
	node, err := newInstance.WithExpressions(expressions...)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// WithCatalog implements the sql.CatalogTableFunction interface
func (tf *QueryDiffTableFunction) WithCatalog(c sql.Catalog) (sql.TableFunction, error) {
	newInstance := *tf
	newInstance.catalog = c
	pro, ok := c.(sql.DatabaseProvider)
	if !ok {
		return nil, fmt.Errorf("unable to get database provider")
	}
	newInstance.engine = gms.NewDefault(pro)
	err := newInstance.evalQueries()
	if err != nil {
		return nil, err
	}
	return &newInstance, nil
}

func (tf *QueryDiffTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(tf.Schema())
	numRows, _, err := tf.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (tf *QueryDiffTableFunction) RowCount(_ *sql.Context) (uint64, bool, error) {
	return queryDiffDefaultRowCount, false, nil
}

func (tf *QueryDiffTableFunction) evalQuery(query sql.Expression) (sql.Schema, sql.RowIter, error) {
	q, err := query.Eval(tf.ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	qStr, isStr := q.(string)
	if !isStr {
		return nil, nil, fmt.Errorf("query must be a string, not %T", q)
	}
	qStr = strings.TrimSpace(qStr)
	if !strings.HasPrefix(strings.ToLower(qStr), "select") { // TODO: allow "with?"
		return nil, nil, fmt.Errorf("query must be a SELECT statement")
	}
	sch, iter, _, err := tf.engine.Query(tf.ctx, qStr)
	return sch, iter, err
}

func (tf *QueryDiffTableFunction) evalQueries() error {
	var err error
	if tf.schema1, tf.rowIter1, err = tf.evalQuery(tf.query1); err != nil {
		return err
	}
	if tf.schema2, tf.rowIter2, err = tf.evalQuery(tf.query2); err != nil {
		return err
	}
	tf.sqlSch = append(tf.sqlSch, tf.schema1.Copy()...)
	tf.sqlSch = append(tf.sqlSch, tf.schema2.Copy()...)
	tf.sqlSch = append(tf.sqlSch, &sql.Column{Name: "diff_type", Type: gmstypes.Text})
	for i := range tf.schema1 {
		tf.sqlSch[i].Source = "query_diff"
		tf.sqlSch[i].Name = "from_" + tf.sqlSch[i].Name
	}
	offset := len(tf.schema1)
	for i := range tf.schema2 {
		idx := offset + i
		tf.sqlSch[idx].Source = "query_diff"
		tf.sqlSch[idx].Name = "to_" + tf.sqlSch[idx].Name
	}
	return nil
}

// Database implements the sql.Databaser interface
func (tf *QueryDiffTableFunction) Database() sql.Database {
	return tf.database
}

// WithDatabase implements the sql.Databaser interface
func (tf *QueryDiffTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	ntf := *tf
	ntf.database = database
	return &ntf, nil
}

// Expressions implements the sql.Expressioner interface
func (tf *QueryDiffTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{tf.query1, tf.query2}
}

// WithExpressions implements the sql.Expressioner interface
func (tf *QueryDiffTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(tf.Name(), "2", len(expression))
	}

	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(tf.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(tf.Name(), expr.String())
		}
	}

	newQdtf := *tf
	newQdtf.query1 = expression[0]
	newQdtf.query2 = expression[1]

	return &newQdtf, nil
}

// Children implements the sql.Node interface
func (tf *QueryDiffTableFunction) Children() []sql.Node {
	return nil
}

func (tf *QueryDiffTableFunction) compareRows(pkOrds []int, row1, row2 sql.Row) (int, bool, error) {
	var cmp int
	var err error
	for i, pkOrd := range pkOrds {
		cmp, err = tf.schema1[i].Type.Compare(row1.GetValue(pkOrd), row2.GetValue(pkOrd))
		if err != nil {
			return 0, false, err
		}
		if cmp != 0 {
			break
		}
	}
	var diff bool
	for i := 0; i < row1.Len(); i++ {
		if row1.GetValue(i) != row2.GetValue(i) {
			diff = true
			break
		}
	}
	return cmp, diff, nil
}

// RowIter implements the sql.Node interface
// TODO: actually implement a row iterator
func (tf *QueryDiffTableFunction) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if !tf.schema1.Equals(tf.schema2) {
		// todo: schema is currently an unreliable source of primary key columns
		return tf.keylessRowIter()
	}
	return tf.pkRowIter()
}

// keylessRowIter uses the entire row for difference comparison
func (tf *QueryDiffTableFunction) keylessRowIter() (sql.RowIter, error) {
	var results []sql.Row
	var newRow sql.UntypedSqlRow
	nilRow1, nilRow2 := make(sql.UntypedSqlRow, len(tf.schema1)), make(sql.UntypedSqlRow, len(tf.schema2))
	for {
		row, err := tf.rowIter1.Next(tf.ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		newRow = append(append(row.Values(), nilRow2...), "deleted")
		results = append(results, newRow)
	}
	for {
		row, err := tf.rowIter2.Next(tf.ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		newRow = append(append(nilRow1, row.Values()...), "added")
		results = append(results, newRow)
	}
	return sql.RowsToRowIter(results...), nil
}

// pkRowIter uses primary keys to do an efficient row comparison
func (tf *QueryDiffTableFunction) pkRowIter() (sql.RowIter, error) {
	var results []sql.Row
	var newRow sql.UntypedSqlRow
	row1, err1 := tf.rowIter1.Next(tf.ctx)
	row2, err2 := tf.rowIter2.Next(tf.ctx)
	var pkOrds []int
	for i, col := range tf.schema1 {
		if col.PrimaryKey {
			pkOrds = append(pkOrds, i)
		}
	}

	nilRow := make(sql.UntypedSqlRow, len(tf.schema1))
	for err1 == nil && err2 == nil {
		cmp, d, err := tf.compareRows(pkOrds, row1, row2)
		if err != nil {
			return nil, err
		}
		switch cmp {
		case -1: // deleted
			newRow = append(append(row1.Values(), nilRow...), "deleted")
			results = append(results, newRow)
			row1, err1 = tf.rowIter1.Next(tf.ctx)
		case 1: // added
			newRow = append(append(nilRow, row2.Values()...), "added")
			results = append(results, newRow)
			row2, err2 = tf.rowIter2.Next(tf.ctx)
		default: // modified or no change
			if d {
				newRow = append(append(row1.Values(), row2.Values()...), "modified")
				results = append(results, newRow)
			}
			row1, err1 = tf.rowIter1.Next(tf.ctx)
			row2, err2 = tf.rowIter2.Next(tf.ctx)
		}
	}

	// Append any remaining rows
	if err1 == io.EOF && err2 == io.EOF {
		return sql.RowsToRowIter(results...), nil
	} else if err1 == io.EOF {
		newRow = append(append(nilRow, row2.Values()...), "added")
		results = append(results, newRow)
		for {
			row2, err2 = tf.rowIter2.Next(tf.ctx)
			if err2 == io.EOF {
				break
			}
			newRow = append(append(nilRow, row2.Values()...), "added")
			results = append(results, newRow)
		}
	} else if err2 == io.EOF {
		newRow = append(append(row1.Values(), nilRow...), "deleted")
		results = append(results, newRow)
		for {
			row1, err1 = tf.rowIter1.Next(tf.ctx)
			if err1 == io.EOF {
				break
			}
			newRow = append(append(row1.Values(), nilRow...), "deleted")
			results = append(results, newRow)
		}
	} else {
		if err1 != nil {
			return nil, err1
		} else {
			return nil, err2
		}
	}
	return sql.RowsToRowIter(results...), nil
}

// WithChildren implements the sql.Node interface
func (tf *QueryDiffTableFunction) WithChildren(node ...sql.Node) (sql.Node, error) {
	if len(node) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return tf, nil
}

// CheckAuth implements the interface sql.AuthorizationCheckerNode.
func (tf *QueryDiffTableFunction) CheckAuth(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	subject := sql.PrivilegeCheckSubject{Database: tf.database.Name()}
	return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select))
}

// Schema implements the sql.Node interface
func (tf *QueryDiffTableFunction) Schema() sql.Schema {
	if !tf.Resolved() {
		return nil
	}
	if tf.sqlSch == nil {
		panic("schema hasn't been generated yet")
	}
	return tf.sqlSch
}

// Resolved implements the sql.Resolvable interface
func (tf *QueryDiffTableFunction) Resolved() bool {
	return tf.query1.Resolved() && tf.query2.Resolved()
}

func (tf *QueryDiffTableFunction) IsReadOnly() bool {
	// TODO: This table function is going to run two arbitrary queries
	// after evaluating the string expressions. We don't have access to the
	// string expressions here. In |evalQuery|, we have an adhoc check to
	// see if we are only running a |SELECT|. This works for the most part
	// for now --- non-read-only dfunctions may violate our assumption here.
	return true
}

// String implements the Stringer interface
func (tf *QueryDiffTableFunction) String() string {
	return fmt.Sprintf("DOLT_QUERY_DIFF('%s', '%s')", tf.query1.String(), tf.query2.String())
}

// Name implements the sql.TableFunction interface
func (tf *QueryDiffTableFunction) Name() string {
	return "dolt_query_diff"
}
