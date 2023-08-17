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

package sqle

import (
	"fmt"
	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"io"
)

var _ sql.TableFunction = (*QueryDiffTableFunction)(nil)
var _ sql.CatalogTableFunction = (*QueryDiffTableFunction)(nil)
var _ sql.ExecSourceRel = (*QueryDiffTableFunction)(nil)

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
func (qdtf *QueryDiffTableFunction) NewInstance(ctx *sql.Context, database sql.Database, expressions []sql.Expression) (sql.Node, error) {
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
func (qdtf *QueryDiffTableFunction) WithCatalog(c sql.Catalog) (sql.TableFunction, error) {
	newInstance := *qdtf
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

func (qdtf *QueryDiffTableFunction) evalQueries() error {
	q1, err := qdtf.query1.Eval(qdtf.ctx, nil)
	if err != nil {
		return err
	}
	q2, err := qdtf.query2.Eval(qdtf.ctx, nil)
	if err != nil {
		return err
	}
	if qdtf.schema1, qdtf.rowIter1, err = qdtf.engine.Query(qdtf.ctx, q1.(string)); err != nil {
		return err
	}
	if qdtf.schema2, qdtf.rowIter2, err = qdtf.engine.Query(qdtf.ctx, q2.(string)); err != nil {
		return err
	}
	// TODO: need to deep copy
	qdtf.sqlSch = append(qdtf.sqlSch, qdtf.schema1.Copy()...)
	qdtf.sqlSch = append(qdtf.sqlSch, qdtf.schema2.Copy()...)
	qdtf.sqlSch = append(qdtf.sqlSch, &sql.Column{Name: "diff_type", Type: gmstypes.Text})
	for i := range qdtf.schema1 {
		qdtf.sqlSch[i].Source = "query_diff"
		qdtf.sqlSch[i].Name = "from_" + qdtf.sqlSch[i].Name
	}
	for i := range qdtf.schema2 {
		qdtf.sqlSch[len(qdtf.schema1) + i].Source = "query_diff"
		qdtf.sqlSch[len(qdtf.schema1) + i].Name = "to_" + qdtf.sqlSch[len(qdtf.schema1) + i].Name
	}
	return nil
}

// Database implements the sql.Databaser interface
func (qdtf *QueryDiffTableFunction) Database() sql.Database {
	return qdtf.database
}

// WithDatabase implements the sql.Databaser interface
func (qdtf *QueryDiffTableFunction) WithDatabase(database sql.Database) (sql.Node, error) {
	nqdtf := *qdtf
	nqdtf.database = database
	return &nqdtf, nil
}

// Expressions implements the sql.Expressioner interface
func (qdtf *QueryDiffTableFunction) Expressions() []sql.Expression {
	return []sql.Expression{qdtf.query1, qdtf.query2}
}

// WithExpressions implements the sql.Expressioner interface
func (qdtf *QueryDiffTableFunction) WithExpressions(expression ...sql.Expression) (sql.Node, error) {
	if len(expression) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(qdtf.Name(), "exactly 2", len(expression))
	}

	for _, expr := range expression {
		if !expr.Resolved() {
			return nil, ErrInvalidNonLiteralArgument.New(qdtf.Name(), expr.String())
		}
		// prepared statements resolve functions beforehand, so above check fails
		if _, ok := expr.(sql.FunctionExpression); ok {
			return nil, ErrInvalidNonLiteralArgument.New(qdtf.Name(), expr.String())
		}
	}

	newQdtf := *qdtf
	newQdtf.query1 = expression[0]
	newQdtf.query2 = expression[1]

	return &newQdtf, nil
}

// Children implements the sql.Node interface
func (qdtf *QueryDiffTableFunction) Children() []sql.Node {
	return nil
}

func (qdtf *QueryDiffTableFunction) compareRows(pkOrds []int, row1, row2 sql.Row) (int, bool) {
	var cmp int
	for _, pkOrd := range pkOrds {
		pk1, _ := gmstypes.ConvertToString(row1[pkOrd], gmstypes.Text)
		pk2, _ := gmstypes.ConvertToString(row2[pkOrd], gmstypes.Text)
		if pk1 < pk2 {
			cmp = -1
		} else if pk1 > pk2 {
			cmp = 1
		} else {
			cmp = 0
		}
	}
	var diff bool
	for i := 0; i < len(row1); i++ {
		a, _ := gmstypes.ConvertToString(row1[i], gmstypes.Text)
		b, _ := gmstypes.ConvertToString(row2[i], gmstypes.Text)
		if a != b {
			diff = true
			break
		}
	}
	return cmp, diff
}

// RowIter implements the sql.Node interface
func (qdtf *QueryDiffTableFunction) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	// TODO: assume both are sorted according to their primary keys
	row1, err1 := qdtf.rowIter1.Next(qdtf.ctx)
	row2, err2 := qdtf.rowIter2.Next(qdtf.ctx)
	if !qdtf.schema1.Equals(qdtf.schema2) {
		return sql.RowsToRowIter(), nil
	}
	var pkOrds []int
	for i, col := range qdtf.schema1 {
		if col.PrimaryKey {
			pkOrds = append(pkOrds, i)
		}
	}
	var results []sql.Row
	var newRow sql.Row
	nilRow := make(sql.Row, len(qdtf.schema1))
	for err1 == nil && err2 == nil {
		cmp, d := qdtf.compareRows(pkOrds, row1, row2)
		switch cmp {
		case -1: // deleted
			newRow = append(append(row1, nilRow...), "deleted")
			results = append(results, newRow)
			row1, err1 = qdtf.rowIter1.Next(qdtf.ctx)
		case 1: // added
			newRow = append(append(nilRow, row2...), "added")
			results = append(results, newRow)
			row2, err2 = qdtf.rowIter2.Next(qdtf.ctx)
		default: // modified or no change
			if d {
				newRow = append(append(row1, row2...), "modified")
				results = append(results, newRow)
			}
			row1, err1 = qdtf.rowIter1.Next(qdtf.ctx)
			row2, err2 = qdtf.rowIter2.Next(qdtf.ctx)
		}
	}

	// Append any remaining rows
	if err1 == io.EOF && err2 == io.EOF {
	} else if err1 == io.EOF {
		newRow = append(append(nilRow, row2...), "added")
		results = append(results, newRow)
		for {
			row2, err2 = qdtf.rowIter2.Next(qdtf.ctx)
			if err2 == io.EOF {
				break
			}
			newRow = append(append(nilRow, row2...), "added")
			results = append(results, newRow)
		}
	} else if err2 == io.EOF {
		newRow = append(append(row1, nilRow...), "deleted")
		results = append(results, newRow)
		for {
			row1, err1 = qdtf.rowIter1.Next(qdtf.ctx)
			if err1 == io.EOF {
				break
			}
			newRow = append(append(row1, nilRow...), "deleted")
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
func (qdtf *QueryDiffTableFunction) WithChildren(node ...sql.Node) (sql.Node, error) {
	if len(node) != 0 {
		return nil, fmt.Errorf("unexpected children")
	}
	return qdtf, nil
}

// CheckPrivileges implements the sql.Node interface
func (qdtf *QueryDiffTableFunction) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
	return opChecker.UserHasPrivileges(ctx, sql.NewPrivilegedOperation(qdtf.database.Name(), "", "", sql.PrivilegeType_Select))
}

// Schema implements the sql.Node interface
func (qdtf *QueryDiffTableFunction) Schema() sql.Schema {
	if !qdtf.Resolved() {
		return nil
	}

	if qdtf.sqlSch == nil {
		panic("schema hasn't been generated yet")
	}

	return qdtf.sqlSch
}

// Resolved implements the sql.Resolvable interface
func (qdtf *QueryDiffTableFunction) Resolved() bool {
	return qdtf.query1.Resolved() && qdtf.query2.Resolved()
}

// String implements the Stringer interface
func (qdtf *QueryDiffTableFunction) String() string {
	return fmt.Sprintf("DOLT_QUERY_DIFF('%s', '%s')", qdtf.query1.String(), qdtf.query2.String())
}

// Name implements the sql.TableFunction interface
func (qdtf *QueryDiffTableFunction) Name() string {
	return "dolt_query_diff"
}



