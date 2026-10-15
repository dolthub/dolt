// Copyright 2026 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const mergeBasesTableFunctionName = "dolt_merge_bases"

var _ sql.TableFunction = (*MergeBasesTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*MergeBasesTableFunction)(nil)

// MergeBasesTableFunction returns one row for every best common ancestor of two commits, like `git merge-base --all`.
type MergeBasesTableFunction struct {
	db    sql.Database
	exprs []sql.Expression
}

// NewInstance implements the sql.TableFunction interface
func (m *MergeBasesTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(m.Name(), 2, len(args))
	}
	return &MergeBasesTableFunction{db: db, exprs: args}, nil
}

// Name implements the sql.TableFunction interface
func (m *MergeBasesTableFunction) Name() string {
	return mergeBasesTableFunctionName
}

// String implements the Stringer interface
func (m *MergeBasesTableFunction) String() string {
	exprStrs := make([]string, len(m.exprs))
	for i, expr := range m.exprs {
		exprStrs[i] = expr.String()
	}
	return fmt.Sprintf("DOLT_MERGE_BASES(%s)", strings.Join(exprStrs, ", "))
}

// Resolved implements the sql.Resolvable interface
func (m *MergeBasesTableFunction) Resolved() bool {
	for _, expr := range m.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

// CheckAuth implements the sql.AuthorizationCheckerNode interface. Like dolt_log, it reveals commit history, so it
// requires SELECT on every table.
func (m *MergeBasesTableFunction) CheckAuth(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	baseDB, _ := doltdb.SplitRevisionDbName(m.db.Name())
	tblNames, err := m.db.GetTableNames(ctx)
	if err != nil {
		return false
	}

	operations := make([]sql.PrivilegedOperation, len(tblNames))
	for i, tblName := range tblNames {
		subject := sql.PrivilegeCheckSubject{Database: baseDB, Table: tblName}
		operations[i] = sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Select)
	}
	return opChecker.UserHasPrivileges(ctx, operations...)
}

// Expressions implements the sql.Expressioner interface
func (m *MergeBasesTableFunction) Expressions() []sql.Expression {
	return m.exprs
}

// WithExpressions implements the sql.Expressioner interface
func (m *MergeBasesTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(m.Name(), 2, len(exprs))
	}
	nm := *m
	nm.exprs = exprs
	return &nm, nil
}

// Database implements the sql.Databaser interface
func (m *MergeBasesTableFunction) Database() sql.Database {
	return m.db
}

// WithDatabase implements the sql.Databaser interface
func (m *MergeBasesTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	nm := *m
	nm.db = db
	return &nm, nil
}

// IsReadOnly implements the sql.Node interface
func (m *MergeBasesTableFunction) IsReadOnly() bool {
	return true
}

// Schema implements the sql.Node interface
func (m *MergeBasesTableFunction) Schema(ctx *sql.Context) sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "merge_base", Type: types.Text, Nullable: false},
	}
}

// Children implements the sql.Node interface
func (m *MergeBasesTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface
func (m *MergeBasesTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return m, nil
}

// RowIter implements the sql.Node interface. A NULL argument yields no rows.
func (m *MergeBasesTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	sqlDb, ok := m.db.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unable to get dolt database")
	}
	ddb := sqlDb.DbData().Ddb
	sess := dsess.DSessFromSess(ctx.Session)
	headRef, err := sess.CWBHeadRef(ctx, sqlDb.Name())
	if err != nil {
		return nil, err
	}

	specs := make([]string, len(m.exprs))
	for i, expr := range m.exprs {
		val, err := expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return sql.RowsToRowIter(), nil
		}
		if specs[i], ok = val.(string); !ok {
			return nil, fmt.Errorf("received '%v' when expecting string", val)
		}
	}

	left, err := dfunctions.ResolveRefSpec(ctx, headRef, ddb, specs[0])
	if err != nil {
		return nil, err
	}
	right, err := dfunctions.ResolveRefSpec(ctx, headRef, ddb, specs[1])
	if err != nil {
		return nil, err
	}
	bases, err := merge.MergeBases(ctx, left, right)
	if err != nil {
		return nil, err
	}

	rows := make([]sql.Row, len(bases))
	for i, base := range bases {
		rows[i] = sql.Row{base.String()}
	}
	return sql.RowsToRowIter(rows...), nil
}
