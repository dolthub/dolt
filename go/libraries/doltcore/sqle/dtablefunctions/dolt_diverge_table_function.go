// Copyright 2025 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ sql.TableFunction = (*DivergeTableFunction)(nil)

type DivergeTableFunction struct {
	db    sql.Database
	exprs []sql.Expression
}

// NewInstance creates a new instance of TableFunction interface
func (d *DivergeTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) < 2 {
		// TODO: should this just return empty set when there's one argument?
		return nil, sql.ErrInvalidArgumentNumber.New(d.Name(), "at least 2", len(args))
	}
	return &DivergeTableFunction{
		db:    db,
		exprs: args,
	}, nil
}

// Name implements the sql.Node interface
func (d *DivergeTableFunction) Name() string {
	return "DOLT_DIVERGE"
}

// String implements the Stringer interface
func (d *DivergeTableFunction) String() string {
	exprStrs := make([]string, len(d.exprs))
	for i, expr := range d.exprs {
		exprStrs[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", d.Name(), strings.Join(exprStrs, ", "))
}

// Resolved implements the sql.Resolvable interface
func (d *DivergeTableFunction) Resolved() bool {
	for _, expr := range d.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

// Expressions implements the sql.Expressioner interface
func (d *DivergeTableFunction) Expressions() []sql.Expression {
	return d.exprs
}

// WithExpressions implements the sql.Expressioner interface
func (d *DivergeTableFunction) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	nd := *d
	nd.exprs = exprs
	return &nd, nil
}

// Database implements the sql.Databaser interface
func (d *DivergeTableFunction) Database() sql.Database {
	return d.db
}

// WithDatabase implements the sql.Databaser interface
func (d *DivergeTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	nd := *d
	nd.db = db
	return &nd, nil
}

// IsReadOnly implements the sql.Node interface
func (d *DivergeTableFunction) IsReadOnly() bool {
	return true
}

// Schema implements the sql.Node interface
func (d *DivergeTableFunction) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "branch", Type: types.Text, Nullable: false},
		&sql.Column{Name: "commits_ahead", Type: types.Uint64, Nullable: false},
		&sql.Column{Name: "commits_behind", Type: types.Uint64, Nullable: false},
	}
}

// Children implements the sql.Node interface
func (d *DivergeTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface
func (d *DivergeTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	return d, nil
}

// RowIter implements the sql.Node interface
func (d *DivergeTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	sqlDb, ok := d.db.(dsess.SqlDatabase)
	if !ok {
		return nil, fmt.Errorf("unable to get dolt database")
	}
	ddb := sqlDb.DbData().Ddb

	sess := dsess.DSessFromSess(ctx.Session)
	dbName := sess.Session.GetCurrentDatabase()
	headRef, err := sess.CWBHeadRef(ctx, dbName)
	if err != nil {
		return nil, err
	}

	specs, err := mustExpressionsToString(ctx, d.exprs)
	if err != nil {
		return nil, err
	}
	if len(specs) == 1 {
		return sql.RowsToRowIter(), nil
	}

	commits := make([]*doltdb.Commit, len(specs))
	for i, spec := range specs {
		cs, cErr := doltdb.NewCommitSpec(spec)
		if cErr != nil {
			return nil, err
		}
		optCmt, oErr := ddb.Resolve(ctx, cs, headRef)
		if oErr != nil {
			return nil, oErr
		}
		commit, optCommitOk := optCmt.ToCommit()
		if !optCommitOk {
			return nil, doltdb.ErrGhostCommitEncountered
		}
		commits[i] = commit
	}

	baseCommit := commits[0]
	branchCommits := commits[1:]
	baseHeight, err := baseCommit.Height()
	if err != nil {
		return nil, err
	}

	var rows []sql.Row
	for i, branchCommit := range branchCommits {
		branchHeight, bErr := branchCommit.Height()
		if bErr != nil {
			return nil, bErr
		}

		ancOptCommit, ancErr := doltdb.GetCommitAncestor(ctx, baseCommit, branchCommit)
		if ancErr != nil {
			return nil, err
		}
		ancCommit, ancCommitOk := ancOptCommit.ToCommit()
		if !ancCommitOk {
			return nil, doltdb.ErrGhostCommitEncountered
		}
		ancHeight, _ := ancCommit.Height()

		ahead := branchHeight - ancHeight
		behind := baseHeight - ancHeight
		rows = append(rows, sql.Row{specs[i+1], ahead, behind})
	}

	return sql.RowsToRowIter(rows...), nil
}
