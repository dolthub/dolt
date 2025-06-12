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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var _ sql.TableFunction = (*BranchStatusTableFunction)(nil)

type BranchStatusTableFunction struct {
	db    sql.Database
	exprs []sql.Expression
}

// NewInstance creates a new instance of TableFunction interface
func (b *BranchStatusTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New(b.Name(), "at least 1", len(args))
	}
	return &BranchStatusTableFunction{
		db:    db,
		exprs: args,
	}, nil
}

// Name implements the sql.Node interface
func (b *BranchStatusTableFunction) Name() string {
	return "DOLT_BRANCH_STATUS"
}

// String implements the Stringer interface
func (b *BranchStatusTableFunction) String() string {
	exprStrs := make([]string, len(b.exprs))
	for i, expr := range b.exprs {
		exprStrs[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", b.Name(), strings.Join(exprStrs, ", "))
}

// Resolved implements the sql.Resolvable interface
func (b *BranchStatusTableFunction) Resolved() bool {
	for _, expr := range b.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

// Expressions implements the sql.Expressioner interface
func (b *BranchStatusTableFunction) Expressions() []sql.Expression {
	return b.exprs
}

// WithExpressions implements the sql.Expressioner interface
func (b *BranchStatusTableFunction) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	nd := *b
	nd.exprs = exprs
	return &nd, nil
}

// Database implements the sql.Databaser interface
func (b *BranchStatusTableFunction) Database() sql.Database {
	return b.db
}

// WithDatabase implements the sql.Databaser interface
func (b *BranchStatusTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	nd := *b
	nd.db = db
	return &nd, nil
}

// IsReadOnly implements the sql.Node interface
func (b *BranchStatusTableFunction) IsReadOnly() bool {
	return true
}

// Schema implements the sql.Node interface
func (b *BranchStatusTableFunction) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "branch", Type: types.Text, Nullable: false},
		&sql.Column{Name: "commits_ahead", Type: types.Uint64, Nullable: false},
		&sql.Column{Name: "commits_behind", Type: types.Uint64, Nullable: false},
	}
}

// Children implements the sql.Node interface
func (b *BranchStatusTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface
func (b *BranchStatusTableFunction) WithChildren(children ...sql.Node) (sql.Node, error) {
	return b, nil
}

// RowIter implements the sql.Node interface
func (b *BranchStatusTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	sqlDb, ok := b.db.(dsess.SqlDatabase)
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

	specs, err := mustExpressionsToString(ctx, b.exprs)
	if err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, sql.ErrInvalidArgumentNumber.New(b.Name(), "at least 1", 0)
	}
	if len(specs) == 1 {
		return sql.RowsToRowIter(), nil
	}

	commits := make([]*doltdb.Commit, len(specs))
	for i, spec := range specs {
		cs, cErr := doltdb.NewCommitSpec(spec)
		if cErr != nil {
			return nil, cErr
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

	baseHash, err := baseCommit.HashOf()
	if err != nil {
		return nil, err
	}
	baseCommitClosure, err := baseCommit.GetCommitClosure(ctx)
	if err != nil {
		return nil, err
	}
	baseAncestors, err := baseCommitClosure.AsHashSet(ctx)
	if err != nil {
		return nil, err
	}
	baseAncestors.Insert(baseHash)

	var rows []sql.Row
	for i, branchCommit := range branchCommits {
		branchHash, hErr := branchCommit.HashOf()
		if hErr != nil {
			return nil, hErr
		}

		// same commit will have no differences
		var ahead, behind uint64
		if branchHash.Equal(baseHash) {
			rows = append(rows, sql.Row{specs[i+1], ahead, behind})
			continue
		}

		branchCommitClosure, bErr := branchCommit.GetCommitClosure(ctx)
		if bErr != nil {
			return nil, bErr
		}
		branchAncestors, bErr := branchCommitClosure.AsHashSet(ctx)
		if bErr != nil {
			return nil, bErr
		}
		branchAncestors.Insert(branchHash)
		for branchAncestor := range branchAncestors {
			if !baseAncestors.Has(branchAncestor) {
				ahead++
			}
		}
		for baseAncestor := range baseAncestors {
			if !branchAncestors.Has(baseAncestor) {
				behind++
			}
		}
		rows = append(rows, sql.Row{specs[i+1], ahead, behind})
	}

	return sql.RowsToRowIter(rows...), nil
}
