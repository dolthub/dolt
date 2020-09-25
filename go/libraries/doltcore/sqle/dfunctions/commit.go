// Copyright 2020 Liquidata, Inc.
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

package dfunctions

import (
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

const CommitFuncName = "commit"

type CommitFunc struct {
	expression.UnaryExpression
}

// NewCommitFunc creates a new CommitFunc expression.
func NewCommitFunc(e sql.Expression) sql.Expression {
	return &CommitFunc{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (cf *CommitFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := cf.Child.Eval(ctx, row)

	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	commitMessage, ok := val.(string)

	if !ok {
		return nil, errors.New("branch name is not a string")
	}

	dbName := ctx.GetCurrentDatabase()
	dSess := sqle.DSessFromSess(ctx.Session)
	parent, _, err := dSess.GetParentCommit(ctx, dbName)

	if err != nil {
		return nil, err
	}

	root, ok := dSess.GetRoot(dbName)

	if !ok {
		return nil, fmt.Errorf("unknown database '%s'", dbName)
	}

	ddb, ok := dSess.GetDoltDB(dbName)

	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	h, err := ddb.WriteRootValue(ctx, root)

	if err != nil {
		return nil, err
	}

	if dSess.Username == "" || dSess.Email == "" {
		return nil, errors.New("commit function failure: Username and/or email not configured")
	}

	meta, err := doltdb.NewCommitMeta(dSess.Username, dSess.Email, commitMessage)

	if err != nil {
		return nil, err
	}

	cm, err := ddb.WriteDanglingCommit(ctx, h, []*doltdb.Commit{parent}, meta)

	if err != nil {
		return nil, err
	}

	h, err = cm.HashOf()

	if err != nil {
		return nil, err
	}

	return h.String(), nil
}

// String implements the Stringer interface.
func (cf *CommitFunc) String() string {
	return fmt.Sprintf("COMMIT(%s)", cf.Child.String())
}

// IsNullable implements the Expression interface.
func (cf *CommitFunc) IsNullable() bool {
	return cf.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (cf *CommitFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(cf, len(children), 1)
	}

	return NewCommitFunc(children[0]), nil
}

// Type implements the Expression interface.
func (cf *CommitFunc) Type() sql.Type {
	return sql.Text
}
