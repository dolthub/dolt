// Copyright 2021 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

const SquashFuncName = "squash"

type SquashFunc struct {
	expression.UnaryExpression
}

func NewSquashFunc(child sql.Expression) sql.Expression {
	return &SquashFunc{expression.UnaryExpression{Child: child}}
}

func (s SquashFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	sess := sqle.DSessFromSess(ctx.Session)

	branchVal, err := s.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	branchName, ok := branchVal.(string)
	if !ok {
		return nil, fmt.Errorf("error: SQUASH() was given a non-string branch name")
	}

	dbName := sess.GetCurrentDatabase()
	ddb, ok := sess.GetDoltDB(dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	root, ok := sess.GetRoot(dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	parent, _, parentRoot, err := getParent(ctx, sess, dbName)
	if err != nil {
		return nil, err
	}

	err = checkForUncommittedChanges(root, parentRoot)
	if err != nil {
		return nil, err
	}

	cm, _, err := getBranchCommit(ctx, branchName, ddb)
	if err != nil {
		return nil, err
	}

	mergeRoot, _, err := merge.MergeCommits(ctx, parent, cm)
	if err != nil {
		return nil, err
	}

	h, err := ddb.WriteRootValue(ctx, mergeRoot)
	if err != nil {
		return nil, err
	}

	hashStr := h.String()

	//// Update the session and editor with the new root.
	//sess.SetRoot(dbName, hashStr, mergeRoot)
	//
	//err = sess.SetEditorRoot(ctx, dbName, root)
	//if err != nil {
	//	return nil, err
	//}
	//
	//// Clear the cache associated with the DB.
	//sess.ClearCache(dbName)

	return hashStr, nil
}

func (s SquashFunc) Resolved() bool {
	return s.Child.Resolved()
}

func (s SquashFunc) String() string {
	return fmt.Sprintf("SQUASH(%s)", s.Child.String())
}

func (s SquashFunc) Type() sql.Type {
	return sql.Text
}

func (s SquashFunc) IsNullable() bool {
	return false
}

func (s SquashFunc) Children() []sql.Expression {
	return []sql.Expression{s.Child}
}

func (s SquashFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 1)
	}

	return NewSquashFunc(children[0]), nil
}
