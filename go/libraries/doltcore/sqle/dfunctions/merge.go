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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/hash"
)

const MergeFuncName = "merge"

type MergeFunc struct {
	expression.UnaryExpression
}

// NewMergeFunc creates a new MergeFunc expression.
func NewMergeFunc(e sql.Expression) sql.Expression {
	return &MergeFunc{expression.UnaryExpression{Child: e}}
}

// Eval implements the Expression interface.
func (cf *MergeFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := cf.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	} else if val == nil {
		return nil, nil
	}

	sess := sqle.DSessFromSess(ctx.Session)
	if sess.Username == "" || sess.Email == "" {
		return nil, errors.New("commit function failure: Username and/or email not configured")
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

	parent, ph, parentRoot, err := getParent(ctx, err, sess, dbName)
	if err != nil {
		return nil, err
	}

	err = checkForUncommittedChanges(root, parentRoot)
	if err != nil {
		return nil, err
	}

	cm, cmh, err := getBranchCommit(ctx, ok, val, err, ddb)
	if err != nil {
		return nil, err
	}

	mergeRoot, _, err := merge.MergeCommits(ctx, parent, cm)
	if err == merge.ErrFastForward {
		return cmh.String(), nil
	}

	h, err := ddb.WriteRootValue(ctx, mergeRoot)
	if err != nil {
		return nil, err
	}

	commitMessage := fmt.Sprintf("SQL Generated commit merging %s into %s", ph.String(), cmh.String())
	meta, err := doltdb.NewCommitMeta(sess.Username, sess.Email, commitMessage)
	if err != nil {
		return nil, err
	}

	mergeCommit, err := ddb.WriteDanglingCommit(ctx, h, []*doltdb.Commit{parent, cm}, meta)
	if err != nil {
		return nil, err
	}

	h, err = mergeCommit.HashOf()
	if err != nil {
		return nil, err
	}

	return h.String(), nil
}

func checkForUncommittedChanges(root *doltdb.RootValue, parentRoot *doltdb.RootValue) error {
	rh, err := root.HashOf()

	if err != nil {
		return err
	}

	prh, err := parentRoot.HashOf()

	if err != nil {
		return err
	}

	if rh != prh {
		return errors.New("cannot merge with uncommitted changes")
	}

	return nil
}

func getBranchCommit(ctx *sql.Context, ok bool, val interface{}, err error, ddb *doltdb.DoltDB) (*doltdb.Commit, hash.Hash, error) {
	paramStr, ok := val.(string)

	if !ok {
		return nil, hash.Hash{}, errors.New("branch name is not a string")
	}

	branchRef, err := getBranchInsensitive(ctx, paramStr, ddb)

	if err != nil {
		return nil, hash.Hash{}, err
	}

	cm, err := ddb.ResolveRef(ctx, branchRef)

	if err != nil {
		return nil, hash.Hash{}, err
	}

	cmh, err := cm.HashOf()

	if err != nil {
		return nil, hash.Hash{}, err
	}

	return cm, cmh, nil
}

func getParent(ctx *sql.Context, err error, sess *sqle.DoltSession, dbName string) (*doltdb.Commit, hash.Hash, *doltdb.RootValue, error) {
	parent, ph, err := sess.GetParentCommit(ctx, dbName)

	if err != nil {
		return nil, hash.Hash{}, nil, err
	}

	parentRoot, err := parent.GetRootValue()

	if err != nil {
		return nil, hash.Hash{}, nil, err
	}

	return parent, ph, parentRoot, nil
}

// String implements the Stringer interface.
func (cf *MergeFunc) String() string {
	return fmt.Sprintf("Merge(%s)", cf.Child.String())
}

// IsNullable implements the Expression interface.
func (cf *MergeFunc) IsNullable() bool {
	return cf.Child.IsNullable()
}

// WithChildren implements the Expression interface.
func (cf *MergeFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(cf, len(children), 1)
	}

	return NewMergeFunc(children[0]), nil
}

// Type implements the Expression interface.
func (cf *MergeFunc) Type() sql.Type {
	return sql.Text
}
