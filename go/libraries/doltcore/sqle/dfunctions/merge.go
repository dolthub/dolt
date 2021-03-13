// Copyright 2020 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/hash"
)

const MergeFuncName = "merge"

type MergeFunc struct {
	children []sql.Expression
}

// NewMergeFunc creates a new MergeFunc expression.
func NewMergeFunc(args ...sql.Expression) (sql.Expression, error) {
	return &MergeFunc{children: args}, nil
}

// Eval implements the Expression interface.
func (cf *MergeFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	sess := sqle.DSessFromSess(ctx.Session)

	// TODO: Move to a separate MERGE argparser.
	ap := cli.CreateCommitArgParser()
	args, err := getDoltArgs(ctx, row, cf.Children())

	if err != nil {
		return nil, err
	}

	apr := cli.ParseArgs(ap, args, nil)

	// The fist argument should be the branch name.
	branchName := apr.Arg(0)

	var name, email string
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			return nil, err
		}
	} else {
		name = sess.Username
		email = sess.Email
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

	parent, ph, parentRoot, err := getParent(ctx, sess, dbName)
	if err != nil {
		return nil, err
	}

	err = checkForUncommittedChanges(root, parentRoot)
	if err != nil {
		return nil, err
	}

	cm, cmh, err := getBranchCommit(ctx, branchName, ddb)
	if err != nil {
		return nil, err
	}

	// No need to write a merge commit, if the parent can ffw to the commit coming from the branch.
	canFF, err := parent.CanFastForwardTo(ctx, cm)
	if err != nil {
		return nil, err
	}

	if canFF {
		return cmh.String(), nil
	}

	mergeRoot, _, err := merge.MergeCommits(ctx, parent, cm)

	if err != nil {
		return nil, err
	}

	h, err := ddb.WriteRootValue(ctx, mergeRoot)
	if err != nil {
		return nil, err
	}

	commitMessage := fmt.Sprintf("SQL Generated commit merging %s into %s", ph.String(), cmh.String())
	meta, err := doltdb.NewCommitMeta(name, email, commitMessage)
	if err != nil {
		return nil, err
	}

	mergeCommit, err := ddb.CommitDanglingWithParentCommits(ctx, h, []*doltdb.Commit{parent, cm}, meta)
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

func getBranchCommit(ctx *sql.Context, val interface{}, ddb *doltdb.DoltDB) (*doltdb.Commit, hash.Hash, error) {
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

func getParent(ctx *sql.Context, sess *sqle.DoltSession, dbName string) (*doltdb.Commit, hash.Hash, *doltdb.RootValue, error) {
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
	childrenStrings := make([]string, len(cf.children))

	for i, child := range cf.children {
		childrenStrings[i] = child.String()
	}
	return fmt.Sprintf("Merge(%s)", strings.Join(childrenStrings, ","))
}

// IsNullable implements the Expression interface.
func (cf *MergeFunc) IsNullable() bool {
	return false
}

func (cf *MergeFunc) Resolved() bool {
	for _, child := range cf.Children() {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (cf *MergeFunc) Children() []sql.Expression {
	return cf.children
}

// WithChildren implements the Expression interface.
func (cf *MergeFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewMergeFunc(children...)
}

// Type implements the Expression interface.
func (cf *MergeFunc) Type() sql.Type {
	return sql.Text
}
