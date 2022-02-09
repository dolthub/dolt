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
	goerrors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
)

const MergeFuncName = "merge"

var ErrUncommittedChanges = goerrors.NewKind("cannot merge with uncommitted changes")

type MergeFunc struct {
	children []sql.Expression
}

// NewMergeFunc creates a new MergeFunc expression.
func NewMergeFunc(args ...sql.Expression) (sql.Expression, error) {
	return &MergeFunc{children: args}, nil
}

// Eval implements the Expression interface.
func (cf *MergeFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	sess := dsess.DSessFromSess(ctx.Session)

	// TODO: Move to a separate MERGE argparser.
	ap := cli.CreateCommitArgParser()
	args, err := getDoltArgs(ctx, row, cf.Children())

	if err != nil {
		return nil, err
	}

	apr, err := ap.Parse(args)
	if err != nil {
		return nil, err
	}

	// The fist argument should be the branch name.
	branchName := apr.Arg(0)

	var name, email string
	if authorStr, ok := apr.GetValue(cli.AuthorParam); ok {
		name, email, err = cli.ParseAuthor(authorStr)
		if err != nil {
			return nil, err
		}
	} else {
		name = sess.Username()
		email = sess.Email()
	}

	dbName := sess.GetCurrentDatabase()
	ddb, ok := sess.GetDoltDB(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	roots, ok := sess.GetRoots(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	head, hh, headRoot, err := getHead(ctx, sess, dbName)
	if err != nil {
		return nil, err
	}

	err = checkForUncommittedChanges(roots.Working, headRoot)
	if err != nil {
		return nil, err
	}

	cm, cmh, err := getBranchCommit(ctx, branchName, ddb)
	if err != nil {
		return nil, err
	}

	// No need to write a merge commit, if the head can ffw to the commit coming from the branch.
	canFF, err := head.CanFastForwardTo(ctx, cm)
	if err != nil {
		return nil, err
	}

	if canFF {
		ancRoot, err := head.GetRootValue()
		if err != nil {
			return nil, err
		}
		mergedRoot, err := cm.GetRootValue()
		if err != nil {
			return nil, err
		}
		if cvPossible, err := merge.MayHaveConstraintViolations(ctx, ancRoot, mergedRoot); err != nil {
			return nil, err
		} else if !cvPossible {
			return cmh.String(), nil
		}
	}

	dbState, ok, err := sess.LookupDbState(ctx, dbName)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("Could not load database %s", dbName)
	}

	mergeRoot, _, err := merge.MergeCommits(ctx, head, cm, dbState.EditOpts())

	if err != nil {
		return nil, err
	}

	h, err := ddb.WriteRootValue(ctx, mergeRoot)
	if err != nil {
		return nil, err
	}

	commitMessage := fmt.Sprintf("SQL Generated commit merging %s into %s", hh.String(), cmh.String())
	meta, err := doltdb.NewCommitMeta(name, email, commitMessage)
	if err != nil {
		return nil, err
	}

	mergeCommit, err := ddb.CommitDanglingWithParentCommits(ctx, h, []*doltdb.Commit{head, cm}, meta)
	if err != nil {
		return nil, err
	}

	h, err = mergeCommit.HashOf()
	if err != nil {
		return nil, err
	}

	return h.String(), nil
}

func checkForUncommittedChanges(root *doltdb.RootValue, headRoot *doltdb.RootValue) error {
	rh, err := root.HashOf()

	if err != nil {
		return err
	}

	hrh, err := headRoot.HashOf()

	if err != nil {
		return err
	}

	if rh != hrh {
		return ErrUncommittedChanges.New()
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

	cm, err := ddb.ResolveCommitRef(ctx, branchRef)

	if err != nil {
		return nil, hash.Hash{}, err
	}

	cmh, err := cm.HashOf()

	if err != nil {
		return nil, hash.Hash{}, err
	}

	return cm, cmh, nil
}

func getHead(ctx *sql.Context, sess *dsess.DoltSession, dbName string) (*doltdb.Commit, hash.Hash, *doltdb.RootValue, error) {
	head, err := sess.GetHeadCommit(ctx, dbName)
	if err != nil {
		return nil, hash.Hash{}, nil, err
	}

	hh, err := head.HashOf()
	if err != nil {
		return nil, hash.Hash{}, nil, err
	}

	headRoot, err := head.GetRootValue()
	if err != nil {
		return nil, hash.Hash{}, nil, err
	}

	return head, hh, headRoot, nil
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
