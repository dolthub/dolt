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
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"strings"
)

const DoltMergeFuncName = "dolt_merge"

type DoltMergeFunc struct {
	expression.NaryExpression
}

func (d DoltMergeFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	sess := sqle.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(dbName)

	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	// TODO: Move to a separate MERGE argparser.
	ap := cli.CreateCommitArgParser()
	args, err := getDoltArgs(ctx, row, d.Children())

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

	cm, cmh, err := getBranchCommit(ctx, ok, branchName, err, ddb)
	if err != nil {
		return nil, err
	}

	// No need to write a merge commit, if the parent can ffw to the commit coming from the branch.
	//canFF, err := parent.CanFastForwardTo(ctx, cm)
	//if err != nil {
	//	return nil, err
	//}
	//
	//if canFF {
	//	return cmh.String(), nil
	//}

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

	mergeCommit, err := ddb.WriteDanglingCommit(ctx, h, []*doltdb.Commit{parent, cm}, meta)
	if err != nil {
		return nil, err
	}

	h, err = mergeCommit.HashOf()
	if err != nil {
		return nil, err
	}

	err = mergedRootToWorking(ctx, dbData, mergeRoot, cm)
	if err != nil {
		return nil, err
	}

	return h.String(), nil
}

func (d DoltMergeFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_MERGE(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltMergeFunc) Type() sql.Type {
	return sql.Int8
}

func (d DoltMergeFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltMergeFunc(children...)
}

func NewDoltMergeFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltMergeFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

func executeFFMerge(ctx *sql.Context, squash bool, dbData env.DbData, mergeRoot *doltdb.RootValue, cm2 *doltdb.Commit) error {
	rv, err := cm2.GetRootValue()

	if err != nil {
		return errors.New("Failed to return root value.")
	}

	stagedHash, err := dbData.Ddb.WriteRootValue(ctx, rv)

	if err != nil {
		return errors.New("Failed to write database.")
	}

	workingHash := stagedHash
	if !squash {
		err = dbData.Ddb.FastForward(ctx, dbData.Rsr.CWBHeadRef(), cm2)

		if err != nil {
			return errors.New("Failed to write database")
		}
	}

	err = dbData.Rsw.SetWorkingHash(workingHash)
	if err != nil {
		return err
	}

	err = dbData.Rsw.SetStagedHash(stagedHash)
	if err != nil {
		return err
	}

	return nil
}

func mergedRootToWorking(ctx *sql.Context, dbData env.DbData, mergeRoot *doltdb.RootValue, cm2 *doltdb.Commit) error {
	workingRoot := mergeRoot

	// TODO: apply changes on top of working?

	h2, err := cm2.HashOf()

	if err != nil {
		return err
	}

	err = dbData.Rsw.StartMerge(h2.String())

	if err != nil {
		return err
	}

	_, err = env.UpdateWorkingRoot(ctx, dbData.Ddb, dbData.Rsw, workingRoot)
	if err != nil {
		return err
	}

	_, err = env.UpdateStagedRoot(ctx, dbData.Ddb, dbData.Rsw, workingRoot)
	if err != nil {
		return err
	}

	return nil
}