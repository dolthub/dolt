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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
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

	ap := cli.CreateMergeArgParser()
	args, err := getDoltArgs(ctx, row, d.Children())

	if err != nil {
		return nil, err
	}

	apr := cli.ParseArgs(ap, args, nil)

	// The first argument should be the branch name.
	branchName := apr.Arg(0)

	ddb, ok := sess.GetDoltDB(dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	root, ok := sess.GetRoot(dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	parent, _, parentRoot, err := getParent(ctx, err, sess, dbName)
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
	canFF, err := parent.CanFastForwardTo(ctx, cm)
	if err != nil {
		return nil, err
	}

	if canFF {
		err = executeFFMerge(ctx, false, dbData, cm)
		if err != nil {
			return nil, err
		}
		return cmh.String(), err
	} else {
		return nil, errors.New("DOLT_MERGE only supports fast forwards right now")
	}
}

func (d DoltMergeFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_MERGE(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltMergeFunc) Type() sql.Type {
	return sql.Text
}

func (d DoltMergeFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltMergeFunc(children...)
}

func NewDoltMergeFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltMergeFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

func executeFFMerge(ctx *sql.Context, squash bool, dbData env.DbData, cm2 *doltdb.Commit) error {
	rv, err := cm2.GetRootValue()

	if err != nil {
		return errors.New("Failed to return root value.")
	}

	stagedHash, err := dbData.Ddb.WriteRootValue(ctx, rv)

	if err != nil {
		return err
	}

	workingHash := stagedHash
	if !squash {
		err = dbData.Ddb.FastForward(ctx, dbData.Rsr.CWBHeadRef(), cm2)

		if err != nil {
			return err
		}
	}

	err = dbData.Rsw.SetWorkingHash(ctx, workingHash)
	if err != nil {
		return err
	}

	err = dbData.Rsw.SetStagedHash(ctx, stagedHash)
	if err != nil {
		return err
	}

	return nil
}
