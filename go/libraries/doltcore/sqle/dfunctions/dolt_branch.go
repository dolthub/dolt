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
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const DoltBranchFuncName = "dolt_branch"

var EmptyBranchNameErr = errors.New("error: cannot branch empty string")
var InvalidArgErr = errors.New("error: invalid usage")

// Deprecated: please use the version in the dprocedures package
type DoltBranchFunc struct {
	expression.NaryExpression
}

// Deprecated: please use the version in the dprocedures package
func NewDoltBranchFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltBranchFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

func (d DoltBranchFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_BRANCH(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltBranchFunc) Type() sql.Type {
	return sql.Int8
}

func (d DoltBranchFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltBranchFunc(children...)
}

func (d DoltBranchFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}
	return DoDoltBranch(ctx, args)
}

func DoDoltBranch(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	apr, err := cli.CreateBranchArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	switch {
	case apr.Contains(cli.CopyFlag):
		err = makeACopyOfBranch(ctx, dbData, apr)
		if err != nil {
			return 1, err
		}
	case apr.Contains(cli.MoveFlag):
		return 1, errors.New("Renaming a branch is not supported.")
	case apr.Contains(cli.DeleteFlag):
		return 1, errors.New("Deleting branches is not supported.")
	case apr.Contains(cli.DeleteForceFlag):
		return 1, errors.New("Deleting branches is not supported.")
	default:
		// regular branch - create new branch
		if apr.NArg() != 1 {
			return 1, InvalidArgErr
		}

		branchName := apr.Arg(0)
		if len(branchName) == 0 {
			return 1, EmptyBranchNameErr
		}

		err = createNewBranch(ctx, dbData, branchName)
		if err != nil {
			return 1, err
		}
	}

	return 0, nil
}

func createNewBranch(ctx *sql.Context, dbData env.DbData, branchName string) error {
	// Check if the branch already exists.
	isBranch, err := actions.IsBranch(ctx, dbData.Ddb, branchName)
	if err != nil {
		return err
	} else if isBranch {
		return errors.New(fmt.Sprintf("fatal: A branch named '%s' already exists.", branchName))
	}

	startPt := fmt.Sprintf("head")
	return actions.CreateBranchWithStartPt(ctx, dbData, branchName, startPt, false)
}

func makeACopyOfBranch(ctx *sql.Context, dbData env.DbData, apr *argparser.ArgParseResults) error {
	if apr.NArg() != 2 {
		return InvalidArgErr
	}

	srcBr := apr.Args[0]
	if len(srcBr) == 0 {
		return EmptyBranchNameErr
	}

	destBr := apr.Args[1]
	if len(destBr) == 0 {
		return EmptyBranchNameErr
	}

	force := apr.Contains(cli.ForceFlag)
	return copyABranch(ctx, dbData, srcBr, destBr, force)
}

func copyABranch(ctx *sql.Context, dbData env.DbData, srcBr string, destBr string, force bool) error {
	err := actions.CopyBranchOnDB(ctx, dbData.Ddb, srcBr, destBr, force)
	if err != nil {
		if err == doltdb.ErrBranchNotFound {
			return errors.New(fmt.Sprintf("fatal: A branch named '%s' not found", srcBr))
		} else if err == actions.ErrAlreadyExists {
			return errors.New(fmt.Sprintf("fatal: A branch named '%s' already exists.", destBr))
		} else if err == doltdb.ErrInvBranchName {
			return errors.New(fmt.Sprintf("fatal: '%s' is not a valid branch name.", destBr))
		} else {
			return errors.New(fmt.Sprintf("fatal: Unexpected error copying branch from '%s' to '%s'", srcBr, destBr))
		}
	}

	return nil
}
