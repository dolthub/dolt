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
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"strings"
)

const DoltBranchFuncName = "dolt_branch"

var EmptyBranchNameErr = errors.New("error: cannot branch empty string")

type DoltBranchFunc struct {
	expression.NaryExpression
}

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
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	ap := cli.CreateBranchArgParser()

	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}

	apr, err := ap.Parse(args)
	if err != nil {
		return 1, err
	}

	force := apr.Contains(cli.ForceFlag)

	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	if apr.Contains(cli.CopyFlag) {
		if aErr := validateArgs(apr, 2); aErr != nil {
			return 1, aErr
		}

		srcBranch := apr.Arg(0)
		destBranch := apr.Arg(1)

		err = makeACopyOfBranch(ctx, dbData, srcBranch, destBranch, force)
		if err != nil { return 1, err }
		return 0, nil
	} else {
		if aErr := validateArgs(apr, 1); aErr != nil {
			return 1, aErr
		}

		branchName := apr.Arg(0)

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

func makeACopyOfBranch(ctx *sql.Context, dbData env.DbData, srcBranch string, destBranch string, force bool) error {
	err := actions.CopyBranchOnDB(ctx, dbData.Ddb, srcBranch, destBranch, force)

	if err != nil {
		if err == doltdb.ErrBranchNotFound {
			return errors.New(fmt.Sprintf("fatal: A branch named '%s' not found", srcBranch))
		} else if err == actions.ErrAlreadyExists {
			return errors.New(fmt.Sprintf("fatal: A branch named '%s' already exists.", destBranch))
		} else if err == doltdb.ErrInvBranchName {
			return errors.New(fmt.Sprintf("fatal: '%s' is not a valid branch name.", destBranch))
		} else {
			return errors.New(fmt.Sprintf("fatal: Unexpected error copying branch from '%s' to '%s'", srcBranch, destBranch))
		}
	}
	return nil
}

func validateArgs(apr *argparser.ArgParseResults, numArg int) error {
	if apr.NArg() != numArg {
		return errors.New("Invalid usage.")
	}

	for _, arg := range apr.Args {
		if len(arg) == 0 {
			return ErrEmptyBranchName
		}
	}

	return nil
}
