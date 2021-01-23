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
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"strings"
)

type DoltCheckoutFunc struct {
	expression.NaryExpression
}

func (d DoltCheckoutFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	dSess := sqle.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(dbName)

	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	ap := cli.CreateCheckoutArgParser()
	args, err := getDoltArgs(ctx, row, d.Children())

	if err != nil {
		return 1, err
	}

	apr := cli.ParseArgs(ap, args, nil)

	if (apr.Contains(cli.CheckoutCoBranch) && apr.NArg() > 1) || (!apr.Contains(cli.CheckoutCoBranch) && apr.NArg() == 0) {
		return 1, errors.New("Improper usage.")
	}

	// Checking out new branch.
	if newBranch, newBranchOk := apr.GetValue(cli.CheckoutCoBranch); newBranchOk {
		if len(newBranch) == 0 {
			err = errors.New("error: cannot checkout empty string")
		} else {
			err = checkoutNewBranch(ctx, dbData, newBranch, apr)
		}

		if err != nil {
			return 1, err
		}

		return 0, nil
	}

	name := apr.Arg(0)

	if len(name) == 0 {
		return 1, errors.New("error: cannot checkout empty string")
	}

	if isBranch, err := actions.IsBranch(ctx, dbData.Ddb, name); err != nil {
		return 1, err
	} else if isBranch {
		//verr := checkoutBranch(ctx, dEnv, name) // TODO
		//return HandleVErrAndExitCode(verr, usagePrt)
	}

	return 0, nil
}

func checkoutNewBranch(ctx context.Context, dbData env.DbData, newBranch string, apr *argparser.ArgParseResults) error {
	startPt := "head"
	if apr.NArg() == 1 {
		startPt = apr.Arg(0)
	}

	err := actions.CreateBranchWithStartPt(ctx, dbData, newBranch, startPt, false)

	if err != nil {
		return err
	}

	// TODO: checkout to branch
	// err = actions.CheckoutBranch()

	fmt.Sprintf("Switched to branch '%s'\n", newBranch)
	return nil
}

func (d DoltCheckoutFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_CHECKOUT(%s)", strings.Join(childrenStrings, ","))
}

// TODO: This might not a branch
func (d DoltCheckoutFunc) Type() sql.Type {
	return sql.Int8
}


func (d DoltCheckoutFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltCheckoutFunc(children...)
}

func NewDoltCheckoutFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltCheckoutFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}
