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
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

const DoltCheckoutFuncName = "dolt_checkout"

var ErrEmptyBranchName = errors.New("error: cannot checkout empty string")

// Deprecated: please use the version in the dprocedures package
type DoltCheckoutFunc struct {
	expression.NaryExpression
}

func (d DoltCheckoutFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}
	return DoDoltCheckout(ctx, args)
}

func DoDoltCheckout(ctx *sql.Context, args []string) (int, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	apr, err := cli.CreateCheckoutArgParser().Parse(args)
	if err != nil {
		return 1, err
	}

	if (apr.Contains(cli.CheckoutCoBranch) && apr.NArg() > 1) || (!apr.Contains(cli.CheckoutCoBranch) && apr.NArg() == 0) {
		return 1, errors.New("Improper usage.")
	}

	// Checking out new branch.
	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	if newBranch, newBranchOk := apr.GetValue(cli.CheckoutCoBranch); newBranchOk {
		if len(newBranch) == 0 {
			err = errors.New("error: cannot checkout empty string")
		} else {
			err = checkoutNewBranch(ctx, dbName, dbData, roots, newBranch, "")
		}

		if err != nil {
			return 1, err
		}

		return 0, nil
	}

	name := apr.Arg(0)

	// Check if user wants to checkout branch.
	if isBranch, err := actions.IsBranch(ctx, dbData.Ddb, name); err != nil {
		return 1, err
	} else if isBranch {
		err = checkoutBranch(ctx, dbName, roots, dbData, name)
		if err != nil {
			return 1, err
		}
		return 0, nil
	}

	err = checkoutTables(ctx, roots, dbName, args)
	if err != nil && apr.NArg() == 1 {
		err = checkoutRemoteBranch(ctx, dbName, dbData, roots, name)
	}

	if err != nil {
		return 1, err
	}

	return 0, nil
}

func checkoutRemoteBranch(ctx *sql.Context, dbName string, dbData env.DbData, roots doltdb.Roots, branchName string) error {
	if len(branchName) == 0 {
		return ErrEmptyBranchName
	}

	if ref, refExists, err := actions.GetRemoteBranchRef(ctx, dbData.Ddb, branchName); err != nil {
		return errors.New("fatal: unable to read from data repository")
	} else if refExists {
		return checkoutNewBranch(ctx, dbName, dbData, roots, branchName, ref.String())
	} else {
		return fmt.Errorf("error: could not find %s", branchName)
	}
}

func checkoutNewBranch(ctx *sql.Context, dbName string, dbData env.DbData, roots doltdb.Roots, branchName, startPt string) error {
	if len(branchName) == 0 {
		return ErrEmptyBranchName
	}

	if startPt == "" {
		startPt = "head"
	}

	err := actions.CreateBranchWithStartPt(ctx, dbData, branchName, startPt, false)
	if err != nil {
		return err
	}

	return checkoutBranch(ctx, dbName, roots, dbData, branchName)
}

func checkoutBranch(ctx *sql.Context, dbName string, roots doltdb.Roots, dbData env.DbData, branchName string) error {
	if len(branchName) == 0 {
		return ErrEmptyBranchName
	}
	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(branchName))
	if err != nil {
		return err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	return dSess.SwitchWorkingSet(ctx, dbName, wsRef)
}

func checkoutTables(ctx *sql.Context, roots doltdb.Roots, name string, tables []string) error {
	roots, err := actions.MoveTablesFromHeadToWorking(ctx, roots, tables)

	if err != nil {
		if doltdb.IsRootValUnreachable(err) {
			rt := doltdb.GetUnreachableRootType(err)
			return fmt.Errorf("error: unable to read the %s", rt.String())
		} else if actions.IsTblNotExist(err) {
			return fmt.Errorf("error: given tables do not exist")
		} else {
			return fmt.Errorf("fatal: Unexpected error checking out tables")
		}
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	return dSess.SetRoots(ctx, name, roots)
}

func (d DoltCheckoutFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_CHECKOUT(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltCheckoutFunc) Type() sql.Type {
	return sql.Int8
}

func (d DoltCheckoutFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltCheckoutFunc(children...)
}

// Deprecated: please use the version in the dprocedures package
func NewDoltCheckoutFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltCheckoutFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}
