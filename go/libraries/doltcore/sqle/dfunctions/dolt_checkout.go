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
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

const DoltCheckoutFuncName = "dolt_checkout"

var ErrEmptyBranchName = errors.New("error: cannot checkout empty string")

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

	apr, err := ap.Parse(args)
	if err != nil {
		return 1, err
	}

	if (apr.Contains(cli.CheckoutCoBranch) && apr.NArg() > 1) || (!apr.Contains(cli.CheckoutCoBranch) && apr.NArg() == 0) {
		return 1, errors.New("Improper usage.")
	}

	// Checking out new branch.
	if newBranch, newBranchOk := apr.GetValue(cli.CheckoutCoBranch); newBranchOk {
		if len(newBranch) == 0 {
			err = errors.New("error: cannot checkout empty string")
		} else {
			err = checkoutNewBranch(ctx, dbData, newBranch, "")
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
		err = checkoutBranch(ctx, dbData, name)
		if err != nil {
			return 1, err
		}
		return 0, nil
	}

	// Check if user want to checkout table or docs.
	tbls, docs, err := actions.GetTablesOrDocs(dbData.Drw, args)
	if err != nil {
		return 1, errors.New("error: unable to parse arguments.")
	}

	if len(docs) > 0 {
		return 1, errors.New("error: docs not supported in sql mode")
	}

	err = checkoutTables(ctx, dbData, tbls)

	if err != nil && apr.NArg() == 1 {
		err = checkoutRemoteBranch(ctx, dbData, name)
	}

	if err != nil {
		return 1, err
	}

	return 0, nil
}

func checkoutRemoteBranch(ctx *sql.Context, dbData env.DbData, branchName string) error {
	if len(branchName) == 0 {
		return ErrEmptyBranchName
	}

	if ref, refExists, err := actions.GetRemoteBranchRef(ctx, dbData.Ddb, branchName); err != nil {
		return errors.New("fatal: unable to read from data repository")
	} else if refExists {
		return checkoutNewBranch(ctx, dbData, branchName, ref.String())
	} else {
		return fmt.Errorf("error: could not find %s", branchName)
	}
}

func checkoutNewBranch(ctx *sql.Context, dbData env.DbData, branchName, startPt string) error {
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

	return checkoutBranch(ctx, dbData, branchName)
}

func checkoutBranch(ctx *sql.Context, dbData env.DbData, branchName string) error {
	return nil
	// TODO: fix me
	// if len(branchName) == 0 {
	// 	return ErrEmptyBranchName
	// }
	//
	// // TODO: this isn't right. It updates the working root for the head ref, but needs to only update it for the
	// //  session. Otherwise this won't respect transaction boundaries.
	// err := actions.CheckoutBranchNoDocs(ctx, dbData, branchName)
	//
	// if err != nil {
	// 	if err == doltdb.ErrBranchNotFound {
	// 		return fmt.Errorf("fatal: Branch '%s' not found.", branchName)
	// 	} else if doltdb.IsRootValUnreachable(err) {
	// 		rt := doltdb.GetUnreachableRootType(err)
	// 		return fmt.Errorf("error: unable to read the %s", rt.String())
	// 	} else if actions.IsCheckoutWouldOverwrite(err) {
	// 		tbls := actions.CheckoutWouldOverwriteTables(err)
	// 		msg := "error: Your local changes to the following tables would be overwritten by checkout: \n"
	// 		for _, tbl := range tbls {
	// 			msg = msg + tbl + "\n"
	// 		}
	// 		return errors.New(msg)
	// 	} else if err == doltdb.ErrAlreadyOnBranch {
	// 		return nil // No need to return an error if on the same branch
	// 	} else {
	// 		return fmt.Errorf("fatal: Unexpected error checking out branch '%s'", branchName)
	// 	}
	// }
	//
	// return updateHeadAndWorkingSessionVars(ctx, dbData)
}

func checkoutTables(ctx *sql.Context, dbData env.DbData, tables []string) error {
	return nil
	// TODO: fix me
	// err := actions.CheckoutTables(ctx, dbData, tables)
	//
	// if err != nil {
	// 	if doltdb.IsRootValUnreachable(err) {
	// 		rt := doltdb.GetUnreachableRootType(err)
	// 		return fmt.Errorf("error: unable to read the %s", rt.String())
	// 	} else if actions.IsTblNotExist(err) {
	// 		return fmt.Errorf("error: given tables do not exist")
	// 	} else {
	// 		return fmt.Errorf("fatal: Unexpected error checking out tables")
	// 	}
	// }
	//
	// return updateHeadAndWorkingSessionVars(ctx, dbData)
}

// updateHeadAndWorkingSessionVars explicitly sets the head and working hash.
func updateHeadAndWorkingSessionVars(ctx *sql.Context, dbData env.DbData) error {
	headHash, err := dbData.Rsr.CWBHeadHash(ctx)
	if err != nil {
		return err
	}
	hs := headHash.String()

	hasWorkingChanges := hasWorkingSetChanges(dbData.Rsr)
	hasStagedChanges, err := hasStagedSetChanges(ctx, dbData.Ddb, dbData.Rsr)

	if err != nil {
		return err
	}

	// TODO: fix this
	// workingHash := dbData.Rsr.WorkingHash().String()
	workingHash := ""

	// This will update the session table editor's root and clear its cache.
	if !hasStagedChanges && !hasWorkingChanges {
		return setHeadAndWorkingSessionRoot(ctx, hs)
	}

	err = setSessionRootExplicit(ctx, hs, sqle.HeadKeySuffix)
	if err != nil {
		return err
	}

	return setSessionRootExplicit(ctx, workingHash, sqle.WorkingKeySuffix)
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

func (d DoltCheckoutFunc) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewDoltCheckoutFunc(ctx, children...)
}

func NewDoltCheckoutFunc(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	return &DoltCheckoutFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}
