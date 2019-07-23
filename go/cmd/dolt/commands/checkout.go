// Copyright 2019 Liquidata, Inc.
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

package commands

import (
	"context"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var coShortDesc = `Switch branches or restore working tree tables`
var coLongDesc = `Updates tables in the working set to match the staged versions. If no paths are given, dolt checkout will also update HEAD to set the specified branch as the current branch.

dolt checkout <branch>
   To prepare for working on <branch>, switch to it by updating the index and the tables in the working tree, and by pointing HEAD at the branch. Local modifications to the tables in the working
   tree are kept, so that they can be committed to the <branch>.

dolt checkout -b <new_branch> [<start point>]
   Specifying -b causes a new branch to be created as if dolt branch were called and then checked out.

dolt checkout <table>...
  To update table(s) with their values in HEAD `

var coSynopsis = []string{
	`<branch>`,
	`<table>...`,
	`-b <new-branch> [<start-point>]`,
}

func Checkout(commandStr string, args []string, dEnv *env.DoltEnv) int {
	const coBranchArg = "b"
	ap := argparser.NewArgParser()
	ap.SupportsString(coBranchArg, "", "branch", "Create a new branch named <new_branch> and start it at <start_point>.")
	helpPrt, usagePrt := cli.HelpAndUsagePrinters(commandStr, coShortDesc, coLongDesc, coSynopsis, ap)
	apr := cli.ParseArgs(ap, args, helpPrt)

	if (apr.Contains(coBranchArg) && apr.NArg() > 1) || (!apr.Contains(coBranchArg) && apr.NArg() == 0) {
		usagePrt()
		return 1
	} else {
		var verr errhand.VerboseError
		newBranch, nbOk := apr.GetValue(coBranchArg)

		if nbOk {
			startPt := "head"
			if apr.NArg() == 1 {
				startPt = apr.Arg(0)
			}

			verr = checkoutNewBranch(dEnv, newBranch, startPt)
		} else {
			name := apr.Arg(0)
			isBranch, rootsWithTable, err := actions.BranchOrTable(context.Background(), dEnv, name)

			if err != nil {
				verr = errhand.BuildDError("fatal: unable to read from data repository.").AddCause(err).Build()
			} else if !rootsWithTable.IsEmpty() || apr.NArg() > 1 {
				verr = checkoutTable(dEnv, apr.Args())
			} else if isBranch {
				verr = checkoutBranch(dEnv, name)
			} else {
				verr = errhand.BuildDError("error: could not find %s", name).Build()
			}
		}

		if verr != nil {
			cli.PrintErrln(verr.Verbose())
			return 1
		}
	}

	return 0
}

func checkoutNewBranch(dEnv *env.DoltEnv, newBranch, startPt string) errhand.VerboseError {
	verr := createBranchWithStartPt(dEnv, newBranch, startPt, false)

	if verr != nil {
		return verr
	}

	return checkoutBranch(dEnv, newBranch)
}

func checkoutTable(dEnv *env.DoltEnv, tables []string) errhand.VerboseError {
	err := actions.CheckoutTables(context.Background(), dEnv, tables)

	if err != nil {
		if actions.IsRootValUnreachable(err) {
			return unreadableRootToVErr(err)
		} else if actions.IsTblNotExist(err) {
			badTbls := actions.GetTablesForError(err)
			bdr := errhand.BuildDError("")
			for _, tbl := range badTbls {
				bdr.AddDetails("error: table '%s' did not match any table(s) known to dolt.", tbl)
			}

			return bdr.Build()
		} else {
			bdr := errhand.BuildDError("fatal: Unexpected error checking out tables")
			bdr.AddCause(err)
			return bdr.Build()
		}
	}

	return nil
}

func checkoutBranch(dEnv *env.DoltEnv, name string) errhand.VerboseError {
	err := actions.CheckoutBranch(context.Background(), dEnv, name)

	if err != nil {
		if err == doltdb.ErrBranchNotFound {
			return errhand.BuildDError("fatal: Branch '%s' not found.", name).Build()
		} else if actions.IsRootValUnreachable(err) {
			return unreadableRootToVErr(err)
		} else if actions.IsCheckoutWouldOverwrite(err) {
			tbls := actions.CheckoutWouldOverwriteTables(err)
			bdr := errhand.BuildDError("error: Your local changes to the following tables would be overwritten by checkout:")
			for _, tbl := range tbls {
				bdr.AddDetails("\t" + tbl)
			}

			bdr.AddDetails("Please commit your changes or stash them before you switch branches.")
			bdr.AddDetails("Aborting")
			return bdr.Build()
		} else if err == doltdb.ErrAlreadyOnBranch {
			return errhand.BuildDError("Already on branch '%s'", name).Build()
		} else {
			bdr := errhand.BuildDError("fatal: Unexpected error checking out branch '%s'", name)
			bdr.AddCause(err)
			return bdr.Build()
		}
	}

	cli.Printf("Switched to branch '%s'\n", name)
	return nil
}

func unreadableRootToVErr(err error) errhand.VerboseError {
	rt := actions.GetUnreachableRootType(err)
	bdr := errhand.BuildDError("error: unable to read the %s", rt.String())
	return bdr.AddCause(actions.GetUnreachableRootCause(err)).Build()
}
