// Copyright 2023 Dolthub, Inc.
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

package dprocedures

import (
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var doltRebaseProcedureSchema = []*sql.Column{
	{
		Name:     "status",
		Type:     types.Int64,
		Nullable: false,
	},
	{
		Name:     "message",
		Type:     types.LongText,
		Nullable: true,
	},
}

var RebaseActionEnumType = types.MustCreateEnumType([]string{"pick", "skip", "squash"}, sql.Collation_Default)

var DoltRebaseSystemTableSchema = []*sql.Column{
	{

		Name:     "rebase_order",
		Type:     types.Uint16,
		Nullable: false,
	},
	{
		Name:     "action",
		Type:     RebaseActionEnumType,
		Nullable: false,
	},
	{
		Name:     "commit_hash",
		Type:     types.Text,
		Nullable: false,
	},
	{
		Name:     "commit_message",
		Type:     types.Text,
		Nullable: false,
	},
}

// rebaseWorkingBranch is the name of the temporary branch used when performing a rebase. Normally, a rebase happens
// in the context of a detatched HEAD, but because Dolt doesn't support that well, we use a temporary branch.
// TODO: Eventually, we need to change this name so that it uses a UUID or at least the current branch name.
const rebaseWorkingBranch = "dolt_rebase_42"

func doltRebase(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltRebase(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltRebase(ctx *sql.Context, args []string) (int, error) {
	// TODO: Replace with arg parser usage
	if len(args) == 0 {
		return 1, fmt.Errorf("not enough args")
	}

	if len(args) > 2 {
		return 1, fmt.Errorf("too many args")
	}

	if strings.ToLower(args[0]) == "--abort" {
		err := abortRebase(ctx)
		if err != nil {
			return 1, err
		} else {
			return 0, nil
		}
	} else if strings.ToLower(args[0]) == "--continue" {
		err := continueRebase(ctx)
		if err != nil {
			return 1, err
		} else {
			return 0, nil
		}
	} else {
		// must be start rebase...
		err := startRebase(ctx, args[0])
		if err != nil {
			return 1, err
		} else {
			return 0, nil
		}
	}

	return 0, nil
}

func startRebase(ctx *sql.Context, upstreamPoint string) error {
	if upstreamPoint == "" {
		return fmt.Errorf("no upstream branch specified")
	}

	doltSession := dsess.DSessFromSess(ctx.Session)

	headRef, err := doltSession.CWBHeadRef(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	dbData, ok := doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		panic("not okay getting database!")
	}

	rebaseBranch := headRef.GetPath()
	startCommit, err := dbData.Ddb.ResolveCommitRef(ctx, ref.NewBranchRef(rebaseBranch))
	if err != nil {
		return err
	}

	commitSpec, err := doltdb.NewCommitSpec(upstreamPoint)
	if err != nil {
		return err
	}

	upstreamCommit, err := dbData.Ddb.Resolve(ctx, commitSpec, headRef)
	if err != nil {
		return err
	}

	// TODO: Use a better API than the stored procedure function :-/
	rowIter, err := doltBranch(ctx, rebaseWorkingBranch, upstreamPoint)
	if err != nil {
		return err
	}
	// TODO: handle dolt_branch results
	if err = drainRowIterator(ctx, rowIter); err != nil {
		return err
	}

	// Checkout our new branch
	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(rebaseWorkingBranch))
	if err != nil {
		return err
	}
	err = doltSession.SwitchWorkingSet(ctx, ctx.GetCurrentDatabase(), wsRef)
	if err != nil {
		return err
	}

	dbData, ok = doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		panic("database not okay!")
	}

	db, err := doltSession.Provider().Database(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	newWorkingSet, err := workingSet.StartRebase(ctx, upstreamCommit, rebaseBranch)
	if err != nil {
		return err
	}

	err = doltSession.SetWorkingSet(ctx, ctx.GetCurrentDatabase(), newWorkingSet)
	if err != nil {
		return err
	}

	// Create the rebase plan table
	rdb, ok := db.(dsess.RebaseableDatabase)
	if !ok {
		return fmt.Errorf("expected a dsess.RebaseableDatabase implementation, but received a %T", db)
	}
	err = rdb.CreateRebasePlan(ctx, startCommit, upstreamCommit)
	if err != nil {
		return err
	}

	return nil
}

func abortRebase(ctx *sql.Context) error {
	doltSession := dsess.DSessFromSess(ctx.Session)

	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	if !workingSet.RebaseActive() {
		return fmt.Errorf("no rebase in progress")
	}

	// TODO: remove the dolt_rebase table

	// Move HEAD back to the original head
	rebaseState := workingSet.RebaseState()

	workingSet = workingSet.AbortRebase()

	// Switch back to the original branch head
	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(rebaseState.Branch()))
	if err != nil {
		return err
	}

	err = doltSession.SwitchWorkingSet(ctx, ctx.GetCurrentDatabase(), wsRef)
	if err != nil {
		return err
	}

	return nil
}

func continueRebase(ctx *sql.Context) error {
	// TODO: validate the dolt_rebase table
	//       - ensure the commits are valid, meaning valid commits, and
	//         in the chain between HEAD and ontoCommit
	//       - make sure the rebase order doesn't have duplicate numbers

	// TODO: Seems like the working set RebaseState will eventually need to keep track of
	//       where it is in the rebase plan...
	//       RebaseOrder is probably what makes the most sense to track... we don't
	//       technically need that until rebasing causes control to stop and go back
	//       to the user though.

	// TODO: How are we going to support editing commit messages?

	// Validate that we are in an interactive rebase
	doltSession := dsess.DSessFromSess(ctx.Session)
	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	if !workingSet.RebaseActive() {
		return fmt.Errorf("no rebase in progress")
	}

	// iterate over the rebase plan in dolt_rebase and process each commit
	db, err := doltSession.Provider().Database(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	table, ok, err := db.GetTableInsensitive(ctx, doltdb.RebaseTableName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("unable to find dolt_rebase table")
	}
	resolvedTable := plan.NewResolvedTable(table, db, nil)
	sort := plan.NewSort([]sql.SortField{{
		Column: expression.NewGetField(0, types.Int64, "rebase_order", false),
		Order:  sql.Ascending,
	}}, resolvedTable)
	iter, err := rowexec.DefaultBuilder.Build(ctx, sort, nil)
	if err != nil {
		return err
	}
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		err = processRow(ctx, row)
		if err != nil {
			return err
		}
	}

	// Update the branch being rebased to point to the same commit as our temporary working branch
	rebaseBranchWorkingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	dbData, ok := doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		panic("not okay!! !!")
	}
	err = copyABranch(ctx, dbData, rebaseWorkingBranch, rebaseBranchWorkingSet.RebaseState().Branch(), true, nil)
	if err != nil {
		return err
	}

	// Checkout the branch being rebased
	previousBranchWorkingSetRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(rebaseBranchWorkingSet.RebaseState().Branch()))
	if err != nil {
		return err
	}
	err = doltSession.SwitchWorkingSet(ctx, ctx.GetCurrentDatabase(), previousBranchWorkingSetRef)
	if err != nil {
		return err
	}

	// delete the temporary working branch
	dbData, ok = doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return fmt.Errorf("unable to lookup dbdata")
	}
	err = actions.DeleteBranch(ctx, dbData, rebaseWorkingBranch, actions.DeleteOptions{
		Force: true,
	}, doltSession.Provider(), nil)
	if err != nil {
		return err
	}

	return nil
}

func processRow(ctx *sql.Context, row sql.Row) error {
	i, ok := row[1].(uint16)
	if !ok {
		// TODO: check for NULL, too
		panic(fmt.Sprintf("invalid enum value: %v (%T)", row[1], row[1]))
	}
	rebaseAction, ok := RebaseActionEnumType.At(int(i))
	if !ok {
		panic("invalid enum value!")
	}

	switch rebaseAction {
	case "pick":
		fmt.Printf("cherry-picking commit: %s\n", row[2].(string))
		// NOTE: After our first call to cherry-pick, the tx is committed, so a new tx needs to be started
		doltSession := dsess.DSessFromSess(ctx.Session)
		if doltSession.GetTransaction() == nil {
			_, err := doltSession.StartTransaction(ctx, sql.ReadWrite)
			if err != nil {
				return err
			}
		}
		// Perform the cherry-pick
		resultsIter, err := doltCherryPick(ctx, row[2].(string))
		if err != nil {
			return err
		}
		// TODO: handle cherry-pick results
		return drainRowIterator(ctx, resultsIter)

	case "skip":
		fmt.Printf("skipping commit: %s\n", row[2].(string))
		return nil

	case "squash":
		fmt.Printf("squashing commit: %s\n", row[2].(string))
		// TODO: validate that squash is NOT the first action!

		// NOTE: After our first call to cherry-pick, the tx is committed, so a new tx needs to be started
		doltSession := dsess.DSessFromSess(ctx.Session)
		if doltSession.GetTransaction() == nil {
			_, err := doltSession.StartTransaction(ctx, sql.ReadWrite)
			if err != nil {
				return err
			}
		}
		// Perform the cherry-pick
		// TODO: Need to combine the commit messages
		resultsIter, err := doltCherryPickWithAmend(ctx, row[2].(string))
		if err != nil {
			return err
		}
		// TODO: handle cherry-pick results
		return drainRowIterator(ctx, resultsIter)

	default:
		return fmt.Errorf("rebase action '%s' is not supported", rebaseAction)
	}
}

func drainRowIterator(ctx *sql.Context, iter sql.RowIter) error {
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		fmt.Printf("cherry-pick result: %v\n", row)
	}
}
