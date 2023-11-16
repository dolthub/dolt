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

func doltRebase(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	res, err := doDoltRebase(ctx, args)
	if err != nil {
		return nil, err
	}
	return rowToIter(int64(res)), nil
}

func doDoltRebase(ctx *sql.Context, args []string) (int, error) {
	// TODO: Set working set metadata for active rebase (similar to merge metadata)
	//       - how does this work for merge again?

	if len(args) > 1 {
		return 1, fmt.Errorf("too many args")
	}

	if len(args) == 1 {
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
		}
	}

	if len(args) == 0 {
		err := startRebase(ctx)
		if err != nil {
			return 1, err
		} else {
			return 0, nil
		}
	}

	return 0, nil
}

func startRebase(ctx *sql.Context) error {
	doltSession := dsess.DSessFromSess(ctx.Session)

	// TODO: For now, just rewind back a couple of commits...
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

	commitSpec, err := doltdb.NewCommitSpec("HEAD~~~")
	if err != nil {
		return err
	}

	ontoCommit, err := dbData.Ddb.Resolve(ctx, commitSpec, headRef)
	if err != nil {
		return err
	}

	// TODO: Use a better API than the stored procedure function :-/
	rowIter, err := doltBranch(ctx, "dolt_rebase_42", "HEAD~~~")
	if err != nil {
		return err
	}
	// TODO: handle dolt_branch results
	if err = drainRowIterator(ctx, rowIter); err != nil {
		return err
	}

	// --- Checkout new branch
	wsRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef("dolt_rebase_42"))
	if err != nil {
		return err
	}
	err = doltSession.SwitchWorkingSet(ctx, ctx.GetCurrentDatabase(), wsRef)
	if err != nil {
		return err
	}
	// ---

	// TODO: What's the right way to create the dolt_reset system table... seems like we want it to be a real table, but
	//       we don't want to allow it to be checked in and committed, right?
	//       If it were a traditional, read-only system table, we'd probably regenerate it each time it was queried, which
	//       isn't the behavior we want. We want it generated once and then let the customer change it. Once we start
	//       executing the rebase plan, we CANNOT allow it to be changed though.

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

	newWorkingSet, err := workingSet.StartRebase(ctx, ontoCommit, rebaseBranch)
	if err != nil {
		return err
	}

	err = doltSession.SetWorkingSet(ctx, ctx.GetCurrentDatabase(), newWorkingSet)
	if err != nil {
		return err
	}

	// ---

	rdb, ok := db.(dsess.RebaseableDatabase)
	if !ok {
		return fmt.Errorf("expected a dsess.RebaseableDatabase implementation, but received a %T", db)
	}
	err = rdb.CreateRebasePlan(ctx, startCommit, ontoCommit)
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
		return fmt.Errorf("no active rebase")
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

	// TODO: How are we going to edit commit messages?

	// Switch HEAD to our ontoCommit
	doltSession := dsess.DSessFromSess(ctx.Session)
	workingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	if !workingSet.RebaseActive() {
		return fmt.Errorf("no rebase in progress")
	}

	db, err := doltSession.Provider().Database(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}

	// iterate over the rebase plan in dolt_rebase and cherry-pick each commit
	// Read the contents of the dolt_rebase table...

	table, ok, err := db.GetTableInsensitive(ctx, doltdb.RebaseTableName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("unable to find dolt_rebase table")
	}

	// ---

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

	// TODO: Make '-i' arg required?
	// TODO: Make the ontoCommit required?

	// TODO: WHy is the dolt_rebase table not able to be updated?
	//       - need this soon for next rebase actions! (squash and reordering)

	// checkout the branch being rebased
	rebaseBranchWorkingSet, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	previousBranchWorkingSetRef, err := ref.WorkingSetRefForHead(ref.NewBranchRef(rebaseBranchWorkingSet.RebaseState().Branch()))
	if err != nil {
		return err
	}
	err = doltSession.SwitchWorkingSet(ctx, ctx.GetCurrentDatabase(), previousBranchWorkingSetRef)
	if err != nil {
		return err
	}
	// ---

	// reset the rebaseBranch to point to the same commit as dolt_rebase_42
	dbData, ok := doltSession.GetDbData(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return fmt.Errorf("unable to lookup dbdata")
	}
	roots, ok := doltSession.GetRoots(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return fmt.Errorf("Could not load database %s", ctx.GetCurrentDatabase())
	}
	newHead, roots, err := actions.ResetHardTables(ctx, dbData, "dolt_rebase_42", roots)
	if err != nil {
		return err
	}
	if newHead != nil {
		headRef, err := dbData.Rsr.CWBHeadRef()
		if err != nil {
			return err
		}
		if err := dbData.Ddb.SetHeadToCommit(ctx, headRef, newHead); err != nil {
			return err
		}
	}
	ws, err := doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	err = doltSession.SetWorkingSet(ctx, ctx.GetCurrentDatabase(), ws.WithWorkingRoot(roots.Working).WithStagedRoot(roots.Staged).ClearMerge().ClearRebase())
	if err != nil {
		return err
	}
	// ---

	// delete the dolt_rebase_42 branch
	err = actions.DeleteBranch(ctx, dbData, "dolt_rebase_42", actions.DeleteOptions{
		Force: true,
	}, doltSession.Provider(), nil)
	if err != nil {
		return err
	}

	// Clear out rebase state (actually...isn't the state on the branch that was deleted? do we really need this?
	workingSet, err = doltSession.WorkingSet(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	workingSet = workingSet.ClearRebase()
	err = doltSession.SetWorkingSet(ctx, ctx.GetCurrentDatabase(), workingSet)
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

	default:
		return fmt.Errorf("only the 'pick' rebase action is supported, but '%s' was requested", rebaseAction)
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
