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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/cherry_pick"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/rebase"
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

var RebaseActionEnumType = types.MustCreateEnumType([]string{
	rebase.RebaseActionDrop,
	rebase.RebaseActionPick,
	rebase.RebaseActionReword,
	rebase.RebaseActionSquash,
	rebase.RebaseActionFixup}, sql.Collation_Default)

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

	// rebaseWorkingBranch is the name of the temporary branch used when performing a rebase. In Git, a rebase
	// happens with a detatched HEAD, but Dolt doesn't support that, we use a temporary branch.
	rebaseWorkingBranch := "dolt_rebase_" + rebaseBranch
	var rsc doltdb.ReplicationStatusController
	err = actions.CreateBranchWithStartPt(ctx, dbData, rebaseWorkingBranch, upstreamPoint, false, &rsc)
	if err != nil {
		return err
	}
	err = commitTransaction(ctx, doltSession, &rsc)
	if err != nil {
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

	// Create the rebase plan
	rebasePlan, err := rebase.CreateRebasePlan(ctx, startCommit, upstreamCommit)
	if err != nil {
		return err
	}

	rdb, ok := db.(dsess.RebaseableDatabase)
	if !ok {
		return fmt.Errorf("expected a dsess.RebaseableDatabase implementation, but received a %T", db)
	}
	return rdb.SaveRebasePlan(ctx, rebasePlan)
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
	// TODO: preRebaseWorkingRoot isn't used anymore, right? We can remove it?
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

// TODO: Make '-i' arg required?

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

	// Validate that we are in an interactive rebase
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

	rdb, ok := db.(dsess.RebaseableDatabase)
	if !ok {
		return fmt.Errorf("expected a dsess.RebaseableDatabase implementation, but received a %T", db)
	}
	rebasePlan, err := rdb.LoadRebasePlan(ctx)
	if err != nil {
		return err
	}

	for _, step := range rebasePlan.Members {
		err = processRebaseAction(ctx, &step)
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

	rebaseBranch := rebaseBranchWorkingSet.RebaseState().Branch()
	rebaseWorkingBranch := "dolt_rebase_" + rebaseBranch

	err = copyABranch(ctx, dbData, rebaseWorkingBranch, rebaseBranch, true, nil)
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

func processRebaseAction(ctx *sql.Context, planStep *doltdb.RebasePlanMember) error {
	// Make sure we have a transaction opened for the session
	// NOTE: After our first call to cherry-pick, the tx is committed, so a new tx needs to be started
	//       as we process additional rebase actions.
	doltSession := dsess.DSessFromSess(ctx.Session)
	if doltSession.GetTransaction() == nil {
		_, err := doltSession.StartTransaction(ctx, sql.ReadWrite)
		if err != nil {
			return err
		}
	}

	switch planStep.Action {
	case rebase.RebaseActionDrop:
		return nil

	case rebase.RebaseActionPick, rebase.RebaseActionReword:
		options := cherry_pick.CherryPickOptions{}
		if planStep.Action == rebase.RebaseActionReword {
			options.CommitMessage = planStep.CommitMsg
		}
		_, _, err := cherry_pick.CherryPick(ctx, planStep.CommitHash, options)
		return err

	case rebase.RebaseActionSquash:
		// TODO: validate that squash (or fixup!) is NOT the first action!
		//       would be better to put rebase plan validation into an earlier/separate step,
		//       instead of mixing it in with execution.
		commitMessage, err := squashCommitMessage(ctx, planStep.CommitHash)
		if err != nil {
			return err
		}
		_, _, err = cherry_pick.CherryPick(ctx, planStep.CommitHash, cherry_pick.CherryPickOptions{
			Amend:         true,
			CommitMessage: commitMessage,
		})
		return err

	case rebase.RebaseActionFixup:
		// TODO: It shouldn't be necessary for us to lookup the previous commit message and
		//       specify it here. If we specify that we want to cherry pick as an amend, then cherry
		//       pick should amend the previous commit without overwriting the commit message.
		commitMessage, err := previousCommitMessage(ctx)
		if err != nil {
			return err
		}
		_, _, err = cherry_pick.CherryPick(ctx, planStep.CommitHash, cherry_pick.CherryPickOptions{
			Amend:         true,
			CommitMessage: commitMessage,
		})
		return err

	default:
		return fmt.Errorf("rebase action '%s' is not supported", planStep.Action)
	}
}

func previousCommitMessage(ctx *sql.Context) (string, error) {
	// TODO: Remove this function after we fix the issue with cherry_pick.CherryPick overriding the commit message
	//       by default when the Amend option is given.
	doltSession := dsess.DSessFromSess(ctx.Session)
	headCommit, err := doltSession.GetHeadCommit(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return "", err
	}
	headCommitMeta, err := headCommit.GetCommitMeta(ctx)
	if err != nil {
		return "", err
	}

	return headCommitMeta.Description, nil
}

// squashCommitMessage looks up the commit at HEAD and the commit identified by |nextCommitHash| and squashes their two
// commit messages together.
func squashCommitMessage(ctx *sql.Context, nextCommitHash string) (string, error) {
	doltSession := dsess.DSessFromSess(ctx.Session)
	headCommit, err := doltSession.GetHeadCommit(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return "", err
	}
	headCommitMeta, err := headCommit.GetCommitMeta(ctx)
	if err != nil {
		return "", err
	}

	ddb, ok := doltSession.GetDoltDB(ctx, ctx.GetCurrentDatabase())
	if !ok {
		return "", fmt.Errorf("unable to get doltdb!")
	}
	spec, err := doltdb.NewCommitSpec(nextCommitHash)
	if err != nil {
		return "", err
	}
	headRef, err := doltSession.CWBHeadRef(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return "", err
	}
	nextCommit, err := ddb.Resolve(ctx, spec, headRef)
	if err != nil {
		return "", err
	}
	nextCommitMeta, err := nextCommit.GetCommitMeta(ctx)
	if err != nil {
		return "", err
	}
	commitMessage := headCommitMeta.Description + "\n\n" + nextCommitMeta.Description

	return commitMessage, nil
}
