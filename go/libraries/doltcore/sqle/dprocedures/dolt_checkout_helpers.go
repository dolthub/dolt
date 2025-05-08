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
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

// MoveWorkingSetToBranch moves the working set from the currently checked out branch onto the branch specified
// by `brName`. This is a POTENTIALLY DESTRUCTIVE ACTION used during command line checkout
func MoveWorkingSetToBranch(ctx *sql.Context, brName string, force bool, isNewBranch bool) error {
	branchRef := ref.NewBranchRef(brName)
	dSess := dsess.DSessFromSess(ctx.Session)
	dbName := dSess.GetCurrentDatabase()
	headRef, err := dSess.CWBHeadRef(ctx, dbName)
	if err != nil {
		return err
	}

	db, hasDb := dSess.GetDoltDB(ctx, dbName)
	if !hasDb {
		return fmt.Errorf("unable to load database")
	}

	hasRef, err := db.HasRef(ctx, branchRef)
	if err != nil {
		return err
	}
	if !hasRef {
		return doltdb.ErrBranchNotFound
	}

	if ref.Equals(headRef, branchRef) {
		return doltdb.ErrAlreadyOnBranch
	}

	branchHead, err := actions.BranchHeadRoot(ctx, db, brName)
	if err != nil {
		return err
	}

	workingSetExists := true
	initialWs, err := dSess.WorkingSet(ctx, dbName)
	if err == doltdb.ErrWorkingSetNotFound {
		// ignore, but don't reset the working set
		workingSetExists = false
	} else if err != nil {
		return err
	}

	if !force {
		currentRoots, hasRoots := dSess.GetRoots(ctx, dbName)
		if !hasRoots {
			return fmt.Errorf("unable to resolve roots for %s", dbName)
		}
		newBranchRoots, err := db.ResolveBranchRoots(ctx, branchRef)
		if err != nil {
			return err
		}
		if !isNewBranch {
			wouldStomp, err := actions.CheckoutWouldStompWorkingSetChanges(ctx, currentRoots, newBranchRoots)
			if err != nil {
				return err
			}
			if wouldStomp {
				return actions.ErrWorkingSetsOnBothBranches
			}
		}
	}

	initialRoots, hasRoots := dSess.GetRoots(ctx, dbName)
	if !hasRoots {
		return fmt.Errorf("unable to get roots")
	}

	// roots will be empty/nil if the working set is not set (working set is not set if the current branch was deleted)
	if errors.Is(err, doltdb.ErrBranchNotFound) || errors.Is(err, doltdb.ErrWorkingSetNotFound) {
		workingSetExists = false
	} else if err != nil {
		return err
	}

	hasChanges := false
	if workingSetExists {
		hasChanges, _, _, err = actions.RootHasUncommittedChanges(initialRoots)
		if err != nil {
			return err
		}
	}

	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return fmt.Errorf("Could not load database %s", dbName)
	}

	// Only if the current working set has uncommitted changes do we carry them forward to the branch being checked out.
	// If this is the case, then the destination branch must *not* have any uncommitted changes, as checked by
	// checkoutWouldStompWorkingSetChanges
	if hasChanges {
		err = transferWorkingChanges(ctx, dbName, initialRoots, branchHead, branchRef, force)
		if err != nil {
			return err
		}
	} else {
		wsRef, err := ref.WorkingSetRefForHead(branchRef)
		if err != nil {
			return err
		}

		err = dSess.SwitchWorkingSet(ctx, dbName, wsRef)
		if err != nil {
			return err
		}

	}

	if workingSetExists && hasChanges {
		err = actions.CleanOldWorkingSet(ctx, dbData, db, dSess.Username(), dSess.Email(), initialRoots, headRef, initialWs)
		if err != nil {
			return err
		}
	}

	return nil
}

// transferWorkingChanges computes new roots for `branchRef` by applying the changes from the staged and working sets
// of `initialRoots` onto the branch head specified by `branchHead`. This is a DESTRUCTIVE ACTION used during command
// line checkout, to move the working set changes onto a new branch.
func transferWorkingChanges(
	ctx *sql.Context,
	dbName string,
	initialRoots doltdb.Roots,
	branchHead doltdb.RootValue,
	branchRef ref.BranchRef,
	force bool,
) error {
	dSess := dsess.DSessFromSess(ctx.Session)

	// Compute the new roots before switching the working set.
	// This way, we don't leave the branch in a bad state in the event of an error.
	newRoots, err := actions.RootsForBranch(ctx, initialRoots, branchHead, force)
	if err != nil {
		return err
	}

	wsRef, err := ref.WorkingSetRefForHead(branchRef)
	if err != nil {
		return err
	}

	err = dSess.SwitchWorkingSet(ctx, dbName, wsRef)
	if err != nil {
		return err
	}

	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return err
	}

	newWs := ws.WithWorkingRoot(newRoots.Working).WithStagedRoot(newRoots.Staged)

	err = dSess.SetWorkingSet(ctx, dbName, newWs)

	if err != nil {
		return err
	}

	return nil
}

// willModifyDb determines whether or not this operation is a no-op and can return early with a helpful message.
func willModifyDb(dSess *dsess.DoltSession, data env.DbData, dbName, branchName string, updateHead bool) (bool, error) {
	headRef, err := data.Rsr.CWBHeadRef()
	// If we're in a detached head state, allow checking out a new branch.
	if err == doltdb.ErrOperationNotSupportedInDetachedHead {
		return true, nil
	}
	if err != nil {
		return false, err
	}

	// If the operation won't modify either the active session or the default session, return early.
	isModification := headRef.GetPath() != branchName
	if updateHead {
		fs, err := dSess.Provider().FileSystemForDatabase(dbName)
		if err != nil {
			return false, err
		}
		repoState, err := env.LoadRepoState(fs)
		if err != nil {
			return false, err
		}
		defaultBranch := repoState.CWBHeadRef().GetPath()
		isModification = isModification || (defaultBranch != branchName)
	}
	return isModification, nil
}

func generateSuccessMessage(newBranch, upstream string) string {
	result := fmt.Sprintf("Switched to branch '%s'", newBranch)
	if upstream != "" {
		result += fmt.Sprintf("\nbranch '%s' set up to track '%s'.", newBranch, upstream)
	}
	return result
}
