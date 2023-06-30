package dprocedures

import (
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
)

func CheckoutBranch(ctx *sql.Context, brName string, force bool) error {
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
		if actions.CheckoutWouldStompWorkingSetChanges(currentRoots, newBranchRoots) {
			return actions.ErrWorkingSetsOnBothBranches
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

func transferWorkingChanges(
	ctx *sql.Context,
	dbName string,
	initialRoots doltdb.Roots,
	branchHead *doltdb.RootValue,
	branchRef ref.BranchRef,
	force bool,
) error {
	dSess := dsess.DSessFromSess(ctx.Session)
	newRoots, err := actions.RootsForBranch(ctx, initialRoots, branchHead, force)
	if err != nil {
		return err
	}

	// important to not update the checked out branch until after we have done the error checking above, otherwise we
	// potentially leave the client in a bad state
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

	// For backwards compatibility we support the branch not having a working set, but generally speaking it already
	// should have one
	if err == doltdb.ErrWorkingSetNotFound {
		wsRef, err := ref.WorkingSetRefForHead(branchRef)
		if err != nil {
			return err
		}
		ws = doltdb.EmptyWorkingSet(wsRef)
	} else if err != nil {
		return err
	}

	newWs := ws.WithWorkingRoot(newRoots.Working).WithStagedRoot(newRoots.Staged)

	err = dSess.SetWorkingSet(ctx, dbName, newWs)

	if err != nil {
		return err
	}

	return nil
}
