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

package dsess

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// CheckAccessForDb checks whether the current user has the given permissions for the given database.
// This has to live here, rather than in the branch_control package, to prevent a dependency cycle with that package.
// We could also avoid this by defining branchController as an interface used by dsess.
func CheckAccessForDb(ctx context.Context, db VersionedDatabase, flags branch_control.Permissions) error {
	branchAwareSession := branch_control.GetBranchAwareSession(ctx)
	// A nil session means we're not in the SQL context, so we allow all operations
	if branchAwareSession == nil {
		return nil
	}

	controller := branchAwareSession.GetController()
	// Any context that has a non-nil session should always have a non-nil controller, so this is an error
	if controller == nil {
		return branch_control.ErrMissingController.New()
	}

	controller.Access.RWMutex.RLock()
	defer controller.Access.RWMutex.RUnlock()

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()

	if db.RevisionType() != RevisionTypeBranch {
		// not a branch db, no check necessary
		return nil
	}

	dbName, branch := doltdb.SplitRevisionDbName(db.RevisionQualifiedName())

	// Get the permissions for the branch, user, and host combination
	_, perms := controller.Access.Match(dbName, branch, user, host)
	// If either the flags match or the user is an admin for this branch, then we allow access
	if (perms&flags == flags) || (perms&branch_control.Permissions_Admin == branch_control.Permissions_Admin) {
		return nil
	}
	return branch_control.ErrIncorrectPermissions.New(user, host, branch)
}

// CheckAccessOrMergeActive returns nil when the caller has Permissions_Write
// on the current branch, or has Permissions_Merge AND the working set has an
// active merge AND the working root contains data conflicts. Otherwise it
// returns the access-denied error from the more specific check.
//
// Used by procedures (e.g., DOLT_CONFLICTS_RESOLVE) that should be reachable
// by merge-permission callers only when there is actually a merge with data
// conflicts to resolve. Mirrors the rule used by conflicts-table writes.
func CheckAccessOrMergeActive(ctx *sql.Context) error {
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Write); err == nil {
		return nil
	}
	if err := branch_control.CheckAccess(ctx, branch_control.Permissions_Merge); err != nil {
		return err
	}
	dbName := ctx.GetCurrentDatabase()
	ws, err := DSessFromSess(ctx.Session).WorkingSet(ctx, dbName)
	if err != nil {
		return err
	}
	if !ws.MergeActive() {
		return mergePermDenied(ctx)
	}
	hasConflicts, err := doltdb.HasConflicts(ctx, ws.WorkingRoot())
	if err != nil {
		return err
	}
	if !hasConflicts {
		return mergePermDenied(ctx)
	}
	return nil
}

func mergePermDenied(ctx *sql.Context) error {
	bas := branch_control.GetBranchAwareSession(ctx)
	var user, host, branch string
	if bas != nil {
		user, host = bas.GetUser(), bas.GetHost()
		branch, _ = bas.GetBranch()
	}
	return branch_control.ErrIncorrectPermissions.New(user, host, branch)
}
