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

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// CheckAccessForDb checks whether the current user has the given permissions for the given database.
// This has to live here, rather than in the branch_control package, to prevent a dependency cycle with that package.
// We could also avoid this by defining branchController as an interface used by dsess.
func CheckAccessForDb(ctx context.Context, db SqlDatabase, flags branch_control.Permissions) error {
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
