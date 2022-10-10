// Copyright 2022 Dolthub, Inc.
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

package branch_control

import (
	"context"
	"fmt"
	"os"

	"github.com/dolthub/go-mysql-server/sql"
)

// Context represents the interface that must be inherited from the context.
type Context interface {
	GetBranch() (string, error)
	GetUser() string
	GetHost() string
	GetController() *Controller
}

// Controller is the central hub for branch control functions. This is passed within a context.
type Controller struct {
	Access    *access
	Namespace *namespace
}

//TODO: delete me
var StaticController = &Controller{}
var enabled = false

func init() {
	if os.Getenv("DOLT_ENABLE_BRANCH_CONTROL") != "" {
		enabled = true
	}
	StaticController = CreateControllerWithSuperUser(context.Background(), "root", "localhost")
}

// CreateController returns an empty *Controller.
func CreateController(ctx context.Context) *Controller {
	accessTbl := newAccess("", "")
	return &Controller{
		Access:    accessTbl,
		Namespace: newNamespace(accessTbl, "", ""),
	}
}

// CreateControllerWithSuperUser returns a controller with the given user and host set as an immutable super user.
func CreateControllerWithSuperUser(ctx context.Context, superUser string, superHost string) *Controller {
	accessTbl := newAccess(superUser, superHost)
	return &Controller{
		Access:    accessTbl,
		Namespace: newNamespace(accessTbl, superUser, superHost),
	}
}

// CheckAccess returns whether the given context has the correct permissions on its selected branch. In general, SQL
// statements will almost always return a *sql.Context, so any checks from the SQL path will correctly check for branch
// permissions. However, not all CLI commands use *sql.Context, and therefore will not have any user associated with
// the context. In these cases, CheckAccess will pass as we want to allow all local commands to ignore branch
// permissions.
func CheckAccess(ctx context.Context, flags Permissions) error {
	if !enabled {
		return nil
	}
	branchAwareSession := getBranchAwareSession(ctx)
	// A nil session means we're not in the SQL context, so we allow all operations
	if branchAwareSession == nil {
		return nil
	}
	StaticController.Access.RWMutex.RLock()
	defer StaticController.Access.RWMutex.RUnlock()

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
	// Check if the user is the super user, which has access to all operations
	if user == StaticController.Access.SuperUser && host == StaticController.Access.SuperHost {
		return nil
	}
	branch, err := branchAwareSession.GetBranch()
	if err != nil {
		return err
	}
	// Get the permissions for the branch, user, and host combination
	_, perms := StaticController.Access.Match(branch, user, host)
	// If either the flags match or the user is an admin for this branch, then we allow access
	if (perms&flags == flags) || (perms&Permissions_Admin == Permissions_Admin) {
		return nil
	}
	return fmt.Errorf("`%s`@`%s` does not have the correct permissions on branch `%s`", user, host, branch)
}

// CanCreateBranch returns whether the given context can create a branch with the given name. In general, SQL statements
// will almost always return a *sql.Context, so any checks from the SQL path will be able to validate a branch's name.
// However, not all CLI commands use *sql.Context, and therefore will not have any user associated with the context. In
// these cases, CanCreateBranch will pass as we want to allow all local commands to freely create branches.
func CanCreateBranch(ctx context.Context, branchName string) error {
	if !enabled {
		return nil
	}
	branchAwareSession := getBranchAwareSession(ctx)
	if branchAwareSession == nil {
		return nil
	}
	StaticController.Namespace.RWMutex.RLock()
	defer StaticController.Namespace.RWMutex.RUnlock()

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
	if StaticController.Namespace.CanCreate(branchName, user, host) {
		return nil
	}
	return fmt.Errorf("`%s`@`%s` cannot create a branch named `%s`", user, host, branchName)
}

// CanDeleteBranch returns whether the given context can delete a branch with the given name. In general, SQL statements
// will almost always return a *sql.Context, so any checks from the SQL path will be able to validate a branch's name.
// However, not all CLI commands use *sql.Context, and therefore will not have any user associated with the context. In
// these cases, CanDeleteBranch will pass as we want to allow all local commands to freely delete branches.
func CanDeleteBranch(ctx context.Context, branchName string) error {
	if !enabled {
		return nil
	}
	branchAwareSession := getBranchAwareSession(ctx)
	// A nil session means we're not in the SQL context, so we allow the delete operation
	if branchAwareSession == nil {
		return nil
	}
	StaticController.Access.RWMutex.RLock()
	defer StaticController.Access.RWMutex.RUnlock()

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
	// Check if the user is the super user, which is always able to delete branches
	if user == StaticController.Access.SuperUser && host == StaticController.Access.SuperHost {
		return nil
	}
	// Get the permissions for the branch, user, and host combination
	_, perms := StaticController.Access.Match(branchName, user, host)
	// If the user has the write or admin flags, then we allow access
	if (perms&Permissions_Write == Permissions_Write) || (perms&Permissions_Admin == Permissions_Admin) {
		return nil
	}
	return fmt.Errorf("`%s`@`%s` cannot delete the branch `%s`", user, host, branchName)
}

// getBranchAwareSession returns the session contained within the context. If the context does NOT contain a session,
// then nil is returned.
func getBranchAwareSession(ctx context.Context) Context {
	if sqlCtx, ok := ctx.(*sql.Context); ok {
		if bas, ok := sqlCtx.Session.(Context); ok {
			return bas
		}
	} else if bas, ok := ctx.(Context); ok {
		return bas
	}
	return nil
}
