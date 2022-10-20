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
	goerrors "errors"
	"fmt"
	"os"

	"github.com/dolthub/go-mysql-server/sql"
	flatbuffers "github.com/google/flatbuffers/go"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/gen/fb/serial"
)

var (
	ErrIncorrectPermissions = errors.NewKind("`%s`@`%s` does not have the correct permissions on branch `%s`")
	ErrCannotCreateBranch   = errors.NewKind("`%s`@`%s` cannot create a branch named `%s`")
	ErrCannotDeleteBranch   = errors.NewKind("`%s`@`%s` cannot delete the branch `%s`")
	ErrExpressionsTooLong   = errors.NewKind("expressions are too long [%q, %q, %q]")
	ErrInsertingRow         = errors.NewKind("`%s`@`%s` cannot add the row [%q, %q, %q, %q]")
	ErrUpdatingRow          = errors.NewKind("`%s`@`%s` cannot update the row [%q, %q, %q]")
	ErrUpdatingToRow        = errors.NewKind("`%s`@`%s` cannot update the row [%q, %q, %q] to the new branch expression %q")
	ErrDeletingRow          = errors.NewKind("`%s`@`%s` cannot delete the row [%q, %q, %q]")
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
	Access    *Access
	Namespace *Namespace

	branchControlFilePath string
	doltConfigDirPath     string
}

// TODO: delete me
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
	return CreateControllerWithSuperUser(ctx, "", "")
}

// CreateControllerWithSuperUser returns a controller with the given user and host set as an immutable super user.
func CreateControllerWithSuperUser(ctx context.Context, superUser string, superHost string) *Controller {
	//TODO: put in the context
	accessTbl := newAccess(superUser, superHost)
	return &Controller{
		Access:    accessTbl,
		Namespace: newNamespace(accessTbl, superUser, superHost),
	}
}

// LoadData loads the data from the given location into the controller.
func LoadData(ctx context.Context, branchControlFilePath string, doltConfigDirPath string) error {
	//TODO: load into the context's controller
	if !enabled {
		return nil
	}

	// Do not attempt to load from an empty file path
	if len(branchControlFilePath) == 0 {
		return nil
	}

	StaticController.branchControlFilePath = branchControlFilePath
	StaticController.doltConfigDirPath = doltConfigDirPath
	data, err := os.ReadFile(branchControlFilePath)
	if err != nil && !goerrors.Is(err, os.ErrNotExist) {
		return err
	}
	// Nothing to load so we can return
	if len(data) == 0 {
		return nil
	}
	// Load the tables
	if serial.GetFileID(data) != serial.BranchControlFileID {
		return fmt.Errorf("unable to deserialize branch controller, unknown file ID `%s`", serial.GetFileID(data))
	}
	bc, err := serial.TryGetRootAsBranchControl(data, serial.MessagePrefixSz)
	if err != nil {
		return err
	}
	access, err := bc.TryAccessTbl(nil)
	if err != nil {
		return err
	}
	namespace, err := bc.TryNamespaceTbl(nil)
	if err != nil {
		return err
	}
	// The Deserialize functions acquire write locks, so we don't acquire them here
	if err = StaticController.Access.Deserialize(access); err != nil {
		return err
	}
	if err = StaticController.Namespace.Deserialize(namespace); err != nil {
		return err
	}
	return nil
}

// SaveData saves the data from the context's controller to the location pointed by it.
func SaveData(ctx context.Context) error {
	//TODO: load from the context's controller
	if !enabled {
		return nil
	}

	// If we never set a save location then we just return
	if len(StaticController.branchControlFilePath) == 0 {
		return nil
	}
	// Create the doltcfg directory if it doesn't exist
	if len(StaticController.doltConfigDirPath) != 0 {
		if _, err := os.Stat(StaticController.doltConfigDirPath); os.IsNotExist(err) {
			if mkErr := os.Mkdir(StaticController.doltConfigDirPath, 0777); mkErr != nil {
				return mkErr
			}
		} else if err != nil {
			return err
		}
	}
	b := flatbuffers.NewBuilder(1024)
	// The Serialize functions acquire read locks, so we don't acquire them here
	accessOffset := StaticController.Access.Serialize(b)
	namespaceOffset := StaticController.Namespace.Serialize(b)
	serial.BranchControlStart(b)
	serial.BranchControlAddAccessTbl(b, accessOffset)
	serial.BranchControlAddNamespaceTbl(b, namespaceOffset)
	root := serial.BranchControlEnd(b)
	data := serial.FinishMessage(b, root, []byte(serial.BranchControlFileID))
	return os.WriteFile(StaticController.branchControlFilePath, data, 0777)
}

// Reset is a temporary function just for testing. Once the controller is in the context, this will be unnecessary.
func Reset() {
	//TODO: remove this once the controller is in the context
	StaticController = CreateControllerWithSuperUser(context.Background(), StaticController.Access.SuperUser, StaticController.Access.SuperHost)
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
	branchAwareSession := GetBranchAwareSession(ctx)
	// A nil session means we're not in the SQL context, so we allow all operations
	if branchAwareSession == nil {
		return nil
	}
	StaticController.Access.RWMutex.RLock()
	defer StaticController.Access.RWMutex.RUnlock()

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
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
	return ErrIncorrectPermissions.New(user, host, branch)
}

// CanCreateBranch returns whether the given context can create a branch with the given name. In general, SQL statements
// will almost always return a *sql.Context, so any checks from the SQL path will be able to validate a branch's name.
// However, not all CLI commands use *sql.Context, and therefore will not have any user associated with the context. In
// these cases, CanCreateBranch will pass as we want to allow all local commands to freely create branches.
func CanCreateBranch(ctx context.Context, branchName string) error {
	if !enabled {
		return nil
	}
	branchAwareSession := GetBranchAwareSession(ctx)
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
	return ErrCannotCreateBranch.New(user, host, branchName)
}

// CanDeleteBranch returns whether the given context can delete a branch with the given name. In general, SQL statements
// will almost always return a *sql.Context, so any checks from the SQL path will be able to validate a branch's name.
// However, not all CLI commands use *sql.Context, and therefore will not have any user associated with the context. In
// these cases, CanDeleteBranch will pass as we want to allow all local commands to freely delete branches.
func CanDeleteBranch(ctx context.Context, branchName string) error {
	if !enabled {
		return nil
	}
	branchAwareSession := GetBranchAwareSession(ctx)
	// A nil session means we're not in the SQL context, so we allow the delete operation
	if branchAwareSession == nil {
		return nil
	}
	StaticController.Access.RWMutex.RLock()
	defer StaticController.Access.RWMutex.RUnlock()

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
	// Get the permissions for the branch, user, and host combination
	_, perms := StaticController.Access.Match(branchName, user, host)
	// If the user has the write or admin flags, then we allow access
	if (perms&Permissions_Write == Permissions_Write) || (perms&Permissions_Admin == Permissions_Admin) {
		return nil
	}
	return ErrCannotDeleteBranch.New(user, host, branchName)
}

// GetBranchAwareSession returns the session contained within the context. If the context does NOT contain a session,
// then nil is returned.
func GetBranchAwareSession(ctx context.Context) Context {
	if sqlCtx, ok := ctx.(*sql.Context); ok {
		if bas, ok := sqlCtx.Session.(Context); ok {
			return bas
		}
	} else if bas, ok := ctx.(Context); ok {
		return bas
	}
	return nil
}
