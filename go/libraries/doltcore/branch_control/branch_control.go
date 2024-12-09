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
	"strings"
	"sync/atomic"

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"
	"github.com/dolthub/go-mysql-server/sql"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var (
	ErrIncorrectPermissions  = errors.NewKind("`%s`@`%s` does not have the correct permissions on branch `%s`")
	ErrCannotCreateBranch    = errors.NewKind("`%s`@`%s` cannot create a branch named `%s`")
	ErrCannotDeleteBranch    = errors.NewKind("`%s`@`%s` cannot delete the branch `%s`")
	ErrExpressionsTooLong    = errors.NewKind("expressions are too long [%q, %q, %q, %q]")
	ErrInsertingAccessRow    = errors.NewKind("`%s`@`%s` cannot add the row [%q, %q, %q, %q, %q]")
	ErrInsertingNamespaceRow = errors.NewKind("`%s`@`%s` cannot add the row [%q, %q, %q, %q]")
	ErrUpdatingRow           = errors.NewKind("`%s`@`%s` cannot update the row [%q, %q, %q, %q]")
	ErrUpdatingToRow         = errors.NewKind("`%s`@`%s` cannot update the row [%q, %q, %q, %q] to the new branch expression [%q, %q]")
	ErrDeletingRow           = errors.NewKind("`%s`@`%s` cannot delete the row [%q, %q, %q, %q]")
	ErrMissingController     = errors.NewKind("a context has a non-nil session but is missing its branch controller")
)

// Context represents the interface that must be inherited from the context.
type Context interface {
	GetBranch() (string, error)
	GetCurrentDatabase() string
	GetUser() string
	GetHost() string
	GetPrivilegeSet() (sql.PrivilegeSet, uint64)
	GetController() *Controller
	GetFileSystem() filesys.Filesys
}

// Controller is the central hub for branch control functions. This is passed within a context.
type Controller struct {
	Access    *Access
	Namespace *Namespace

	Serialized atomic.Pointer[[]byte]

	// A callback which we call when we successfully save new data.
	// The new data will be available in |Serialized|.
	SavedCallback func(context.Context)

	branchControlFilePath string
	doltConfigDirPath     string
}

// CreateDefaultController returns a default controller, which only has a single entry allowing all users to have write
// permissions on all branches (only the super user has admin, if a super user has been set). This is equivalent to
// passing empty strings to LoadData.
func CreateDefaultController(ctx context.Context) *Controller {
	controller, err := LoadData(ctx, "", "")
	if err != nil {
		panic(err) // should never happen
	}
	return controller
}

// LoadData loads the data from the given location and returns a controller. Returns the default controller if the
// `branchControlFilePath` is empty.
func LoadData(ctx context.Context, branchControlFilePath string, doltConfigDirPath string) (*Controller, error) {
	accessTbl := newAccess()
	controller := &Controller{
		Access:                accessTbl,
		Namespace:             newNamespace(accessTbl),
		branchControlFilePath: branchControlFilePath,
		doltConfigDirPath:     doltConfigDirPath,
	}

	// Do not attempt to load from an empty file path
	if len(branchControlFilePath) == 0 {
		// If the path is empty, then we should populate the controller with the default row to ensure normal (expected) operation
		controller.Access.insertDefaultRow()
		return controller, nil
	}

	data, err := os.ReadFile(branchControlFilePath)
	if err != nil && !goerrors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	err = controller.LoadData(ctx, data /* isFirstLoad */, true)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize config at '%s': %w", branchControlFilePath, err)
	}
	return controller, nil
}

func (controller *Controller) LoadData(ctx context.Context, data []byte, isFirstLoad bool) error {
	controller.Access.RWMutex.Lock()
	defer controller.Access.RWMutex.Unlock()

	// Nothing to load so we can return
	if len(data) == 0 {
		// As there is nothing to load, we should populate the controller with the default row to ensure normal (expected) operation
		controller.Access.insertDefaultRow()
		controller.Serialized.Store(&data)
		if controller.SavedCallback != nil {
			controller.SavedCallback(ctx)
		}
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

	rollback := controller.Serialized.Load()

	// TODO: Better concurrency control here. We see |Namespace| and
	// |Access| in different views of the data here.

	// The Deserialize functions acquire write locks, so we don't acquire them here
	if err = controller.Access.Deserialize(access); err != nil {
		// TODO: More principaled rollback. Hopefully this does not fail.
		controller.LoadData(ctx, *rollback, isFirstLoad)
		return err
	}
	if err = controller.Namespace.Deserialize(namespace); err != nil {
		// TODO: More principaled rollback. Hopefully this does not fail.
		controller.LoadData(ctx, *rollback, isFirstLoad)
		return err
	}

	controller.Serialized.Store(&data)
	if controller.SavedCallback != nil {
		controller.SavedCallback(ctx)
	}

	return nil
}

// SaveData saves the data from the context's controller to the location pointed by it.
func SaveData(ctx context.Context) error {
	branchAwareSession := GetBranchAwareSession(ctx)
	// A nil session means we're not in the SQL context, so we've got nothing to serialize
	if branchAwareSession == nil {
		return nil
	}
	controller := branchAwareSession.GetController()
	// If there is no controller in the context, then we have nothing to serialize
	if controller == nil {
		return nil
	}

	return controller.SaveData(ctx, branchAwareSession.GetFileSystem())
}

func (controller *Controller) SaveData(ctx context.Context, fs filesys.Filesys) error {
	// If we never set a save location then we just return
	if len(controller.branchControlFilePath) == 0 {
		return nil
	}

	// Create the doltcfg directory if it doesn't exist
	if len(controller.doltConfigDirPath) != 0 {
		if mkErr := fs.MkDirs(controller.doltConfigDirPath); mkErr != nil {
			return mkErr
		}
	}

	controller.Access.RWMutex.Lock()
	defer controller.Access.RWMutex.Unlock()

	b := flatbuffers.NewBuilder(1024)
	// The Serialize functions acquire read locks, so we don't acquire them here
	accessOffset := controller.Access.Serialize(b)
	namespaceOffset := controller.Namespace.Serialize(b)
	serial.BranchControlStart(b)
	serial.BranchControlAddAccessTbl(b, accessOffset)
	serial.BranchControlAddNamespaceTbl(b, namespaceOffset)
	root := serial.BranchControlEnd(b)
	// serial.FinishMessage() limits files to 2^24 bytes, so this works around it while maintaining read compatibility
	b.Prep(1, flatbuffers.SizeInt32+4+serial.MessagePrefixSz)
	b.FinishWithFileIdentifier(root, []byte(serial.BranchControlFileID))
	data := b.Bytes[b.Head()-serial.MessagePrefixSz:]

	err := fs.WriteFile(controller.branchControlFilePath, data, 0660)
	if err != nil {
		return err
	}

	controller.Serialized.Store(&data)
	if controller.SavedCallback != nil {
		controller.SavedCallback(ctx)
	}
	return nil
}

// CheckAccess returns whether the given context has the correct permissions on its selected branch. In general, SQL
// statements will almost always return a *sql.Context, so any checks from the SQL path will correctly check for branch
// permissions. However, not all CLI commands use *sql.Context, and therefore will not have any user associated with
// the context. In these cases, CheckAccess will pass as we want to allow all local commands to ignore branch
// permissions.
func CheckAccess(ctx context.Context, flags Permissions) error {
	branchAwareSession := GetBranchAwareSession(ctx)
	// A nil session means we're not in the SQL context, so we allow all operations
	if branchAwareSession == nil {
		return nil
	}
	controller := branchAwareSession.GetController()
	// Any context that has a non-nil session should always have a non-nil controller, so this is an error
	if controller == nil {
		return ErrMissingController.New()
	}
	controller.Access.RWMutex.RLock()
	defer controller.Access.RWMutex.RUnlock()

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
	database := getDatabaseNameOnly(branchAwareSession.GetCurrentDatabase())
	branch, err := branchAwareSession.GetBranch()
	if err != nil {
		return err
	}
	// Get the permissions for the branch, user, and host combination
	_, perms := controller.Access.Match(database, branch, user, host)
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
	branchAwareSession := GetBranchAwareSession(ctx)
	// A nil session means we're not in the SQL context, so we allow the create operation
	if branchAwareSession == nil {
		return nil
	}
	controller := branchAwareSession.GetController()
	// Any context that has a non-nil session should always have a non-nil controller, so this is an error
	if controller == nil {
		return ErrMissingController.New()
	}
	controller.Namespace.RWMutex.RLock()
	defer controller.Namespace.RWMutex.RUnlock()

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
	database := getDatabaseNameOnly(branchAwareSession.GetCurrentDatabase())
	if controller.Namespace.CanCreate(database, branchName, user, host) {
		return nil
	}
	return ErrCannotCreateBranch.New(user, host, branchName)
}

// CanDeleteBranch returns whether the given context can delete a branch with the given name. In general, SQL statements
// will almost always return a *sql.Context, so any checks from the SQL path will be able to validate a branch's name.
// However, not all CLI commands use *sql.Context, and therefore will not have any user associated with the context. In
// these cases, CanDeleteBranch will pass as we want to allow all local commands to freely delete branches.
func CanDeleteBranch(ctx context.Context, branchName string) error {
	branchAwareSession := GetBranchAwareSession(ctx)
	// A nil session means we're not in the SQL context, so we allow the delete operation
	if branchAwareSession == nil {
		return nil
	}
	controller := branchAwareSession.GetController()
	// Any context that has a non-nil session should always have a non-nil controller, so this is an error
	if controller == nil {
		return ErrMissingController.New()
	}
	controller.Access.RWMutex.RLock()
	defer controller.Access.RWMutex.RUnlock()

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
	database := getDatabaseNameOnly(branchAwareSession.GetCurrentDatabase())
	// Get the permissions for the branch, user, and host combination
	_, perms := controller.Access.Match(database, branchName, user, host)
	// If the user has the write or admin flags, then we allow access
	if (perms&Permissions_Write == Permissions_Write) || (perms&Permissions_Admin == Permissions_Admin) {
		return nil
	}
	return ErrCannotDeleteBranch.New(user, host, branchName)
}

// AddAdminForContext adds an entry in the access table for the user represented by the given context. If the
// context is missing some functionality that is needed to perform the addition, such as a user or the Controller, then
// this simply returns.
func AddAdminForContext(ctx context.Context, branchName string) error {
	branchAwareSession := GetBranchAwareSession(ctx)
	if branchAwareSession == nil {
		return nil
	}
	controller := branchAwareSession.GetController()
	if controller == nil {
		return nil
	}

	user := branchAwareSession.GetUser()
	host := branchAwareSession.GetHost()
	database := getDatabaseNameOnly(branchAwareSession.GetCurrentDatabase())
	// Check if we already have admin permissions for the given branch, as there's no need to do another insertion if so
	controller.Access.RWMutex.RLock()
	_, modPerms := controller.Access.Match(database, branchName, user, host)
	controller.Access.RWMutex.RUnlock()
	if modPerms&Permissions_Admin == Permissions_Admin {
		return nil
	}
	controller.Access.RWMutex.Lock()
	controller.Access.Insert(database, branchName, user, host, Permissions_Admin)
	controller.Access.RWMutex.Unlock()
	return SaveData(ctx)
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

// HasDatabasePrivileges returns whether the given context's user has the correct privileges to modify any table entries
// that match the given database. The following are the required privileges:
//
// Global Space:   SUPER, GRANT
// Global Space:   CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, EXECUTE, GRANT
// Database Space: CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, EXECUTE, GRANT
//
// Any user that may grant SUPER is considered to be a super user. In addition, any user that may grant the suite of
// alteration privileges is also considered a super user. The SUPER privilege does not exist at the database level, it
// is a global privilege only.
func HasDatabasePrivileges(ctx Context, database string) bool {
	if ctx == nil {
		return true
	}
	privSet, counter := ctx.GetPrivilegeSet()
	if counter == 0 {
		return false
	}
	hasSuper := privSet.Has(sql.PrivilegeType_Super, sql.PrivilegeType_GrantOption)
	isGlobalAdmin := privSet.Has(sql.PrivilegeType_Create, sql.PrivilegeType_Alter, sql.PrivilegeType_Drop,
		sql.PrivilegeType_Insert, sql.PrivilegeType_Update, sql.PrivilegeType_Delete, sql.PrivilegeType_Execute, sql.PrivilegeType_GrantOption)
	isDatabaseAdmin := privSet.Database(database).Has(sql.PrivilegeType_Create, sql.PrivilegeType_Alter, sql.PrivilegeType_Drop,
		sql.PrivilegeType_Insert, sql.PrivilegeType_Update, sql.PrivilegeType_Delete, sql.PrivilegeType_Execute, sql.PrivilegeType_GrantOption)
	return hasSuper || isGlobalAdmin || isDatabaseAdmin
}

// getDatabaseNameOnly gets the database name only, which is useful for when the database name includes a revision.
// This is a direct reimplementation of the logic in dsess.SplitRevisionDbName, however we cannot use that function due
// to import cycles.
func getDatabaseNameOnly(dbName string) string {
	return strings.SplitN(dbName, "/", 2)[0]
}
