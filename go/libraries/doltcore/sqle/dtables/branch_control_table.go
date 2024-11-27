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

package dtables

import (
	"fmt"
	"math"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

const (
	AccessTableName = "dolt_branch_control"
)

// PermissionsStrings is a slice of strings representing the available branch_control.branch_control.Permissions. The order of the
// strings should exactly match the order of the branch_control.Permissions according to their flag value.
var PermissionsStrings = []string{"admin", "write", "read"}

// accessSchema is the schema for the "dolt_branch_control" table.
var accessSchema = sql.Schema{
	&sql.Column{
		Name:       "database",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     AccessTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "branch",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     AccessTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "user",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_bin),
		Source:     AccessTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "host",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     AccessTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "permissions",
		Type:       types.MustCreateSetType(PermissionsStrings, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     AccessTableName,
		PrimaryKey: false,
	},
}

// BranchControlTable provides a layer over the branch_control.Access structure, exposing it as a system table.
type BranchControlTable struct {
	*branch_control.Access
}

var _ sql.Table = BranchControlTable{}
var _ sql.InsertableTable = BranchControlTable{}
var _ sql.ReplaceableTable = BranchControlTable{}
var _ sql.UpdatableTable = BranchControlTable{}
var _ sql.DeletableTable = BranchControlTable{}
var _ sql.RowInserter = BranchControlTable{}
var _ sql.RowReplacer = BranchControlTable{}
var _ sql.RowUpdater = BranchControlTable{}
var _ sql.RowDeleter = BranchControlTable{}

// NewBranchControlTable returns a new BranchControlTable.
func NewBranchControlTable(access *branch_control.Access) BranchControlTable {
	return BranchControlTable{access}
}

// Name implements the interface sql.Table.
func (tbl BranchControlTable) Name() string {
	return AccessTableName
}

// String implements the interface sql.Table.
func (tbl BranchControlTable) String() string {
	return AccessTableName
}

// Schema implements the interface sql.Table.
func (tbl BranchControlTable) Schema() sql.Schema {
	return accessSchema
}

// Collation implements the interface sql.Table.
func (tbl BranchControlTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the interface sql.Table.
func (tbl BranchControlTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows implements the interface sql.Table.
func (tbl BranchControlTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	tbl.RWMutex.RLock()
	defer tbl.RWMutex.RUnlock()

	var rows []sql.Row
	rowIter := tbl.Iter()
	for value, ok := rowIter.Next(); ok; value, ok = rowIter.Next() {
		rows = append(rows, sql.UntypedSqlRow{
			value.Database,
			value.Branch,
			value.User,
			value.Host,
			uint64(value.Permissions),
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Inserter implements the interface sql.InsertableTable.
func (tbl BranchControlTable) Inserter(context *sql.Context) sql.RowInserter {
	return tbl
}

// Replacer implements the interface sql.ReplaceableTable.
func (tbl BranchControlTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return tbl
}

// Updater implements the interface sql.UpdatableTable.
func (tbl BranchControlTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return tbl
}

// Deleter implements the interface sql.DeletableTable.
func (tbl BranchControlTable) Deleter(context *sql.Context) sql.RowDeleter {
	return tbl
}

// StatementBegin implements the interface sql.TableEditor.
func (tbl BranchControlTable) StatementBegin(ctx *sql.Context) {
	//TODO: will use the binlog to implement
}

// DiscardChanges implements the interface sql.TableEditor.
func (tbl BranchControlTable) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	//TODO: will use the binlog to implement
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (tbl BranchControlTable) StatementComplete(ctx *sql.Context) error {
	//TODO: will use the binlog to implement
	return nil
}

// Insert implements the interface sql.RowInserter.
func (tbl BranchControlTable) Insert(ctx *sql.Context, row sql.Row) error {
	tbl.RWMutex.Lock()
	defer tbl.RWMutex.Unlock()

	// Database, Branch, and Host are case-insensitive, while user is case-sensitive
	database := strings.ToLower(branch_control.FoldExpression(row.GetValue(0).(string)))
	branch := strings.ToLower(branch_control.FoldExpression(row.GetValue(1).(string)))
	user := branch_control.FoldExpression(row.GetValue(2).(string))
	host := strings.ToLower(branch_control.FoldExpression(row.GetValue(3).(string)))
	perms := branch_control.Permissions(row.GetValue(4).(uint64))

	// Verify that the lengths of each expression fit within an uint16
	if len(database) > math.MaxUint16 || len(branch) > math.MaxUint16 || len(user) > math.MaxUint16 || len(host) > math.MaxUint16 {
		return branch_control.ErrExpressionsTooLong.New(database, branch, user, host)
	}

	// A nil session means we're not in the SQL context, so we allow the insertion in such a case
	if branchAwareSession := branch_control.GetBranchAwareSession(ctx); branchAwareSession != nil &&
		// Having the correct database privileges also allows the insertion
		!branch_control.HasDatabasePrivileges(branchAwareSession, database) {
		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
		// determine if the user attempting the insertion has permission to perform the insertion.
		_, modPerms := tbl.Match(database, branch, insertUser, insertHost)
		if modPerms&branch_control.Permissions_Admin != branch_control.Permissions_Admin {
			permStr, _ := accessSchema[4].Type.(sql.SetType).BitsToString(uint64(perms))
			return branch_control.ErrInsertingAccessRow.New(insertUser, insertHost, database, branch, user, host, permStr)
		}
	}

	// We check if we're inserting a subset of an already-existing row. If we are, we deny the insertion as the existing
	// row will already match against ALL possible values for this row.
	if ok, modPerms := tbl.Match(database, branch, user, host); ok {
		permBits := uint64(modPerms)
		permStr, _ := accessSchema[4].Type.(sql.SetType).BitsToString(permBits)
		return sql.NewUniqueKeyErr(
			fmt.Sprintf(`[%q, %q, %q, %q, %q]`, database, branch, user, host, permStr),
			true,
			sql.UntypedSqlRow{database, branch, user, host, permBits})
	}

	tbl.Access.Insert(database, branch, user, host, perms)
	return nil
}

// Update implements the interface sql.RowUpdater.
func (tbl BranchControlTable) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	tbl.RWMutex.Lock()
	defer tbl.RWMutex.Unlock()

	// Database, Branch, and Host are case-insensitive, while User is case-sensitive
	oldDatabase := strings.ToLower(branch_control.FoldExpression(old.GetValue(0).(string)))
	oldBranch := strings.ToLower(branch_control.FoldExpression(old.GetValue(1).(string)))
	oldUser := branch_control.FoldExpression(old.GetValue(2).(string))
	oldHost := strings.ToLower(branch_control.FoldExpression(old.GetValue(3).(string)))
	newDatabase := strings.ToLower(branch_control.FoldExpression(new.GetValue(0).(string)))
	newBranch := strings.ToLower(branch_control.FoldExpression(new.GetValue(1).(string)))
	newUser := branch_control.FoldExpression(new.GetValue(2).(string))
	newHost := strings.ToLower(branch_control.FoldExpression(new.GetValue(3).(string)))
	newPerms := branch_control.Permissions(new.GetValue(4).(uint64))

	// Verify that the lengths of each expression fit within an uint16
	if len(newDatabase) > math.MaxUint16 || len(newBranch) > math.MaxUint16 || len(newUser) > math.MaxUint16 || len(newHost) > math.MaxUint16 {
		return branch_control.ErrExpressionsTooLong.New(newDatabase, newBranch, newUser, newHost)
	}

	// If we're not updating the same row, then we check for a row violation
	if oldDatabase != newDatabase || oldBranch != newBranch || oldUser != newUser || oldHost != newHost {
		if ok, modPerms := tbl.Match(newDatabase, newBranch, newUser, newHost); ok {
			permBits := uint64(modPerms)
			permStr, _ := accessSchema[4].Type.(sql.SetType).BitsToString(permBits)
			return sql.NewUniqueKeyErr(
				fmt.Sprintf(`[%q, %q, %q, %q, %q]`, newDatabase, newBranch, newUser, newHost, permStr),
				true,
				sql.UntypedSqlRow{newDatabase, newBranch, newUser, newHost, permBits})
		}
	}

	// A nil session means we're not in the SQL context, so we'd allow the update in such a case
	if branchAwareSession := branch_control.GetBranchAwareSession(ctx); branchAwareSession != nil {
		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		if !branch_control.HasDatabasePrivileges(branchAwareSession, oldDatabase) {
			// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
			// determine if the user attempting the update has permission to perform the update on the old branch name.
			_, modPerms := tbl.Match(oldDatabase, oldBranch, insertUser, insertHost)
			if modPerms&branch_control.Permissions_Admin != branch_control.Permissions_Admin {
				return branch_control.ErrUpdatingRow.New(insertUser, insertHost, oldDatabase, oldBranch, oldUser, oldHost)
			}
		}
		if !branch_control.HasDatabasePrivileges(branchAwareSession, newDatabase) {
			// Similar to the block above, we check if the user has permission to use the new branch name
			_, modPerms := tbl.Match(newDatabase, newBranch, insertUser, insertHost)
			if modPerms&branch_control.Permissions_Admin != branch_control.Permissions_Admin {
				return branch_control.ErrUpdatingToRow.
					New(insertUser, insertHost, oldDatabase, oldBranch, oldUser, oldHost, newDatabase, newBranch)
			}
		}
	}

	tbl.Access.Delete(oldDatabase, oldBranch, oldUser, oldHost)
	tbl.Access.Insert(newDatabase, newBranch, newUser, newHost, newPerms)
	return nil
}

// Delete implements the interface sql.RowDeleter.
func (tbl BranchControlTable) Delete(ctx *sql.Context, row sql.Row) error {
	tbl.RWMutex.Lock()
	defer tbl.RWMutex.Unlock()

	// Database, Branch, and Host are case-insensitive, while User is case-sensitive
	database := strings.ToLower(branch_control.FoldExpression(row.GetValue(0).(string)))
	branch := strings.ToLower(branch_control.FoldExpression(row.GetValue(1).(string)))
	user := branch_control.FoldExpression(row.GetValue(2).(string))
	host := strings.ToLower(branch_control.FoldExpression(row.GetValue(3).(string)))

	// A nil session means we're not in the SQL context, so we allow the deletion in such a case
	if branchAwareSession := branch_control.GetBranchAwareSession(ctx); branchAwareSession != nil &&
		// Having the correct database privileges also allows the deletion
		!branch_control.HasDatabasePrivileges(branchAwareSession, database) {
		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
		// determine if the user attempting the deletion has permission to perform the deletion.
		_, modPerms := tbl.Match(database, branch, insertUser, insertHost)
		if modPerms&branch_control.Permissions_Admin != branch_control.Permissions_Admin {
			return branch_control.ErrDeletingRow.New(insertUser, insertHost, database, branch, user, host)
		}
	}

	tbl.Access.Delete(database, branch, user, host)
	return nil
}

// Close implements the interface sql.Closer.
func (tbl BranchControlTable) Close(context *sql.Context) error {
	return branch_control.SaveData(context)
}
