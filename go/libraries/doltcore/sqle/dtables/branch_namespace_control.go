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
	"context"
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
	NamespaceTableName = "dolt_branch_namespace_control"
)

// namespaceSchema is the schema for the "dolt_branch_namespace_control" table.
var namespaceSchema = sql.Schema{
	&sql.Column{
		Name:       "database",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     NamespaceTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "branch",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     NamespaceTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "user",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_bin),
		Source:     NamespaceTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "host",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     NamespaceTableName,
		PrimaryKey: true,
	},
}

// BranchNamespaceControlTable provides a layer over the branch_control.Namespace structure, exposing it as a system
// table.
type BranchNamespaceControlTable struct {
	*branch_control.Namespace
}

var _ sql.Table = BranchNamespaceControlTable{}
var _ sql.InsertableTable = BranchNamespaceControlTable{}
var _ sql.ReplaceableTable = BranchNamespaceControlTable{}
var _ sql.UpdatableTable = BranchNamespaceControlTable{}
var _ sql.DeletableTable = BranchNamespaceControlTable{}
var _ sql.RowInserter = BranchNamespaceControlTable{}
var _ sql.RowReplacer = BranchNamespaceControlTable{}
var _ sql.RowUpdater = BranchNamespaceControlTable{}
var _ sql.RowDeleter = BranchNamespaceControlTable{}

// NewBranchNamespaceControlTable returns a new BranchNamespaceControlTable.
func NewBranchNamespaceControlTable(namespace *branch_control.Namespace) BranchNamespaceControlTable {
	return BranchNamespaceControlTable{namespace}
}

// Name implements the interface sql.Table.
func (tbl BranchNamespaceControlTable) Name() string {
	return NamespaceTableName
}

// String implements the interface sql.Table.
func (tbl BranchNamespaceControlTable) String() string {
	return NamespaceTableName
}

// Schema implements the interface sql.Table.
func (tbl BranchNamespaceControlTable) Schema() sql.Schema {
	return namespaceSchema
}

// Collation implements the interface sql.Table.
func (tbl BranchNamespaceControlTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the interface sql.Table.
func (tbl BranchNamespaceControlTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows implements the interface sql.Table.
func (tbl BranchNamespaceControlTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	tbl.RWMutex.RLock()
	defer tbl.RWMutex.RUnlock()

	var rows []sql.Row
	for _, value := range tbl.Values {
		rows = append(rows, sql.UntypedSqlRow{
			value.Database,
			value.Branch,
			value.User,
			value.Host,
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Inserter implements the interface sql.InsertableTable.
func (tbl BranchNamespaceControlTable) Inserter(context *sql.Context) sql.RowInserter {
	return tbl
}

// Replacer implements the interface sql.ReplaceableTable.
func (tbl BranchNamespaceControlTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return tbl
}

// Updater implements the interface sql.UpdatableTable.
func (tbl BranchNamespaceControlTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return tbl
}

// Deleter implements the interface sql.DeletableTable.
func (tbl BranchNamespaceControlTable) Deleter(context *sql.Context) sql.RowDeleter {
	return tbl
}

// StatementBegin implements the interface sql.TableEditor.
func (tbl BranchNamespaceControlTable) StatementBegin(ctx *sql.Context) {
	//TODO: will use the binlog to implement
}

// DiscardChanges implements the interface sql.TableEditor.
func (tbl BranchNamespaceControlTable) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	//TODO: will use the binlog to implement
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (tbl BranchNamespaceControlTable) StatementComplete(ctx *sql.Context) error {
	//TODO: will use the binlog to implement
	return nil
}

// Insert implements the interface sql.RowInserter.
func (tbl BranchNamespaceControlTable) Insert(ctx *sql.Context, row sql.Row) error {
	tbl.RWMutex.Lock()
	defer tbl.RWMutex.Unlock()

	// Database, Branch, and Host are case-insensitive, while user is case-sensitive
	database := strings.ToLower(branch_control.FoldExpression(row.GetValue(0).(string)))
	branch := strings.ToLower(branch_control.FoldExpression(row.GetValue(1).(string)))
	user := branch_control.FoldExpression(row.GetValue(2).(string))
	host := strings.ToLower(branch_control.FoldExpression(row.GetValue(3).(string)))

	// Verify that the lengths of each expression fit within an uint16
	if len(database) > math.MaxUint16 || len(branch) > math.MaxUint16 || len(user) > math.MaxUint16 || len(host) > math.MaxUint16 {
		return branch_control.ErrExpressionsTooLong.New(database, branch, user, host)
	}

	// A nil session means we're not in the SQL context, so we allow the insertion in such a case
	if branchAwareSession := branch_control.GetBranchAwareSession(ctx); branchAwareSession != nil &&
		// Having the correct database privileges also allows the insertion
		!branch_control.HasDatabasePrivileges(branchAwareSession, database) {

		// tbl.Access() shares a lock with the namespace table. No need to acquire its lock.

		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
		// determine if the user attempting the insertion has permission to perform the insertion.
		_, modPerms := tbl.Access().Match(database, branch, insertUser, insertHost)
		if modPerms&branch_control.Permissions_Admin != branch_control.Permissions_Admin {
			return branch_control.ErrInsertingNamespaceRow.New(insertUser, insertHost, database, branch, user, host)
		}
	}

	return tbl.insert(ctx, database, branch, user, host)
}

// Update implements the interface sql.RowUpdater.
func (tbl BranchNamespaceControlTable) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
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

	// Verify that the lengths of each expression fit within an uint16
	if len(newDatabase) > math.MaxUint16 || len(newBranch) > math.MaxUint16 || len(newUser) > math.MaxUint16 || len(newHost) > math.MaxUint16 {
		return branch_control.ErrExpressionsTooLong.New(newDatabase, newBranch, newUser, newHost)
	}

	// If we're not updating the same row, then we pre-emptively check for a row violation
	if oldDatabase != newDatabase || oldBranch != newBranch || oldUser != newUser || oldHost != newHost {
		if tblIndex := tbl.GetIndex(newDatabase, newBranch, newUser, newHost); tblIndex != -1 {
			return sql.NewUniqueKeyErr(
				fmt.Sprintf(`[%q, %q, %q, %q]`, newDatabase, newBranch, newUser, newHost),
				true,
				sql.UntypedSqlRow{newDatabase, newBranch, newUser, newHost})
		}
	}

	// A nil session means we're not in the SQL context, so we'd allow the update in such a case
	if branchAwareSession := branch_control.GetBranchAwareSession(ctx); branchAwareSession != nil {
		// tbl.Access() shares a lock with the namespace table. No need to acquire its lock.

		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		if !branch_control.HasDatabasePrivileges(branchAwareSession, oldDatabase) {
			// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
			// determine if the user attempting the update has permission to perform the update on the old branch name.
			_, modPerms := tbl.Access().Match(oldDatabase, oldBranch, insertUser, insertHost)
			if modPerms&branch_control.Permissions_Admin != branch_control.Permissions_Admin {
				return branch_control.ErrUpdatingRow.New(insertUser, insertHost, oldDatabase, oldBranch, oldUser, oldHost)
			}
		}
		if !branch_control.HasDatabasePrivileges(branchAwareSession, newDatabase) {
			// Similar to the block above, we check if the user has permission to use the new branch name
			_, modPerms := tbl.Access().Match(newDatabase, newBranch, insertUser, insertHost)
			if modPerms&branch_control.Permissions_Admin != branch_control.Permissions_Admin {
				return branch_control.ErrUpdatingToRow.
					New(insertUser, insertHost, oldDatabase, oldBranch, oldUser, oldHost, newDatabase, newBranch)
			}
		}
	}

	if tblIndex := tbl.GetIndex(oldDatabase, oldBranch, oldUser, oldHost); tblIndex != -1 {
		if err := tbl.delete(ctx, oldDatabase, oldBranch, oldUser, oldHost); err != nil {
			return err
		}
	}
	return tbl.insert(ctx, newDatabase, newBranch, newUser, newHost)
}

// Delete implements the interface sql.RowDeleter.
func (tbl BranchNamespaceControlTable) Delete(ctx *sql.Context, row sql.Row) error {
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

		// tbl.Access() shares a lock with the namespace table. No need to acquire its lock.

		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
		// determine if the user attempting the deletion has permission to perform the deletion.
		_, modPerms := tbl.Access().Match(database, branch, insertUser, insertHost)
		if modPerms&branch_control.Permissions_Admin != branch_control.Permissions_Admin {
			return branch_control.ErrDeletingRow.New(insertUser, insertHost, database, branch, user, host)
		}
	}

	return tbl.delete(ctx, database, branch, user, host)
}

// Close implements the interface sql.Closer.
func (tbl BranchNamespaceControlTable) Close(context *sql.Context) error {
	return branch_control.SaveData(context)
}

// insert adds the given branch, user, and host expression strings to the table. Assumes that the expressions have
// already been folded.
func (tbl BranchNamespaceControlTable) insert(ctx context.Context, database, branch, user, host string) error {
	// If we already have this in the table, then we return a duplicate PK error
	if tblIndex := tbl.GetIndex(database, branch, user, host); tblIndex != -1 {
		return sql.NewUniqueKeyErr(
			fmt.Sprintf(`[%q, %q, %q, %q]`, database, branch, user, host),
			true,
			sql.UntypedSqlRow{database, branch, user, host})
	}

	// Add an entry to the binlog
	tbl.GetBinlog().Insert(database, branch, user, host, 0)
	// Add the expressions to their respective slices
	databaseExpr := branch_control.ParseExpression(database, sql.Collation_utf8mb4_0900_ai_ci)
	branchExpr := branch_control.ParseExpression(branch, sql.Collation_utf8mb4_0900_ai_ci)
	userExpr := branch_control.ParseExpression(user, sql.Collation_utf8mb4_0900_bin)
	hostExpr := branch_control.ParseExpression(host, sql.Collation_utf8mb4_0900_ai_ci)
	nextIdx := uint32(len(tbl.Values))
	tbl.Databases = append(tbl.Databases, branch_control.MatchExpression{CollectionIndex: nextIdx, SortOrders: databaseExpr})
	tbl.Branches = append(tbl.Branches, branch_control.MatchExpression{CollectionIndex: nextIdx, SortOrders: branchExpr})
	tbl.Users = append(tbl.Users, branch_control.MatchExpression{CollectionIndex: nextIdx, SortOrders: userExpr})
	tbl.Hosts = append(tbl.Hosts, branch_control.MatchExpression{CollectionIndex: nextIdx, SortOrders: hostExpr})
	tbl.Values = append(tbl.Values, branch_control.NamespaceValue{
		Database: database,
		Branch:   branch,
		User:     user,
		Host:     host,
	})
	return nil
}

// delete removes the given branch, user, and host expression strings from the table. Assumes that the expressions have
// already been folded.
func (tbl BranchNamespaceControlTable) delete(ctx context.Context, database, branch, user, host string) error {
	// If we don't have this in the table, then we just return
	tblIndex := tbl.GetIndex(database, branch, user, host)
	if tblIndex == -1 {
		return nil
	}

	endIndex := len(tbl.Values) - 1
	// Add an entry to the binlog
	tbl.GetBinlog().Delete(database, branch, user, host, 0)
	// Remove the matching row from all slices by first swapping with the last element
	tbl.Databases[tblIndex], tbl.Databases[endIndex] = tbl.Databases[endIndex], tbl.Databases[tblIndex]
	tbl.Branches[tblIndex], tbl.Branches[endIndex] = tbl.Branches[endIndex], tbl.Branches[tblIndex]
	tbl.Users[tblIndex], tbl.Users[endIndex] = tbl.Users[endIndex], tbl.Users[tblIndex]
	tbl.Hosts[tblIndex], tbl.Hosts[endIndex] = tbl.Hosts[endIndex], tbl.Hosts[tblIndex]
	tbl.Values[tblIndex], tbl.Values[endIndex] = tbl.Values[endIndex], tbl.Values[tblIndex]
	// Then we remove the last element
	tbl.Databases = tbl.Databases[:endIndex]
	tbl.Branches = tbl.Branches[:endIndex]
	tbl.Users = tbl.Users[:endIndex]
	tbl.Hosts = tbl.Hosts[:endIndex]
	tbl.Values = tbl.Values[:endIndex]
	// Then we update the index for the match expressions
	if tblIndex != endIndex {
		tbl.Databases[tblIndex].CollectionIndex = uint32(tblIndex)
		tbl.Branches[tblIndex].CollectionIndex = uint32(tblIndex)
		tbl.Users[tblIndex].CollectionIndex = uint32(tblIndex)
		tbl.Hosts[tblIndex].CollectionIndex = uint32(tblIndex)
	}
	return nil
}
