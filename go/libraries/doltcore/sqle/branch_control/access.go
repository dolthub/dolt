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
	"math"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

// Permissions are a set of flags that denote a user's allowed functionality on a branch.
type Permissions uint64

const (
	Permissions_Admin Permissions = 1 << iota // Permissions_Admin grants unrestricted control over a branch, including modification of table entries
	Permissions_Write                         // Permissions_Write allows for all modifying operations on a branch, but does not allow modification of table entries
)

// permissionsStrings is a slice of strings representing the above permissions. The order of the strings should exactly
// match the order of the permissions according to their flag value.
var permissionsStrings = []string{"admin", "write"}

const (
	AccessTableName = "dolt_branch_control"
)

// accessSchema is the schema for the "dolt_branch_control" table.
var accessSchema = sql.Schema{
	&sql.Column{
		Name:       "branch",
		Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     AccessTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "user",
		Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_bin),
		Source:     AccessTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "host",
		Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     AccessTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "permissions",
		Type:       sql.MustCreateSetType(permissionsStrings, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     AccessTableName,
		PrimaryKey: false,
	},
}

// access contains all of the expressions that comprise the "dolt_branch_control" table, which handles write access to
// branches, along with write access to the branch control system tables.
type access struct {
	binlog *Binlog

	Branches  []MatchExpression
	Users     []MatchExpression
	Hosts     []MatchExpression
	Values    []accessValue
	SuperUser string
	SuperHost string
	RWMutex   *sync.RWMutex
}

var _ sql.Table = (*access)(nil)
var _ sql.InsertableTable = (*access)(nil)
var _ sql.ReplaceableTable = (*access)(nil)
var _ sql.UpdatableTable = (*access)(nil)
var _ sql.DeletableTable = (*access)(nil)
var _ sql.RowInserter = (*access)(nil)
var _ sql.RowReplacer = (*access)(nil)
var _ sql.RowUpdater = (*access)(nil)
var _ sql.RowDeleter = (*access)(nil)

// accessValue contains the user-facing values of a particular row, along with the permissions for a row.
type accessValue struct {
	Branch      string
	User        string
	Host        string
	Permissions Permissions
}

// newAccess returns a new access.
func newAccess(superUser string, superHost string) *access {
	return &access{
		Branches:  nil,
		Users:     nil,
		Hosts:     nil,
		Values:    nil,
		SuperUser: superUser,
		SuperHost: superHost,
		RWMutex:   &sync.RWMutex{},
	}
}

// Match returns whether any entries match the given branch, user, and host, along with their permissions.
func (tbl *access) Match(branch string, user string, host string) (bool, Permissions) {
	allowedSet := Match(tbl.Users, user, sql.Collation_utf8mb4_0900_bin)
	allowedSet = Match(tbl.RestrictHosts(allowedSet), host, sql.Collation_utf8mb4_0900_ai_ci)
	allowedSet = Match(tbl.RestrictBranches(allowedSet), branch, sql.Collation_utf8mb4_0900_ai_ci)
	return len(allowedSet) > 0, tbl.GatherPermissions(allowedSet)
}

// GetIndex returns the index of the given branch, user, and host expressions. If the expressions cannot be found,
// returns -1. Assumes that the given expressions have already been folded.
func (tbl *access) GetIndex(branchExpr string, userExpr string, hostExpr string) int {
	for i, value := range tbl.Values {
		if value.Branch == branchExpr && value.User == userExpr && value.Host == hostExpr {
			return i
		}
	}
	return -1
}

// BinlogTable returns the BinlogTable for the access table.
func (tbl *access) BinlogTable() BinlogTable {
	return BinlogTable{
		Log:      tbl.binlog,
		IsAccess: true,
	}
}

// RestrictBranches returns all branches referenced in the allowed set.
func (tbl *access) RestrictBranches(allowed []uint32) []MatchExpression {
	if len(allowed) == 0 {
		return nil
	}
	matchExprs := make([]MatchExpression, len(allowed))
	for idx, branchIdx := range allowed {
		matchExprs[idx] = tbl.Branches[branchIdx]
	}
	return matchExprs
}

// RestrictUsers returns all users referenced in the allowed set.
func (tbl *access) RestrictUsers(allowed []uint32) []MatchExpression {
	if len(allowed) == 0 {
		return nil
	}
	matchExprs := make([]MatchExpression, len(allowed))
	for idx, branchIdx := range allowed {
		matchExprs[idx] = tbl.Users[branchIdx]
	}
	return matchExprs
}

// RestrictHosts returns all hosts referenced in the allowed set.
func (tbl *access) RestrictHosts(allowed []uint32) []MatchExpression {
	if len(allowed) == 0 {
		return nil
	}
	matchExprs := make([]MatchExpression, len(allowed))
	for idx, branchIdx := range allowed {
		matchExprs[idx] = tbl.Hosts[branchIdx]
	}
	return matchExprs
}

// GatherPermissions combines all permissions from the allowed set and returns the result.
func (tbl *access) GatherPermissions(allowed []uint32) Permissions {
	perms := Permissions(0)
	for _, permIdx := range allowed {
		perms |= tbl.Values[permIdx].Permissions
	}
	return perms
}

// Name implements the interface sql.Table.
func (tbl *access) Name() string {
	return AccessTableName
}

// String implements the interface sql.Table.
func (tbl *access) String() string {
	return AccessTableName
}

// Schema implements the interface sql.Table.
func (tbl *access) Schema() sql.Schema {
	return accessSchema
}

// Collation implements the interface sql.Table.
func (tbl *access) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the interface sql.Table.
func (tbl *access) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows implements the interface sql.Table.
func (tbl *access) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	tbl.RWMutex.RLock()
	defer tbl.RWMutex.RUnlock()

	rows := []sql.Row{{"%", tbl.SuperUser, tbl.SuperHost, uint64(Permissions_Admin)}}
	for _, value := range tbl.Values {
		rows = append(rows, sql.Row{
			value.Branch,
			value.User,
			value.Host,
			uint64(value.Permissions),
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Inserter implements the interface sql.InsertableTable.
func (tbl *access) Inserter(context *sql.Context) sql.RowInserter {
	return tbl
}

// Replacer implements the interface sql.ReplaceableTable.
func (tbl *access) Replacer(ctx *sql.Context) sql.RowReplacer {
	return tbl
}

// Updater implements the interface sql.UpdatableTable.
func (tbl *access) Updater(ctx *sql.Context) sql.RowUpdater {
	return tbl
}

// Deleter implements the interface sql.DeletableTable.
func (tbl *access) Deleter(context *sql.Context) sql.RowDeleter {
	return tbl
}

// StatementBegin implements the interface sql.TableEditor.
func (tbl *access) StatementBegin(ctx *sql.Context) {
	//TODO: will use the binlog to implement
}

// DiscardChanges implements the interface sql.TableEditor.
func (tbl *access) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	//TODO: will use the binlog to implement
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (tbl *access) StatementComplete(ctx *sql.Context) error {
	//TODO: will use the binlog to implement
	return nil
}

// Insert implements the interface sql.RowInserter.
func (tbl *access) Insert(ctx *sql.Context, row sql.Row) error {
	tbl.RWMutex.Lock()
	defer tbl.RWMutex.Unlock()

	// Branch and Host are case-insensitive, while user is case-sensitive
	branch := strings.ToLower(FoldExpression(row[0].(string)))
	user := FoldExpression(row[1].(string))
	host := strings.ToLower(FoldExpression(row[2].(string)))
	perms := Permissions(row[3].(uint64))

	// Verify that the lengths of each expression fit within an uint16
	if len(branch) > math.MaxUint16 || len(user) > math.MaxUint16 || len(host) > math.MaxUint16 {
		return fmt.Errorf("expressions are too long [%q, %q, %q]", branch, user, host)
	}

	// A nil session means we're not in the SQL context, so we allow the insertion in such a case
	if branchAwareSession := getBranchAwareSession(ctx); branchAwareSession != nil {
		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
		// determine if the user attempting the insertion has permission to perform the insertion.
		_, modPerms := tbl.Match(branch, insertUser, insertHost)
		if modPerms&Permissions_Admin != Permissions_Admin {
			permStr, _ := accessSchema[3].Type.(sql.SetType).BitsToString(uint64(perms))
			return fmt.Errorf("`%s`@`%s` cannot add the row [%q, %q, %q, %q]",
				insertUser, insertHost, branch, user, host, permStr)
		}
	}

	return tbl.insert(ctx, branch, user, host, perms)
}

// Update implements the interface sql.RowUpdater.
func (tbl *access) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	tbl.RWMutex.Lock()
	defer tbl.RWMutex.Unlock()

	// Branch and Host are case-insensitive, while user is case-sensitive
	oldBranch := strings.ToLower(FoldExpression(old[0].(string)))
	oldUser := FoldExpression(old[1].(string))
	oldHost := strings.ToLower(FoldExpression(old[2].(string)))
	newBranch := strings.ToLower(FoldExpression(new[0].(string)))
	newUser := FoldExpression(new[1].(string))
	newHost := strings.ToLower(FoldExpression(new[2].(string)))
	newPerms := Permissions(new[3].(uint64))

	// Verify that the lengths of each expression fit within an uint16
	if len(newBranch) > math.MaxUint16 || len(newUser) > math.MaxUint16 || len(newHost) > math.MaxUint16 {
		return fmt.Errorf("expressions are too long [%q, %q, %q]", newBranch, newUser, newHost)
	}

	// If we're not updating the same row, then we pre-emptively check for a row violation
	if oldBranch != newBranch || oldUser != newUser || oldHost != newHost {
		if tblIndex := tbl.GetIndex(newBranch, newUser, newHost); tblIndex != -1 {
			permBits := uint64(tbl.Values[tblIndex].Permissions)
			permStr, _ := accessSchema[3].Type.(sql.SetType).BitsToString(permBits)
			return sql.NewUniqueKeyErr(
				fmt.Sprintf(`[%q, %q, %q, %q]`, newBranch, newUser, newHost, permStr),
				true,
				sql.Row{newBranch, newUser, newHost, permBits})
		}
	}

	// A nil session means we're not in the SQL context, so we'd allow the update in such a case
	if branchAwareSession := getBranchAwareSession(ctx); branchAwareSession != nil {
		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
		// determine if the user attempting the update has permission to perform the update on the old branch name.
		_, modPerms := tbl.Match(oldBranch, insertUser, insertHost)
		if modPerms&Permissions_Admin != Permissions_Admin {
			return fmt.Errorf("`%s`@`%s` cannot update the row [%q, %q, %q]",
				insertUser, insertHost, oldBranch, oldUser, oldHost)
		}
		// Now we check if the user has permission use the new branch name
		_, modPerms = tbl.Match(newBranch, insertUser, insertHost)
		if modPerms&Permissions_Admin != Permissions_Admin {
			return fmt.Errorf("`%s`@`%s` cannot update the row [%q, %q, %q] to the new branch expression %q",
				insertUser, insertHost, oldBranch, oldUser, oldHost, newBranch)
		}
	}

	if tblIndex := tbl.GetIndex(oldBranch, oldUser, oldHost); tblIndex != -1 {
		if err := tbl.delete(ctx, oldBranch, oldUser, oldHost); err != nil {
			return err
		}
	}
	return tbl.insert(ctx, newBranch, newUser, newHost, newPerms)
}

// Delete implements the interface sql.RowDeleter.
func (tbl *access) Delete(ctx *sql.Context, row sql.Row) error {
	tbl.RWMutex.Lock()
	defer tbl.RWMutex.Unlock()

	// Branch and Host are case-insensitive, while user is case-sensitive
	branch := strings.ToLower(FoldExpression(row[0].(string)))
	user := FoldExpression(row[1].(string))
	host := strings.ToLower(FoldExpression(row[2].(string)))

	// Verify that the lengths of each expression fit within an uint16
	if len(branch) > math.MaxUint16 || len(user) > math.MaxUint16 || len(host) > math.MaxUint16 {
		return fmt.Errorf("expressions are too long [%q, %q, %q]", branch, user, host)
	}

	// A nil session means we're not in the SQL context, so we allow the deletion in such a case
	if branchAwareSession := getBranchAwareSession(ctx); branchAwareSession != nil {
		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
		// determine if the user attempting the deletion has permission to perform the deletion.
		_, modPerms := tbl.Match(branch, insertUser, insertHost)
		if modPerms&Permissions_Admin != Permissions_Admin {
			return fmt.Errorf("`%s`@`%s` cannot delete the row [%q, %q, %q]",
				insertUser, insertHost, branch, user, host)
		}
	}

	return tbl.delete(ctx, branch, user, host)
}

// Close implements the interface sql.Closer.
func (tbl *access) Close(context *sql.Context) error {
	//TODO: write the binlog
	return nil
}

// insert adds the given branch, user, and host expression strings to the table. Assumes that the expressions have
// already been folded.
func (tbl *access) insert(ctx context.Context, branch string, user string, host string, perms Permissions) error {
	// If we already have this in the table, then we return a duplicate PK error
	if tblIndex := tbl.GetIndex(branch, user, host); tblIndex != -1 {
		permBits := uint64(tbl.Values[tblIndex].Permissions)
		permStr, _ := accessSchema[3].Type.(sql.SetType).BitsToString(permBits)
		return sql.NewUniqueKeyErr(
			fmt.Sprintf(`[%q, %q, %q, %q]`, branch, user, host, permStr),
			true,
			sql.Row{branch, user, host, permBits})
	}

	// Add the expressions to their respective slices
	branchExpr := ParseExpression(branch, sql.Collation_utf8mb4_0900_ai_ci)
	userExpr := ParseExpression(user, sql.Collation_utf8mb4_0900_bin)
	hostExpr := ParseExpression(host, sql.Collation_utf8mb4_0900_ai_ci)
	nextIdx := uint32(len(tbl.Values))
	tbl.Branches = append(tbl.Branches, MatchExpression{nextIdx, branchExpr})
	tbl.Users = append(tbl.Users, MatchExpression{nextIdx, userExpr})
	tbl.Hosts = append(tbl.Hosts, MatchExpression{nextIdx, hostExpr})
	tbl.Values = append(tbl.Values, accessValue{
		Branch:      branch,
		User:        user,
		Host:        host,
		Permissions: perms,
	})
	return nil
}

// delete removes the given branch, user, and host expression strings from the table. Assumes that the expressions have
// already been folded.
func (tbl *access) delete(ctx context.Context, branch string, user string, host string) error {
	// If we don't have this in the table, then we just return
	tblIndex := tbl.GetIndex(branch, user, host)
	if tblIndex == -1 {
		return nil
	}

	endIndex := len(tbl.Values) - 1
	// Remove the matching row from all slices by first swapping with the last element
	tbl.Branches[tblIndex], tbl.Branches[endIndex] = tbl.Branches[endIndex], tbl.Branches[tblIndex]
	tbl.Users[tblIndex], tbl.Users[endIndex] = tbl.Users[endIndex], tbl.Users[tblIndex]
	tbl.Hosts[tblIndex], tbl.Hosts[endIndex] = tbl.Hosts[endIndex], tbl.Hosts[tblIndex]
	tbl.Values[tblIndex], tbl.Values[endIndex] = tbl.Values[endIndex], tbl.Values[tblIndex]
	// Then we remove the last element
	tbl.Branches = tbl.Branches[:endIndex]
	tbl.Users = tbl.Users[:endIndex]
	tbl.Hosts = tbl.Hosts[:endIndex]
	tbl.Values = tbl.Values[:endIndex]
	return nil
}
