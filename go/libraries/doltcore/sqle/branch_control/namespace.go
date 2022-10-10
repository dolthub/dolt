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

const (
	NamespaceTableName = "dolt_branch_namespace_control"
)

// namespaceSchema is the schema for the "dolt_branch_namespace_control" table.
var namespaceSchema = sql.Schema{
	&sql.Column{
		Name:       "branch",
		Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     NamespaceTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "user",
		Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_bin),
		Source:     NamespaceTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "host",
		Type:       sql.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     NamespaceTableName,
		PrimaryKey: true,
	},
}

// namespace contains all of the expressions that comprise the "dolt_branch_namespace_control" table, which controls
// which users may use which branch names when creating branches. Modification of this table is handled by the access
// table.
type namespace struct {
	access *access
	binlog *Binlog

	Branches  []MatchExpression
	Users     []MatchExpression
	Hosts     []MatchExpression
	Values    []namespaceValue
	SuperUser string
	SuperHost string
	RWMutex   *sync.RWMutex
}

var _ sql.Table = (*namespace)(nil)
var _ sql.InsertableTable = (*namespace)(nil)
var _ sql.ReplaceableTable = (*namespace)(nil)
var _ sql.UpdatableTable = (*namespace)(nil)
var _ sql.DeletableTable = (*namespace)(nil)
var _ sql.RowInserter = (*namespace)(nil)
var _ sql.RowReplacer = (*namespace)(nil)
var _ sql.RowUpdater = (*namespace)(nil)
var _ sql.RowDeleter = (*namespace)(nil)

// namespaceValue contains the user-facing values of a particular row.
type namespaceValue struct {
	Branch string
	User   string
	Host   string
}

// newNamespace returns a new namespace.
func newNamespace(accessTbl *access, superUser string, superHost string) *namespace {
	return &namespace{
		access:    accessTbl,
		Branches:  nil,
		Users:     nil,
		Hosts:     nil,
		Values:    nil,
		SuperUser: superUser,
		SuperHost: superHost,
		RWMutex:   &sync.RWMutex{},
	}
}

// CanCreate checks the given branch, and returns whether the given user and host combination is able to create that
// branch. Handles the super user case.
func (tbl *namespace) CanCreate(branch string, user string, host string) bool {
	// Super user can always create branches
	if user == tbl.SuperUser && host == tbl.SuperHost {
		return true
	}
	matchedSet := Match(tbl.Branches, branch, sql.Collation_utf8mb4_0900_ai_ci)
	// If there are no branch entries, then the namespace is unrestricted
	if len(matchedSet) == 0 {
		return true
	}

	// We take either the longest match, or the set of longest matches if multiple matches have the same length
	longest := -1
	var narrowedSet []uint32
	for _, matched := range matchedSet {
		matchedValue := tbl.Values[matched]
		// If we've found a longer match, then we reset the slice. We append to it in the following if statement.
		if len(matchedValue.Branch) > longest {
			narrowedSet = narrowedSet[:0]
		}
		if len(matchedValue.Branch) >= longest {
			narrowedSet = append(narrowedSet, matched)
		}
	}

	narrowedSet = Match(tbl.RestrictUsers(narrowedSet), user, sql.Collation_utf8mb4_0900_bin)
	narrowedSet = Match(tbl.RestrictHosts(narrowedSet), host, sql.Collation_utf8mb4_0900_ai_ci)
	return len(narrowedSet) > 0
}

// GetIndex returns the index of the given branch, user, and host expressions. If the expressions cannot be found,
// returns -1. Assumes that the given expressions have already been folded.
func (tbl *namespace) GetIndex(branchExpr string, userExpr string, hostExpr string) int {
	for i, value := range tbl.Values {
		if value.Branch == branchExpr && value.User == userExpr && value.Host == hostExpr {
			return i
		}
	}
	return -1
}

// BinlogTable returns the BinlogTable for the namespace table.
func (tbl *namespace) BinlogTable() BinlogTable {
	return BinlogTable{
		Log:      tbl.binlog,
		IsAccess: false,
	}
}

// RestrictBranches returns all branches referenced in the allowed set.
func (tbl *namespace) RestrictBranches(allowed []uint32) []MatchExpression {
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
func (tbl *namespace) RestrictUsers(allowed []uint32) []MatchExpression {
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
func (tbl *namespace) RestrictHosts(allowed []uint32) []MatchExpression {
	if len(allowed) == 0 {
		return nil
	}
	matchExprs := make([]MatchExpression, len(allowed))
	for idx, branchIdx := range allowed {
		matchExprs[idx] = tbl.Hosts[branchIdx]
	}
	return matchExprs
}

// Name implements the interface sql.Table.
func (tbl *namespace) Name() string {
	return NamespaceTableName
}

// String implements the interface sql.Table.
func (tbl *namespace) String() string {
	return NamespaceTableName
}

// Schema implements the interface sql.Table.
func (tbl *namespace) Schema() sql.Schema {
	return namespaceSchema
}

// Collation implements the interface sql.Table.
func (tbl *namespace) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the interface sql.Table.
func (tbl *namespace) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows implements the interface sql.Table.
func (tbl *namespace) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	tbl.RWMutex.RLock()
	defer tbl.RWMutex.RUnlock()

	var rows []sql.Row
	for _, value := range tbl.Values {
		rows = append(rows, sql.Row{
			value.Branch,
			value.User,
			value.Host,
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Inserter implements the interface sql.InsertableTable.
func (tbl *namespace) Inserter(context *sql.Context) sql.RowInserter {
	return tbl
}

// Replacer implements the interface sql.ReplaceableTable.
func (tbl *namespace) Replacer(ctx *sql.Context) sql.RowReplacer {
	return tbl
}

// Updater implements the interface sql.UpdatableTable.
func (tbl *namespace) Updater(ctx *sql.Context) sql.RowUpdater {
	return tbl
}

// Deleter implements the interface sql.DeletableTable.
func (tbl *namespace) Deleter(context *sql.Context) sql.RowDeleter {
	return tbl
}

// StatementBegin implements the interface sql.TableEditor.
func (tbl *namespace) StatementBegin(ctx *sql.Context) {
	//TODO: will use the binlog to implement
}

// DiscardChanges implements the interface sql.TableEditor.
func (tbl *namespace) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	//TODO: will use the binlog to implement
	return nil
}

// StatementComplete implements the interface sql.TableEditor.
func (tbl *namespace) StatementComplete(ctx *sql.Context) error {
	//TODO: will use the binlog to implement
	return nil
}

// Insert implements the interface sql.RowInserter.
func (tbl *namespace) Insert(ctx *sql.Context, row sql.Row) error {
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

	// A nil session means we're not in the SQL context, so we allow the insertion in such a case
	if branchAwareSession := getBranchAwareSession(ctx); branchAwareSession != nil {
		// Need to acquire a read lock on the access table since we have to read from it
		tbl.access.RWMutex.RLock()
		defer tbl.access.RWMutex.RUnlock()

		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
		// determine if the user attempting the insertion has permission to perform the insertion.
		_, modPerms := tbl.access.Match(branch, insertUser, insertHost)
		if modPerms&Permissions_Admin != Permissions_Admin {
			return fmt.Errorf("`%s`@`%s` cannot add the row [%q, %q, %q]",
				insertUser, insertHost, branch, user, host)
		}
	}

	return tbl.insert(ctx, branch, user, host)
}

// Update implements the interface sql.RowUpdater.
func (tbl *namespace) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	tbl.RWMutex.Lock()
	defer tbl.RWMutex.Unlock()

	// Branch and Host are case-insensitive, while user is case-sensitive
	oldBranch := strings.ToLower(FoldExpression(old[0].(string)))
	oldUser := FoldExpression(old[1].(string))
	oldHost := strings.ToLower(FoldExpression(old[2].(string)))
	newBranch := strings.ToLower(FoldExpression(new[0].(string)))
	newUser := FoldExpression(new[1].(string))
	newHost := strings.ToLower(FoldExpression(new[2].(string)))

	// Verify that the lengths of each expression fit within an uint16
	if len(newBranch) > math.MaxUint16 || len(newUser) > math.MaxUint16 || len(newHost) > math.MaxUint16 {
		return fmt.Errorf("expressions are too long [%q, %q, %q]", newBranch, newUser, newHost)
	}

	// If we're not updating the same row, then we pre-emptively check for a row violation
	if oldBranch != newBranch || oldUser != newUser || oldHost != newHost {
		if tblIndex := tbl.GetIndex(newBranch, newUser, newHost); tblIndex != -1 {
			return sql.NewUniqueKeyErr(
				fmt.Sprintf(`[%q, %q, %q]`, newBranch, newUser, newHost),
				true,
				sql.Row{newBranch, newUser, newHost})
		}
	}

	// A nil session means we're not in the SQL context, so we'd allow the update in such a case
	if branchAwareSession := getBranchAwareSession(ctx); branchAwareSession != nil {
		// Need to acquire a read lock on the access table since we have to read from it
		tbl.access.RWMutex.RLock()
		defer tbl.access.RWMutex.RUnlock()

		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
		// determine if the user attempting the update has permission to perform the update on the old branch name.
		_, modPerms := tbl.access.Match(oldBranch, insertUser, insertHost)
		if modPerms&Permissions_Admin != Permissions_Admin {
			return fmt.Errorf("`%s`@`%s` cannot update the row [%q, %q, %q]",
				insertUser, insertHost, oldBranch, oldUser, oldHost)
		}
		// Now we check if the user has permission use the new branch name
		_, modPerms = tbl.access.Match(newBranch, insertUser, insertHost)
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
	return tbl.insert(ctx, newBranch, newUser, newHost)
}

// Delete implements the interface sql.RowDeleter.
func (tbl *namespace) Delete(ctx *sql.Context, row sql.Row) error {
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
		// Need to acquire a read lock on the access table since we have to read from it
		tbl.access.RWMutex.RLock()
		defer tbl.access.RWMutex.RUnlock()

		insertUser := branchAwareSession.GetUser()
		insertHost := branchAwareSession.GetHost()
		// As we've folded the branch expression, we can use it directly as though it were a normal branch name to
		// determine if the user attempting the deletion has permission to perform the deletion.
		_, modPerms := tbl.access.Match(branch, insertUser, insertHost)
		if modPerms&Permissions_Admin != Permissions_Admin {
			return fmt.Errorf("`%s`@`%s` cannot delete the row [%q, %q, %q]",
				insertUser, insertHost, branch, user, host)
		}
	}

	return tbl.delete(ctx, branch, user, host)
}

// Close implements the interface sql.Closer.
func (tbl *namespace) Close(context *sql.Context) error {
	//TODO: write the binlog
	return nil
}

// insert adds the given branch, user, and host expression strings to the table. Assumes that the expressions have
// already been folded.
func (tbl *namespace) insert(ctx context.Context, branch string, user string, host string) error {
	// If we already have this in the table, then we return a duplicate PK error
	if tblIndex := tbl.GetIndex(branch, user, host); tblIndex != -1 {
		return sql.NewUniqueKeyErr(
			fmt.Sprintf(`[%q, %q, %q]`, branch, user, host),
			true,
			sql.Row{branch, user, host})
	}

	// Add the expressions to their respective slices
	branchExpr := ParseExpression(branch, sql.Collation_utf8mb4_0900_ai_ci)
	userExpr := ParseExpression(user, sql.Collation_utf8mb4_0900_bin)
	hostExpr := ParseExpression(host, sql.Collation_utf8mb4_0900_ai_ci)
	nextIdx := uint32(len(tbl.Values))
	tbl.Branches = append(tbl.Branches, MatchExpression{nextIdx, branchExpr})
	tbl.Users = append(tbl.Users, MatchExpression{nextIdx, userExpr})
	tbl.Hosts = append(tbl.Hosts, MatchExpression{nextIdx, hostExpr})
	return nil
}

// delete removes the given branch, user, and host expression strings from the table. Assumes that the expressions have
// already been folded.
func (tbl *namespace) delete(ctx context.Context, branch string, user string, host string) error {
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
