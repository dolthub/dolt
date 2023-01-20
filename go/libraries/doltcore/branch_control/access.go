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
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
)

// Permissions are a set of flags that denote a user's allowed functionality on a branch.
type Permissions uint64

const (
	Permissions_Admin Permissions = 1 << iota // Permissions_Admin grants unrestricted control over a branch, including modification of table entries
	Permissions_Write                         // Permissions_Write allows for all modifying operations on a branch, but does not allow modification of table entries
)

// Access contains all of the expressions that comprise the "dolt_branch_control" table, which handles write Access to
// branches, along with write access to the branch control system tables.
type Access struct {
	binlog *Binlog

	Databases []MatchExpression
	Branches  []MatchExpression
	Users     []MatchExpression
	Hosts     []MatchExpression
	Values    []AccessValue
	RWMutex   *sync.RWMutex
}

// AccessValue contains the user-facing values of a particular row, along with the permissions for a row.
type AccessValue struct {
	Database    string
	Branch      string
	User        string
	Host        string
	Permissions Permissions
}

// newAccess returns a new Access.
func newAccess() *Access {
	return &Access{
		binlog:    NewAccessBinlog(nil),
		Databases: nil,
		Branches:  nil,
		Users:     nil,
		Hosts:     nil,
		Values:    nil,
		RWMutex:   &sync.RWMutex{},
	}
}

// Match returns whether any entries match the given database, branch, user, and host, along with their permissions.
// Requires external synchronization handling, therefore manually manage the RWMutex.
func (tbl *Access) Match(database string, branch string, user string, host string) (bool, Permissions) {
	filteredIndexes := Match(tbl.Users, user, sql.Collation_utf8mb4_0900_bin)

	filteredHosts := tbl.filterHosts(filteredIndexes)
	indexPool.Put(filteredIndexes)
	filteredIndexes = Match(filteredHosts, host, sql.Collation_utf8mb4_0900_ai_ci)
	matchExprPool.Put(filteredHosts)

	filteredDatabases := tbl.filterDatabases(filteredIndexes)
	indexPool.Put(filteredIndexes)
	filteredIndexes = Match(filteredDatabases, database, sql.Collation_utf8mb4_0900_ai_ci)
	matchExprPool.Put(filteredDatabases)

	filteredBranches := tbl.filterBranches(filteredIndexes)
	indexPool.Put(filteredIndexes)
	filteredIndexes = Match(filteredBranches, branch, sql.Collation_utf8mb4_0900_ai_ci)
	matchExprPool.Put(filteredBranches)

	bRes, pRes := len(filteredIndexes) > 0, tbl.gatherPermissions(filteredIndexes)
	indexPool.Put(filteredIndexes)
	return bRes, pRes
}

// GetIndex returns the index of the given branch, user, and host expressions. If the expressions cannot be found,
// returns -1. Assumes that the given expressions have already been folded. Requires external synchronization handling,
// therefore manually manage the RWMutex.
func (tbl *Access) GetIndex(databaseExpr string, branchExpr string, userExpr string, hostExpr string) int {
	for i, value := range tbl.Values {
		if value.Database == databaseExpr && value.Branch == branchExpr && value.User == userExpr && value.Host == hostExpr {
			return i
		}
	}
	return -1
}

// GetBinlog returns the table's binlog.
func (tbl *Access) GetBinlog() *Binlog {
	return tbl.binlog
}

// Serialize returns the offset for the Access table written to the given builder.
func (tbl *Access) Serialize(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	tbl.RWMutex.RLock()
	defer tbl.RWMutex.RUnlock()

	// Serialize the binlog
	binlog := tbl.binlog.Serialize(b)
	// Initialize field offset slices
	databaseOffsets := make([]flatbuffers.UOffsetT, len(tbl.Databases))
	branchOffsets := make([]flatbuffers.UOffsetT, len(tbl.Branches))
	userOffsets := make([]flatbuffers.UOffsetT, len(tbl.Users))
	hostOffsets := make([]flatbuffers.UOffsetT, len(tbl.Hosts))
	valueOffsets := make([]flatbuffers.UOffsetT, len(tbl.Values))
	// Get field offsets
	for i, matchExpr := range tbl.Databases {
		databaseOffsets[i] = matchExpr.Serialize(b)
	}
	for i, matchExpr := range tbl.Branches {
		branchOffsets[i] = matchExpr.Serialize(b)
	}
	for i, matchExpr := range tbl.Users {
		userOffsets[i] = matchExpr.Serialize(b)
	}
	for i, matchExpr := range tbl.Hosts {
		hostOffsets[i] = matchExpr.Serialize(b)
	}
	for i, val := range tbl.Values {
		valueOffsets[i] = val.Serialize(b)
	}
	// Get the field vectors
	serial.BranchControlAccessStartDatabasesVector(b, len(databaseOffsets))
	for i := len(databaseOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(databaseOffsets[i])
	}
	databases := b.EndVector(len(databaseOffsets))
	serial.BranchControlAccessStartBranchesVector(b, len(branchOffsets))
	for i := len(branchOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(branchOffsets[i])
	}
	branches := b.EndVector(len(branchOffsets))
	serial.BranchControlAccessStartUsersVector(b, len(userOffsets))
	for i := len(userOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(userOffsets[i])
	}
	users := b.EndVector(len(userOffsets))
	serial.BranchControlAccessStartHostsVector(b, len(hostOffsets))
	for i := len(hostOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(hostOffsets[i])
	}
	hosts := b.EndVector(len(hostOffsets))
	serial.BranchControlAccessStartValuesVector(b, len(valueOffsets))
	for i := len(valueOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(valueOffsets[i])
	}
	values := b.EndVector(len(valueOffsets))
	// Write the table
	serial.BranchControlAccessStart(b)
	serial.BranchControlAccessAddBinlog(b, binlog)
	serial.BranchControlAccessAddDatabases(b, databases)
	serial.BranchControlAccessAddBranches(b, branches)
	serial.BranchControlAccessAddUsers(b, users)
	serial.BranchControlAccessAddHosts(b, hosts)
	serial.BranchControlAccessAddValues(b, values)
	return serial.BranchControlAccessEnd(b)
}

// Deserialize populates the table with the data from the flatbuffers representation.
func (tbl *Access) Deserialize(fb *serial.BranchControlAccess) error {
	tbl.RWMutex.Lock()
	defer tbl.RWMutex.Unlock()

	// Verify that the table is empty
	if len(tbl.Values) != 0 {
		return fmt.Errorf("cannot deserialize to a non-empty access table")
	}
	// Verify that all fields have the same length
	if fb.DatabasesLength() != fb.BranchesLength() ||
		fb.BranchesLength() != fb.UsersLength() ||
		fb.UsersLength() != fb.HostsLength() ||
		fb.HostsLength() != fb.ValuesLength() {
		return fmt.Errorf("cannot deserialize an access table with differing field lengths")
	}
	// Read the binlog
	binlog, err := fb.TryBinlog(nil)
	if err != nil {
		return err
	}
	if err = tbl.binlog.Deserialize(binlog); err != nil {
		return err
	}
	// Initialize every slice
	tbl.Databases = make([]MatchExpression, fb.DatabasesLength())
	tbl.Branches = make([]MatchExpression, fb.BranchesLength())
	tbl.Users = make([]MatchExpression, fb.UsersLength())
	tbl.Hosts = make([]MatchExpression, fb.HostsLength())
	tbl.Values = make([]AccessValue, fb.ValuesLength())
	// Read the databases
	for i := 0; i < fb.DatabasesLength(); i++ {
		serialMatchExpr := &serial.BranchControlMatchExpression{}
		fb.Databases(serialMatchExpr, i)
		tbl.Databases[i] = deserializeMatchExpression(serialMatchExpr)
	}
	// Read the branches
	for i := 0; i < fb.BranchesLength(); i++ {
		serialMatchExpr := &serial.BranchControlMatchExpression{}
		fb.Branches(serialMatchExpr, i)
		tbl.Branches[i] = deserializeMatchExpression(serialMatchExpr)
	}
	// Read the users
	for i := 0; i < fb.UsersLength(); i++ {
		serialMatchExpr := &serial.BranchControlMatchExpression{}
		fb.Users(serialMatchExpr, i)
		tbl.Users[i] = deserializeMatchExpression(serialMatchExpr)
	}
	// Read the hosts
	for i := 0; i < fb.HostsLength(); i++ {
		serialMatchExpr := &serial.BranchControlMatchExpression{}
		fb.Hosts(serialMatchExpr, i)
		tbl.Hosts[i] = deserializeMatchExpression(serialMatchExpr)
	}
	// Read the values
	for i := 0; i < fb.ValuesLength(); i++ {
		serialAccessValue := &serial.BranchControlAccessValue{}
		fb.Values(serialAccessValue, i)
		tbl.Values[i] = AccessValue{
			Database:    string(serialAccessValue.Database()),
			Branch:      string(serialAccessValue.Branch()),
			User:        string(serialAccessValue.User()),
			Host:        string(serialAccessValue.Host()),
			Permissions: Permissions(serialAccessValue.Permissions()),
		}
	}
	return nil
}

// filterDatabases returns all databases that match the given collection indexes.
func (tbl *Access) filterDatabases(filters []uint32) []MatchExpression {
	if len(filters) == 0 {
		return nil
	}
	matchExprs := matchExprPool.Get().([]MatchExpression)[:0]
	for _, filter := range filters {
		matchExprs = append(matchExprs, tbl.Databases[filter])
	}
	return matchExprs
}

// filterBranches returns all branches that match the given collection indexes.
func (tbl *Access) filterBranches(filters []uint32) []MatchExpression {
	if len(filters) == 0 {
		return nil
	}
	matchExprs := matchExprPool.Get().([]MatchExpression)[:0]
	for _, filter := range filters {
		matchExprs = append(matchExprs, tbl.Branches[filter])
	}
	return matchExprs
}

// filterUsers returns all users that match the given collection indexes.
func (tbl *Access) filterUsers(filters []uint32) []MatchExpression {
	if len(filters) == 0 {
		return nil
	}
	matchExprs := matchExprPool.Get().([]MatchExpression)[:0]
	for _, filter := range filters {
		matchExprs = append(matchExprs, tbl.Users[filter])
	}
	return matchExprs
}

// filterHosts returns all hosts that match the given collection indexes.
func (tbl *Access) filterHosts(filters []uint32) []MatchExpression {
	if len(filters) == 0 {
		return nil
	}
	matchExprs := matchExprPool.Get().([]MatchExpression)[:0]
	for _, filter := range filters {
		matchExprs = append(matchExprs, tbl.Hosts[filter])
	}
	return matchExprs
}

// gatherPermissions combines all permissions from the given collection indexes and returns the result.
func (tbl *Access) gatherPermissions(collectionIndexes []uint32) Permissions {
	perms := Permissions(0)
	for _, collectionIndex := range collectionIndexes {
		perms |= tbl.Values[collectionIndex].Permissions
	}
	return perms
}

// insertDefaultRow adds a row that allows all users to access and modify all branches, but does not allow them to
// modify any branch control tables. This was the default behavior of Dolt before the introduction of branch permissions.
func (tbl *Access) insertDefaultRow() {
	// Check if the appropriate row already exists
	for _, value := range tbl.Values {
		if value.Database == "%" && value.Branch == "%" && value.User == "%" && value.Host == "%" {
			// Getting to this state will be disallowed in the future, but if the row exists without any perms, then add
			// the Write perm
			if uint64(value.Permissions) == 0 {
				value.Permissions = Permissions_Write
			}
			return
		}
	}
	tbl.insert("%", "%", "%", "%", Permissions_Write)
}

// insert adds the given expressions to the table. This does not perform any sort of validation whatsoever, so it is
// important to ensure that the expressions are valid before insertion.
func (tbl *Access) insert(database string, branch string, user string, host string, perms Permissions) {
	// Database, Branch, and Host are case-insensitive, while User is case-sensitive
	database = strings.ToLower(FoldExpression(database))
	branch = strings.ToLower(FoldExpression(branch))
	user = FoldExpression(user)
	host = strings.ToLower(FoldExpression(host))
	// Each expression is capped at 2¹⁶-1 values, so we truncate to 2¹⁶-2 and add the any-match character at the end if it's over
	if len(database) > math.MaxUint16 {
		database = string(append([]byte(database[:math.MaxUint16-1]), byte('%')))
	}
	if len(branch) > math.MaxUint16 {
		branch = string(append([]byte(branch[:math.MaxUint16-1]), byte('%')))
	}
	if len(user) > math.MaxUint16 {
		user = string(append([]byte(user[:math.MaxUint16-1]), byte('%')))
	}
	if len(host) > math.MaxUint16 {
		host = string(append([]byte(host[:math.MaxUint16-1]), byte('%')))
	}
	// Add the expression strings to the binlog
	tbl.binlog.Insert(database, branch, user, host, uint64(perms))
	// Parse and insert the expressions
	databaseExpr := ParseExpression(database, sql.Collation_utf8mb4_0900_ai_ci)
	branchExpr := ParseExpression(branch, sql.Collation_utf8mb4_0900_ai_ci)
	userExpr := ParseExpression(user, sql.Collation_utf8mb4_0900_bin)
	hostExpr := ParseExpression(host, sql.Collation_utf8mb4_0900_ai_ci)
	nextIdx := uint32(len(tbl.Values))
	tbl.Databases = append(tbl.Databases, MatchExpression{CollectionIndex: nextIdx, SortOrders: databaseExpr})
	tbl.Branches = append(tbl.Branches, MatchExpression{CollectionIndex: nextIdx, SortOrders: branchExpr})
	tbl.Users = append(tbl.Users, MatchExpression{CollectionIndex: nextIdx, SortOrders: userExpr})
	tbl.Hosts = append(tbl.Hosts, MatchExpression{CollectionIndex: nextIdx, SortOrders: hostExpr})
	tbl.Values = append(tbl.Values, AccessValue{
		Database:    database,
		Branch:      branch,
		User:        user,
		Host:        host,
		Permissions: perms,
	})
}

// Serialize returns the offset for the AccessValue written to the given builder.
func (val *AccessValue) Serialize(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	database := b.CreateSharedString(val.Database)
	branch := b.CreateSharedString(val.Branch)
	user := b.CreateSharedString(val.User)
	host := b.CreateSharedString(val.Host)

	serial.BranchControlAccessValueStart(b)
	serial.BranchControlAccessValueAddDatabase(b, database)
	serial.BranchControlAccessValueAddBranch(b, branch)
	serial.BranchControlAccessValueAddUser(b, user)
	serial.BranchControlAccessValueAddHost(b, host)
	serial.BranchControlAccessValueAddPermissions(b, uint64(val.Permissions))
	return serial.BranchControlAccessValueEnd(b)
}
