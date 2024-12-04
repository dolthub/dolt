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
	"sync"

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/gen/fb/serial"
)

// Namespace contains all of the expressions that comprise the "dolt_branch_namespace_control" table, which controls
// which users may use which branch names when creating branches. Modification of this table is handled by the Access
// table.
type Namespace struct {
	access *Access
	binlog *Binlog

	Databases []MatchExpression
	Branches  []MatchExpression
	Users     []MatchExpression
	Hosts     []MatchExpression
	Values    []NamespaceValue
	RWMutex   *sync.RWMutex
}

// NamespaceValue contains the user-facing values of a particular row.
type NamespaceValue struct {
	Database string
	Branch   string
	User     string
	Host     string
}

// newNamespace returns a new Namespace.
func newNamespace(accessTbl *Access) *Namespace {
	return &Namespace{
		binlog:    NewNamespaceBinlog(nil),
		access:    accessTbl,
		Databases: nil,
		Branches:  nil,
		Users:     nil,
		Hosts:     nil,
		Values:    nil,
		RWMutex:   accessTbl.RWMutex,
	}
}

// CanCreate checks the given database and branch, and returns whether the given user and host combination is able to
// create that branch. Handles the super user case.
func (tbl *Namespace) CanCreate(database string, branch string, user string, host string) bool {
	filteredIndexes := Match(tbl.Databases, database, sql.Collation_utf8mb4_0900_ai_ci)
	// If there are no database entries, then the Namespace is unrestricted
	if len(filteredIndexes) == 0 {
		indexPool.Put(filteredIndexes)
		return true
	}

	filteredBranches := tbl.filterBranches(filteredIndexes)
	indexPool.Put(filteredIndexes)
	matchedSet := Match(filteredBranches, branch, sql.Collation_utf8mb4_0900_ai_ci)
	matchExprPool.Put(filteredBranches)
	// If there are no branch entries, then the Namespace is unrestricted
	if len(matchedSet) == 0 {
		indexPool.Put(matchedSet)
		return true
	}

	// We take either the longest match, or the set of longest matches if multiple matches have the same length
	longest := -1
	filteredIndexes = indexPool.Get().([]uint32)[:0]
	for _, matched := range matchedSet {
		matchedValue := tbl.Values[matched]
		// If we've found a longer match, then we reset the slice. We append to it in the following if statement.
		if len(matchedValue.Branch) > longest {
			longest = len(matchedValue.Branch)
			filteredIndexes = filteredIndexes[:0]
		}
		if len(matchedValue.Branch) >= longest {
			filteredIndexes = append(filteredIndexes, matched)
		}
	}
	indexPool.Put(matchedSet)

	filteredUsers := tbl.filterUsers(filteredIndexes)
	indexPool.Put(filteredIndexes)
	filteredIndexes = Match(filteredUsers, user, sql.Collation_utf8mb4_0900_bin)
	matchExprPool.Put(filteredUsers)

	filteredHosts := tbl.filterHosts(filteredIndexes)
	indexPool.Put(filteredIndexes)
	filteredIndexes = Match(filteredHosts, host, sql.Collation_utf8mb4_0900_ai_ci)
	matchExprPool.Put(filteredHosts)

	result := len(filteredIndexes) > 0
	indexPool.Put(filteredIndexes)
	return result
}

// GetIndex returns the index of the given database, branch, user, and host expressions. If the expressions cannot be
// found, returns -1. Assumes that the given expressions have already been folded.
func (tbl *Namespace) GetIndex(databaseExpr string, branchExpr string, userExpr string, hostExpr string) int {
	for i, value := range tbl.Values {
		if value.Database == databaseExpr && value.Branch == branchExpr && value.User == userExpr && value.Host == hostExpr {
			return i
		}
	}
	return -1
}

// GetBinlog returns the table's binlog.
func (tbl *Namespace) GetBinlog() *Binlog {
	return tbl.binlog
}

// Access returns the Access table.
func (tbl *Namespace) Access() *Access {
	return tbl.access
}

// Serialize returns the offset for the Namespace table written to the given builder.
func (tbl *Namespace) Serialize(b *flatbuffers.Builder) flatbuffers.UOffsetT {
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
	serial.BranchControlNamespaceStartDatabasesVector(b, len(databaseOffsets))
	for i := len(databaseOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(databaseOffsets[i])
	}
	databases := b.EndVector(len(databaseOffsets))
	serial.BranchControlNamespaceStartBranchesVector(b, len(branchOffsets))
	for i := len(branchOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(branchOffsets[i])
	}
	branches := b.EndVector(len(branchOffsets))
	serial.BranchControlNamespaceStartUsersVector(b, len(userOffsets))
	for i := len(userOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(userOffsets[i])
	}
	users := b.EndVector(len(userOffsets))
	serial.BranchControlNamespaceStartHostsVector(b, len(hostOffsets))
	for i := len(hostOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(hostOffsets[i])
	}
	hosts := b.EndVector(len(hostOffsets))
	serial.BranchControlNamespaceStartValuesVector(b, len(valueOffsets))
	for i := len(valueOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(valueOffsets[i])
	}
	values := b.EndVector(len(valueOffsets))
	// Write the table
	serial.BranchControlNamespaceStart(b)
	serial.BranchControlNamespaceAddBinlog(b, binlog)
	serial.BranchControlNamespaceAddDatabases(b, databases)
	serial.BranchControlNamespaceAddBranches(b, branches)
	serial.BranchControlNamespaceAddUsers(b, users)
	serial.BranchControlNamespaceAddHosts(b, hosts)
	serial.BranchControlNamespaceAddValues(b, values)
	return serial.BranchControlNamespaceEnd(b)
}

func (tbl *Namespace) reinit() {
	tbl.binlog = NewNamespaceBinlog(nil)
	tbl.Databases = nil
	tbl.Branches = nil
	tbl.Users = nil
	tbl.Hosts = nil
	tbl.Values = nil
}

// Deserialize populates the table with the data from the flatbuffers representation.
func (tbl *Namespace) Deserialize(fb *serial.BranchControlNamespace) error {
	// Verify that all fields have the same length
	if fb.DatabasesLength() != fb.BranchesLength() ||
		fb.BranchesLength() != fb.UsersLength() ||
		fb.UsersLength() != fb.HostsLength() ||
		fb.HostsLength() != fb.ValuesLength() {
		return fmt.Errorf("cannot deserialize a namespace table with differing field lengths")
	}
	// Read the binlog
	binlog, err := fb.TryBinlog(nil)
	if err != nil {
		return err
	}
	if err = tbl.binlog.Deserialize(binlog); err != nil {
		return err
	}

	tbl.reinit()

	// Initialize every slice
	tbl.Databases = make([]MatchExpression, fb.DatabasesLength())
	tbl.Branches = make([]MatchExpression, fb.BranchesLength())
	tbl.Users = make([]MatchExpression, fb.UsersLength())
	tbl.Hosts = make([]MatchExpression, fb.HostsLength())
	tbl.Values = make([]NamespaceValue, fb.ValuesLength())
	// Read the databases
	for i := 0; i < fb.DatabasesLength(); i++ {
		serialMatchExpr := &serial.BranchControlMatchExpression{}
		_, err = fb.TryDatabases(serialMatchExpr, i)
		if err != nil {
			return err
		}
		tbl.Databases[i] = deserializeMatchExpression(serialMatchExpr)
	}
	// Read the branches
	for i := 0; i < fb.BranchesLength(); i++ {
		serialMatchExpr := &serial.BranchControlMatchExpression{}
		_, err = fb.TryBranches(serialMatchExpr, i)
		if err != nil {
			return err
		}
		tbl.Branches[i] = deserializeMatchExpression(serialMatchExpr)
	}
	// Read the users
	for i := 0; i < fb.UsersLength(); i++ {
		serialMatchExpr := &serial.BranchControlMatchExpression{}
		_, err = fb.TryUsers(serialMatchExpr, i)
		if err != nil {
			return err
		}
		tbl.Users[i] = deserializeMatchExpression(serialMatchExpr)
	}
	// Read the hosts
	for i := 0; i < fb.HostsLength(); i++ {
		serialMatchExpr := &serial.BranchControlMatchExpression{}
		_, err = fb.TryHosts(serialMatchExpr, i)
		if err != nil {
			return err
		}
		tbl.Hosts[i] = deserializeMatchExpression(serialMatchExpr)
	}
	// Read the values
	for i := 0; i < fb.ValuesLength(); i++ {
		serialNamespaceValue := &serial.BranchControlNamespaceValue{}
		_, err = fb.TryValues(serialNamespaceValue, i)
		if err != nil {
			return err
		}
		tbl.Values[i] = NamespaceValue{
			Database: string(serialNamespaceValue.Database()),
			Branch:   string(serialNamespaceValue.Branch()),
			User:     string(serialNamespaceValue.User()),
			Host:     string(serialNamespaceValue.Host()),
		}
	}
	return nil
}

// filterDatabases returns all databases that match the given collection indexes.
func (tbl *Namespace) filterDatabases(filters []uint32) []MatchExpression {
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
func (tbl *Namespace) filterBranches(filters []uint32) []MatchExpression {
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
func (tbl *Namespace) filterUsers(filters []uint32) []MatchExpression {
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
func (tbl *Namespace) filterHosts(filters []uint32) []MatchExpression {
	if len(filters) == 0 {
		return nil
	}
	matchExprs := matchExprPool.Get().([]MatchExpression)[:0]
	for _, filter := range filters {
		matchExprs = append(matchExprs, tbl.Hosts[filter])
	}
	return matchExprs
}

// Serialize returns the offset for the NamespaceValue written to the given builder.
func (val *NamespaceValue) Serialize(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	database := b.CreateSharedString(val.Database)
	branch := b.CreateSharedString(val.Branch)
	user := b.CreateSharedString(val.User)
	host := b.CreateSharedString(val.Host)

	serial.BranchControlNamespaceValueStart(b)
	serial.BranchControlNamespaceValueAddDatabase(b, database)
	serial.BranchControlNamespaceValueAddBranch(b, branch)
	serial.BranchControlNamespaceValueAddUser(b, user)
	serial.BranchControlNamespaceValueAddHost(b, host)
	return serial.BranchControlNamespaceValueEnd(b)
}
