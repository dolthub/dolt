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
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// Permissions are a set of flags that denote a user's allowed functionality on a branch.
type Permissions uint64

const (
	Permissions_Admin Permissions = 1 << iota // Permissions_Admin grants unrestricted control over a branch, including modification of table entries
	Permissions_Write                         // Permissions_Write allows for all modifying operations on a branch, but does not allow modification of table entries
)

const (
	currentAccessVersion = uint16(1)
)

// Access contains all of the expressions that comprise the "dolt_branch_control" table, which handles write Access to
// branches, along with write access to the branch control system tables.
type Access struct {
	binlog *Binlog

	Branches  []MatchExpression
	Users     []MatchExpression
	Hosts     []MatchExpression
	Values    []AccessValue
	SuperUser string
	SuperHost string
	RWMutex   *sync.RWMutex
}

// AccessValue contains the user-facing values of a particular row, along with the permissions for a row.
type AccessValue struct {
	Branch      string
	User        string
	Host        string
	Permissions Permissions
}

// newAccess returns a new Access.
func newAccess(superUser string, superHost string) *Access {
	return &Access{
		binlog:    NewAccessBinlog(nil),
		Branches:  nil,
		Users:     nil,
		Hosts:     nil,
		Values:    nil,
		SuperUser: superUser,
		SuperHost: superHost,
		RWMutex:   &sync.RWMutex{},
	}
}

// Match returns whether any entries match the given branch, user, and host, along with their permissions. Requires
// external synchronization handling, therefore manually manage the RWMutex.
func (tbl *Access) Match(branch string, user string, host string) (bool, Permissions) {
	if tbl.SuperUser == user && tbl.SuperHost == host {
		return true, Permissions_Admin
	}

	filteredIndexes := Match(tbl.Users, user, sql.Collation_utf8mb4_0900_bin)

	filteredHosts := tbl.filterHosts(filteredIndexes)
	indexPool.Put(filteredIndexes)
	filteredIndexes = Match(filteredHosts, host, sql.Collation_utf8mb4_0900_ai_ci)
	matchExprPool.Put(filteredHosts)

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
func (tbl *Access) GetIndex(branchExpr string, userExpr string, hostExpr string) int {
	for i, value := range tbl.Values {
		if value.Branch == branchExpr && value.User == userExpr && value.Host == hostExpr {
			return i
		}
	}
	return -1
}

// Serialize writes the table to the given buffer. All encoded integers are big-endian.
func (tbl *Access) Serialize(buffer *bytes.Buffer) {
	tbl.RWMutex.RLock()
	defer tbl.RWMutex.RUnlock()

	// Write the version bytes
	writeUint16(buffer, currentAccessVersion)
	// Write the number of entries
	numOfEntries := uint32(len(tbl.Values))
	writeUint32(buffer, numOfEntries)

	// Write the rows
	for _, matchExpr := range tbl.Branches {
		matchExpr.Serialize(buffer)
	}
	for _, matchExpr := range tbl.Users {
		matchExpr.Serialize(buffer)
	}
	for _, matchExpr := range tbl.Hosts {
		matchExpr.Serialize(buffer)
	}
	for _, val := range tbl.Values {
		val.Serialize(buffer)
	}
	// Write the binlog
	_ = tbl.binlog.Serialize(buffer)
}

// Deserialize populates the table with the given data. Returns an error if the data cannot be deserialized, or if the
// table has already been written to. Deserialize must be called on an empty table.
func (tbl *Access) Deserialize(data []byte, position *uint64) error {
	tbl.RWMutex.Lock()
	defer tbl.RWMutex.Unlock()

	if len(tbl.Values) != 0 {
		return fmt.Errorf("cannot deserialize to a non-empty access table")
	}
	// Read the version
	version := binary.BigEndian.Uint16(data[*position:])
	*position += 2
	if version != currentAccessVersion {
		// If we ever increment the access version, this will instead handle the conversion from previous versions
		return fmt.Errorf(`cannot deserialize an access table with version "%d"`, version)
	}
	// Read the number of entries
	numOfEntries := binary.BigEndian.Uint32(data[*position:])
	*position += 4
	// Read the rows
	tbl.Branches = deserializeMatchExpressions(numOfEntries, data, position)
	tbl.Users = deserializeMatchExpressions(numOfEntries, data, position)
	tbl.Hosts = deserializeMatchExpressions(numOfEntries, data, position)
	tbl.Values = make([]AccessValue, numOfEntries)
	for i := uint32(0); i < numOfEntries; i++ {
		tbl.Values[i] = deserializeAccessValue(data, position)
	}
	return tbl.binlog.Deserialize(data, position)
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

// Serialize writes the value to the given buffer. All encoded integers are big-endian.
func (val *AccessValue) Serialize(buffer *bytes.Buffer) {
	// Write the branch
	branchLen := uint16(len(val.Branch))
	writeUint16(buffer, branchLen)
	buffer.WriteString(val.Branch)
	// Write the user
	userLen := uint16(len(val.User))
	writeUint16(buffer, userLen)
	buffer.WriteString(val.User)
	// Write the host
	hostLen := uint16(len(val.Host))
	writeUint16(buffer, hostLen)
	buffer.WriteString(val.Host)
	// Write the permissions
	writeUint64(buffer, uint64(val.Permissions))
}

// deserializeAccessValue returns a AccessValue from the data at the given position. Assumes that the given data's
// encoded integers are big-endian.
func deserializeAccessValue(data []byte, position *uint64) AccessValue {
	val := AccessValue{}
	// Read the branch
	branchLen := uint64(binary.BigEndian.Uint16(data[*position:]))
	*position += 2
	val.Branch = string(data[*position : *position+branchLen])
	*position += branchLen
	// Read the user
	userLen := uint64(binary.BigEndian.Uint16(data[*position:]))
	*position += 2
	val.User = string(data[*position : *position+userLen])
	*position += userLen
	// Read the host
	hostLen := uint64(binary.BigEndian.Uint16(data[*position:]))
	*position += 2
	val.Host = string(data[*position : *position+hostLen])
	*position += hostLen
	// Read the permissions
	val.Permissions = Permissions(binary.BigEndian.Uint64(data[*position:]))
	*position += 8
	return val
}
