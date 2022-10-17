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

const (
	currentNamespaceVersion = uint16(1)
)

// Namespace contains all of the expressions that comprise the "dolt_branch_namespace_control" table, which controls
// which users may use which branch names when creating branches. Modification of this table is handled by the Access
// table.
type Namespace struct {
	access *Access
	binlog *Binlog

	Branches  []MatchExpression
	Users     []MatchExpression
	Hosts     []MatchExpression
	Values    []NamespaceValue
	SuperUser string
	SuperHost string
	RWMutex   *sync.RWMutex
}

// NamespaceValue contains the user-facing values of a particular row.
type NamespaceValue struct {
	Branch string
	User   string
	Host   string
}

// newNamespace returns a new Namespace.
func newNamespace(accessTbl *Access, superUser string, superHost string) *Namespace {
	return &Namespace{
		binlog:    NewNamespaceBinlog(nil),
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
func (tbl *Namespace) CanCreate(branch string, user string, host string) bool {
	// Super user can always create branches
	if user == tbl.SuperUser && host == tbl.SuperHost {
		return true
	}
	matchedSet := Match(tbl.Branches, branch, sql.Collation_utf8mb4_0900_ai_ci)
	// If there are no branch entries, then the Namespace is unrestricted
	if len(matchedSet) == 0 {
		indexPool.Put(matchedSet)
		return true
	}

	// We take either the longest match, or the set of longest matches if multiple matches have the same length
	longest := -1
	filteredIndexes := indexPool.Get().([]uint32)[:0]
	for _, matched := range matchedSet {
		matchedValue := tbl.Values[matched]
		// If we've found a longer match, then we reset the slice. We append to it in the following if statement.
		if len(matchedValue.Branch) > longest {
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

// GetIndex returns the index of the given branch, user, and host expressions. If the expressions cannot be found,
// returns -1. Assumes that the given expressions have already been folded.
func (tbl *Namespace) GetIndex(branchExpr string, userExpr string, hostExpr string) int {
	for i, value := range tbl.Values {
		if value.Branch == branchExpr && value.User == userExpr && value.Host == hostExpr {
			return i
		}
	}
	return -1
}

// Access returns the Access table.
func (tbl *Namespace) Access() *Access {
	return tbl.access
}

// Serialize writes the table to the given buffer. All encoded integers are big-endian.
func (tbl *Namespace) Serialize(buffer *bytes.Buffer) {
	tbl.RWMutex.RLock()
	defer tbl.RWMutex.RUnlock()

	// Write the version bytes
	writeUint16(buffer, currentNamespaceVersion)
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
func (tbl *Namespace) Deserialize(data []byte, position *uint64) error {
	tbl.RWMutex.Lock()
	defer tbl.RWMutex.Unlock()

	if len(tbl.Values) != 0 {
		return fmt.Errorf("cannot deserialize to a non-empty namespace table")
	}
	// Read the version
	version := binary.BigEndian.Uint16(data[*position:])
	*position += 2
	if version != currentNamespaceVersion {
		// If we ever increment the namespace version, this will instead handle the conversion from previous versions
		return fmt.Errorf(`cannot deserialize an namespace table with version "%d"`, version)
	}
	// Read the number of entries
	numOfEntries := binary.BigEndian.Uint32(data[*position:])
	*position += 4
	// Read the rows
	tbl.Branches = deserializeMatchExpressions(numOfEntries, data, position)
	tbl.Users = deserializeMatchExpressions(numOfEntries, data, position)
	tbl.Hosts = deserializeMatchExpressions(numOfEntries, data, position)
	tbl.Values = make([]NamespaceValue, numOfEntries)
	for i := uint32(0); i < numOfEntries; i++ {
		tbl.Values[i] = deserializeNamespaceValue(data, position)
	}
	return tbl.binlog.Deserialize(data, position)
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

// Serialize writes the value to the given buffer. All encoded integers are big-endian.
func (val *NamespaceValue) Serialize(buffer *bytes.Buffer) {
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
}

// deserializeNamespaceValue returns a NamespaceValue from the data at the given position. Also returns the new
// position. Assumes that the given data's encoded integers are big-endian.
func deserializeNamespaceValue(data []byte, position *uint64) NamespaceValue {
	val := NamespaceValue{}
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
	return val
}
