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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
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
