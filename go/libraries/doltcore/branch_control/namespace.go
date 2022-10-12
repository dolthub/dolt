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
