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

	flatbuffers "github.com/dolthub/flatbuffers/v23/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
)

// Permissions are a set of flags that denote a user's allowed functionality on a branch.
type Permissions uint64

const (
	Permissions_Admin Permissions = 1 << iota // Permissions_Admin grants unrestricted control over a branch, including modification of table entries
	Permissions_Write                         // Permissions_Write allows for all modifying operations on a branch, but does not allow modification of table entries
	Permissions_Read                          // Permissions_Read allows for reading from a branch, which is equivalent to having no permissions

	Permissions_None Permissions = 0 // Permissions_None represents a lack of permissions, which defaults to allowing reading
)

// Access contains all of the expressions that comprise the "dolt_branch_control" table, which handles write Access to
// branches, along with write access to the branch control system tables.
type Access struct {
	Root     *MatchNode
	RWMutex  *sync.RWMutex
	binlog   *Binlog
	rows     []AccessRow
	freeRows []uint32
}

// AccessRow contains the user-facing values of a particular row, along with the permissions for a row.
type AccessRow struct {
	Database    string
	Branch      string
	User        string
	Host        string
	Permissions Permissions
}

// AccessRowIter is an iterator over all valid rows.
type AccessRowIter struct {
	access *Access
	idx    uint32
}

// newAccess returns a new Access.
func newAccess() *Access {
	return &Access{
		RWMutex: &sync.RWMutex{},
	}
}

// Match returns whether any entries match the given database, branch, user, and host, along with their permissions.
// Requires external synchronization handling, therefore manually manage the RWMutex.
func (tbl *Access) Match(database string, branch string, user string, host string) (bool, Permissions) {
	results := tbl.Root.Match(database, branch, user, host)
	// We use the result(s) with the longest length
	length := uint32(0)
	perms := Permissions_None
	for _, result := range results {
		if result.Length > length {
			perms = result.Permissions
			length = result.Length
		} else if result.Length == length {
			perms |= result.Permissions
		}
	}
	return len(results) > 0, perms
}

// GetBinlog returns the table's binlog.
func (tbl *Access) GetBinlog() *Binlog {
	return tbl.binlog
}

// Serialize returns the offset for the Access table written to the given builder.
func (tbl *Access) Serialize(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	// Serialize the binlog
	binlog := tbl.binlog.Serialize(b)
	serial.BranchControlAccessStart(b)
	serial.BranchControlAccessAddBinlog(b, binlog)
	return serial.BranchControlAccessEnd(b)
}

func (tbl *Access) reinit() {
	tbl.Root = &MatchNode{
		SortOrders: []int32{columnMarker},
		Children:   make(map[int32]*MatchNode),
		Data:       nil,
	}
	tbl.binlog = NewAccessBinlog(nil)
	tbl.rows = nil
	tbl.freeRows = nil
}

// Deserialize populates the table with the data from the flatbuffers representation.
func (tbl *Access) Deserialize(fb *serial.BranchControlAccess) error {
	// Read the binlog
	fbBinlog, err := fb.TryBinlog(nil)
	if err != nil {
		return err
	}
	binlog := NewAccessBinlog(nil)
	if err = binlog.Deserialize(fbBinlog); err != nil {
		return err
	}

	tbl.reinit()

	// Recreate the table from the binlog
	for _, binlogRow := range binlog.rows {
		if binlogRow.IsInsert {
			tbl.Insert(binlogRow.Database, binlogRow.Branch, binlogRow.User, binlogRow.Host, Permissions(binlogRow.Permissions))
		} else {
			tbl.Delete(binlogRow.Database, binlogRow.Branch, binlogRow.User, binlogRow.Host)
		}
	}
	return nil
}

// insertDefaultRow adds a row that allows all users to access and modify all branches, but does not allow them to
// modify any branch control tables. This was the default behavior of Dolt before the introduction of branch permissions.
func (tbl *Access) insertDefaultRow() {
	tbl.reinit()
	tbl.Insert("%", "%", "%", "%", Permissions_Write)
}

// Insert adds the given expressions to the table. This does not perform any sort of validation whatsoever, so it is
// important to ensure that the expressions are valid before insertion. Folds all strings that are given. Overwrites any
// existing entries with the new permissions. Requires external synchronization handling, therefore manually manage the
// RWMutex.
func (tbl *Access) Insert(database string, branch string, user string, host string, perms Permissions) {
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
	// Add the insertion entry to the binlog
	tbl.binlog.Insert(database, branch, user, host, uint64(perms))
	// Add to the rows and grab the insertion index
	var index uint32
	if len(tbl.freeRows) > 0 {
		index = tbl.freeRows[len(tbl.freeRows)-1]
		tbl.freeRows = tbl.freeRows[:len(tbl.freeRows)-1]
		tbl.rows[index] = AccessRow{
			Database:    database,
			Branch:      branch,
			User:        user,
			Host:        host,
			Permissions: perms,
		}
	} else {
		if len(tbl.rows) >= math.MaxUint32 {
			// If someone has this many branches in Dolt then they're doing something very interesting, we'll probably
			// fail elsewhere way before this point
			panic(fmt.Errorf("branch control has a maximum limit of %d branches", math.MaxUint32-1))
		}
		index = uint32(len(tbl.rows))
		tbl.rows = append(tbl.rows, AccessRow{
			Database:    database,
			Branch:      branch,
			User:        user,
			Host:        host,
			Permissions: perms,
		})
	}
	// Add the entry to the root node
	tbl.Root.Add(database, branch, user, host, MatchNodeData{
		Permissions: perms,
		RowIndex:    index,
	})
}

// Delete removes the given expressions from the table. This does not perform any sort of validation whatsoever, so it
// is important to ensure that the expressions are valid before deletion. Folds all strings that are given. Requires
// external synchronization handling, therefore manually manage the RWMutex.
func (tbl *Access) Delete(database string, branch string, user string, host string) {
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
	// Add the deletion entry to the binlog
	tbl.binlog.Delete(database, branch, user, host, uint64(Permissions_None))
	// Remove the entry from the root node
	removedIndex := tbl.Root.Remove(database, branch, user, host)
	// Remove from the rows
	if removedIndex != math.MaxUint32 {
		tbl.freeRows = append(tbl.freeRows, removedIndex)
	}
}

// Iter returns an iterator that goes over all valid rows. The iterator does not acquire a read lock, therefore this
// requires external synchronization handling via RWMutex.
func (tbl *Access) Iter() *AccessRowIter {
	return &AccessRowIter{
		access: tbl,
		idx:    0,
	}
}

// Next returns the next valid row. Returns false if there are no more rows.
func (iter *AccessRowIter) Next() (AccessRow, bool) {
OuterLoop:
	for iter.idx < uint32(len(iter.access.rows)) {
		idx := iter.idx
		iter.idx++
		// Not the most efficient, but I expect this to be empty 99% of the time so it should be fine
		for _, freeRow := range iter.access.freeRows {
			if idx == freeRow {
				continue OuterLoop
			}
		}
		return iter.access.rows[idx], true
	}
	return AccessRow{}, false
}

// Consolidate reduces the permission set down to the most representative permission. For example, having both admin and
// write permissions are equivalent to only having the admin permission. Additionally, having no permissions is
// equivalent to only having the read permission.
func (perm Permissions) Consolidate() Permissions {
	if perm&Permissions_Admin == Permissions_Admin {
		return Permissions_Admin
	} else if perm&Permissions_Write == Permissions_Write {
		return Permissions_Write
	} else {
		return Permissions_Read
	}
}
