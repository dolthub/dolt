// Copyright 2025 Dolthub, Inc.
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
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

var _ sql.Table = (*StashesTable)(nil)
var _ sql.StatisticsTable = (*StashesTable)(nil)

type StashesTable struct {
	ddb       *doltdb.DoltDB
	tableName string
}

func NewStashesTable(_ *sql.Context, ddb *doltdb.DoltDB, tableName string) sql.Table {
	return &StashesTable{ddb, tableName}
}

func (st *StashesTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(st.Schema())
	numRows, _, err := st.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (st *StashesTable) RowCount(ctx *sql.Context) (uint64, bool, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 0, false, fmt.Errorf("Empty database name.")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return 0, false, sql.ErrDatabaseNotFound.New(dbName)
	}

	stashes, err := dbData.Ddb.GetStashes(ctx)
	if err != nil {
		return 0, false, err
	}
	return uint64(len(stashes)), true, nil
}

// Name is a sql.Table interface function which returns the name of the table
func (st *StashesTable) Name() string {
	return st.tableName
}

// String is a sql.Table interface function which returns the name of the table
func (st *StashesTable) String() string {
	return st.tableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the remotes system table
func (st *StashesTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "name", Type: types.Text, Source: st.tableName, PrimaryKey: false, Nullable: false},
		{Name: "stash_id", Type: types.Text, Source: st.tableName, PrimaryKey: false, Nullable: false},
		{Name: "branch", Type: types.Text, Source: st.tableName, PrimaryKey: false, Nullable: false},
		{Name: "hash", Type: types.Text, Source: st.tableName, PrimaryKey: false, Nullable: false},
		{Name: "commit_message", Type: types.Text, Source: st.tableName, PrimaryKey: false, Nullable: true},
	}
}

// Collation implements the sql.Table interface.
func (st *StashesTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (st *StashesTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (st *StashesTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return NewStashItr(ctx, st.ddb)
}

type StashItr struct {
	stashes []*doltdb.Stash
	idx     int
}

// NewStashItr creates a StashItr from the current environment.
func NewStashItr(ctx *sql.Context, _ *doltdb.DoltDB) (*StashItr, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return nil, fmt.Errorf("Empty database name.")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	stashes, err := dbData.Ddb.GetStashes(ctx)
	if err != nil {
		return nil, err
	}

	return &StashItr{stashes, 0}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *StashItr) Next(*sql.Context) (sql.Row, error) {
	if itr.idx >= len(itr.stashes) {
		return nil, io.EOF
	}

	defer func() {
		itr.idx++
	}()

	stash := itr.stashes[itr.idx]
	commitHash, err := stash.HeadCommit.HashOf()
	if err != nil {
		return nil, err
	}

	// BranchName and StashReference are of the form refs/heads/name
	// or refs/stashes/name, so we need to parse them to get names
	branch := ref.NewBranchRef(stash.BranchName).GetPath()
	stashRef := ref.NewStashRef(stash.StashReference).GetPath()

	return sql.NewRow(stashRef, stash.Name, branch, commitHash.String(), stash.Description), nil
}

// Close closes the iterator.
func (itr *StashItr) Close(*sql.Context) error {
	return nil
}

var _ sql.RowReplacer = stashWriter{nil}
var _ sql.RowUpdater = stashWriter{nil}
var _ sql.RowInserter = stashWriter{nil}
var _ sql.RowDeleter = stashWriter{nil}

type stashWriter struct {
	rt *StashesTable
}

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (bWr stashWriter) Insert(_ *sql.Context, _ sql.Row) error {
	return fmt.Errorf("the dolt_stashes table is read-only; use the dolt_stash stored procedure to edit stashes")
}

// Update the given row. Provides both the old and new rows.
func (bWr stashWriter) Update(_ *sql.Context, _ sql.Row, _ sql.Row) error {
	return fmt.Errorf("the dolt_stash table is read-only; use the dolt_stash stored procedure to edit stashes")
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (bWr stashWriter) Delete(_ *sql.Context, _ sql.Row) error {
	return fmt.Errorf("the dolt_stash table is read-only; use the dolt_stash stored procedure to edit stashes")
}

// StatementBegin implements the interface sql.TableEditor. Currently a no-op.
func (bWr stashWriter) StatementBegin(*sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor. Currently a no-op.
func (bWr stashWriter) DiscardChanges(_ *sql.Context, _ error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor. Currently a no-op.
func (bWr stashWriter) StatementComplete(*sql.Context) error {
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (bWr stashWriter) Close(*sql.Context) error {
	return nil
}
