// Copyright 2021 Dolthub, Inc.
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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

const remotesDefaultRowCount = 1

var _ sql.Table = (*RemotesTable)(nil)
var _ sql.UpdatableTable = (*RemotesTable)(nil)
var _ sql.DeletableTable = (*RemotesTable)(nil)
var _ sql.InsertableTable = (*RemotesTable)(nil)
var _ sql.ReplaceableTable = (*RemotesTable)(nil)
var _ sql.StatisticsTable = (*RemotesTable)(nil)

// RemotesTable is a sql.Table implementation that implements a system table which shows the dolt remotes
type RemotesTable struct {
	ddb       *doltdb.DoltDB
	tableName string
}

// NewRemotesTable creates a RemotesTable
func NewRemotesTable(_ *sql.Context, ddb *doltdb.DoltDB, tableName string) sql.Table {
	return &RemotesTable{ddb, tableName}
}

func (rt *RemotesTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(rt.Schema())
	numRows, _, err := rt.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (rt *RemotesTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return remotesDefaultRowCount, false, nil
}

// Name is a sql.Table interface function which returns the name of the table
func (rt *RemotesTable) Name() string {
	return rt.tableName
}

// String is a sql.Table interface function which returns the name of the table
func (rt *RemotesTable) String() string {
	return rt.tableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the remotes system table
func (rt *RemotesTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "name", Type: types.Text, Source: rt.tableName, PrimaryKey: true, Nullable: false},
		{Name: "url", Type: types.Text, Source: rt.tableName, PrimaryKey: false, Nullable: false},
		{Name: "fetch_specs", Type: types.JSON, Source: rt.tableName, PrimaryKey: false, Nullable: true},
		{Name: "params", Type: types.JSON, Source: rt.tableName, PrimaryKey: false, Nullable: true},
	}
}

// Collation implements the sql.Table interface.
func (rt *RemotesTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (rt *RemotesTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (rt *RemotesTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return NewRemoteItr(ctx, rt.ddb)
}

// RemoteItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type RemoteItr struct {
	remotes []env.Remote
	idx     int
}

// NewRemoteItr creates a RemoteItr from the current environment.
func NewRemoteItr(ctx *sql.Context, ddb *doltdb.DoltDB) (*RemoteItr, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return nil, fmt.Errorf("Empty database name.")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := sess.GetDbData(ctx, dbName)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	remoteMap, err := dbData.Rsr.GetRemotes()
	if err != nil {
		return nil, err
	}
	remotes := []env.Remote{}

	remoteMap.Iter(func(key string, val env.Remote) bool {
		remotes = append(remotes, val)
		return true
	})

	return &RemoteItr{remotes, 0}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *RemoteItr) Next(*sql.Context) (sql.Row, error) {
	if itr.idx >= len(itr.remotes) {
		return nil, io.EOF
	}

	defer func() {
		itr.idx++
	}()

	remote := itr.remotes[itr.idx]

	fs, _, err := types.JSON.Convert(remote.FetchSpecs)
	if err != nil {
		return nil, err
	}
	params, _, err := types.JSON.Convert(remote.Params)
	if err != nil {
		return nil, err
	}

	return sql.NewRow(remote.Name, remote.Url, fs, params), nil
}

// Close closes the iterator.
func (itr *RemoteItr) Close(*sql.Context) error {
	return nil
}

// Replacer returns a RowReplacer for this table. The RowReplacer will have Insert and optionally Delete called once
// for each row, followed by a call to Close() when all rows have been processed.
func (rt *RemotesTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return remoteWriter{rt}
}

// Updater returns a RowUpdater for this table. The RowUpdater will have Update called once for each row to be
// updated, followed by a call to Close() when all rows have been processed.
func (rt *RemotesTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return remoteWriter{rt}
}

// Inserter returns an Inserter for this table. The Inserter will get one call to Insert() for each row to be
// inserted, and will end with a call to Close() to finalize the insert operation.
func (rt *RemotesTable) Inserter(*sql.Context) sql.RowInserter {
	return remoteWriter{rt}
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
// and will end with a call to Close() to finalize the delete operation.
func (rt *RemotesTable) Deleter(*sql.Context) sql.RowDeleter {
	return remoteWriter{rt}
}

var _ sql.RowReplacer = remoteWriter{nil}
var _ sql.RowUpdater = remoteWriter{nil}
var _ sql.RowInserter = remoteWriter{nil}
var _ sql.RowDeleter = remoteWriter{nil}

type remoteWriter struct {
	rt *RemotesTable
}

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (bWr remoteWriter) Insert(ctx *sql.Context, r sql.Row) error {
	return fmt.Errorf("the dolt_remotes table is read-only; use the dolt_remote stored procedure to edit remotes")
}

// Update the given row. Provides both the old and new rows.
func (bWr remoteWriter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	return fmt.Errorf("the dolt_remotes table is read-only; use the dolt_remote stored procedure to edit remotes")
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (bWr remoteWriter) Delete(ctx *sql.Context, r sql.Row) error {
	return fmt.Errorf("the dolt_remotes table is read-only; use the dolt_remote stored procedure to edit remotes")
}

// StatementBegin implements the interface sql.TableEditor. Currently a no-op.
func (bWr remoteWriter) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor. Currently a no-op.
func (bWr remoteWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor. Currently a no-op.
func (bWr remoteWriter) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (bWr remoteWriter) Close(*sql.Context) error {
	return nil
}
