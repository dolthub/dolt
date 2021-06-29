// Copyright 2020 Dolthub, Inc.
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
	"errors"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

var _ sql.Table = (*RemotesTable)(nil)
var _ sql.UpdatableTable = (*RemotesTable)(nil)
var _ sql.DeletableTable = (*RemotesTable)(nil)
var _ sql.InsertableTable = (*RemotesTable)(nil)
var _ sql.ReplaceableTable = (*RemotesTable)(nil)

// RemotesTable is a sql.Table implementation that implements a system table which shows the dolt remotes
type RemotesTable struct {
	ddb *doltdb.DoltDB
}

// NewRemotesTable creates a RemotesTable
func NewRemotesTable(_ *sql.Context, ddb *doltdb.DoltDB) sql.Table {
	return &RemotesTable{ddb}
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// RemotesTableName
func (bt *RemotesTable) Name() string {
	return doltdb.RemotesTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// RemotesTableName
func (bt *RemotesTable) String() string {
	return doltdb.RemotesTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the remotes system table
func (bt *RemotesTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "name", Type: sql.Text, Source: doltdb.RemotesTableName, PrimaryKey: true, Nullable: false},
		{Name: "url", Type: sql.Text, Source: doltdb.RemotesTableName, PrimaryKey: false, Nullable: false},
		{Name: "fetch_specs", Type: sql.JSON, Source: doltdb.RemotesTableName, PrimaryKey: false, Nullable: true},
		{Name: "params", Type: sql.JSON, Source: doltdb.RemotesTableName, PrimaryKey: false, Nullable: true},
	}
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (bt *RemotesTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return sqlutil.NewSinglePartitionIter(types.Map{}), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (bt *RemotesTable) PartitionRows(sqlCtx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return NewRemoteItr(sqlCtx, bt.ddb)
}

// RemoteItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type RemoteItr struct {
	remotes []env.Remote
	idx     int
}

// NewRemoteItr creates a RemoteItr from the current environment.
func NewRemoteItr(sqlCtx *sql.Context, ddb *doltdb.DoltDB) (*RemoteItr, error) {
	// TODO : dEnv.FS

	repoState, err := env.LoadRepoState(filesys.LocalFS)
	if err != nil {
		return nil, err
	}

	idx := 0
	remotes := make([]env.Remote, len(repoState.Remotes))
	for _, remote := range repoState.Remotes {
		remotes[idx] = remote
		idx++
	}

	return &RemoteItr{remotes, 0}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *RemoteItr) Next() (sql.Row, error) {
	if itr.idx >= len(itr.remotes) {
		return nil, io.EOF
	}

	defer func() {
		itr.idx++
	}()

	remote := itr.remotes[itr.idx]

	return sql.NewRow(remote.Name, remote.Url, remote.FetchSpecs, remote.Params), nil
}

// Close closes the iterator.
func (itr *RemoteItr) Close(*sql.Context) error {
	return nil
}

// Replacer returns a RowReplacer for this table. The RowReplacer will have Insert and optionally Delete called once
// for each row, followed by a call to Close() when all rows have been processed.
func (bt *RemotesTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return remoteWriter{bt}
}

// Updater returns a RowUpdater for this table. The RowUpdater will have Update called once for each row to be
// updated, followed by a call to Close() when all rows have been processed.
func (bt *RemotesTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return remoteWriter{bt}
}

// Inserter returns an Inserter for this table. The Inserter will get one call to Insert() for each row to be
// inserted, and will end with a call to Close() to finalize the insert operation.
func (bt *RemotesTable) Inserter(*sql.Context) sql.RowInserter {
	return remoteWriter{bt}
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
// and will end with a call to Close() to finalize the delete operation.
func (bt *RemotesTable) Deleter(*sql.Context) sql.RowDeleter {
	return remoteWriter{bt}
}

var _ sql.RowReplacer = remoteWriter{nil}
var _ sql.RowUpdater = remoteWriter{nil}
var _ sql.RowInserter = remoteWriter{nil}
var _ sql.RowDeleter = remoteWriter{nil}

type remoteWriter struct {
	bt *RemotesTable
}

func validateRow(ctx *sql.Context, r sql.Row) (*env.Remote, error) {
	name, ok := r[0].(string)

	if !ok {
		return nil, errors.New("invalid type for name")
	}

	url, ok := r[1].(string)

	if !ok {
		return nil, errors.New("invalid value type for url")
	}

	fetchSpecsDoc, ok := r[2].(sql.JSONValue)

	if !ok {
		return nil, errors.New("invalid value type for fetch_specs")
	}

	fetchSpecsInterface, err := fetchSpecsDoc.Unmarshall(ctx)
	if err != nil {
		return nil, err
	}

	fetchSpecs, ok := fetchSpecsInterface.Val.([]string)

	if !ok {
		return nil, errors.New("invalid value type for params json")
	}

	for _, fetchSpec := range fetchSpecs {
		_, err := ref.ParseRefSpecForRemote(name, fetchSpec)
		if err != nil {
			return nil, err
		}
	}

	paramsDoc, ok := r[3].(sql.JSONValue)

	if !ok {
		return nil, errors.New("invalid value type for params")
	}

	paramsInterface, err := paramsDoc.Unmarshall(ctx)
	if err != nil {
		return nil, err
	}

	params, ok := paramsInterface.Val.(map[string]string)

	if !ok {
		return nil, errors.New("invalid value type for params json")
	}

	//remote := env.NewRemote(name, url, params)
	remote := env.Remote{Name: name, Url: url, FetchSpecs: fetchSpecs, Params: params}
	return &remote, nil

}

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (bWr remoteWriter) Insert(ctx *sql.Context, r sql.Row) error {
	remote, err := validateRow(ctx, r)

	if err != nil {
		return err
	}

	repoState, err := env.LoadRepoState(filesys.LocalFS)
	if err != nil {
		return err
	}

	repoState.AddRemote(*remote)

	return nil
}

// Update the given row. Provides both the old and new rows.
func (bWr remoteWriter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	return bWr.Insert(ctx, new)
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (bWr remoteWriter) Delete(ctx *sql.Context, r sql.Row) error {
	remote, err := validateRow(ctx, r)
	if err != nil {
		return err
	}

	// TODO : dEnv.FS
	repoState, err := env.LoadRepoState(filesys.LocalFS)
	if err != nil {
		return err
	}

	if _, ok := repoState.Remotes[remote.Name]; !ok {
		return errhand.BuildDError("error: unknown remote " + remote.Name).Build()
	}

	ddb := bWr.bt.ddb
	refs, err := ddb.GetRemotes(ctx)

	if err != nil {
		return errhand.BuildDError("error: failed to read from db").AddCause(err).Build()
	}

	for _, r := range refs {
		rr := r.(ref.RemoteRef)

		if rr.GetRemote() == remote.Name {
			err = ddb.DeleteBranch(ctx, rr)

			if err != nil {
				return errhand.BuildDError("error: failed to delete remote tracking ref '%s'", rr.String()).Build()
			}
		}
	}

	delete(repoState.Remotes, remote.Name)
	// TODO : dEnv.FS
	err = repoState.Save(filesys.LocalFS)

	if err != nil {
		return errhand.BuildDError("error: unable to save changes.").AddCause(err).Build()
	}

	return nil
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
