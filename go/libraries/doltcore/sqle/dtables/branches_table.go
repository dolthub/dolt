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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

var _ sql.Table = (*BranchesTable)(nil)
var _ sql.UpdatableTable = (*BranchesTable)(nil)
var _ sql.DeletableTable = (*BranchesTable)(nil)
var _ sql.InsertableTable = (*BranchesTable)(nil)
var _ sql.ReplaceableTable = (*BranchesTable)(nil)

// BranchesTable is a sql.Table implementation that implements a system table which shows the dolt branches
type BranchesTable struct {
	ddb *doltdb.DoltDB
}

// NewBranchesTable creates a BranchesTable
func NewBranchesTable(_ *sql.Context, ddb *doltdb.DoltDB) sql.Table {
	return &BranchesTable{ddb}
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// BranchesTableName
func (bt *BranchesTable) Name() string {
	return doltdb.BranchesTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// BranchesTableName
func (bt *BranchesTable) String() string {
	return doltdb.BranchesTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the branches system table
func (bt *BranchesTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "name", Type: sql.Text, Source: doltdb.BranchesTableName, PrimaryKey: true, Nullable: false},
		{Name: "hash", Type: sql.Text, Source: doltdb.BranchesTableName, PrimaryKey: false, Nullable: false},
		{Name: "latest_committer", Type: sql.Text, Source: doltdb.BranchesTableName, PrimaryKey: false, Nullable: true},
		{Name: "latest_committer_email", Type: sql.Text, Source: doltdb.BranchesTableName, PrimaryKey: false, Nullable: true},
		{Name: "latest_commit_date", Type: sql.Datetime, Source: doltdb.BranchesTableName, PrimaryKey: false, Nullable: true},
		{Name: "latest_commit_message", Type: sql.Text, Source: doltdb.BranchesTableName, PrimaryKey: false, Nullable: true},
	}
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (bt *BranchesTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (bt *BranchesTable) PartitionRows(sqlCtx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return NewBranchItr(sqlCtx, bt.ddb)
}

// BranchItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type BranchItr struct {
	branches []string
	commits  []*doltdb.Commit
	idx      int
}

// NewBranchItr creates a BranchItr from the current environment.
func NewBranchItr(sqlCtx *sql.Context, ddb *doltdb.DoltDB) (*BranchItr, error) {
	branches, err := ddb.GetBranches(sqlCtx)

	if err != nil {
		return nil, err
	}

	branchNames := make([]string, len(branches))
	commits := make([]*doltdb.Commit, len(branches))
	for i, branch := range branches {
		commit, err := ddb.ResolveCommitRef(sqlCtx, branch)

		if err != nil {
			return nil, err
		}

		branchNames[i] = branch.GetPath()
		commits[i] = commit
	}

	return &BranchItr{branchNames, commits, 0}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *BranchItr) Next(*sql.Context) (sql.Row, error) {
	if itr.idx >= len(itr.commits) {
		return nil, io.EOF
	}

	defer func() {
		itr.idx++
	}()

	name := itr.branches[itr.idx]
	cm := itr.commits[itr.idx]
	meta, err := cm.GetCommitMeta()

	if err != nil {
		return nil, err
	}

	h, err := cm.HashOf()

	if err != nil {
		return nil, err
	}

	return sql.NewRow(name, h.String(), meta.Name, meta.Email, meta.Time(), meta.Description), nil
}

// Close closes the iterator.
func (itr *BranchItr) Close(*sql.Context) error {
	return nil
}

// Replacer returns a RowReplacer for this table. The RowReplacer will have Insert and optionally Delete called once
// for each row, followed by a call to Close() when all rows have been processed.
func (bt *BranchesTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return branchWriter{bt}
}

// Updater returns a RowUpdater for this table. The RowUpdater will have Update called once for each row to be
// updated, followed by a call to Close() when all rows have been processed.
func (bt *BranchesTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return branchWriter{bt}
}

// Inserter returns an Inserter for this table. The Inserter will get one call to Insert() for each row to be
// inserted, and will end with a call to Close() to finalize the insert operation.
func (bt *BranchesTable) Inserter(*sql.Context) sql.RowInserter {
	return branchWriter{bt}
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
// and will end with a call to Close() to finalize the delete operation.
func (bt *BranchesTable) Deleter(*sql.Context) sql.RowDeleter {
	return branchWriter{bt}
}

var _ sql.RowReplacer = branchWriter{nil}
var _ sql.RowUpdater = branchWriter{nil}
var _ sql.RowInserter = branchWriter{nil}
var _ sql.RowDeleter = branchWriter{nil}

type branchWriter struct {
	bt *BranchesTable
}

func branchAndHashFromRow(r sql.Row) (string, string, error) {
	branchName, ok := r[0].(string)

	if !ok {
		return "", "", errors.New("invalid value type for branch")
	} else if !doltdb.IsValidUserBranchName(branchName) {
		return "", "", doltdb.ErrInvBranchName
	}

	commitHash, ok := r[1].(string)

	if !ok {
		return "", "", errors.New("invalid value type for hash")
	}

	return branchName, commitHash, nil
}

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (bWr branchWriter) Insert(ctx *sql.Context, r sql.Row) error {
	branchName, commitHash, err := branchAndHashFromRow(r)

	if err != nil {
		return err
	}

	cs, err := doltdb.NewCommitSpec(commitHash)

	if err != nil {
		return err
	}

	ddb := bWr.bt.ddb
	cm, err := ddb.Resolve(ctx, cs, nil)

	if err != nil {
		return err
	}

	branchRef := ref.NewBranchRef(branchName)

	// TODO: this isn't safe in a SQL context, since we have to update the working set of the new branch and it's a
	//  race. It needs to be able to retry the same as committing a transaction.
	err = ddb.NewBranchAtCommit(ctx, branchRef, cm)
	if err != nil {
		return err
	}

	return bWr.bt.ddb.ExecuteCommitHooks(ctx, branchRef.String())
}

// Update the given row. Provides both the old and new rows.
func (bWr branchWriter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	return bWr.Insert(ctx, new)
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (bWr branchWriter) Delete(ctx *sql.Context, r sql.Row) error {
	branchName, _, err := branchAndHashFromRow(r)

	if err != nil {
		return err
	}

	brRef := ref.NewBranchRef(branchName)
	exists, err := bWr.bt.ddb.HasRef(ctx, brRef)

	if err != nil {
		return err
	}

	if !exists {
		return sql.ErrDeleteRowNotFound.New()
	}

	return bWr.bt.ddb.DeleteBranch(ctx, brRef)
}

// StatementBegin implements the interface sql.TableEditor. Currently a no-op.
func (bWr branchWriter) StatementBegin(ctx *sql.Context) {}

// DiscardChanges implements the interface sql.TableEditor. Currently a no-op.
func (bWr branchWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// StatementComplete implements the interface sql.TableEditor. Currently a no-op.
func (bWr branchWriter) StatementComplete(ctx *sql.Context) error {
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (bWr branchWriter) Close(*sql.Context) error {
	return nil
}
