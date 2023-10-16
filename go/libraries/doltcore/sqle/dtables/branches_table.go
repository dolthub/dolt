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
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

const branchesDefaultRowCount = 10

var _ sql.Table = (*BranchesTable)(nil)
var _ sql.StatisticsTable = (*BranchesTable)(nil)
var _ sql.UpdatableTable = (*BranchesTable)(nil)
var _ sql.DeletableTable = (*BranchesTable)(nil)
var _ sql.InsertableTable = (*BranchesTable)(nil)
var _ sql.ReplaceableTable = (*BranchesTable)(nil)

// BranchesTable is the system table that accesses branches
type BranchesTable struct {
	db     dsess.SqlDatabase
	remote bool
}

// NewBranchesTable creates a BranchesTable
func NewBranchesTable(_ *sql.Context, db dsess.SqlDatabase) sql.Table {
	return &BranchesTable{db: db}
}

// NewRemoteBranchesTable creates a BranchesTable with only remote refs
func NewRemoteBranchesTable(_ *sql.Context, ddb dsess.SqlDatabase) sql.Table {
	return &BranchesTable{ddb, true}
}

func (bt *BranchesTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(bt.Schema())
	numRows, _, err := bt.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (bt *BranchesTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return branchesDefaultRowCount, false, nil
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// BranchesTableName
func (bt *BranchesTable) Name() string {
	if bt.remote {
		return doltdb.RemoteBranchesTableName
	}
	return doltdb.BranchesTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// BranchesTableName
func (bt *BranchesTable) String() string {
	if bt.remote {
		return doltdb.RemoteBranchesTableName
	}
	return doltdb.BranchesTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the branches system table
func (bt *BranchesTable) Schema() sql.Schema {
	tableName := doltdb.BranchesTableName
	if bt.remote {
		tableName = doltdb.RemoteBranchesTableName
	}

	columns := []*sql.Column{
		{Name: "name", Type: types.Text, Source: tableName, PrimaryKey: true, Nullable: false},
		{Name: "hash", Type: types.Text, Source: tableName, PrimaryKey: false, Nullable: false},
		{Name: "latest_committer", Type: types.Text, Source: tableName, PrimaryKey: false, Nullable: true},
		{Name: "latest_committer_email", Type: types.Text, Source: tableName, PrimaryKey: false, Nullable: true},
		{Name: "latest_commit_date", Type: types.Datetime, Source: tableName, PrimaryKey: false, Nullable: true},
		{Name: "latest_commit_message", Type: types.Text, Source: tableName, PrimaryKey: false, Nullable: true},
	}
	if !bt.remote {
		columns = append(columns, &sql.Column{Name: "remote", Type: types.Text, Source: tableName, PrimaryKey: false, Nullable: true})
		columns = append(columns, &sql.Column{Name: "branch", Type: types.Text, Source: tableName, PrimaryKey: false, Nullable: true})
	}
	return columns
}

// Collation implements the sql.Table interface.
func (bt *BranchesTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (bt *BranchesTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (bt *BranchesTable) PartitionRows(sqlCtx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return NewBranchItr(sqlCtx, bt)
}

// BranchItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type BranchItr struct {
	table    *BranchesTable
	branches []string
	commits  []*doltdb.Commit
	idx      int
}

// NewBranchItr creates a BranchItr from the current environment.
func NewBranchItr(ctx *sql.Context, table *BranchesTable) (*BranchItr, error) {
	var branchRefs []ref.DoltRef
	var err error
	db := table.db
	remote := table.remote

	txRoot, err := dsess.TransactionRoot(ctx, db)
	if err != nil {
		return nil, err
	}

	ddb := db.DbData().Ddb

	if remote {
		branchRefs, err = ddb.GetRefsOfTypeByNomsRoot(ctx, map[ref.RefType]struct{}{ref.RemoteRefType: {}}, txRoot)
		if err != nil {
			return nil, err
		}
	} else {
		branchRefs, err = ddb.GetBranchesByNomsRoot(ctx, txRoot)
		if err != nil {
			return nil, err
		}
	}

	branchNames := make([]string, len(branchRefs))
	commits := make([]*doltdb.Commit, len(branchRefs))
	for i, branch := range branchRefs {
		commit, err := ddb.ResolveCommitRefAtRoot(ctx, branch, txRoot)

		if err != nil {
			return nil, err
		}

		if branch.GetType() == ref.RemoteRefType {
			branchNames[i] = "remotes/" + branch.GetPath()
		} else {
			branchNames[i] = branch.GetPath()
		}

		commits[i] = commit
	}

	return &BranchItr{
		table:    table,
		branches: branchNames,
		commits:  commits,
		idx:      0,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *BranchItr) Next(ctx *sql.Context) (sql.Row, error) {
	if itr.idx >= len(itr.commits) {
		return nil, io.EOF
	}

	defer func() {
		itr.idx++
	}()

	name := itr.branches[itr.idx]
	cm := itr.commits[itr.idx]
	meta, err := cm.GetCommitMeta(ctx)

	if err != nil {
		return nil, err
	}

	h, err := cm.HashOf()

	if err != nil {
		return nil, err
	}

	remoteBranches := itr.table.remote
	if remoteBranches {
		return sql.NewRow(name, h.String(), meta.Name, meta.Email, meta.Time(), meta.Description), nil
	} else {
		branches, err := itr.table.db.DbData().Rsr.GetBranches()

		if err != nil {
			return nil, err
		}

		remoteName := ""
		branchName := ""
		branch, ok := branches[name]
		if ok {
			remoteName = branch.Remote
			branchName = branch.Merge.Ref.GetPath()
		}
		return sql.NewRow(name, h.String(), meta.Name, meta.Email, meta.Time(), meta.Description, remoteName, branchName), nil
	}
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

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (bWr branchWriter) Insert(ctx *sql.Context, r sql.Row) error {
	return fmt.Errorf("the dolt_branches table is read-only; use the dolt_branch stored procedure to edit remotes")
}

// Update the given row. Provides both the old and new rows.
func (bWr branchWriter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	return fmt.Errorf("the dolt_branches table is read-only; use the dolt_branch stored procedure to edit remotes")
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (bWr branchWriter) Delete(ctx *sql.Context, r sql.Row) error {
	return fmt.Errorf("the dolt_branches table is read-only; use the dolt_branch stored procedure to edit remotes")
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
