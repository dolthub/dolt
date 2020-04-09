// Copyright 2020 Liquidata, Inc.
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

package sqle

import (
	"io"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
)

const (
	// BranchesTableName is the system table name
	BranchesTableName = "dolt_branches"
)

var _ sql.Table = (*BranchesTable)(nil)

// BranchesTable is a sql.Table implementation that implements a system table which shows the dolt branches
type BranchesTable struct {
	ddb *doltdb.DoltDB
	rsr env.RepoStateReader
}

// NewBranchesTable creates a BranchesTable
func NewBranchesTable(ddb *doltdb.DoltDB, rs env.RepoStateReader) *BranchesTable {
	return &BranchesTable{ddb: ddb, rsr: rs}
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// BranchesTableName
func (dt *BranchesTable) Name() string {
	return BranchesTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// BranchesTableName
func (dt *BranchesTable) String() string {
	return BranchesTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the branches system table
func (dt *BranchesTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "name", Type: sql.Text, Source: BranchesTableName, PrimaryKey: true},
		{Name: "hash", Type: sql.Text, Source: BranchesTableName, PrimaryKey: true},
		{Name: "latest_committer", Type: sql.Text, Source: BranchesTableName, PrimaryKey: false},
		{Name: "latest_committer_email", Type: sql.Text, Source: BranchesTableName, PrimaryKey: false},
		{Name: "latest_commit_date", Type: sql.Datetime, Source: BranchesTableName, PrimaryKey: false},
		{Name: "latest_commit_message", Type: sql.Text, Source: BranchesTableName, PrimaryKey: false},
	}
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (dt *BranchesTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &doltTablePartitionIter{}, nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (dt *BranchesTable) PartitionRows(sqlCtx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return NewBranchItr(sqlCtx, dt.ddb, dt.rsr)
}

// BranchItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type BranchItr struct {
	branches []string
	commits  []*doltdb.Commit
	idx      int
}

// NewBranchItr creates a BranchItr from the current environment.
func NewBranchItr(sqlCtx *sql.Context, ddb *doltdb.DoltDB, rsr env.RepoStateReader) (*BranchItr, error) {
	branches, err := ddb.GetBranches(sqlCtx)

	if err != nil {
		return nil, err
	}

	branchNames := make([]string, len(branches))
	commits := make([]*doltdb.Commit, len(branches))
	for i, branch := range branches {
		cs, err := doltdb.NewCommitSpec("HEAD", branch.GetPath())

		if err != nil {
			return nil, err
		}

		commit, err := ddb.Resolve(sqlCtx, cs)

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
func (itr *BranchItr) Next() (sql.Row, error) {
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
func (itr *BranchItr) Close() error {
	return nil
}
