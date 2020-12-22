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
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"
	"io"
)

// StatusTable is a sql.Table implementation that implements a system table which shows the dolt branches
type StatusTable struct {
	ddb *doltdb.DoltDB
	rsr env.RepoStateReader
	drw env.DocsReadWriter
}

func (s StatusTable) Name() string {
	return doltdb.StatusTableName
}

func (s StatusTable) String() string {
	return doltdb.StatusTableName
}

func (s StatusTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "table_name", Type: sql.Text, Source: doltdb.StatusTableName, PrimaryKey: true, Nullable: false},
		{Name: "staged", Type: sql.Boolean, Source: doltdb.StatusTableName, PrimaryKey: false, Nullable: false},
		{Name: "status", Type: sql.Text, Source: doltdb.StatusTableName, PrimaryKey: false, Nullable: false},
	}
}

// TODO: Better understand partitions.
func (s StatusTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return sqlutil.NewSinglePartitionIter(), nil
}

func (s StatusTable) PartitionRows(context *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return NewStatusItr(context, &s)
}

// NewStatusTable creates a StatusTable
func NewStatusTable(_ *sql.Context, ddb *doltdb.DoltDB, rsr env.RepoStateReader, drw env.DocsReadWriter) sql.Table {
	return &StatusTable{ddb: ddb, rsr: rsr, drw: drw}
}

// BranchItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type StatusItr struct {
	tables []string
	isStaged []bool
	statuses  []string
	idx		int
}

// TODO: Refactor out status constants
func NewStatusItr(ctx *sql.Context, st *StatusTable) (*StatusItr, error) {
	ddb := st.ddb
	rsr := st.rsr
	// drw := st.drw

	staged, notStaged, err := diff.GetStagedUnstagedTableDeltas(ctx, ddb, rsr)

	if err != nil {
		return &StatusItr{}, err
	}

	// stagedDocDiffs, notStagedDocDiffs, err := diff.GetDocDiffs(ctx, ddb, rsr, drw)

	workingTblsInConflict, _, _, err := merge.GetTablesInConflict(ctx, ddb, rsr)

	if err != nil {
		return &StatusItr{}, err
	}

	tLength := len(staged) + len(notStaged)
	tables := make([]string, tLength)
	isStaged := make([]bool, tLength)
	statuses := make([]string, tLength)

	for i, td := range staged {
		isStaged[i] = true
		if td.IsAdd() {
			tables[i] = td.CurName()
			statuses[i] = "new table"
		} else if td.IsDrop() {
			tables[i] = td.CurName()
			statuses[i] = "deleted"
		} else if td.IsRename() {
			tables[i] = fmt.Sprintf("%s -> %s", td.FromName, td.ToName)
			statuses[i] = "renamed"
		} else {
			tables[i] = td.CurName()
			statuses[i] = "modified"
		}
	}

	for i, td := range notStaged {
		isStaged[i] = false
		if td.IsAdd() {
			tables[i] = td.CurName()
			statuses[i] = "new table"
		} else if td.IsDrop() {
			tables[i] = td.CurName()
			statuses[i] = "deleted"
		} else if td.IsRename() {
			tables[i] = fmt.Sprintf("%s -> %s", td.FromName, td.ToName)
			statuses[i] = "renamed"
		} else {
			tables[i] = td.CurName()
			statuses[i] = "modified"
		}
	}

	for i, tdName := range workingTblsInConflict {
		isStaged[i] = false
		statuses[i] = "merge conflict"
		tables[i] = tdName
	}


	return &StatusItr{tables: tables, isStaged: isStaged, statuses: statuses, idx: 0}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *StatusItr) Next() (sql.Row, error) {
	if itr.idx >= len(itr.tables) {
		return nil, io.EOF
	}

	defer func() {
		itr.idx++
	}()

	return sql.NewRow(itr.tables[itr.idx], itr.isStaged[itr.idx], itr.statuses[itr.idx]), nil
}

// Close closes the iterator.
func (itr *StatusItr) Close() error {
	return nil
}




















































