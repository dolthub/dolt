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

func NewStatusItr(ctx *sql.Context, st *StatusTable) (*StatusItr, error) {
	ddb := st.ddb
	rsr := st.rsr
	drw := st.drw

	stagedTables, unstagedTables, err := diff.GetStagedUnstagedTableDeltas(ctx, ddb, rsr)

	if err != nil {
		return &StatusItr{}, err
	}

	stagedDocDiffs, notStagedDocDiffs, err := diff.GetDocDiffs(ctx, ddb, rsr, drw)

	if err != nil {
		return &StatusItr{}, err
	}

	workingTblsInConflict, _, _, err := merge.GetTablesInConflict(ctx, ddb, rsr)

	if err != nil {
		return &StatusItr{}, err
	}

	workingDocsInConflict, err := merge.GetDocsInConflict(ctx, ddb, rsr, drw)

	if err != nil {
		return &StatusItr{}, err
	}

	tLength := len(stagedTables) + len(unstagedTables) + len(stagedDocDiffs.Docs) + len(notStagedDocDiffs.Docs) + len(workingTblsInConflict) + len(workingDocsInConflict.Docs)
	tables := make([]string, tLength)
	isStaged := make([]bool, tLength)
	statuses := make([]string, tLength)

	itr := StatusItr{tables: tables, isStaged: isStaged, statuses: statuses, idx: 0}

	idx := handleStagedTables(stagedTables, &itr, 0)
	idx = handleUnstagedTables(unstagedTables, &itr, idx)
	idx = handleStagedDocDiffs(stagedDocDiffs, &itr, idx)
	idx = handleUnstagedDocDiffs(notStagedDocDiffs, &itr, idx)
	idx = handleWorkingTablesInConflict(workingTblsInConflict, &itr, idx)
	idx = handleWorkingDocConflicts(workingDocsInConflict, &itr, idx)

	return &itr, nil
}

func handleStagedTables(staged []diff.TableDelta, itr *StatusItr, idx int) int {
	for _, td := range staged {
		itr.isStaged[idx] = true
		if td.IsAdd() {
			itr.tables[idx] = td.CurName()
			itr.statuses[idx] = "new table"
		} else if td.IsDrop() {
			itr.tables[idx] = td.CurName()
			itr.statuses[idx] = "deleted"
		} else if td.IsRename() {
			itr.tables[idx] = fmt.Sprintf("%s -> %s", td.FromName, td.ToName)
			itr.statuses[idx] = "renamed"
		} else {
			itr.tables[idx] = td.CurName()
			itr.statuses[idx] = "modified"
		}

		idx += 1
	}

	return idx
}

func handleUnstagedTables(notStaged []diff.TableDelta, itr *StatusItr, idx int) int {
	for _, td := range notStaged {
		itr.isStaged[idx] = false
		if td.IsAdd() {
			itr.tables[idx] = td.CurName()
			itr.statuses[idx] = "new table"
		} else if td.IsDrop() {
			itr.tables[idx] = td.CurName()
			itr.statuses[idx] = "deleted"
		} else if td.IsRename() {
			itr.tables[idx] = fmt.Sprintf("%s -> %s", td.FromName, td.ToName)
			itr.statuses[idx] = "renamed"
		} else {
			itr.tables[idx] = td.CurName()
			itr.statuses[idx] = "modified"
		}

		idx += 1
	}
	return idx
}

var docDiffTypeToLabel = map[diff.DocDiffType]string{
	diff.ModifiedDoc: "modified",
	diff.RemovedDoc:  "deleted",
	diff.AddedDoc:    "new doc",
}

func handleStagedDocDiffs(staged *diff.DocDiffs, itr *StatusItr, idx int) int {
	for _, docName := range staged.Docs {
		dType := staged.DocToType[docName]

		itr.tables[idx] = docName
		itr.isStaged[idx] = true
		itr.statuses[idx] = docDiffTypeToLabel[dType]

		idx += 1
	}

	return idx
}

func handleUnstagedDocDiffs(notStaged *diff.DocDiffs, itr *StatusItr, idx int) int {
	for _, docName := range notStaged.Docs {
		dType := notStaged.DocToType[docName]

		itr.tables[idx] = docName
		itr.isStaged[idx] = false
		itr.statuses[idx] = docDiffTypeToLabel[dType]

		idx += 1
	}

	return idx
}

const mergeConflictStatus = "conflict"

func handleWorkingTablesInConflict(workingTables []string, itr *StatusItr, idx int) int {
	for _, tableName := range workingTables {
		itr.tables[idx] = tableName
		itr.isStaged[idx] = false
		itr.statuses[idx] = mergeConflictStatus

		idx += 1
	}

	return idx
}

func handleWorkingDocConflicts(workingDocs *diff.DocDiffs, itr *StatusItr, idx int) int {
	for _, docName := range workingDocs.Docs {
		itr.tables[idx] = docName
		itr.isStaged[idx] = false
		itr.statuses[idx] = mergeConflictStatus

		idx += 1
	}

	return idx
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




















































