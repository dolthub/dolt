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

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

// StatusTable is a sql.Table implementation that implements a system table which shows the dolt branches
type StatusTable struct {
	ddb           *doltdb.DoltDB
	rootsProvider env.RootsProvider
	drw           env.DocsReadWriter
	dbName        string
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

func (s StatusTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (s StatusTable) PartitionRows(context *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return newStatusItr(context, &s)
}

// NewStatusTable creates a StatusTable
func NewStatusTable(_ *sql.Context, dbName string, ddb *doltdb.DoltDB, rp env.RootsProvider, drw env.DocsReadWriter) sql.Table {
	return &StatusTable{
		ddb:           ddb,
		dbName:        dbName,
		rootsProvider: rp,
		drw:           drw,
	}
}

// StatusIter is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type StatusItr struct {
	tables   []string
	isStaged []bool
	statuses []string
	idx      int
}

func newStatusItr(ctx *sql.Context, st *StatusTable) (*StatusItr, error) {
	rp := st.rootsProvider
	drw := st.drw

	roots, err := rp.GetRoots(ctx)
	if err != nil {
		return nil, err
	}

	stagedTables, unstagedTables, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	docsOnDisk, err := drw.GetDocsOnDisk()
	if err != nil {
		return nil, err
	}
	stagedDocDiffs, unStagedDocDiffs, err := diff.GetDocDiffs(ctx, roots, docsOnDisk)
	if err != nil {
		return nil, err
	}

	workingTblsInConflict, _, _, err := merge.GetTablesInConflict(ctx, roots)
	if err != nil {
		return nil, err
	}

	docs, err := drw.GetDocsOnDisk()
	if err != nil {
		return nil, err
	}
	workingDocsInConflict, err := merge.GetDocsInConflict(ctx, roots.Working, docs)
	if err != nil {
		return nil, err
	}

	tLength := len(stagedTables) + len(unstagedTables) + len(stagedDocDiffs.Docs) + len(unStagedDocDiffs.Docs) + len(workingTblsInConflict) + len(workingDocsInConflict.Docs)

	tables := make([]string, tLength)
	isStaged := make([]bool, tLength)
	statuses := make([]string, tLength)

	itr := &StatusItr{tables: tables, isStaged: isStaged, statuses: statuses, idx: 0}

	idx := handleStagedUnstagedTables(stagedTables, unstagedTables, itr, 0)
	idx = handleStagedUnstagedDocDiffs(stagedDocDiffs, unStagedDocDiffs, itr, idx)
	idx = handleWorkingTablesInConflict(workingTblsInConflict, itr, idx)
	idx = handleWorkingDocConflicts(workingDocsInConflict, itr, idx)

	return itr, nil
}

var tblDiffTypeToLabel = map[diff.TableDiffType]string{
	diff.ModifiedTable: "modified",
	diff.RenamedTable:  "renamed",
	diff.RemovedTable:  "deleted",
	diff.AddedTable:    "new table",
}

func handleStagedUnstagedTables(staged, unstaged []diff.TableDelta, itr *StatusItr, idx int) int {
	combined := append(staged, unstaged...)
	for i, td := range combined {
		itr.isStaged[idx] = i < len(staged)
		if td.IsAdd() {
			itr.tables[idx] = td.CurName()
			itr.statuses[idx] = tblDiffTypeToLabel[diff.AddedTable]
		} else if td.IsDrop() {
			itr.tables[idx] = td.CurName()
			itr.statuses[idx] = tblDiffTypeToLabel[diff.RemovedTable]
		} else if td.IsRename() {
			itr.tables[idx] = fmt.Sprintf("%s -> %s", td.FromName, td.ToName)
			itr.statuses[idx] = tblDiffTypeToLabel[diff.RemovedTable]
		} else {
			itr.tables[idx] = td.CurName()
			itr.statuses[idx] = tblDiffTypeToLabel[diff.ModifiedTable]
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

func handleStagedUnstagedDocDiffs(staged *diff.DocDiffs, unstaged *diff.DocDiffs, itr *StatusItr, idx int) int {
	combined := append(staged.Docs, unstaged.Docs...)
	for i, docName := range combined {
		dType := staged.DocToType[docName]

		itr.tables[idx] = docName
		itr.isStaged[idx] = i < len(staged.Docs)
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
func (itr *StatusItr) Next(*sql.Context) (sql.Row, error) {
	if itr.idx >= len(itr.tables) {
		return nil, io.EOF
	}

	defer func() {
		itr.idx++
	}()

	return sql.NewRow(itr.tables[itr.idx], itr.isStaged[itr.idx], itr.statuses[itr.idx]), nil
}

// Close closes the iterator.
func (itr *StatusItr) Close(*sql.Context) error {
	return nil
}
