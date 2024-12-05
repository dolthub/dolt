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

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

const statusDefaultRowCount = 10

// StatusTable is a sql.Table implementation that implements a system table which shows the dolt branches
type StatusTable struct {
	tableName     string
	ddb           *doltdb.DoltDB
	workingSet    *doltdb.WorkingSet
	rootsProvider env.RootsProvider
}

var _ sql.StatisticsTable = (*StatusTable)(nil)

func (st StatusTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(st.Schema())
	numRows, _, err := st.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (st StatusTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return statusDefaultRowCount, false, nil
}

func (st StatusTable) Name() string {
	return st.tableName
}

func (st StatusTable) String() string {
	return st.tableName
}

func getDoltStatusSchema(tableName string) sql.Schema {
	return []*sql.Column{
		{Name: "table_name", Type: types.Text, Source: tableName, PrimaryKey: true, Nullable: false},
		{Name: "staged", Type: types.Boolean, Source: tableName, PrimaryKey: true, Nullable: false},
		{Name: "status", Type: types.Text, Source: tableName, PrimaryKey: true, Nullable: false},
	}
}

// GetDoltStatusSchema returns the schema of the dolt_status system table. This is used
// by Doltgres to update the dolt_status schema using Doltgres types.
var GetDoltStatusSchema = getDoltStatusSchema

func (st StatusTable) Schema() sql.Schema {
	return GetDoltStatusSchema(st.tableName)
}

func (st StatusTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (st StatusTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (st StatusTable) PartitionRows(context *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return newStatusItr(context, &st)
}

// NewStatusTable creates a StatusTable
func NewStatusTable(_ *sql.Context, tableName string, ddb *doltdb.DoltDB, ws *doltdb.WorkingSet, rp env.RootsProvider) sql.Table {
	return &StatusTable{
		tableName:     tableName,
		ddb:           ddb,
		workingSet:    ws,
		rootsProvider: rp,
	}
}

// StatusItr is a sql.RowIter implementation which iterates over each commit as if it'st a row in the table.
type StatusItr struct {
	rows []statusTableRow
}

type statusTableRow struct {
	tableName string
	isStaged  bool
	status    string
}

func containsTableName(name string, names []doltdb.TableName) bool {
	for _, s := range names {
		if s.String() == name {
			return true
		}
	}
	return false
}

func newStatusItr(ctx *sql.Context, st *StatusTable) (*StatusItr, error) {
	rp := st.rootsProvider

	roots, err := rp.GetRoots(ctx)
	if err != nil {
		return nil, err
	}

	stagedTables, unstagedTables, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	// Some tables may differ only in column tags and/or recorded conflicts.
	// We try to make such changes invisible to users and shouldn't display them for unstaged tables.
	changedUnstagedTables := make([]diff.TableDelta, 0, len(unstagedTables))
	for _, unstagedTableDiff := range unstagedTables {
		changed, err := unstagedTableDiff.HasChangesIgnoringColumnTags(ctx)
		if err != nil {
			return nil, err
		}
		if changed {
			changedUnstagedTables = append(changedUnstagedTables, unstagedTableDiff)
		}
	}
	unstagedTables = changedUnstagedTables

	stagedSchemas, unstagedSchemas, err := diff.GetStagedUnstagedDatabaseSchemaDeltas(ctx, roots)
	if err != nil {
		return nil, err
	}

	rows := make([]statusTableRow, 0, len(stagedTables)+len(unstagedTables)+len(stagedSchemas)+len(unstagedSchemas))

	cvTables, err := doltdb.TablesWithConstraintViolations(ctx, roots.Working)
	if err != nil {
		return nil, err
	}

	for _, tbl := range cvTables {
		rows = append(rows, statusTableRow{
			tableName: tbl.String(),
			status:    "constraint violation",
		})
	}

	if st.workingSet.MergeActive() {
		ms := st.workingSet.MergeState()
		for _, tbl := range ms.TablesWithSchemaConflicts() {
			rows = append(rows, statusTableRow{
				tableName: tbl.String(),
				isStaged:  false,
				status:    "schema conflict",
			})
		}

		for _, tbl := range ms.MergedTables() {
			rows = append(rows, statusTableRow{
				tableName: tbl.String(),
				isStaged:  true,
				status:    mergedStatus,
			})
		}
	}

	cnfTables, err := doltdb.TablesWithDataConflicts(ctx, roots.Working)
	if err != nil {
		return nil, err
	}
	for _, tbl := range cnfTables {
		rows = append(rows, statusTableRow{
			tableName: tbl.String(),
			status:    mergeConflictStatus,
		})
	}

	for _, td := range stagedTables {
		tblName := tableName(td)
		if doltdb.IsFullTextTable(tblName) {
			continue
		}
		if containsTableName(tblName, cvTables) {
			continue
		}
		rows = append(rows, statusTableRow{
			tableName: tblName,
			isStaged:  true,
			status:    statusString(td),
		})
	}
	for _, td := range unstagedTables {
		tblName := tableName(td)
		if doltdb.IsFullTextTable(tblName) {
			continue
		}
		if containsTableName(tblName, cvTables) {
			continue
		}
		rows = append(rows, statusTableRow{
			tableName: tblName,
			isStaged:  false,
			status:    statusString(td),
		})
	}

	for _, sd := range stagedSchemas {
		rows = append(rows, statusTableRow{
			tableName: sd.CurName(),
			isStaged:  true,
			status:    schemaStatusString(sd),
		})
	}

	for _, sd := range unstagedSchemas {
		rows = append(rows, statusTableRow{
			tableName: sd.CurName(),
			isStaged:  false,
			status:    schemaStatusString(sd),
		})
	}

	return &StatusItr{rows: rows}, nil
}

func schemaStatusString(sd diff.DatabaseSchemaDelta) string {
	if sd.IsAdd() {
		return "new schema"
	} else if sd.IsDrop() {
		return "deleted schema"
	} else {
		panic("unexpected schema delta")
	}
}

func tableName(td diff.TableDelta) string {
	if td.IsRename() {
		return fmt.Sprintf("%s -> %s", td.FromName.String(), td.ToName.String())
	} else {
		return td.CurName()
	}
}

func statusString(td diff.TableDelta) string {
	if td.IsAdd() {
		return "new table"
	} else if td.IsDrop() {
		return "deleted"
	} else if td.IsRename() {
		return "renamed"
	} else {
		return "modified"
	}
}

const mergeConflictStatus = "conflict"
const mergedStatus = "merged"

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *StatusItr) Next(*sql.Context) (sql.Row, error) {
	if len(itr.rows) <= 0 {
		return nil, io.EOF
	}
	row := itr.rows[0]
	itr.rows = itr.rows[1:]
	return sql.NewRow(row.tableName, row.isStaged, row.status), nil
}

// Close closes the iterator.
func (itr *StatusItr) Close(*sql.Context) error {
	return nil
}
