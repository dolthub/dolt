// Copyright 2026 Dolthub, Inc.
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
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/adapters"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

const statusIgnoredDefaultRowCount = 10

// StatusIgnoredTable is a sql.Table implementation that shows status including ignored tables.
// This is the SQL equivalent of `dolt status --ignored`.
type StatusIgnoredTable struct {
	rootsProvider env.RootsProvider[*sql.Context]
	ddb           *doltdb.DoltDB
	workingSet    *doltdb.WorkingSet
	tableName     string
}

var _ sql.StatisticsTable = (*StatusIgnoredTable)(nil)

func (st StatusIgnoredTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(st.Schema())
	numRows, _, err := st.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (st StatusIgnoredTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return statusIgnoredDefaultRowCount, false, nil
}

func (st StatusIgnoredTable) Name() string {
	return st.tableName
}

func (st StatusIgnoredTable) String() string {
	return st.tableName
}

func (st StatusIgnoredTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "table_name", Type: types.Text, Source: doltdb.StatusIgnoredTableName, PrimaryKey: true, Nullable: false},
		{Name: "staged", Type: types.Boolean, Source: doltdb.StatusIgnoredTableName, PrimaryKey: true, Nullable: false},
		{Name: "status", Type: types.Text, Source: doltdb.StatusIgnoredTableName, PrimaryKey: true, Nullable: false},
		{Name: "ignored", Type: types.Boolean, Source: doltdb.StatusIgnoredTableName, PrimaryKey: false, Nullable: false},
	}
}

func (st StatusIgnoredTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (st StatusIgnoredTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (st StatusIgnoredTable) PartitionRows(context *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return newStatusIgnoredItr(context, &st)
}

// NewStatusIgnoredTable creates a new StatusIgnoredTable using either an integrators' [adapters.TableAdapter] or the
// NewStatusIgnoredTableWithNoAdapter constructor (the default implementation provided by Dolt).
func NewStatusIgnoredTable(ctx *sql.Context, tableName string, ddb *doltdb.DoltDB, ws *doltdb.WorkingSet, rp env.RootsProvider[*sql.Context]) sql.Table {
	adapter, ok := adapters.DoltTableAdapterRegistry.GetAdapter(tableName)
	if ok {
		return adapter.NewTable(ctx, tableName, ddb, ws, rp)
	}

	return NewStatusIgnoredTableWithNoAdapter(ctx, tableName, ddb, ws, rp)
}

// NewStatusIgnoredTableWithNoAdapter returns a new StatusIgnoredTable.
func NewStatusIgnoredTableWithNoAdapter(_ *sql.Context, tableName string, ddb *doltdb.DoltDB, ws *doltdb.WorkingSet, rp env.RootsProvider[*sql.Context]) sql.Table {
	return &StatusIgnoredTable{
		tableName:     tableName,
		ddb:           ddb,
		workingSet:    ws,
		rootsProvider: rp,
	}
}

// StatusIgnoredItr is a sql.RowIter implementation for the status_ignored table.
type StatusIgnoredItr struct {
	rows []statusIgnoredTableRow
}

type statusIgnoredTableRow struct {
	tableName string
	status    string
	isStaged  byte // not a bool bc wire protocol confuses bools and tinyint(1)
	ignored   bool
}

func newStatusIgnoredItr(ctx *sql.Context, st *StatusIgnoredTable) (*StatusIgnoredItr, error) {
	// If no roots provider was set, then there is no status to report
	if st.rootsProvider == nil {
		return &StatusIgnoredItr{rows: nil}, nil
	}

	// Get the base status data using the shared function
	statusRows, unstagedTables, err := getStatusRowsData(ctx, st.rootsProvider, st.workingSet)
	if err != nil {
		return nil, err
	}

	// Get ignore patterns for checking if unstaged tables are ignored
	ignorePatterns, err := getIgnorePatterns(ctx, st.rootsProvider)
	if err != nil {
		return nil, err
	}

	// Build a set of unstaged table names for quick lookup
	unstagedTableNames := buildUnstagedTableNameSet(unstagedTables)

	// Convert status rows to status_ignored rows, adding the ignored column
	rows := make([]statusIgnoredTableRow, len(statusRows))
	for i, row := range statusRows {
		ignored := false
		// Only check ignore patterns for unstaged NEW tables (same as Git behavior).
		// Tables that are modified, deleted, or renamed are already tracked,
		// so ignore patterns don't apply to them.
		if row.isStaged == byte(0) && row.status == newTableStatus && unstagedTableNames[row.tableName] {
			tblNameObj := doltdb.TableName{Name: row.tableName}
			result, err := ignorePatterns.IsTableNameIgnored(tblNameObj)
			if err != nil {
				return nil, err
			}
			if result == doltdb.Ignore {
				ignored = true
			}
		}
		rows[i] = statusIgnoredTableRow{
			tableName: row.tableName,
			isStaged:  row.isStaged,
			status:    row.status,
			ignored:   ignored,
		}
	}

	return &StatusIgnoredItr{rows: rows}, nil
}

// getIgnorePatterns fetches the ignore patterns from the roots provider.
func getIgnorePatterns(ctx *sql.Context, rp env.RootsProvider[*sql.Context]) (doltdb.IgnorePatterns, error) {
	roots, err := rp.GetRoots(ctx)
	if err != nil {
		return nil, err
	}
	schemas := []string{doltdb.DefaultSchemaName}
	ignorePatternMap, err := doltdb.GetIgnoredTablePatterns(ctx, roots, schemas)
	if err != nil {
		return nil, err
	}
	return ignorePatternMap[doltdb.DefaultSchemaName], nil
}

// buildUnstagedTableNameSet builds a set of unstaged table names for quick lookup.
func buildUnstagedTableNameSet(unstagedTables []diff.TableDelta) map[string]bool {
	result := make(map[string]bool, len(unstagedTables))
	for _, td := range unstagedTables {
		result[td.CurName()] = true
	}
	return result
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
func (itr *StatusIgnoredItr) Next(*sql.Context) (sql.Row, error) {
	if len(itr.rows) <= 0 {
		return nil, io.EOF
	}
	row := itr.rows[0]
	itr.rows = itr.rows[1:]
	return sql.NewRow(row.tableName, row.isStaged, row.status, row.ignored), nil
}

// Close closes the iterator.
func (itr *StatusIgnoredItr) Close(*sql.Context) error {
	return nil
}
