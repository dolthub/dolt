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
	numBytesPerRow := schema.SchemaAvgLength(st.Schema(ctx))
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

func (st StatusIgnoredTable) Schema(ctx *sql.Context) sql.Schema {
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

	// A table matched by conflicting patterns is reported as not ignored.
	ignoredTables, err := ignoredNewTables(ctx, st.rootsProvider, unstagedTables, true)
	if err != nil {
		return nil, err
	}

	// Convert status rows to status_ignored rows, adding the ignored column
	rows := make([]statusIgnoredTableRow, len(statusRows))
	for i, row := range statusRows {
		// Only unstaged NEW tables can be ignored (same as Git behavior). Tables that are
		// modified, deleted, or renamed are already tracked, so ignore patterns don't apply.
		ignored := row.isStaged == byte(0) && row.status == newTableStatus && ignoredTables[row.tableName]
		rows[i] = statusIgnoredTableRow{
			tableName: row.tableName,
			isStaged:  row.isStaged,
			status:    row.status,
			ignored:   ignored,
		}
	}

	return &StatusIgnoredItr{rows: rows}, nil
}

// ignoredNewTables returns the names, as shown in the status tables, of the unstaged new tables
// that dolt_ignore ignores. Each table is matched against the patterns of its own schema, by its
// name without the schema, so schema-qualified tables (as in Doltgres) match the same patterns as
// tables without a schema. A table matched by conflicting patterns is an error unless
// |allowConflicts| is set, in which case it is not ignored.
func ignoredNewTables(ctx *sql.Context, rp env.RootsProvider[*sql.Context], unstagedTables []diff.TableDelta, allowConflicts bool) (map[string]bool, error) {
	var names []doltdb.TableName
	for _, td := range unstagedTables {
		if td.IsAdd() {
			names = append(names, td.ToName)
		}
	}
	ignored := make(map[string]bool)
	if len(names) == 0 {
		return ignored, nil
	}
	roots, err := rp.GetRoots(ctx)
	if err != nil {
		return nil, err
	}
	patterns, err := doltdb.GetIgnoredTablePatterns(ctx, roots, doltdb.GetUniqueSchemaNamesFromTableNames(names))
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		schemaPatterns := patterns[name.Schema]
		result, err := schemaPatterns.IsTableNameIgnored(name)
		if err != nil {
			if allowConflicts && doltdb.AsDoltIgnoreInConflict(err) != nil {
				continue
			}
			return nil, err
		}
		if result == doltdb.Ignore {
			ignored[name.String()] = true
		}
	}
	return ignored, nil
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
