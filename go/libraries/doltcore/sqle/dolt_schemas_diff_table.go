// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqle

import (
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

const (
	// DiffTypeCol is the column name for the type of change (added, modified, removed)
	DiffTypeCol = "diff_type"
)

// doltSchemasDiffTable implements the dolt_diff_dolt_schemas system table
type doltSchemasDiffTable struct {
	name     string
	ddb      *doltdb.DoltDB
	fromRoot doltdb.RootValue
	toRoot   doltdb.RootValue
	fromRef  string
	toRef    string
	db       Database // Add database reference for DoltTable creation
}

var _ sql.Table = (*doltSchemasDiffTable)(nil)
var _ sql.PrimaryKeyTable = (*doltSchemasDiffTable)(nil)

// NewDoltSchemasDiffTable creates a new dolt_schemas diff table instance
func NewDoltSchemasDiffTable(ctx *sql.Context, ddb *doltdb.DoltDB, fromRoot, toRoot doltdb.RootValue, fromRef, toRef string, db Database) sql.Table {
	return &doltSchemasDiffTable{
		name:     doltdb.DoltDiffTablePrefix + doltdb.SchemasTableName,
		ddb:      ddb,
		fromRoot: fromRoot,
		toRoot:   toRoot,
		fromRef:  fromRef,
		toRef:    toRef,
		db:       db,
	}
}

// Name implements sql.Table
func (dsdt *doltSchemasDiffTable) Name() string {
	return dsdt.name
}

// String implements sql.Table
func (dsdt *doltSchemasDiffTable) String() string {
	return dsdt.name
}

// Schema implements sql.Table
func (dsdt *doltSchemasDiffTable) Schema() sql.Schema {
	// For diff tables, we need "to_" and "from_" prefixed columns plus commit info
	return sql.Schema{
		// TO columns
		&sql.Column{Name: "to_" + doltdb.SchemasTablesTypeCol, Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_" + doltdb.SchemasTablesNameCol, Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_" + doltdb.SchemasTablesFragmentCol, Type: types.LongText, Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_" + doltdb.SchemasTablesExtraCol, Type: types.JSON, Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_" + doltdb.SchemasTablesSqlModeCol, Type: types.MustCreateString(sqltypes.VarChar, 256, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_commit", Type: types.MustCreateString(sqltypes.VarChar, 1023, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "to_commit_date", Type: types.DatetimeMaxPrecision, Nullable: true, Source: dsdt.name},
		
		// FROM columns
		&sql.Column{Name: "from_" + doltdb.SchemasTablesTypeCol, Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_" + doltdb.SchemasTablesNameCol, Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_" + doltdb.SchemasTablesFragmentCol, Type: types.LongText, Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_" + doltdb.SchemasTablesExtraCol, Type: types.JSON, Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_" + doltdb.SchemasTablesSqlModeCol, Type: types.MustCreateString(sqltypes.VarChar, 256, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_commit", Type: types.MustCreateString(sqltypes.VarChar, 1023, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsdt.name},
		&sql.Column{Name: "from_commit_date", Type: types.DatetimeMaxPrecision, Nullable: true, Source: dsdt.name},
		
		// Diff type column
		&sql.Column{Name: DiffTypeCol, Type: types.MustCreateString(sqltypes.VarChar, 1023, sql.Collation_utf8mb4_0900_ai_ci), Nullable: false, Source: dsdt.name},
	}
}

// Collation implements sql.Table
func (dsdt *doltSchemasDiffTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements sql.Table
func (dsdt *doltSchemasDiffTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &doltSchemasDiffPartitionIter{
		fromRoot: dsdt.fromRoot,
		toRoot:   dsdt.toRoot,
		fromRef:  dsdt.fromRef,
		toRef:    dsdt.toRef,
	}, nil
}

// PartitionRows implements sql.Table
func (dsdt *doltSchemasDiffTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	p := partition.(*doltSchemasDiffPartition)
	return &doltSchemasDiffRowIter{
		ctx:      ctx,
		fromRoot: p.fromRoot,
		toRoot:   p.toRoot,
		fromRef:  p.fromRef,
		toRef:    p.toRef,
		db:       dsdt.db, // Pass database reference
	}, nil
}

// PrimaryKeySchema implements sql.PrimaryKeyTable
func (dsdt *doltSchemasDiffTable) PrimaryKeySchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     dsdt.Schema(),
		PkOrdinals: []int{0, 1}, // to_type, to_name
	}
}

// doltSchemasDiffPartition represents a single partition for dolt_schemas diff
type doltSchemasDiffPartition struct {
	fromRoot doltdb.RootValue
	toRoot   doltdb.RootValue
	fromRef  string
	toRef    string
}

func (dsdp *doltSchemasDiffPartition) Key() []byte {
	return []byte(dsdp.fromRef + ".." + dsdp.toRef)
}

// doltSchemasDiffPartitionIter creates a single partition for the diff
type doltSchemasDiffPartitionIter struct {
	fromRoot doltdb.RootValue
	toRoot   doltdb.RootValue
	fromRef  string
	toRef    string
	consumed bool
}

func (dsdpi *doltSchemasDiffPartitionIter) Next(ctx *sql.Context) (sql.Partition, error) {
	if dsdpi.consumed {
		return nil, io.EOF
	}
	dsdpi.consumed = true

	return &doltSchemasDiffPartition{
		fromRoot: dsdpi.fromRoot,
		toRoot:   dsdpi.toRoot,
		fromRef:  dsdpi.fromRef,
		toRef:    dsdpi.toRef,
	}, nil
}

func (dsdpi *doltSchemasDiffPartitionIter) Close(ctx *sql.Context) error {
	return nil
}

// doltSchemasDiffRowIter iterates through dolt_schemas differences
type doltSchemasDiffRowIter struct {
	ctx      *sql.Context
	fromRoot doltdb.RootValue
	toRoot   doltdb.RootValue
	fromRef  string
	toRef    string
	db       Database // Add database reference for DoltTable creation
	rows     []sql.Row
	idx      int
}

func (dsdri *doltSchemasDiffRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if dsdri.rows == nil {
		// Initialize diff rows
		err := dsdri.loadDiffRows()
		if err != nil {
			return nil, err
		}
	}

	if dsdri.idx >= len(dsdri.rows) {
		return nil, io.EOF
	}

	row := dsdri.rows[dsdri.idx]
	dsdri.idx++

	return row, nil
}

func (dsdri *doltSchemasDiffRowIter) loadDiffRows() error {
	// Get dolt_schemas tables from both roots
	fromTbl, fromExists, err := dsdri.fromRoot.GetTable(dsdri.ctx, doltdb.TableName{Name: doltdb.SchemasTableName})
	if err != nil {
		return err
	}

	toTbl, toExists, err := dsdri.toRoot.GetTable(dsdri.ctx, doltdb.TableName{Name: doltdb.SchemasTableName})
	if err != nil {
		return err
	}

	// Build maps of schema rows for comparison
	fromRows := make(map[string]sql.Row)
	toRows := make(map[string]sql.Row)

	// Read from table if it exists
	if fromExists {
		if err := dsdri.readDoltSchemasRows(fromTbl, dsdri.fromRoot, fromRows); err != nil {
			return err
		}
	}

	// Read to table if it exists
	if toExists {
		if err := dsdri.readDoltSchemasRows(toTbl, dsdri.toRoot, toRows); err != nil {
			return err
		}
	}

	// Generate diff rows
	rows := make([]sql.Row, 0)

	// Find added and modified rows
	for key, toRow := range toRows {
		if fromRow, exists := fromRows[key]; exists {
			// Compare rows to see if modified
			if !rowsEqual(fromRow, toRow) {
				// Modified row: to_* columns from toRow, from_* columns from fromRow
				diffRow := dsdri.createDiffRow(toRow, fromRow, "modified")
				rows = append(rows, diffRow)
			}
		} else {
			// Added row: to_* columns from toRow, from_* columns are null
			diffRow := dsdri.createDiffRow(toRow, nil, "added")
			rows = append(rows, diffRow)
		}
	}

	// Find removed rows
	for key, fromRow := range fromRows {
		if _, exists := toRows[key]; !exists {
			// Removed row: to_* columns are null, from_* columns from fromRow
			diffRow := dsdri.createDiffRow(nil, fromRow, "removed")
			rows = append(rows, diffRow)
		}
	}

	dsdri.rows = rows
	return nil
}

func (dsdri *doltSchemasDiffRowIter) readDoltSchemasRows(tbl *doltdb.Table, root doltdb.RootValue, rowMap map[string]sql.Row) error {
	// Get the schema from the table
	sch, err := tbl.GetSchema(dsdri.ctx)
	if err != nil {
		return err
	}

	// Create a DoltTable using the database reference we have
	doltTable, err := NewDoltTable(doltdb.SchemasTableName, sch, tbl, dsdri.db, editor.Options{})
	if err != nil {
		return err
	}

	// Lock the table to this specific root
	lockedTable, err := doltTable.LockedToRoot(dsdri.ctx, root)
	if err != nil {
		return err
	}

	// Get partitions and read rows
	partitions, err := lockedTable.Partitions(dsdri.ctx)
	if err != nil {
		return err
	}

	var baseRows []sql.Row
	for {
		partition, err := partitions.Next(dsdri.ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		rowIter, err := lockedTable.PartitionRows(dsdri.ctx, partition)
		if err != nil {
			return err
		}

		for {
			row, err := rowIter.Next(dsdri.ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			baseRows = append(baseRows, row)
		}

		err = rowIter.Close(dsdri.ctx)
		if err != nil {
			return err
		}
	}

	err = partitions.Close(dsdri.ctx)
	if err != nil {
		return err
	}

	// Process each row and add to map
	for _, row := range baseRows {
		// Create key from type and name columns
		if len(row) >= 2 && row[0] != nil && row[1] != nil {
			key := strings.ToLower(row[0].(string)) + ":" + strings.ToLower(row[1].(string))
			rowMap[key] = row
		}
	}

	return nil
}

// createDiffRow creates a diff row with proper to_* and from_* column layout
func (dsdri *doltSchemasDiffRowIter) createDiffRow(toRow, fromRow sql.Row, diffType string) sql.Row {
	// Expected schema: 7 to_* columns + 7 from_* columns + 1 diff_type = 15 columns
	row := make(sql.Row, 15)
	
	// TO columns (indices 0-6)
	if toRow != nil && len(toRow) >= 5 {
		copy(row[0:5], toRow[0:5])  // to_type, to_name, to_fragment, to_extra, to_sql_mode
		row[5] = dsdri.toRef      // to_commit
		row[6] = nil              // to_commit_date (we'll set this later if needed)
	} else {
		// All to_* columns are null for removed rows
		for i := 0; i < 7; i++ {
			row[i] = nil
		}
	}
	
	// FROM columns (indices 7-13)
	if fromRow != nil && len(fromRow) >= 5 {
		copy(row[7:12], fromRow[0:5])  // from_type, from_name, from_fragment, from_extra, from_sql_mode
		row[12] = dsdri.fromRef       // from_commit
		row[13] = nil                 // from_commit_date (we'll set this later if needed)
	} else {
		// All from_* columns are null for added rows
		for i := 7; i < 14; i++ {
			row[i] = nil
		}
	}
	
	// Diff type column (index 14)
	row[14] = diffType
	
	return row
}

// rowsEqual compares two SQL rows for equality
func rowsEqual(row1, row2 sql.Row) bool {
	if len(row1) != len(row2) {
		return false
	}

	for i, val1 := range row1 {
		val2 := row2[i]
		if val1 == nil && val2 == nil {
			continue
		}
		if val1 == nil || val2 == nil {
			return false
		}
		if val1 != val2 {
			return false
		}
	}

	return true
}

func (dsdri *doltSchemasDiffRowIter) Close(ctx *sql.Context) error {
	return nil
}

