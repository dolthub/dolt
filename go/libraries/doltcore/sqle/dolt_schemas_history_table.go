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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// doltSchemasHistoryTable implements the dolt_history_dolt_schemas system table
type doltSchemasHistoryTable struct {
	name string
	ddb  *doltdb.DoltDB
	head *doltdb.Commit
}

var _ sql.Table = (*doltSchemasHistoryTable)(nil)
var _ sql.PrimaryKeyTable = (*doltSchemasHistoryTable)(nil)

// NewDoltSchemasHistoryTable creates a new dolt_schemas history table instance
func NewDoltSchemasHistoryTable(ddb *doltdb.DoltDB, head *doltdb.Commit) sql.Table {
	return &doltSchemasHistoryTable{
		name: doltdb.DoltHistoryTablePrefix + doltdb.SchemasTableName,
		ddb:  ddb,
		head: head,
	}
}

// Name implements sql.Table
func (dsht *doltSchemasHistoryTable) Name() string {
	return dsht.name
}

// String implements sql.Table
func (dsht *doltSchemasHistoryTable) String() string {
	return dsht.name
}

// Schema implements sql.Table
func (dsht *doltSchemasHistoryTable) Schema() sql.Schema {
	// Base schema from dolt_schemas table
	baseSch := sql.Schema{
		&sql.Column{Name: doltdb.SchemasTablesTypeCol, Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: false, PrimaryKey: true, Source: dsht.name},
		&sql.Column{Name: doltdb.SchemasTablesNameCol, Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: false, PrimaryKey: true, Source: dsht.name},
		&sql.Column{Name: doltdb.SchemasTablesFragmentCol, Type: types.LongText, Nullable: true, Source: dsht.name},
		&sql.Column{Name: doltdb.SchemasTablesExtraCol, Type: types.JSON, Nullable: true, Source: dsht.name},
		&sql.Column{Name: doltdb.SchemasTablesSqlModeCol, Type: types.MustCreateString(sqltypes.VarChar, 256, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dsht.name},
	}

	// Add commit history columns
	historySch := make(sql.Schema, len(baseSch), len(baseSch)+3)
	copy(historySch, baseSch)
	
	historySch = append(historySch,
		&sql.Column{Name: CommitHashCol, Type: CommitHashColType, Nullable: false, PrimaryKey: true, Source: dsht.name},
		&sql.Column{Name: CommitterCol, Type: CommitterColType, Nullable: false, Source: dsht.name},
		&sql.Column{Name: CommitDateCol, Type: types.Datetime, Nullable: false, Source: dsht.name},
	)

	return historySch
}

// Collation implements sql.Table
func (dsht *doltSchemasHistoryTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements sql.Table
func (dsht *doltSchemasHistoryTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	// Use the same commit iterator pattern as HistoryTable
	cmItr := doltdb.CommitItrForRoots[*sql.Context](dsht.ddb, dsht.head)
	return &commitPartitioner{cmItr: cmItr}, nil
}

// PartitionRows implements sql.Table
func (dsht *doltSchemasHistoryTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	cp := partition.(*commitPartition)
	return &doltSchemasHistoryRowIter{
		ctx:    ctx,
		ddb:    dsht.ddb,
		commit: cp.cm,
	}, nil
}

// PrimaryKeySchema implements sql.PrimaryKeyTable
func (dsht *doltSchemasHistoryTable) PrimaryKeySchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     dsht.Schema(),
		PkOrdinals: []int{0, 1, 5}, // type, name, commit_hash
	}
}

// doltSchemasHistoryRowIter iterates through dolt_schemas rows for a single commit
type doltSchemasHistoryRowIter struct {
	ctx    *sql.Context
	ddb    *doltdb.DoltDB
	commit *doltdb.Commit
	rows   []sql.Row
	idx    int
}

func (dshri *doltSchemasHistoryRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if dshri.rows == nil {
		// Initialize rows from the commit's dolt_schemas table
		err := dshri.loadRows()
		if err != nil {
			return nil, err
		}
	}

	if dshri.idx >= len(dshri.rows) {
		return nil, io.EOF
	}

	row := dshri.rows[dshri.idx]
	dshri.idx++

	return row, nil
}

func (dshri *doltSchemasHistoryRowIter) loadRows() error {
	root, err := dshri.commit.GetRootValue(dshri.ctx)
	if err != nil {
		return err
	}

	// Check if dolt_schemas table exists in this commit
	tbl, ok, err := root.GetTable(dshri.ctx, doltdb.TableName{Name: doltdb.SchemasTableName})
	if err != nil {
		return err
	}
	if !ok {
		// No dolt_schemas table in this commit, return empty rows
		dshri.rows = make([]sql.Row, 0)
		return nil
	}

	// Get commit metadata
	commitHash, err := dshri.commit.HashOf()
	if err != nil {
		return err
	}
	commitMeta, err := dshri.commit.GetCommitMeta(dshri.ctx)
	if err != nil {
		return err
	}

	// Convert commit metadata to SQL values
	commitHashStr := commitHash.String()
	committerStr := commitMeta.Name + " <" + commitMeta.Email + ">"
	commitDate := commitMeta.Time()

	// Read the table data using SqlRowsFromDurableIndex
	sch, err := tbl.GetSchema(dshri.ctx)
	if err != nil {
		return err
	}

	rowData, err := tbl.GetRowData(dshri.ctx)
	if err != nil {
		return err
	}

	baseRows, err := SqlRowsFromDurableIndex(rowData, sch)
	if err != nil {
		return err
	}

	// Add commit metadata to each row
	rows := make([]sql.Row, 0, len(baseRows))
	for _, baseRow := range baseRows {
		// Append commit columns to the base row
		sqlRow := make(sql.Row, len(baseRow)+3)
		copy(sqlRow, baseRow)
		sqlRow[len(baseRow)] = commitHashStr
		sqlRow[len(baseRow)+1] = committerStr
		sqlRow[len(baseRow)+2] = commitDate

		rows = append(rows, sqlRow)
	}

	dshri.rows = rows
	return nil
}

func (dshri *doltSchemasHistoryRowIter) Close(ctx *sql.Context) error {
	return nil
}

