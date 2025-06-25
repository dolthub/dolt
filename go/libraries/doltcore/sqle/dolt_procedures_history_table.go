// Copyright 2025 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// doltProceduresHistoryTable implements the dolt_history_dolt_procedures system table
type doltProceduresHistoryTable struct {
	name string
	ddb  *doltdb.DoltDB
	head *doltdb.Commit
	db   Database // Add database reference for DoltTable creation
}

var _ sql.Table = (*doltProceduresHistoryTable)(nil)
var _ sql.PrimaryKeyTable = (*doltProceduresHistoryTable)(nil)

// DoltProceduresHistoryTable creates a dolt_procedures history table instance
func DoltProceduresHistoryTable(ddb *doltdb.DoltDB, head *doltdb.Commit, db Database) sql.Table {
	return &doltProceduresHistoryTable{
		name: doltdb.DoltHistoryTablePrefix + doltdb.ProceduresTableName,
		ddb:  ddb,
		head: head,
		db:   db,
	}
}

// Name implements sql.Table
func (dpht *doltProceduresHistoryTable) Name() string {
	return dpht.name
}

// String implements sql.Table
func (dpht *doltProceduresHistoryTable) String() string {
	return dpht.name
}

// Schema implements sql.Table
func (dpht *doltProceduresHistoryTable) Schema() sql.Schema {
	// Base schema from dolt_procedures table
	baseSch := sql.Schema{
		&sql.Column{Name: doltdb.ProceduresTableNameCol, Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_utf8mb4_0900_ai_ci), Nullable: false, PrimaryKey: true, Source: dpht.name},
		&sql.Column{Name: doltdb.ProceduresTableCreateStmtCol, Type: types.MustCreateString(sqltypes.VarChar, 4096, sql.Collation_utf8mb4_0900_ai_ci), Nullable: false, Source: dpht.name},
		&sql.Column{Name: doltdb.ProceduresTableCreatedAtCol, Type: types.Timestamp, Nullable: false, Source: dpht.name},
		&sql.Column{Name: doltdb.ProceduresTableModifiedAtCol, Type: types.Timestamp, Nullable: false, Source: dpht.name},
		&sql.Column{Name: doltdb.ProceduresTableSqlModeCol, Type: types.MustCreateString(sqltypes.VarChar, 256, sql.Collation_utf8mb4_0900_ai_ci), Nullable: true, Source: dpht.name},
	}

	// Add commit history columns
	historySch := make(sql.Schema, len(baseSch), len(baseSch)+3)
	copy(historySch, baseSch)

	historySch = append(historySch,
		&sql.Column{Name: CommitHashCol, Type: CommitHashColType, Nullable: false, PrimaryKey: true, Source: dpht.name},
		&sql.Column{Name: CommitterCol, Type: CommitterColType, Nullable: false, Source: dpht.name},
		&sql.Column{Name: CommitDateCol, Type: types.Datetime, Nullable: false, Source: dpht.name},
	)

	return historySch
}

// Collation implements sql.Table
func (dpht *doltProceduresHistoryTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements sql.Table
func (dpht *doltProceduresHistoryTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	// Use the same commit iterator pattern as HistoryTable
	cmItr := doltdb.CommitItrForRoots[*sql.Context](dpht.ddb, dpht.head)
	return &commitPartitioner{cmItr: cmItr}, nil
}

// PartitionRows implements sql.Table
func (dpht *doltProceduresHistoryTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	cp := partition.(*commitPartition)
	return &doltProceduresHistoryRowIter{
		ctx:     ctx,
		ddb:     dpht.ddb,
		commit:  cp.cm,
		history: dpht,
	}, nil
}

// PrimaryKeySchema implements sql.PrimaryKeyTable
func (dpht *doltProceduresHistoryTable) PrimaryKeySchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     dpht.Schema(),
		PkOrdinals: []int{0, 5}, // name, commit_hash
	}
}

// doltProceduresHistoryRowIter iterates through dolt_procedures rows for a single commit
type doltProceduresHistoryRowIter struct {
	ctx     *sql.Context
	ddb     *doltdb.DoltDB
	commit  *doltdb.Commit
	history *doltProceduresHistoryTable // Add reference to parent table
	rows    []sql.Row
	idx     int
}

// Next implements sql.RowIter
func (dphri *doltProceduresHistoryRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if dphri.rows == nil {
		// Initialize rows from the commit's dolt_procedures table
		err := dphri.loadRows()
		if err != nil {
			return nil, err
		}
	}

	if dphri.idx >= len(dphri.rows) {
		return nil, io.EOF
	}

	row := dphri.rows[dphri.idx]
	dphri.idx++

	return row, nil
}

func (dphri *doltProceduresHistoryRowIter) loadRows() error {
	root, err := dphri.commit.GetRootValue(dphri.ctx)
	if err != nil {
		return err
	}

	// Get the table at this commit
	tbl, ok, err := root.GetTable(dphri.ctx, doltdb.TableName{Name: doltdb.ProceduresTableName})
	if err != nil {
		return err
	}
	if !ok {
		// No dolt_procedures table in this commit, return empty rows
		dphri.rows = make([]sql.Row, 0)
		return nil
	}

	// Get commit metadata
	commitHash, err := dphri.commit.HashOf()
	if err != nil {
		return err
	}
	commitMeta, err := dphri.commit.GetCommitMeta(dphri.ctx)
	if err != nil {
		return err
	}

	// Convert commit metadata to SQL values
	commitHashStr := commitHash.String()
	committerStr := commitMeta.Name + " <" + commitMeta.Email + ">"
	commitDate := commitMeta.Time()

	// Get the schema
	sch, err := tbl.GetSchema(dphri.ctx)
	if err != nil {
		return err
	}

	// Create a DoltTable using the database reference we now have
	doltTable, err := NewDoltTable(doltdb.ProceduresTableName, sch, tbl, dphri.history.db, editor.Options{})
	if err != nil {
		return err
	}

	// Lock the table to this specific commit's root
	lockedTable, err := doltTable.LockedToRoot(dphri.ctx, root)
	if err != nil {
		return err
	}

	// Get partitions and read rows
	partitions, err := lockedTable.Partitions(dphri.ctx)
	if err != nil {
		return err
	}

	var baseRows []sql.Row
	for {
		partition, err := partitions.Next(dphri.ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		rowIter, err := lockedTable.PartitionRows(dphri.ctx, partition)
		if err != nil {
			return err
		}

		for {
			row, err := rowIter.Next(dphri.ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			baseRows = append(baseRows, row)
		}

		err = rowIter.Close(dphri.ctx)
		if err != nil {
			return err
		}
	}

	err = partitions.Close(dphri.ctx)
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

	dphri.rows = rows
	return nil
}

// Close implements sql.RowIter
func (dphri *doltProceduresHistoryRowIter) Close(ctx *sql.Context) error {
	return nil
}