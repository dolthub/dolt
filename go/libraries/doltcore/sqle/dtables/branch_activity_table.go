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

package dtables

import (
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var _ sql.Table = (*BranchActivityTable)(nil)

// BranchActivityTable is a read-only system table that tracks branch activity
type BranchActivityTable struct {
	db        dsess.SqlDatabase
	tableName string
}

func NewBranchActivityTable(_ *sql.Context, db dsess.SqlDatabase) sql.Table {
	return &BranchActivityTable{db: db, tableName: doltdb.BranchActivityTableName}
}

func (bat *BranchActivityTable) Name() string {
	return bat.tableName
}

func (bat *BranchActivityTable) String() string {
	return bat.tableName
}

func (bat *BranchActivityTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "branch", Type: types.Text, Source: bat.tableName, PrimaryKey: true, Nullable: false, DatabaseSource: bat.db.Name()},
		{Name: "last_read", Type: types.Datetime, Source: bat.tableName, PrimaryKey: false, Nullable: false, DatabaseSource: bat.db.Name()},
		{Name: "last_write", Type: types.Datetime, Source: bat.tableName, PrimaryKey: false, Nullable: false, DatabaseSource: bat.db.Name()},
		{Name: "system_start_time", Type: types.Datetime, Source: bat.tableName, PrimaryKey: false, Nullable: false, DatabaseSource: bat.db.Name()},
	}
}

func (bat *BranchActivityTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (bat *BranchActivityTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (bat *BranchActivityTable) PartitionRows(sqlCtx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	return NewBranchActivityItr(sqlCtx, bat)
}

type BranchActivityItr struct {
	table *BranchActivityTable
	idx   int
	rows  []sql.Row
}

func NewBranchActivityItr(ctx *sql.Context, table *BranchActivityTable) (*BranchActivityItr, error) {
	activityData := doltdb.GetBranchActivity()
	
	rows := make([]sql.Row, 0, len(activityData))
	for _, data := range activityData {
		var lastRead, lastWrite interface{}
		
		if data.LastRead != nil {
			lastRead = *data.LastRead
		} else {
			lastRead = nil
		}
		
		if data.LastWrite != nil {
			lastWrite = *data.LastWrite
		} else {
			lastWrite = nil
		}
		
		row := sql.NewRow(data.Branch, lastRead, lastWrite, data.SystemStartTime)
		rows = append(rows, row)
	}

	return &BranchActivityItr{
		table: table,
		idx:   0,
		rows:  rows,
	}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
func (itr *BranchActivityItr) Next(ctx *sql.Context) (sql.Row, error) {
	if itr.idx >= len(itr.rows) {
		return nil, io.EOF
	}

	row := itr.rows[itr.idx]
	itr.idx++
	return row, nil
}

// Close closes the iterator.
func (itr *BranchActivityItr) Close(*sql.Context) error {
	return nil
}
