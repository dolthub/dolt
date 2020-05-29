// Copyright 2020 Liquidata, Inc.
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

	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
)

var _ sql.Table = (*ConflictsTable)(nil)

// ConflictsTable is a sql.Table implementation that implements a system table which shows the current conflicts
type ConflictsTable struct {
	dbName string
	ddb    *doltdb.DoltDB
}

// NewConflictsTable creates a ConflictsTable
func NewConflictsTable(ctx *sql.Context, dbName string) (*ConflictsTable, error) {
	ddb, ok := DSessFromSess(ctx.Session).GetDoltDB(dbName)

	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}

	return &ConflictsTable{dbName: dbName, ddb: ddb}, nil
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// ConflictsTableName
func (dt *ConflictsTable) Name() string {
	return doltdb.ConflictsTableName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// ConflictsTableName
func (dt *ConflictsTable) String() string {
	return doltdb.ConflictsTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the log system table.
func (dt *ConflictsTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "table", Type: sql.Text, Source: doltdb.ConflictsTableName, PrimaryKey: true},
		{Name: "num_conflicts", Type: sql.Uint64, Source: doltdb.ConflictsTableName, PrimaryKey: false},
	}
}

type conflictsPartition struct {
	name    string
	size    uint64
	done    bool
	schemas doltdb.Conflict
	//cnfItr types.MapIterator
}

// Key returns a unique key for the partition
func (p *conflictsPartition) Key() []byte {
	return []byte(p.name)
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (p *conflictsPartition) Next() (sql.Row, error) {
	if p.done {
		return nil, io.EOF
	}

	p.done = true
	return sql.NewRow(p.name, p.size), nil
}

// Close the iterator.
func (p *conflictsPartition) Close() error {
	return nil
}

type conflictsPartitions struct {
	partitions []*conflictsPartition
	pos        int
}

// Next returns the next partition or io.EOF when done
func (p *conflictsPartitions) Next() (sql.Partition, error) {
	if p.pos >= len(p.partitions) {
		return nil, io.EOF
	}

	np := p.partitions[p.pos]
	p.pos++

	return np, nil
}

// Close closes the PartitionIter
func (p *conflictsPartitions) Close() error {
	return nil
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Conflict data is partitioned by table.
func (dt *ConflictsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	sess := DSessFromSess(ctx.Session)
	root, ok := sess.GetRoot(dt.dbName)

	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dt.dbName)
	}

	tblNames, err := root.TablesInConflict(ctx)

	if err != nil {
		return nil, err
	}

	var partitions []*conflictsPartition
	for _, tblName := range tblNames {
		tbl, ok, err := root.GetTable(ctx, tblName)

		if err != nil {
			return nil, err
		} else if ok {
			schemas, m, err := tbl.GetConflicts(ctx)

			if err != nil {
				return nil, err
			}

			partitions = append(partitions, &conflictsPartition{tblName, m.Len(), false, schemas})
		}
	}

	return &conflictsPartitions{partitions: partitions}, nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (dt *ConflictsTable) PartitionRows(_ *sql.Context, part sql.Partition) (sql.RowIter, error) {
	cp := part.(*conflictsPartition)
	return cp, nil
}

// ConflictsItr is a sql.RowItr implementation which iterates over each commit as if it's a row in the table.
type ConflictsItr struct {
}

// NewLogItr creates a LogItr from the current environment.
func NewConflictsItr(sqlCtx *sql.Context, dbName string, ddb *doltdb.DoltDB) (*ConflictsItr, error) {
	return &ConflictsItr{}, nil
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (itr *ConflictsItr) Next() (sql.Row, error) {
	return nil, io.EOF
}

// Close closes the iterator.
func (itr *ConflictsItr) Close() error {
	return nil
}
