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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/conflict"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

var _ sql.Table = (*TableOfTablesInConflict)(nil)

// TableOfTablesInConflict is a sql.Table implementation that implements a system table which shows the current conflicts
type TableOfTablesInConflict struct {
	ddb  *doltdb.DoltDB
	root *doltdb.RootValue
}

// NewTableOfTablesInConflict creates a TableOfTablesInConflict
func NewTableOfTablesInConflict(_ *sql.Context, ddb *doltdb.DoltDB, root *doltdb.RootValue) sql.Table {
	return &TableOfTablesInConflict{ddb: ddb, root: root}
}

// Name is a sql.Table interface function which returns the name of the table which is defined by the constant
// TableOfTablesInConflictName
func (dt *TableOfTablesInConflict) Name() string {
	return doltdb.TableOfTablesInConflictName
}

// String is a sql.Table interface function which returns the name of the table which is defined by the constant
// TableOfTablesInConflictName
func (dt *TableOfTablesInConflict) String() string {
	return doltdb.TableOfTablesInConflictName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the log system table.
func (dt *TableOfTablesInConflict) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "table", Type: sql.Text, Source: doltdb.TableOfTablesInConflictName, PrimaryKey: true},
		{Name: "num_conflicts", Type: sql.Uint64, Source: doltdb.TableOfTablesInConflictName, PrimaryKey: false},
	}
}

type tableInConflict struct {
	name    string
	size    uint64
	done    bool
	schemas conflict.ConflictSchema
	//cnfItr types.MapIterator
}

// Key returns a unique key for the partition
func (p *tableInConflict) Key() []byte {
	return []byte(p.name)
}

// Next retrieves the next row. It will return io.EOF if it's the last row.
// After retrieving the last row, Close will be automatically closed.
func (p *tableInConflict) Next(*sql.Context) (sql.Row, error) {
	if p.done {
		return nil, io.EOF
	}

	p.done = true
	return sql.NewRow(p.name, p.size), nil
}

// Close the iterator.
func (p *tableInConflict) Close(*sql.Context) error {
	return nil
}

type tablesInConflict struct {
	partitions []*tableInConflict
	pos        int
}

var _ sql.RowIter = &tableInConflict{}

// Next returns the next partition or io.EOF when done
func (p *tablesInConflict) Next(*sql.Context) (sql.Partition, error) {
	if p.pos >= len(p.partitions) {
		return nil, io.EOF
	}

	np := p.partitions[p.pos]
	p.pos++

	return np, nil
}

// Close closes the PartitionIter
func (p *tablesInConflict) Close(*sql.Context) error {
	return nil
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Conflict data is partitioned by table.
func (dt *TableOfTablesInConflict) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	tblNames, err := dt.root.TablesInConflict(ctx)

	if err != nil {
		return nil, err
	}

	var partitions []*tableInConflict
	for _, tblName := range tblNames {
		tbl, ok, err := dt.root.GetTable(ctx, tblName)

		if err != nil {
			return nil, err
		} else if ok {
			schemas, m, err := tbl.GetConflicts(ctx)

			if err != nil {
				return nil, err
			}

			partitions = append(partitions, &tableInConflict{tblName, m.Len(), false, schemas})
		}
	}

	return &tablesInConflict{partitions: partitions}, nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (dt *TableOfTablesInConflict) PartitionRows(_ *sql.Context, part sql.Partition) (sql.RowIter, error) {
	cp := part.(*tableInConflict)
	return cp, nil
}
