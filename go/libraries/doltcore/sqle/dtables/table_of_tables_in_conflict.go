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
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var _ sql.Table = (*TableOfTablesInConflict)(nil)

// TableOfTablesInConflict is a sql.Table implementation that implements a system table which shows the current conflicts
type TableOfTablesInConflict struct {
	dbName string
	ddb    *doltdb.DoltDB
}

// NewTableOfTablesInConflict creates a TableOfTablesInConflict
func NewTableOfTablesInConflict(_ *sql.Context, dbName string, ddb *doltdb.DoltDB) sql.Table {
	return &TableOfTablesInConflict{dbName: dbName, ddb: ddb}
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
		{Name: "table", Type: types.Text, Source: doltdb.TableOfTablesInConflictName, PrimaryKey: true, DatabaseSource: dt.dbName},
		{Name: "num_conflicts", Type: types.Uint64, Source: doltdb.TableOfTablesInConflictName, PrimaryKey: false, DatabaseSource: dt.dbName},
	}
}

// Collation implements the sql.Table interface.
func (dt *TableOfTablesInConflict) Collation() sql.CollationID {
	return sql.Collation_Default
}

type tableInConflict struct {
	name string
	size uint64
	done bool
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
	sess := dsess.DSessFromSess(ctx.Session)
	ws, err := sess.WorkingSet(ctx, dt.dbName)
	if err != nil {
		return nil, err
	}

	root := ws.WorkingRoot()
	tblNames, err := doltdb.TablesWithDataConflicts(ctx, root)
	if err != nil {
		return nil, err
	}

	if ws.MergeActive() {
		schConflicts := ws.MergeState().TablesWithSchemaConflicts()
		// TODO: schema name
		tblNames = append(tblNames, doltdb.ToTableNames(schConflicts, doltdb.DefaultSchemaName)...)
	}

	var partitions []*tableInConflict
	for _, tblName := range tblNames {
		tbl, ok, err := root.GetTable(ctx, tblName)

		if err != nil {
			return nil, err
		} else if ok {
			n, err := tbl.NumRowsInConflict(ctx)
			if err != nil {
				return nil, err
			}
			partitions = append(partitions, &tableInConflict{name: tblName.Name, size: n})
		}
	}

	return &tablesInConflict{partitions: partitions}, nil
}

// PartitionRows is a sql.Table interface function that gets a row iterator for a partition
func (dt *TableOfTablesInConflict) PartitionRows(_ *sql.Context, part sql.Partition) (sql.RowIter, error) {
	cp := part.(*tableInConflict)
	return cp, nil
}
