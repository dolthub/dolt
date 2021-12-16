// Copyright 2021 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// TableOfTablesWithViolations is a sql.Table implementation that implements a system table which shows the
// tables that contain constraint violations.
type TableOfTablesWithViolations struct {
	root *doltdb.RootValue
}

var _ sql.Table = (*TableOfTablesWithViolations)(nil)

// NewTableOfTablesConstraintViolations creates a TableOfTablesWithViolations.
func NewTableOfTablesConstraintViolations(ctx *sql.Context, root *doltdb.RootValue) sql.Table {
	return &TableOfTablesWithViolations{root: root}
}

// Name implements the interface sql.Table.
func (totwv *TableOfTablesWithViolations) Name() string {
	return doltdb.TableOfTablesWithViolationsName
}

// String implements the interface sql.Table.
func (totwv *TableOfTablesWithViolations) String() string {
	return doltdb.TableOfTablesWithViolationsName
}

// Schema implements the interface sql.Table.
func (totwv *TableOfTablesWithViolations) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "table", Type: sql.Text, Source: doltdb.TableOfTablesWithViolationsName, PrimaryKey: true},
		{Name: "num_violations", Type: sql.Uint64, Source: doltdb.TableOfTablesWithViolationsName, PrimaryKey: false},
	}
}

// Partitions implements the interface sql.Table.
func (totwv *TableOfTablesWithViolations) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	tblNames, err := totwv.root.TablesWithConstraintViolations(ctx)
	if err != nil {
		return nil, err
	}
	tblPartitions := make([]tableOfTablesPartition, len(tblNames))
	for i := range tblNames {
		tblPartitions[i] = tableOfTablesPartition(tblNames[i])
	}
	return &tableOfTablesPartitionIter{
		idx:      0,
		tblNames: tblPartitions,
	}, nil
}

// PartitionRows implements the interface sql.Table.
func (totwv *TableOfTablesWithViolations) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	tblName := string(part.Key())
	var rows []sql.Row
	tbl, _, ok, err := totwv.root.GetTableInsensitive(ctx, tblName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("TablesWithConstraintViolations returned %s but it cannot be found", tblName)
	}
	data, err := tbl.GetConstraintViolations(ctx)
	if err != nil {
		return nil, err
	}
	rows = append(rows, sql.Row{tblName, data.Len()})
	return sql.RowsToRowIter(rows...), nil
}

// tableOfTablesPartitionIter is the partition iterator for TableOfTablesWithViolations.
type tableOfTablesPartitionIter struct {
	idx      int
	tblNames []tableOfTablesPartition
}

var _ sql.PartitionIter = (*tableOfTablesPartitionIter)(nil)

// Next implements the interface sql.PartitionIter.
func (t *tableOfTablesPartitionIter) Next(*sql.Context) (sql.Partition, error) {
	if t.idx >= len(t.tblNames) {
		return nil, io.EOF
	}
	nextTable := t.tblNames[t.idx]
	t.idx++
	return nextTable, nil
}

// Close implements the interface sql.PartitionIter.
func (t *tableOfTablesPartitionIter) Close(context *sql.Context) error {
	return nil
}

// tableOfTablesPartition is a partition returned from tableOfTablesPartitionIter, which is just a table name.
type tableOfTablesPartition string

var _ sql.Partition = tableOfTablesPartition("")

// Key implements the interface sql.Partition.
func (t tableOfTablesPartition) Key() []byte {
	return []byte(t)
}
