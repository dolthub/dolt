// Copyright 2022 Dolthub, Inc.
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

package writer

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
)

type nomsFkIndexer struct {
	writer  *nomsTableWriter
	idxName string
	idxSch  schema.Schema
	nrr     *noms.ReadRange
}

var _ sql.Table = nomsFkIndexer{}

func (n nomsFkIndexer) Name() string {
	return n.writer.tableName
}

func (n nomsFkIndexer) String() string {
	return n.writer.tableName
}

func (n nomsFkIndexer) Schema() sql.Schema {
	return n.writer.sqlSch
}

func (n nomsFkIndexer) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(fkDummyPartition{}), nil
}

func (n nomsFkIndexer) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	dRows, err := n.writer.tableEditor.GetIndexedRows(ctx, n.nrr.Start, n.idxName, n.idxSch)
	if err != nil {
		return nil, err
	}
	rows := make([]sql.Row, len(dRows))
	for i, dRow := range dRows {
		rows[i], err = sqlutil.DoltRowToSqlRow(dRow, n.writer.sch)
		if err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(rows...), err
}

// fkDummyPartition is used to return a partition that will be ignored, as a foreign key indexer does not handle
// partitioning, however a partition must be used in order to retrieve rows.
type fkDummyPartition struct{}

var _ sql.Partition = fkDummyPartition{}

// Key implements the interface sql.Partition.
func (n fkDummyPartition) Key() []byte {
	return nil
}
