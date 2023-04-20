// Copyright 2023 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ sql.Table = (*BranchesTable)(nil)
var _ sql.UpdatableTable = (*BranchesTable)(nil)
var _ sql.DeletableTable = (*BranchesTable)(nil)
var _ sql.InsertableTable = (*BranchesTable)(nil)
var _ sql.ReplaceableTable = (*BranchesTable)(nil)

// IgnoreTable is the system table that stores patterns for table names that should not be committed.
type IgnoreTable struct {
	ddb *doltdb.DoltDB
}

func (i *IgnoreTable) Name() string {
	return doltdb.IgnoreTableName
}

func (i *IgnoreTable) String() string {
	return doltdb.IgnoreTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the dolt_ignore system table.
func (i *IgnoreTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: "pattern", Type: types.Text, Source: doltdb.IgnoreTableName, PrimaryKey: true},
		{Name: "ignored", Type: types.Boolean, Source: doltdb.IgnoreTableName, PrimaryKey: false, Nullable: false},
	}
}

func (i *IgnoreTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.  Currently the data is unpartitioned.
func (i *IgnoreTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

func (i *IgnoreTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	//TODO implement me
	panic("implement me")
}

// NewIgnoreTable creates an IgnoreTable
func NewIgnoreTable(_ *sql.Context, ddb *doltdb.DoltDB) sql.Table {
	return &IgnoreTable{ddb: ddb}
}
