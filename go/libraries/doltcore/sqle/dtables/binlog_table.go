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

package dtables

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

const (
	AccessBinlogTableName    = AccessTableName + "_binlog"
	NamespaceBinlogTableName = NamespaceTableName + "_binlog"
)

// accessBinlogSchema is the schema for the "dolt_branch_control_binlog" table.
var accessBinlogSchema = sql.Schema{
	&sql.Column{
		Name:       "index",
		Type:       types.Int64,
		Source:     AccessBinlogTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "operation",
		Type:       types.MustCreateEnumType([]string{"insert", "delete"}, sql.Collation_utf8mb4_0900_bin),
		Source:     AccessBinlogTableName,
		PrimaryKey: false,
	},
	&sql.Column{
		Name:       "branch",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     AccessBinlogTableName,
		PrimaryKey: false,
	},
	&sql.Column{
		Name:       "user",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_bin),
		Source:     AccessBinlogTableName,
		PrimaryKey: false,
	},
	&sql.Column{
		Name:       "host",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     AccessBinlogTableName,
		PrimaryKey: false,
	},
	&sql.Column{
		Name:       "permissions",
		Type:       types.MustCreateSetType(PermissionsStrings, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     AccessBinlogTableName,
		PrimaryKey: false,
	},
}

// namespaceBinlogSchema is the schema for the "dolt_branch_namespace_control_binlog" table.
var namespaceBinlogSchema = sql.Schema{
	&sql.Column{
		Name:       "index",
		Type:       types.Int64,
		Source:     NamespaceBinlogTableName,
		PrimaryKey: true,
	},
	&sql.Column{
		Name:       "operation",
		Type:       types.MustCreateEnumType([]string{"insert", "delete"}, sql.Collation_utf8mb4_0900_bin),
		Source:     NamespaceBinlogTableName,
		PrimaryKey: false,
	},
	&sql.Column{
		Name:       "branch",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     NamespaceBinlogTableName,
		PrimaryKey: false,
	},
	&sql.Column{
		Name:       "user",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_bin),
		Source:     NamespaceBinlogTableName,
		PrimaryKey: false,
	},
	&sql.Column{
		Name:       "host",
		Type:       types.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_utf8mb4_0900_ai_ci),
		Source:     NamespaceBinlogTableName,
		PrimaryKey: false,
	},
}

// BinlogTable provides a queryable view over the Binlog.
type BinlogTable struct {
	Log      *branch_control.Binlog
	IsAccess bool
}

var _ sql.Table = BinlogTable{}

// Name implements the interface sql.Table.
func (b BinlogTable) Name() string {
	if b.IsAccess {
		return AccessBinlogTableName
	} else {
		return NamespaceBinlogTableName
	}
}

// String implements the interface sql.Table.
func (b BinlogTable) String() string {
	if b.IsAccess {
		return AccessBinlogTableName
	} else {
		return NamespaceBinlogTableName
	}
}

// Schema implements the interface sql.Table.
func (b BinlogTable) Schema() sql.Schema {
	if b.IsAccess {
		return accessBinlogSchema
	} else {
		return namespaceBinlogSchema
	}
}

// Collation implements the interface sql.Table.
func (b BinlogTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions implements the interface sql.Table.
func (b BinlogTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return index.SinglePartitionIterFromNomsMap(nil), nil
}

// PartitionRows implements the interface sql.Table.
func (b BinlogTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	b.Log.RWMutex.RLock()
	defer b.Log.RWMutex.RUnlock()

	binlogRows := b.Log.Rows()
	rows := make([]sql.Row, len(binlogRows))
	for i := 0; i < len(binlogRows); i++ {
		logRow := binlogRows[i]
		operation := uint16(1)
		if !logRow.IsInsert {
			operation = 2
		}

		if b.IsAccess {
			rows[i] = sql.UntypedSqlRow{
				int64(i),
				operation,
				logRow.Branch,
				logRow.User,
				logRow.Host,
				logRow.Permissions,
			}
		} else {
			rows[i] = sql.UntypedSqlRow{
				int64(i),
				operation,
				logRow.Branch,
				logRow.User,
				logRow.Host,
			}
		}
	}
	return sql.RowsToRowIter(rows...), nil
}
