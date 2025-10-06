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
	"github.com/dolthub/go-mysql-server/sql"
	sqlTypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
)

func doltGlobalTablesSchema() sql.Schema {
	return []*sql.Column{
		{Name: doltdb.GlobalTableTableNameCol, Type: sqlTypes.VarChar, Source: doltdb.GetGlobalTablesTableName(), PrimaryKey: true},
		{Name: doltdb.GlobalTablesRefCol, Type: sqlTypes.VarChar, Source: doltdb.GetGlobalTablesTableName(), Nullable: true},
		{Name: doltdb.GlobalTablesRefTableCol, Type: sqlTypes.VarChar, Source: doltdb.GetGlobalTablesTableName(), Nullable: true},
		{Name: doltdb.GlobalTablesOptionsCol, Type: sqlTypes.VarChar, Source: doltdb.GetGlobalTablesTableName(), Nullable: true},
	}
}

var GetDoltGlobalTablesSchema = doltGlobalTablesSchema

// NewGlobalTablesTable creates a
func NewGlobalTablesTable(_ *sql.Context, backingTable VersionableTable) sql.Table {
	return &BackedSystemTable{
		backingTable: backingTable,
		tableName:    getDoltGlobalTablesName(),
		schema:       GetDoltGlobalTablesSchema(),
	}
}

// NewEmptyGlobalTablesTable creates an empty
func NewEmptyGlobalTablesTable(_ *sql.Context) sql.Table {
	return &BackedSystemTable{
		tableName: getDoltGlobalTablesName(),
		schema:    GetDoltGlobalTablesSchema(),
	}
}

func getDoltGlobalTablesName() doltdb.TableName {
	if resolve.UseSearchPath {
		return doltdb.TableName{Schema: doltdb.DoltNamespace, Name: doltdb.GetGlobalTablesTableName()}
	}
	return doltdb.TableName{Name: doltdb.GetGlobalTablesTableName()}
}
