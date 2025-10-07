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

func doltNonlocalTablesSchema() sql.Schema {
	return []*sql.Column{
		{Name: doltdb.NonlocalTableTableNameCol, Type: sqlTypes.VarChar, Source: doltdb.GetNonlocalTablesTableName(), PrimaryKey: true},
		{Name: doltdb.NonlocalTableRefCol, Type: sqlTypes.VarChar, Source: doltdb.GetNonlocalTablesTableName(), Nullable: true},
		{Name: doltdb.NonlocalTablesRefTableCol, Type: sqlTypes.VarChar, Source: doltdb.GetNonlocalTablesTableName(), Nullable: true},
		{Name: doltdb.NonlocalTablesOptionsCol, Type: sqlTypes.VarChar, Source: doltdb.GetNonlocalTablesTableName(), Nullable: true},
	}
}

var GetDoltNonlocalTablesSchema = doltNonlocalTablesSchema

// NewNonlocallTablesTable creates a new dolt_table_aliases table
func NewNonlocallTablesTable(_ *sql.Context, backingTable VersionableTable) sql.Table {
	return &UserSpaceSystemTable{
		backingTable: backingTable,
		tableName:    getDoltNonlocalTablesName(),
		schema:       GetDoltNonlocalTablesSchema(),
	}
}

// NewEmptyNonlocalTablesTable creates an empty dolt_table_aliases table
func NewEmptyNonlocalTablesTable(_ *sql.Context) sql.Table {
	return &UserSpaceSystemTable{
		tableName: getDoltNonlocalTablesName(),
		schema:    GetDoltNonlocalTablesSchema(),
	}
}

func getDoltNonlocalTablesName() doltdb.TableName {
	if resolve.UseSearchPath {
		return doltdb.TableName{Schema: doltdb.DoltNamespace, Name: doltdb.GetNonlocalTablesTableName()}
	}
	return doltdb.TableName{Name: doltdb.GetNonlocalTablesTableName()}
}
