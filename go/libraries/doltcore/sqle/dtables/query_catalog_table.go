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
	"github.com/dolthub/go-mysql-server/sql"
	sqlTypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
)

func doltQueryCatalogSchema() sql.Schema {
	return []*sql.Column{
		{Name: doltdb.QueryCatalogIdCol, Type: sqlTypes.LongText, Source: doltdb.GetQueryCatalogTableName(), PrimaryKey: true},
		{Name: doltdb.QueryCatalogOrderCol, Type: sqlTypes.Int32, Source: doltdb.GetQueryCatalogTableName(), Nullable: false},
		{Name: doltdb.QueryCatalogNameCol, Type: sqlTypes.Text, Source: doltdb.GetQueryCatalogTableName(), Nullable: false},
		{Name: doltdb.QueryCatalogQueryCol, Type: sqlTypes.Text, Source: doltdb.GetQueryCatalogTableName(), Nullable: false},
		{Name: doltdb.QueryCatalogDescriptionCol, Type: sqlTypes.Text, Source: doltdb.GetQueryCatalogTableName()},
	}
}

var GetDoltQueryCatalogSchema = doltQueryCatalogSchema

// NewQueryCatalogTable creates a QueryCatalogTable
func NewQueryCatalogTable(_ *sql.Context, backingTable VersionableTable) sql.Table {
	return &UserSpaceSystemTable{
		backingTable: backingTable,
		tableName:    getDoltQueryCatalogTableName(),
		schema:       GetDoltQueryCatalogSchema(),
	}
}

// NewEmptyQueryCatalogTable creates an empty QueryCatalogTable
func NewEmptyQueryCatalogTable(_ *sql.Context) sql.Table {
	return &UserSpaceSystemTable{
		tableName: getDoltQueryCatalogTableName(),
		schema:    GetDoltQueryCatalogSchema(),
	}
}

func getDoltQueryCatalogTableName() doltdb.TableName {
	if resolve.UseSearchPath {
		return doltdb.TableName{Schema: doltdb.DoltNamespace, Name: doltdb.GetQueryCatalogTableName()}
	}
	return doltdb.TableName{Name: doltdb.GetQueryCatalogTableName()}
}
