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
	"github.com/dolthub/go-mysql-server/sql"
	sqlTypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

func doltIgnoreSchema() sql.Schema {
	return []*sql.Column{
		{Name: "pattern", Type: sqlTypes.Text, Source: doltdb.IgnoreTableName, PrimaryKey: true},
		{Name: "ignored", Type: sqlTypes.Boolean, Source: doltdb.IgnoreTableName, PrimaryKey: false, Nullable: false},
	}
}

// GetDoltIgnoreSchema returns the schema of the dolt_ignore system table. This is used
// by Doltgres to update the dolt_ignore schema using Doltgres types.
var GetDoltIgnoreSchema = doltIgnoreSchema

// NewIgnoreTable creates an IgnoreTable
func NewIgnoreTable(_ *sql.Context, backingTable VersionableTable, schemaName string) sql.Table {
	return &BackedSystemTable{
		backingTable: backingTable,
		tableName: doltdb.TableName{
			Name:   doltdb.IgnoreTableName,
			Schema: schemaName,
		},
		schema: GetDoltIgnoreSchema(),
	}
}

// NewEmptyIgnoreTable creates an IgnoreTable
func NewEmptyIgnoreTable(_ *sql.Context, schemaName string) sql.Table {
	return &BackedSystemTable{
		tableName: doltdb.TableName{
			Name:   doltdb.IgnoreTableName,
			Schema: schemaName,
		},
		schema: GetDoltIgnoreSchema(),
	}
}
