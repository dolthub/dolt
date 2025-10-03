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

func doltTestsSchema() sql.Schema {
	return []*sql.Column{
		{Name: "test_name", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: true},
		{Name: "test_group", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: false, Nullable: true},
		{Name: "test_query", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: false, Nullable: false},
		{Name: "assertion_type", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: false, Nullable: false},
		{Name: "assertion_comparator", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: false, Nullable: false},
		{Name: "assertion_value", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: false, Nullable: true},
	}
}

// GetDoltTestsSchema returns the schema of the dolt_tests system table. This is used
// by Doltgres to update the dolt_tests schema using Doltgres types.
var GetDoltTestsSchema = doltTestsSchema

// NewTestsTable creates a TestsTable
func NewTestsTable(_ *sql.Context, backingTable VersionableTable) sql.Table {
	return &BackedSystemTable{
		backingTable: backingTable,
		tableName:    getDoltTestsTableName(),
		schema:       GetDoltTestsSchema(),
	}
}

// NewEmptyTestsTable creates an empty TestsTable
func NewEmptyTestsTable(_ *sql.Context) sql.Table {
	return &DocsTable{
		BackedSystemTable: BackedSystemTable{
			tableName: getDoltTestsTableName(),
			schema:    GetDoltTestsSchema(),
		},
	}
}

func getDoltTestsTableName() doltdb.TableName {
	if resolve.UseSearchPath {
		return doltdb.TableName{Schema: doltdb.DoltNamespace, Name: doltdb.TestsTableName}
	}
	return doltdb.TableName{Name: doltdb.GetTestsTableName()}
}
