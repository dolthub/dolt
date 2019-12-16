// Copyright 2019 Liquidata, Inc.
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

package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestExecuteShow(t *testing.T) {
	peopleSchemaStr := SchemaAsCreateStmt("people", PeopleTestSchema)

	peopleSchemaRows := Rs(
		NewResultSetRow(types.String("id"), types.String("BIGINT"), types.String("NO"), types.String("PRI"), types.String("NULL"), types.String("")),
		NewResultSetRow(types.String("first_name"), types.String("LONGTEXT"), types.String("NO"), types.String(""), types.String("NULL"), types.String("")),
		NewResultSetRow(types.String("last_name"), types.String("LONGTEXT"), types.String("NO"), types.String(""), types.String("NULL"), types.String("")),
		NewResultSetRow(types.String("is_married"), types.String("BOOLEAN"), types.String("YES"), types.String(""), types.String("NULL"), types.String("")),
		NewResultSetRow(types.String("age"), types.String("BIGINT"), types.String("YES"), types.String(""), types.String("NULL"), types.String("")),
		NewResultSetRow(types.String("rating"), types.String("DOUBLE"), types.String("YES"), types.String(""), types.String("NULL"), types.String("")),
		NewResultSetRow(types.String("uuid"), types.String("LONGTEXT"), types.String("YES"), types.String(""), types.String("NULL"), types.String("")),
		NewResultSetRow(types.String("num_episodes"), types.String("BIGINT UNSIGNED"), types.String("YES"), types.String(""), types.String("NULL"), types.String("")),
	)

	tests := []struct {
		name           string
		query          string
		expectedRows   []row.Row
		expectedSchema schema.Schema
		expectedErr    string
	}{
		{
			name:  "show create table",
			query: "show create table people",
			expectedRows: Rs(
				NewResultSetRow(types.String("people"), types.String(peopleSchemaStr)),
			),
			expectedSchema: showCreateTableSchema(),
		},
		{
			name:  "show create table case insensitive",
			query: "show create table PeOPle",
			expectedRows: Rs(
				NewResultSetRow(types.String("people"), types.String(peopleSchemaStr)),
			),
			expectedSchema: showCreateTableSchema(),
		},
		{
			name:  "show tables",
			query: "show tables",
			expectedRows: Rs(
				NewResultSetRow(types.String("appearances")),
				NewResultSetRow(types.String("episodes")),
				NewResultSetRow(types.String("people")),
				NewResultSetRow(types.String("dolt_log")),
			),
			expectedSchema: showTablesSchema(),
		},
		{
			name:           "show columns from table",
			query:          "show columns from people",
			expectedRows:   peopleSchemaRows,
			expectedSchema: showColumnsSchema(),
		},
		{
			name:           "show columns from table case insensitive",
			query:          "show columns from PeOpLe",
			expectedRows:   peopleSchemaRows,
			expectedSchema: showColumnsSchema(),
		},
		{
			name:        "show columns from unknown table",
			query:       "show columns from notFound",
			expectedErr: "Unknown table: 'notFound'",
		},
		{
			name:           "describe table",
			query:          "describe people",
			expectedRows:   peopleSchemaRows,
			expectedSchema: showColumnsSchema(),
		},
		{
			name:           "describe table case insensitive",
			query:          "describe PeOpLE",
			expectedRows:   peopleSchemaRows,
			expectedSchema: showColumnsSchema(),
		},
		{
			name:        "describe unknown table",
			query:       "describe notFound",
			expectedErr: "Unknown table: 'notFound'",
		},
		{
			name:        "show databases",
			query:       "show databases",
			expectedErr: "Unsupported show statement",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			root, _ := dEnv.WorkingRoot(context.Background())

			sqlStatement, err := sqlparser.Parse(tt.query)
			if err != nil {
				assert.FailNow(t, "Couldn't parse query "+tt.query, "%v", err.Error())
			}

			if tt.expectedRows != nil && tt.expectedSchema == nil {
				require.Fail(t, "Incorrect test setup: schema must both be provided when rows are")
			}

			s := sqlStatement.(*sqlparser.Show)

			rows, sch, err := ExecuteShow(context.Background(), root, s)

			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectedRows, rows)
			assert.Equal(t, tt.expectedSchema, sch)
		})
	}
}
