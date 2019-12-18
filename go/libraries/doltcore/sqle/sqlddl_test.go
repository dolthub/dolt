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

package sqle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql/sqltestutil"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestCreateTable(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedTable  string
		expectedSchema schema.Schema
		expectedErr    string
	}{
		{
			name:          "Test create single column schema",
			query:         "create table testTable (id int primary key)",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{})),
		},
		{
			name:          "Test create two column schema",
			query:         "create table testTable (id int primary key, age int)",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, false)),
		},
		{
			name:          "Test syntax error",
			query:         "create table testTable id int, age int",
			expectedTable: "testTable",
			expectedErr:   "syntax error",
		},
		{
			name:        "Test no primary keys",
			query:       "create table testTable (id int, age int)",
			expectedErr: "no primary key columns",
		},
		{
			name:        "Test bad table name",
			query:       "create table _testTable (id int primary key, age int)",
			expectedErr: "Invalid table name",
		},
		{
			name:        "Test bad table name begins with number",
			query:       "create table 1testTable (id int primary key, age int)",
			expectedErr: "syntax error",
		},
		{
			name:        "Test in use table name",
			query:       "create table people (id int primary key, age int)",
			expectedErr: "table with name people already exists",
		},
		{
			name:           "Test in use table name with if not exists",
			query:          "create table if not exists people (id int primary key, age int)",
			expectedTable:  "people",
			expectedSchema: PeopleTestSchema,
		},
		{
			name:          "Test types",
			query:         "create table testTable (id int primary key, age int, first varchar(255), is_married boolean)",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, false),
				schema.NewColumn("first", 2, types.StringKind, false),
				schema.NewColumn("is_married", 3, types.IntKind, false)),
		},
		{
			name:          "Test all supported types",
			expectedTable: "testTable",
			query: `create table testTable (
							c0 int primary key comment 'tag:0', 
							c1 tinyint comment 'tag:1',
							c2 smallint comment 'tag:2',
							c3 mediumint comment 'tag:3',
							c4 integer comment 'tag:4',
							c5 bigint comment 'tag:5',
							c6 bool comment 'tag:6',
							c7 boolean comment 'tag:7',
							c8 bit comment 'tag:8',
							c9 text comment 'tag:9',
							c10 tinytext comment 'tag:10',
							c11 mediumtext comment 'tag:11',
							c12 longtext comment 'tag:12',
							c16 char comment 'tag:16',
							c17 varchar(255) comment 'tag:17',
							c18 varchar(80) comment 'tag:18',
							c19 float comment 'tag:19',
							c20 double comment 'tag:20',
							c22 int unsigned comment 'tag:22',
							c23 tinyint unsigned comment 'tag:23',
							c24 smallint unsigned comment 'tag:24',
							c25 mediumint unsigned comment 'tag:25',
							c26 bigint unsigned comment 'tag:26')`,
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("c0", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("c1", 1, types.IntKind, false),
				schema.NewColumn("c2", 2, types.IntKind, false),
				schema.NewColumn("c3", 3, types.IntKind, false),
				schema.NewColumn("c4", 4, types.IntKind, false),
				schema.NewColumn("c5", 5, types.IntKind, false),
				schema.NewColumn("c6", 6, types.IntKind, false),
				schema.NewColumn("c7", 7, types.IntKind, false),
				schema.NewColumn("c8", 8, types.UintKind, false),
				schema.NewColumn("c9", 9, types.StringKind, false),
				schema.NewColumn("c10", 10, types.StringKind, false),
				schema.NewColumn("c11", 11, types.StringKind, false),
				schema.NewColumn("c12", 12, types.StringKind, false),
				// TODO: add back in support for blob columns when they are supported
				// schema.NewColumn("c13", 13, types.BlobKind, false),
				// schema.NewColumn("c14", 14, types.BlobKind, false),
				// schema.NewColumn("c15", 15, types.BlobKind, false),
				schema.NewColumn("c16", 16, types.StringKind, false),
				schema.NewColumn("c17", 17, types.StringKind, false),
				schema.NewColumn("c18", 18, types.StringKind, false),
				schema.NewColumn("c19", 19, types.FloatKind, false),
				schema.NewColumn("c20", 20, types.FloatKind, false),
				// TODO: add back in support for c21 when decimal support is added
				//schema.NewColumn("c21", 21, types.FloatKind, false),
				schema.NewColumn("c22", 22, types.UintKind, false),
				schema.NewColumn("c23", 23, types.UintKind, false),
				schema.NewColumn("c24", 24, types.UintKind, false),
				schema.NewColumn("c25", 25, types.UintKind, false),
				schema.NewColumn("c26", 26, types.UintKind, false),
				// TODO: add back in support for c27 when UUID support is added
				//schema.NewColumn("c27", 27, types.UUIDKind, false),
			),
		},
		{
			name:          "Test primary keys",
			query:         "create table testTable (id int, age int, first varchar(80), is_married bool, primary key (id, age))",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first", 2, types.StringKind, false),
				schema.NewColumn("is_married", 3, types.IntKind, false)),
		},
		{
			name:          "Test not null constraints",
			query:         "create table testTable (id int, age int, first varchar(80) not null, is_married bool, primary key (id, age))",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first", 2, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", 3, types.IntKind, false)),
		},
		{
			name:          "Test quoted columns",
			query:         "create table testTable (`id` int, `age` int, `timestamp` varchar(80), `is married` bool, primary key (`id`, `age`))",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("timestamp", 2, types.StringKind, false),
				schema.NewColumn("is married", 3, types.IntKind, false)),
		},
		{
			name:          "Test tag comments",
			query:         "create table testTable (id int primary key comment 'tag:5', age int comment 'tag:10')",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", 5, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 10, types.IntKind, false)),
		},
		{
			name:          "Test faulty tag comments",
			query:         "create table testTable (id int primary key comment 'tag:a', age int comment 'this is my personal area')",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, false)),
		},
		// Real world examples for regression testing
		{
			name: "Test ip2nation",
			query: `CREATE TABLE ip2nation (
		  ip int(11) unsigned NOT NULL default 0,
		  country char(2) NOT NULL default '',
		  PRIMARY KEY (ip)
		);`,
			expectedTable: "ip2nation",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("ip", 0, types.UintKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("country", 1, types.StringKind, false, schema.NotNullConstraint{})),
		},
		{
			name:          "Test ip2nationCountries",
			expectedTable: "ip2nationCountries",
			query: `CREATE TABLE ip2nationCountries (
  code varchar(4) NOT NULL default '',
  iso_code_2 varchar(2) NOT NULL default '',
  iso_code_3 varchar(3) default '',
  iso_country varchar(255) NOT NULL default '',
  country varchar(255) NOT NULL default '',
  lat float NOT NULL default 0.0,
  lon float NOT NULL default 0.0,
  PRIMARY KEY (code)
);`,
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("code", 0, types.StringKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("iso_code_2", 1, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("iso_code_3", 2, types.StringKind, false),
				schema.NewColumn("iso_country", 3, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("country", 4, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("lat", 5, types.FloatKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("lon", 6, types.FloatKind, false, schema.NotNullConstraint{})),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			ctx := context.Background()
			root, _ := dEnv.WorkingRoot(ctx)

			updatedRoot, err := ExecuteSql(dEnv, root, tt.query)

			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}

			require.NotNil(t, updatedRoot)

			table, ok, err := updatedRoot.GetTable(ctx, tt.expectedTable)
			require.True(t, ok)
			require.NoError(t, err)

			sch, err := table.GetSchema(ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedSchema, sch)
		})
	}
}

func TestDropTable(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		tableNames  []string
		expectedErr string
	}{
		{
			name:       "drop table",
			query:      "drop table people",
			tableNames: []string{"people"},
		},
		{
			name:       "drop table case insensitive",
			query:      "drop table PEOPLE",
			tableNames: []string{"people"},
		},
		{
			name:       "drop table if exists",
			query:      "drop table if exists people",
			tableNames: []string{"people"},
		},
		{
			name:        "drop non existent",
			query:       "drop table notfound",
			expectedErr: "table not found: notfound",
		},
		{
			name:       "drop non existent if exists",
			query:      "drop table if exists notFound",
			tableNames: []string{"notFound"},
		},
		{
			name:       "drop many tables",
			query:      "drop table people, appearances, episodes",
			tableNames: []string{"people", "appearances", "episodes"},
		},
		{
			name:       "drop many tables, some don't exist",
			query:      "drop table if exists people, not_real, appearances, episodes",
			tableNames: []string{"people", "appearances", "not_real", "episodes"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			ctx := context.Background()
			root, _ := dEnv.WorkingRoot(ctx)

			updatedRoot, err := ExecuteSql(dEnv, root, tt.query)

			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}

			require.NotNil(t, updatedRoot)
			for _, tableName := range tt.tableNames {
				has, err := updatedRoot.HasTable(ctx, tableName)
				assert.NoError(t, err)
				assert.False(t, has)
			}
		})
	}
}
