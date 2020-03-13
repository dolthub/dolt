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
	"fmt"
	"strings"
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
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
			query:         "create table testTable (id int primary key comment 'tag:101')",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 101, sql.Int32, true, schema.NotNullConstraint{})),
		},
		{
			name:          "Test create two column schema",
			query:         "create table testTable (id int primary key comment 'tag:102', age int comment 'tag:103')",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 102, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 103, sql.Int32, false)),
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
			name:        "Test reserved table name",
			query:       "create table dolt_table (id int primary key, age int)",
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
			query:         `create table testTable (
								id int primary key comment 'tag:104', 
								age int comment 'tag:105', 
								first_name varchar(255) comment 'tag:106', 
								is_married boolean comment 'tag:107') `,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 104, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 105, sql.Int32, false),
				schemaNewColumn(t, "first_name", 106, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false),
				schemaNewColumn(t, "is_married", 107, sql.Boolean, false)),
		},
		{
			name:          "Test all supported types",
			expectedTable: "testTable",
			query: `create table testTable (
							c0 int primary key comment 'tag:200',
							c1 tinyint comment 'tag:201',
							c2 smallint comment 'tag:202',
							c3 mediumint comment 'tag:203',
							c4 integer comment 'tag:204',
							c5 bigint comment 'tag:205',
							c6 bool comment 'tag:206',
							c7 boolean comment 'tag:207',
							c8 bit(10) comment 'tag:208',
							c9 text comment 'tag:209',
							c10 tinytext comment 'tag:210',
							c11 mediumtext comment 'tag:211',
							c12 longtext comment 'tag:212',
							c16 char(5) comment 'tag:216',
							c17 varchar(255) comment 'tag:217',
							c18 varchar(80) comment 'tag:218',
							c19 float comment 'tag:219',
							c20 double comment 'tag:220',
							c22 int unsigned comment 'tag:222',
							c23 tinyint unsigned comment 'tag:223',
							c24 smallint unsigned comment 'tag:224',
							c25 mediumint unsigned comment 'tag:225',
							c26 bigint unsigned comment 'tag:226')`,
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "c0", 200, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "c1", 201, sql.Int8, false),
				schemaNewColumn(t, "c2", 202, sql.Int16, false),
				schemaNewColumn(t, "c3", 203, sql.Int24, false),
				schemaNewColumn(t, "c4", 204, sql.Int32, false),
				schemaNewColumn(t, "c5", 205, sql.Int64, false),
				schemaNewColumn(t, "c6", 206, sql.Boolean, false),
				schemaNewColumn(t, "c7", 207, sql.Boolean, false),
				schemaNewColumn(t, "c8", 208, sql.MustCreateBitType(10), false),
				schemaNewColumn(t, "c9", 209, sql.Text, false),
				schemaNewColumn(t, "c10", 210, sql.TinyText, false),
				schemaNewColumn(t, "c11", 211, sql.MediumText, false),
				schemaNewColumn(t, "c12", 212, sql.LongText, false),
				//schemaNewColumn(t, "c13", 13, sql.TinyBlob, false),
				//schemaNewColumn(t, "c14", 14, sql.Blob, false),
				//schemaNewColumn(t, "c15", 15, sql.LongBlob, false),
				schemaNewColumn(t, "c16", 216, sql.MustCreateStringWithDefaults(sqltypes.Char, 5), false),
				schemaNewColumn(t, "c17", 217, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false),
				schemaNewColumn(t, "c18", 218, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "c19", 219, sql.Float32, false),
				schemaNewColumn(t, "c20", 220, sql.Float64, false),
				//schemaNewColumn(t, "c21", 21, sql.MustCreateDecimalType(10, 5), false),
				schemaNewColumn(t, "c22", 222, sql.Uint32, false),
				schemaNewColumn(t, "c23", 223, sql.Uint8, false),
				schemaNewColumn(t, "c24", 224, sql.Uint16, false),
				schemaNewColumn(t, "c25", 225, sql.Uint24, false),
				schemaNewColumn(t, "c26", 226, sql.Uint64, false),
			),
		},
		{
			name:          "Test primary keys",
			query:         `create table testTable (
								id int comment 'tag:300', 
								age int comment 'tag:301', 
								first_name varchar(80) comment 'tag:302', 
								is_married bool comment 'tag:303', 
								primary key (id, age))`,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 300, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 301, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "first_name", 302, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "is_married", 303, sql.Boolean, false)),
		},
		{
			name:          "Test not null constraints",
			query:         `create table testTable (
								id int comment 'tag:304', 
								age int comment 'tag:305', 
								first_name varchar(80) not null comment 'tag:306', 
								is_married bool comment 'tag:307', 
								primary key (id, age))`,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 304, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 305, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "first_name", 306, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "is_married", 307, sql.Boolean, false)),
		},
		{
			name:          "Test quoted columns",
			query:         "create table testTable (" +
								"`id` int comment 'tag:308', " +
								"`age` int comment 'tag:309', " +
								"`timestamp` varchar(80) comment 'tag:310', " +
								"`is married` bool comment 'tag:311', " +
								"primary key (`id`, `age`))",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 308, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 309, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "timestamp", 310, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "is married", 311, sql.Boolean, false)),
		},
		{
			name:          "Test tag comments",
			query:         `create table testTable (
								id int primary key comment 'tag:5', age int comment 'tag:10')`,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 5, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 10, sql.Int32, false)),
		},
		//{
		//	name:          "Test faulty tag comments",
		//	query:         `create table testTable (
		//						id int primary key comment 'tag:a', age int comment 'this is my personal area')`,
		//	expectedTable: "testTable",
		//	expectedSchema: dtestutils.CreateSchema(
		//		schemaNewColumn(t, "id", 0, sql.Int32, true, schema.NotNullConstraint{}),
		//		schemaNewColumn(t, "age", 1, sql.Int32, false)),
		//},
		// Real world examples for regression testing
		{
			name: "Test ip2nation",
			query: `CREATE TABLE ip2nation (
							ip int(11) unsigned NOT NULL default 0 comment 'tag:400',
							country char(2) NOT NULL default '' comment 'tag:401',
							PRIMARY KEY (ip));`,
			expectedTable: "ip2nation",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "ip", 400, sql.Uint32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "country", 401, sql.MustCreateStringWithDefaults(sqltypes.Char, 2), false, schema.NotNullConstraint{})),
		},
		{
			name:          "Test ip2nationCountries",
			expectedTable: "ip2nationCountries",
			query: `CREATE TABLE ip2nationCountries (
							code varchar(4) NOT NULL default '' COMMENT 'tag:410',
							iso_code_2 varchar(2) NOT NULL default '' COMMENT 'tag:411',
							iso_code_3 varchar(3) default '' COMMENT 'tag:412',
							iso_country varchar(255) NOT NULL default '' COMMENT 'tag:413',
							country varchar(255) NOT NULL default '' COMMENT 'tag:414',
							lat float NOT NULL default 0.0 COMMENT 'tag:415',
							lon float NOT NULL default 0.0 COMMENT 'tag:416',
							PRIMARY KEY (code));`,
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "code", 410, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 4), true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "iso_code_2", 411, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2), false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "iso_code_3", 412, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 3), false),
				schemaNewColumn(t, "iso_country", 413, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "country", 414, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "lat", 415, sql.Float32, false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "lon", 416, sql.Float32, false, schema.NotNullConstraint{})),
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

func TestAddColumn(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:  "alter add column",
			query: "alter table people add (newColumn varchar(80) comment 'tag:100')",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 100, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false)),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 100, nil),
		},
		{
			name:  "alter add column first",
			query: "alter table people add newColumn varchar(80) comment 'tag:100' first",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "newColumn", 100, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 100, nil),
		},
		{
			name:  "alter add column middle",
			query: "alter table people add newColumn varchar(80) comment 'tag:100' after last_name",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "newColumn", 100, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 100, nil),
		},
		{
			name:  "alter add column not null",
			query: "alter table people add (newColumn varchar(80) not null default 'default' comment 'tag:100')",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 100, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 100, types.String("default")),
		},
		{
			name:  "alter add column not null with expression default",
			query: "alter table people add (newColumn int not null default 2+2/2 comment 'tag:100')",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 100, sql.Int32, false, schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 100, types.Int(3)),
		},
		{
			name:  "alter add column not null with negative expression",
			query: "alter table people add (newColumn float not null default -1.1 comment 'tag:100')",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 100, sql.Float32, false, schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 100, types.Float(float32(-1.1))),
		},
		{
			name:        "alter add column not null with type mismatch in default",
			query:       "alter table people add (newColumn float not null default 'not a number' comment 'tag:100')",
			expectedErr: "incompatible type",
		},
		{
			name:        "alter add column column not found",
			query:       "alter table people add column newColumn float comment 'tag:100' after notFound",
			expectedErr: "table people does not have column notFound",
		},
		{
			name:        "alter add column table not found",
			query:       "alter table notFound add column newColumn float comment 'tag:100'",
			expectedErr: "table not found: notFound",
		},
		{
			name:        "alter add column with tag conflict",
			query:       "alter table people add (newColumn float default 1.0 comment 'tag:1')",
			expectedErr: "A column with the tag 1 already exists",
		},
		{
			name:        "alter add column not null without default",
			query:       "alter table people add (newColumn varchar(80) not null comment 'tag:100')",
			expectedErr: "must have a non-null default value",
		},
		{
			name:  "alter add column nullable",
			query: "alter table people add (newColumn bigint comment 'tag:100')",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 100, sql.Int64, false)),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter add column with optional column keyword",
			query: "alter table people add column (newColumn varchar(80) comment 'tag:100')",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 100, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false)),
			expectedRows: AllPeopleRows,
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

			assert.NotNil(t, updatedRoot)
			table, _, err := updatedRoot.GetTable(ctx, PeopleTableName)
			assert.NoError(t, err)
			sch, err := table.GetSchema(ctx)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedSchema, sch)

			updatedTable, ok, err := updatedRoot.GetTable(ctx, "people")
			assert.NoError(t, err)
			require.True(t, ok)

			rowData, err := updatedTable.GetRowData(ctx)
			assert.NoError(t, err)
			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				r, err := row.FromNoms(tt.expectedSchema, key.(types.Tuple), value.(types.Tuple))
				assert.NoError(t, err)
				foundRows = append(foundRows, r)
				return false, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
}

func TestModifyAndChangeColumn(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:  "alter modify column reorder middle",
			query: "alter table people modify column first_name longtext not null after last_name",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter modify column reorder first",
			query: "alter table people modify column first_name longtext not null first",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter modify column drop null constraint",
			query: "alter table people modify column first_name longtext null",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter change column rename and reorder",
			query: "alter table people change first_name christian_name longtext not null after last_name",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("christian_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter change column rename and reorder first",
			query: "alter table people change column first_name christian_name longtext not null first",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("christian_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter change column drop null constraint",
			query: "alter table people change column first_name first_name longtext null",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:        "alter modify column change tag",
			query:       "alter table people modify column first_name longtext not null comment 'tag:100'",
			expectedErr: "A column with the name first_name already exists",
		},
		{
			name:        "alter modify column not null with type mismatch in default",
			query:       "alter table people modify rating double default 'not a number'",
			expectedErr: "incompatible type for default value",
		},
		{
			name:        "alter modify column with tag conflict",
			query:       "alter table people modify rating double default 1.0 comment 'tag:1'",
			expectedErr: "A column with the name rating already exists",
		},
		{
			name:        "alter modify column with type change",
			query:       "alter table people modify rating varchar(10)",
			expectedErr: "column types cannot be changed",
		},
		{
			name:        "alter modify column not null, existing null values",
			query:       "alter table people modify num_episodes bigint unsigned not null",
			expectedErr: "cannot change column to NOT NULL",
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

			assert.NotNil(t, updatedRoot)
			table, _, err := updatedRoot.GetTable(ctx, PeopleTableName)
			assert.NoError(t, err)
			sch, err := table.GetSchema(ctx)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedSchema, sch)

			updatedTable, ok, err := updatedRoot.GetTable(ctx, "people")
			assert.NoError(t, err)
			require.True(t, ok)

			rowData, err := updatedTable.GetRowData(ctx)
			assert.NoError(t, err)
			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				r, err := row.FromNoms(tt.expectedSchema, key.(types.Tuple), value.(types.Tuple))
				assert.NoError(t, err)
				foundRows = append(foundRows, r)
				return false, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
}

func TestDropColumn(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:           "alter drop column",
			query:          "alter table people drop rating",
			expectedSchema: dtestutils.RemoveColumnFromSchema(PeopleTestSchema, RatingTag),
			expectedRows:   dtestutils.ConvertToSchema(dtestutils.RemoveColumnFromSchema(PeopleTestSchema, RatingTag), AllPeopleRows...),
		},
		{
			name:           "alter drop column with optional column keyword",
			query:          "alter table people drop column rating",
			expectedSchema: dtestutils.RemoveColumnFromSchema(PeopleTestSchema, RatingTag),
			expectedRows:   dtestutils.ConvertToSchema(dtestutils.RemoveColumnFromSchema(PeopleTestSchema, RatingTag), AllPeopleRows...),
		},
		{
			name:        "drop primary key",
			query:       "alter table people drop column id",
			expectedErr: "Cannot drop column in primary key",
		},
		{
			name:        "table not found",
			query:       "alter table notFound drop column id",
			expectedErr: "table not found: notFound",
		},
		{
			name:        "column not found",
			query:       "alter table people drop column notFound",
			expectedErr: "table people does not have column notFound",
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
			table, _, err := updatedRoot.GetTable(ctx, PeopleTableName)
			assert.NoError(t, err)
			sch, err := table.GetSchema(ctx)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedSchema, sch)

			updatedTable, ok, err := updatedRoot.GetTable(ctx, "people")
			assert.NoError(t, err)
			require.True(t, ok)

			rowData, err := updatedTable.GetRowData(ctx)
			assert.NoError(t, err)
			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				updatedSch, err := updatedTable.GetSchema(ctx)
				assert.NoError(t, err)
				r, err := row.FromNoms(updatedSch, key.(types.Tuple), value.(types.Tuple))
				assert.NoError(t, err)
				foundRows = append(foundRows, r)
				return false, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
}

func TestRenameColumn(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:  "alter rename column with column and as keywords",
			query: "alter table people rename column rating as newRating",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("newRating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter rename column with column and to keyword",
			query: "alter table people rename column rating to newRating",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("newRating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter rename primary key column",
			query: "alter table people rename column id to newId",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("newId", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.BoolKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.UUIDKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:        "table not found",
			query:       "alter table notFound rename column id to newId",
			expectedErr: "table not found: notFound",
		},
		{
			name:        "column not found",
			query:       "alter table people rename column notFound to newNotFound",
			expectedErr: "table people does not have column notFound",
		},
		{
			name:        "column name collision",
			query:       "alter table people rename column id to age",
			expectedErr: "A column with the name age already exists",
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
			table, _, err := updatedRoot.GetTable(ctx, PeopleTableName)
			assert.NoError(t, err)
			sch, err := table.GetSchema(ctx)
			assert.Equal(t, tt.expectedSchema, sch)

			updatedTable, ok, err := updatedRoot.GetTable(ctx, "people")
			assert.NoError(t, err)
			require.True(t, ok)

			rowData, err := updatedTable.GetRowData(ctx)
			assert.NoError(t, err)
			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				updatedSch, err := updatedTable.GetSchema(ctx)
				assert.NoError(t, err)
				r, err := row.FromNoms(updatedSch, key.(types.Tuple), value.(types.Tuple))
				assert.NoError(t, err)
				foundRows = append(foundRows, r)
				return false, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedRows, foundRows)
		})
	}
}

func TestRenameTable(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		oldTableName   string
		newTableName   string
		expectedSchema schema.Schema
		expectedRows   []row.Row
		expectedErr    string
	}{
		{
			name:           "alter rename table",
			query:          "rename table people to newPeople",
			oldTableName:   "people",
			newTableName:   "newPeople",
			expectedSchema: PeopleTestSchema,
			expectedRows:   AllPeopleRows,
		},
		{
			name:           "alter rename table with alter syntax",
			query:          "alter table people rename to newPeople",
			oldTableName:   "people",
			newTableName:   "newPeople",
			expectedSchema: PeopleTestSchema,
			expectedRows:   AllPeopleRows,
		},
		{
			name:           "rename multiple tables",
			query:          "rename table people to newPeople, appearances to newAppearances",
			oldTableName:   "appearances",
			newTableName:   "newAppearances",
			expectedSchema: AppearancesTestSchema,
			expectedRows:   AllAppsRows,
		},
		{
			name:        "table not found",
			query:       "rename table notFound to newNowFound",
			expectedErr: "table not found: notFound",
		},
		{
			name:        "invalid table name",
			query:       "rename table people to `123`",
			expectedErr: "Invalid table name",
		},
		{
			name:        "reserved table name",
			query:       "rename table people to dolt_table",
			expectedErr: "Invalid table name",
		},
		{
			name:        "table name in use",
			query:       "rename table people to appearances",
			expectedErr: "table already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			ctx := context.Background()
			root, _ := dEnv.WorkingRoot(ctx)

			updatedRoot, err := ExecuteSql(dEnv, root, tt.query)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}
			require.NotNil(t, updatedRoot)

			has, err := updatedRoot.HasTable(ctx, tt.oldTableName)
			require.NoError(t, err)
			assert.False(t, has)
			newTable, ok, err := updatedRoot.GetTable(ctx, tt.newTableName)
			require.NoError(t, err)
			require.True(t, ok)

			sch, err := newTable.GetSchema(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expectedSchema, sch)

			rowData, err := newTable.GetRowData(ctx)
			require.NoError(t, err)
			var foundRows []row.Row
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				r, err := row.FromNoms(tt.expectedSchema, key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				foundRows = append(foundRows, r)
				return false, nil
			})

			require.NoError(t, err)

			// Some test cases deal with rows declared in a different order than noms returns them, so use an order-
			// insensitive comparison here.
			assert.ElementsMatch(t, tt.expectedRows, foundRows)
		})
	}
}

func TestAlterSystemTables(t *testing.T) {
	systemTableNames := []string{"dolt_docs", "dolt_log", "dolt_history_people", "dolt_diff_people"}
	reservedTableNames := []string{"dolt_schemas", "dolt_query_catalog"}

	dEnv := dtestutils.CreateTestEnv()
	CreateTestDatabase(dEnv, t)

	t.Run("Create", func(t *testing.T) {
		for _, tableName := range append(systemTableNames, reservedTableNames...) {
			assertFails(t, dEnv, fmt.Sprintf("create table %s (a int primary key not null)", tableName), "reserved")
		}
	})

	dtestutils.CreateTestTable(t, dEnv, "dolt_docs",
		env.DoltDocsSchema,
		NewRow(types.String("LICENSE.md"), types.String("A license")))
	dtestutils.CreateTestTable(t, dEnv, doltdb.DoltQueryCatalogTableName,
		DoltQueryCatalogSchema,
		NewRow(types.String("abc123"), types.Uint(1), types.String("example"), types.String("select 2+2 from dual"), types.String("description")))
	dtestutils.CreateTestTable(t, dEnv, doltdb.SchemasTableName,
		schemasTableDoltSchema(),
		NewRowWithPks([]types.Value{types.String("view"), types.String("name")}, types.String("select 2+2 from dual")))

	// The _history and _diff tables give not found errors right now because of https://github.com/liquidata-inc/dolt/issues/373.
	// We can remove the divergent failure logic when the issue is fixed.
	t.Run("Drop", func(t *testing.T) {
		for _, tableName := range systemTableNames {
			expectedErr := "system table"
			if strings.HasPrefix(tableName, "dolt_diff") || strings.HasPrefix(tableName, "dolt_history") {
				expectedErr = "system tables cannot be dropped or altered"
			}
			assertFails(t, dEnv, fmt.Sprintf("drop table %s", tableName), expectedErr)
		}
		for _, tableName := range reservedTableNames {
			assertSucceeds(t, dEnv, fmt.Sprintf("drop table %s", tableName))
		}
	})

	t.Run("Rename", func(t *testing.T) {
		for _, tableName := range systemTableNames {
			expectedErr := "system table"
			if strings.HasPrefix(tableName, "dolt_diff") || strings.HasPrefix(tableName, "dolt_history") {
				expectedErr = "system tables cannot be dropped or altered"
			}
			assertFails(t, dEnv, fmt.Sprintf("rename table %s to newname", tableName), expectedErr)
		}
		for _, tableName := range reservedTableNames {
			assertSucceeds(t, dEnv, fmt.Sprintf("rename table %s to newname", tableName))
		}
	})

	t.Run("Alter", func(t *testing.T) {
		for _, tableName := range append(systemTableNames, reservedTableNames...) {
			expectedErr := "cannot be altered"
			if strings.HasPrefix(tableName, "dolt_diff") || strings.HasPrefix(tableName, "dolt_history") {
				expectedErr = " cannot be altered"
			}
			assertFails(t, dEnv, fmt.Sprintf("alter table %s add column a int", tableName), expectedErr)
		}
	})
}

func schemasTableDoltSchema() schema.Schema {
	// this is a dummy test environment and will not be used,
	// dolt_schema table tags will be parsed from the comments in SchemaTableSchema()
	testEnv := dtestutils.CreateTestEnv()
	return mustGetDoltSchema(SchemasTableSchema(), testEnv)
}

func assertFails(t *testing.T, dEnv *env.DoltEnv, query, expectedErr string) {
	ctx := context.Background()
	root, _ := dEnv.WorkingRoot(ctx)
	_, err := ExecuteSql(dEnv, root, query)
	require.Error(t, err, query)
	assert.Contains(t, err.Error(), expectedErr)
}

func assertSucceeds(t *testing.T, dEnv *env.DoltEnv, query string) {
	ctx := context.Background()
	root, _ := dEnv.WorkingRoot(ctx)
	_, err := ExecuteSql(dEnv, root, query)
	assert.NoError(t, err, query)
}
