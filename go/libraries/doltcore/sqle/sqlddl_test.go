// Copyright 2019 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
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
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{})),
		},
		{
			name:          "Test create two column schema",
			query:         "create table testTable (id int primary key, age int)",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, false)),
		},
		{
			name:          "Test create two column keyless schema",
			query:         "create table testTable (id int, age int)",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, false),
				schemaNewColumn(t, "age", 7208, sql.Int32, false)),
		},
		{
			name:          "Test syntax error",
			query:         "create table testTable id int, age int",
			expectedTable: "testTable",
			expectedErr:   "syntax error",
		},
		{
			name:        "Test bad table name",
			query:       "create table -testTable (id int primary key, age int)",
			expectedErr: "syntax error",
		},
		{
			name:        "Test reserved table name",
			query:       "create table dolt_table (id int primary key, age int)",
			expectedErr: "Invalid table name",
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
			name: "Test types",
			query: `create table testTable (
								id int primary key, 
								age int, 
								first_name varchar(255), 
								is_married boolean) `,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, false),
				schemaNewColumn(t, "first_name", 3264, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false),
				schemaNewColumn(t, "is_married", 14626, sql.Boolean, false)),
		},
		{
			name:          "Test all supported types",
			expectedTable: "testTable",
			query: `create table testTable (
							c0 int primary key,
							c1 tinyint,
							c2 smallint,
							c3 mediumint,
							c4 integer,
							c5 bigint,
							c6 bool,
							c7 boolean,
							c8 bit(10),
							c9 text,
							c10 tinytext,
							c11 mediumtext,
							c12 longtext,
							c16 char(5),
							c17 varchar(255),
							c18 varchar(80),
							c19 float,
							c20 double,
							c22 int unsigned,
							c23 tinyint unsigned,
							c24 smallint unsigned,
							c25 mediumint unsigned,
							c26 bigint unsigned)`,
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "c0", 594, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "c1", 601, sql.Int8, false),
				schemaNewColumn(t, "c2", 14542, sql.Int16, false),
				schemaNewColumn(t, "c3", 13309, sql.Int24, false),
				schemaNewColumn(t, "c4", 15884, sql.Int32, false),
				schemaNewColumn(t, "c5", 14619, sql.Int64, false),
				schemaNewColumn(t, "c6", 13192, sql.Boolean, false),
				schemaNewColumn(t, "c7", 5981, sql.Boolean, false),
				schemaNewColumn(t, "c8", 14871, sql.MustCreateBitType(10), false),
				schemaNewColumn(t, "c9", 4167, sql.Text, false),
				schemaNewColumn(t, "c10", 1965, sql.TinyText, false),
				schemaNewColumn(t, "c11", 12860, sql.MediumText, false),
				schemaNewColumn(t, "c12", 7155, sql.LongText, false),
				//schemaNewColumn(t, "c13", 113, sql.TinyBlob, false),
				//schemaNewColumn(t, "c14", 114, sql.Blob, false),
				//schemaNewColumn(t, "c15", 115, sql.LongBlob, false),
				schemaNewColumn(t, "c16", 15859, sql.MustCreateStringWithDefaults(sqltypes.Char, 5), false),
				schemaNewColumn(t, "c17", 11710, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false),
				schemaNewColumn(t, "c18", 6838, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "c19", 9377, sql.Float32, false),
				schemaNewColumn(t, "c20", 15979, sql.Float64, false),
				//schemaNewColumn(t, "c21", 121, sql.MustCreateDecimalType(10, 5), false),
				schemaNewColumn(t, "c22", 2910, sql.Uint32, false),
				schemaNewColumn(t, "c23", 8740, sql.Uint8, false),
				schemaNewColumn(t, "c24", 8689, sql.Uint16, false),
				schemaNewColumn(t, "c25", 5243, sql.Uint24, false),
				schemaNewColumn(t, "c26", 9338, sql.Uint64, false),
			),
		},
		{
			name: "Test primary keys",
			query: `create table testTable (
								id int, 
								age int, 
								first_name varchar(80), 
								is_married bool, 
								primary key (id, age))`,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "first_name", 3264, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "is_married", 14626, sql.Boolean, false)),
		},
		{
			name: "Test not null constraints",
			query: `create table testTable (
								id int, 
								age int, 
								first_name varchar(80) not null, 
								is_married bool, 
								primary key (id, age))`,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "first_name", 3264, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "is_married", 14626, sql.Boolean, false)),
		},
		{
			name: "Test quoted columns",
			query: "create table testTable (" +
				"`id` int, " +
				"`age` int, " +
				"`timestamp` varchar(80), " +
				"`is married` bool, " +
				"primary key (`id`, `age`))",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "timestamp", 10168, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "is married", 14626, sql.Boolean, false)),
		},
		{
			name: "Test tag comments",
			query: `create table testTable (
								id int primary key, age int)`,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, false)),
		},
		// Real world examples for regression testing
		{
			name: "Test ip2nation",
			query: `CREATE TABLE ip2nation (
							ip int(11) unsigned NOT NULL default 0,
							country char(2) NOT NULL default '',
							PRIMARY KEY (ip));`,
			expectedTable: "ip2nation",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumnWDefVal(t, "ip", 7265, sql.Uint32, true, "0", schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "country", 6630, sql.MustCreateStringWithDefaults(sqltypes.Char, 2), false, `""`, schema.NotNullConstraint{})),
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
							PRIMARY KEY (code));`,
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumnWDefVal(t, "code", 7802, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 4), true, `""`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "iso_code_2", 9266, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2), false, `""`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "iso_code_3", 8427, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 3), false, `""`),
				schemaNewColumnWDefVal(t, "iso_country", 7151, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false, `""`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "country", 879, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false, `""`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "lat", 3502, sql.Float32, false, "0", schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "lon", 9907, sql.Float32, false, "0", schema.NotNullConstraint{})),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			dtestutils.CreateTestTable(t, dEnv, PeopleTableName, PeopleTestSchema, AllPeopleRows...)
			ctx := context.Background()
			root, _ := dEnv.WorkingRoot(ctx)

			updatedRoot, err := ExecuteSql(t, dEnv, root, tt.query)

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
			equalSchemas(t, tt.expectedSchema, sch)
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

			updatedRoot, err := ExecuteSql(t, dEnv, root, tt.query)

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
			name:  "alter add string column no default",
			query: "alter table people add (newColumn varchar(80))",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 4208, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false)),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 4208, nil),
		},
		{
			name:  "alter add float column without default",
			query: "alter table people add (newColumn float)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 4208, sql.Float32, false)),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 4208, nil),
		},
		{
			name:  "alter add uint column without default",
			query: "alter table people add (newColumn bigint unsigned)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 4208, sql.Uint64, false)),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 4208, nil),
		},
		{
			name:  "alter add int column default",
			query: "alter table people add (newColumn int default 2)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 2803, sql.Int32, false, "2")),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 2803, types.Int(int32(2))),
		},
		{
			name:  "alter add uint column default",
			query: "alter table people add (newColumn bigint unsigned default 20)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 517, sql.Uint64, false, "20")),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 517, types.Uint(uint64(20))),
		},
		{
			name:  "alter add string column with default",
			query: "alter table people add (newColumn varchar(80) default 'hi')",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 13690, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, `"hi"`)),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 13690, types.String("hi")),
		},
		{
			name:  "alter add column first",
			query: "alter table people add newColumn varchar(80) first",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "newColumn", 4208, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 4208, nil),
		},
		{
			name:  "alter add column middle",
			query: "alter table people add newColumn varchar(80) after last_name",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "newColumn", 4208, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 4208, nil),
		},
		{
			name:  "alter add column not null",
			query: "alter table people add (newColumn varchar(80) not null default 'default')",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 13690, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, `"default"`, schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 13690, types.String("default")),
		},
		{
			name:  "alter add column not null with expression default",
			query: "alter table people add (newColumn int not null default 2+2/2)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 2803, sql.Int32, false, "((2 + (2 / 2)))", schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 2803, types.Int(3)),
		},
		{
			name:  "alter add column not null with negative expression",
			query: "alter table people add (newColumn float not null default -1.1)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 12469, sql.Float32, false, "-1.1", schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 12469, types.Float(float32(-1.1))),
		},
		{
			name:        "alter add column not null with type mismatch in default",
			query:       "alter table people add (newColumn float not null default 'not a number')",
			expectedErr: "incompatible type",
		},
		{
			name:        "alter add column column not found",
			query:       "alter table people add column newColumn float after notFound",
			expectedErr: `table "people" does not have column "notFound"`,
		},
		{
			name:        "alter add column table not found",
			query:       "alter table notFound add column newColumn float",
			expectedErr: "table not found: notFound",
		},
		{
			name:  "alter add column not null without default",
			query: "alter table people add (newColumn varchar(80) not null)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 13690, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, "", schema.NotNullConstraint{})),
			expectedRows: dtestutils.AddColToRows(t, AllPeopleRows, 13690, types.String("")),
		},
		{
			name:  "alter add column nullable",
			query: "alter table people add (newColumn bigint)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 4435, sql.Int64, false)),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter add column with optional column keyword",
			query: "alter table people add column (newColumn varchar(80))",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 4208, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false)),
			expectedRows: AllPeopleRows,
		},
		{
			name:        "alter table add column name clash",
			query:       "alter table people add column(age int)",
			expectedErr: `Column "age" already exists`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			ctx := context.Background()
			root, _ := dEnv.WorkingRoot(ctx)

			updatedRoot, err := ExecuteSql(t, dEnv, root, tt.query)

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
			equalSchemas(t, tt.expectedSchema, sch)

			updatedTable, ok, err := updatedRoot.GetTable(ctx, "people")
			assert.NoError(t, err)
			require.True(t, ok)

			rowData, err := updatedTable.GetNomsRowData(ctx)
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
			query: "alter table people modify column first_name varchar(16383) not null after last_name",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter modify column reorder first",
			query: "alter table people modify column first_name varchar(16383) not null first",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter modify column drop null constraint",
			query: "alter table people modify column first_name varchar(16383) null",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter change column rename and reorder",
			query: "alter table people change first_name christian_name varchar(16383) not null after last_name",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("christian_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter change column rename and reorder first",
			query: "alter table people change column first_name christian_name varchar(16383) not null first",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("christian_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter change column drop null constraint",
			query: "alter table people change column first_name first_name varchar(16383) null",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: AllPeopleRows,
		},
		{
			name:        "alter modify column not null with type mismatch in default",
			query:       "alter table people modify rating double default 'not a number'",
			expectedErr: "incompatible type for default value",
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

			updatedRoot, err := ExecuteSql(t, dEnv, root, tt.query)

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
			equalSchemas(t, tt.expectedSchema, sch)

			updatedTable, ok, err := updatedRoot.GetTable(ctx, "people")
			assert.NoError(t, err)
			require.True(t, ok)

			rowData, err := updatedTable.GetNomsRowData(ctx)
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

func TestModifyColumnType(t *testing.T) {
	tests := []struct {
		name            string
		setupStmts      []string
		alterStmt       string
		tableName       string
		expectedRows    [][]types.Value
		expectedIdxRows [][]types.Value
		expectedErr     bool
	}{
		{
			name: "alter modify column type similar types",
			setupStmts: []string{
				"create table test(pk bigint primary key, v1 bigint, index (v1))",
				"insert into test values (0, 3), (1, 2)",
			},
			alterStmt: "alter table test modify column v1 int",
			tableName: "test",
			expectedRows: [][]types.Value{
				{types.Int(0), types.Int(3)},
				{types.Int(1), types.Int(2)},
			},
			expectedIdxRows: [][]types.Value{
				{types.Int(2), types.Int(1)},
				{types.Int(3), types.Int(0)},
			},
		},
		{
			name: "alter modify column type different types",
			setupStmts: []string{
				"create table test(pk bigint primary key, v1 bigint, index (v1))",
				"insert into test values (0, 3), (1, 2)",
			},
			alterStmt: "alter table test modify column v1 varchar(20)",
			tableName: "test",
			expectedRows: [][]types.Value{
				{types.Int(0), types.String("3")},
				{types.Int(1), types.String("2")},
			},
			expectedIdxRows: [][]types.Value{
				{types.String("2"), types.Int(1)},
				{types.String("3"), types.Int(0)},
			},
		},
		{
			name: "alter modify column type different types reversed",
			setupStmts: []string{
				"create table test(pk bigint primary key, v1 varchar(20), index (v1))",
				`insert into test values (0, "3"), (1, "2")`,
			},
			alterStmt: "alter table test modify column v1 bigint",
			tableName: "test",
			expectedRows: [][]types.Value{
				{types.Int(0), types.Int(3)},
				{types.Int(1), types.Int(2)},
			},
			expectedIdxRows: [][]types.Value{
				{types.Int(2), types.Int(1)},
				{types.Int(3), types.Int(0)},
			},
		},
		{
			name: "alter modify column type primary key",
			setupStmts: []string{
				"create table test(pk bigint primary key, v1 bigint, index (v1))",
				"insert into test values (0, 3), (1, 2)",
			},
			alterStmt: "alter table test modify column pk varchar(20)",
			tableName: "test",
			expectedRows: [][]types.Value{
				{types.String("0"), types.Int(3)},
				{types.String("1"), types.Int(2)},
			},
			expectedIdxRows: [][]types.Value{
				{types.Int(2), types.String("1")},
				{types.Int(3), types.String("0")},
			},
		},
		{
			name: "alter modify column type incompatible types with empty table",
			setupStmts: []string{
				"create table test(pk bigint primary key, v1 bit(20), index (v1))",
			},
			alterStmt:       "alter table test modify column pk datetime",
			tableName:       "test",
			expectedRows:    [][]types.Value(nil),
			expectedIdxRows: [][]types.Value(nil),
		},
		{
			name: "alter modify column type incompatible types with non-empty table",
			setupStmts: []string{
				"create table test(pk bigint primary key, v1 bit(20), index (v1))",
				"insert into test values (1, 1)",
			},
			alterStmt:   "alter table test modify column pk datetime",
			expectedErr: true,
		},
		{
			name: "alter modify column type different types incompatible values",
			setupStmts: []string{
				"create table test(pk bigint primary key, v1 varchar(20), index (v1))",
				"insert into test values (0, 3), (1, 'a')",
			},
			alterStmt:   "alter table test modify column v1 bigint",
			expectedErr: true,
		},
		{
			name: "alter modify column type foreign key parent",
			setupStmts: []string{
				"create table test(pk bigint primary key, v1 bigint, index (v1))",
				"create table test2(pk bigint primary key, v1 bigint, index (v1), foreign key (v1) references test(v1))",
			},
			alterStmt:   "alter table test modify column v1 varchar(20)",
			expectedErr: true,
		},
		{
			name: "alter modify column type foreign key child",
			setupStmts: []string{
				"create table test(pk bigint primary key, v1 bigint, index (v1))",
				"create table test2(pk bigint primary key, v1 bigint, index (v1), foreign key (v1) references test(v1))",
			},
			alterStmt:   "alter table test2 modify column v1 varchar(20)",
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			ctx := context.Background()
			root, _ := dEnv.WorkingRoot(ctx)
			var err error

			for _, stmt := range test.setupStmts {
				root, err = ExecuteSql(t, dEnv, root, stmt)
				require.NoError(t, err)
			}
			root, err = ExecuteSql(t, dEnv, root, test.alterStmt)
			if test.expectedErr == false {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				return
			}

			table, _, err := root.GetTable(ctx, test.tableName)
			require.NoError(t, err)
			sch, err := table.GetSchema(ctx)
			require.NoError(t, err)
			rowData, err := table.GetNomsRowData(ctx)
			require.NoError(t, err)

			var foundRows [][]types.Value
			err = rowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				r, err := row.FromNoms(sch, key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				var vals []types.Value
				_, _ = r.IterSchema(sch, func(tag uint64, val types.Value) (stop bool, err error) {
					vals = append(vals, val)
					return false, nil
				})
				foundRows = append(foundRows, vals)
				return false, nil
			})
			require.NoError(t, err)
			assert.Equal(t, test.expectedRows, foundRows)

			foundRows = nil
			idx := sch.Indexes().AllIndexes()[0]
			idxRowData, err := table.GetNomsIndexRowData(ctx, idx.Name())
			require.NoError(t, err)
			err = idxRowData.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
				r, err := row.FromNoms(idx.Schema(), key.(types.Tuple), value.(types.Tuple))
				require.NoError(t, err)
				var vals []types.Value
				_, _ = r.IterSchema(idx.Schema(), func(tag uint64, val types.Value) (stop bool, err error) {
					vals = append(vals, val)
					return false, nil
				})
				foundRows = append(foundRows, vals)
				return false, nil
			})
			require.NoError(t, err)
			assert.Equal(t, test.expectedIdxRows, foundRows)
		})
	}
}

func TestDropColumnStatements(t *testing.T) {
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
			expectedErr: `table "people" does not have column "notFound"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			ctx := context.Background()
			root, _ := dEnv.WorkingRoot(ctx)

			updatedRoot, err := ExecuteSql(t, dEnv, root, tt.query)

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

			rowData, err := updatedTable.GetNomsRowData(ctx)
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
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("newRating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
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
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("newRating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
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
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
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
			expectedErr: `table "people" does not have column "notFound"`,
		},
		{
			name:        "column name collision",
			query:       "alter table people rename column id to AGE",
			expectedErr: "Column \"AGE\" already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			ctx := context.Background()
			root, _ := dEnv.WorkingRoot(ctx)

			updatedRoot, err := ExecuteSql(t, dEnv, root, tt.query)

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
			require.NoError(t, err)
			assert.Equal(t, tt.expectedSchema, sch)

			updatedTable, ok, err := updatedRoot.GetTable(ctx, "people")
			assert.NoError(t, err)
			require.True(t, ok)

			rowData, err := updatedTable.GetNomsRowData(ctx)
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

func TestRenameTableStatements(t *testing.T) {
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
			name:           "alter rename table with alter syntax",
			query:          "alter table people rename to 123People",
			oldTableName:   "people",
			newTableName:   "123People",
			expectedSchema: PeopleTestSchema,
			expectedRows:   AllPeopleRows,
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
			expectedErr: "already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			CreateTestDatabase(dEnv, t)
			ctx := context.Background()
			root, _ := dEnv.WorkingRoot(ctx)

			updatedRoot, err := ExecuteSql(t, dEnv, root, tt.query)
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

			rowData, err := newTable.GetNomsRowData(ctx)
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
	systemTableNames := []string{"dolt_docs", "dolt_log", "dolt_history_people", "dolt_diff_people", "dolt_commit_diff_people"}
	reservedTableNames := []string{"dolt_schemas", "dolt_query_catalog"}

	var dEnv *env.DoltEnv
	setup := func() {
		dEnv = dtestutils.CreateTestEnv()
		CreateTestDatabase(dEnv, t)

		dtestutils.CreateTestTable(t, dEnv, "dolt_docs",
			doltdocs.DocsSchema,
			NewRow(types.String("LICENSE.md"), types.String("A license")))
		dtestutils.CreateTestTable(t, dEnv, doltdb.DoltQueryCatalogTableName,
			dtables.DoltQueryCatalogSchema,
			NewRow(types.String("abc123"), types.Uint(1), types.String("example"), types.String("select 2+2 from dual"), types.String("description")))
		dtestutils.CreateTestTable(t, dEnv, doltdb.SchemasTableName,
			SchemasTableSchema(),
			NewRowWithPks([]types.Value{types.String("view"), types.String("name")}, types.String("select 2+2 from dual")))
	}

	t.Run("Create", func(t *testing.T) {
		setup()
		for _, tableName := range append(systemTableNames, reservedTableNames...) {
			assertFails(t, dEnv, fmt.Sprintf("create table %s (a int primary key not null)", tableName), "reserved")
		}
	})

	// The _history and _diff tables give not found errors right now because of https://github.com/dolthub/dolt/issues/373.
	// We can remove the divergent failure logic when the issue is fixed.
	t.Run("Drop", func(t *testing.T) {
		setup()
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
		setup()
		for _, tableName := range systemTableNames {
			expectedErr := "system table"
			if strings.HasPrefix(tableName, "dolt_diff") || strings.HasPrefix(tableName, "dolt_history") {
				expectedErr = "system tables cannot be dropped or altered"
			}
			assertFails(t, dEnv, fmt.Sprintf("rename table %s to newname", tableName), expectedErr)
		}
		for i, tableName := range reservedTableNames {
			assertSucceeds(t, dEnv, fmt.Sprintf("rename table %s to newname%d", tableName, i))
		}
	})

	t.Run("Alter", func(t *testing.T) {
		setup()
		for _, tableName := range append(systemTableNames, reservedTableNames...) {
			expectedErr := "cannot be altered"
			if strings.HasPrefix(tableName, "dolt_diff") || strings.HasPrefix(tableName, "dolt_history") {
				expectedErr = " cannot be altered"
			}
			assertFails(t, dEnv, fmt.Sprintf("alter table %s add column a int", tableName), expectedErr)
		}
	})
}

func TestParseCreateTableStatement(t *testing.T) {
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
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{})),
		},
		{
			name:          "Test create table starting with number",
			query:         "create table 123table (id int primary key)",
			expectedTable: "`123table`",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{})),
		},
		{
			name:          "Test create two column schema",
			query:         "create table testTable (id int primary key, age int)",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, false)),
		},
		{
			name:          "Test syntax error",
			query:         "create table testTable id int, age int",
			expectedTable: "testTable",
			expectedErr:   "syntax error",
		},
		{
			name: "Test types",
			query: `create table testTable (
								id int primary key, 
								age int, 
								first_name varchar(255), 
								is_married boolean) `,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, false),
				schemaNewColumn(t, "first_name", 3264, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false),
				schemaNewColumn(t, "is_married", 14626, sql.Boolean, false)),
		},
		{
			name:          "Test all supported types",
			expectedTable: "testTable",
			query: `create table testTable (
							c0 int primary key,
							c1 tinyint,
							c2 smallint,
							c3 mediumint,
							c4 integer,
							c5 bigint,
							c6 bool,
							c7 boolean,
							c8 bit(10),
							c9 text,
							c10 tinytext,
							c11 mediumtext,
							c12 longtext,
							c16 char(5),
							c17 varchar(255),
							c18 varchar(80),
							c19 float,
							c20 double,
							c22 int unsigned,
							c23 tinyint unsigned,
							c24 smallint unsigned,
							c25 mediumint unsigned,
							c26 bigint unsigned)`,
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "c0", 594, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "c1", 601, sql.Int8, false),
				schemaNewColumn(t, "c2", 14542, sql.Int16, false),
				schemaNewColumn(t, "c3", 13309, sql.Int24, false),
				schemaNewColumn(t, "c4", 15884, sql.Int32, false),
				schemaNewColumn(t, "c5", 14619, sql.Int64, false),
				schemaNewColumn(t, "c6", 13192, sql.Boolean, false),
				schemaNewColumn(t, "c7", 5981, sql.Boolean, false),
				schemaNewColumn(t, "c8", 14871, sql.MustCreateBitType(10), false),
				schemaNewColumn(t, "c9", 4167, sql.Text, false),
				schemaNewColumn(t, "c10", 1965, sql.TinyText, false),
				schemaNewColumn(t, "c11", 12860, sql.MediumText, false),
				schemaNewColumn(t, "c12", 7155, sql.LongText, false),
				//schemaNewColumn(t, "c13", 113, sql.TinyBlob, false),
				//schemaNewColumn(t, "c14", 114, sql.Blob, false),
				//schemaNewColumn(t, "c15", 115, sql.LongBlob, false),
				schemaNewColumn(t, "c16", 15859, sql.MustCreateStringWithDefaults(sqltypes.Char, 5), false),
				schemaNewColumn(t, "c17", 11710, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false),
				schemaNewColumn(t, "c18", 6838, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "c19", 9377, sql.Float32, false),
				schemaNewColumn(t, "c20", 15979, sql.Float64, false),
				//schemaNewColumn(t, "c21", 121, sql.MustCreateDecimalType(10, 5), false),
				schemaNewColumn(t, "c22", 2910, sql.Uint32, false),
				schemaNewColumn(t, "c23", 8740, sql.Uint8, false),
				schemaNewColumn(t, "c24", 8689, sql.Uint16, false),
				schemaNewColumn(t, "c25", 5243, sql.Uint24, false),
				schemaNewColumn(t, "c26", 9338, sql.Uint64, false),
			),
		},
		{
			name: "Test primary keys",
			query: `create table testTable (
								id int, 
								age int, 
								first_name varchar(80), 
								is_married bool, 
								primary key (id, age))`,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "first_name", 3264, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "is_married", 14626, sql.Boolean, false)),
		},
		{
			name: "Test not null constraints",
			query: `create table testTable (
								id int, 
								age int, 
								first_name varchar(80) not null, 
								is_married bool, 
								primary key (id, age))`,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "first_name", 3264, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "is_married", 14626, sql.Boolean, false)),
		},
		{
			name: "Test quoted columns",
			query: "create table testTable (" +
				"`id` int, " +
				"`age` int, " +
				"`timestamp` varchar(80), " +
				"`is married` bool, " +
				"primary key (`id`, `age`))",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "timestamp", 10168, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "is married", 14626, sql.Boolean, false)),
		},
		{
			name: "Test tag comments",
			query: `create table testTable (
								id int primary key, age int)`,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, sql.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, sql.Int32, false)),
		},
		// Real world examples for regression testing
		{
			name: "Test ip2nation",
			query: `CREATE TABLE ip2nation (
							ip int(11) unsigned NOT NULL default 0,
							country char(2) NOT NULL default '',
							PRIMARY KEY (ip));`,
			expectedTable: "ip2nation",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumnWDefVal(t, "ip", 7265, sql.Uint32, true, "0", schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "country", 6630, sql.MustCreateStringWithDefaults(sqltypes.Char, 2), false, `""`, schema.NotNullConstraint{})),
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
							PRIMARY KEY (code));`,
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumnWDefVal(t, "code", 7802, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 4), true, `""`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "iso_code_2", 9266, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2), false, `""`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "iso_code_3", 8427, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 3), false, `""`),
				schemaNewColumnWDefVal(t, "iso_country", 7151, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false, `""`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "country", 879, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false, `""`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "lat", 3502, sql.Float32, false, "0", schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "lon", 9907, sql.Float32, false, "0", schema.NotNullConstraint{})),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			ctx := context.Background()
			root, _ := dEnv.WorkingRoot(ctx)

			tblName, sch, err := sqlutil.ParseCreateTableStatement(ctx, root, tt.query)

			if tt.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				require.NoError(t, err)
				equalSchemas(t, tt.expectedSchema, sch)
				assert.Equal(t, tt.expectedTable, tblName)
			}
		})
	}
}

func TestIndexOverwrite(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		panic(err)
	}
	root, err = ExecuteSql(t, dEnv, root, `
CREATE TABLE parent (
  pk bigint PRIMARY KEY,
  v1 bigint,
  INDEX (v1)
);
CREATE TABLE child (
  pk varchar(10) PRIMARY KEY,
  parent_value bigint,
  CONSTRAINT fk_child FOREIGN KEY (parent_value)
    REFERENCES parent(v1)
);
CREATE TABLE child_idx (
  pk varchar(10) PRIMARY KEY,
  parent_value bigint,
  INDEX (parent_value),
  CONSTRAINT fk_child_idx FOREIGN KEY (parent_value)
    REFERENCES parent(v1)
);
CREATE TABLE child_unq (
  pk varchar(10) PRIMARY KEY,
  parent_value bigint,
  CONSTRAINT fk_child_unq FOREIGN KEY (parent_value)
    REFERENCES parent(v1)
);
CREATE TABLE child_non_unq (
  pk varchar(10) PRIMARY KEY,
  parent_value bigint,
  CONSTRAINT fk_child_non_unq FOREIGN KEY (parent_value)
    REFERENCES parent(v1)
);
INSERT INTO parent VALUES (1, 1), (2, 2), (3, 3), (4, NULL), (5, 5), (6, 6), (7, 7);
INSERT INTO child VALUES ('1', 1), ('2', NULL), ('3', 3), ('4', 3), ('5', 5);
INSERT INTO child_idx VALUES ('1', 1), ('2', NULL), ('3', 3), ('4', 3), ('5', 5);
INSERT INTO child_unq VALUES ('1', 1), ('2', NULL), ('3', 3), ('4', NULL), ('5', 5);
INSERT INTO child_non_unq VALUES ('1', 1), ('2', NULL), ('3', 3), ('4', 3), ('5', 5);
`)
	// test index creation
	require.NoError(t, err)
	root, err = ExecuteSql(t, dEnv, root, "CREATE INDEX abc ON child (parent_value);")
	require.NoError(t, err)
	_, err = ExecuteSql(t, dEnv, root, "CREATE INDEX abc_idx ON child_idx (parent_value);")
	require.NoError(t, err)
	root, err = ExecuteSql(t, dEnv, root, "CREATE UNIQUE INDEX abc_unq ON child_unq (parent_value);")
	require.NoError(t, err)
	_, err = ExecuteSql(t, dEnv, root, "CREATE UNIQUE INDEX abc_non_unq ON child_non_unq (parent_value);")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "UNIQUE constraint violation")
	}

	// check foreign keys for updated index (or verify they weren't updated)
	fkc, err := root.GetForeignKeyCollection(ctx)
	require.NoError(t, err)
	fkChild, ok := fkc.GetByNameCaseInsensitive("fk_child")
	require.True(t, ok)
	require.Equal(t, "abc", fkChild.TableIndex)
	fkChildIdx, ok := fkc.GetByNameCaseInsensitive("fk_child_idx")
	require.True(t, ok)
	require.Equal(t, "abc_idx", fkChildIdx.TableIndex)
	fkChildUnq, ok := fkc.GetByNameCaseInsensitive("fk_child_unq")
	require.True(t, ok)
	require.Equal(t, "abc_unq", fkChildUnq.TableIndex)
	fkChildNonUnq, ok := fkc.GetByNameCaseInsensitive("fk_child_non_unq")
	require.True(t, ok)
	require.Equal(t, "parent_value", fkChildNonUnq.TableIndex)

	// insert tests against index
	root, err = ExecuteSql(t, dEnv, root, "INSERT INTO child VALUES ('6', 5)")
	require.NoError(t, err)
	root, err = ExecuteSql(t, dEnv, root, "INSERT INTO child_idx VALUES ('6', 5)")
	require.NoError(t, err)
	_, err = ExecuteSql(t, dEnv, root, "INSERT INTO child_unq VALUES ('6', 5)")
	if assert.Error(t, err) {
		assert.True(t, sql.ErrUniqueKeyViolation.Is(err.(sql.WrappedInsertError).Cause))
	}
	root, err = ExecuteSql(t, dEnv, root, "INSERT INTO child_non_unq VALUES ('6', 5)")
	require.NoError(t, err)

	// insert tests against foreign key
	_, err = ExecuteSql(t, dEnv, root, "INSERT INTO child VALUES ('9', 9)")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "Foreign key violation")
	}
	_, err = ExecuteSql(t, dEnv, root, "INSERT INTO child_idx VALUES ('9', 9)")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "Foreign key violation")
	}
	_, err = ExecuteSql(t, dEnv, root, "INSERT INTO child_unq VALUES ('9', 9)")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "Foreign key violation")
	}
	_, err = ExecuteSql(t, dEnv, root, "INSERT INTO child_non_unq VALUES ('9', 9)")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "Foreign key violation")
	}
}

func TestCreateIndexUnique(t *testing.T) {
	dEnv := dtestutils.CreateTestEnv()
	root, err := dEnv.WorkingRoot(context.Background())
	if err != nil {
		panic(err)
	}
	root, err = ExecuteSql(t, dEnv, root, `
CREATE TABLE pass_unique (
  pk1 BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
CREATE TABLE fail_unique (
  pk1 BIGINT PRIMARY KEY,
  v1 BIGINT,
  v2 BIGINT
);
INSERT INTO pass_unique VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO fail_unique VALUES (1, 1, 1), (2, 2, 2), (3, 2, 3);
`)
	require.NoError(t, err)
	root, err = ExecuteSql(t, dEnv, root, "CREATE UNIQUE INDEX idx_v1 ON pass_unique(v1)")
	assert.NoError(t, err)
	root, err = ExecuteSql(t, dEnv, root, "CREATE UNIQUE INDEX idx_v1 ON fail_unique(v1)")
	if assert.Error(t, err) {
		assert.Contains(t, strings.ToLower(err.Error()), "unique")
	}
}

func assertFails(t *testing.T, dEnv *env.DoltEnv, query, expectedErr string) {
	ctx := context.Background()
	root, _ := dEnv.WorkingRoot(ctx)
	_, err := ExecuteSql(t, dEnv, root, query)
	require.Error(t, err, query)
	assert.Contains(t, err.Error(), expectedErr)
}

func assertSucceeds(t *testing.T, dEnv *env.DoltEnv, query string) {
	ctx := context.Background()
	root, _ := dEnv.WorkingRoot(ctx)
	_, err := ExecuteSql(t, dEnv, root, query)
	assert.NoError(t, err, query)
}
