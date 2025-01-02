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

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
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
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{})),
		},
		{
			name:          "Test create two column schema",
			query:         "create table testTable (id int primary key, age int)",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, false)),
		},
		{
			name:          "Test create two column keyless schema",
			query:         "create table testTable (id int, age int)",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, false),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, false)),
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
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, false),
				schemaNewColumn(t, "first_name", 3264, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false),
				schemaNewColumn(t, "is_married", 14626, gmstypes.Boolean, false)),
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
							c26 bigint unsigned,
							c27 tinyint(1))`,
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "c0", 594, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "c1", 601, gmstypes.Int8, false),
				schemaNewColumn(t, "c2", 14542, gmstypes.Int16, false),
				schemaNewColumn(t, "c3", 13309, gmstypes.Int24, false),
				schemaNewColumn(t, "c4", 15884, gmstypes.Int32, false),
				schemaNewColumn(t, "c5", 14619, gmstypes.Int64, false),
				schemaNewColumn(t, "c6", 13192, gmstypes.Boolean, false),
				schemaNewColumn(t, "c7", 5981, gmstypes.Boolean, false),
				schemaNewColumn(t, "c8", 14871, gmstypes.MustCreateBitType(10), false),
				schemaNewColumn(t, "c9", 4167, gmstypes.Text, false),
				schemaNewColumn(t, "c10", 1965, gmstypes.TinyText, false),
				schemaNewColumn(t, "c11", 12860, gmstypes.MediumText, false),
				schemaNewColumn(t, "c12", 7155, gmstypes.LongText, false),
				//schemaNewColumn(t, "c13", 113, sql.TinyBlob, false),
				//schemaNewColumn(t, "c14", 114, sql.Blob, false),
				//schemaNewColumn(t, "c15", 115, sql.LongBlob, false),
				schemaNewColumn(t, "c16", 15859, gmstypes.MustCreateStringWithDefaults(sqltypes.Char, 5), false),
				schemaNewColumn(t, "c17", 11710, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false),
				schemaNewColumn(t, "c18", 6838, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "c19", 9377, gmstypes.Float32, false),
				schemaNewColumn(t, "c20", 15979, gmstypes.Float64, false),
				//schemaNewColumn(t, "c21", 121, sql.MustCreateDecimalType(10, 5), false),
				schemaNewColumn(t, "c22", 2910, gmstypes.Uint32, false),
				schemaNewColumn(t, "c23", 8740, gmstypes.Uint8, false),
				schemaNewColumn(t, "c24", 8689, gmstypes.Uint16, false),
				schemaNewColumn(t, "c25", 5243, gmstypes.Uint24, false),
				schemaNewColumn(t, "c26", 9338, gmstypes.Uint64, false),
				schemaNewColumn(t, "c27", 5981, gmstypes.Boolean, false),
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
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "first_name", 3264, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "is_married", 14626, gmstypes.Boolean, false)),
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
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "first_name", 3264, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "is_married", 14626, gmstypes.Boolean, false)),
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
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "timestamp", 10168, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "is married", 14626, gmstypes.Boolean, false)),
		},
		{
			name: "Test tag comments",
			query: `create table testTable (
								id int primary key, age int)`,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, false)),
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
				schemaNewColumnWDefVal(t, "ip", 7265, gmstypes.Uint32, true, "0", schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "country", 6630, gmstypes.MustCreateStringWithDefaults(sqltypes.Char, 2), false, `''`, schema.NotNullConstraint{})),
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
				schemaNewColumnWDefVal(t, "code", 7802, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 4), true, `''`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "iso_code_2", 9266, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 2), false, `''`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "iso_code_3", 8427, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 3), false, `''`),
				schemaNewColumnWDefVal(t, "iso_country", 7151, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false, `''`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "country", 879, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false, `''`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "lat", 3502, gmstypes.Float32, false, "0", schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "lon", 9907, gmstypes.Float32, false, "0", schema.NotNullConstraint{})),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dEnv, err := CreateEmptyTestDatabase()
			require.NoError(t, err)
			defer dEnv.DoltDB.Close()

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			updatedRoot, err := ExecuteSql(dEnv, root, tt.query)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}

			require.NotNil(t, updatedRoot)

			table, ok, err := updatedRoot.GetTable(ctx, doltdb.TableName{Name: tt.expectedTable})
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
			ctx := context.Background()
			dEnv, err := CreateTestDatabase()
			require.NoError(t, err)
			defer dEnv.DoltDB.Close()

			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

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
				has, err := updatedRoot.HasTable(ctx, doltdb.TableName{Name: tableName})
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
				schemaNewColumn(t, "newColumn", 4208, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false)),
			expectedRows: addColToRows(t, AllPeopleRows, 4208, nil),
		},
		{
			name:  "alter add float column without default",
			query: "alter table people add (newColumn float)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 4208, gmstypes.Float32, false)),
			expectedRows: addColToRows(t, AllPeopleRows, 4208, nil),
		},
		{
			name:  "alter add uint column without default",
			query: "alter table people add (newColumn bigint unsigned)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 4208, gmstypes.Uint64, false)),
			expectedRows: addColToRows(t, AllPeopleRows, 4208, nil),
		},
		{
			name:  "alter add int column default",
			query: "alter table people add (newColumn int default 2)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 2803, gmstypes.Int32, false, "2")),
			expectedRows: addColToRows(t, AllPeopleRows, 2803, types.Int(int32(2))),
		},
		{
			name:  "alter add uint column default",
			query: "alter table people add (newColumn bigint unsigned default 20)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 517, gmstypes.Uint64, false, "20")),
			expectedRows: addColToRows(t, AllPeopleRows, 517, types.Uint(uint64(20))),
		},
		{
			name:  "alter add string column with default",
			query: "alter table people add (newColumn varchar(80) default 'hi')",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 13690, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, `'hi'`)),
			expectedRows: addColToRows(t, AllPeopleRows, 13690, types.String("hi")),
		},
		{
			name:  "alter add column first",
			query: "alter table people add newColumn varchar(80) first",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "newColumn", 4208, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: addColToRows(t, AllPeopleRows, 4208, nil),
		},
		{
			name:  "alter add column middle",
			query: "alter table people add newColumn varchar(80) after last_name",
			expectedSchema: dtestutils.CreateSchema(
				schema.NewColumn("id", IdTag, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first_name", FirstNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("last_name", LastNameTag, types.StringKind, false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "newColumn", 4208, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false),
				schema.NewColumn("age", AgeTag, types.IntKind, false),
				schema.NewColumn("rating", RatingTag, types.FloatKind, false),
				schema.NewColumn("uuid", UuidTag, types.StringKind, false),
				schema.NewColumn("num_episodes", NumEpisodesTag, types.UintKind, false),
			),
			expectedRows: addColToRows(t, AllPeopleRows, 4208, nil),
		},
		{
			name:  "alter add column not null",
			query: "alter table people add (newColumn varchar(80) not null default 'default')",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 13690, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, `'default'`, schema.NotNullConstraint{})),
			expectedRows: addColToRows(t, AllPeopleRows, 13690, types.String("default")),
		},
		{
			name:  "alter add column not null with expression default",
			query: "alter table people add (newColumn int not null default (2+2/2))",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 2803, gmstypes.Int32, false, "((2 + (2 / 2)))", schema.NotNullConstraint{})),
			expectedRows: addColToRows(t, AllPeopleRows, 2803, types.Int(3)),
		},
		{
			name:  "alter add column not null with negative expression",
			query: "alter table people add (newColumn float not null default -1.1)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumnWDefVal(t, "newColumn", 12469, gmstypes.Float32, false, "-1.1", schema.NotNullConstraint{})),
			expectedRows: addColToRows(t, AllPeopleRows, 12469, types.Float(float32(-1.1))),
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
				schemaNewColumnWDefVal(t, "newColumn", 13690, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, "", schema.NotNullConstraint{})),
			expectedRows: addColToRows(t, AllPeopleRows, 13690, types.String("")),
		},
		{
			name:  "alter add column nullable",
			query: "alter table people add (newColumn bigint)",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 4435, gmstypes.Int64, false)),
			expectedRows: AllPeopleRows,
		},
		{
			name:  "alter add column with optional column keyword",
			query: "alter table people add column (newColumn varchar(80))",
			expectedSchema: dtestutils.AddColumnToSchema(PeopleTestSchema,
				schemaNewColumn(t, "newColumn", 4208, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false)),
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
			dEnv, err := CreateTestDatabase()
			require.NoError(t, err)
			defer dEnv.DoltDB.Close()

			ctx := context.Background()
			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			updatedRoot, err := ExecuteSql(dEnv, root, tt.query)

			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}

			assert.NotNil(t, updatedRoot)
			table, _, err := updatedRoot.GetTable(ctx, doltdb.TableName{Name: PeopleTableName})

			assert.NoError(t, err)
			sch, err := table.GetSchema(ctx)
			assert.NoError(t, err)
			equalSchemas(t, tt.expectedSchema, sch)

			if types.Format_Default != types.Format_LD_1 {
				return // todo: convert these to enginetests
			}

			updatedTable, ok, err := updatedRoot.GetTable(ctx, doltdb.TableName{Name: "people"})
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
			dEnv, err := CreateTestDatabase()
			require.NoError(t, err)
			defer dEnv.DoltDB.Close()

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
			table, _, err := updatedRoot.GetTable(ctx, doltdb.TableName{Name: PeopleTableName})
			assert.NoError(t, err)
			sch, err := table.GetSchema(ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedSchema, sch)

			if types.Format_Default != types.Format_LD_1 {
				return // todo: convert these to enginetests
			}

			updatedTable, ok, err := updatedRoot.GetTable(ctx, doltdb.TableName{Name: "people"})
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
			query:       "rename table people to `a!trailing^space*is%the(worst) `",
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
			dEnv, err := CreateTestDatabase()
			require.NoError(t, err)
			defer dEnv.DoltDB.Close()

			ctx := context.Background()
			root, err := dEnv.WorkingRoot(ctx)
			require.NoError(t, err)

			updatedRoot, err := ExecuteSql(dEnv, root, tt.query)
			if len(tt.expectedErr) > 0 {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			} else {
				require.NoError(t, err)
			}
			require.NotNil(t, updatedRoot)

			has, err := updatedRoot.HasTable(ctx, doltdb.TableName{Name: tt.oldTableName})
			require.NoError(t, err)
			assert.False(t, has)

			newTable, ok, err := updatedRoot.GetTable(ctx, doltdb.TableName{Name: tt.newTableName})
			require.NoError(t, err)
			require.True(t, ok)

			sch, err := newTable.GetSchema(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expectedSchema, sch)

			if types.Format_Default != types.Format_LD_1 {
				return // todo: convert these to enginetests
			}

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
	systemTableNames := []string{"dolt_log", "dolt_history_people", "dolt_diff_people", "dolt_commit_diff_people", "dolt_schemas"}
	reservedTableNames := []string{"dolt_query_catalog", "dolt_docs", "dolt_procedures", "dolt_ignore"}

	var dEnv *env.DoltEnv
	var err error
	setup := func() {
		dEnv, err = CreateTestDatabase()
		require.NoError(t, err)

		CreateTestTable(t, dEnv, "dolt_docs", doltdb.DocsSchema,
			"INSERT INTO dolt_docs VALUES ('LICENSE.md','A license')")
		CreateTestTable(t, dEnv, doltdb.DoltQueryCatalogTableName, dtables.DoltQueryCatalogSchema,
			"INSERT INTO dolt_query_catalog VALUES ('abc123', 1, 'example', 'select 2+2 from dual', 'description')")
		ExecuteSetupSQL(context.Background(), `
    CREATE VIEW name as select 2+2 from dual;
		CREATE PROCEDURE simple_proc2() SELECT 1+1;
		INSERT INTO dolt_ignore VALUES ('test', 1);`)(t, dEnv)
	}

	t.Run("Create", func(t *testing.T) {
		setup()
		defer dEnv.DoltDB.Close()
		for _, tableName := range append(systemTableNames, reservedTableNames...) {
			assertFails(t, dEnv, fmt.Sprintf("create table %s (a int primary key not null)", tableName), "reserved")
		}
	})

	t.Run("Drop", func(t *testing.T) {
		setup()
		defer dEnv.DoltDB.Close()
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
		defer dEnv.DoltDB.Close()
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
		defer dEnv.DoltDB.Close()
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
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{})),
		},
		{
			name:          "Test create table starting with number",
			query:         "create table 123table (id int primary key)",
			expectedTable: "123table",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{})),
		},
		{
			name:          "Test create two column schema",
			query:         "create table testTable (id int primary key, age int)",
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, false)),
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
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, false),
				schemaNewColumn(t, "first_name", 3264, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false),
				schemaNewColumn(t, "is_married", 14626, gmstypes.Boolean, false)),
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
				schemaNewColumn(t, "c0", 594, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "c1", 601, gmstypes.Int8, false),
				schemaNewColumn(t, "c2", 14542, gmstypes.Int16, false),
				schemaNewColumn(t, "c3", 13309, gmstypes.Int24, false),
				schemaNewColumn(t, "c4", 15884, gmstypes.Int32, false),
				schemaNewColumn(t, "c5", 14619, gmstypes.Int64, false),
				schemaNewColumn(t, "c6", 13192, gmstypes.Boolean, false),
				schemaNewColumn(t, "c7", 5981, gmstypes.Boolean, false),
				schemaNewColumn(t, "c8", 14871, gmstypes.MustCreateBitType(10), false),
				schemaNewColumn(t, "c9", 4167, gmstypes.Text, false),
				schemaNewColumn(t, "c10", 1965, gmstypes.TinyText, false),
				schemaNewColumn(t, "c11", 12860, gmstypes.MediumText, false),
				schemaNewColumn(t, "c12", 7155, gmstypes.LongText, false),
				//schemaNewColumn(t, "c13", 113, sql.TinyBlob, false),
				//schemaNewColumn(t, "c14", 114, sql.Blob, false),
				//schemaNewColumn(t, "c15", 115, sql.LongBlob, false),
				schemaNewColumn(t, "c16", 15859, gmstypes.MustCreateStringWithDefaults(sqltypes.Char, 5), false),
				schemaNewColumn(t, "c17", 11710, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false),
				schemaNewColumn(t, "c18", 6838, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "c19", 9377, gmstypes.Float32, false),
				schemaNewColumn(t, "c20", 15979, gmstypes.Float64, false),
				//schemaNewColumn(t, "c21", 121, sql.MustCreateDecimalType(10, 5), false),
				schemaNewColumn(t, "c22", 2910, gmstypes.Uint32, false),
				schemaNewColumn(t, "c23", 8740, gmstypes.Uint8, false),
				schemaNewColumn(t, "c24", 8689, gmstypes.Uint16, false),
				schemaNewColumn(t, "c25", 5243, gmstypes.Uint24, false),
				schemaNewColumn(t, "c26", 9338, gmstypes.Uint64, false),
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
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "first_name", 3264, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "is_married", 14626, gmstypes.Boolean, false)),
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
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "first_name", 3264, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false, schema.NotNullConstraint{}),
				schemaNewColumn(t, "is_married", 14626, gmstypes.Boolean, false)),
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
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "timestamp", 10168, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 80), false),
				schemaNewColumn(t, "is married", 14626, gmstypes.Boolean, false)),
		},
		{
			name: "Test tag comments",
			query: `create table testTable (
								id int primary key, age int)`,
			expectedTable: "testTable",
			expectedSchema: dtestutils.CreateSchema(
				schemaNewColumn(t, "id", 4817, gmstypes.Int32, true, schema.NotNullConstraint{}),
				schemaNewColumn(t, "age", 7208, gmstypes.Int32, false)),
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
				schemaNewColumnWDefVal(t, "ip", 7265, gmstypes.Uint32, true, "0", schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "country", 6630, gmstypes.MustCreateStringWithDefaults(sqltypes.Char, 2), false, `''`, schema.NotNullConstraint{})),
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
				schemaNewColumnWDefVal(t, "code", 7802, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 4), true, `''`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "iso_code_2", 9266, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 2), false, `''`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "iso_code_3", 8427, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 3), false, `''`),
				schemaNewColumnWDefVal(t, "iso_country", 7151, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false, `''`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "country", 879, gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 255), false, `''`, schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "lat", 3502, gmstypes.Float32, false, "0.0", schema.NotNullConstraint{}),
				schemaNewColumnWDefVal(t, "lon", 9907, gmstypes.Float32, false, "0.0", schema.NotNullConstraint{})),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			defer dEnv.DoltDB.Close()
			ctx := context.Background()
			root, _ := dEnv.WorkingRoot(ctx)
			//eng, dbName, _ := engine.NewSqlEngineForEnv(ctx, dEnv)
			eng, sqlCtx := newTestEngine(ctx, dEnv)

			_, iter, _, err := eng.Query(sqlCtx, "create database test")
			if err != nil {
				panic(err)
			}
			_, _ = sql.RowIterToRows(sqlCtx, iter)
			sqlCtx.SetCurrentDatabase("test")

			tblName, sch, err := sqlutil.ParseCreateTableStatement(sqlCtx, root, eng, tt.query)

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

func newTestEngine(ctx context.Context, dEnv *env.DoltEnv) (*gms.Engine, *sql.Context) {
	pro, err := NewDoltDatabaseProviderWithDatabases("main", dEnv.FS, nil, nil)
	if err != nil {
		panic(err)
	}

	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	if err != nil {
		panic(err)
	}

	doltSession, err := dsess.NewDoltSession(sql.NewBaseSession(), pro, dEnv.Config.WriteableConfig(), nil, nil, writer.NewWriteSession)
	if err != nil {
		panic(err)
	}

	sqlCtx := sql.NewContext(ctx, sql.WithSession(doltSession))
	sqlCtx.SetCurrentDatabase(mrEnv.GetFirstDatabase())

	return gms.New(analyzer.NewBuilder(pro).Build(), &gms.Config{
		IsReadOnly:     false,
		IsServerLocked: false,
	}), sqlCtx
}

func TestIndexOverwrite(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB.Close()
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		panic(err)
	}
	root, err = ExecuteSql(dEnv, root, `
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
	root, err = ExecuteSql(dEnv, root, "CREATE INDEX abc ON child (parent_value);")
	require.NoError(t, err)
	_, err = ExecuteSql(dEnv, root, "CREATE INDEX abc_idx ON child_idx (parent_value);")
	require.NoError(t, err)
	root, err = ExecuteSql(dEnv, root, "CREATE UNIQUE INDEX abc_unq ON child_unq (parent_value);")
	require.NoError(t, err)
	_, err = ExecuteSql(dEnv, root, "CREATE UNIQUE INDEX abc_non_unq ON child_non_unq (parent_value);")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "duplicate unique key given")
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
	require.Equal(t, "fk_child_non_unq", fkChildNonUnq.TableIndex)

	// insert tests against index
	root, err = ExecuteSql(dEnv, root, "INSERT INTO child VALUES ('6', 5)")
	require.NoError(t, err)
	root, err = ExecuteSql(dEnv, root, "INSERT INTO child_idx VALUES ('6', 5)")
	require.NoError(t, err)
	_, err = ExecuteSql(dEnv, root, "INSERT INTO child_unq VALUES ('6', 5)")
	if assert.Error(t, err) {
		assert.True(t, sql.ErrUniqueKeyViolation.Is(err.(sql.WrappedInsertError).Cause))
	}
	root, err = ExecuteSql(dEnv, root, "INSERT INTO child_non_unq VALUES ('6', 5)")
	require.NoError(t, err)

	// insert tests against foreign key
	_, err = ExecuteSql(dEnv, root, "INSERT INTO child VALUES ('9', 9)")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "Foreign key violation")
	}
	_, err = ExecuteSql(dEnv, root, "INSERT INTO child_idx VALUES ('9', 9)")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "Foreign key violation")
	}
	_, err = ExecuteSql(dEnv, root, "INSERT INTO child_unq VALUES ('9', 9)")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "Foreign key violation")
	}
	_, err = ExecuteSql(dEnv, root, "INSERT INTO child_non_unq VALUES ('9', 9)")
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "Foreign key violation")
	}
}

func TestDropPrimaryKey(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB.Close()
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		panic(err)
	}

	t.Run("drop primary key", func(t *testing.T) {
		// setup
		root, err = ExecuteSql(dEnv, root, "create table parent (i int, j int, k int, index i (i), index ij (i, j), index ijk (i, j, k), index j (j), index kji (k, j, i));")
		require.NoError(t, err)
		root, err = ExecuteSql(dEnv, root, "create table child (x int, y int, constraint fk_child foreign key (x, y) references parent (i, j));")
		require.NoError(t, err)

		// check foreign keys for updated index
		fkc, err := root.GetForeignKeyCollection(ctx)
		require.NoError(t, err)
		fkChild, ok := fkc.GetByNameCaseInsensitive("fk_child")
		require.True(t, ok)
		require.Equal(t, "fk_child", fkChild.TableIndex)
		require.Equal(t, "ij", fkChild.ReferencedTableIndex)

		// add primary key
		root, err = ExecuteSql(dEnv, root, "alter table parent add primary key (i, j);")
		require.NoError(t, err)

		// dropping secondary index ij, should choose ijk
		root, err = ExecuteSql(dEnv, root, "alter table parent drop index ij;")
		require.NoError(t, err)

		// check foreign keys for updated index
		fkc, err = root.GetForeignKeyCollection(ctx)
		require.NoError(t, err)
		fkChild, ok = fkc.GetByNameCaseInsensitive("fk_child")
		require.True(t, ok)
		require.Equal(t, "fk_child", fkChild.TableIndex)
		require.Equal(t, "ijk", fkChild.ReferencedTableIndex)

		// dropping secondary index ijk, should switch to primary key
		root, err = ExecuteSql(dEnv, root, "alter table parent drop index ijk;")
		require.NoError(t, err)

		// check foreign keys for updated index
		fkc, err = root.GetForeignKeyCollection(ctx)
		require.NoError(t, err)
		fkChild, ok = fkc.GetByNameCaseInsensitive("fk_child")
		require.True(t, ok)
		require.Equal(t, "fk_child", fkChild.TableIndex)
		require.Equal(t, "", fkChild.ReferencedTableIndex)

		// no viable secondary indexes left, should be unable to drop primary key
		_, err = ExecuteSql(dEnv, root, "alter table parent drop primary key;")
		require.Error(t, err)
	})
}

func TestDropIndex(t *testing.T) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB.Close()
	root, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		panic(err)
	}
	t.Run("drop secondary indexes", func(t *testing.T) {
		// setup
		root, err = ExecuteSql(dEnv, root, "create table parent (i int);")
		require.NoError(t, err)
		root, err = ExecuteSql(dEnv, root, "alter table parent add index idx1 (i);")
		require.NoError(t, err)
		root, err = ExecuteSql(dEnv, root, "alter table parent add index idx2 (i);")
		require.NoError(t, err)
		root, err = ExecuteSql(dEnv, root, "alter table parent add index idx3 (i);")
		require.NoError(t, err)
		root, err = ExecuteSql(dEnv, root, "create table child (j int, constraint fk_child foreign key (j) references parent (i));")
		require.NoError(t, err)

		// drop and check next index
		fkc, err := root.GetForeignKeyCollection(ctx)
		require.NoError(t, err)
		fkChild, ok := fkc.GetByNameCaseInsensitive("fk_child")
		require.True(t, ok)
		require.Equal(t, "fk_child", fkChild.TableIndex)
		require.Equal(t, "idx1", fkChild.ReferencedTableIndex)

		// dropping secondary index, should switch to existing index
		root, err = ExecuteSql(dEnv, root, "alter table parent drop index idx1;")
		require.NoError(t, err)
		fkc, err = root.GetForeignKeyCollection(ctx)
		require.NoError(t, err)
		fkChild, ok = fkc.GetByNameCaseInsensitive("fk_child")
		require.True(t, ok)
		require.Equal(t, "fk_child", fkChild.TableIndex)
		require.Equal(t, "idx2", fkChild.ReferencedTableIndex)

		// dropping secondary index, should switch to existing index
		root, err = ExecuteSql(dEnv, root, "alter table parent drop index idx2;")
		require.NoError(t, err)
		fkc, err = root.GetForeignKeyCollection(ctx)
		require.NoError(t, err)
		fkChild, ok = fkc.GetByNameCaseInsensitive("fk_child")
		require.True(t, ok)
		require.Equal(t, "fk_child", fkChild.TableIndex)
		require.Equal(t, "idx3", fkChild.ReferencedTableIndex)

		// dropping secondary index, should fail since there are no indexes to replace it
		_, err = ExecuteSql(dEnv, root, "alter table parent drop index idx3;")
		require.Error(t, err)
	})
}

func TestCreateIndexUnique(t *testing.T) {
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB.Close()
	root, err := dEnv.WorkingRoot(context.Background())
	if err != nil {
		panic(err)
	}
	root, err = ExecuteSql(dEnv, root, `
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
	root, err = ExecuteSql(dEnv, root, "CREATE UNIQUE INDEX idx_v1 ON pass_unique(v1)")
	assert.NoError(t, err)
	root, err = ExecuteSql(dEnv, root, "CREATE UNIQUE INDEX idx_v1 ON fail_unique(v1)")
	if assert.Error(t, err) {
		assert.Contains(t, strings.ToLower(err.Error()), "unique")
	}
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
