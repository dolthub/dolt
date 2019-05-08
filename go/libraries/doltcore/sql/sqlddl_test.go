package sql

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/xwb1989/sqlparser"
)

func TestExecuteCreate(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedSchema schema.Schema
		expectedErr    bool
	}{
		{
			name:  "Test create single column schema",
			query: "create table people (id int primary key)",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{})),
		},
		{
			name:  "Test create two column schema",
			query: "create table people (id int primary key, age int)",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, false)),
		},
		{
			name:        "Test syntax error",
			query:       "create table people id int, age int",
			expectedErr: true,
		},
		{
			name:        "Test no primary keys",
			query:       "create table people (id int, age int)",
			expectedErr: true,
		},
		{
			name:  "Test types",
			query: "create table people (id int primary key, age int, first varchar, is_married boolean)",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, false),
				schema.NewColumn("first", 2, types.StringKind, false),
				schema.NewColumn("is_married", 3, types.BoolKind, false)),
		},
		{
			name: "Test all supported types",
			query: `create table people (
							c0 int primary key, 
							c1 tinyint,
							c2 smallint,
							c3 mediumint,
							c4 integer,
							c5 bigint,
							c6 bool,
							c7 boolean,
							c8 bit,
							c9 text,
							c10 tinytext,
							c11 mediumtext,
							c12 longtext,
							c13 blob,
							c14 tinyblob,
							c15 mediumblob,
							c16 char,
							c17 varchar,
							c18 varchar(80),
							c19 float,
							c20 double,
							c21 decimal,
							c22 int unsigned,
							c23 tinyint unsigned,
							c24 smallint unsigned,
							c25 mediumint unsigned,
							c26 bigint unsigned,
              c27 uuid)`,
			expectedSchema: createSchema(
				schema.NewColumn("c0", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("c1", 1, types.IntKind, false),
				schema.NewColumn("c2", 2, types.IntKind, false),
				schema.NewColumn("c3", 3, types.IntKind, false),
				schema.NewColumn("c4", 4, types.IntKind, false),
				schema.NewColumn("c5", 5, types.IntKind, false),
				schema.NewColumn("c6", 6, types.BoolKind, false),
				schema.NewColumn("c7", 7, types.BoolKind, false),
				schema.NewColumn("c8", 8, types.BoolKind, false),
				schema.NewColumn("c9", 9, types.StringKind, false),
				schema.NewColumn("c10", 10, types.StringKind, false),
				schema.NewColumn("c11", 11, types.StringKind, false),
				schema.NewColumn("c12", 12, types.StringKind, false),
				schema.NewColumn("c13", 13, types.BlobKind, false),
				schema.NewColumn("c14", 14, types.BlobKind, false),
				schema.NewColumn("c15", 15, types.BlobKind, false),
				schema.NewColumn("c16", 16, types.StringKind, false),
				schema.NewColumn("c17", 17, types.StringKind, false),
				schema.NewColumn("c18", 18, types.StringKind, false),
				schema.NewColumn("c19", 19, types.FloatKind, false),
				schema.NewColumn("c20", 20, types.FloatKind, false),
				schema.NewColumn("c21", 21, types.FloatKind, false),
				schema.NewColumn("c22", 22, types.UintKind, false),
				schema.NewColumn("c23", 23, types.UintKind, false),
				schema.NewColumn("c24", 24, types.UintKind, false),
				schema.NewColumn("c25", 25, types.UintKind, false),
				schema.NewColumn("c26", 26, types.UintKind, false),
				schema.NewColumn("c27", 27, types.UUIDKind, false),
			),
		},
		{
			name:  "Test primary keys",
			query: "create table people (id int, age int, first varchar(80), is_married bool, primary key (id, age))",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first", 2, types.StringKind, false),
				schema.NewColumn("is_married", 3, types.BoolKind, false)),
		},
		{
			name:  "Test not null constraints",
			query: "create table people (id int, age int, first varchar(80) not null, is_married bool, primary key (id, age))",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first", 2, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", 3, types.BoolKind, false)),
		},
		{
			name:  "Test quoted columns",
			query: "create table people (`id` int, `age` int, `timestamp` varchar(80), `is married` bool, primary key (`id`, `age`))",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("timestamp", 2, types.StringKind, false),
				schema.NewColumn("is married", 3, types.BoolKind, false)),
		},
		{
			name:  "Test tag comments",
			query: "create table people (id int primary key comment 'tag:5', age int comment 'tag:10')",
			expectedSchema: createSchema(
				schema.NewColumn("id", 5, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 10, types.IntKind, false)),
		},
		{
			name:  "Test faulty tag comments",
			query: "create table people (id int primary key comment 'tag:a', age int comment 'this is my personal area')",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, false)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dEnv := dtestutils.CreateTestEnv()
			root, _ := dEnv.WorkingRoot(context.Background())

			sqlStatement, err := sqlparser.Parse(tt.query)
			assert.NoError(t, err)

			s := sqlStatement.(*sqlparser.DDL)

			updatedRoot, sch, err := ExecuteCreate(context.Background(), dEnv.DoltDB, root, s, tt.query)

			if err != nil {
				if !tt.expectedErr {
					require.NoError(t, err)
				}
			} else {
				require.False(t, tt.expectedErr, "expected error")
			}

			assert.Equal(t, tt.expectedErr, err != nil, "unexpected error condition")
			if !tt.expectedErr {
				assert.NotNil(t, updatedRoot)
				assert.Equal(t, tt.expectedSchema, sch)
			}
		})
	}
}
