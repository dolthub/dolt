package sql

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/stretchr/testify/assert"
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
			name: "Test create single column schema",
			query: "create table people (id int primary key)",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{})),
		},
		{
			name: "Test create two column schema",
			query: "create table people (id int primary key, age int)",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, false)),
		},
		{
			name: "Test syntax error",
			query: "create table people id int, age int",
			expectedErr: true,
		},
		{
			name: "Test no primary keys",
			query: "create table people (id int, age int)",
			expectedErr: true,
		},
		{
			name: "Test types",
			query: "create table people (id int primary key, age int, first varchar(80), is_married bit)",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, false),
				schema.NewColumn("first", 2, types.StringKind, false),
				schema.NewColumn("is_married", 3, types.BoolKind, false)),
		},
		{
			name: "Test primary keys",
			query: "create table people (id int, age int, first varchar(80), is_married bit, primary key (id, age))",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first", 2, types.StringKind, false),
				schema.NewColumn("is_married", 3, types.BoolKind, false)),
		},
		{
			name: "Test not null constraints",
			query: "create table people (id int, age int, first varchar(80) not null, is_married bit, primary key (id, age))",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first", 2, types.StringKind, false, schema.NotNullConstraint{}),
				schema.NewColumn("is_married", 3, types.BoolKind, false)),
		},
		{
			name: "Test quoted columns",
			query: "create table people (`id` int, `age` int, `first` varchar(80), `is_married` bit, primary key (`id`, `age`))",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("first", 2, types.StringKind, false),
				schema.NewColumn("is_married", 3, types.BoolKind, false)),
		},
		{
			name: "Test tag comments",
			query: "create table people (id int primary key comment 'tag:5', age int comment 'tag:10')",
			expectedSchema: createSchema(
				schema.NewColumn("id", 5, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 10, types.IntKind, false)),
		},
		{
			name: "Test faulty tag comments",
			query: "create table people (id int primary key comment 'tag:a', age int comment 'this is my personal area')",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true, schema.NotNullConstraint{}),
				schema.NewColumn("age", 1, types.IntKind, false)),
		},
	}

	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		root, _ := dEnv.WorkingRoot(context.Background())
		sqlStatement, err := sqlparser.Parse(tt.query)
		assert.Nil(t, err)

		s := sqlStatement.(*sqlparser.DDL)

		t.Run(tt.name, func(t *testing.T) {
			updatedRoot, sch, err := ExecuteCreate(context.Background(), dEnv.DoltDB, root, s, tt.query)

			assert.Equal(t, tt.expectedErr, err != nil, "unexpected error condition")
			if !tt.expectedErr {
				assert.NotNil(t, updatedRoot)
				assert.Equal(t, tt.expectedSchema, sch)
			}
		})
	}
}
