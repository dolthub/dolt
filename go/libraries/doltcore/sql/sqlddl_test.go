package sql

import (
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
				schema.NewColumn("id", 0, types.IntKind, true)),
		},
		{
			name: "Test create two single column schema",
			query: "create table people (id int primary key, age int)",
			expectedSchema: createSchema(
				schema.NewColumn("id", 0, types.IntKind, true),
				schema.NewColumn("age", 1, types.IntKind, false)),
		},

	}
	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		root, _ := dEnv.WorkingRoot()
		sqlStatement, _ := sqlparser.Parse(tt.query)
		s := sqlStatement.(*sqlparser.DDL)

		t.Run(tt.name, func(t *testing.T) {
			updatedRoot, sch, err := ExecuteCreate(dEnv.DoltDB, root, s, tt.query)

			assert.Equal(t, tt.expectedErr, err != nil)
			assert.NotNil(t, updatedRoot)
			assert.Equal(t, tt.expectedSchema, sch)
		})
	}
}
