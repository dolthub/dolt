package sql

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/dtestutils"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/stretchr/testify/assert"
	"github.com/xwb1989/sqlparser"
	"testing"
)

func TestWhereClauseErrorHandling(t *testing.T) {
	tests := []struct {
		name           string
		query          string
		expectedRows   []row.Row
		expectedSchema schema.Schema
		expectedErr    string
	}{
		{
			name: "Type mismatch: int -> string",
			query: `select * from people where first = 0`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: int -> bool",
			query: `select * from people where is_married = 0`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: int -> uuid",
			query: `select * from people where uuid = 0`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: string -> int",
			query: `select * from people where age = "yes"`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: string -> float",
			query: `select * from people where rating = "great"`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: string -> uint",
			query: `select * from people where num_episodes = "so many"`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: string -> uuid",
			query: `select * from people where uuid = "this is a uuid i promise"`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: float -> string",
			query: `select * from people where first = 1.5`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: float -> bool",
			query: `select * from people where is_married = 1.5`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: float -> int",
			query: `select * from people where age = 10.5`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: bool -> int",
			query: `select * from people where age = true`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: bool -> float",
			query: `select * from people where rating = false`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: bool -> string",
			query: `select * from people where first = true`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: bool -> uuid",
			query: `select * from people where uuid = false`,
			expectedErr: "Type mismatch:",
		},
		{
			name: "Type mismatch: non-bool column used as bool",
			query: `select * from people where uuid`,
			expectedErr: "Type mismatch:",
		},
	}
	for _, tt := range tests {
		dEnv := dtestutils.CreateTestEnv()
		createTestDatabase(dEnv, t)
		root, _ := dEnv.WorkingRoot(context.Background())

		sqlStatement, _ := sqlparser.Parse(tt.query)
		s := sqlStatement.(*sqlparser.Select)

		t.Run(tt.name, func(t *testing.T) {
			if tt.expectedRows != nil && tt.expectedSchema == nil {
				assert.Fail(t, "Incorrect test setup: schema must both be provided when rows are")
				t.FailNow()
			}

			rows, sch, err := ExecuteSelect(context.Background(), root, s, tt.query)
			if tt.expectedErr != "" {
				assert.NotNil(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				assert.Nil(t, err)
			}
			untypedRows := convertRows(t, tt.expectedRows, peopleTestSchema, tt.expectedSchema)
			assert.Equal(t, untypedRows, rows)
			assert.Equal(t, tt.expectedSchema, sch)
		})
	}
}
